"""Pure Risk Engine checks (SPEC §4.5, spec 2026-10-11 §4-5). No I/O, no clock.

`check_entry` evaluates the 11 checks in order and stops at the first failure; that
reason is what gets persisted. `check_exit` is reduce-only (G1): it never looks at
control state, pauses or rate limits, so a stop-out can't be blocked.
"""

from collections.abc import Sequence
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from autotrader.broker.base import BrokerOrder
from autotrader.core.config import StrategyProfile
from autotrader.core.kill import ControlStatus
from autotrader.risk.types import (
    EntryIntent,
    ExitRequest,
    HeldPosition,
    MarketSnapshot,
    Pause,
    PortfolioState,
    RiskDecision,
)

_BPS = Decimal(10_000)
_PCT = Decimal(100)
WIN_RATE_MARGIN = Decimal("0.10")  # SPEC §4.5 #11: confidence capped at win rate + 10 pp


def _reject(reason: str, pause: Pause | None = None) -> RiskDecision:
    return RiskDecision(approved=False, reasons=(reason,), pause=pause)


def _step(value: Decimal, step: Decimal, rounding: str) -> Decimal:
    return (value / step).to_integral_value(rounding=rounding) * step


def entry_limit(ask: Decimal, slippage_bps: Decimal, price_increment: Decimal) -> Decimal:
    """Marketable IOC limit (D14): ask plus the allowance, rounded up to the price step."""
    return _step(ask * (1 + slippage_bps / _BPS), price_increment, ROUND_CEILING)


def check_entry(
    intent: EntryIntent,
    portfolio: PortfolioState,
    market: MarketSnapshot,
    *,
    profile: StrategyProfile,
    control: ControlStatus,
    mode_matches: bool,
) -> RiskDecision:
    limits = profile.risk
    # 1. control and mode
    if not control.entries_allowed:
        return _reject("control: " + "; ".join(control.reasons))
    if not mode_matches:
        return _reject("mode mismatch between config and broker")
    # 2. market
    if intent.symbol not in profile.universe:
        return _reject(f"{intent.symbol} not in universe")
    if market.stale:
        return _reject("symbol stale")
    if market.spread_bps is None or market.ask is None or market.last_trade is None:
        return _reject("no quote or last trade")
    if market.spread_bps > profile.scanner.max_spread_bps:
        return _reject(f"spread {market.spread_bps} bps > {profile.scanner.max_spread_bps}")
    # 3. daily loss
    loss = portfolio.day_start_equity - portfolio.equity
    if loss >= limits.max_daily_loss_pct / _PCT * portfolio.day_start_equity:
        return _reject(f"daily loss {loss} >= {limits.max_daily_loss_pct} %", "until_next_day")
    # 4. drawdown
    drawdown = portfolio.hwm - portfolio.equity
    if drawdown >= limits.max_drawdown_pct / _PCT * portfolio.hwm:
        return _reject(f"drawdown {drawdown} >= {limits.max_drawdown_pct} %", "manual")
    # 5. positions (dust ignored)
    held = {p.symbol for p in portfolio.positions if not p.is_dust}
    busy = held | {o.symbol for o in portfolio.open_entries}
    if intent.symbol in busy:
        return _reject(f"{intent.symbol} already has a position or open entry order")
    if len(busy) >= limits.max_positions:
        return _reject(f"{len(busy)} positions/open entries >= max_positions")
    # 6. stop
    entry = entry_limit(market.ask, limits.entry_slippage_bps, market.rules.price_increment)
    if market.atr is None:
        return _reject("ATR unavailable")
    if intent.stop >= entry:
        return _reject(f"stop {intent.stop} not below entry {entry}")
    distance = entry - intent.stop
    if not limits.min_stop_atr * market.atr <= distance <= limits.max_stop_atr * market.atr:
        return _reject(
            f"stop distance {distance} outside {limits.min_stop_atr}-"
            f"{limits.max_stop_atr} x ATR {market.atr}"
        )
    return _size_and_finish(intent, portfolio, market, profile, entry, distance, market.last_trade)


def _size_and_finish(
    intent: EntryIntent,
    portfolio: PortfolioState,
    market: MarketSnapshot,
    profile: StrategyProfile,
    entry: Decimal,
    distance: Decimal,
    last_trade: Decimal,
) -> RiskDecision:
    limits, rules = profile.risk, market.rules
    taker = market.fees.taker_bps / _BPS
    # 7. size (G4 + cash cap, D14)
    caps = {
        "risk": limits.risk_per_trade_pct / _PCT * portfolio.equity / distance,
        "position": limits.max_position_pct / _PCT * portfolio.equity / entry,
        "cash": portfolio.cash / (entry * (1 + taker)),
    }
    bound_by = min(caps, key=caps.__getitem__)
    qty = _step(intent.size_fraction * caps[bound_by], rules.qty_increment, ROUND_FLOOR)
    if qty < rules.min_qty or qty * entry < rules.min_notional:
        return _reject(f"size {qty} below broker minimum")
    notional = qty * entry
    # 8. exposure (reject, never shrink)
    exposure = (
        sum((p.market_value for p in portfolio.positions), Decimal(0))
        + sum((o.notional for o in portfolio.open_entries), Decimal(0))
        + notional
    )
    if exposure > limits.max_gross_exposure_pct / _PCT * portfolio.equity:
        return _reject(f"exposure {exposure} > {limits.max_gross_exposure_pct} % of equity")
    # 9. order rate (entries only; exits never count)
    if portfolio.entries_last_hour >= limits.max_orders_per_hour:
        return _reject(f"{portfolio.entries_last_hour} entry orders per hour >= limit")
    if portfolio.entries_today >= limits.max_orders_per_day:
        return _reject(f"{portfolio.entries_today} entry orders per day >= limit")
    # 10. price sanity
    if abs(entry - last_trade) > limits.max_price_deviation_pct / _PCT * last_trade:
        return _reject(
            f"entry {entry} deviates > {limits.max_price_deviation_pct} % from {last_trade}"
        )
    if intent.take_profit <= entry:
        return _reject(f"take profit {intent.take_profit} not above entry {entry}")
    # 11. fee-adjusted expectancy (G3, D4)
    # `strategy_win_rate is None` falls back to the prior even when the trade count is high
    # (cannot happen via the engine, which derives the rate from the count).
    rate = portfolio.strategy_win_rate
    if rate is None or portfolio.strategy_trades_n < profile.learning.min_trades_for_evidence:
        rate = limits.default_win_rate_prior
    p = min(intent.confidence, rate + WIN_RATE_MARGIN)
    ev = (
        qty * (intent.take_profit - entry) * p
        - qty * distance * (1 - p)
        - notional * taker
        - qty * intent.take_profit * taker
    )
    if ev <= 0:
        return _reject(f"expectancy {ev} <= 0 after fees (p = {p})")
    order = BrokerOrder(
        symbol=intent.symbol,
        side="buy",
        qty=qty,
        type="limit",
        limit_price=entry,
        time_in_force="ioc",
        client_order_id=f"at-c{intent.cycle_id}-e",
    )
    return RiskDecision(
        approved=True,
        reasons=(f"sized by {bound_by} cap",),
        adjusted={"entry": str(entry), "bound_by": bound_by, "expected_value": str(ev)},
        qty=qty,
        order=order,
    )


def check_exit(
    request: ExitRequest, positions: Sequence[HeldPosition], *, mode_matches: bool
) -> RiskDecision:
    if not mode_matches:
        return _reject("mode mismatch between config and broker")
    position = next((p for p in positions if p.symbol == request.symbol), None)
    if position is None or position.is_dust:
        return _reject(f"no position to exit in {request.symbol}")
    return RiskDecision(approved=True, reasons=(f"exit: {request.reason}",), qty=position.qty)
