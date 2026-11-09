"""Pure Risk Engine checks (SPEC §4.5, spec 2026-10-11 §4-5). No I/O, no clock.

`check_entry` evaluates the 11 checks in order and stops at the first failure; that
reason is what gets persisted. `check_exit` is reduce-only (G1): it never looks at
control state, pauses or rate limits, so a stop-out can't be blocked.
"""

from collections.abc import Sequence
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from autotrader.broker.base import BrokerOrder, FeeSchedule
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


def net_pnl_per_unit(
    entry: Decimal, exit_price: Decimal, fees: FeeSchedule, exit_slippage_bps: Decimal
) -> Decimal:
    """Estimated net cash result per unit bought at `entry` (D16): the exit fills at
    `exit_price` less the adverse slippage allowance, both legs pay the taker fee (IOC
    entry, market exit). A buy fee in the coin (`base`, Alpaca) leaves fewer units to
    sell; one in USD (`quote`) is paid on top. Never both deductions at once."""
    fee = fees.taker_bps / _BPS
    proceeds_per_unit = exit_price * (1 - exit_slippage_bps / _BPS) * (1 - fee)
    if fees.buy_fee_asset == "base":
        return (1 - fee) * proceeds_per_unit - entry
    return proceeds_per_unit - entry * (1 + fee)


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
    bid, ask, last_trade = market.bid, market.ask, market.last_trade
    if bid is None or ask is None or last_trade is None:
        return _reject("no quote or last trade")
    # Second line behind the feed's own filter: never size off a crossed or empty book.
    # A locked book (bid == ask) is a valid price.
    if bid <= 0 or ask <= 0 or last_trade <= 0 or bid > ask:
        return _reject(f"invalid quote: bid {bid}, ask {ask}, last trade {last_trade}")
    # Derived here from the same bid/ask, so the spread can't disagree with the prices.
    spread_bps = (ask - bid) / ((ask + bid) / 2) * _BPS
    if spread_bps > profile.scanner.max_spread_bps:
        return _reject(f"spread {spread_bps:.2f} bps > {profile.scanner.max_spread_bps}")
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
    entry = entry_limit(ask, limits.entry_slippage_bps, market.rules.price_increment)
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
    return _size_and_finish(intent, portfolio, market, profile, entry, distance, last_trade)


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
    # 7. size (G4 + cash cap, D14). D16: the risk budget bounds the estimated *net* loss
    # at the stop (fees on both legs + exit slippage), not just the price distance.
    # Positive: stop < entry (check 6), 0 <= fee < 100 % (FeeSchedule), 0 <= slip < 100 %.
    unit_loss = -net_pnl_per_unit(entry, intent.stop, market.fees, limits.exit_slippage_bps)
    caps = {
        "risk": limits.risk_per_trade_pct / _PCT * portfolio.equity / unit_loss,
        "position": limits.max_position_pct / _PCT * portfolio.equity / entry,
        "cash": portfolio.cash / (entry * (1 + taker)),
    }
    bound_by = min(caps, key=caps.__getitem__)
    qty = _step(intent.size_fraction * caps[bound_by], rules.qty_increment, ROUND_FLOOR)
    if qty < rules.min_qty or qty * entry < rules.min_notional:
        return _reject(f"size {qty} below broker minimum")
    # A coin buy fee shrinks the held qty; below min_qty a FULLY filled position would be
    # dust (not exitable, frees its slot). Fee rounded up to the qty step: never
    # under-estimated. A small partial IOC fill can still leave dust (the ledger writes it off at
    # close, WP 1.5b).
    if market.fees.buy_fee_asset == "base":
        net = qty - _step(qty * taker, rules.qty_increment, ROUND_CEILING)
        if net < rules.min_qty:
            return _reject(f"net qty {net} after coin fee below broker minimum {rules.min_qty}")
    notional = qty * entry
    # 8. exposure (reject, never shrink). An unpriced (no or stale feed price) non-dust
    # holding makes the sum unknowable; cost basis would undercount, so reject. Dust is
    # ignored when unpriced and counted when priced.
    for pos in portfolio.positions:
        if pos.market_value is None and not pos.is_dust:
            return _reject(f"held position {pos.symbol} has no current price")
    exposure = (
        sum((p.market_value for p in portfolio.positions if p.market_value is not None), Decimal(0))
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
    # Same cash-flow model as the size (D16); the TP exit also gets the adverse slippage.
    unit_gain = net_pnl_per_unit(entry, intent.take_profit, market.fees, limits.exit_slippage_bps)
    ev = qty * (p * unit_gain - (1 - p) * unit_loss)
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
        adjusted={
            "entry": str(entry),
            "bound_by": bound_by,
            "expected_value": str(ev),
            # R stays the price distance; the D16 estimate is reported next to it.
            "initial_risk": str(qty * distance),
            "atr": str(market.atr),
            "est_net_stop_loss": str(qty * unit_loss),
        },
        qty=qty,
        order=order,
    )


def check_exit(
    request: ExitRequest, positions: Sequence[HeldPosition], *, mode_matches: bool
) -> RiskDecision:
    if not mode_matches:
        return _reject("mode mismatch between config and broker")
    position = next((p for p in positions if p.symbol == request.symbol), None)
    if position is None:
        return _reject(f"no position to exit in {request.symbol}")
    if position.is_dust:
        return _reject(f"dust only in {request.symbol}")
    return RiskDecision(approved=True, reasons=(f"exit: {request.reason}",), qty=position.qty)
