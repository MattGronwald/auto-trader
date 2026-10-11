"""Pure risk checks (spec §4, §5). Base case: entry 100.1 (ask 100 + 10 bps), stop 98
(d = 2.1), ATR 2, equity 1000 -> qty 2.997 bound by the 30 % position cap."""

from decimal import Decimal as D
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from autotrader.broker.base import AssetRules, FeeSchedule
from autotrader.core.config import StrategyProfile, load_profile
from autotrader.core.kill import ControlStatus
from autotrader.risk.checks import check_entry, check_exit, entry_limit
from autotrader.risk.types import (
    EntryIntent,
    ExitRequest,
    HeldPosition,
    MarketSnapshot,
    OpenEntry,
    PortfolioState,
    RiskDecision,
)

PROFILE: StrategyProfile = load_profile(
    Path(__file__).resolve().parents[3] / "strategies" / "fast_momentum_v1.yaml"
)
BTC, ETH, SOL = "BTC/USD", "ETH/USD", "SOL/USD"
RULES = AssetRules(
    min_qty=D("0.0001"),
    qty_increment=D("0.0001"),
    min_notional=D("1"),
    price_increment=D("0.01"),
)
FEES = FeeSchedule(maker_bps=D("15"), taker_bps=D("25"))
ALLOWED = ControlStatus(killed=False, reasons=())

INTENT = EntryIntent(
    cycle_id=7,
    symbol=BTC,
    stop=D("98"),
    take_profit=D("106"),
    confidence=D("0.6"),
    size_fraction=D("1"),
)
PORTFOLIO = PortfolioState(
    equity=D("1000"), cash=D("1000"), day_start_equity=D("1000"), hwm=D("1000")
)
MARKET = MarketSnapshot(
    last_trade=D("100"),
    bid=D("99.98"),
    ask=D("100"),
    spread_bps=D("2"),
    atr=D("2"),
    stale=False,
    rules=RULES,
    fees=FEES,
)


def run(
    intent: dict[str, Any] | None = None,
    portfolio: dict[str, Any] | None = None,
    market: dict[str, Any] | None = None,
    *,
    control: ControlStatus = ALLOWED,
    mode_matches: bool = True,
) -> RiskDecision:
    return check_entry(
        INTENT.model_copy(update=intent or {}),
        PORTFOLIO.model_copy(update=portfolio or {}),
        MARKET.model_copy(update=market or {}),
        profile=PROFILE,
        control=control,
        mode_matches=mode_matches,
    )


def held(symbol: str, qty: str = "1", value: str = "100") -> HeldPosition:
    return HeldPosition(symbol=symbol, qty=D(qty), market_value=D(value), min_qty=D("0.001"))


def test_entry_limit_rounds_up_to_price_increment() -> None:
    assert entry_limit(D("100"), D("10"), D("0.01")) == D("100.10")
    assert entry_limit(D("100.003"), D("10"), D("0.01")) == D("100.11")  # 100.103003 up


def test_base_case_is_approved_as_marketable_ioc_limit() -> None:
    d = run()
    assert d.approved, d.reasons
    assert d.qty == D("2.997")
    assert d.adjusted["bound_by"] == "position"
    assert d.order is not None
    assert (d.order.type, d.order.time_in_force, d.order.limit_price) == (
        "limit",
        "ioc",
        D("100.10"),
    )
    assert (d.order.side, d.order.qty, d.order.client_order_id) == ("buy", D("2.997"), "at-c7-e")


@pytest.mark.parametrize(
    ("kwargs", "reason"),
    [
        # 1 control and mode
        ({"control": ControlStatus(killed=True, reasons=("kill file present",))}, "control"),
        ({"mode_matches": False}, "mode"),
        # 2 market
        ({"intent": {"symbol": "DOGE/USD"}}, "universe"),
        ({"market": {"stale": True}}, "stale"),
        ({"market": {"spread_bps": None}}, "quote"),
        ({"market": {"ask": None}}, "quote"),
        ({"market": {"last_trade": None}}, "quote"),
        ({"market": {"spread_bps": D("15.01")}}, "spread"),
        # 5 positions
        ({"portfolio": {"positions": (held(BTC),)}}, "already"),
        ({"portfolio": {"open_entries": (OpenEntry(symbol=BTC, notional=D("1")),)}}, "already"),
        (
            {
                "portfolio": {
                    "positions": (held(ETH),),
                    "open_entries": (OpenEntry(symbol=SOL, notional=D("1")),),
                }
            },
            "max_positions",
        ),
        # 6 stop
        ({"market": {"atr": None}}, "ATR"),
        ({"intent": {"stop": D("100.10")}}, "below entry"),
        ({"intent": {"stop": D("98.51")}}, "stop distance"),  # d = 1.59 < 0.8 x 2
        ({"intent": {"stop": D("94.09")}}, "stop distance"),  # d = 6.01 > 3.0 x 2
    ],
)
def test_entry_rejections_checks_1_to_6(kwargs: dict[str, Any], reason: str) -> None:
    d = run(**kwargs)
    assert not d.approved
    assert reason in d.reasons[0]
    assert d.order is None and d.pause is None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"market": {"spread_bps": D("15")}},  # at the limit
        {"intent": {"stop": D("98.5")}},  # d = 1.6 = 0.8 x ATR
        # d = 6.0 = 3.0 x ATR; tp 120 keeps expectancy positive once Task 5 adds check 11
        {"intent": {"stop": D("94.1"), "take_profit": D("120")}},
        # dust ETH is ignored: 1 open entry (SOL) < 2
        {
            "portfolio": {
                "positions": (held(ETH, qty="0.0005"),),
                "open_entries": (OpenEntry(symbol=SOL, notional=D("1")),),
            }
        },
        {"portfolio": {"positions": (held(BTC, qty="0.0005"),)}},  # dust in the same symbol
    ],
)
def test_entry_boundaries_pass_checks_1_to_6(kwargs: dict[str, Any]) -> None:
    d = run(**kwargs)
    assert d.approved, d.reasons


def test_daily_loss_rejects_and_pauses_until_next_day() -> None:
    assert run(portfolio={"equity": D("970.01")}).approved  # loss 29.99 < 30
    d = run(portfolio={"equity": D("970")})  # loss 30 = 3 % of 1000
    assert not d.approved and "daily loss" in d.reasons[0]
    assert d.pause == "until_next_day"


def test_drawdown_rejects_and_pauses_manually() -> None:
    ok = {"equity": D("990.01"), "day_start_equity": D("990.01"), "hwm": D("1100")}
    assert run(portfolio=ok).approved  # 109.99 < 110
    d = run(portfolio={**ok, "equity": D("990"), "day_start_equity": D("990")})
    assert not d.approved and "drawdown" in d.reasons[0]
    assert d.pause == "manual"


def test_size_below_broker_minimum_is_rejected() -> None:
    d = run(portfolio={"cash": D("0.01")})  # cash cap floors to 0
    assert not d.approved and "below broker minimum" in d.reasons[0]


# --- exit ----------------------------------------------------------------------------


def test_exit_approved_for_held_position_with_broker_qty() -> None:
    d = check_exit(
        ExitRequest(symbol=BTC, reason="stop"), [held(BTC, qty="2.9895")], mode_matches=True
    )
    assert d.approved and d.qty == D("2.9895") and d.reasons == ("exit: stop",)


@pytest.mark.parametrize(
    ("positions", "mode_matches", "reason"),
    [
        ([held(BTC)], False, "mode"),
        ([], True, "no position"),
        ([held(ETH)], True, "no position"),
        ([held(BTC, qty="0.0005")], True, "no position"),  # dust
    ],
)
def test_exit_rejections(positions: list[HeldPosition], mode_matches: bool, reason: str) -> None:
    d = check_exit(ExitRequest(symbol=BTC, reason="kill"), positions, mode_matches=mode_matches)
    assert not d.approved and reason in d.reasons[0]


# --- checks 7-11 -----------------------------------------------------------------------


@pytest.mark.parametrize(
    ("kwargs", "reason"),
    [
        # 7 size: 1e-5 x 2.997 rounds down to 0
        ({"intent": {"size_fraction": D("0.00001")}}, "below broker minimum"),
        # 8 exposure: 300.01 + 2.997 x 100.1 = 600.0097 > 600
        ({"portfolio": {"positions": (held(ETH, value="300.01"),)}}, "exposure"),
        # 9 rate
        ({"portfolio": {"entries_last_hour": 6}}, "per hour"),
        ({"portfolio": {"entries_today": 30}}, "per day"),
        # 10 price sanity: |100.1 - 99| = 1.1 > 0.99
        ({"market": {"last_trade": D("99")}}, "deviates"),
        ({"intent": {"take_profit": D("100.10")}}, "take profit"),
        # 11 expectancy: tp too close
        ({"intent": {"take_profit": D("101")}}, "expectancy"),
        # 11 with history: 20 trades at 20 % -> p = 0.3 -> negative
        ({"portfolio": {"strategy_trades_n": 20, "strategy_win_rate": D("0.2")}}, "expectancy"),
    ],
)
def test_entry_rejections_checks_7_to_11(kwargs: dict[str, Any], reason: str) -> None:
    d = run(**kwargs)
    assert not d.approved
    assert reason in d.reasons[0]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"portfolio": {"positions": (held(ETH, value="300"),)}},  # 599.9997 <= 600
        {"portfolio": {"entries_last_hour": 5, "entries_today": 29}},
        # 19 trades: still the 0.40 prior, p = min(0.6, 0.5) = 0.5 -> positive
        {"portfolio": {"strategy_trades_n": 19, "strategy_win_rate": D("0.2")}},
    ],
)
def test_entry_boundaries_pass_checks_7_to_11(kwargs: dict[str, Any]) -> None:
    assert run(**kwargs).approved


def test_open_entry_notional_counts_toward_exposure() -> None:
    d = run(portfolio={"open_entries": (OpenEntry(symbol=ETH, notional=D("300.01")),)})
    assert not d.approved and "exposure" in d.reasons[0]


def test_sizing_caps_choose_the_smallest() -> None:
    assert run(portfolio={"cash": D("100")}).adjusted["bound_by"] == "cash"
    d = run(intent={"stop": D("94.1"), "take_profit": D("120")})  # d = 6: 10/6 = 1.666 < 2.997
    assert d.adjusted["bound_by"] == "risk" and d.qty == D("1.6666")


def test_expected_value_is_reported() -> None:
    # p = 0.5; per unit: 5.9 x 0.5 - 2.1 x 0.5 - 100.1 x 0.0025 - 106 x 0.0025 = 1.38475
    d = run()
    assert D(d.adjusted["expected_value"]) == D("2.997") * D("1.38475")


money = st.decimals(min_value=D("10"), max_value=D("100000"), places=2)


@settings(max_examples=300)
@given(
    equity=money,
    cash_share=st.decimals(min_value=D("0"), max_value=D("1"), places=3),
    ask=st.decimals(min_value=D("1"), max_value=D("100000"), places=2),
    stop_atr=st.decimals(min_value=D("0.8"), max_value=D("3"), places=2),
    reward=st.decimals(min_value=D("0.5"), max_value=D("10"), places=2),
    fraction=st.decimals(min_value=D("0.01"), max_value=D("1"), places=2),
    confidence=st.decimals(min_value=D("0"), max_value=D("1"), places=2),
)
def test_sizing_invariants(
    equity: D, cash_share: D, ask: D, stop_atr: D, reward: D, fraction: D, confidence: D
) -> None:
    entry = entry_limit(ask, PROFILE.risk.entry_slippage_bps, RULES.price_increment)
    atr = ask / 50
    stop = (entry - stop_atr * atr).quantize(D("0.0001"))
    if stop <= 0:
        return
    distance = entry - stop
    tp = entry + reward * distance
    cash = equity * cash_share
    d = run(
        intent={
            "stop": stop,
            "take_profit": tp,
            "confidence": confidence,
            "size_fraction": fraction,
        },
        portfolio={"equity": equity, "cash": cash, "day_start_equity": equity, "hwm": equity},
        market={"ask": ask, "last_trade": ask, "atr": atr},
    )
    if not d.approved:
        return
    assert d.qty is not None and d.order is not None
    qty, taker = d.qty, FEES.taker_bps / D(10_000)
    tiny = D("1e-18") * equity  # Decimal division rounds to 28 digits
    assert qty > 0 and qty % RULES.qty_increment == 0
    assert qty * distance <= PROFILE.risk.risk_per_trade_pct / 100 * equity + tiny
    assert qty * entry <= PROFILE.risk.max_position_pct / 100 * equity + tiny
    assert qty * entry * (1 + taker) <= cash + tiny
    assert stop < entry < tp
    assert D(d.adjusted["expected_value"]) > 0
