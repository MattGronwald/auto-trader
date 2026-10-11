"""Pure risk checks (spec §4, §5). Base case: entry 100.1 (ask 100 + 10 bps), stop 98
(d = 2.1), ATR 2, equity 1000 -> qty 2.997 bound by the 30 % position cap."""

from decimal import Decimal as D
from pathlib import Path
from typing import Any

import pytest

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
