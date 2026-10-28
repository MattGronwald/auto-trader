"""Pure exit rules (spec 2026-10-19 section 4.3.1, 4.4)."""

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal as D

import pytest

from autotrader.core.config import Trailing
from autotrader.core.exit_rules import (
    Backoff,
    ExitRules,
    ExitState,
    TrailUpdate,
    classify_sells,
    evaluate,
    evaluate_bar,
)
from autotrader.core.types import Bar

T0 = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)
ON = ExitRules(Trailing(enabled=True, trigger_r=D("1"), trail_atr=D("1.5")), False, 300)
STATE = ExitState(  # R = 5, activation at 105, trail distance 1.5 x 2 = 3
    entry_price=D("100"),
    stop=D("95"),
    tp=D("110"),
    time_limit_at=T0 + timedelta(hours=8),
    atr_at_entry=D("2"),
    trail_high=None,
    trail_stop=None,
)


@pytest.mark.parametrize(
    ("bid", "reason"),
    [("95", "stop"), ("95.01", None), ("110", "take_profit"), ("109.99", None), ("94", "stop")],
)
def test_price_triggers_fire_at_their_boundary(bid: str, reason: str | None) -> None:
    assert evaluate(STATE, D(bid), 0.0, T0, ON)[0] == reason


def test_trailing_activates_at_exactly_trigger_r_and_only_rises() -> None:
    assert evaluate(STATE, D("104.99"), 0.0, T0, ON) == (None, None)
    assert evaluate(STATE, D("105"), 0.0, T0, ON) == (None, TrailUpdate(D("105"), D("102")))
    up = replace(STATE, trail_high=D("106"), trail_stop=D("103"))
    assert evaluate(up, D("104"), 0.0, T0, ON) == (None, None)  # no lower high, no lower stop
    assert evaluate(up, D("103"), 0.0, T0, ON) == ("trailing", None)


def test_the_trailing_stop_never_sits_below_the_stop() -> None:
    wide = replace(STATE, atr_at_entry=D("10"))  # 105 - 15 = 90 < 95
    assert evaluate(wide, D("105"), 0.0, T0, ON) == (None, TrailUpdate(D("105"), D("95")))
    # trail_stop == stop: the reason stays "stop"
    assert (
        evaluate(replace(wide, trail_high=D("105"), trail_stop=D("95")), D("95"), 0.0, T0, ON)[0]
        == "stop"
    )


@pytest.mark.parametrize(
    "state",
    [replace(STATE, atr_at_entry=None), replace(STATE, stop=None), replace(STATE, stop=D("100"))],
)
def test_no_trailing_without_atr_stop_or_positive_r(state: ExitState) -> None:
    assert evaluate(state, D("108"), 0.0, T0, ON) == (None, None)  # above 105, below the TP


def test_trailing_disabled_by_config() -> None:
    off = replace(ON, trailing=Trailing(enabled=False, trigger_r=D("1"), trail_atr=D("1.5")))
    assert evaluate(STATE, D("108"), 0.0, T0, off) == (None, None)


def test_precedence_and_time_without_a_price() -> None:
    late = replace(STATE, time_limit_at=T0)
    assert evaluate(late, D("95"), 0.0, T0, ON)[0] == "stop"  # stop beats time
    assert evaluate(late, D("110"), 0.0, T0, ON)[0] == "take_profit"  # TP beats time
    assert evaluate(late, None, None, T0, ON)[0] == "time"
    assert evaluate(late, None, None, T0 - timedelta(seconds=1), ON)[0] is None


def test_stale_fires_only_when_enabled_and_past_the_threshold() -> None:
    stale_on = replace(ON, stale_exit_enabled=True)
    assert evaluate(STATE, None, 300.0, T0, stale_on)[0] is None
    assert evaluate(STATE, None, 300.5, T0, stale_on)[0] == "stale"
    assert evaluate(STATE, None, 10_000.0, T0, ON)[0] is None
    assert evaluate(STATE, None, None, T0, stale_on)[0] is None
    assert evaluate(replace(STATE, time_limit_at=T0), None, 999.0, T0, stale_on)[0] == "time"


def test_no_trailing_update_on_a_firing_step() -> None:
    assert evaluate(replace(STATE, tp=D("105")), D("105"), 0.0, T0, ON) == ("take_profit", None)


def _bar(low: float, high: float) -> Bar:
    return Bar(
        symbol="BTC/USD", timeframe="1m", ts=T0, open=100, high=high, low=low, close=100, volume=1
    )


def test_a_bar_through_stop_and_tp_is_a_stop_first() -> None:
    assert evaluate_bar(STATE, _bar(94, 111)) == "stop"
    assert evaluate_bar(STATE, _bar(96, 111)) == "take_profit"
    assert evaluate_bar(STATE, _bar(96, 109)) is None


@dataclass(frozen=True)
class Row:  # satisfies SellRow
    broker_order_id: str | None
    status: str


@pytest.mark.parametrize(
    ("rows", "state"),
    [
        ([], "attempt"),
        ([Row(None, "not_submitted")], "attempt"),
        ([Row(None, "rejected")], "attempt"),
        ([Row("b", "rejected")], "attempt"),
        ([Row(None, "pending")], "unresolved"),
        ([Row(None, "unknown"), Row("b", "new")], "unresolved"),
        ([Row("b", "new")], "in_flight"),
        ([Row("b", "partially_filled"), Row("c", "filled")], "in_flight"),
        ([Row("b", "filled")], "settling"),
        ([Row("b", "canceled"), Row(None, "not_submitted")], "settling"),
    ],
)
def test_sell_rows_classify_by_precedence(rows: list[Row], state: str) -> None:
    assert classify_sells(rows) == state


def test_backoff_doubles_to_a_30_s_cap_and_alerts_at_3_then_every_10th() -> None:
    b = Backoff()
    now = T0
    delays: list[float] = []
    alerts: list[int] = []
    for n in range(1, 25):
        assert b.due(now)
        if b.fail(now):
            alerts.append(n)
        assert b.next_at is not None
        delays.append((b.next_at - now).total_seconds())
        assert not b.due(now)
        now = b.next_at
    assert delays[:7] == [1, 2, 4, 8, 16, 30, 30]
    assert alerts == [3, 13, 23]
    b.reset()
    assert (b.failures, b.next_at, b.due(T0)) == (0, None, True)
