"""Pure order-resolver helpers (spec 2026-11-10): id formats, ownership, broker-view match."""

from datetime import UTC, datetime, timedelta
from decimal import Decimal as D

import pytest

from autotrader.broker.base import BrokerOrderView
from autotrader.risk.resolve import (
    CREATED_AT_SKEW,
    entry_client_id,
    exit_client_id,
    mismatches,
    owns,
)

JID = "0123abcd"
SUBMITTED = datetime(2026, 11, 10, 12, 0, tzinfo=UTC)


def view(**kw: object) -> BrokerOrderView:
    base: dict[str, object] = dict(
        order_id="b-1",
        client_order_id=f"at-{JID}-c7-e",
        symbol="BTC/USD",
        side="buy",
        qty=D("1"),
        status="new",
        filled_qty=D(0),
        created_at=SUBMITTED,
    )
    return BrokerOrderView(**{**base, **kw})


def check(v: BrokerOrderView, broker_order_id: str | None = None) -> tuple[str, ...]:
    return mismatches(
        v,
        symbol="BTC/USD",
        side="buy",
        qty=D("1"),
        submitted_at=SUBMITTED,
        broker_order_id=broker_order_id,
    )


def test_id_formats() -> None:
    assert entry_client_id(JID, 7) == "at-0123abcd-c7-e"
    assert exit_client_id(JID, 42) == "at-0123abcd-o42"
    assert timedelta(minutes=5) == CREATED_AT_SKEW


@pytest.mark.parametrize(
    ("client_id", "expected"),
    [
        (f"at-{JID}-c7-e", True),
        (f"at-{JID}-o5", True),
        ("at-ffffffff-c7-e", False),
        (None, False),
        ("at-o5", False),
        ("at-c7-e", False),
        (f"at-{JID}x-c7-e", False),  # prefix collision: the dash ends the id
    ],
)
def test_owns(client_id: str | None, expected: bool) -> None:
    assert owns(client_id, JID) is expected


def test_match_is_empty() -> None:
    assert check(view()) == ()
    assert check(view(order_id="b-1"), "b-1") == ()


def test_one_entry_per_differing_field() -> None:
    assert len(check(view(symbol="ETH/USD"))) == 1
    assert len(check(view(side="sell"))) == 1
    assert len(check(view(qty=D("2")))) == 1
    assert len(check(view(created_at=SUBMITTED - timedelta(minutes=6)))) == 1
    assert len(check(view(order_id="b-2"), "b-1")) == 1
    all_wrong = view(
        symbol="ETH/USD",
        side="sell",
        qty=D("2"),
        order_id="b-2",
        created_at=SUBMITTED - timedelta(hours=1),
    )
    assert len(check(all_wrong, "b-1")) == 5


def test_created_at_bound_is_inclusive() -> None:
    assert check(view(created_at=SUBMITTED - CREATED_AT_SKEW)) == ()
    late = view(created_at=SUBMITTED - CREATED_AT_SKEW - timedelta(microseconds=1))
    assert len(check(late)) == 1
    assert check(view(created_at=SUBMITTED + timedelta(hours=1))) == ()


def test_order_id_is_compared_only_when_given() -> None:
    assert check(view(order_id="anything")) == ()
    assert len(check(view(order_id="anything"), "b-1")) == 1
