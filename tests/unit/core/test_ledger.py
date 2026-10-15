"""Ledger (WP 1.5b; spec 2026-10-15-fill-position-accounting-design.md) on SimBroker and a
migrated SQLite journal."""

import asyncio
import tempfile
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal as D
from pathlib import Path
from typing import Any

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.broker.base import (
    Account,
    AssetRules,
    BrokerOrder,
    FeeSchedule,
    Fill,
    OrderRejected,
)
from autotrader.broker.sim import FillPlan, SimBroker
from autotrader.core.bus import CommitUncertain, DomainEvent, EventBus
from autotrader.core.config import load_profile
from autotrader.core.kill import PAUSED_KEY
from autotrader.core.ledger import CASH_CHECK_KEY, Ledger, _cash_effect
from autotrader.core.types import Bar
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade
from autotrader.risk.types import OrderUpdate, SyncResult
from tests.dbhooks import CommitHook, hooked_sessionmaker

REPO = Path(__file__).resolve().parents[3]
PROFILE = load_profile(REPO / "strategies" / "fast_momentum_v1.yaml")
BTC = "BTC/USD"
T0 = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
RULES = AssetRules(
    min_qty=D("0.0001"), qty_increment=D("0.0001"), min_notional=D("1"), price_increment=D("0.01")
)
QUOTE = FeeSchedule(maker_bps=D("15"), taker_bps=D("25"), buy_fee_asset="quote")
BASE = FeeSchedule(maker_bps=D("15"), taker_bps=D("25"), buy_fee_asset="base")


def bar(i: int, price: float = 100.0) -> Bar:
    return Bar(
        symbol=BTC,
        timeframe="1m",
        ts=T0 + timedelta(minutes=i),
        open=price,
        high=price,
        low=price,
        close=price,
        volume=1.0,
    )


def make_sim(fees: FeeSchedule = QUOTE, cash: str = "1000") -> SimBroker:
    sim = SimBroker(cash=D(cash), fees=fees, slippage_bps=D(0), asset_rules={BTC: RULES})
    sim.process_bar(bar(0))
    return sim


@pytest.fixture
def db_url(tmp_path: Path) -> str:
    url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    upgrade(url)
    return url


@pytest.fixture
async def sessions(db_url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = make_engine(db_url)
    yield make_sessionmaker(engine)
    await engine.dispose()


@pytest.fixture
async def bus(sessions: async_sessionmaker[AsyncSession]) -> AsyncIterator[EventBus]:
    b = EventBus(sessions)
    yield b
    await b.close()


@pytest.fixture
def ledger(bus: EventBus, sessions: async_sessionmaker[AsyncSession]) -> Ledger:
    return Ledger(bus, sessions, clock=lambda: T0)


async def new_cycle(sessions: async_sessionmaker[AsyncSession], state: str = "SUBMITTED") -> int:
    async with sessions() as s:
        strategy = await repo.register_strategy(s, PROFILE, "yaml")
        row = m.Cycle(
            strategy_id=strategy.id,
            symbol=BTC,
            rule_id="r",
            signal_features={},
            state=state,
            created_at=T0,
        )
        s.add(row)
        await s.commit()
        return row.id


async def place(
    sessions: async_sessionmaker[AsyncSession],
    sim: SimBroker,
    side: str,
    qty: str,
    *,
    cycle_id: int | None = None,
    exit_reason: str | None = None,
    limit: str | None = None,
) -> tuple[int, str]:
    """Submit like the Risk Engine does and journal the row. `side`: "buy" (market, or an
    IOC limit with `limit`), "sell" (market) or "close" (close_position). `qty` is what
    the row records. Returns (order row id, broker order id)."""
    if side == "close":
        ack = await sim.close_position(BTC)
    elif limit is None:
        ack = await sim.submit_order(
            BrokerOrder(
                symbol=BTC, side="buy" if side == "buy" else "sell", qty=D(qty), type="market"
            )
        )
    else:
        ack = await sim.submit_order(
            BrokerOrder(
                symbol=BTC,
                side="buy",
                qty=D(qty),
                type="limit",
                limit_price=D(limit),
                time_in_force="ioc",
            )
        )
    buy = side == "buy"
    async with sessions() as s:
        row = await repo.insert_order(
            s,
            cycle_id=cycle_id,
            client_order_id=None,
            symbol=BTC,
            mode="paper",
            type_="market" if limit is None else "limit",
            side="buy" if buy else "sell",
            qty=D(qty),
            limit_price=None if limit is None else D(limit),
            stop_price=D("95") if buy else None,
            tp_price=D("110") if buy else None,
            status="pending",
            now=T0,
            exit_reason=exit_reason,
        )
        await repo.update_order(s, row.id, status=ack.status, now=T0, broker_order_id=ack.order_id)
    return row.id, ack.order_id


async def update(
    sim: SimBroker, row_id: int, broker_id: str, *, visible: int | None = None
) -> OrderUpdate:
    """What the engine's sync reads for one order; `visible` hides later executions."""
    status = await sim.get_order(broker_id)
    found = await sim.order_fills(broker_id)
    return OrderUpdate(
        order_row_id=row_id,
        status=status.status,
        filled_qty=status.filled_qty,
        fills=tuple(found if visible is None else found[:visible]),
        min_qty=RULES.min_qty,
    )


async def sync(sessions: async_sessionmaker[AsyncSession], sim: SimBroker) -> SyncResult:
    async with sessions() as s:
        rows = await repo.unsettled_orders(s)
    return SyncResult(
        updates=tuple(
            [await update(sim, r.id, r.broker_order_id) for r in rows if r.broker_order_id]
        )
    )


async def orders(sessions: async_sessionmaker[AsyncSession]) -> list[m.Order]:
    async with sessions() as s:
        return list(await s.scalars(select(m.Order).order_by(m.Order.id)))


async def fills(sessions: async_sessionmaker[AsyncSession]) -> list[m.Fill]:
    async with sessions() as s:
        return list(await s.scalars(select(m.Fill).order_by(m.Fill.id)))


async def positions(sessions: async_sessionmaker[AsyncSession]) -> list[m.Position]:
    async with sessions() as s:
        return list(await s.scalars(select(m.Position).order_by(m.Position.id)))


async def event_rows(sessions: async_sessionmaker[AsyncSession]) -> list[m.Event]:
    async with sessions() as s:
        return list(await s.scalars(select(m.Event).order_by(m.Event.id)))


async def event_types(sessions: async_sessionmaker[AsyncSession]) -> list[str]:
    return [e.type for e in await event_rows(sessions)]


async def paused(sessions: async_sessionmaker[AsyncSession]) -> bool:
    async with sessions() as s:
        return await repo.get_control(s, PAUSED_KEY) is True


async def cycle_state(sessions: async_sessionmaker[AsyncSession], cycle_id: int) -> str | None:
    async with sessions() as s:
        return await repo.cycle_state(s, cycle_id)


async def dust_balance(sessions: async_sessionmaker[AsyncSession]) -> D | None:
    async with sessions() as s:
        row = await s.get(m.SymbolDust, BTC)
        return None if row is None else row.qty


def anomaly_kind(rows: list[m.Event]) -> Any:
    return [e.payload["kind"] for e in rows if e.type == "LedgerAnomaly"][-1]


# --- entries ------------------------------------------------------------------------


async def test_full_entry_opens_the_position_and_fills_the_cycle(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    cycle = await new_cycle(sessions)
    async with sessions() as s:
        await repo.record_risk_decision(
            s,
            cycle_id=cycle,
            kind="entry",
            approved=True,
            reasons=[],
            adjusted={"initial_risk": "5"},
            computed_size=D("1"),
            ts=T0,
        )
    row, oid = await place(sessions, sim, "buy", "1", cycle_id=cycle)
    sim.process_bar(bar(1))

    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    (pos,) = await positions(sessions)
    assert (pos.cycle_id, pos.symbol, pos.qty, pos.entry_price, pos.fees) == (
        cycle,
        BTC,
        D("1"),
        D("100"),
        D("0.25"),
    )
    assert (pos.stop, pos.tp, pos.initial_risk, pos.closed_at) == (D("95"), D("110"), D("5"), None)
    (order,) = await orders(sessions)
    assert (order.status, order.filled_qty, order.position_id) == ("filled", D("1"), pos.id)
    assert order.settled_at is not None
    (fill,) = await fills(sessions)
    assert (fill.broker_fill_id, fill.side, fill.fee_asset) == (f"{oid}:1", "buy", "quote")
    assert await cycle_state(sessions, cycle) == "FILLED"
    assert await event_types(sessions) == ["OrderSettled", "CycleTransitioned"]
    settled = (await event_rows(sessions))[0].payload
    assert (settled["filled_qty"], settled["settled"], settled["status"]) == ("1", True, "filled")


async def test_partial_fill_then_cancel_fills_the_cycle_as_partial(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    cycle = await new_cycle(sessions)
    sim.script(BTC, "buy", FillPlan((D("0.5"), "cancel")))
    row, oid = await place(sessions, sim, "buy", "1", cycle_id=cycle)
    sim.process_bar(bar(1))
    sim.process_bar(bar(2))

    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    (order,) = await orders(sessions)
    assert (order.status, order.filled_qty) == ("canceled", D("0.5"))
    assert await cycle_state(sessions, cycle) == "FILLED"
    moved = [e.payload for e in await event_rows(sessions) if e.type == "CycleTransitioned"]
    assert moved[-1]["details"] == {"filled_qty": "0.5", "ordered_qty": "1", "partial": True}


async def test_a_final_zero_fill_expires_the_cycle(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    cycle = await new_cycle(sessions, state="RISK_CHECK")
    sim.script(BTC, "buy", FillPlan(("cancel",)))
    row, oid = await place(sessions, sim, "buy", "1", cycle_id=cycle)
    sim.process_bar(bar(1))

    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    assert await cycle_state(sessions, cycle) == "EXPIRED"
    assert await positions(sessions) == []
    (order,) = await orders(sessions)
    assert order.settled_at is not None


async def test_a_never_submitted_entry_settles_and_is_rejected_like_the_runner_does(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    cycle = await new_cycle(sessions, state="RISK_CHECK")
    async with sessions() as s:
        row = await repo.insert_order(
            s,
            cycle_id=cycle,
            client_order_id=None,
            symbol=BTC,
            mode="paper",
            type_="limit",
            side="buy",
            qty=D("1"),
            limit_price=D("100"),
            stop_price=None,
            tp_price=None,
            status="not_submitted",
            now=T0,
        )
    update_ = OrderUpdate(order_row_id=row.id, status="not_submitted", filled_qty=D(0))
    assert await ledger.apply(update_) == "applied"
    (order,) = await orders(sessions)
    assert order.settled_at is not None
    assert await cycle_state(sessions, cycle) == "REJECTED"  # as the runner would write it
    async with sessions() as s:
        (cycle_row,) = list(await s.scalars(select(m.Cycle).where(m.Cycle.id == cycle)))
    assert cycle_row.reject_reason == "order not_submitted"


async def test_terminal_cycles_are_never_reopened_but_their_fills_are_booked(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    errored = await new_cycle(sessions, state="ERROR")
    filled = await new_cycle(sessions, state="FILLED")  # the runner saw an immediate fill
    first, first_oid = await place(sessions, sim, "buy", "1", cycle_id=errored)
    sim.process_bar(bar(1))
    assert await ledger.apply(await update(sim, first, first_oid)) == "applied"
    second, second_oid = await place(sessions, sim, "buy", "1", cycle_id=filled)
    sim.process_bar(bar(2))
    assert await ledger.apply(await update(sim, second, second_oid)) == "applied"

    assert await cycle_state(sessions, errored) == "ERROR"
    assert await cycle_state(sessions, filled) == "FILLED"
    assert await event_types(sessions) == ["OrderSettled", "OrderSettled"]
    assert len(await fills(sessions)) == 2


def test_a_sync_is_complete_only_without_failed_orders() -> None:
    # Keeps risk/ at 100 % branch coverage from this task on (the engine uses it in Task 7).
    assert SyncResult().complete
    assert not SyncResult(failed=(1,)).complete


# --- idempotency and status/fill skew ---------------------------------------------------


async def test_a_redelivered_fill_is_booked_once_whatever_its_exponent(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    sim.script(BTC, "buy", FillPlan((D("0.5"),)))
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    sim.process_bar(bar(2))
    again = await update(sim, row, oid)
    respelled = (again.fills[0].model_copy(update={"qty": D("0.50000")}), *again.fills[1:])

    assert await ledger.apply(again.model_copy(update={"fills": respelled})) == "applied"
    assert len(await fills(sessions)) == 2
    (pos,) = await positions(sessions)
    assert pos.qty == D("1")


async def test_the_same_fill_twice_in_one_update_is_booked_once(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    once = await update(sim, row, oid)

    assert (
        await ledger.apply(once.model_copy(update={"fills": once.fills + once.fills})) == "applied"
    )
    assert len(await fills(sessions)) == 1
    (pos,) = await positions(sessions)
    assert pos.qty == D("1")


async def test_quantities_compare_by_value_across_digit_counts(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    # "2" > "10" as strings: Money is text on SQLite, so a SQL-side compare would be wrong.
    sim = make_sim(cash="2000")
    sim.script(BTC, "buy", FillPlan((D("0.2"),)))
    row, oid = await place(sessions, sim, "buy", "10")
    sim.process_bar(bar(1))
    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    sim.process_bar(bar(2))
    assert await ledger.apply(await update(sim, row, oid)) == "applied"

    (order,) = await orders(sessions)
    assert (order.filled_qty, order.settled_at is not None) == (D("10"), True)
    assert not await paused(sessions)


async def test_fills_ahead_of_the_status_apply_without_settling(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    current = await update(sim, row, oid)
    older_status = current.model_copy(update={"status": "partially_filled", "filled_qty": D("0.5")})

    assert await ledger.apply(older_status) == "applied"
    (order,) = await orders(sessions)
    assert (order.filled_qty, order.status, order.settled_at) == (D("1"), "partially_filled", None)
    assert not await paused(sessions)
    assert await ledger.apply(current) == "applied"
    (order,) = await orders(sessions)
    assert (order.status, order.settled_at is not None) == ("filled", True)


async def test_fills_not_yet_visible_settle_on_a_later_pass(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    sim.script(BTC, "buy", FillPlan((D("0.5"),)))
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    sim.process_bar(bar(2))

    assert await ledger.apply(await update(sim, row, oid, visible=1)) == "applied"
    (order,) = await orders(sessions)
    assert (order.filled_qty, order.status, order.settled_at) == (D("0.5"), "filled", None)
    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    (order,) = await orders(sessions)
    assert (order.filled_qty, order.settled_at is not None) == (D("1"), True)


async def test_an_older_update_never_moves_the_status_back(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    sim.script(BTC, "buy", FillPlan((D("0.5"),)))
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    current = await update(sim, row, oid)
    assert await ledger.apply(current) == "applied"

    older = current.model_copy(update={"status": "new", "filled_qty": D(0), "fills": ()})
    assert await ledger.apply(older) == "noop"
    (order,) = await orders(sessions)
    assert (order.status, order.filled_qty) == ("partially_filled", D("0.5"))


async def test_two_racing_applies_book_an_update_once(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    once = await update(sim, row, oid)

    outcomes = await asyncio.gather(ledger.apply(once), ledger.apply(once))
    assert sorted(outcomes) == ["applied", "noop"]
    assert len(await fills(sessions)) == 1


# --- anomalies ---------------------------------------------------------------------------


async def test_a_known_fill_with_other_values_is_an_anomaly(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    sim.script(BTC, "buy", FillPlan((D("0.5"),)))
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    first = await update(sim, row, oid)
    assert await ledger.apply(first) == "applied"

    changed = (first.fills[0].model_copy(update={"price": D("101")}),)
    assert await ledger.apply(first.model_copy(update={"fills": changed})) == "anomaly"
    assert await paused(sessions)
    assert anomaly_kind(await event_rows(sessions)) == "fill_mismatch"
    (order,) = await orders(sessions)
    assert order.filled_qty == D("0.5")


async def test_a_final_status_below_the_applied_qty_is_overfilled(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    lying = (await update(sim, row, oid)).model_copy(
        update={"status": "canceled", "filled_qty": D("0.5")}
    )

    assert await ledger.apply(lying) == "anomaly"
    assert anomaly_kind(await event_rows(sessions)) == "overfilled"
    assert await fills(sessions) == [] and await positions(sessions) == []
    assert await paused(sessions)


async def test_buy_fills_beyond_the_order_qty_are_overfilled(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    once = await update(sim, row, oid)
    extra = once.fills[0].model_copy(update={"fill_id": f"{oid}:99"})

    too_much = once.model_copy(update={"fills": (*once.fills, extra), "filled_qty": D("2")})
    assert await ledger.apply(too_much) == "anomaly"
    assert anomaly_kind(await event_rows(sessions)) == "overfilled"


async def test_a_new_fill_on_a_settled_order_is_an_anomaly_and_a_stale_update_a_noop(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    once = await update(sim, row, oid)
    assert await ledger.apply(once) == "applied"

    assert await ledger.apply(once) == "noop"
    late = once.fills[0].model_copy(update={"fill_id": "late"})
    assert await ledger.apply(once.model_copy(update={"fills": (*once.fills, late)})) == "anomaly"
    assert anomaly_kind(await event_rows(sessions)) == "fill_after_settle"


# --- commit verification (PR #25 review) ---------------------------------------------------


@pytest.fixture
def hook() -> CommitHook:
    return CommitHook()


@pytest.fixture
async def hooked(
    db_url: str, hook: CommitHook
) -> AsyncIterator[tuple[Ledger, list[DomainEvent], EventBus]]:
    """A ledger whose commits go through `hook`, and every event its bus delivers."""
    engine = make_engine(db_url)
    hooked_sessions = hooked_sessionmaker(engine, hook)
    hbus = EventBus(hooked_sessions)
    delivered: list[DomainEvent] = []

    async def collect(event: DomainEvent) -> None:
        delivered.append(event)

    hbus.subscribe(DomainEvent, collect)
    yield Ledger(hbus, hooked_sessions, clock=lambda: T0), delivered, hbus
    await hbus.close()
    await engine.dispose()


async def test_an_older_pause_never_confirms_an_anomaly_whose_commit_failed(
    sessions: async_sessionmaker[AsyncSession],
    hooked: tuple[Ledger, list[DomainEvent], EventBus],
    hook: CommitHook,
) -> None:
    ledger, delivered, hbus = hooked
    sim = make_sim()
    sim.script(BTC, "buy", FillPlan((D("0.5"),)))
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    first = await update(sim, row, oid)
    assert await ledger.apply(first) == "applied"
    async with sessions() as s:
        await repo.set_control(s, PAUSED_KEY, True)  # an earlier incident
    await hbus.drain()
    delivered.clear()
    changed = (first.fills[0].model_copy(update={"price": D("101")}),)
    hook.fail_before = 1  # the anomaly unit's commit fails without committing

    with pytest.raises(CommitUncertain):
        await ledger.apply(first.model_copy(update={"fills": changed}))
    await hbus.drain()
    assert "LedgerAnomaly" not in await event_types(sessions)
    assert delivered == []


async def test_a_lost_ack_on_a_committed_unit_delivers_all_its_events_once(
    sessions: async_sessionmaker[AsyncSession],
    hooked: tuple[Ledger, list[DomainEvent], EventBus],
    hook: CommitHook,
) -> None:
    ledger, delivered, hbus = hooked
    sim = make_sim()
    cycle = await new_cycle(sessions)
    row, oid = await place(sessions, sim, "buy", "1", cycle_id=cycle)
    sim.process_bar(bar(1))
    hook.fail_after = 1  # the unit commits, then the acknowledgement is lost

    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    await hbus.drain()
    assert [type(e).__name__ for e in delivered] == ["OrderSettled", "CycleTransitioned"]
    assert await event_types(sessions) == ["OrderSettled", "CycleTransitioned"]
    assert await cycle_state(sessions, cycle) == "FILLED"


# --- exits -------------------------------------------------------------------------------


async def settle_entry(
    sessions: async_sessionmaker[AsyncSession],
    ledger: Ledger,
    sim: SimBroker,
    n: int,
    qty: str = "1",
) -> int:
    """Buy `qty` at market, fill it on bar `n` and book it settled."""
    row, oid = await place(sessions, sim, "buy", qty)
    sim.process_bar(bar(n))
    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    return row


async def test_a_full_exit_closes_with_exact_pnl_on_quote_fees(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    await settle_entry(sessions, ledger, sim, 1)
    row, oid = await place(sessions, sim, "close", "1", exit_reason="take_profit")
    sim.process_bar(bar(2, 110.0))

    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    (pos,) = await positions(sessions)
    # in: 110 - 0.275 = 109.725; out: 100 + 0.25 = 100.25
    assert (pos.exit_reason, pos.realized_pnl, pos.fees, pos.dust_qty) == (
        "take_profit",
        D("9.475"),
        D("0.525"),
        D("0"),
    )
    assert pos.closed_at == T0 + timedelta(minutes=2)
    assert await dust_balance(sessions) is None


async def test_a_full_exit_closes_with_exact_pnl_on_coin_fees(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim(BASE)
    await settle_entry(sessions, ledger, sim, 1)  # net 0.9975 coins for 100 USD
    row, oid = await place(sessions, sim, "close", "0.9975", exit_reason="time")
    sim.process_bar(bar(2, 110.0))

    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    (pos,) = await positions(sessions)
    # in: 0.9975 * 110 = 109.725 - fee 0.2743125; out: 100 (the coin fee is not cash)
    assert (pos.realized_pnl, pos.fees) == (D("9.4506875"), D("0.5243125"))


async def test_a_tiny_partial_ioc_entry_closes_at_once_as_dust(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim(BASE)
    cycle = await new_cycle(sessions)
    sim.script(BTC, "buy", FillPlan((D("0.00005"),)))  # slice floors to 0.0001
    row, oid = await place(sessions, sim, "buy", "2.997", cycle_id=cycle, limit="100")
    sim.process_bar(bar(1))

    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    (pos,) = await positions(sessions)
    assert (pos.exit_reason, pos.dust_qty, pos.realized_pnl) == (
        "dust",
        D("0.00009975"),
        D("-0.01"),
    )
    assert await dust_balance(sessions) == D("0.00009975")
    assert await event_types(sessions) == ["OrderSettled", "CycleTransitioned", "DustLeft"]
    assert await cycle_state(sessions, cycle) == "FILLED"


async def test_a_partial_exit_keeps_the_position_open(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    await settle_entry(sessions, ledger, sim, 1)
    row, oid = await place(sessions, sim, "sell", "0.4", exit_reason="time")
    sim.process_bar(bar(2))

    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    (pos,) = await positions(sessions)
    assert (pos.qty, pos.closed_at) == (D("0.6"), None)


async def test_an_exit_in_two_fills_reduces_then_closes_with_exact_pnl(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    await settle_entry(sessions, ledger, sim, 1)
    sim.script(BTC, "sell", FillPlan((D("0.4"),)))  # the rest sells on the next bar
    row, oid = await place(sessions, sim, "close", "1", exit_reason="trailing")
    sim.process_bar(bar(2, 110.0))  # sells 0.4

    first = await update(sim, row, oid)
    assert first.status == "partially_filled"
    assert await ledger.apply(first) == "applied"
    (pos,) = await positions(sessions)
    assert (pos.qty, pos.closed_at) == (D("0.6"), None)
    exit_order = (await orders(sessions))[1]
    assert (exit_order.filled_qty, exit_order.settled_at) == (D("0.4"), None)

    sim.process_bar(bar(3, 105.0))  # sells the other 0.6: the exit is filled
    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    (pos,) = await positions(sessions)
    # in: 44 - 0.11 + 63 - 0.1575 = 106.7325; out: 100 + 0.25
    assert (pos.exit_reason, pos.qty, pos.realized_pnl, pos.fees, pos.dust_qty) == (
        "trailing",
        D("0"),
        D("6.4825"),
        D("0.5175"),
        D("0"),
    )
    assert pos.closed_at == T0 + timedelta(minutes=3)
    assert len(await fills(sessions)) == 3
    assert not await paused(sessions)


async def test_the_position_closes_only_after_its_entry_settles(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    sim.script(BTC, "buy", FillPlan((D("0.5"), "cancel")))
    entry, entry_oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))  # 0.5 filled, the rest still open
    assert await ledger.apply(await update(sim, entry, entry_oid)) == "applied"
    exit_row, exit_oid = await place(sessions, sim, "close", "0.5", exit_reason="stop")
    sim.process_bar(bar(2, 95.0))  # the entry is canceled, the exit sells 0.5

    assert await ledger.apply(await update(sim, exit_row, exit_oid)) == "applied"
    (pos,) = await positions(sessions)
    assert (pos.qty, pos.closed_at) == (D("0"), None)  # entry not settled yet: still open
    assert await ledger.apply(await update(sim, entry, entry_oid)) == "applied"
    (pos,) = await positions(sessions)
    # in: 47.5 - 0.11875; out: 50 + 0.125
    assert (pos.exit_reason, pos.realized_pnl) == ("stop", D("-2.74375"))


async def test_a_late_entry_fill_after_a_protective_exit_lands_on_the_open_position(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    sim.script(BTC, "buy", FillPlan((D("0.5"),)))  # the rest fills on the next bar
    entry, entry_oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    assert await ledger.apply(await update(sim, entry, entry_oid)) == "applied"
    exit_row, exit_oid = await place(sessions, sim, "close", "0.5", exit_reason="stop")
    sim.process_bar(bar(2))  # the entry fills its other 0.5, the exit sells 0.5

    assert await ledger.apply(await update(sim, exit_row, exit_oid)) == "applied"
    assert await ledger.apply(await update(sim, entry, entry_oid)) == "applied"
    (pos,) = await positions(sessions)
    assert (pos.qty, pos.closed_at) == (D("0.5"), None)
    (held,) = await sim.get_positions()
    assert held.qty == D("0.5")


async def test_a_lagging_exit_status_keeps_the_position_open_until_the_exit_settles(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    sim.script(BTC, "buy", FillPlan((D("0.5"), "cancel")))
    entry, entry_oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    assert await ledger.apply(await update(sim, entry, entry_oid, visible=1)) == "applied"
    exit_row, exit_oid = await place(sessions, sim, "close", "0.5", exit_reason="stop")
    sim.process_bar(bar(2))
    exit_now = await update(sim, exit_row, exit_oid)

    assert await ledger.apply(exit_now.model_copy(update={"status": "new"})) == "applied"
    assert await ledger.apply(await update(sim, entry, entry_oid)) == "applied"  # entry settles
    (pos,) = await positions(sessions)
    assert pos.closed_at is None  # not closed as "dust" while the exit is unsettled
    assert await ledger.apply(exit_now) == "applied"
    (pos,) = await positions(sessions)
    assert pos.exit_reason == "stop"


async def test_an_exit_filling_more_than_its_recorded_qty_is_not_overfilled(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    await settle_entry(sessions, ledger, sim, 1)
    row, oid = await place(sessions, sim, "close", "0.5", exit_reason="kill")  # sells all 1
    sim.process_bar(bar(2))

    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    (pos,) = await positions(sessions)
    assert (pos.exit_reason, pos.qty) == ("kill", D("0"))
    assert not await paused(sessions)


# --- ordering and readiness ---------------------------------------------------------------


async def test_catch_up_defers_an_exit_whose_entry_lookup_failed(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    entry, _ = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    exit_row, exit_oid = await place(sessions, sim, "close", "1", exit_reason="stop")
    sim.process_bar(bar(2))

    exit_only = SyncResult(updates=(await update(sim, exit_row, exit_oid),), failed=(entry,))
    await ledger.apply_all(exit_only)
    assert await fills(sessions) == [] and not await paused(sessions)
    await ledger.apply_all(await sync(sessions, sim))
    (pos,) = await positions(sessions)
    assert pos.exit_reason == "stop"
    assert all(o.settled_at is not None for o in await orders(sessions))


async def test_an_oversell_waits_for_incomplete_entry_history_and_keeps_old_dust(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    async with sessions() as s:
        s.add(m.SymbolDust(symbol=BTC, qty=D("0.00005"), updated_at=T0))
        await s.commit()
    sim.script(BTC, "buy", FillPlan((D("0.5"),)))
    entry, entry_oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    sim.process_bar(bar(2))  # final, filled 1 in two executions
    exit_row, exit_oid = await place(sessions, sim, "close", "1", exit_reason="stop")
    sim.process_bar(bar(3))

    first = SyncResult(
        updates=(
            await update(sim, entry, entry_oid, visible=1),  # status final 1, only 0.5 shown
            await update(sim, exit_row, exit_oid),
        )
    )
    await ledger.apply_all(first)
    exit_order = (await orders(sessions))[1]
    assert (exit_order.filled_qty, exit_order.settled_at) == (D(0), None)
    assert not await paused(sessions)
    assert await dust_balance(sessions) == D("0.00005")

    await ledger.apply_all(await sync(sessions, sim))
    (pos,) = await positions(sessions)
    assert (pos.exit_reason, pos.realized_pnl, pos.dust_consumed) == ("stop", D("-0.5"), D(0))
    assert await dust_balance(sessions) == D("0.00005")


async def test_carried_dust_is_consumed_by_the_next_trade(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim(BASE)
    sim.script(BTC, "buy", FillPlan((D("0.00005"),)))
    first, first_oid = await place(sessions, sim, "buy", "2.997", limit="100")
    sim.process_bar(bar(1))
    assert await ledger.apply(await update(sim, first, first_oid)) == "applied"  # dust
    await settle_entry(sessions, ledger, sim, 2)  # net 0.9975
    (held,) = await sim.get_positions()
    assert held.qty == D("0.99759975")  # the broker merged the dust in
    row, oid = await place(sessions, sim, "close", str(held.qty), exit_reason="time")
    sim.process_bar(bar(3))

    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    _, second = await positions(sessions)
    # in: 99.759975 - 0.2493999375; out: 100
    assert (second.dust_consumed, second.dust_qty, second.realized_pnl) == (
        D("0.00009975"),
        D("0"),
        D("-0.4894249375"),
    )
    assert await dust_balance(sessions) == D("0")
    assert await sim.get_positions() == []


async def test_a_sell_beyond_open_qty_and_dust_is_an_oversell(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    await settle_entry(sessions, ledger, sim, 1)
    row, oid = await place(sessions, sim, "close", "1", exit_reason="stop")
    sim.process_bar(bar(2))
    real = await update(sim, row, oid)
    bigger = (real.fills[0].model_copy(update={"qty": D("1.5")}),)

    lying = real.model_copy(update={"fills": bigger, "filled_qty": D("1.5")})
    assert await ledger.apply(lying) == "anomaly"
    assert anomaly_kind(await event_rows(sessions)) == "oversell"
    (pos,) = await positions(sessions)
    assert (pos.qty, pos.closed_at) == (D("1"), None)


def _sell_fill(fill_id: str) -> Fill:
    return Fill(
        fill_id=fill_id,
        order_id="x",
        symbol=BTC,
        side="sell",
        qty=D("1"),
        price=D("100"),
        fee=D("0.25"),
        fee_asset="quote",
        ts=T0,
    )


async def _sell_row(sessions: async_sessionmaker[AsyncSession], status: str = "new") -> int:
    async with sessions() as s:
        row = await repo.insert_order(
            s,
            cycle_id=None,
            client_order_id=None,
            symbol=BTC,
            mode="paper",
            type_="market",
            side="sell",
            qty=D("1"),
            limit_price=None,
            stop_price=None,
            tp_price=None,
            status=status,
            now=T0,
            exit_reason="flatten",
        )
        return row.id


async def test_a_sell_without_any_position_is_an_anomaly(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    row = await _sell_row(sessions)
    lone = OrderUpdate(
        order_row_id=row,
        status="filled",
        filled_qty=D("1"),
        fills=(_sell_fill("x:1"),),
        min_qty=RULES.min_qty,
    )
    assert await ledger.apply(lone) == "anomaly"
    assert anomaly_kind(await event_rows(sessions)) == "sell_without_position"


async def test_a_sell_without_a_position_waits_for_an_earlier_unsettled_order(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    async with sessions() as s:  # an entry whose submit outcome is unknown (no broker id)
        await repo.insert_order(
            s,
            cycle_id=None,
            client_order_id="c-1",
            symbol=BTC,
            mode="paper",
            type_="limit",
            side="buy",
            qty=D("1"),
            limit_price=D("100"),
            stop_price=None,
            tp_price=None,
            status="unknown",
            now=T0,
        )
    row = await _sell_row(sessions)
    lone = OrderUpdate(
        order_row_id=row,
        status="filled",
        filled_qty=D("1"),
        fills=(_sell_fill("x:1"),),
        min_qty=RULES.min_qty,
    )
    assert await ledger.apply(lone) == "deferred"
    assert not await paused(sessions)


async def test_a_sell_with_two_open_positions_is_ambiguous_and_books_nothing(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    async with sessions() as s:  # check() never admits this; the ledger must not guess
        for qty in ("0.00005", "1"):
            s.add(
                m.Position(
                    symbol=BTC,
                    side="long",
                    qty=D(qty),
                    entry_price=D("100"),
                    opened_at=T0,
                    fees=D(0),
                    dust_consumed=D(0),
                )
            )
        await s.commit()
    row = await _sell_row(sessions)
    small = _sell_fill("x:1").model_copy(update={"qty": D("0.00001")})
    update_ = OrderUpdate(
        order_row_id=row,
        status="partially_filled",
        filled_qty=D("0.00001"),
        fills=(small,),
        min_qty=RULES.min_qty,
    )

    assert await ledger.apply(update_) == "anomaly"
    assert anomaly_kind(await event_rows(sessions)) == "ambiguous_position"
    assert [p.qty for p in await positions(sessions)] == [D("0.00005"), D("1")]
    (order,) = await orders(sessions)
    assert (order.position_id, order.filled_qty) == (None, D(0))


# --- cash check (D17, spec 4.6) -----------------------------------------------------------


async def baseline_of(sessions: async_sessionmaker[AsyncSession]) -> dict[str, object]:
    async with sessions() as s:
        value = await repo.get_control(s, CASH_CHECK_KEY)
    assert isinstance(value, dict)
    return value


async def check(ledger: Ledger, account: Account) -> str:
    return await ledger.check_cash(account, await ledger.cash_guard())


async def test_the_first_check_only_records_a_baseline(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    assert await check(ledger, await sim.get_account()) == "baseline"
    baseline = await baseline_of(sessions)
    assert (baseline["cash"], baseline["fill_watermark"]) == ("1000", 0)
    assert await event_types(sessions) == ["CashChecked"]


async def test_cash_explained_by_booked_fills_is_ok(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    assert await check(ledger, await sim.get_account()) == "baseline"
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    assert await ledger.apply(await update(sim, row, oid)) == "applied"

    assert await check(ledger, await sim.get_account()) == "ok"
    assert not await paused(sessions)
    assert (await baseline_of(sessions))["cash"] == "899.75"


async def test_an_unexplained_change_pauses_once_and_rebaselines(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    assert await check(ledger, await sim.get_account()) == "baseline"
    sim.cash -= D("19.84")  # the unexplained paper drop of 2026-10-14

    assert await check(ledger, await sim.get_account()) == "discrepancy"
    assert await paused(sessions)
    (event,) = [e for e in await event_rows(sessions) if e.type == "CashDiscrepancy"]
    assert (event.payload["diff"], event.payload["n_fills"]) == ("-19.84", 0)
    assert await check(ledger, await sim.get_account()) == "ok"  # measured from the new cash


async def test_the_check_skips_while_an_order_is_unsettled(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    assert await check(ledger, await sim.get_account()) == "baseline"
    before = await baseline_of(sessions)
    await place(sessions, sim, "buy", "1")  # unsettled until booked

    assert await check(ledger, await sim.get_account()) == "skipped"
    assert await baseline_of(sessions) == before


async def test_an_order_settled_between_guard_and_check_skips_the_check(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    assert await check(ledger, await sim.get_account()) == "baseline"
    guard = await ledger.cash_guard()
    account = await sim.get_account()  # cash 1000
    row, oid = await place(sessions, sim, "buy", "1")  # submitted, filled and settled
    sim.process_bar(bar(1))
    assert await ledger.apply(await update(sim, row, oid)) == "applied"

    assert await ledger.check_cash(account, guard) == "skipped"  # not a false incident
    assert not await paused(sessions)
    assert await check(ledger, await sim.get_account()) == "ok"


async def test_the_tolerance_is_a_cent_per_fill(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    assert await check(ledger, await sim.get_account()) == "baseline"
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    assert await ledger.apply(await update(sim, row, oid)) == "applied"  # expected 899.75

    truncated = Account(equity=D("999.74"), cash=D("899.74"))  # one fill: 0.01 allowed
    assert await check(ledger, truncated) == "ok"
    assert await check(ledger, Account(equity=D("999.72"), cash=D("899.72"))) == "discrepancy"


async def test_a_baseline_at_the_same_timestamp_never_confirms_a_failed_cash_check(
    sessions: async_sessionmaker[AsyncSession],
    hooked: tuple[Ledger, list[DomainEvent], EventBus],
    hook: CommitHook,
) -> None:
    ledger, delivered, hbus = hooked
    sim = make_sim()
    guard = await ledger.cash_guard()
    assert await ledger.check_cash(await sim.get_account(), guard) == "baseline"  # ts T0
    hook.fail_before = 1

    with pytest.raises(CommitUncertain):
        await ledger.check_cash(await sim.get_account(), await ledger.cash_guard())
    await hbus.drain()
    assert await event_types(sessions) == ["CashChecked"]  # only the first one
    assert [type(e).__name__ for e in delivered] == ["CashChecked"]


_trades = st.lists(
    st.tuples(
        st.integers(min_value=2, max_value=40),  # qty in 0.01 BTC
        st.sampled_from(["full", "half", "sliver"]),
        st.floats(min_value=90, max_value=110, allow_nan=False),
        st.floats(min_value=90, max_value=110, allow_nan=False),
    ),
    min_size=1,
    max_size=6,
)


async def _replay(url: str, trades: list[tuple[int, str, float, float]], fee_asset: str) -> None:
    engine = make_engine(url)
    sessions = make_sessionmaker(engine)
    bus = EventBus(sessions)
    ledger = Ledger(bus, sessions, clock=lambda: T0)
    sim = make_sim(QUOTE.model_copy(update={"buy_fee_asset": fee_asset}))
    i = 0
    try:
        for n, plan, price_in, price_out in trades:
            qty = D(n) / 100
            if plan == "half":
                sim.script(BTC, "buy", FillPlan((D("0.5"), "cancel")))
            elif plan == "sliver":
                sim.script(BTC, "buy", FillPlan((D("0.0001") / qty, "cancel")))
            await place(sessions, sim, "buy", str(qty))
            for _ in range(2):  # the fill bar, then the scripted cancel (if any)
                i += 1
                sim.process_bar(bar(i, price_in))
            await ledger.apply_all(await sync(sessions, sim))
            held = await sim.get_positions()
            if not held or held[0].qty < RULES.min_qty:
                continue  # all dust: nothing to close
            try:
                await place(sessions, sim, "close", str(held[0].qty), exit_reason="time")
            except OrderRejected:
                break  # below min notional: open but not closable; one trade per symbol
            i += 1
            sim.process_bar(bar(i, price_out))
            await ledger.apply_all(await sync(sessions, sim))
        async with sessions() as s:
            booked = list(await s.scalars(select(m.Fill)))
            open_qty = sum(
                (
                    p.qty
                    for p in await s.scalars(
                        select(m.Position).where(m.Position.closed_at.is_(None))
                    )
                ),
                D(0),
            )
            dust = await s.get(m.SymbolDust, BTC)
            assert await repo.get_control(s, PAUSED_KEY) is None
        assert abs(D("1000") + sum((_cash_effect(f) for f in booked), D(0)) - sim.cash) < D("1e-18")
        held_qty = sum((p.qty for p in await sim.get_positions()), D(0))
        assert open_qty + (D(0) if dust is None else dust.qty) == held_qty
    finally:
        await bus.close()
        await engine.dispose()


@given(_trades, st.sampled_from(["base", "quote"]))
@settings(max_examples=25, deadline=None)
def test_the_ledger_reproduces_broker_cash_and_holdings(
    trades: list[tuple[int, str, float, float]], fee_asset: str
) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        url = f"sqlite+aiosqlite:///{tmp}/ledger.db"
        upgrade(url)  # sync: Alembic runs its own event loop
        asyncio.run(_replay(url, trades, fee_asset))
