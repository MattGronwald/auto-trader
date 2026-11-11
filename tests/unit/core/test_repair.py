"""Journal repair operations (`core/repair.py`, WP 1.5d-2, spec 2026-11-10 5.1-5.4, 8).

Reuses the order resolver rig (`tests/unit/risk/test_resolver.py`): SimBroker, a migrated
SQLite journal, the engine `Clock`/`Mono`, and `ledger.apply_all(await engine.sync_orders())`
as one sync pass. `link`'s broker read is `read_order_by_client_id` bound to the SimBroker,
as the CLI binds it to its broker.
"""

from collections.abc import AsyncIterator
from decimal import Decimal as D
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog.testing import capture_logs

import tests.unit.risk.test_engine as te
import tests.unit.risk.test_resolver as tr
from autotrader.broker.base import BrokerOrder, BrokerOrderView
from autotrader.broker.sim import SimBroker
from autotrader.core.bus import CommitUncertain, EventBus
from autotrader.core.kill import PAUSED_KEY, ControlGate
from autotrader.core.repair import (
    Lookup,
    RepairPlan,
    RepairRefused,
    commit,
    prepare_link,
    prepare_not_submitted,
    prepare_oversell,
)
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.journal.db import make_engine
from autotrader.risk.engine import read_order_by_client_id
from autotrader.risk.resolve import entry_client_id
from autotrader.risk.types import ExitRequest
from tests.dbhooks import CommitHook, hooked_sessionmaker

BTC, ETH, NOW, SEC = tr.BTC, tr.ETH, tr.NOW, tr.SEC
Rig = tr.Rig


@pytest.fixture
def db_url(tmp_path: Path) -> str:
    return tr.migrated(tmp_path / "a.db")


@pytest.fixture
async def sessions(db_url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    async for s in tr._journal(db_url):
        yield s


@pytest.fixture
def gate(tmp_path: Path, sessions: async_sessionmaker[AsyncSession]) -> ControlGate:
    return ControlGate(tmp_path / "KILL", sessions)


@pytest.fixture
def r(sessions: async_sessionmaker[AsyncSession], gate: ControlGate) -> Rig:
    return tr.rig(tr.make_sim(), sessions, gate)


def lookup_of(sim: SimBroker) -> Lookup:
    async def lookup(cid: str) -> BrokerOrderView | None:
        return await read_order_by_client_id(sim, cid, timeout_s=5)

    return lookup


async def do(r: Rig, plan: RepairPlan, evidence: str = "checked the broker UI at 10:00") -> None:
    await commit(EventBus(r.sessions), plan, evidence=evidence, operator="op", now=r.clock.now)


async def repairs(sessions: async_sessionmaker[AsyncSession]) -> list[dict[str, Any]]:
    return [e.payload for e in await tr.events(sessions, "OperatorRepair")]


async def settle(sessions: async_sessionmaker[AsyncSession], row_id: int) -> None:
    async with sessions() as s:
        await s.execute(update(m.Order).where(m.Order.id == row_id).values(settled_at=NOW))
        await s.commit()


async def own_cid(sessions: async_sessionmaker[AsyncSession], n: int) -> str:
    return entry_client_id(await tr.jid_of(sessions), n)


async def submit(sim: SimBroker, cid: str, qty: D = D("0.01")) -> str:
    ack = await sim.submit_order(
        BrokerOrder(symbol=BTC, side="buy", qty=qty, type="market", client_order_id=cid)
    )
    return ack.order_id


# --- common rules ---------------------------------------------------------------------


async def test_every_command_refuses_a_settled_row(r: Rig) -> None:
    cid = await own_cid(r.sessions, 1)
    oid = await submit(r.sim, cid)
    row = await tr.insert_row(r.sessions, cid=cid)
    await settle(r.sessions, row)
    for prepare in (
        prepare_link(r.sessions, row, oid, lookup_of(r.sim)),
        prepare_not_submitted(r.sessions, row),
        prepare_oversell(r.sessions, row),
    ):
        with pytest.raises(RepairRefused, match="already settled"):
            await prepare
    assert await repairs(r.sessions) == []


async def test_every_command_refuses_a_missing_row(r: Rig) -> None:
    with pytest.raises(RepairRefused, match="no order row 99"):
        await prepare_not_submitted(r.sessions, 99)


async def test_commit_refuses_empty_evidence(r: Rig) -> None:
    row = await tr.insert_row(r.sessions, cid=None)
    plan = await prepare_not_submitted(r.sessions, row)
    with pytest.raises(RepairRefused, match="evidence"):
        await do(r, plan, evidence="  ")
    assert (await tr.order(r.sessions, row)).status == "pending"


async def test_commit_refuses_a_row_changed_since_prepare(r: Rig) -> None:
    row = await tr.insert_row(r.sessions, cid=None)
    plan = await prepare_not_submitted(r.sessions, row)
    async with r.sessions() as s:  # e.g. a core run raised an incident in between
        await s.execute(
            update(m.Order).where(m.Order.id == row).values(incident="not_found", incident_at=NOW)
        )
        await s.commit()
    with capture_logs() as logs, pytest.raises(RepairRefused, match="row changed since"):
        await do(r, plan)
    assert "bus.unit_failed" not in [e["event"] for e in logs]
    after = await tr.order(r.sessions, row)
    assert (after.status, after.incident) == ("pending", "not_found")
    assert await repairs(r.sessions) == []


async def test_commit_refuses_a_row_settled_since_prepare(r: Rig) -> None:
    row = await tr.insert_row(r.sessions, cid=None)
    plan = await prepare_not_submitted(r.sessions, row)
    await settle(r.sessions, row)
    with pytest.raises(RepairRefused, match="row changed since"):
        await do(r, plan)
    assert await repairs(r.sessions) == []


async def test_a_failed_commit_writes_neither_the_row_nor_the_event(db_url: str, r: Rig) -> None:
    row = await tr.insert_row(r.sessions, cid=None)
    plan = await prepare_not_submitted(r.sessions, row)
    hook = CommitHook()
    engine = make_engine(db_url)
    hook.fail_before = 1
    try:
        with pytest.raises(CommitUncertain):
            await commit(
                EventBus(hooked_sessionmaker(engine, hook)),
                plan,
                evidence="e",
                operator="op",
                now=NOW,
            )
    finally:
        await engine.dispose()
    assert (await tr.order(r.sessions, row)).status == "pending"
    assert await repairs(r.sessions) == []


# --- link (5.2) -------------------------------------------------------------------------


async def test_link_writes_the_id_and_one_operator_repair(r: Rig) -> None:
    cid = await own_cid(r.sessions, 1)
    oid = await submit(r.sim, cid)
    row = await tr.insert_row(r.sessions, cid=cid, status="unknown")
    async with r.sessions() as s:  # a type (a) incident: still linkable
        await s.execute(
            update(m.Order).where(m.Order.id == row).values(incident="not_found", incident_at=NOW)
        )
        await s.commit()
    plan = await prepare_link(r.sessions, row, oid, lookup_of(r.sim))
    r.clock.now = NOW + 100 * SEC
    await do(r, plan)
    linked = await tr.order(r.sessions, row)
    assert (linked.broker_order_id, linked.incident, linked.incident_at) == (oid, None, None)
    assert linked.updated_at == NOW + 100 * SEC
    (event,) = await repairs(r.sessions)
    assert event["action"] == "link" and event["order_row_id"] == row
    assert event["before"] == {
        "broker_order_id": None,
        "incident": "not_found",
        "incident_at": NOW.isoformat(),
        "resolve_since": None,
    }
    assert event["after"] == {
        "broker_order_id": oid,
        "incident": None,
        "incident_at": None,
        "resolve_since": None,
    }
    assert (event["evidence"], event["operator"]) == ("checked the broker UI at 10:00", "op")


@pytest.mark.parametrize("cid", [None, "at-o5", "at-c7-e", "at-deadbeef-c1-e"])
async def test_link_refuses_rows_without_ownership_proof(r: Rig, cid: str | None) -> None:
    oid = await submit(r.sim, cid or "at-anything")
    row = await tr.insert_row(r.sessions, cid=cid)
    with pytest.raises(RepairRefused, match="no ownership proof"):
        await prepare_link(r.sessions, row, oid, lookup_of(r.sim))


async def test_link_refuses_without_journal_id(r: Rig) -> None:
    cid = await own_cid(r.sessions, 1)
    oid = await submit(r.sim, cid)
    row = await tr.insert_row(r.sessions, cid=cid)
    async with r.sessions() as s:
        await s.execute(delete(m.ControlState).where(m.ControlState.key == repo.JOURNAL_ID_KEY))
        await s.commit()
    with pytest.raises(RepairRefused, match="no ownership proof"):
        await prepare_link(r.sessions, row, oid, lookup_of(r.sim))


@pytest.fixture
def other_url(tmp_path: Path) -> str:
    return tr.migrated(tmp_path / "b.db")


async def test_link_never_takes_another_journals_order(
    tmp_path: Path, other_url: str, r: Rig
) -> None:
    """Spec 8 test 9a: journal B's lost order looks like journal A's (same symbol, side, qty,
    cycle id), but B's row is found only by B's own client id, so linking it to A's order
    is refused."""
    async for other in tr._journal(other_url):
        b = tr.rig(r.sim, other, ControlGate(tmp_path / "KILL-B", other))
        mine = await r.entry()
        assert mine.broker_order_id is not None
        r.sim.fail_submit_after_accept()
        lost = await b.entry()
        assert lost.order_row_id is not None and lost.status == "unknown"
        with pytest.raises(RepairRefused, match=f"order_id .* != {mine.broker_order_id}"):
            await prepare_link(other, lost.order_row_id, mine.broker_order_id, lookup_of(r.sim))
        assert await repairs(other) == []


async def test_link_refuses_a_lookup_miss(r: Rig) -> None:
    row = await tr.insert_row(r.sessions, cid=await own_cid(r.sessions, 1))
    with pytest.raises(RepairRefused, match="no order with client id"):
        await prepare_link(r.sessions, row, "sim-1", lookup_of(r.sim))


async def test_link_refuses_a_failing_lookup(r: Rig) -> None:
    row = await tr.insert_row(r.sessions, cid=await own_cid(r.sessions, 1))

    async def broken(cid: str) -> BrokerOrderView | None:
        raise TimeoutError

    with pytest.raises(RepairRefused, match=r"broker lookup failed: "):
        await prepare_link(r.sessions, row, "sim-1", broken)


async def test_link_refuses_a_different_order_id(r: Rig) -> None:
    cid = await own_cid(r.sessions, 1)
    oid = await submit(r.sim, cid)
    row = await tr.insert_row(r.sessions, cid=cid)
    with pytest.raises(RepairRefused, match=f"order_id {oid} != sim-999"):
        await prepare_link(r.sessions, row, "sim-999", lookup_of(r.sim))


async def test_link_refuses_a_qty_mismatch_listing_every_difference(r: Rig) -> None:
    cid = await own_cid(r.sessions, 1)
    oid = await submit(r.sim, cid)
    row = await tr.insert_row(r.sessions, cid=cid, qty=D("0.02"))
    with pytest.raises(RepairRefused) as refused:
        await prepare_link(r.sessions, row, "sim-999", lookup_of(r.sim))
    assert "qty 0.01 != 0.02" in str(refused.value)
    assert f"order_id {oid} != sim-999" in str(refused.value)
    assert await repairs(r.sessions) == []


async def test_link_refuses_a_view_with_another_client_id(r: Rig) -> None:
    cid = await own_cid(r.sessions, 1)
    oid = await submit(r.sim, cid)
    row = await tr.insert_row(r.sessions, cid=cid)
    real = lookup_of(r.sim)

    async def lying(c: str) -> BrokerOrderView | None:
        view = await real(c)
        assert view is not None
        return view.model_copy(update={"client_order_id": "at-other-c1-e"})

    with pytest.raises(RepairRefused, match="client_order_id at-other-c1-e != "):
        await prepare_link(r.sessions, row, oid, lying)


@pytest.mark.parametrize(
    ("status", "broker_order_id", "incident"),
    [
        ("new", "sim-1", None),  # linked, no incident
        ("not_submitted", None, None),  # no broker id but not pending/unknown
        ("unknown", "sim-1", "no_ownership_proof"),  # not a type (b) incident
    ],
)
async def test_link_refuses_wrong_state_rows(
    r: Rig, status: str, broker_order_id: str | None, incident: str | None
) -> None:
    cid = await own_cid(r.sessions, 1)
    oid = await submit(r.sim, cid)
    row = await tr.insert_row(r.sessions, cid=cid, status=status, broker_order_id=broker_order_id)
    async with r.sessions() as s:
        await s.execute(update(m.Order).where(m.Order.id == row).values(incident=incident))
        await s.commit()
    with pytest.raises(RepairRefused, match="not linkable"):
        await prepare_link(r.sessions, row, oid, lookup_of(r.sim))


async def test_link_replaces_a_type_b_incidents_id_with_the_rekeyed_one(r: Rig) -> None:
    """Spec 8 test 10, last clause."""
    row, oid = await tr.acked_entry(r)
    cid = (await tr.order(r.sessions, row)).client_order_id
    assert cid is not None
    new_id = r.sim.rekey(oid)
    visible = {"now": False}
    r.sim.hide(cid, lambda: visible["now"])
    await r.sync_at(0)
    await r.sync_at(80)
    marked = await tr.order(r.sessions, row)
    assert (marked.incident, marked.broker_order_id) == ("not_found", oid)
    visible["now"] = True
    plan = await prepare_link(r.sessions, row, new_id, lookup_of(r.sim))
    await do(r, plan)
    linked = await tr.order(r.sessions, row)
    assert (linked.broker_order_id, linked.incident, linked.resolve_since) == (new_id, None, None)
    (event,) = await repairs(r.sessions)
    assert event["before"]["broker_order_id"] == oid
    r.sim.process_bar(tr.bar(31, BTC))
    await r.sync_at(82)
    booked = await tr.order(r.sessions, row)
    assert booked.settled_at is not None and booked.filled_qty == booked.qty


class FlakyLookup(SimBroker):
    """Client-id lookups raise while `down` is set."""

    down = False

    async def get_order_by_client_id(self, client_order_id: str) -> BrokerOrderView | None:
        if self.down:
            raise ConnectionError("lookup down")
        return await super().get_order_by_client_id(client_order_id)


async def test_type_b_lookup_failed_incident_is_repairable(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    """PR #34 finding 3, end to end (spec 8 test 10)."""
    r = tr.rig(tr.make_sim(FlakyLookup), sessions, gate)
    sim = r.sim
    assert isinstance(sim, FlakyLookup)
    row, oid = await tr.acked_entry(r)
    sim.forget(oid)
    sim.down = True
    for t in (0, 40, 79):
        await r.sync_at(t)
    assert (await tr.order(sessions, row)).incident is None
    await r.sync_at(80)
    marked = await tr.order(sessions, row)
    assert (marked.incident, marked.broker_order_id) == ("lookup_failed", oid)
    sim.unforget(oid)
    sim.down = False
    plan = await prepare_link(sessions, row, oid, lookup_of(sim))
    await do(r, plan)
    (event,) = await repairs(sessions)
    assert event["action"] == "link"
    assert event["before"]["incident"] == "lookup_failed"
    sim.process_bar(tr.bar(31, BTC))
    r.clock.now = NOW + 82 * SEC
    await r.ledger.apply_all(await r.engine.sync_orders())
    booked = await tr.order(sessions, row)
    assert booked.settled_at is not None and booked.filled_qty == booked.qty
    assert booked.incident is None


# --- not_submitted (5.3) ------------------------------------------------------------------


async def test_not_submitted_then_sync_settles_with_zero_fill(r: Rig) -> None:
    """Spec 8 test 12."""
    r.sim.hide(await tr.next_entry_cid(r.sessions), lambda: False)
    r.sim.fail_submit_after_accept()
    await r.entry()
    (row,) = await te.orders(r.sessions)
    await r.sync_at(80)
    assert (await tr.order(r.sessions, row.id)).incident == "not_found"
    blocked = await r.entry(BTC)
    assert not blocked.decision.approved
    plan = await prepare_not_submitted(r.sessions, row.id)
    await do(r, plan)
    (event,) = await repairs(r.sessions)
    assert event["action"] == "not_submitted"
    assert event["before"] == {
        "status": "unknown",
        "incident": "not_found",
        "incident_at": (NOW + 80 * SEC).isoformat(),
    }
    assert event["after"] == {"status": "not_submitted", "incident": None, "incident_at": None}
    r.clock.now = NOW + 82 * SEC
    await r.ledger.apply_all(await r.engine.sync_orders())
    settled = await tr.order(r.sessions, row.id)
    assert settled.settled_at is not None and settled.filled_qty == 0
    assert settled.status == "not_submitted"
    again = await r.entry(BTC)
    assert again.decision.approved, again.decision.reasons


@pytest.mark.parametrize(
    ("status", "broker_order_id"), [("unknown", "sim-1"), ("new", None), ("rejected", None)]
)
async def test_not_submitted_refuses_wrong_state_rows(
    r: Rig, status: str, broker_order_id: str | None
) -> None:
    row = await tr.insert_row(r.sessions, cid=None, status=status, broker_order_id=broker_order_id)
    with pytest.raises(RepairRefused, match="needs no broker id"):
        await prepare_not_submitted(r.sessions, row)


# --- oversell (5.4) ---------------------------------------------------------------------


async def paused(sessions: async_sessionmaker[AsyncSession]) -> bool:
    async with sessions() as s:
        return bool(await repo.get_control(s, PAUSED_KEY))


async def oversold(r: Rig) -> int:
    """A journal position of one entry; the broker holds twice that (a buy outside the
    journal), so the exit sells more than the position: the ledger reports `oversell`."""
    row, _ = await tr.acked_entry(r)
    r.sim.process_bar(tr.bar(31, BTC))
    await r.sync_at(2)
    entry = await tr.order(r.sessions, row)
    assert entry.settled_at is not None
    await r.sim.submit_order(
        BrokerOrder(symbol=BTC, side="buy", qty=entry.qty, type="market", client_order_id="ext-1")
    )
    r.sim.process_bar(tr.bar(32, BTC))
    sold = await r.engine.exit(ExitRequest(symbol=BTC, reason="stop"))
    assert sold.order_row_id is not None and sold.status == "new"
    r.sim.process_bar(tr.bar(33, BTC))
    await r.sync_at(4)
    async with r.sessions() as s:
        kinds = await repo.last_anomaly_kinds(s, [sold.order_row_id])
    assert kinds == {sold.order_row_id: "oversell"}
    assert await paused(r.sessions)
    return sold.order_row_id


async def anomalies(sessions: async_sessionmaker[AsyncSession]) -> int:
    return len(await tr.events(sessions, "LedgerAnomaly"))


async def test_oversell_repair_then_sync_closes_the_position(r: Rig) -> None:
    """Spec 8 test 13, end to end."""
    row = await oversold(r)
    before = await anomalies(r.sessions)
    plan = await prepare_oversell(r.sessions, row)
    await do(r, plan)
    (event,) = await repairs(r.sessions)
    assert (event["action"], event["before"], event["after"]) == (
        "oversell",
        {"excess_external": False},
        {"excess_external": True},
    )
    await r.sync_at(6)
    sell = await tr.order(r.sessions, row)
    assert sell.settled_at is not None and sell.excess_external
    (sold,) = await tr.events(r.sessions, "ExternalHoldingSold")
    assert sold.payload["order_row_id"] == row
    assert await anomalies(r.sessions) == before
    assert await paused(r.sessions)
    async with r.sessions() as s:
        assert await repo.open_position_symbols(s) == set()


async def test_oversell_refuses_wrong_state_rows(r: Rig) -> None:
    buy = await tr.insert_row(r.sessions, cid=None, symbol=ETH)
    with pytest.raises(RepairRefused, match="not a sell"):
        await prepare_oversell(r.sessions, buy)
    row = await oversold(r)
    async with r.sessions() as s:  # a later anomaly of another kind is the latest
        await repo.append_event(
            s,
            "LedgerAnomaly",
            {"kind": "fill_mismatch", "order_row_id": row, "detail": "x"},
            cycle_id=None,
            ts=NOW,
        )
    with pytest.raises(RepairRefused, match="latest ledger anomaly is fill_mismatch"):
        await prepare_oversell(r.sessions, row)


async def test_oversell_refuses_a_sell_without_anomaly_or_already_marked(r: Rig) -> None:
    async with r.sessions() as s:
        sell = await repo.insert_order(
            s,
            cycle_id=None,
            client_order_id=None,
            symbol=BTC,
            mode="paper",
            type_="market",
            side="sell",
            qty=D("0.01"),
            limit_price=None,
            stop_price=None,
            tp_price=None,
            status="new",
            now=NOW,
        )
    with pytest.raises(RepairRefused, match="latest ledger anomaly is None"):
        await prepare_oversell(r.sessions, sell.id)
    async with r.sessions() as s:
        await s.execute(update(m.Order).where(m.Order.id == sell.id).values(excess_external=True))
        await s.commit()
    with pytest.raises(RepairRefused, match="already marked"):
        await prepare_oversell(r.sessions, sell.id)


async def test_no_repair_touches_positions_fills_or_the_pause(r: Rig) -> None:
    row = await oversold(r)

    async def snapshot() -> tuple[Any, ...]:
        async with r.sessions() as s:
            pos = [(p.id, p.qty, p.closed_at) for p in await s.scalars(select(m.Position))]
            fills = [f.id for f in await s.scalars(select(m.Fill))]
            return pos, fills, await repo.get_control(s, PAUSED_KEY)

    before = await snapshot()
    await do(r, await prepare_oversell(r.sessions, row))
    assert await snapshot() == before
