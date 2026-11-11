"""The order resolver in `RiskEngine.sync_orders()` (WP 1.5d-2, spec 2026-11-10 3.1-3.8, 8).

Every pass runs the real pair `ledger.apply_all(await engine.sync_orders())` on SimBroker and
a migrated SQLite journal. Time is the engine `Clock` (advanced by hand); the budget clock is
`Mono`. Both symbols get a bar stamped at the engine's `NOW` before any submit, so SimBroker's
`created_at` (its last bar time) equals the row's `submitted_at` unless a test moves it.
"""

import asyncio
from collections.abc import AsyncIterator, MutableMapping
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal as D
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog.testing import capture_logs

import tests.unit.risk.test_engine as te
from autotrader.broker.base import BrokerOrder, BrokerOrderView, OrderAck, OrderStatus
from autotrader.broker.sim import SimBroker
from autotrader.core.bus import EventBus
from autotrader.core.kill import ControlGate
from autotrader.core.ledger import Ledger
from autotrader.core.types import Bar, Quote
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade
from autotrader.risk.engine import OrderResult, RiskEngine, read_order_by_client_id
from autotrader.risk.resolve import entry_client_id
from autotrader.risk.types import ExitRequest, SyncResult
from tests.health import seed_baseline

BTC, ETH, NOW = te.BTC, "ETH/USD", te.NOW
SEC = timedelta(seconds=1)


def bar(i: int, symbol: str) -> Bar:
    return te.five(i).model_copy(update={"symbol": symbol})


@dataclass
class View(te.FakeView):
    """The test_engine market view, for BTC and ETH alike."""

    def history(self, symbol: str, timeframe: str) -> list[Bar]:
        if symbol not in (BTC, ETH) or timeframe != "5m":
            return []
        return [b.model_copy(update={"symbol": symbol}) for b in self.bars]

    def quote(self, symbol: str) -> Quote | None:
        if self.q is None or symbol not in (BTC, ETH):
            return None
        return self.q.model_copy(update={"symbol": symbol})

    def last_price(self, symbol: str) -> float | None:
        return self.last if symbol in (BTC, ETH) else None


def make_sim(cls: type[SimBroker] = SimBroker) -> SimBroker:
    sim = cls(
        cash=D("1000"),
        fees=te.FEES,
        slippage_bps=D(0),
        asset_rules={BTC: te.RULES, ETH: te.RULES},
        timeframe="5m",
    )
    for i in (29, 30):  # bar 30 is stamped NOW: SimBroker's created_at for later submits
        for symbol in (BTC, ETH):
            sim.process_bar(bar(i, symbol))
    return sim


def migrated(path: Path) -> str:
    url = f"sqlite+aiosqlite:///{path}"
    upgrade(url)  # sync (runs its own event loop): only from sync fixtures
    return url


async def _journal(url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = make_engine(url)
    sessions = make_sessionmaker(engine)
    await seed_baseline(sessions, NOW)
    async with sessions() as s:
        await repo.register_strategy(s, te.PROFILE, "yaml")
    yield sessions
    await engine.dispose()


@pytest.fixture
def db_url(tmp_path: Path) -> str:
    return migrated(tmp_path / "a.db")


@pytest.fixture
def other_url(tmp_path: Path) -> str:
    return migrated(tmp_path / "b.db")


@pytest.fixture
async def sessions(db_url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    async for s in _journal(db_url):
        yield s


@pytest.fixture
async def other(other_url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    async for s in _journal(other_url):
        yield s


@pytest.fixture(autouse=True)
async def _never_not_submitted(
    sessions: async_sessionmaker[AsyncSession],
) -> AsyncIterator[None]:
    """Spec 8.14: no scenario in this module ends with a row the resolver marked
    `not_submitted` (none of them takes a pre-submit path)."""
    yield
    assert all(o.status != "not_submitted" for o in await te.orders(sessions))


@dataclass
class Rig:
    sim: SimBroker
    sessions: async_sessionmaker[AsyncSession]
    clock: te.Clock
    mono: te.Mono
    ledger: Ledger
    engine: RiskEngine
    gate: ControlGate

    async def sync(self) -> SyncResult:
        result = await self.engine.sync_orders()
        await self.ledger.apply_all(result)
        return result

    async def sync_at(self, seconds: float) -> SyncResult:
        self.clock.now = NOW + seconds * SEC
        return await self.sync()

    async def entry(self, symbol: str = BTC) -> OrderResult:
        cycle = await new_cycle(self.sessions, symbol)
        return await self.engine.check(te.intent(cycle, symbol=symbol, **SMALL))

    def restart(self) -> "Rig":
        """A new process on the same journal and broker: empty in-flight set and memory."""
        return rig(self.sim, self.sessions, self.gate, self.clock, self.mono)


SMALL: dict[str, Any] = {"size_fraction": D("0.3")}  # two entries fit every exposure cap


def rig(
    sim: SimBroker,
    sessions: async_sessionmaker[AsyncSession],
    gate: ControlGate,
    clock: te.Clock | None = None,
    mono: te.Mono | None = None,
    ledger: Any = None,
) -> Rig:
    clock = clock or te.Clock(NOW)
    mono = mono or te.Mono()
    real = Ledger(EventBus(sessions), sessions, clock=clock)
    engine = te.make_engine_(
        sim, sessions, gate, view=View(), clock=clock, monotonic=mono, ledger=ledger or real
    )
    return Rig(sim, sessions, clock, mono, real, engine, gate)


@pytest.fixture
def gate(tmp_path: Path, sessions: async_sessionmaker[AsyncSession]) -> ControlGate:
    return ControlGate(tmp_path / "KILL", sessions)


@pytest.fixture
def r(sessions: async_sessionmaker[AsyncSession], gate: ControlGate) -> Rig:
    return rig(make_sim(), sessions, gate)


async def new_cycle(sessions: async_sessionmaker[AsyncSession], symbol: str = BTC) -> int:
    async with sessions() as s:
        strategy = await repo.register_strategy(s, te.PROFILE, "yaml")
        row = m.Cycle(
            strategy_id=strategy.id,
            symbol=symbol,
            rule_id="r",
            signal_features={},
            state="RISK_CHECK",
            created_at=NOW,
        )
        s.add(row)
        await s.commit()
        return row.id


async def jid_of(sessions: async_sessionmaker[AsyncSession]) -> str:
    async with sessions() as s:
        jid = await repo.journal_id(s)
    assert jid is not None
    return jid


async def next_entry_cid(sessions: async_sessionmaker[AsyncSession]) -> str:
    """The client id the next `Rig.entry()` will carry (its cycle is the next cycle id)."""
    async with sessions() as s:
        last = await s.scalar(select(m.Cycle.id).order_by(m.Cycle.id.desc()).limit(1))
    return entry_client_id(await jid_of(sessions), (last or 0) + 1)


async def insert_row(
    sessions: async_sessionmaker[AsyncSession],
    *,
    cid: str | None,
    status: str = "pending",
    qty: D = D("0.01"),
    symbol: str = BTC,
    submitted_at: datetime = NOW,
    broker_order_id: str | None = None,
) -> int:
    async with sessions() as s:
        row = await repo.insert_order(
            s,
            cycle_id=None,
            client_order_id=cid,
            symbol=symbol,
            mode="paper",
            type_="market",
            side="buy",
            qty=qty,
            limit_price=None,
            stop_price=None,
            tp_price=None,
            status=status,
            now=submitted_at,
        )
        if broker_order_id is not None:
            await repo.update_order(
                s, row.id, status=status, now=submitted_at, broker_order_id=broker_order_id
            )
        return row.id


async def order(sessions: async_sessionmaker[AsyncSession], row_id: int) -> m.Order:
    async with sessions() as s:
        row = await s.get(m.Order, row_id)
    assert row is not None
    return row


async def events(sessions: async_sessionmaker[AsyncSession], type_: str) -> list[m.Event]:
    async with sessions() as s:
        return list(
            await s.scalars(select(m.Event).where(m.Event.type == type_).order_by(m.Event.id))
        )


async def fills(sessions: async_sessionmaker[AsyncSession], row_id: int) -> list[m.Fill]:
    async with sessions() as s:
        return list(await s.scalars(select(m.Fill).where(m.Fill.order_id == row_id)))


def actions(result: SyncResult) -> list[tuple[int, str, str | None]]:
    return [(x.order_row_id, x.action, x.reason) for x in result.resolutions]


class LookupSpy(SimBroker):
    """Records every client-id lookup and `get_order` call."""

    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        self.lookups: list[str] = []
        self.reads: list[str] = []

    async def get_order_by_client_id(self, client_order_id: str) -> BrokerOrderView | None:
        self.lookups.append(client_order_id)
        return await super().get_order_by_client_id(client_order_id)

    async def get_order(self, order_id: str) -> OrderStatus:
        self.reads.append(order_id)
        return await super().get_order(order_id)


def spy_rig(sessions: async_sessionmaker[AsyncSession], gate: ControlGate) -> Rig:
    return rig(make_sim(LookupSpy), sessions, gate)


def spy(r: Rig) -> LookupSpy:
    assert isinstance(r.sim, LookupSpy)
    return r.sim


# --- type (a): rows without a broker id ---------------------------------------------------


async def test_lost_response_is_linked_and_booked(r: Rig) -> None:
    r.sim.fail_submit_after_accept()
    result = await r.entry()
    assert result.decision.approved and result.status == "unknown"
    (row,) = await te.orders(r.sessions)
    assert (row.status, row.broker_order_id) == ("unknown", None)
    r.sim.process_bar(bar(31, BTC))  # the order fills BEFORE the resolver links it
    first = await r.sync_at(2)
    assert actions(first) == [(row.id, "link", None)]
    linked = await order(r.sessions, row.id)
    (resolved,) = await events(r.sessions, "OrderResolved")
    assert linked.broker_order_id is not None
    assert linked.broker_order_id == resolved.payload["broker_order_id"]
    assert linked.settled_at is None  # read and booked by the next pass
    await r.sync_at(4)
    booked = await order(r.sessions, row.id)
    assert booked.settled_at is not None and booked.filled_qty == booked.qty
    assert len(await fills(r.sessions, row.id)) == 1
    await r.sync_at(6)  # nothing booked twice
    assert len(await fills(r.sessions, row.id)) == 1
    async with r.sessions() as s:
        assert await repo.open_position_symbols(s) == {BTC}


async def test_order_hidden_for_50s_is_linked_without_incident(r: Rig) -> None:
    r.sim.hide(await next_entry_cid(r.sessions), lambda: r.clock.now >= NOW + 50 * SEC)
    r.sim.fail_submit_after_accept()
    await r.entry()
    (row,) = await te.orders(r.sessions)
    for t in range(0, 50, 10):
        assert actions(await r.sync_at(t)) == []
    assert actions(await r.sync_at(50)) == [(row.id, "link", None)]
    await r.sync_at(52)
    assert await events(r.sessions, "OrderIncident") == []
    linked = await order(r.sessions, row.id)
    assert (linked.incident, linked.broker_order_id is not None) == (None, True)


async def test_never_visible_becomes_not_found_at_80s_not_earlier(r: Rig) -> None:
    r.sim.hide(await next_entry_cid(r.sessions), lambda: False)
    r.sim.fail_submit_after_accept()
    await r.entry()
    (row,) = await te.orders(r.sessions)
    for t in (0, 30, 69, 70, 79):
        assert actions(await r.sync_at(t)) == []
    assert (await order(r.sessions, row.id)).incident is None
    assert actions(await r.sync_at(80)) == [(row.id, "incident", "not_found")]
    marked = await order(r.sessions, row.id)
    assert (marked.incident, marked.status, marked.settled_at) == ("not_found", "unknown", None)
    assert marked.incident_at == NOW + 80 * SEC
    btc = await r.entry(BTC)
    assert not btc.decision.approved
    assert not btc.decision.reasons[0].startswith("state unavailable")
    eth = await r.entry(ETH)
    assert eth.decision.approved, eth.decision.reasons


class RaisingLookup(SimBroker):
    async def get_order_by_client_id(self, client_order_id: str) -> BrokerOrderView | None:
        raise ConnectionError("lookup down")


async def test_raising_lookup_becomes_lookup_failed_never_zero_match(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    r = rig(make_sim(RaisingLookup), sessions, gate)
    r.sim.fail_submit_after_accept()
    await r.entry()
    (row,) = await te.orders(sessions)
    for t in (0, 40, 79):
        assert actions(await r.sync_at(t)) == []
    assert actions(await r.sync_at(80)) == [(row.id, "incident", "lookup_failed")]
    assert (await order(sessions, row.id)).status == "unknown"


async def test_restart_resolves_pending_rows_with_persisted_clocks(r: Rig) -> None:
    jid = await jid_of(r.sessions)
    cid = entry_client_id(jid, 41)
    ack = await r.sim.submit_order(
        BrokerOrder(symbol=BTC, side="buy", qty=D("0.01"), type="market", client_order_id=cid)
    )
    pending = await insert_row(r.sessions, cid=cid)  # a "previous process" row
    lost_cid = entry_client_id(jid, 42)
    lost = await r.sim.submit_order(
        BrokerOrder(symbol=ETH, side="buy", qty=D("0.01"), type="market", client_order_id=lost_cid)
    )
    known = await insert_row(
        r.sessions, cid=lost_cid, symbol=ETH, status="new", broker_order_id=lost.order_id
    )
    r.sim.rekey(lost.order_id)
    r.sim.hide(lost_cid, lambda: False)
    first = await r.sync_at(10)
    assert actions(first) == [(pending, "link", None), (known, "start_clock", None)]
    assert (await order(r.sessions, pending)).broker_order_id == ack.order_id
    assert (await order(r.sessions, known)).resolve_since == NOW + 10 * SEC
    again = r.restart()
    assert actions(await again.sync_at(89)) == []
    assert (await order(r.sessions, known)).resolve_since == NOW + 10 * SEC
    assert actions(await again.sync_at(90)) == [(known, "incident", "not_found")]


class BlockedSubmit(te.HangingSubmit, LookupSpy):
    """`submit_order` waits for `release`, then submits for real."""

    def __init__(self, **kw: Any) -> None:
        super().__init__(**kw)
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        self.entered.set()
        await self.release.wait()
        return await SimBroker.submit_order(self, order)


async def test_in_flight_row_is_never_looked_up(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    r = rig(make_sim(BlockedSubmit), sessions, gate)
    sim = r.sim
    assert isinstance(sim, BlockedSubmit)
    task = asyncio.create_task(r.entry())
    await asyncio.wait_for(sim.entered.wait(), 5)
    (row,) = await te.orders(sessions)
    assert (row.status, row.broker_order_id) == ("pending", None)
    result = await r.sync_at(200)  # far past its deadline: still never looked up
    assert actions(result) == [] and sim.lookups == []
    sim.release.set()
    done = await asyncio.wait_for(task, 5)
    assert done.status == "new"
    assert (await order(sessions, row.id)).incident is None


@pytest.mark.parametrize("variant", ["qty", "created_at"])
async def test_client_id_hit_with_wrong_qty_or_old_created_at_is_a_mismatch(
    variant: str, r: Rig
) -> None:
    cid = entry_client_id(await jid_of(r.sessions), 7)
    await r.sim.submit_order(  # created_at = NOW (the sim's last bar)
        BrokerOrder(symbol=BTC, side="buy", qty=D("0.01"), type="market", client_order_id=cid)
    )
    if variant == "qty":
        row = await insert_row(r.sessions, cid=cid, qty=D("0.02"))
    else:  # submitted 1 s past the 5-minute skew bound after the broker's created_at
        row = await insert_row(r.sessions, cid=cid, submitted_at=NOW + timedelta(minutes=5) + SEC)
    assert actions(await r.sync_at(0)) == [(row, "incident", "client_id_mismatch")]
    marked = await order(r.sessions, row)
    assert (marked.incident, marked.broker_order_id) == ("client_id_mismatch", None)
    assert await events(r.sessions, "OrderResolved") == []


async def test_rows_without_ownership_proof_are_incidents_without_lookup(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    r = spy_rig(sessions, gate)
    rows = [await insert_row(sessions, cid=c) for c in (None, "at-o5", "at-c7-e")]
    result = await r.sync_at(0)
    assert actions(result) == [(i, "incident", "no_ownership_proof") for i in rows]
    assert spy(r).lookups == []
    assert [(await order(sessions, i)).incident for i in rows] == ["no_ownership_proof"] * 3


async def test_other_journals_order_is_never_linked(
    tmp_path: Path,
    sessions: async_sessionmaker[AsyncSession],
    other: async_sessionmaker[AsyncSession],
    gate: ControlGate,
) -> None:
    sim = make_sim()
    a = rig(sim, sessions, gate)
    b = rig(sim, other, ControlGate(tmp_path / "KILL-B", other))
    assert await jid_of(sessions) != await jid_of(other)
    mine = await a.entry()
    assert mine.status == "new" and mine.broker_order_id is not None
    sim.fail_submit_after_accept()
    lost = await b.entry()  # same cycle id, symbol, side and qty
    assert lost.status == "unknown"
    (b_row,) = await te.orders(other)
    (a_row,) = await te.orders(sessions)
    assert (b_row.cycle_id, b_row.qty) == (a_row.cycle_id, a_row.qty)
    assert actions(await b.sync_at(0)) == [(b_row.id, "link", None)]
    linked = await order(other, b_row.id)
    assert linked.broker_order_id is not None
    assert linked.broker_order_id != mine.broker_order_id


# --- type (b): rows whose broker id the broker no longer knows ----------------------------


async def acked_entry(r: Rig, symbol: str = BTC) -> tuple[int, str]:
    result = await r.entry(symbol)
    assert result.order_row_id is not None and result.broker_order_id is not None
    return result.order_row_id, result.broker_order_id


async def test_type_b_incident_after_check_cleared_the_clock_is_dropped(r: Rig) -> None:
    row, oid = await acked_entry(r)
    r.sim.forget(oid)
    assert actions(await r.sync_at(0)) == [(row, "start_clock", None)]
    r.clock.now = NOW + 80 * SEC
    decided = await r.engine.sync_orders()
    assert actions(decided) == [(row, "incident", "lookup_inconsistent")]
    r.sim.unforget(oid)
    await r.entry(ETH)  # check()'s read succeeds: it clears the clock first
    assert (await order(r.sessions, row)).resolve_since is None
    await r.ledger.apply_all(decided)
    kept = await order(r.sessions, row)
    assert (kept.incident, kept.resolve_since) == (None, None)
    assert await events(r.sessions, "OrderIncident") == []
    r.sim.process_bar(bar(31, BTC))
    await r.sync_at(82)
    booked = await order(r.sessions, row)
    assert booked.settled_at is not None and booked.filled_qty == booked.qty


class HeldLedger:
    """Holds `check()`'s `apply_all` (when armed) until `release` is set."""

    def __init__(self) -> None:
        self.real: Ledger | None = None
        self.armed = False
        self.reached = asyncio.Event()
        self.release = asyncio.Event()

    async def apply_all(self, result: SyncResult) -> frozenset[str]:
        assert self.real is not None
        if self.armed:
            self.armed = False
            self.reached.set()
            await self.release.wait()
        return await self.real.apply_all(result)


async def test_delayed_check_clear_keeps_the_new_episode_bound(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    held = HeldLedger()
    r = rig(make_sim(), sessions, gate, ledger=held)
    held.real = r.ledger
    row, oid = await acked_entry(r)
    r.sim.forget(oid)
    t0 = NOW + 1 * SEC
    await r.sync_at(1)
    assert (await order(sessions, row)).resolve_since == t0
    r.sim.unforget(oid)
    held.armed = True
    task = asyncio.create_task(r.entry(ETH))  # its read yields clear_clock(t0); apply held
    await asyncio.wait_for(held.reached.wait(), 5)
    assert actions(await r.sync_at(5)) == [(row, "clear_clock", None)]  # the sync loop clears
    assert (await order(sessions, row)).resolve_since is None
    r.sim.forget(oid)
    t1 = NOW + 20 * SEC
    await r.sync_at(20)
    assert (await order(sessions, row)).resolve_since == t1
    held.release.set()
    await asyncio.wait_for(task, 5)
    assert (await order(sessions, row)).resolve_since == t1  # the late clear(t0) did nothing
    assert actions(await r.sync_at(99)) == []
    assert actions(await r.sync_at(100)) == [(row, "incident", "lookup_inconsistent")]
    marked = await order(sessions, row)
    assert (marked.incident, marked.incident_at) == ("lookup_inconsistent", t1 + 80 * SEC)


async def test_incident_holds_back_later_rows_on_its_symbol(r: Rig) -> None:
    visible = {"btc": False}
    r.sim.hide(await next_entry_cid(r.sessions), lambda: visible["btc"])
    r.sim.fail_submit_after_accept()
    await r.entry()
    (entry,) = await te.orders(r.sessions)
    await r.sync_at(80)
    assert (await order(r.sessions, entry.id)).incident == "not_found"
    eth_row, _ = await acked_entry(r, ETH)
    for symbol in (BTC, ETH):
        r.sim.process_bar(bar(31, symbol))  # the hidden BTC entry and the ETH entry fill
    sold = await r.engine.exit(ExitRequest(symbol=BTC, reason="stop"))
    assert sold.status == "new" and sold.order_row_id is not None
    r.sim.process_bar(bar(32, BTC))  # the sell fills
    with capture_logs() as logs:
        await r.sync_at(90)
    assert {"event": "ledger.update_deferred", "order_row": sold.order_row_id} in [
        {k: e[k] for k in ("event", "order_row") if k in e} for e in logs
    ]
    assert await fills(r.sessions, sold.order_row_id) == []
    assert (await order(r.sessions, eth_row)).settled_at is not None  # ETH books normally
    async with r.sessions() as s:  # what `repair link` writes (Task 9)
        await s.execute(
            update(m.Order)
            .where(m.Order.id == entry.id)
            .values(broker_order_id="sim-1", incident=None, incident_at=None, resolve_since=None)
        )
        await s.commit()
    visible["btc"] = True
    await r.sync_at(92)
    first, second = await order(r.sessions, entry.id), await order(r.sessions, sold.order_row_id)
    assert first.settled_at is not None and second.settled_at is not None
    assert len(await fills(r.sessions, entry.id)) == len(await fills(r.sessions, second.id)) == 1
    async with r.sessions() as s:
        assert await repo.open_position_symbols(s) == {ETH}


class BrokenRead(LookupSpy):
    async def get_order(self, order_id: str) -> OrderStatus:
        raise ConnectionError("read down")


async def test_generic_read_failure_stays_failed_not_type_b(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    r = rig(make_sim(BrokenRead), sessions, gate)
    cid = entry_client_id(await jid_of(sessions), 3)
    row = await insert_row(sessions, cid=cid, status="new", broker_order_id="sim-77")
    result = await r.sync_at(500)
    assert result.failed == (row,) and actions(result) == [] and spy(r).lookups == []


async def test_resolver_idles_without_journal_id(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    r = spy_rig(sessions, gate)
    jid = await jid_of(sessions)
    lost = await insert_row(sessions, cid=entry_client_id(jid, 1), submitted_at=NOW - 500 * SEC)
    gone = await insert_row(
        sessions, cid=entry_client_id(jid, 2), status="new", broker_order_id="sim-404"
    )
    async with sessions() as s:
        await s.execute(delete(m.ControlState).where(m.ControlState.key == repo.JOURNAL_ID_KEY))
        await s.commit()
    with capture_logs() as logs:
        result = await r.sync_at(0)
    assert actions(result) == [] and result.failed == (gone,) and spy(r).lookups == []
    assert "risk.journal_id_missing" in [e["event"] for e in logs]
    assert (await order(sessions, lost)).incident is None


async def test_type_b_blocks_all_until_deadline_then_only_its_symbol(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    r = spy_rig(sessions, gate)
    row, oid = await acked_entry(r)
    cid = (await order(sessions, row)).client_order_id
    assert cid is not None
    r.sim.rekey(oid)
    r.sim.hide(cid, lambda: False)
    first = await r.sync_at(0)
    assert first.failed == (row,) and actions(first) == [(row, "start_clock", None)]
    assert (await order(sessions, row)).resolve_since == NOW
    for t in (10, 79):
        r.clock.now = NOW + t * SEC
        eth = await r.entry(ETH)
        assert eth.decision.reasons == ("state unavailable: SyncIncomplete",)
        assert (await r.sync_at(t)).failed == (row,)
    result = await r.sync_at(80)
    assert actions(result) == [(row, "incident", "not_found")] and result.failed == ()
    eth = await r.entry(ETH)
    assert eth.decision.approved, eth.decision.reasons
    btc = await r.entry(BTC)
    assert not btc.decision.approved
    assert not btc.decision.reasons[0].startswith("state unavailable")
    spy(r).reads.clear()
    await r.sync_at(82)
    assert oid not in spy(r).reads


async def test_type_b_inconsistent_lookup_is_one_incident_across_restart(r: Rig) -> None:
    row, oid = await acked_entry(r)
    r.sim.forget(oid)  # get_order misses, the client-id lookup keeps matching
    for t in (0, 20, 40):
        await r.sync_at(t)
        assert (await order(r.sessions, row)).resolve_since == NOW
    again = r.restart()
    for t in (60, 79):
        assert actions(await again.sync_at(t)) == []
        assert (await order(r.sessions, row)).resolve_since == NOW
    assert actions(await again.sync_at(80)) == [(row, "incident", "lookup_inconsistent")]
    for t in (82, 90):
        await again.sync_at(t)
    (incident,) = await events(r.sessions, "OrderIncident")
    assert incident.payload["reason"] == "lookup_inconsistent"


class BudgetEater(LookupSpy):
    """When armed, the next client-id lookup spends the whole sync budget."""

    mono: te.Mono | None = None
    armed = False

    async def get_order_by_client_id(self, client_order_id: str) -> BrokerOrderView | None:
        if self.armed and self.mono is not None:
            self.armed = False
            self.mono.t += 100.0
        return await super().get_order_by_client_id(client_order_id)


async def test_spent_budget_never_counts_as_zero_match(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    r = rig(make_sim(BudgetEater), sessions, gate)
    sim = r.sim
    assert isinstance(sim, BudgetEater)
    sim.mono = r.mono
    jid = await jid_of(sessions)
    young = await insert_row(sessions, cid=entry_client_id(jid, 1))  # +30 s at pass 2
    old = await insert_row(sessions, cid=entry_client_id(jid, 2), submitted_at=NOW - 55 * SEC)
    assert actions(await r.sync_at(0)) == []  # both looked up: not_found, remembered
    never = await insert_row(sessions, cid=entry_client_id(jid, 3), submitted_at=NOW - 60 * SEC)
    sim.armed = True
    sim.lookups.clear()
    result = await r.sync_at(30)
    assert sim.lookups == [entry_client_id(jid, 1)]  # the rest were not looked up
    assert actions(result) == [(old, "incident", "not_found"), (never, "incident", "lookup_failed")]
    assert (await order(sessions, young)).incident is None


async def test_cleared_clock_restarts_at_the_next_miss(r: Rig) -> None:
    row, oid = await acked_entry(r)
    r.sim.forget(oid)
    await r.sync_at(0)
    assert (await order(r.sessions, row)).resolve_since == NOW
    r.sim.unforget(oid)
    result = await r.sync_at(10)
    assert actions(result) == [(row, "clear_clock", None)]
    assert result.resolutions[0].clock == NOW
    assert (await order(r.sessions, row)).resolve_since is None
    r.sim.forget(oid)
    await r.sync_at(20)
    assert (await order(r.sessions, row)).resolve_since == NOW + 20 * SEC


async def test_incident_reminder_every_60s(r: Rig) -> None:
    row = await insert_row(r.sessions, cid=None)
    other = await insert_row(r.sessions, cid="at-o5", symbol=ETH)

    async def reminders(at_mono: float, t: float) -> list[MutableMapping[str, Any]]:
        r.mono.t = at_mono
        with capture_logs() as logs:
            await r.sync_at(t)
        return [e for e in logs if e["event"] == "risk.order_incident_open"]

    (first,) = await reminders(0, 0)  # the incidents of this pass
    assert first["log_level"] == "error"
    assert first["order_rows"] == [
        {"order_row": row, "reason": "no_ownership_proof"},
        {"order_row": other, "reason": "no_ownership_proof"},
    ]
    assert await reminders(30, 2) == []
    assert await reminders(59.5, 4) == []
    (again,) = await reminders(60, 6)  # now read from the journal
    assert again["order_rows"] == first["order_rows"]
    assert await reminders(90, 8) == []


class PartlyRaising(SimBroker):
    """Client-id lookups of the ids in `raise_for` raise; the others answer."""

    raise_for: frozenset[str] = frozenset()

    async def get_order_by_client_id(self, client_order_id: str) -> BrokerOrderView | None:
        if client_order_id in self.raise_for:
            raise ConnectionError("lookup down")
        return await super().get_order_by_client_id(client_order_id)


async def test_resolver_never_sets_not_submitted(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    """Zero matches and failing lookups, far past the bound: incidents, never
    `not_submitted` (the module's autouse check covers every other scenario too)."""
    r = rig(make_sim(PartlyRaising), sessions, gate)
    sim = r.sim
    assert isinstance(sim, PartlyRaising)
    r.sim.hide(await next_entry_cid(sessions), lambda: False)
    r.sim.fail_submit_after_accept()
    await r.entry()
    sim.raise_for = frozenset({await next_entry_cid(sessions)})
    r.sim.fail_submit_after_accept()
    await r.entry(ETH)
    for t in (0, 80, 200, 1000):
        await r.sync_at(t)
    assert [(o.status, o.incident) for o in await te.orders(sessions)] == [
        ("unknown", "not_found"),
        ("unknown", "lookup_failed"),
    ]


# --- read_order_by_client_id: the repair CLI's broker read (spec 5.2) ---------------------


async def test_read_order_by_client_id_finds_the_order(r: Rig) -> None:
    cid = entry_client_id(await jid_of(r.sessions), 9)
    ack = await r.sim.submit_order(
        BrokerOrder(symbol=BTC, side="buy", qty=D("0.01"), type="market", client_order_id=cid)
    )
    view = await read_order_by_client_id(r.sim, cid, timeout_s=5)
    assert view is not None and (view.order_id, view.client_order_id) == (ack.order_id, cid)


async def test_read_order_by_client_id_returns_none_for_a_miss(r: Rig) -> None:
    assert await read_order_by_client_id(r.sim, "at-00000000-c1-e", timeout_s=5) is None


class HangingLookup(SimBroker):
    async def get_order_by_client_id(self, client_order_id: str) -> BrokerOrderView | None:
        await asyncio.Event().wait()
        return None


async def test_read_order_by_client_id_times_out() -> None:
    with pytest.raises(TimeoutError):
        await read_order_by_client_id(make_sim(HangingLookup), "at-00000000-c1-e", timeout_s=0.01)
