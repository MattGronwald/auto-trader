"""Runner/ledger race on the real engine and ledger (WP 1.5d-3, 1.5b follow-up c; spec
2026-11-11 section 7 tests 19-23).

The Cycle Runner and the ledger's sync pass can both move a cycle on from RISK_CHECK or
SUBMITTED. Each test forces one interleaving with an explicit double: `SyncFirst` runs a sync
pass inside `check()` (before the runner writes SUBMITTED), `FilledAckSim` acks the entry
`filled`, and a wrapped `runner._advance` runs a sync pass right after a chosen transition
write. `CommitHook` is not used: it blocks while the bus lock is held, and the ledger books
through `publish_tx` on the same bus, so it would deadlock.
"""

import time
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from decimal import Decimal as D
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog.testing import capture_logs

from autotrader.broker.base import BrokerOrder, OrderAck
from autotrader.broker.sim import FillPlan, SimBroker
from autotrader.core.bus import EventBus
from autotrader.core.cycle import TERMINAL, CycleRunner, EntryChecker
from autotrader.core.kill import ControlGate
from autotrader.core.ledger import Ledger
from autotrader.core.runtime import _snapshot_step, _sync_step
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade
from autotrader.risk.engine import OrderResult, RiskEngine
from autotrader.risk.types import EntryIntent
from tests.health import seed_baseline
from tests.integration.test_cycle_runner import (
    BTC,
    FEES,
    NOW,
    RULES,
    View,
    candidate,
    five,
    profile,
    sim_broker,
)

FILL_BAR = five(30)  # the first bar after the entry's submit (five(29) is the sim's clock)


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


@pytest.fixture
def db_url(tmp_path: Path) -> str:
    # Sync fixture: Alembic's upgrade runs its own event loop (asyncio.run).
    url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    upgrade(url)
    return url


@pytest.fixture
async def sessions(db_url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = make_engine(db_url)
    sessions = make_sessionmaker(engine)
    await seed_baseline(sessions, NOW)  # a trusted daily-loss baseline (once per DB)
    yield sessions
    await engine.dispose()


@dataclass
class Rig:
    runner: CycleRunner
    engine: RiskEngine
    ledger: Ledger
    bus: EventBus
    sim: SimBroker
    clock: Clock


@pytest.fixture
async def rigs() -> AsyncIterator[list[Rig]]:
    """Every rig a test builds is closed at teardown."""
    built: list[Rig] = []
    yield built
    for rig in built:
        await rig.runner.close(budget_s=5)
        await rig.bus.close()


async def make_rig(
    tmp_path: Path,
    sessions: async_sessionmaker[AsyncSession],
    sim: SimBroker,
    rigs: list[Rig],
    *,
    healthy: bool = True,
    risk: Callable[[RiskEngine, Ledger, SimBroker], EntryChecker] | None = None,
) -> Rig:
    """A real engine, ledger and runner on `sim`, recovered. `healthy` pokes the engine's
    snapshot and cash health and opens admission; otherwise both are left to the test.
    `risk` wraps the engine as the runner's `EntryChecker` (race doubles)."""
    prof = profile()
    async with sessions() as s:
        strategy_id = (await repo.register_strategy(s, prof, "yaml")).id
    sim.process_bar(five(29))
    clock = Clock(NOW)
    gate = ControlGate(tmp_path / "KILL", sessions)
    view = View()
    bus = EventBus(sessions)
    ledger = Ledger(bus, sessions, clock=clock)
    engine = RiskEngine(
        sim,
        profile=prof,
        strategy_id=strategy_id,
        mode="paper",
        gate=gate,
        market=view,
        sessions=sessions,
        ledger=ledger,
        clock=clock,
    )
    runner = CycleRunner(
        profile=prof,
        strategy_id=strategy_id,
        market=view,
        gate=gate,
        risk=engine if risk is None else risk(engine, ledger, sim),
        bus=bus,
        sessions=sessions,
        clock=clock,
    )
    rig = Rig(runner, engine, ledger, bus, sim, clock)
    rigs.append(rig)
    await runner.recover()
    if healthy:
        engine.last_snapshot_at = time.monotonic()
        engine.cash_reconciled()
        runner.open_admission()
    return rig


class SyncFirst:
    """The ledger completes the cycle before the runner writes SUBMITTED: the real `check()`,
    then the entry fills (or ends) on the next bar and a sync pass books it."""

    def __init__(self, engine: RiskEngine, ledger: Ledger, sim: SimBroker) -> None:
        self.engine, self.ledger, self.sim = engine, ledger, sim

    async def check(self, intent: EntryIntent) -> OrderResult:
        result = await self.engine.check(intent)
        self.sim.process_bar(FILL_BAR)
        failed = await self.ledger.apply_all(await self.engine.sync_orders())
        assert failed == frozenset()
        return result


class FilledAckSim(SimBroker):
    """The entry fills inside the submit, and the ack says `filled`."""

    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        ack = await super().submit_order(order)
        self.process_bar(FILL_BAR)
        return ack.model_copy(update={"status": "filled"})


def filled_ack_sim() -> FilledAckSim:
    return FilledAckSim(
        cash=D("1000"), fees=FEES, slippage_bps=D("0"), asset_rules={BTC: RULES}, timeframe="5m"
    )


def zero_fill_sim() -> SimBroker:
    sim = sim_broker()
    sim.script(BTC, "buy", FillPlan(("cancel",)))  # IOC ends canceled, nothing filled
    return sim


def sync_after(rig: Rig, state: str, monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Run one sync pass right after the runner's `state` write, before its next write.
    Returns the states the runner wrote, in order."""
    original = rig.runner._advance
    written: list[str] = []

    async def advance(cur: Any, to: Any, *args: Any, **kwargs: Any) -> None:
        await original(cur, to, *args, **kwargs)
        written.append(str(to))
        if to == state:
            await _sync_step(rig.engine, rig.ledger)

    monkeypatch.setattr(rig.runner, "_advance", advance)
    return written


async def the_cycle(sessions: async_sessionmaker[AsyncSession]) -> m.Cycle:
    async with sessions() as s:
        (cycle,) = list(await s.scalars(select(m.Cycle)))
        return cycle


async def edges(
    sessions: async_sessionmaker[AsyncSession], cycle_id: int
) -> list[tuple[str | None, str]]:
    async with sessions() as s:
        rows = await s.scalars(
            select(m.Event)
            .where(m.Event.type == "CycleTransitioned", m.Event.cycle_id == cycle_id)
            .order_by(m.Event.id)
        )
        return [(r.payload["from_state"], r.payload["to_state"]) for r in rows]


async def count(sessions: async_sessionmaker[AsyncSession], model: Any) -> int:
    async with sessions() as s:
        return int(await s.scalar(select(func.count()).select_from(model)) or 0)


def terminal(e: list[tuple[str | None, str]]) -> list[tuple[str | None, str]]:
    return [(f, t) for f, t in e if t in {str(s) for s in TERMINAL}]


def log_events(logs: list[Any], name: str) -> list[Any]:
    return [e for e in logs if e["event"] == name]


async def test_sync_completes_filled_before_the_runner_writes_submitted(
    tmp_path: Path, sessions: async_sessionmaker[AsyncSession], rigs: list[Rig]
) -> None:
    rig = await make_rig(tmp_path, sessions, sim_broker(), rigs, risk=SyncFirst)
    with capture_logs() as logs:
        await rig.runner.on_candidate(candidate())
        await rig.runner.join()

    cycle = await the_cycle(sessions)
    e = await edges(sessions, cycle.id)
    assert cycle.state == "FILLED"
    assert terminal(e) == [("RISK_CHECK", "FILLED")]
    assert "SUBMITTED" not in [t for _, t in e]
    assert [(x["state"], x["to_state"]) for x in log_events(logs, "cycle.completed_elsewhere")] == [
        ("RISK_CHECK", "SUBMITTED")
    ]
    assert log_events(logs, "cycle.stuck") == []
    assert await count(sessions, m.Position) == 1
    assert await count(sessions, m.Fill) == len(await rig.sim.order_fills("sim-1")) == 1


async def test_runner_submitted_new_then_sync_fills(
    tmp_path: Path, sessions: async_sessionmaker[AsyncSession], rigs: list[Rig]
) -> None:
    rig = await make_rig(tmp_path, sessions, sim_broker(), rigs)
    await rig.runner.on_candidate(candidate())
    await rig.runner.join()
    cycle = await the_cycle(sessions)
    assert cycle.state == "SUBMITTED"
    async with sessions() as s:
        (order,) = list(await s.scalars(select(m.Order)))
    assert order.status == "new"
    assert await count(sessions, m.Position) == 0

    rig.sim.process_bar(FILL_BAR)
    await _sync_step(rig.engine, rig.ledger)

    cycle = await the_cycle(sessions)
    assert cycle.state == "FILLED"
    assert terminal(await edges(sessions, cycle.id)) == [("SUBMITTED", "FILLED")]
    assert await count(sessions, m.Position) == 1
    assert await count(sessions, m.Fill) == len(await rig.sim.order_fills("sim-1")) == 1


async def test_runner_filled_and_sync_book_the_same_order_once(
    tmp_path: Path,
    sessions: async_sessionmaker[AsyncSession],
    rigs: list[Rig],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rig = await make_rig(tmp_path, sessions, filled_ack_sim(), rigs)
    written = sync_after(rig, "SUBMITTED", monkeypatch)
    with capture_logs() as logs:
        await rig.runner.on_candidate(candidate())
        await rig.runner.join()
    await _sync_step(rig.engine, rig.ledger)  # a later pass sees the same fill again

    cycle = await the_cycle(sessions)
    assert written[-1] == "SUBMITTED"  # the runner's FILLED write lost the race
    assert cycle.state == "FILLED"
    assert terminal(await edges(sessions, cycle.id)) == [("SUBMITTED", "FILLED")]
    assert [(x["state"], x["to_state"]) for x in log_events(logs, "cycle.completed_elsewhere")] == [
        ("SUBMITTED", "FILLED")
    ]
    assert log_events(logs, "cycle.stuck") == []
    assert await count(sessions, m.Position) == 1
    broker_fills = await rig.sim.order_fills("sim-1")
    assert len(broker_fills) == 1
    async with sessions() as s:
        booked = list(await s.scalars(select(m.Fill.broker_fill_id)))
    assert booked == [f.fill_id for f in broker_fills]


@pytest.mark.parametrize("order", ["sync_first", "runner_first"])
async def test_zero_fill_ends_expired_once(
    order: str, tmp_path: Path, sessions: async_sessionmaker[AsyncSession], rigs: list[Rig]
) -> None:
    sim = zero_fill_sim()
    if order == "sync_first":
        rig = await make_rig(tmp_path, sessions, sim, rigs, risk=SyncFirst)
        with capture_logs() as logs:
            await rig.runner.on_candidate(candidate())
            await rig.runner.join()
        expected = [("RISK_CHECK", "EXPIRED")]
        elsewhere = [("RISK_CHECK", "SUBMITTED")]
    else:
        rig = await make_rig(tmp_path, sessions, sim, rigs)
        with capture_logs() as logs:
            await rig.runner.on_candidate(candidate())
            await rig.runner.join()
            assert (await the_cycle(sessions)).state == "SUBMITTED"
            sim.process_bar(FILL_BAR)
            await _sync_step(rig.engine, rig.ledger)
        expected = [("SUBMITTED", "EXPIRED")]
        elsewhere = []

    cycle = await the_cycle(sessions)
    assert cycle.state == "EXPIRED"
    assert terminal(await edges(sessions, cycle.id)) == expected
    assert [
        (x["state"], x["to_state"]) for x in log_events(logs, "cycle.completed_elsewhere")
    ] == elsewhere
    assert log_events(logs, "cycle.stuck") == []
    assert (await sim.get_order("sim-1")).status == "canceled"
    assert await count(sessions, m.Position) == 0
    assert await count(sessions, m.Fill) == 0


async def test_snapshot_step_then_approved_check_through_the_runner(
    tmp_path: Path, sessions: async_sessionmaker[AsyncSession], rigs: list[Rig]
) -> None:
    # `sessions` seeded the baseline; the engine starts with no loop snapshot and a cash fault.
    rig = await make_rig(tmp_path, sessions, sim_broker(), rigs, healthy=False)
    assert rig.engine.health_faults() == ("equity snapshot overdue", "cash reconciliation failed")

    await _snapshot_step(rig.engine, rig.ledger)
    assert rig.engine.health_faults() == ()
    rig.clock.advance(1)  # check() writes its own snapshot at its ts; equity_snapshots.ts is unique

    rig.runner.open_admission()
    await rig.runner.on_candidate(candidate())
    await rig.runner.join()

    cycle = await the_cycle(sessions)
    assert cycle.state == "SUBMITTED"
    assert terminal(await edges(sessions, cycle.id)) == []
    async with sessions() as s:
        (decision,) = list(await s.scalars(select(m.RiskDecision)))
        (order,) = list(await s.scalars(select(m.Order)))
    assert decision.approved
    assert (order.cycle_id, order.status) == (cycle.id, "new")
    assert await count(sessions, m.Position) == 0
    assert await count(sessions, m.Fill) == 0
