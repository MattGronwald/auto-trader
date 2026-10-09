import asyncio
import io
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.core.bus import BusClosedError, DomainEvent, EventBus
from autotrader.core.logging import configure_logging
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade

T0 = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)


class Ping(DomainEvent):
    n: int


class Pong(DomainEvent):
    text: str


class LoudPing(Ping):
    pass


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


async def _events(sessions: async_sessionmaker[AsyncSession]) -> list[m.Event]:
    async with sessions() as s:
        return list(await s.scalars(select(m.Event).order_by(m.Event.id)))


# --- persistence ---------------------------------------------------------------------


async def test_publish_persists_type_payload_cycle(
    bus: EventBus, sessions: async_sessionmaker[AsyncSession]
) -> None:
    await bus.publish(Ping(n=7, ts=T0))

    (row,) = await _events(sessions)
    assert row.type == "Ping"
    assert row.payload == {"n": 7}
    assert row.cycle_id is None
    assert row.ts == T0


async def test_event_is_persisted_before_handlers_run(
    bus: EventBus, sessions: async_sessionmaker[AsyncSession]
) -> None:
    seen_in_db: list[int] = []

    async def handler(_: Ping) -> None:
        seen_in_db.append(len(await _events(sessions)))

    bus.subscribe(Ping, handler)
    await bus.publish(Ping(n=1))
    await bus.drain()

    assert seen_in_db == [1]


async def test_persist_failure_raises_and_nothing_is_delivered(bus: EventBus) -> None:
    received: list[Ping] = []

    async def handler(e: Ping) -> None:
        received.append(e)

    bus.subscribe(Ping, handler)
    with pytest.raises(Exception):  # noqa: B017 - FK violation from the DB
        await bus.publish(Ping(n=1, cycle_id=999))  # no such cycle
    await bus.drain()

    assert received == []


# --- delivery ------------------------------------------------------------------------


async def test_subscribers_get_their_type_and_subclasses_only(bus: EventBus) -> None:
    pings: list[DomainEvent] = []
    everything: list[DomainEvent] = []

    async def on_ping(e: Ping) -> None:
        pings.append(e)

    async def on_any(e: DomainEvent) -> None:
        everything.append(e)

    bus.subscribe(Ping, on_ping)
    bus.subscribe(DomainEvent, on_any)
    await bus.publish(Ping(n=1))
    await bus.publish(Pong(text="x"))
    await bus.publish(LoudPing(n=2))
    await bus.drain()

    assert [type(e).__name__ for e in pings] == ["Ping", "LoudPing"]
    assert [type(e).__name__ for e in everything] == ["Ping", "Pong", "LoudPing"]


async def test_order_is_preserved_per_subscriber(bus: EventBus) -> None:
    got: list[int] = []

    async def handler(e: Ping) -> None:
        await asyncio.sleep(0)
        got.append(e.n)

    bus.subscribe(Ping, handler)
    for i in range(20):
        await bus.publish(Ping(n=i))
    await bus.drain()

    assert got == list(range(20))


async def test_slow_subscriber_does_not_block_publish_or_others(bus: EventBus) -> None:
    release = asyncio.Event()
    fast: list[int] = []

    async def slow(_: Ping) -> None:
        await release.wait()

    async def quick(e: Ping) -> None:
        fast.append(e.n)

    bus.subscribe(Ping, slow)
    bus.subscribe(Ping, quick)
    await asyncio.wait_for(bus.publish(Ping(n=1)), timeout=1)
    await asyncio.wait_for(bus.publish(Ping(n=2)), timeout=1)
    for _ in range(10):
        await asyncio.sleep(0)

    assert fast == [1, 2]
    release.set()


async def test_failing_handler_is_logged_and_skipped(bus: EventBus) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    got: list[int] = []

    async def flaky(e: Ping) -> None:
        if e.n == 1:
            raise RuntimeError("boom")
        got.append(e.n)

    bus.subscribe(Ping, flaky)
    for i in range(3):
        await bus.publish(Ping(n=i))
    await bus.drain()

    assert got == [0, 2]
    errors = [json.loads(line) for line in buf.getvalue().splitlines()]
    failed = [e for e in errors if e["event"] == "bus.handler_failed"]
    assert len(failed) == 1
    assert failed[0]["event_type"] == "Ping"
    assert "RuntimeError: boom" in failed[0]["exception"]


async def _make_cycle(sessions: async_sessionmaker[AsyncSession]) -> int:
    async with sessions() as s:
        strategy = m.Strategy(name="x", yaml="", hash="h", active_from=T0)
        s.add(strategy)
        await s.flush()
        cycle = m.Cycle(
            strategy_id=strategy.id,
            symbol="BTC/USD",
            rule_id="r",
            signal_features={},
            state="NEW",
            created_at=T0,
        )
        s.add(cycle)
        await s.commit()
        return cycle.id


async def test_handler_logs_carry_the_event_cycle_id(
    bus: EventBus, sessions: async_sessionmaker[AsyncSession]
) -> None:
    cycle_id = await _make_cycle(sessions)

    buf = io.StringIO()
    configure_logging(stream=buf)

    async def handler(_: Ping) -> None:
        structlog.get_logger().info("handling")

    bus.subscribe(Ping, handler)
    await bus.publish(Ping(n=1, cycle_id=cycle_id))
    await bus.drain()

    lines = [json.loads(line) for line in buf.getvalue().splitlines()]
    assert [ln["cycle_id"] for ln in lines if ln["event"] == "handling"] == [cycle_id]


# --- lifecycle -----------------------------------------------------------------------


async def test_close_drains_pending_events(sessions: async_sessionmaker[AsyncSession]) -> None:
    bus = EventBus(sessions)
    got: list[int] = []

    async def handler(e: Ping) -> None:
        await asyncio.sleep(0)
        got.append(e.n)

    bus.subscribe(Ping, handler)
    for i in range(5):
        await bus.publish(Ping(n=i))
    await bus.close()

    assert got == [0, 1, 2, 3, 4]


async def test_publish_after_close_raises(sessions: async_sessionmaker[AsyncSession]) -> None:
    bus = EventBus(sessions)
    await bus.close()

    with pytest.raises(BusClosedError):
        await bus.publish(Ping(n=1))


def test_events_are_immutable() -> None:
    e = Ping(n=1)

    with pytest.raises(ValueError, match="frozen"):
        e.n = 2  # type: ignore[misc]


def test_naive_timestamp_rejected() -> None:
    with pytest.raises(ValueError, match="timezone"):
        Ping(n=1, ts=datetime(2026, 1, 1))


# --- regressions from the PR #6 review -------------------------------------------


class _PausedAppend:
    """Makes repo.append_event pause on a gate *after* its commit, for the first `n` calls.

    After the commit is the adversarial point: the event is durable but not yet delivered.
    """

    def __init__(self, monkeypatch: pytest.MonkeyPatch, n: int = 1) -> None:
        self.entered = asyncio.Event()
        self.gate = asyncio.Event()
        self._remaining = n
        real = repo.append_event

        async def paused(*args: Any, **kwargs: Any) -> m.Event:
            row = await real(*args, **kwargs)
            if self._remaining > 0:
                self._remaining -= 1
                self.entered.set()
                await self.gate.wait()
            return row

        monkeypatch.setattr(repo, "append_event", paused)


async def test_close_waits_for_in_flight_publish(
    sessions: async_sessionmaker[AsyncSession], monkeypatch: pytest.MonkeyPatch
) -> None:
    # P1: an admitted publish used to commit + enqueue after close() had stopped the
    # workers, so the event was persisted but never delivered.
    bus = EventBus(sessions)
    got: list[int] = []

    async def handler(e: Ping) -> None:
        got.append(e.n)

    bus.subscribe(Ping, handler)
    pause = _PausedAppend(monkeypatch)
    publishing = asyncio.create_task(bus.publish(Ping(n=1)))
    await pause.entered.wait()

    closing = asyncio.create_task(bus.close())
    for _ in range(20):
        await asyncio.sleep(0)
    assert not closing.done()  # must not finish while a publish is in flight

    pause.gate.set()
    await asyncio.wait_for(publishing, timeout=2)
    await asyncio.wait_for(closing, timeout=2)

    assert got == [1]
    assert len(await _events(sessions)) == 1


async def test_publish_waiting_during_close_is_rejected_before_persisting(
    sessions: async_sessionmaker[AsyncSession], monkeypatch: pytest.MonkeyPatch
) -> None:
    bus = EventBus(sessions)
    pause = _PausedAppend(monkeypatch)
    first = asyncio.create_task(bus.publish(Ping(n=1)))
    await pause.entered.wait()
    closing = asyncio.create_task(bus.close())
    late = asyncio.create_task(bus.publish(Ping(n=2)))
    for _ in range(20):
        await asyncio.sleep(0)

    pause.gate.set()
    await first
    await closing
    with pytest.raises(BusClosedError):
        await late

    assert [e.payload["n"] for e in await _events(sessions)] == [1]


async def test_concurrent_publishers_deliver_in_persisted_order(
    bus: EventBus, sessions: async_sessionmaker[AsyncSession], monkeypatch: pytest.MonkeyPatch
) -> None:
    # P2: publish 1 paused inside its DB append while publish 2 completed; handlers saw
    # [2, 1] although the event log had ids 1, 2.
    got: list[int] = []

    async def handler(e: Ping) -> None:
        got.append(e.n)

    bus.subscribe(Ping, handler)
    pause = _PausedAppend(monkeypatch)
    first = asyncio.create_task(bus.publish(Ping(n=1)))
    await pause.entered.wait()
    second = asyncio.create_task(bus.publish(Ping(n=2)))
    for _ in range(20):
        await asyncio.sleep(0)

    pause.gate.set()
    await asyncio.gather(first, second)
    await bus.drain()

    persisted = [e.payload["n"] for e in await _events(sessions)]
    assert got == persisted


async def test_handler_may_publish(bus: EventBus) -> None:
    # Publishing from inside a handler must not deadlock on the publish lock.
    pongs: list[str] = []

    async def on_ping(e: Ping) -> None:
        await bus.publish(Pong(text=f"re {e.n}"))

    async def on_pong(e: Pong) -> None:
        pongs.append(e.text)

    bus.subscribe(Ping, on_ping)
    bus.subscribe(Pong, on_pong)
    for i in range(3):
        await bus.publish(Ping(n=i))
    await asyncio.wait_for(bus.drain(), timeout=2)
    await asyncio.wait_for(bus.drain(), timeout=2)  # pongs published during first drain

    assert pongs == ["re 0", "re 1", "re 2"]


async def test_handler_failure_log_keeps_cycle_id(
    bus: EventBus, sessions: async_sessionmaker[AsyncSession]
) -> None:
    # P2: the failure line was logged after leaving bind_cycle(), so cycle_id was null.
    cycle_id = await _make_cycle(sessions)
    buf = io.StringIO()
    configure_logging(stream=buf)

    async def failing(_: Ping) -> None:
        raise RuntimeError("boom")

    bus.subscribe(Ping, failing)
    await bus.publish(Ping(n=1, cycle_id=cycle_id))
    await bus.drain()

    lines = [json.loads(line) for line in buf.getvalue().splitlines()]
    (failed,) = [ln for ln in lines if ln["event"] == "bus.handler_failed"]
    assert failed["cycle_id"] == cycle_id


async def test_queue_growth_past_high_water_is_logged_once(
    bus: EventBus, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("autotrader.core.bus.HIGH_WATER", 3)
    buf = io.StringIO()
    configure_logging(stream=buf)
    release = asyncio.Event()

    async def stuck(_: Ping) -> None:
        await release.wait()

    bus.subscribe(Ping, stuck)
    for i in range(6):
        await bus.publish(Ping(n=i))
    release.set()

    lines = [json.loads(line) for line in buf.getvalue().splitlines()]
    assert [ln["event"] for ln in lines].count("bus.queue_high_water") == 1
