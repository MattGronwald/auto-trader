import asyncio
import io
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from pathlib import Path

import pytest
import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.core.bus import BusClosedError, DomainEvent, EventBus
from autotrader.core.logging import configure_logging
from autotrader.journal import models as m
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


async def test_handler_logs_carry_the_event_cycle_id(
    bus: EventBus, sessions: async_sessionmaker[AsyncSession]
) -> None:
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
        cycle_id = cycle.id

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
