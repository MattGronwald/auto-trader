import asyncio
import io
import json
from collections.abc import AsyncIterator, Awaitable, Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pytest
import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog.testing import capture_logs

from autotrader.core.bus import BusClosedError, CommitUncertain, DomainEvent, EventBus
from autotrader.core.logging import configure_logging
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade
from tests.dbhooks import CommitHook, hooked_sessionmaker

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


# --- publish_tx (WP 1.4, D15) --------------------------------------------------------


class Note(DomainEvent):
    key: str


def note_build(
    key: str,
    *,
    skip: bool = False,
    fail: bool = False,
    hold: asyncio.Event | None = None,
    entered: asyncio.Event | None = None,
) -> Callable[[AsyncSession], Awaitable[DomainEvent | None]]:
    """Writes a control_state row (stand-in for the cycle row) and returns its event."""

    async def build(session: AsyncSession) -> DomainEvent | None:
        session.add(m.ControlState(key=key, value=True))
        await session.flush()
        if entered is not None:
            entered.set()
        if hold is not None:
            await hold.wait()
        if fail:
            raise RuntimeError("build failed")
        return None if skip else Note(key=key)

    return build


async def note_visible(session: AsyncSession, event: DomainEvent) -> bool:
    return isinstance(event, Note) and await session.get(m.ControlState, event.key) is not None


async def note_invisible(session: AsyncSession, event: DomainEvent) -> bool:
    return False


async def has_control(sessions: async_sessionmaker[AsyncSession], key: str) -> bool:
    async with sessions() as s:
        return await s.get(m.ControlState, key) is not None


@pytest.fixture
def hook() -> CommitHook:
    return CommitHook()


@pytest.fixture
async def hbus(db_url: str, hook: CommitHook) -> AsyncIterator[EventBus]:
    engine = make_engine(db_url)
    b = EventBus(hooked_sessionmaker(engine, hook))
    yield b
    await b.close()
    await engine.dispose()


def collect(bus: EventBus) -> list[str]:
    seen: list[str] = []

    async def handler(e: DomainEvent) -> None:
        seen.append(e.key if isinstance(e, Note) else f"{type(e).__name__}")

    bus.subscribe(DomainEvent, handler)
    return seen


async def test_publish_tx_commits_write_and_event_together_and_delivers(
    bus: EventBus, sessions: async_sessionmaker[AsyncSession]
) -> None:
    seen = collect(bus)
    out = await bus.publish_tx(note_build("a"), note_visible)
    await bus.drain()

    assert isinstance(out, Note) and out.key == "a"
    assert seen == ["a"]
    assert await has_control(sessions, "a")
    assert [r.type for r in await _events(sessions)] == ["Note"]


async def test_publish_tx_none_writes_and_delivers_nothing(
    bus: EventBus, sessions: async_sessionmaker[AsyncSession]
) -> None:
    seen = collect(bus)
    assert await bus.publish_tx(note_build("a", skip=True), note_visible) is None
    await bus.drain()
    assert seen == []
    assert not await has_control(sessions, "a")
    assert await _events(sessions) == []


async def test_publish_tx_build_error_rolls_back_and_propagates(
    bus: EventBus, sessions: async_sessionmaker[AsyncSession]
) -> None:
    with pytest.raises(RuntimeError, match="build failed"):
        await bus.publish_tx(note_build("a", fail=True), note_visible)
    assert not await has_control(sessions, "a")
    assert await _events(sessions) == []


async def test_plain_publish_waits_for_a_transactional_unit_and_order_is_kept(
    bus: EventBus, sessions: async_sessionmaker[AsyncSession]
) -> None:
    seen = collect(bus)
    hold, entered = asyncio.Event(), asyncio.Event()
    unit = asyncio.create_task(
        bus.publish_tx(note_build("a", hold=hold, entered=entered), note_visible)
    )
    await entered.wait()
    plain = asyncio.create_task(bus.publish(Ping(n=1)))
    for _ in range(20):
        await asyncio.sleep(0)
    assert not plain.done()  # the unit holds the bus lock

    hold.set()
    await asyncio.gather(unit, plain)
    await bus.drain()
    assert [r.type for r in await _events(sessions)] == ["Note", "Ping"]
    assert seen == ["a", "Ping"]


async def test_cancelled_caller_does_not_stop_the_unit(
    bus: EventBus, sessions: async_sessionmaker[AsyncSession]
) -> None:
    seen = collect(bus)
    hold, entered = asyncio.Event(), asyncio.Event()
    caller = asyncio.create_task(
        bus.publish_tx(note_build("a", hold=hold, entered=entered), note_visible)
    )
    await entered.wait()
    caller.cancel()
    hold.set()
    with pytest.raises(asyncio.CancelledError):
        await caller
    await bus.close()  # waits for the unit, then drains
    assert seen == ["a"]
    assert await has_control(sessions, "a")


async def test_caller_cancelled_after_db_commit_before_confirmation(
    hbus: EventBus, hook: CommitHook, sessions: async_sessionmaker[AsyncSession]
) -> None:
    seen = collect(hbus)
    release = asyncio.Event()
    hook.block_after = release
    caller = asyncio.create_task(hbus.publish_tx(note_build("a"), note_visible))
    await hook.reached.wait()  # the DB has committed; the unit hasn't confirmed yet
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    release.set()
    await hbus.close()
    assert seen == ["a"]  # exactly once
    assert await has_control(sessions, "a")
    assert [r.type for r in await _events(sessions)] == ["Note"]


async def test_commit_error_with_positive_read_back_is_recovered(
    hbus: EventBus, hook: CommitHook, sessions: async_sessionmaker[AsyncSession]
) -> None:
    seen = collect(hbus)
    hook.fail_after = 1
    with capture_logs() as logs:
        out = await hbus.publish_tx(note_build("a"), note_visible)
    await hbus.drain()
    assert isinstance(out, Note)
    assert seen == ["a"]
    assert any(e["event"] == "bus.commit_recovered" for e in logs)


@pytest.mark.parametrize("verify_raises", [False, True])
async def test_commit_error_without_confirmation_is_uncertain(
    verify_raises: bool,
    hbus: EventBus,
    hook: CommitHook,
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    seen = collect(hbus)
    flagged: list[CommitUncertain] = []

    async def broken(session: AsyncSession, event: DomainEvent) -> bool:
        raise RuntimeError("read-back failed")

    hook.fail_before = 1  # nothing committed, but the caller can't know that
    with pytest.raises(CommitUncertain):
        await hbus.publish_tx(
            note_build("a"),
            broken if verify_raises else note_invisible,
            on_uncertain=flagged.append,
        )
    await hbus.drain()
    assert seen == []
    assert len(flagged) == 1


async def test_on_uncertain_fires_even_when_the_caller_is_gone(
    hbus: EventBus, hook: CommitHook
) -> None:
    flagged: list[CommitUncertain] = []
    hold, entered = asyncio.Event(), asyncio.Event()
    hook.fail_before = 1
    caller = asyncio.create_task(
        hbus.publish_tx(
            note_build("a", hold=hold, entered=entered),
            note_invisible,
            on_uncertain=flagged.append,
        )
    )
    await entered.wait()
    caller.cancel()
    hold.set()
    with pytest.raises(asyncio.CancelledError):
        await caller
    await hbus.close()
    assert len(flagged) == 1


async def test_close_finishes_an_in_flight_unit_then_refuses(
    bus: EventBus, sessions: async_sessionmaker[AsyncSession]
) -> None:
    seen = collect(bus)
    hold, entered = asyncio.Event(), asyncio.Event()
    unit = asyncio.create_task(
        bus.publish_tx(note_build("a", hold=hold, entered=entered), note_visible)
    )
    await entered.wait()
    closing = asyncio.create_task(bus.close(budget_s=5))
    for _ in range(20):
        await asyncio.sleep(0)
    hold.set()
    await asyncio.gather(unit, closing)
    assert seen == ["a"]
    with pytest.raises(BusClosedError):
        await bus.publish_tx(note_build("b"), note_visible)
    with pytest.raises(BusClosedError):
        await bus.publish(Ping(n=2))


async def test_close_is_bounded_and_reports_what_it_abandons(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    bus = EventBus(sessions)
    hold, entered = asyncio.Event(), asyncio.Event()
    unit = asyncio.create_task(
        bus.publish_tx(note_build("a", hold=hold, entered=entered), note_visible)
    )
    await entered.wait()
    with capture_logs() as logs:
        await bus.close(budget_s=0.01)  # the unit never finishes within the budget
    assert any(e["event"] == "bus.task_abandoned" for e in logs)
    # The abandoned unit still holds the bus lock: a late publish must fail fast.
    with pytest.raises(BusClosedError):
        await asyncio.wait_for(bus.publish(Ping(n=1)), timeout=5)  # safety net only
    with pytest.raises(BusClosedError):
        await asyncio.wait_for(bus.publish_tx(note_build("b"), note_visible), timeout=5)
    hold.set()  # clean up: let the abandoned unit finish
    await unit


async def test_unit_failure_with_a_gone_caller_is_logged(hbus: EventBus) -> None:
    hold, entered = asyncio.Event(), asyncio.Event()
    with capture_logs() as logs:
        caller = asyncio.create_task(
            hbus.publish_tx(note_build("a", hold=hold, entered=entered, fail=True), note_visible)
        )
        await entered.wait()
        caller.cancel()
        hold.set()
        with pytest.raises(asyncio.CancelledError):
            await caller
        await hbus.close()  # waits for the unit; its done-callback runs first
    (failed,) = [e for e in logs if e["event"] == "bus.unit_failed"]
    assert failed["log_level"] == "error"
    assert "build failed" in failed["error"]


async def test_commit_error_rolls_back_before_the_read_back(
    hbus: EventBus, hook: CommitHook, monkeypatch: pytest.MonkeyPatch
) -> None:
    order: list[str] = []
    real_rollback = AsyncSession.rollback

    async def spy_rollback(self: AsyncSession) -> None:
        order.append("rollback")
        await real_rollback(self)

    async def spy_verify(session: AsyncSession, event: DomainEvent) -> bool:
        order.append("verify")
        return await note_visible(session, event)

    monkeypatch.setattr(AsyncSession, "rollback", spy_rollback)
    hook.fail_after = 1  # committed, acknowledgement lost
    out = await hbus.publish_tx(note_build("a"), spy_verify)
    assert isinstance(out, Note)
    assert order == ["rollback", "verify"]


async def test_a_failing_rollback_after_a_commit_error_is_logged_and_the_read_back_still_runs(
    hbus: EventBus, hook: CommitHook, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def broken_rollback(self: AsyncSession) -> None:
        raise RuntimeError("rollback failed")

    monkeypatch.setattr(AsyncSession, "rollback", broken_rollback)
    hook.fail_after = 1
    with capture_logs() as logs:
        out = await hbus.publish_tx(note_build("a"), note_visible)
    assert isinstance(out, Note)
    assert any(e["event"] == "bus.rollback_failed" for e in logs)


async def test_publish_tx_commits_several_events_atomically_in_order(
    bus: EventBus, sessions: async_sessionmaker[AsyncSession]
) -> None:
    got: list[DomainEvent] = []

    async def handler(event: DomainEvent) -> None:
        got.append(event)

    bus.subscribe(DomainEvent, handler)

    async def build(session: AsyncSession) -> tuple[DomainEvent, ...]:
        session.add(m.ControlState(key="k", value=1))
        return (Ping(ts=T0, n=1), Pong(ts=T0, text="two"))

    async def verify(session: AsyncSession, event: DomainEvent) -> bool:
        return False

    out = await bus.publish_tx(build, verify)
    await bus.drain()
    assert isinstance(out, Ping) and out.n == 1
    assert [e.type for e in await _events(sessions)] == ["Ping", "Pong"]
    assert [type(e).__name__ for e in got] == ["Ping", "Pong"]
    async with sessions() as s:
        assert await repo.get_control(s, "k") == 1


async def test_publish_tx_empty_tuple_writes_and_delivers_nothing(
    bus: EventBus, sessions: async_sessionmaker[AsyncSession]
) -> None:
    async def build(session: AsyncSession) -> tuple[DomainEvent, ...]:
        session.add(m.ControlState(key="k", value=1))
        return ()

    async def verify(session: AsyncSession, event: DomainEvent) -> bool:
        return False

    assert await bus.publish_tx(build, verify) is None
    assert await _events(sessions) == []
    async with sessions() as s:
        assert await repo.get_control(s, "k") is None
