import asyncio
import json
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.core.bus import BusClosedError, DomainEvent, EventBus
from autotrader.core.kill import (
    ControlGate,
    KillCleared,
    KillTriggered,
    clear_kill,
    trigger_kill,
    write_kill_file,
)
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade

NOW = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)


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
def kill_file(tmp_path: Path) -> Path:
    return tmp_path / "control" / "KILL"


@pytest.fixture
def gate(kill_file: Path, sessions: async_sessionmaker[AsyncSession]) -> ControlGate:
    return ControlGate(kill_file, sessions)


async def _set(sessions: async_sessionmaker[AsyncSession], key: str, value: object) -> None:
    async with sessions() as s:
        await repo.set_control(s, key, value)


# --- gate ----------------------------------------------------------------------------


async def test_clean_state_allows_entries(gate: ControlGate) -> None:
    status = await gate.status(NOW)

    assert status.entries_allowed
    assert status.reasons == ()
    assert not status.killed


async def test_touching_kill_file_blocks(gate: ControlGate, kill_file: Path) -> None:
    kill_file.parent.mkdir(parents=True)
    kill_file.touch()  # SPEC §0.6: a bare `touch KILL` is enough

    status = await gate.status(NOW)

    assert status.killed
    assert not status.entries_allowed
    assert any("kill file" in r for r in status.reasons)


async def test_db_kill_flag_blocks(
    gate: ControlGate, sessions: async_sessionmaker[AsyncSession]
) -> None:
    await _set(sessions, "kill", {"source": "api", "reason": "test", "ts": NOW.isoformat()})

    status = await gate.status(NOW)

    assert status.killed
    assert any("control_state" in r for r in status.reasons)


async def test_manual_pause_blocks(
    gate: ControlGate, sessions: async_sessionmaker[AsyncSession]
) -> None:
    await _set(sessions, "paused", True)

    status = await gate.status(NOW)

    assert not status.killed
    assert not status.entries_allowed
    assert any("paused" in r for r in status.reasons)


async def test_paused_until_blocks_until_it_passes(
    gate: ControlGate, sessions: async_sessionmaker[AsyncSession]
) -> None:
    await _set(sessions, "paused_until", (NOW + timedelta(hours=1)).isoformat())

    assert not (await gate.status(NOW)).entries_allowed
    assert (await gate.status(NOW + timedelta(hours=1))).entries_allowed


@pytest.mark.parametrize("value", ["tomorrow", "2026-10-10T00:00:00", 42])
async def test_malformed_paused_until_fails_closed(
    gate: ControlGate, sessions: async_sessionmaker[AsyncSession], value: object
) -> None:
    # Naive timestamps are malformed too: a pause must not hinge on a guessed zone.
    await _set(sessions, "paused_until", value)

    status = await gate.status(NOW)

    assert not status.entries_allowed
    assert any("paused_until" in r for r in status.reasons)


async def test_unreadable_control_state_fails_closed(tmp_path: Path, kill_file: Path) -> None:
    # A DB without the schema: reading control_state raises.
    engine = make_engine(f"sqlite+aiosqlite:///{tmp_path / 'empty.db'}")
    gate = ControlGate(kill_file, make_sessionmaker(engine))

    status = await gate.status(NOW)
    await engine.dispose()

    assert not status.entries_allowed
    assert any("control state unavailable" in r for r in status.reasons)


async def test_kill_file_is_checked_even_when_db_is_down(tmp_path: Path, kill_file: Path) -> None:
    kill_file.parent.mkdir(parents=True)
    kill_file.touch()
    engine = make_engine(f"sqlite+aiosqlite:///{tmp_path / 'empty.db'}")
    gate = ControlGate(kill_file, make_sessionmaker(engine))

    status = await gate.status(NOW)
    await engine.dispose()

    assert status.killed


def test_file_check_is_sync_and_db_free(kill_file: Path) -> None:
    # Called before every broker call; must not need the event loop or the DB.
    assert ControlGate.kill_file_present(kill_file) is False
    kill_file.parent.mkdir(parents=True)
    kill_file.touch()
    assert ControlGate.kill_file_present(kill_file) is True


# --- trigger / clear -----------------------------------------------------------------


async def test_trigger_kill_sets_file_db_and_event(
    gate: ControlGate, kill_file: Path, sessions: async_sessionmaker[AsyncSession]
) -> None:
    bus = EventBus(sessions)
    received: list[KillTriggered] = []

    async def on_kill(e: KillTriggered) -> None:
        received.append(e)

    bus.subscribe(KillTriggered, on_kill)
    result = await trigger_kill(kill_file, sessions, bus, source="cli", reason="test", now=NOW)
    await bus.close()

    assert result.db_error is None
    assert json.loads(kill_file.read_text()) == {
        "source": "cli",
        "reason": "test",
        "ts": NOW.isoformat(),
    }
    async with sessions() as s:
        assert (await repo.get_control(s, "kill"))["source"] == "cli"
        events = list(await s.scalars(select(m.Event)))
    assert [e.type for e in events] == ["KillTriggered"]
    assert [e.reason for e in received] == ["test"]
    assert (await gate.status(NOW)).killed


async def test_trigger_kill_writes_file_even_if_db_fails(tmp_path: Path, kill_file: Path) -> None:
    engine = make_engine(f"sqlite+aiosqlite:///{tmp_path / 'empty.db'}")
    sessions = make_sessionmaker(engine)

    result = await trigger_kill(kill_file, sessions, None, source="cli", reason=None, now=NOW)
    await engine.dispose()

    assert kill_file.exists()  # the kill is in effect regardless
    assert result.db_error is not None


async def test_clear_kill_removes_both_and_records_event(
    gate: ControlGate, kill_file: Path, sessions: async_sessionmaker[AsyncSession]
) -> None:
    bus = EventBus(sessions)
    await trigger_kill(kill_file, sessions, bus, source="cli", reason=None, now=NOW)

    await clear_kill(kill_file, sessions, bus, source="cli", now=NOW)
    await bus.close()

    assert not kill_file.exists()
    assert (await gate.status(NOW)).entries_allowed
    async with sessions() as s:
        types = [e.type for e in await s.scalars(select(m.Event).order_by(m.Event.id))]
    assert types == ["KillTriggered", "KillCleared"]


async def test_clear_kill_is_idempotent(
    kill_file: Path, sessions: async_sessionmaker[AsyncSession]
) -> None:
    await clear_kill(kill_file, sessions, None, source="cli", now=NOW)

    assert not kill_file.exists()


def test_events_carry_source() -> None:
    assert KillTriggered(source="file", reason=None).payload() == {"source": "file", "reason": None}
    assert KillCleared(source="cli").payload() == {"source": "cli"}


# --- regressions from the PR #7 review -------------------------------------------


async def test_failed_clear_keeps_a_file_only_kill(
    gate: ControlGate,
    kill_file: Path,
    sessions: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # P2: clear used to unlink the file before the DB write; a failing write then
    # silently lifted a `touch`-only kill.
    kill_file.parent.mkdir(parents=True)
    kill_file.touch()

    async def broken(*_: object, **__: object) -> None:
        raise RuntimeError("db is read-only")

    monkeypatch.setattr(repo, "set_control", broken)

    with pytest.raises(RuntimeError):
        await clear_kill(kill_file, sessions, None, source="cli", now=NOW)

    assert kill_file.exists()
    monkeypatch.undo()
    assert (await gate.status(NOW)).killed


def test_write_kill_file_needs_no_database(kill_file: Path) -> None:
    record = write_kill_file(kill_file, source="cli", reason="r", now=NOW)

    assert json.loads(kill_file.read_text()) == record
    assert record == {"source": "cli", "reason": "r", "ts": NOW.isoformat()}


# --- regressions from the PR #7 re-review ----------------------------------------


async def test_older_clear_never_removes_a_newer_kill(
    gate: ControlGate,
    kill_file: Path,
    sessions: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # P1: a clear paused in its DB/audit step; meanwhile a new kill latched its file (its
    # DB write failed, the documented degraded mode). The old clear then unlinked it.
    kill_file.parent.mkdir(parents=True)
    kill_file.touch()
    bus = EventBus(sessions)
    entered, gate_open = asyncio.Event(), asyncio.Event()
    real_publish = bus.publish

    async def paused_publish(event: DomainEvent) -> None:
        await real_publish(event)
        entered.set()
        await gate_open.wait()

    monkeypatch.setattr(bus, "publish", paused_publish)
    clearing = asyncio.create_task(clear_kill(kill_file, sessions, bus, source="cli", now=NOW))
    await entered.wait()

    write_kill_file(kill_file, source="cli", reason="new emergency", now=NOW)
    gate_open.set()
    result = await clearing
    await bus.close()

    assert result.file_removed is False
    assert json.loads(kill_file.read_text())["reason"] == "new emergency"
    assert (await gate.status(NOW)).killed


async def test_failed_audit_keeps_a_db_only_kill(
    gate: ControlGate, kill_file: Path, sessions: async_sessionmaker[AsyncSession]
) -> None:
    # P2: DB-only kill, KillCleared fails to persist -> the DB flag was already gone and
    # there was no file, so the clear raised but trading resumed.
    await _set(sessions, "kill", {"source": "api", "reason": "x", "ts": NOW.isoformat()})
    bus = EventBus(sessions)
    await bus.close()  # publish now raises BusClosedError

    with pytest.raises(BusClosedError):
        await clear_kill(kill_file, sessions, bus, source="cli", now=NOW)

    assert (await gate.status(NOW)).killed


async def test_successful_clear_of_db_only_kill_leaves_no_file(
    gate: ControlGate, kill_file: Path, sessions: async_sessionmaker[AsyncSession]
) -> None:
    await _set(sessions, "kill", {"source": "api", "reason": "x", "ts": NOW.isoformat()})

    result = await clear_kill(kill_file, sessions, None, source="cli", now=NOW)

    assert result.file_removed is True
    assert not kill_file.exists()
    assert (await gate.status(NOW)).entries_allowed


def test_kill_file_writes_leave_no_temp_files(kill_file: Path) -> None:
    for i in range(3):
        write_kill_file(kill_file, source="cli", reason=str(i), now=NOW)

    names = sorted(p.name for p in kill_file.parent.iterdir())
    assert names == ["KILL", "KILL.lock"]
