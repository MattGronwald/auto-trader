"""Flatten request/result protocol (WP 1.5c, spec 4.7) on a migrated SQLite journal."""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
import structlog
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from autotrader.core.flatten import (
    FLATTEN_REQUESTED_KEY,
    FLATTEN_RESULT_KEY,
    FlattenRequest,
    cancel_flatten,
    lock_flatten_state,
    read_request,
    read_result,
    request_flatten,
    wait_for_result,
)
from autotrader.core.kill import PAUSED_KEY
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade

T0 = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)
T1 = T0 + timedelta(seconds=5)


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


async def test_a_request_sets_the_pause_and_keeps_a_pending_one(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    first, created = await request_flatten(sessions, source="cli", reason="r", now=T0)
    again, created_again = await request_flatten(sessions, source="shutdown", reason=None, now=T1)
    assert (created, created_again, again) == (True, False, first)
    assert first == FlattenRequest(source="cli", reason="r", ts=T0.isoformat())
    async with sessions() as s:
        assert await repo.get_control(s, PAUSED_KEY) is True
        assert await read_request(s) == first


async def test_cancel_clears_the_request_and_keeps_the_pause(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    req, _ = await request_flatten(sessions, source="cli", reason=None, now=T0)
    assert await cancel_flatten(sessions, now=T1) == req
    assert await cancel_flatten(sessions, now=T1) is None
    async with sessions() as s:
        assert await read_request(s) is None
        assert await repo.get_control(s, FLATTEN_REQUESTED_KEY) is None
        assert await read_result(s) == {
            "request_ts": req.ts,
            "flat": False,
            "cancelled": True,
            "ts": T1.isoformat(),
        }
        assert await repo.get_control(s, PAUSED_KEY) is True


async def test_wait_returns_the_matching_final_result(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    req, _ = await request_flatten(sessions, source="cli", reason=None, now=T0)
    async with sessions() as s:
        await repo.set_control(s, FLATTEN_RESULT_KEY, {"request_ts": "other", "flat": True})
    assert await wait_for_result(sessions, req.ts, wait_s=0.0, poll_s=0.0) is None

    async def complete() -> None:
        await asyncio.sleep(0)
        async with sessions() as s:
            await repo.set_control(s, FLATTEN_RESULT_KEY, {"request_ts": req.ts, "flat": True})

    waiter = asyncio.create_task(wait_for_result(sessions, req.ts, wait_s=5.0, poll_s=0.0))
    await complete()
    assert await waiter == {"request_ts": req.ts, "flat": True}


async def test_wait_returns_the_last_non_final_result_at_the_deadline(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    result = {"request_ts": "x", "flat": False}
    async with sessions() as s:
        await repo.set_control(s, FLATTEN_RESULT_KEY, result)
    assert await wait_for_result(sessions, "x", wait_s=0.01, poll_s=0.0) == result


async def test_the_lock_first_write_blocks_other_writers_until_commit(
    sessions: async_sessionmaker[AsyncSession], db_url: str
) -> None:
    other = create_async_engine(db_url, connect_args={"timeout": 0.05})  # a second process
    try:
        async with sessions() as s:
            await lock_flatten_state(s)  # holds SQLite's write lock, uncommitted
            with pytest.raises(OperationalError, match="locked"):
                async with async_sessionmaker(other)() as o:
                    await repo.set_control(o, FLATTEN_REQUESTED_KEY, None)
            await s.commit()
        async with async_sessionmaker(other)() as o:  # released: the write goes through
            await repo.set_control(o, FLATTEN_REQUESTED_KEY, None)
    finally:
        await other.dispose()


async def test_wait_never_sleeps_past_its_deadline(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    loop = asyncio.get_running_loop()
    start = loop.time()
    assert await wait_for_result(sessions, "none", wait_s=0.01, poll_s=10.0) is None
    assert loop.time() - start < 1.0  # far below poll_s; not a timing check on the deadline


async def test_a_malformed_request_reads_as_none(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    async with sessions() as s:
        await repo.set_control(s, FLATTEN_REQUESTED_KEY, ["junk"])
        with structlog.testing.capture_logs() as logs:
            assert await read_request(s) is None
    assert [e["event"] for e in logs] == ["flatten.request_malformed"]
    assert logs[0]["log_level"] == "error"
