"""Flatten request/result protocol in `control_state` (WP 1.5c, spec 4.7).

A request is the JSON value under `flatten_requested`; its `ts` is the request's identity.
The core's Position Manager works the request off and, on completion, clears it (JSON null,
never a deleted row) and writes `flatten_result` carrying the same `request_ts`. A pending
request is kept, never replaced: a second asker joins it and waits for its result.
`cancel_flatten` clears the request and writes a cancelled result; it never lifts the pause,
which only `resume` does.

Every read-check-write of this state starts with `lock_flatten_state`: on SQLite the write
lock is otherwise only taken at the first DML, after the guard's SELECT (clarification 16).
"""

import asyncio
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import structlog
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.core.kill import PAUSED_KEY
from autotrader.journal import repo
from autotrader.journal.models import ControlState

FLATTEN_REQUESTED_KEY = "flatten_requested"
FLATTEN_RESULT_KEY = "flatten_result"

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class FlattenRequest:
    source: str
    reason: str | None
    ts: str  # ISO 8601; the request's identity

    def as_json(self) -> dict[str, str | None]:
        return {"source": self.source, "reason": self.reason, "ts": self.ts}


def parse_request(value: object) -> FlattenRequest | None:
    if value is None:
        return None
    if (
        isinstance(value, dict)
        and isinstance(value.get("source"), str)
        and isinstance(value.get("ts"), str)
        and isinstance(value.get("reason"), str | None)
    ):
        return FlattenRequest(source=value["source"], reason=value["reason"], ts=value["ts"])
    log.error("flatten.request_malformed", value=repr(value))
    return None


async def read_request(session: AsyncSession) -> FlattenRequest | None:
    return parse_request(await repo.get_control(session, FLATTEN_REQUESTED_KEY))


async def read_result(session: AsyncSession) -> dict[str, Any] | None:
    value = await repo.get_control(session, FLATTEN_RESULT_KEY)
    return value if isinstance(value, dict) else None


async def lock_flatten_state(session: AsyncSession) -> None:
    """Take SQLite's write lock before the caller's guard reads; does not commit."""
    await session.execute(
        update(ControlState)
        .where(ControlState.key == FLATTEN_REQUESTED_KEY)
        .values(key=ControlState.key)
    )


async def request_flatten(
    sessions: async_sessionmaker[AsyncSession],
    *,
    source: str,
    reason: str | None,
    now: datetime,
) -> tuple[FlattenRequest, bool]:
    """Pause and request a flatten, unless one is pending. Returns (request, created)."""
    async with sessions() as session:
        await lock_flatten_state(session)
        pending = await read_request(session)
        if pending is not None:
            await session.merge(ControlState(key=PAUSED_KEY, value=True))
            await session.commit()
            return pending, False
        request = FlattenRequest(source=source, reason=reason, ts=now.isoformat())
        await session.merge(ControlState(key=PAUSED_KEY, value=True))
        await session.merge(ControlState(key=FLATTEN_REQUESTED_KEY, value=request.as_json()))
        await session.commit()
        return request, True


async def cancel_flatten(
    sessions: async_sessionmaker[AsyncSession], *, now: datetime
) -> FlattenRequest | None:
    """Clear the pending request and record a cancelled result; the pause stays."""
    async with sessions() as session:
        await lock_flatten_state(session)
        pending = await read_request(session)
        if pending is None:
            await session.rollback()
            return None
        await session.merge(ControlState(key=FLATTEN_REQUESTED_KEY, value=None))
        await session.merge(
            ControlState(
                key=FLATTEN_RESULT_KEY,
                value={
                    "request_ts": pending.ts,
                    "flat": False,
                    "cancelled": True,
                    "ts": now.isoformat(),
                },
            )
        )
        await session.commit()
        return pending


async def wait_for_result(
    sessions: async_sessionmaker[AsyncSession],
    request_ts: str,
    *,
    wait_s: float,
    poll_s: float = 0.5,
) -> dict[str, Any] | None:
    """Poll until the request's result is flat or cancelled, or `wait_s` runs out.

    Returns the last result seen for `request_ts`, or None. Never sleeps past the deadline.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + wait_s
    seen: dict[str, Any] | None = None
    while True:
        async with sessions() as session:
            result = await read_result(session)
        if result is not None and result.get("request_ts") == request_ts:
            seen = result
            if result.get("flat") or result.get("cancelled"):
                return result
        remaining = deadline - loop.time()
        if remaining <= 0:
            return seen
        await asyncio.sleep(min(poll_s, remaining))
