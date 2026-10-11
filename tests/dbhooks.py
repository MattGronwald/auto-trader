"""Commit-boundary fault injection for tests (WP 1.4, D15).

`hooked_sessionmaker` returns sessions whose `commit()` consults a `CommitHook`:
- `fail_before = n`: the next n commits raise *without* committing (a commit that may
  still land later, from the caller's point of view);
- `fail_after = n`: the next n commits *do* commit, then raise (a lost acknowledgement);
- `block_after = event`: the next commit commits, sets `reached`, then waits for `event`
  (the caller can be cancelled between the DB commit and its confirmation);
- `block_before = event`: the next commit sets `reached` and waits for `event` *before*
  committing (holding the bus lock); `fail_before` then applies as usual.
"""

import asyncio

from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker


def lost_ack() -> OperationalError:
    return OperationalError("COMMIT", None, Exception("connection lost during COMMIT"))


class CommitHook:
    def __init__(self) -> None:
        self.fail_before = 0
        self.fail_after = 0
        self.block_after: asyncio.Event | None = None
        self.block_before: asyncio.Event | None = None
        self.reached = asyncio.Event()

    async def before(self) -> None:
        if self.block_before is not None:
            release, self.block_before = self.block_before, None
            self.reached.set()
            await release.wait()
        if self.fail_before:
            self.fail_before -= 1
            raise lost_ack()

    async def after(self) -> None:
        if self.block_after is not None:
            release, self.block_after = self.block_after, None
            self.reached.set()
            await release.wait()
        if self.fail_after:
            self.fail_after -= 1
            raise lost_ack()


def hooked_sessionmaker(engine: AsyncEngine, hook: CommitHook) -> async_sessionmaker[AsyncSession]:
    class HookedSession(AsyncSession):
        async def commit(self) -> None:
            await hook.before()
            await super().commit()
            await hook.after()

    return async_sessionmaker(engine, class_=HookedSession, expire_on_commit=False)
