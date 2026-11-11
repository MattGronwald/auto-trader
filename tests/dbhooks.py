"""Commit-boundary fault injection for tests (WP 1.4, D15).

`hooked_sessionmaker` returns sessions whose `commit()` consults a `CommitHook`:
- `fail_before = n`: the next n commits raise *without* committing (a commit that may
  still land later, from the caller's point of view);
- `fail_after = n`: the next n commits *do* commit, then raise (a lost acknowledgement);
- `block_after = event`: the next commit commits, sets `reached`, then waits for `event`
  (the caller can be cancelled between the DB commit and its confirmation);
- `block_before = event`: the next commit sets `reached` and waits for `event` *before*
  committing (holding the bus lock); `fail_before` then applies as usual;
- `block_before_update = event`: the next `execute` of an `UPDATE` on `orders` sets
  `update_reached` and waits for `event` *before* executing (after any read the unit did,
  before its first write, so no write lock is held while it waits).
"""

import asyncio
from typing import Any

from sqlalchemy import Table, Update
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
        self.block_before_update: asyncio.Event | None = None
        self.update_reached = asyncio.Event()

    async def before_update(self, statement: object) -> None:
        if self.block_before_update is None:
            return
        if not isinstance(statement, Update):
            return
        table = statement.table
        if not (isinstance(table, Table) and table.name == "orders"):
            return
        release, self.block_before_update = self.block_before_update, None
        self.update_reached.set()
        await release.wait()

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

        async def execute(self, statement: Any, *args: Any, **kwargs: Any) -> Any:
            await hook.before_update(statement)
            return await super().execute(statement, *args, **kwargs)

    return async_sessionmaker(engine, class_=HookedSession, expire_on_commit=False)
