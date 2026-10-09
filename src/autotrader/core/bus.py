"""In-process event bus (SPEC §4.11).

`publish()` persists the event to the append-only `events` table first, then hands it
to subscribers. If persisting fails, `publish()` raises and nothing is delivered, so the
audit trail never misses an event a handler acted on.

Each subscription has its own queue and worker task: a slow handler (e.g. a cycle
waiting on an LLM call) cannot block the publisher or other subscribers, and per
subscriber the delivery order is the publish order. A handler that raises is logged
and skipped; it does not stop its worker.

Subscriptions match by class: subscribing to `DomainEvent` receives every event.
"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import cast

import structlog
from pydantic import AwareDatetime, BaseModel, ConfigDict, Field
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.core.logging import bind_cycle
from autotrader.journal import repo

log = structlog.get_logger(__name__)


class DomainEvent(BaseModel):
    """Base for all events. The class name is the persisted `events.type`."""

    model_config = ConfigDict(frozen=True)

    ts: AwareDatetime = Field(default_factory=lambda: datetime.now(UTC))
    cycle_id: int | None = None

    def payload(self) -> dict[str, object]:
        return self.model_dump(mode="json", exclude={"ts", "cycle_id"})


Handler = Callable[[DomainEvent], Awaitable[None]]


class BusClosedError(RuntimeError):
    pass


@dataclass
class _Subscription:
    event_type: type[DomainEvent]
    handler: Handler
    queue: asyncio.Queue[DomainEvent]
    task: asyncio.Task[None] | None = None


class EventBus:
    def __init__(self, sessions: async_sessionmaker[AsyncSession], maxsize: int = 10_000):
        self._sessions = sessions
        self._maxsize = maxsize
        self._subs: list[_Subscription] = []
        self._closed = False

    def subscribe[E: DomainEvent](
        self, event_type: type[E], handler: Callable[[E], Awaitable[None]]
    ) -> None:
        """Deliver every published `event_type` (incl. subclasses) to `handler`.

        Must be called from within the running event loop.
        """
        sub = _Subscription(event_type, cast(Handler, handler), asyncio.Queue(self._maxsize))
        sub.task = asyncio.create_task(self._worker(sub), name=f"bus:{_name(handler)}")
        self._subs.append(sub)

    async def publish(self, event: DomainEvent) -> None:
        if self._closed:
            raise BusClosedError("event bus is closed")
        event_type = type(event).__name__
        async with self._sessions() as session:
            row = await repo.append_event(
                session, event_type, event.payload(), cycle_id=event.cycle_id, ts=event.ts
            )
        log.debug("bus.published", event_type=event_type, event_id=row.id)
        for sub in self._subs:
            if isinstance(event, sub.event_type):
                await sub.queue.put(event)  # full queue = backpressure on the publisher

    async def drain(self) -> None:
        """Wait until every subscriber has processed everything published so far."""
        await asyncio.gather(*(sub.queue.join() for sub in self._subs))

    async def close(self) -> None:
        """Stop accepting events, let subscribers finish the backlog, stop workers."""
        self._closed = True
        await self.drain()
        tasks = [sub.task for sub in self._subs if sub.task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _worker(self, sub: _Subscription) -> None:
        while True:
            event = await sub.queue.get()
            try:
                with bind_cycle(event.cycle_id):
                    await sub.handler(event)
            except Exception:
                log.exception(
                    "bus.handler_failed",
                    event_type=type(event).__name__,
                    handler=_name(sub.handler),
                )
            finally:
                sub.queue.task_done()


def _name(handler: Callable[..., object]) -> str:
    return getattr(handler, "__qualname__", repr(handler))
