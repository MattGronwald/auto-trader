"""In-process event bus (SPEC §4.11).

`publish()` persists the event to the append-only `events` table first, then hands it
to subscribers. If persisting fails, `publish()` raises and nothing is delivered, so the
audit trail never misses an event a handler acted on.

One lock covers "check closed -> persist -> enqueue", which gives a single global
order: every subscriber sees events in `events.id` order, the same order a dashboard
replay of the table shows. `close()` takes the same lock before marking the bus
closed, so a publish already in progress completes (persisted *and* enqueued) before
shutdown drains the queues, and a later publish is rejected before it persists.

Each subscription has its own unbounded queue and worker task: a slow handler (e.g. a
cycle waiting on an LLM call) cannot block the publisher or other subscribers. Queues
are unbounded on purpose: with the publish lock, a bounded queue could deadlock when a
handler publishes while the lock holder waits for room in that handler's queue. Growth
past `HIGH_WATER` is logged. A handler that raises is logged (with the event's
`cycle_id`) and skipped; it does not stop its worker. Handlers may publish; after
`close()` such a publish raises `BusClosedError` like any other.

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

HIGH_WATER = 1_000  # queued events per subscriber before warning


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
    def __init__(self, sessions: async_sessionmaker[AsyncSession]):
        self._sessions = sessions
        self._subs: list[_Subscription] = []
        self._closed = False
        self._lock = asyncio.Lock()

    def subscribe[E: DomainEvent](
        self, event_type: type[E], handler: Callable[[E], Awaitable[None]]
    ) -> None:
        """Deliver every published `event_type` (incl. subclasses) to `handler`.

        Must be called from within the running event loop.
        """
        sub = _Subscription(event_type, cast(Handler, handler), asyncio.Queue())
        sub.task = asyncio.create_task(self._worker(sub), name=f"bus:{_name(handler)}")
        self._subs.append(sub)

    async def publish(self, event: DomainEvent) -> None:
        event_type = type(event).__name__
        async with self._lock:
            if self._closed:
                raise BusClosedError("event bus is closed")
            async with self._sessions() as session:
                row = await repo.append_event(
                    session, event_type, event.payload(), cycle_id=event.cycle_id, ts=event.ts
                )
            for sub in self._subs:
                if isinstance(event, sub.event_type):
                    sub.queue.put_nowait(event)
                    if sub.queue.qsize() == HIGH_WATER:
                        log.warning(
                            "bus.queue_high_water",
                            handler=_name(sub.handler),
                            queued=HIGH_WATER,
                        )
        log.debug("bus.published", event_type=event_type, event_id=row.id)

    async def drain(self) -> None:
        """Wait until every subscriber has processed everything published so far."""
        await asyncio.gather(*(sub.queue.join() for sub in self._subs))

    async def close(self) -> None:
        """Stop accepting events, let subscribers finish the backlog, stop workers."""
        async with self._lock:  # waits for an in-flight publish to persist + enqueue
            self._closed = True
        await self.drain()
        tasks = [sub.task for sub in self._subs if sub.task is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _worker(self, sub: _Subscription) -> None:
        while True:
            event = await sub.queue.get()
            with bind_cycle(event.cycle_id):  # also covers the failure log line
                try:
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
