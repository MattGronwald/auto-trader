"""In-process event bus (SPEC §4.11).

`publish()` persists the event to the append-only `events` table first, then hands it
to subscribers. If persisting fails, `publish()` raises and nothing is delivered, so the
audit trail never misses an event a handler acted on.

One lock covers "check closed -> persist -> enqueue", which gives a single global
order: every subscriber sees events in `events.id` order, the same order a dashboard
replay of the table shows. `close()` marks the bus closed and then waits for the lock
(within its budget), so a publish already in progress completes (persisted *and*
enqueued) before shutdown drains the queues, and a later publish is rejected before it
persists.

Each subscription has its own unbounded queue and worker task: a slow handler (e.g. a
cycle waiting on an LLM call) cannot block the publisher or other subscribers. Queues
are unbounded on purpose: with the publish lock, a bounded queue could deadlock when a
handler publishes while the lock holder waits for room in that handler's queue. Growth
past `HIGH_WATER` is logged. A handler that raises is logged (with the event's
`cycle_id`) and skipped; it does not stop its worker. Handlers may publish; after
`close()` such a publish raises `BusClosedError` like any other.

`publish_tx(build, verify, on_uncertain=)` (WP 1.4, D15) commits a caller's writes and
their event(s) in one transaction: under the same lock, `build(session)` writes without
committing and returns the event, a tuple of events (all committed in the same
transaction, verified by the first; WP 1.5b), or `None`/`()`: roll back, nothing to do;
the bus appends the event(s), commits once and enqueues. The unit runs in a bus-owned
task the caller awaits through `asyncio.shield`, so cancelling the caller never
interrupts a commit or loses a committed event's delivery. A commit that raises is not
proof of rollback: the bus asks `verify` once in a fresh session; only a positive answer
confirms (deliver), anything else raises `CommitUncertain` after calling `on_uncertain`
from the unit itself. Delivery is exactly once for confirmed commits; an uncertain or
abandoned unit may be persisted without delivery (the `events` table stays the record).

`close(budget_s)` is bounded: it marks the bus closed, waits for in-flight publishes
and units and for the subscriber backlog until the deadline, then stops the workers.
Anything still running at the deadline is logged `bus.task_abandoned` and left to
process exit (the process-level hard deadline, WP 1.5).

Subscriptions match by class: subscribing to `DomainEvent` receives every event.
"""

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, cast

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


class CommitUncertain(RuntimeError):
    """A transactional publish may or may not have committed (D15)."""


Build = Callable[[AsyncSession], Awaitable[DomainEvent | tuple[DomainEvent, ...] | None]]
Verify = Callable[[AsyncSession, DomainEvent], Awaitable[bool]]
OnUncertain = Callable[[CommitUncertain], None]

CLOSE_BUDGET_S = 10.0


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
        self._units: set[asyncio.Task[DomainEvent | None]] = set()

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
        # Checked before the lock too: after a bounded close() an abandoned unit may hold
        # the lock forever, and a late publish must fail fast, not wait on it.
        if self._closed:
            raise BusClosedError("event bus is closed")
        async with self._lock:
            if self._closed:  # closed while waiting for the lock
                raise BusClosedError("event bus is closed")
            async with self._sessions() as session:
                row = await repo.append_event(
                    session, event_type, event.payload(), cycle_id=event.cycle_id, ts=event.ts
                )
            self._enqueue(event)
        log.debug("bus.published", event_type=event_type, event_id=row.id)

    async def publish_tx(
        self, build: Build, verify: Verify, *, on_uncertain: OnUncertain | None = None
    ) -> DomainEvent | None:
        """Commit `build`'s writes and the event(s) it returns together, then deliver them in
        order; returns the first."""
        if self._closed:
            raise BusClosedError("event bus is closed")
        unit = asyncio.create_task(self._unit(build, verify, on_uncertain), name="bus:unit")
        self._units.add(unit)
        unit.add_done_callback(self._unit_done)
        return await asyncio.shield(unit)

    def _unit_done(self, unit: asyncio.Task[DomainEvent | None]) -> None:
        self._units.discard(unit)
        if unit.cancelled():
            return
        exc = unit.exception()  # retrieved here; a caller that is still waiting re-raises it
        if exc is not None and not isinstance(exc, CommitUncertain | repo.StaleTransition):
            # The caller may be gone (cancelled): without this the failure would vanish.
            # CommitUncertain is logged by the unit; StaleTransition by its caller (the
            # runner logs cycle.completed_elsewhere or cycle.stuck).
            log.error("bus.unit_failed", error=repr(exc))

    async def _unit(
        self, build: Build, verify: Verify, on_uncertain: OnUncertain | None
    ) -> DomainEvent | None:
        async with self._lock:
            async with self._sessions() as session:
                built = await build(session)
                events: tuple[DomainEvent, ...] = (
                    built if isinstance(built, tuple) else () if built is None else (built,)
                )
                if not events:
                    await session.rollback()
                    return None
                event = events[0]  # all commit together, so verifying one verifies all
                event_type = type(event).__name__
                for each in events:
                    await repo.append_event(
                        session,
                        type(each).__name__,
                        each.payload(),
                        cycle_id=each.cycle_id,
                        ts=each.ts,
                        commit=False,
                    )
                try:
                    await session.commit()
                except Exception as e:
                    # On SQLite a failed COMMIT may still hold a lock that blocks the
                    # read-back in the fresh session below.
                    try:
                        await session.rollback()
                    except Exception:
                        log.exception("bus.rollback_failed", event_type=event_type)
                    if not await self._confirmed(verify, event):
                        error = CommitUncertain(f"{event_type} may or may not be committed")
                        log.error(
                            "bus.commit_uncertain",
                            event_type=event_type,
                            cycle_id=event.cycle_id,
                            error=repr(e),
                        )
                        if on_uncertain is not None:
                            try:
                                on_uncertain(error)
                            except Exception:
                                log.exception("bus.on_uncertain_failed")
                        raise error from e
                    log.warning(
                        "bus.commit_recovered", event_type=event_type, cycle_id=event.cycle_id
                    )
            for each in events:
                self._enqueue(each)
        return event

    async def _confirmed(self, verify: Verify, event: DomainEvent) -> bool:
        try:
            async with self._sessions() as session:
                return await verify(session, event)
        except Exception:
            log.exception("bus.verify_failed", event_type=type(event).__name__)
            return False

    def _enqueue(self, event: DomainEvent) -> None:
        for sub in self._subs:
            if isinstance(event, sub.event_type):
                sub.queue.put_nowait(event)
                if sub.queue.qsize() == HIGH_WATER:
                    log.warning(
                        "bus.queue_high_water", handler=_name(sub.handler), queued=HIGH_WATER
                    )

    async def drain(self) -> None:
        """Wait until every subscriber has processed everything published so far."""
        await asyncio.gather(*(sub.queue.join() for sub in self._subs))

    async def close(self, budget_s: float = CLOSE_BUDGET_S) -> None:
        """Stop accepting events, finish in-flight work and the backlog within `budget_s`."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + budget_s

        def remaining() -> float:
            return max(0.0, deadline - loop.time())

        self._closed = True  # new publishes raise from here on
        try:
            async with asyncio.timeout(remaining()):
                async with self._lock:  # in-flight publishes and units hold it
                    pass
                if self._units:
                    await asyncio.wait(set(self._units))
                await self.drain()
        except TimeoutError:
            log.warning("bus.close_timeout", budget_s=budget_s)
        workers = [sub.task for sub in self._subs if sub.task is not None]
        for task in workers:
            task.cancel()
        if workers:
            await asyncio.wait(workers, timeout=remaining())
        pending: list[asyncio.Task[Any]] = [*workers, *self._units]
        for task in pending:
            if not task.done():
                log.error("bus.task_abandoned", task=task.get_name())

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
