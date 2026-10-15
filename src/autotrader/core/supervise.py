"""Supervised background loops (WP 1.5a, spec 2026-10-14 section 3).

Each `Loop` runs `step()` every `interval_s` (start to start: the pause is the rest of the
interval; an overrun starts the next step at once, no catch-up) in its own task, so a hung
step in one loop never delays another. The heartbeat is when `step()` last completed;
`check()` logs a stall once per transition and never cancels a slow step (cancelling could
cut a journal write mid-commit). A raising step is logged and the loop carries on; a task
killed by an escaped `BaseException` is logged and restarted. `Freshness` probes track data
age (e.g. the last committed equity snapshot) separately from loop liveness.
`stop(deadline)` lets steps finish until the deadline, then cancels the rest once and leaves
them behind.
"""

import asyncio
import contextlib
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Literal

import structlog

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class Loop:
    name: str
    step: Callable[[], Awaitable[None]]
    interval_s: float
    stall_after_s: float
    stall_level: Literal["warning", "error"] = "warning"


@dataclass(frozen=True)
class Freshness:
    name: str  # logs "<name>_overdue" / "<name>_fresh"
    last: Callable[[], float | None]  # on the supervisor's clock; None = never
    max_age_s: float


class Supervisor:
    def __init__(
        self,
        loops: Sequence[Loop],
        freshness: Sequence[Freshness] = (),
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._loops = {loop.name: loop for loop in loops}
        self._freshness = list(freshness)
        self._clock = clock
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._beats: dict[str, float] = {}
        self._stalled: set[str] = set()
        self._overdue: set[str] = set()
        self._started_at = 0.0
        self._stopping: asyncio.Event | None = None

    def start(self) -> None:
        self._started_at = self._clock()
        self._stopping = asyncio.Event()
        for loop in self._loops.values():
            self._beats[loop.name] = self._started_at
            self._spawn(loop)

    def heartbeat(self, name: str) -> float:
        return self._beats[name]

    def check(self) -> None:
        """Restart dead loop tasks; log stall and freshness transitions."""
        if self._stopping is None or self._stopping.is_set():
            return
        now = self._clock()
        for name, task in list(self._tasks.items()):
            if task.done():
                error = None if task.cancelled() else task.exception()
                log.error(f"{name}.task_died", error=repr(error))
                self._spawn(self._loops[name])
        for name, loop in self._loops.items():
            age = now - self._beats[name]
            if age > loop.stall_after_s:
                if name not in self._stalled:
                    self._stalled.add(name)
                    log_at = log.error if loop.stall_level == "error" else log.warning
                    log_at(f"{name}.stalled", age_s=round(age, 3))
            elif name in self._stalled:
                self._stalled.discard(name)
                log.info(f"{name}.recovered")
        for probe in self._freshness:
            last = probe.last()
            age = now - (last if last is not None else self._started_at)
            if age > probe.max_age_s:
                if probe.name not in self._overdue:
                    self._overdue.add(probe.name)
                    log.warning(f"{probe.name}_overdue", age_s=round(age, 3))
            elif probe.name in self._overdue:
                self._overdue.discard(probe.name)
                log.info(f"{probe.name}_fresh")

    async def stop(self, deadline: float) -> list[str]:
        """Let running steps finish until `deadline` (on `clock`), then cancel the rest
        once and leave them behind. Returns the names that had to be cancelled."""
        if self._stopping is not None:
            self._stopping.set()
        running = {task: name for name, task in self._tasks.items() if not task.done()}
        if not running:
            return []
        _, pending = await asyncio.wait(running, timeout=max(0.0, deadline - self._clock()))
        for task in pending:
            task.cancel()
        return sorted(running[task] for task in pending)

    def _spawn(self, loop: Loop) -> None:
        self._tasks[loop.name] = asyncio.create_task(self._run(loop), name=loop.name)

    async def _run(self, loop: Loop) -> None:
        stopping = self._stopping
        if stopping is None:
            raise RuntimeError("Supervisor.start() was not called")
        while not stopping.is_set():
            started = self._clock()
            try:
                await loop.step()
            except Exception:
                log.exception(f"{loop.name}.step_failed")
            self._beats[loop.name] = self._clock()
            # Start-to-start cadence: sleep only for the rest of the interval. An overrun
            # starts the next step at once, with no catch-up burst.
            await self._pause(stopping, max(0.0, loop.interval_s - (self._clock() - started)))

    async def _pause(self, stopping: asyncio.Event, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stopping.wait(), timeout=seconds)
