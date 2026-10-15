"""Shutdown trigger, watchdog and signal handling (WP 1.5a, spec 2026-10-14 section 4).

`ShutdownControl.begin()` is plain synchronous code: the first SIGINT/SIGTERM calls it from a
Python-level `signal.signal` handler, which runs between bytecodes even while the event loop
is stuck in synchronous Python code. It records the monotonic deadline (T from the trigger)
and starts the watchdog thread *before* asking the loop for the async teardown. Every exit
cause uses it; later calls are no-ops. A second signal calls `os._exit(130)` at once: no
async cleanup and no lock release (process death releases the journal lock). The watchdog
thread ends the process with `os._exit(70)` at T + 5 s if teardown still hangs. It is never
cancelled: it is a daemon thread, so it never delays a normal exit, and it still fires if the
interpreter's own exit hangs (aiosqlite worker threads are non-daemon and are joined there).

Limits: a Python-level handler runs only when the main thread executes bytecode, so a first
signal that arrives while the main thread is stuck in a C call that never returns arms
nothing. The armed watchdog is a Python thread too: it covers native waits that release the
GIL (I/O, select, sleep, SQLite), but not a native call that holds the GIL and never returns.
Only SIGKILL or an outside supervisor ends such a process. A signal that arrives after
another cause (stream death, startup failure, programmatic stop) began shutdown counts as the
first one: it is a no-op, and the next signal exits 130.
"""

import asyncio
import contextlib
import json
import os
import signal
import sys
import threading
import time
from collections.abc import Callable
from types import FrameType
from typing import Protocol, TextIO

WATCHDOG_GRACE_S = 5.0
EXIT_CLEAN = 0
EXIT_FAILED = 1
EXIT_HARD = 130
EXIT_WATCHDOG = 70


class _Timer(Protocol):
    daemon: bool

    def start(self) -> None: ...


TimerFactory = Callable[[float, Callable[[], None]], _Timer]
SignalHandler = Callable[[int, FrameType | None], None]


class Watchdog:
    """Last resort: a daemon thread that ends the process if teardown hangs."""

    def __init__(
        self,
        *,
        timer_factory: TimerFactory = threading.Timer,
        exit: Callable[[int], object] = os._exit,
        stderr: TextIO | None = None,
        log_wait_s: float = 0.5,
    ) -> None:
        self._factory = timer_factory
        self._exit = exit
        self._stderr = stderr
        self._log_wait_s = log_wait_s
        self._timer: _Timer | None = None

    @property
    def armed(self) -> bool:
        return self._timer is not None

    def arm(self, seconds: float) -> None:
        """Guarantee exit within `seconds`: fire `log_wait_s` early, so the bounded
        diagnostic wait in `_fire` still ends by the deadline."""
        if self._timer is not None:
            return
        timer = self._factory(max(0.0, seconds - self._log_wait_s), self._fire)
        timer.daemon = True
        timer.start()
        self._timer = timer

    def _fire(self) -> None:
        """Exit 70, whatever happens to the diagnostic line. The line is best-effort: it is
        written by a throwaway daemon thread that gets at most `log_wait_s`, so a closed,
        broken or blocking stderr can never keep the process alive."""
        try:
            writer = threading.Thread(target=self._write_line, name="watchdog-log", daemon=True)
            writer.start()
            writer.join(self._log_wait_s)
        finally:
            self._exit(EXIT_WATCHDOG)

    def _write_line(self) -> None:
        # Not through structlog: the event loop (and anything it owns) may be stuck.
        stream = self._stderr if self._stderr is not None else sys.stderr
        with contextlib.suppress(Exception):
            stream.write(json.dumps({"event": "core.watchdog_exit", "level": "critical"}) + "\n")
            stream.flush()


class ShutdownControl:
    """The synchronous, idempotent shutdown trigger shared by every exit cause."""

    def __init__(
        self,
        timeout_s: float,
        *,
        arm_watchdog: Callable[[float], None],
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.timeout_s = timeout_s
        self._arm_watchdog = arm_watchdog
        self._monotonic = monotonic
        self.cause: str | None = None
        self.deadline: float | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._wakeup: asyncio.Event | None = None

    @property
    def requested(self) -> bool:
        return self.cause is not None

    def begin(self, cause: str) -> None:
        """Deadline, then watchdog, then wake the loop. Safe in a signal handler, and after the
        attached loop has closed (run_core raised before teardown, or a late signal): then
        there is no loop to wake, and the deadline and the armed watchdog still stand."""
        if self.cause is not None:
            return
        self.cause = cause
        self.deadline = self._monotonic() + self.timeout_s
        self._arm_watchdog(self.timeout_s + WATCHDOG_GRACE_S)
        loop, wakeup = self._loop, self._wakeup
        if loop is not None and wakeup is not None and not loop.is_closed():
            with contextlib.suppress(RuntimeError):  # closed between the check and the call
                loop.call_soon_threadsafe(wakeup.set)

    def attach(self) -> asyncio.Event:
        """Bind to the running loop; the returned event is set once shutdown begins."""
        self._loop = asyncio.get_running_loop()
        self._wakeup = asyncio.Event()
        if self.cause is not None:
            self._wakeup.set()
        return self._wakeup

    def remaining(self) -> float:
        if self.deadline is None:
            return self.timeout_s
        return max(0.0, self.deadline - self._monotonic())


def make_signal_handler(
    control: ShutdownControl, *, exit: Callable[[int], object] = os._exit
) -> SignalHandler:
    """First signal: `control.begin("signal")`. Second: exit 130 immediately."""
    received = 0

    def handler(signum: int, frame: FrameType | None) -> None:
        nonlocal received
        received += 1
        if received == 1:
            control.begin("signal")
        else:
            exit(EXIT_HARD)

    return handler


def install_signal_handlers(handler: SignalHandler) -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, handler)
