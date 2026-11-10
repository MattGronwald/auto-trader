import asyncio
import io
import json
import signal
import subprocess
import sys
import threading
from collections.abc import Callable
from pathlib import Path

import pytest

from autotrader.core.lock import JournalLockError, acquire
from autotrader.core.shutdown import (
    EXIT_HARD,
    EXIT_WATCHDOG,
    WATCHDOG_GRACE_S,
    ShutdownControl,
    Watchdog,
    make_signal_handler,
)


class FakeThread:
    def __init__(self, target: Callable[[], None]) -> None:
        self.target = target
        self.daemon = False
        self.started = False

    def start(self) -> None:
        self.started = True  # never runs the target: tests drive poll()


class Threads:
    def __init__(self) -> None:
        self.made: list[FakeThread] = []

    def __call__(self, target: Callable[[], None]) -> FakeThread:
        thread = FakeThread(target)
        self.made.append(thread)
        return thread


class Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def test_watchdog_arms_once_and_exits_70_with_a_stderr_line() -> None:
    clock, threads, err = Clock(), Threads(), io.StringIO()
    exits: list[int] = []
    dog = Watchdog(thread_factory=threads, clock=clock, exit=exits.append, stderr=err)
    dog.start()

    dog.arm(35)
    dog.arm(99)  # a second arm is a no-op

    (thread,) = threads.made
    assert (thread.daemon, thread.started, dog.armed) == (True, True, True)
    clock.now = 134.4
    assert dog.poll() is False and exits == []
    clock.now = 134.5  # 35 - log wait
    assert dog.poll() is True
    assert exits == [EXIT_WATCHDOG]
    assert json.loads(err.getvalue())["event"] == "core.watchdog_exit"


def test_arm_assigns_a_deadline_without_starting_a_thread() -> None:
    clock, threads = Clock(), Threads()
    exits: list[int] = []
    dog = Watchdog(thread_factory=threads, clock=clock, exit=exits.append, stderr=io.StringIO())
    dog.start()
    assert not dog.armed and dog.poll() is False

    dog.arm(35)

    assert len(threads.made) == 1  # only start() made one
    assert dog.armed
    clock.now = 134.0
    assert dog.poll() is False
    clock.now = 134.5
    assert dog.poll() is True
    assert exits == [EXIT_WATCHDOG]


def test_start_is_idempotent() -> None:
    threads = Threads()
    dog = Watchdog(thread_factory=threads)
    assert not dog.started

    dog.start()
    dog.start()

    assert dog.started and len(threads.made) == 1


def test_arm_keeps_the_first_deadline() -> None:
    clock, threads = Clock(), Threads()
    exits: list[int] = []
    dog = Watchdog(thread_factory=threads, clock=clock, exit=exits.append, stderr=io.StringIO())
    dog.start()
    dog.arm(35)
    clock.now = 120.0
    dog.arm(35)  # would be 154.5 if it replaced the first

    clock.now = 134.5
    assert dog.poll() is True


def test_arm_on_an_unstarted_watchdog_starts_it_and_says_so() -> None:
    threads, err = Threads(), io.StringIO()
    dog = Watchdog(thread_factory=threads, exit=lambda code: None, stderr=err)

    dog.arm(35)

    assert dog.started and dog.armed
    assert len(threads.made) == 1 and threads.made[0].started
    assert json.loads(err.getvalue().splitlines()[0])["event"] == "core.watchdog_late_start"


def test_the_thread_target_polls_until_the_deadline_passes() -> None:
    clock, threads = Clock(), Threads()
    sleeps: list[float] = []

    def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock.now += 50.0

    dog = Watchdog(
        thread_factory=threads,
        clock=clock,
        exit=lambda code: None,
        stderr=io.StringIO(),
        sleep=sleep,
        poll_s=0.1,
    )
    dog.start()
    dog.arm(75)  # deadline 174.5

    threads.made[0].target()  # not armed-time dependent: loops until poll() fires

    assert sleeps == [0.1, 0.1]  # 150.0 -> not yet; 200.0 -> fired


class RaisingStream(io.StringIO):
    def write(self, s: str) -> int:
        raise OSError("broken pipe")


class BlockingStream(io.StringIO):
    def __init__(self) -> None:
        super().__init__()
        self.release = threading.Event()

    def write(self, s: str) -> int:
        self.release.wait(5)  # stands in for a full pipe
        return len(s)


def test_watchdog_exit_including_the_log_wait_stays_within_the_armed_bound() -> None:
    clock = Clock()
    exits: list[int] = []
    dog = Watchdog(
        thread_factory=Threads(),
        clock=clock,
        exit=exits.append,
        log_wait_s=2.0,
        stderr=io.StringIO(),
    )
    dog.start()
    dog.arm(35)

    clock.now = 132.9
    assert dog.poll() is False
    clock.now = 133.0  # fires at 33, the log wait ends by 35
    assert dog.poll() is True


def test_watchdog_exits_70_even_if_stderr_raises() -> None:
    clock = Clock()
    exits: list[int] = []
    dog = Watchdog(thread_factory=Threads(), clock=clock, exit=exits.append, stderr=RaisingStream())
    dog.start()
    dog.arm(35)

    clock.now = 135.0
    assert dog.poll() is True

    assert exits == [EXIT_WATCHDOG]


def test_watchdog_exits_70_even_if_stderr_blocks() -> None:
    clock, stream = Clock(), BlockingStream()
    exits: list[int] = []
    dog = Watchdog(
        thread_factory=Threads(), clock=clock, exit=exits.append, stderr=stream, log_wait_s=0.01
    )
    dog.start()
    dog.arm(35)
    clock.now = 135.0

    try:
        dog.poll()  # returns after the bounded wait, not when the write ends
        assert exits == [EXIT_WATCHDOG]
    finally:
        stream.release.set()


def test_begin_records_deadline_and_arms_before_anything_else() -> None:
    clock = Clock()
    armed: list[float] = []
    control = ShutdownControl(30, arm_watchdog=armed.append, monotonic=clock)

    control.begin("signal")

    assert (control.cause, control.deadline, control.requested) == ("signal", 130.0, True)
    assert armed == [30 + WATCHDOG_GRACE_S]
    clock.now = 110.0
    assert control.remaining() == 20.0


def test_begin_is_idempotent_and_never_rearms() -> None:
    clock = Clock()
    armed: list[float] = []
    control = ShutdownControl(30, arm_watchdog=armed.append, monotonic=clock)
    control.begin("signal")
    clock.now = 125.0

    control.begin("startup_failure")

    assert (control.cause, control.deadline) == ("signal", 130.0)
    assert armed == [35.0]


async def test_first_signal_arms_the_watchdog_while_the_loop_is_blocked() -> None:
    clock, threads = Clock(), Threads()
    exits: list[int] = []
    dog = Watchdog(thread_factory=threads, clock=clock, exit=exits.append)
    dog.start()
    control = ShutdownControl(30, arm_watchdog=dog.arm, monotonic=clock)
    wakeup = control.attach()
    handler = make_signal_handler(control, exit=exits.append)

    # This coroutine doesn't yield between here and the asserts: the loop is "blocked".
    handler(signal.SIGINT, None)
    assert dog.armed and len(threads.made) == 1  # deadline set; no thread started in the handler
    assert control.deadline == 130.0  # measured from the signal
    assert not wakeup.is_set()  # the queued wake-up hasn't run: no loop callback yet

    await asyncio.sleep(0)
    assert wakeup.is_set()
    assert exits == []


def test_second_signal_exits_130_without_touching_the_loop() -> None:
    exits: list[int] = []
    control = ShutdownControl(30, arm_watchdog=lambda s: None)
    handler = make_signal_handler(control, exit=exits.append)

    handler(signal.SIGTERM, None)
    handler(signal.SIGINT, None)

    assert exits == [EXIT_HARD]
    assert control.cause == "signal"


async def test_attach_after_begin_starts_set() -> None:
    control = ShutdownControl(30, arm_watchdog=lambda s: None)
    control.begin("signal")  # e.g. a signal during the migration, before any loop

    assert control.attach().is_set()


def test_begin_after_the_attached_loop_closed_still_records_and_arms() -> None:
    clock = Clock()
    armed: list[float] = []
    control = ShutdownControl(30, arm_watchdog=armed.append, monotonic=clock)

    async def attach() -> None:
        control.attach()

    asyncio.run(attach())  # e.g. run_core raised before its teardown; asyncio.run closed it

    control.begin("startup_failure")  # the CLI's except handler, or a late signal

    assert (control.cause, control.deadline) == ("startup_failure", 130.0)
    assert armed == [30 + WATCHDOG_GRACE_S]


def test_remaining_before_begin_is_the_whole_budget() -> None:
    assert ShutdownControl(30, arm_watchdog=lambda s: None).remaining() == 30


def test_install_signal_handlers_targets_sigint_and_sigterm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installed: list[int] = []
    monkeypatch.setattr("signal.signal", lambda sig, h: installed.append(sig))
    from autotrader.core.shutdown import install_signal_handlers

    install_signal_handlers(lambda signum, frame: None)

    assert installed == [signal.SIGINT, signal.SIGTERM]


HOLD_THEN_HANG = """
import sys, time
from autotrader.core.lock import acquire
from autotrader.core.shutdown import ShutdownControl, install_signal_handlers, make_signal_handler
lock = acquire(sys.argv[1], wait_s=0)
control = ShutdownControl(30, arm_watchdog=lambda seconds: None)
install_signal_handlers(make_signal_handler(control))
print("held", flush=True)
while not control.requested:
    time.sleep(0.01)
print("stopping", flush=True)
while True:  # a teardown that never finishes
    time.sleep(0.01)
"""


def test_lock_is_held_through_a_stuck_shutdown_until_the_second_signal(tmp_path: Path) -> None:
    url = f"sqlite+aiosqlite:///{tmp_path / 'j.db'}"
    proc = subprocess.Popen(  # noqa: S603  (fixed argv: this interpreter + a constant script)
        [sys.executable, "-c", HOLD_THEN_HANG, url],
        stdout=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None
    try:
        assert proc.stdout.readline().strip() == "held"
        proc.send_signal(signal.SIGINT)
        assert proc.stdout.readline().strip() == "stopping"
        with pytest.raises(JournalLockError):  # shutting down still owns the journal
            acquire(url, wait_s=0)
        proc.send_signal(signal.SIGINT)
        assert proc.wait(timeout=10) == EXIT_HARD
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
        proc.stdout.close()
    acquire(url, wait_s=0).release()  # process death released it
