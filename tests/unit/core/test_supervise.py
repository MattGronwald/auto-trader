import asyncio
import io
import json
from typing import Any

from autotrader.core.logging import configure_logging
from autotrader.core.supervise import Freshness, Loop, Supervisor


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def lines(buf: io.StringIO) -> list[dict[str, Any]]:
    return [json.loads(x) for x in buf.getvalue().splitlines() if x.startswith("{")]


def events(buf: io.StringIO) -> list[str]:
    return [ln["event"] for ln in lines(buf)]


async def until(pred: Any, timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while not pred():
            await asyncio.sleep(0.001)


async def test_stall_and_recovery_are_logged_once_per_transition() -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    clock, release = Clock(), asyncio.Event()

    async def step() -> None:
        await release.wait()

    sup = Supervisor([Loop("feed.tick", step, 0.001, 30, stall_level="warning")], clock=clock)
    sup.start()
    clock.now += 31
    sup.check()
    sup.check()
    assert events(buf).count("feed.tick.stalled") == 1
    stalled = next(ln for ln in lines(buf) if ln["event"] == "feed.tick.stalled")
    assert (stalled["level"], stalled["age_s"]) == ("warning", 31.0)

    release.set()
    await until(lambda: sup.heartbeat("feed.tick") == clock.now)
    sup.check()
    assert events(buf).count("feed.tick.recovered") == 1
    await sup.stop(clock.now + 1)


async def test_error_level_stall_for_control_poll() -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    clock = Clock()

    async def step() -> None:
        await asyncio.Event().wait()

    sup = Supervisor([Loop("control.poll", step, 1, 10, stall_level="error")], clock=clock)
    sup.start()
    clock.now += 11
    sup.check()
    stalled = next(ln for ln in lines(buf) if ln["event"] == "control.poll.stalled")
    assert stalled["level"] == "error"
    await sup.stop(clock.now)  # deadline already reached: the hung step is cancelled


async def test_one_hung_loop_never_delays_another() -> None:
    clock, ticks = Clock(), []

    async def hung() -> None:
        await asyncio.Event().wait()

    async def ticking() -> None:
        ticks.append(1)

    sup = Supervisor(
        [Loop("feed.tick", hung, 0.001, 30), Loop("control.poll", ticking, 0.001, 10)],
        clock=clock,
    )
    sup.start()
    await until(lambda: len(ticks) >= 3)
    assert await sup.stop(clock.now) == ["feed.tick"]


async def test_raising_step_keeps_its_loop_running() -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    calls = []

    async def step() -> None:
        calls.append(1)
        raise ConnectionError("down")

    sup = Supervisor([Loop("feed.tick", step, 0.001, 30)])
    sup.start()
    await until(lambda: len(calls) >= 3)
    assert events(buf).count("feed.tick.step_failed") >= 3
    sup.check()
    assert "feed.tick.task_died" not in events(buf)
    await sup.stop(asyncio.get_running_loop().time() + 1)


class Killer(BaseException):
    pass


async def test_dead_task_is_logged_and_restarted() -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    calls = []

    async def step() -> None:
        calls.append(1)
        if len(calls) == 1:
            raise Killer

    sup = Supervisor([Loop("risk.snapshot", step, 0.001, 120)])
    sup.start()
    await until(lambda: len(calls) == 1)
    await asyncio.sleep(0)
    sup.check()
    await until(lambda: len(calls) >= 2)
    died = [ln for ln in lines(buf) if ln["event"] == "risk.snapshot.task_died"]
    assert len(died) == 1 and "Killer" in died[0]["error"]
    await sup.stop(asyncio.get_running_loop().time() + 1)


async def test_check_never_restarts_loops_after_stop() -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)

    async def step() -> None:
        return None

    sup = Supervisor([Loop("feed.tick", step, 0.001, 30)])
    sup.start()
    assert await sup.stop(asyncio.get_running_loop().time() + 1) == []
    sup.check()
    assert "feed.tick.task_died" not in events(buf)


async def test_stop_cancels_a_cancellation_resistant_step_once_and_leaves_it() -> None:
    clock, release = Clock(), asyncio.Event()
    cancels = []

    async def stubborn() -> None:
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancels.append(1)

    sup = Supervisor([Loop("feed.tick", stubborn, 0.001, 30)], clock=clock)
    sup.start()
    await asyncio.sleep(0)
    assert await sup.stop(clock.now + 0.01) == ["feed.tick"]
    await asyncio.sleep(0)
    assert cancels == [1]  # cancelled once, then left behind (not awaited again)
    release.set()
    await asyncio.sleep(0.01)


async def test_cadence_subtracts_step_duration_without_catch_up() -> None:
    clock = Clock()
    durations = iter([3.0, 12.0])
    waits: list[float] = []

    class Recording(Supervisor):
        async def _pause(self, stopping: asyncio.Event, seconds: float) -> None:
            waits.append(seconds)
            if len(waits) == 2:
                stopping.set()
            await asyncio.sleep(0)

    async def step() -> None:
        clock.now += next(durations)

    sup = Recording([Loop("risk.snapshot", step, 10, 120)], clock=clock)
    sup.start()
    await until(lambda: len(waits) == 2)

    assert waits == [7.0, 0.0]  # 10 - 3; then a 12 s overrun: next step at once
    await sup.stop(clock.now + 1)


async def test_freshness_is_tracked_separately_from_liveness() -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    clock = Clock()
    last: list[float | None] = [None]

    async def step() -> None:
        return None  # the loop is alive and keeps completing

    sup = Supervisor(
        [Loop("risk.snapshot", step, 0.001, 120)],
        [Freshness("risk.snapshot", lambda: last[0], 120)],
        clock=clock,
    )
    sup.start()  # no write yet: age measured from start
    clock.now += 121
    await until(lambda: sup.heartbeat("risk.snapshot") == clock.now)  # the loop is alive
    sup.check()
    assert events(buf).count("risk.snapshot_overdue") == 1
    assert "risk.snapshot.stalled" not in events(buf)  # the loop itself is fine

    last[0] = clock.now  # a committed write
    sup.check()
    assert events(buf).count("risk.snapshot_fresh") == 1
    await sup.stop(clock.now + 1)
