import asyncio
import gc
import io
import json
from collections.abc import Awaitable, Callable, Iterator, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine

from autotrader.broker.base import Account, BrokerError, FeeSchedule
from autotrader.core.bus import EventBus
from autotrader.core.config import AppConfig, Settings, load_config, load_profile
from autotrader.core.feed import Latest, MarketFeed
from autotrader.core.kill import ControlStatus
from autotrader.core.lock import JournalLock, acquire
from autotrader.core.logging import configure_logging
from autotrader.core.runtime import ControlPoller, _snapshot_step, run_core
from autotrader.core.shutdown import EXIT_CLEAN, EXIT_FAILED, ShutdownControl
from autotrader.core.types import Bar, Quote
from autotrader.journal import models as m
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade

REPO = Path(__file__).resolve().parents[3]


class StreamingSource:
    """Delivers two live bars, then idles until stopped."""

    def __init__(self, fail_stream: bool = False) -> None:
        self.fail_stream = fail_stream
        self.stopped = asyncio.Event()

    async def history(self, symbols: Sequence[str], start: datetime, end: datetime) -> list[Bar]:
        return []

    async def latest(self, symbol: str) -> Latest:
        return Latest(bar=None, quote=None)

    async def stream(
        self, on_bar: Callable[[Bar], Awaitable[None]], on_quote: Callable[[Quote], Awaitable[None]]
    ) -> None:
        if self.fail_stream:
            raise ConnectionError("stream died")
        # The current minute, computed now: never inside an already finalised window
        # (windows before `now - grace` are final at startup; late bars aren't announced).
        ts = datetime.now(UTC).replace(second=0, microsecond=0)
        for symbol in ("BTC/USD", "ETH/USD"):
            await on_bar(
                Bar(
                    symbol=symbol,
                    timeframe="1m",
                    ts=ts,
                    open=1,
                    high=1,
                    low=1,
                    close=1,
                    volume=1,
                )
            )
        await self.stopped.wait()

    def connected(self) -> bool:
        return True

    async def stop(self) -> None:
        self.stopped.set()


class FakeBroker:
    async def get_account(self) -> Account:
        return Account(equity=Decimal("100000"), cash=Decimal("100000"))

    async def get_positions(self) -> list[Any]:
        return []

    def mode(self) -> str:
        return "paper"

    def fee_schedule(self) -> FeeSchedule:
        return FeeSchedule(maker_bps=Decimal(15), taker_bps=Decimal(25), buy_fee_asset="quote")


class HangingSnapshotBroker(FakeBroker):
    """Answers the startup check, then every later `get_account` hangs (a stuck REST call)."""

    def __init__(self) -> None:
        self.calls = 0

    async def get_account(self) -> Account:
        self.calls += 1
        if self.calls > 1:
            await asyncio.Event().wait()
        return await super().get_account()


async def fake_connect(_: Settings) -> Any:
    return FakeBroker()


@pytest.fixture
def cfg(tmp_path: Path) -> AppConfig:
    text = (
        (REPO / "config.yaml")
        .read_text()
        .replace("strategy: strategies/", f"strategy: {REPO}/strategies/")
    )
    path = tmp_path / "config.yaml"
    path.write_text(text)
    config = load_config(path, Settings(_env_file=None))
    upgrade(config.database.url)
    return config


@pytest.fixture
def lock(cfg: AppConfig) -> Iterator[JournalLock]:
    held = acquire(cfg.database.url, wait_s=0)
    yield held
    held.release()  # tests only: production never releases early


class FakeWatchdog:
    def __init__(self) -> None:
        self.armed: list[float] = []

    def arm(self, seconds: float) -> None:
        self.armed.append(seconds)


def make_control(timeout_s: float = 5.0) -> tuple[ShutdownControl, FakeWatchdog]:
    dog = FakeWatchdog()
    return ShutdownControl(timeout_s, arm_watchdog=dog.arm), dog


def start_core(
    cfg: AppConfig, lock: JournalLock, control: ShutdownControl, **kw: Any
) -> "asyncio.Task[int]":
    kw.setdefault("source", StreamingSource())
    kw.setdefault("connect_broker", fake_connect)
    kw.setdefault("tick_s", 0.01)
    kw.setdefault("supervise_s", 0.01)
    return asyncio.create_task(
        run_core(
            cfg,
            Settings(_env_file=None),
            load_profile(cfg.strategy),
            "yaml",
            lock=lock,
            control=control,
            **kw,
        )
    )


class Stubborn:
    """An awaitable that ignores cancellation until released."""

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.cancels = 0

    async def wait(self) -> None:
        while not self.release.is_set():
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancels += 1


def _log_lines(buf: io.StringIO) -> list[dict[str, Any]]:
    return [json.loads(line) for line in buf.getvalue().splitlines() if line.startswith("{")]


async def _wait_for(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


async def test_run_core_persists_bars_and_responds_to_kill_file(
    cfg: AppConfig, lock: JournalLock
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    control, _ = make_control()
    source = StreamingSource()
    profile = load_profile(cfg.strategy)

    task = start_core(cfg, lock, control, source=source, snapshot_s=0.01)
    try:
        await _wait_for(lambda: any(ln["event"] == "core.started" for ln in _log_lines(buf)))
        cfg.control.kill_file.parent.mkdir(parents=True, exist_ok=True)
        cfg.control.kill_file.touch()
        await _wait_for(lambda: any(ln["event"] == "core.kill_detected" for ln in _log_lines(buf)))
        cfg.control.kill_file.unlink()
        await _wait_for(lambda: any(ln["event"] == "core.kill_cleared" for ln in _log_lines(buf)))
        await _wait_for(
            lambda: any(ln["event"] == "risk.snapshot_written" for ln in _log_lines(buf))
        )
    finally:
        control.begin("stop")
        assert await asyncio.wait_for(task, timeout=5) == EXIT_CLEAN

    started = next(ln for ln in _log_lines(buf) if ln["event"] == "core.started")
    assert started["mode"] == "paper"
    scanner = next(ln for ln in _log_lines(buf) if ln["event"] == "scanner.subscribed")
    assert scanner["timeframe"] == profile.timeframes.signal
    assert scanner["rules"] == [r.id for r in profile.signal_rules]
    assert started["profile"] == "fast_momentum_v1"
    assert source.stopped.is_set()  # clean shutdown
    engine = make_engine(cfg.database.url)
    try:  # dispose even when an assertion fails: a leaked connection breaks a later test
        async with make_sessionmaker(engine)() as s:
            assert await s.scalar(select(func.count()).select_from(m.BarRow)) == 2
            assert await s.scalar(select(func.count()).select_from(m.Strategy)) == 1
            snapshots = await s.scalar(select(func.count()).select_from(m.EquitySnapshot))
            assert snapshots is not None and snapshots >= 1
            closed = [e.payload for e in await s.scalars(select(m.Event)) if e.type == "BarClosed"]
    finally:
        await engine.dispose()
    # Only the 1m announcements are deterministic: depending on the wall-clock minute, the
    # bars' 5m/15m buckets may already have ended and are then (correctly) closed too.
    assert [p["timeframe"] for p in closed].count("1m") == 2


async def test_dead_stream_stops_the_core_with_an_error(cfg: AppConfig, lock: JournalLock) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    control, dog = make_control()

    with pytest.raises(ConnectionError, match="stream died"):
        await asyncio.wait_for(
            start_core(cfg, lock, control, source=StreamingSource(fail_stream=True)), timeout=5
        )
    assert control.cause == "stream_death"
    assert dog.armed  # the coordinator armed the watchdog for this exit cause too
    failed = next(ln for ln in _log_lines(buf) if ln["event"] == "core.failed")
    assert failed["cause"] == "stream_death" and "stream died" in failed["exception"]


async def test_broker_connect_failure_aborts_startup(cfg: AppConfig, lock: JournalLock) -> None:
    async def failing(_: Settings) -> Any:
        raise BrokerError("auth failed")

    control, dog = make_control()
    with pytest.raises(BrokerError, match="auth failed"):
        await start_core(cfg, lock, control, connect_broker=failing)
    assert control.cause == "startup_failure" and dog.armed


class FlakySnapshots:
    """Times out (the engine's own budget), then raises, then succeeds."""

    def __init__(self) -> None:
        self.calls = 0

    async def snapshot_equity(self) -> None:
        self.calls += 1
        if self.calls == 1:
            raise TimeoutError
        if self.calls == 2:
            raise ConnectionError("broker down")


async def test_snapshot_step_logs_failures_and_successes() -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    snapshots = FlakySnapshots()
    for _ in range(3):
        await _snapshot_step(snapshots)
    events = [ln for ln in _log_lines(buf) if ln["event"].startswith("risk.snapshot")]
    assert [(ln["event"], ln.get("timeout")) for ln in events] == [
        ("risk.snapshot_failed", True),
        ("risk.snapshot_failed", None),
        ("risk.snapshot_written", None),
    ]


async def test_hung_snapshot_at_the_default_interval_never_delays_kill_detection(
    cfg: AppConfig, lock: JournalLock
) -> None:
    """Default snapshot_s (60 s); the snapshot hangs inside the engine's own 10 s budget.
    The core loop never waits for a snapshot, only the background snapshot task does."""
    buf = io.StringIO()
    configure_logging(stream=buf)
    broker = HangingSnapshotBroker()

    async def connect(_: Settings) -> Any:
        return broker

    control, _ = make_control(timeout_s=0.5)
    task = start_core(cfg, lock, control, connect_broker=connect)
    try:
        await _wait_for(lambda: any(ln["event"] == "core.started" for ln in _log_lines(buf)))
        await _wait_for(lambda: broker.calls > 1)  # the first snapshot is hanging now
        cfg.control.kill_file.parent.mkdir(parents=True, exist_ok=True)
        cfg.control.kill_file.touch()
        await _wait_for(
            lambda: any(ln["event"] == "core.kill_detected" for ln in _log_lines(buf)), timeout=1
        )
    finally:
        control.begin("stop")
        assert await asyncio.wait_for(task, timeout=15) in (EXIT_CLEAN, EXIT_FAILED)


class SnapshotKiller(BaseException):
    """Escapes the snapshot loop's `except Exception` (stands in for an unexpected end)."""


class DyingSnapshotBroker(FakeBroker):
    """Startup check answers; the first snapshot kills the task; later ones succeed."""

    def __init__(self) -> None:
        self.calls = 0

    async def get_account(self) -> Account:
        self.calls += 1
        if self.calls == 2:
            raise SnapshotKiller
        return await super().get_account()


async def test_dead_snapshot_task_is_logged_and_restarted(
    cfg: AppConfig, lock: JournalLock
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    broker = DyingSnapshotBroker()

    async def connect(_: Settings) -> Any:
        return broker

    control, _ = make_control()
    task = start_core(cfg, lock, control, connect_broker=connect)
    try:
        await _wait_for(
            lambda: any(ln["event"] == "risk.snapshot_written" for ln in _log_lines(buf))
        )
    finally:
        control.begin("stop")
        await asyncio.wait_for(task, timeout=5)
    died = [ln for ln in _log_lines(buf) if ln["event"] == "risk.snapshot.task_died"]
    assert len(died) == 1 and "SnapshotKiller" in died[0]["error"]


async def test_shutdown_requested_before_start_skips_startup(
    cfg: AppConfig, lock: JournalLock
) -> None:
    connected = []

    async def connect(s: Settings) -> Any:
        connected.append(1)
        return FakeBroker()

    control, _ = make_control()
    control.begin("signal")  # e.g. a signal during the CLI's migration
    source = StreamingSource()

    assert await start_core(cfg, lock, control, source=source, connect_broker=connect) == 0
    assert connected == [] and source.stopped.is_set()


async def test_run_core_rejects_a_lock_for_another_journal(cfg: AppConfig, tmp_path: Path) -> None:
    other = acquire(f"sqlite+aiosqlite:///{tmp_path / 'other.db'}", wait_s=0)
    try:
        control, _ = make_control()
        with pytest.raises(ValueError, match="different journal"):
            await start_core(cfg, other, control)
    finally:
        other.release()


async def test_cancellation_resistant_cleanup_is_left_behind_at_the_deadline(
    cfg: AppConfig, lock: JournalLock
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    stubborn = Stubborn()
    source = StreamingSource()
    source.stop = stubborn.wait  # type: ignore[method-assign]
    control, _ = make_control(timeout_s=0.2)

    task = start_core(cfg, lock, control, source=source)
    await _wait_for(lambda: any(ln["event"] == "core.started" for ln in _log_lines(buf)))
    control.begin("stop")
    try:
        assert await asyncio.wait_for(task, timeout=5) == EXIT_FAILED
    finally:
        stubborn.release.set()
        await asyncio.sleep(0.01)
    forced = next(ln for ln in _log_lines(buf) if ln["event"] == "core.shutdown_forced")
    assert "source.stop" in forced["steps"]
    assert "engine.dispose" not in forced["steps"]
    assert stubborn.cancels == 1
    assert lock.held  # run_core never releases the journal lock


async def test_stream_death_with_stuck_cleanup_still_ends_within_the_deadline(
    cfg: AppConfig, lock: JournalLock
) -> None:
    stubborn = Stubborn()
    source = StreamingSource(fail_stream=True)
    source.stop = stubborn.wait  # type: ignore[method-assign]
    control, dog = make_control(timeout_s=0.2)

    try:
        with pytest.raises(ConnectionError):
            await asyncio.wait_for(start_core(cfg, lock, control, source=source), timeout=5)
    finally:
        stubborn.release.set()
        await asyncio.sleep(0.01)
    assert dog.armed and control.cause == "stream_death"


class QuietSource(StreamingSource):
    """Connected, but no bars arrive and the REST price refresh hangs forever."""

    async def stream(
        self, on_bar: Callable[[Bar], Awaitable[None]], on_quote: Callable[[Quote], Awaitable[None]]
    ) -> None:
        await self.stopped.wait()

    async def latest(self, symbol: str) -> Latest:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


async def test_hanging_rest_refresh_never_delays_kill_detection(
    cfg: AppConfig, lock: JournalLock, monkeypatch: pytest.MonkeyPatch
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    monkeypatch.setattr("autotrader.core.runtime.FEED_STALL_S", 0.05)
    control, _ = make_control(timeout_s=0.5)
    task = start_core(cfg, lock, control, source=QuietSource())
    try:
        await _wait_for(lambda: any(ln["event"] == "feed.tick.stalled" for ln in _log_lines(buf)))
        cfg.control.kill_file.parent.mkdir(parents=True, exist_ok=True)
        cfg.control.kill_file.touch()
        await _wait_for(lambda: any(ln["event"] == "core.kill_detected" for ln in _log_lines(buf)))
    finally:
        control.begin("stop")
        await asyncio.wait_for(task, timeout=5)


async def test_kill_file_is_seen_while_the_journal_read_hangs(
    cfg: AppConfig, lock: JournalLock, monkeypatch: pytest.MonkeyPatch
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)

    async def hung_status(self: Any, now: Any = None) -> ControlStatus:
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    monkeypatch.setattr("autotrader.core.runtime.ControlGate.status", hung_status)
    monkeypatch.setattr("autotrader.core.runtime.CONTROL_STALL_S", 0.05)
    control, _ = make_control(timeout_s=0.5)
    task = start_core(cfg, lock, control)
    try:
        await _wait_for(
            lambda: any(ln["event"] == "control.poll.stalled" for ln in _log_lines(buf))
        )
        cfg.control.kill_file.parent.mkdir(parents=True, exist_ok=True)
        cfg.control.kill_file.touch()
        await _wait_for(lambda: any(ln["event"] == "core.kill_detected" for ln in _log_lines(buf)))
    finally:
        control.begin("stop")
        await asyncio.wait_for(task, timeout=5)
    detected = next(ln for ln in _log_lines(buf) if ln["event"] == "core.kill_detected")
    assert detected["reasons"] == [f"kill file present: {cfg.control.kill_file}"]


class FixedGate:
    def __init__(self, status: ControlStatus) -> None:
        self.status_ = status

    async def status(self) -> ControlStatus:
        return self.status_


async def test_poller_keeps_a_known_kill_while_the_journal_is_unavailable(tmp_path: Path) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    gate = FixedGate(ControlStatus(True, ("kill set in control_state: x",)))
    poller = ControlPoller(gate, tmp_path / "KILL")  # type: ignore[arg-type]
    await poller.step()

    gate.status_ = ControlStatus(False, ("control state unavailable: OSError",), available=False)
    await poller.step()
    await poller.step()  # repeated outages don't grow the reasons
    assert poller.latest == ControlStatus(
        True,
        ("kill set in control_state: x", "control state unavailable: OSError"),
        available=False,
    )

    gate.status_ = ControlStatus(False, ())  # a successful read confirms the clear
    await poller.step()
    assert poller.latest == ControlStatus(False, ())
    events = [ln["event"] for ln in _log_lines(buf)]
    assert events == ["core.kill_detected", "core.kill_cleared"]


async def test_poller_never_clears_a_kill_while_the_file_is_present(tmp_path: Path) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    kill_file = tmp_path / "KILL"
    kill_file.touch()  # e.g. created while the journal read was in flight
    poller = ControlPoller(FixedGate(ControlStatus(False, ())), kill_file)  # type: ignore[arg-type]

    await poller.file_step()
    await poller.step()  # the journal says "not killed", but the file is there
    await poller.file_step()
    assert poller.latest is not None and poller.latest.killed

    kill_file.unlink()
    await poller.step()
    events = [ln["event"] for ln in _log_lines(buf)]
    assert events == ["core.kill_detected", "core.kill_cleared"]  # no flapping


async def test_stop_during_startup_is_bounded_by_the_deadline(
    cfg: AppConfig, lock: JournalLock, monkeypatch: pytest.MonkeyPatch
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    in_backfill = asyncio.Event()

    async def hung_backfill(self: Any) -> None:
        in_backfill.set()
        await asyncio.Event().wait()

    monkeypatch.setattr("autotrader.core.runtime.MarketFeed.backfill", hung_backfill)
    control, _ = make_control(timeout_s=0.2)
    task = start_core(cfg, lock, control)
    await asyncio.wait_for(in_backfill.wait(), timeout=5)
    control.begin("signal")

    assert await asyncio.wait_for(task, timeout=5) == EXIT_FAILED
    forced = next(ln for ln in _log_lines(buf) if ln["event"] == "core.shutdown_forced")
    assert forced["steps"] == ["startup"]
    assert "core.started" not in [ln["event"] for ln in _log_lines(buf)]


async def test_startup_finishing_after_the_stop_request_starts_nothing(
    cfg: AppConfig, lock: JournalLock, monkeypatch: pytest.MonkeyPatch
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    in_backfill, go = asyncio.Event(), asyncio.Event()

    async def slow_backfill(self: Any) -> None:
        in_backfill.set()
        await go.wait()

    monkeypatch.setattr("autotrader.core.runtime.MarketFeed.backfill", slow_backfill)
    control, _ = make_control()
    task = start_core(cfg, lock, control)
    await asyncio.wait_for(in_backfill.wait(), timeout=5)
    control.begin("signal")
    await asyncio.sleep(0)
    go.set()  # the backfill finishes within the budget, after the stop request

    assert await asyncio.wait_for(task, timeout=5) == EXIT_CLEAN
    events = [ln["event"] for ln in _log_lines(buf)]
    assert "core.started" not in events and "core.stopped" in events


@pytest.mark.parametrize("target", ["bus.close", "engine.dispose", "source.stop"])
async def test_unsuccessful_cleanup_step_exits_1_and_cleanup_continues(
    cfg: AppConfig, lock: JournalLock, monkeypatch: pytest.MonkeyPatch, target: str
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    source = StreamingSource()
    if target == "bus.close":
        real_close = EventBus.close

        async def close(self: EventBus, budget_s: float = 20.0) -> None:
            await real_close(self, budget_s=budget_s)  # drain for real, then report failure
            raise RuntimeError("bus close failed")

        monkeypatch.setattr("autotrader.core.runtime.EventBus.close", close)
    elif target == "engine.dispose":
        real_dispose = AsyncEngine.dispose

        async def dispose(self: AsyncEngine, close: bool = True) -> None:
            await real_dispose(self, close)  # close the connections for real, then fail
            raise RuntimeError("dispose failed")

        monkeypatch.setattr(AsyncEngine, "dispose", dispose)
    else:

        async def cancelled_stop() -> None:
            source.stopped.set()
            raise asyncio.CancelledError

        source.stop = cancelled_stop  # type: ignore[method-assign]
    control, _ = make_control()

    task = start_core(cfg, lock, control, source=source)
    await _wait_for(lambda: any(ln["event"] == "core.started" for ln in _log_lines(buf)))
    control.begin("stop")

    assert await asyncio.wait_for(task, timeout=5) == EXIT_FAILED
    failed = next(ln for ln in _log_lines(buf) if ln["event"] == "core.shutdown_failed")
    assert failed["steps"] == [target]  # nothing else failed: the later steps still ran
    events = [ln["event"] for ln in _log_lines(buf)]
    assert "core.stopped" not in events and "core.shutdown_forced" not in events
    assert source.stopped.is_set()


class StopRaceSource(StreamingSource):
    """The stream errors at the moment the stop is requested."""

    def __init__(self) -> None:
        super().__init__()
        self.fail = asyncio.Event()

    async def stream(
        self, on_bar: Callable[[Bar], Awaitable[None]], on_quote: Callable[[Quote], Awaitable[None]]
    ) -> None:
        await self.fail.wait()
        raise ConnectionError("stream died at stop")


async def test_stream_error_at_the_stop_request_is_read_and_fails_the_exit(
    cfg: AppConfig, lock: JournalLock
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    loop = asyncio.get_running_loop()
    unhandled: list[dict[str, Any]] = []
    loop.set_exception_handler(lambda _loop, context: unhandled.append(context))
    source = StopRaceSource()
    control, _ = make_control()

    task = start_core(cfg, lock, control, source=source)
    await _wait_for(lambda: any(ln["event"] == "core.started" for ln in _log_lines(buf)))
    source.fail.set()
    control.begin("stop")
    code = await asyncio.wait_for(task, timeout=5)
    del task
    gc.collect()
    await asyncio.sleep(0)

    assert code == EXIT_FAILED
    failed = next(ln for ln in _log_lines(buf) if ln["event"] == "core.shutdown_failed")
    assert failed["steps"] == ["stream"]
    step = next(ln for ln in _log_lines(buf) if ln["event"] == "core.shutdown_step_failed")
    assert step["step"] == "stream" and "stream died at stop" in step["error"]
    assert "core.stopped" not in [ln["event"] for ln in _log_lines(buf)]
    assert unhandled == []  # no "Task exception was never retrieved"


async def test_stop_lets_a_bar_in_flight_finish_its_journal_writes(
    cfg: AppConfig, lock: JournalLock, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A stop that lands while the stream is persisting a bar must not cut that bar's writes:
    the stream gets the remaining budget to end on its own (CI flake on PR #23, where a bar
    row was committed without its BarClosed event)."""
    buf = io.StringIO()
    configure_logging(stream=buf)
    real_on_bar = MarketFeed.on_bar
    in_second, release = asyncio.Event(), asyncio.Event()
    seen: list[Bar] = []

    async def gated_on_bar(self: MarketFeed, bar: Bar) -> None:
        seen.append(bar)
        if len(seen) == 2:  # the second bar is still being handled when the stop arrives
            in_second.set()
            await release.wait()
        await real_on_bar(self, bar)

    monkeypatch.setattr(MarketFeed, "on_bar", gated_on_bar)
    control, _ = make_control()
    task = start_core(cfg, lock, control)
    await asyncio.wait_for(in_second.wait(), timeout=5)
    control.begin("stop")
    await _wait_for(lambda: any(ln["event"] == "core.stopping" for ln in _log_lines(buf)))
    release.set()  # teardown is under way: the bar finishes only if teardown waits for it

    assert await asyncio.wait_for(task, timeout=5) == EXIT_CLEAN
    engine = make_engine(cfg.database.url)
    try:
        async with make_sessionmaker(engine)() as s:
            assert await s.scalar(select(func.count()).select_from(m.BarRow)) == 2
            closed = [e.payload for e in await s.scalars(select(m.Event)) if e.type == "BarClosed"]
    finally:
        await engine.dispose()
    assert [p["timeframe"] for p in closed].count("1m") == 2
