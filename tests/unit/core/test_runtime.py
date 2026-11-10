import asyncio
import contextlib
import gc
import io
import json
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, MutableMapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Literal

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from structlog.testing import capture_logs

import tests.unit.risk.test_engine as te
from autotrader.broker.base import Account, AssetRules, BrokerError, FeeSchedule, Position
from autotrader.broker.sim import SimBroker
from autotrader.core.bus import CommitUncertain, EventBus
from autotrader.core.config import AppConfig, Settings, load_config, load_profile
from autotrader.core.feed import Latest, MarketFeed
from autotrader.core.flatten import request_flatten
from autotrader.core.kill import PAUSED_KEY, ControlGate, ControlStatus
from autotrader.core.ledger import CashGuard, CashResult, Ledger
from autotrader.core.lock import JournalLock, acquire
from autotrader.core.logging import configure_logging
from autotrader.core.runtime import ControlPoller, _snapshot_step, _sync_step, run_core
from autotrader.core.shutdown import EXIT_CLEAN, EXIT_FAILED, ShutdownControl
from autotrader.core.types import Bar, Quote
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade
from autotrader.risk.engine import RiskEngine
from tests.dbhooks import CommitHook, hooked_sessionmaker
from tests.health import seed_baseline

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

    def can_place_orders(self) -> bool:
        return False

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


Stage = Literal["guard", "snapshot", "check"]
STAGES: tuple[Stage, ...] = ("guard", "snapshot", "check")
GUARD = CashGuard(generation=(0, 0), unsettled=False)
ACCOUNT = Account(equity=Decimal("1000"), cash=Decimal("1000"))


class FakeStep:
    """Both sides of `_snapshot_step` (engine and ledger) over one call log, no DB.
    `fail[stage]` raises once in that stage. The fault mirrors the engine's rules:
    stickiness only goes false -> true, and `cash_reconciled()` is a no-op while sticky."""

    def __init__(self, result: CashResult = "ok") -> None:
        self.result: CashResult = result
        self.fail: dict[str, Exception] = {}
        self.calls: list[str] = []
        self.faults: list[tuple[str, bool]] = []
        self.cash_fault: str | None = "not yet reconciled"
        self.sticky = False

    def _stage(self, stage: Stage) -> None:
        self.calls.append(stage)
        error = self.fail.pop(stage, None)
        if error is not None:
            raise error

    async def cash_guard(self) -> CashGuard:
        self._stage("guard")
        return GUARD

    async def snapshot_equity(self) -> Account:
        self._stage("snapshot")
        return ACCOUNT

    async def check_cash(self, account: Account, guard: CashGuard) -> CashResult:
        assert account is ACCOUNT and guard is GUARD  # what the earlier stages returned
        self._stage("check")
        return self.result

    def set_cash_fault(self, reason: str, *, sticky: bool = False) -> None:
        self.faults.append((reason, sticky))
        if not self.sticky:
            self.cash_fault = reason
        self.sticky = self.sticky or sticky

    def cash_reconciled(self) -> None:
        self.calls.append("reconciled")
        if not self.sticky:
            self.cash_fault = None


def _events(logs: list[MutableMapping[str, Any]]) -> list[str]:
    return [e["event"] for e in logs]


@pytest.mark.parametrize("result", ["baseline", "ok", "discrepancy"])
async def test_snapshot_step_runs_guard_snapshot_check_in_order_and_clears_on_ok(
    result: CashResult,
) -> None:
    step = FakeStep(result)
    with capture_logs() as logs:
        await _snapshot_step(step, step)
    assert step.calls == ["guard", "snapshot", "check", "reconciled"]
    assert step.faults == [] and step.cash_fault is None
    assert _events(logs) == ["risk.snapshot_written"]


async def test_skipped_cash_check_leaves_the_fault_unchanged() -> None:
    step = FakeStep("skipped")
    with capture_logs() as logs:
        await _snapshot_step(step, step)
    assert step.calls == ["guard", "snapshot", "check"]  # no cash_reconciled()
    assert step.faults == [] and step.cash_fault == "not yet reconciled"
    assert _events(logs) == ["risk.snapshot_written"]
    step.set_cash_fault("check: ConnectionError")  # a later skipped step clears no fault
    await _snapshot_step(step, step)
    assert step.cash_fault == "check: ConnectionError"


async def test_failed_guard_still_takes_the_snapshot_and_skips_the_check() -> None:
    step = FakeStep()
    step.fail["guard"] = ConnectionError("journal down")
    with capture_logs() as logs:
        await _snapshot_step(step, step)
    assert step.calls == ["guard", "snapshot"]
    assert step.faults == [("guard: ConnectionError", False)]
    failed, written = logs
    assert (failed["event"], failed["stage"], failed["log_level"]) == (
        "ledger.cash_check_failed",
        "guard",
        "error",
    )
    assert failed["exc_info"]
    assert written["event"] == "risk.snapshot_written"


async def test_snapshot_failure_after_commit_sets_the_fault_and_skips_the_check() -> None:
    """`snapshot_equity()` may commit its row and then raise on the position read."""
    step = FakeStep()
    step.fail["snapshot"] = ConnectionError("positions down")
    with capture_logs() as logs:
        await _snapshot_step(step, step)
    assert step.calls == ["guard", "snapshot"]
    assert step.faults == [("snapshot: ConnectionError", False)]
    (failed,) = logs
    assert (failed["event"], failed["log_level"]) == ("risk.snapshot_failed", "error")
    assert failed["exc_info"] and "timeout" not in failed


async def test_failed_cash_check_sets_the_fault_and_logs_the_stage() -> None:
    step = FakeStep()
    step.fail["check"] = ConnectionError("journal down")
    with capture_logs() as logs:
        await _snapshot_step(step, step)
    assert step.calls == ["guard", "snapshot", "check"]
    assert step.faults == [("check: ConnectionError", False)]
    written, failed = logs
    assert written["event"] == "risk.snapshot_written"
    assert (failed["event"], failed["stage"], failed["log_level"]) == (
        "ledger.cash_check_failed",
        "check",
        "error",
    )
    assert failed["exc_info"]


@pytest.mark.parametrize("stage", STAGES)
async def test_commit_uncertain_in_any_stage_sets_a_sticky_fault(stage: Stage) -> None:
    step = FakeStep("ok")
    step.fail[stage] = CommitUncertain("CashChecked may or may not be committed")
    await _snapshot_step(step, step)
    assert step.faults == [(f"{stage}: CommitUncertain", True)]
    assert step.sticky and step.cash_fault == f"{stage}: CommitUncertain"
    await _snapshot_step(step, step)  # every stage succeeds, the check returns ok
    assert step.calls[-4:] == ["guard", "snapshot", "check", "reconciled"]
    assert step.cash_fault == f"{stage}: CommitUncertain"  # held until restart
    fresh = FakeStep("ok")  # a new process (engine) starts clear of it
    await _snapshot_step(fresh, fresh)
    assert fresh.cash_fault is None


@pytest.mark.parametrize("stage", STAGES)
async def test_plain_error_in_any_stage_sets_a_recoverable_fault(stage: Stage) -> None:
    step = FakeStep("ok")
    step.fail[stage] = ConnectionError("down")
    await _snapshot_step(step, step)
    assert step.faults == [(f"{stage}: ConnectionError", False)]
    assert not step.sticky and step.cash_fault == f"{stage}: ConnectionError"
    await _snapshot_step(step, step)
    assert step.cash_fault is None


async def test_snapshot_step_logs_failures_and_successes() -> None:
    step = FakeStep("ok")
    with capture_logs() as logs:
        step.fail["snapshot"] = TimeoutError()  # the engine's own budget
        await _snapshot_step(step, step)
        step.fail["snapshot"] = ConnectionError("broker down")
        await _snapshot_step(step, step)
        await _snapshot_step(step, step)
    events = [e for e in logs if e["event"].startswith("risk.snapshot")]
    assert [(e["event"], e.get("timeout")) for e in events] == [
        ("risk.snapshot_failed", True),
        ("risk.snapshot_failed", None),
        ("risk.snapshot_written", None),
    ]
    assert step.faults == [("snapshot: TimeoutError", False), ("snapshot: ConnectionError", False)]


@pytest.fixture
def journal_url(tmp_path: Path) -> Iterator[str]:
    """A migrated journal; sync, since `upgrade()` runs its own event loop."""
    url = f"sqlite+aiosqlite:///{tmp_path / 'cash.db'}"
    upgrade(url)
    yield url


class ArmingLedger:
    """The real ledger for the step; `arm` makes the next `check_cash` commit fail before it
    commits (armed only after the snapshot committed, so it hits the cash unit itself)."""

    def __init__(self, ledger: Ledger, hook: CommitHook) -> None:
        self.ledger = ledger
        self.hook = hook
        self.arm = False
        self.results: list[str] = []

    async def cash_guard(self) -> CashGuard:
        return await self.ledger.cash_guard()

    async def check_cash(self, account: Account, guard: CashGuard) -> CashResult:
        if self.arm:
            self.hook.fail_before, self.arm = 1, False
        try:
            result = await self.ledger.check_cash(account, guard)
        except Exception as e:
            self.results.append(type(e).__name__)
            raise
        self.results.append(result)
        return result


@dataclass
class CashRig:
    risk: RiskEngine
    ledger: ArmingLedger
    sim: SimBroker
    clock: Any
    sessions: async_sessionmaker[AsyncSession]

    async def step(self) -> None:
        self.clock.now += timedelta(seconds=60)  # `equity_snapshots.ts` is unique
        await _snapshot_step(self.risk, self.ledger)

    async def entry(self) -> tuple[str, ...]:
        self.clock.now += timedelta(seconds=1)  # check() writes its own peak snapshot
        result = await self.risk.check(te.intent(await te.new_cycle(self.sessions)))
        assert not result.decision.approved
        return result.decision.reasons

    async def paused(self) -> Any:
        async with self.sessions() as s:
            return await repo.get_control(s, PAUSED_KEY)


@contextlib.asynccontextmanager
async def cash_rig(url: str, kill_file: Path) -> AsyncIterator[CashRig]:
    """Real `RiskEngine` + `Ledger` + SimBroker over a commit-hooked journal, unhealthy at
    start (no loop snapshot, `not yet reconciled`), with a trusted day-start baseline."""
    db = make_engine(url)
    hook = CommitHook()
    sessions = hooked_sessionmaker(db, hook)
    clock = te.Clock(te.NOW)
    sim = SimBroker(
        cash=Decimal("1000"),
        fees=te.FEES,
        slippage_bps=Decimal(0),
        asset_rules={te.BTC: te.RULES},
        timeframe="5m",
    )
    sim.process_bar(te.five(29))
    ledger = Ledger(EventBus(sessions), sessions, clock=clock)
    gate = ControlGate(kill_file, sessions)
    risk = te.make_engine_(
        sim, sessions, gate, clock=clock, monotonic=te.Mono(), ledger=ledger, healthy=False
    )
    try:
        await seed_baseline(sessions, te.NOW)
        yield CashRig(risk, ArmingLedger(ledger, hook), sim, clock, sessions)
    finally:
        await db.dispose()


async def test_failed_discrepancy_commit_keeps_entries_closed(
    journal_url: str, tmp_path: Path
) -> None:
    """Spec 11 cases 10 and 14: the PR #29 probe (-19.84 USD), the `CashDiscrepancy` unit's
    commit fails and cannot be verified: no pause, entries still rejected, until restart."""
    async with cash_rig(journal_url, tmp_path / "KILL") as rig:
        await rig.step()
        assert rig.ledger.results == ["baseline"] and rig.risk.cash_fault is None

        rig.sim.cash -= Decimal("19.84")
        rig.ledger.arm = True
        await rig.step()
        assert rig.ledger.results[-1] == "CommitUncertain"
        assert await rig.paused() is None  # the pause rolled back with the event
        assert rig.risk.cash_fault == "check: CommitUncertain" and rig.risk.cash_fault_sticky
        assert await rig.entry() == ("cash reconciliation failed",)

        await rig.step()  # hook clear: the same discrepancy commits its pause now
        assert rig.ledger.results[-1] == "discrepancy"
        assert await rig.paused() is True
        assert rig.risk.cash_fault == "check: CommitUncertain" and rig.risk.cash_fault_sticky


async def test_discrepancy_clears_a_plain_fault_and_the_pause_rejects(
    journal_url: str, tmp_path: Path
) -> None:
    async with cash_rig(journal_url, tmp_path / "KILL") as rig:
        await rig.step()
        assert rig.ledger.results == ["baseline"]
        rig.risk.set_cash_fault("check: ConnectionError")
        rig.sim.cash -= Decimal("19.84")
        await rig.step()
        assert rig.ledger.results[-1] == "discrepancy"
        assert rig.risk.cash_fault is None and rig.risk.health_faults() == ()
        assert await rig.paused() is True
        assert await rig.entry() == ("control: trading paused (manual resume required)",)


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


# --- WP 1.5c: order-sync and Position Manager loops, shutdown.flatten -------------------


async def _insert_unsettled_order(cfg: AppConfig) -> None:
    """One unsettled buy row with broker id "b-1": the sync loop reads it from the broker."""
    now = datetime.now(UTC)
    engine = make_engine(cfg.database.url)
    try:
        async with make_sessionmaker(engine)() as s:
            s.add(
                m.Order(
                    broker_order_id="b-1",
                    symbol="BTC/USD",
                    mode="paper",
                    type="limit",
                    side="buy",
                    qty=Decimal("0.01"),
                    limit_price=Decimal(100),
                    status="new",
                    submitted_at=now,
                    updated_at=now,
                )
            )
            await s.commit()
    finally:
        await engine.dispose()


async def _control_value(cfg: AppConfig, key: str) -> Any:
    engine = make_engine(cfg.database.url)
    try:
        async with make_sessionmaker(engine)() as s:
            row = await s.get(m.ControlState, key)
            return None if row is None else row.value
    finally:
        await engine.dispose()


def _with_shutdown_flatten(cfg: AppConfig) -> AppConfig:
    return cfg.model_copy(update={"shutdown": cfg.shutdown.model_copy(update={"flatten": True})})


class HangingSyncBroker(FakeBroker):
    """An unsettled order makes the sync loop read it; every read hangs."""

    def __init__(self) -> None:
        self.can_calls = 0
        self.order_reads = 0

    def can_place_orders(self) -> bool:
        self.can_calls += 1
        return False

    async def get_order(self, order_id: str) -> Any:
        self.order_reads += 1
        await asyncio.Event().wait()


class HoldingBroker(FakeBroker):
    """Holds 0.5 BTC, well above `min_qty`, until `release()` is true; then holds nothing."""

    def __init__(self, release: Callable[[], bool] = lambda: False) -> None:
        self.release = release

    async def get_positions(self) -> list[Any]:
        if self.release():
            return []
        return [Position(symbol="BTC/USD", qty=Decimal("0.5"), avg_entry_price=Decimal(100))]

    async def asset_rules(self, symbol: str) -> AssetRules:
        return AssetRules(
            min_qty=Decimal("0.0001"),
            qty_increment=Decimal("0.0001"),
            min_notional=Decimal(1),
            price_increment=Decimal("0.01"),
        )


async def test_a_hung_order_sync_never_delays_the_position_manager(
    cfg: AppConfig, lock: JournalLock
) -> None:
    await _insert_unsettled_order(cfg)
    broker = HangingSyncBroker()

    async def connect(_: Settings) -> Any:
        return broker

    control, _ = make_control(timeout_s=0.5)
    task = start_core(cfg, lock, control, connect_broker=connect, sync_s=0.01, manage_s=0.01)
    try:
        await _wait_for(lambda: broker.order_reads >= 1)  # the sync step is hanging now
        seen = broker.can_calls
        await _wait_for(lambda: broker.can_calls >= seen + 3)  # manage keeps stepping
    finally:
        control.begin("stop")
        assert await asyncio.wait_for(task, timeout=15) in (EXIT_CLEAN, EXIT_FAILED)


async def test_shutdown_flatten_reports_flat_on_a_graceful_stop(
    cfg: AppConfig, lock: JournalLock
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    flatten_cfg = _with_shutdown_flatten(cfg)
    control, _ = make_control()

    task = start_core(flatten_cfg, lock, control, sync_s=0.01, manage_s=0.01)
    await _wait_for(lambda: any(ln["event"] == "core.started" for ln in _log_lines(buf)))
    control.begin("stop")

    assert await asyncio.wait_for(task, timeout=10) == EXIT_CLEAN
    flattened = next(ln for ln in _log_lines(buf) if ln["event"] == "core.shutdown_flattened")
    assert flattened["dust"] == {}
    result = await _control_value(cfg, "flatten_result")
    assert result["flat"] is True
    assert await _control_value(cfg, "flatten_requested") is None
    assert await _control_value(cfg, "paused") is True  # resuming is the operator's call


async def test_shutdown_flatten_times_out_and_teardown_continues(
    cfg: AppConfig, lock: JournalLock
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)

    async def connect(_: Settings) -> Any:
        return HoldingBroker()  # can't place orders: the holding is never sold

    control, _ = make_control(timeout_s=2.0)  # flatten waits ~1 s, teardown keeps ~1 s
    task = start_core(
        _with_shutdown_flatten(cfg),
        lock,
        control,
        connect_broker=connect,
        sync_s=0.01,
        manage_s=0.01,
    )
    await _wait_for(lambda: any(ln["event"] == "core.started" for ln in _log_lines(buf)))
    control.begin("stop")

    assert await asyncio.wait_for(task, timeout=10) == EXIT_CLEAN
    events = [ln["event"] for ln in _log_lines(buf)]
    assert "core.shutdown_flattened" not in events
    assert events.index("core.shutdown_flatten_incomplete") < events.index("core.stopped")
    incomplete = next(
        ln for ln in _log_lines(buf) if ln["event"] == "core.shutdown_flatten_incomplete"
    )
    assert incomplete["level"] == "error"
    # The Position Manager's first non-flat refresh runs at once (clarification 14).
    assert incomplete["remaining"]["holdings"] == [{"symbol": "BTC/USD", "qty": "0.5"}]


async def test_shutdown_flatten_keeps_a_pending_cli_request(
    cfg: AppConfig, lock: JournalLock
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    engine = make_engine(cfg.database.url)
    try:
        cli, created = await request_flatten(
            make_sessionmaker(engine), source="cli", reason="test", now=datetime.now(UTC)
        )
    finally:
        await engine.dispose()
    assert created
    control, _ = make_control()

    async def connect(_: Settings) -> Any:
        return HoldingBroker(release=lambda: control.requested)  # flat only after the stop

    task = start_core(
        _with_shutdown_flatten(cfg),
        lock,
        control,
        connect_broker=connect,
        sync_s=0.01,
        manage_s=0.01,
    )
    await _wait_for(lambda: any(ln["event"] == "core.started" for ln in _log_lines(buf)))
    control.begin("stop")

    assert await asyncio.wait_for(task, timeout=10) == EXIT_CLEAN
    result = await _control_value(cfg, "flatten_result")
    assert result["request_ts"] == cli.ts and result["flat"] is True
    assert "core.shutdown_flattened" in [ln["event"] for ln in _log_lines(buf)]


async def test_shutdown_flatten_is_skipped_after_a_startup_failure(
    cfg: AppConfig, lock: JournalLock
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)

    async def failing(_: Settings) -> Any:
        raise BrokerError("auth failed")

    control, _ = make_control()
    with pytest.raises(BrokerError, match="auth failed"):
        await start_core(_with_shutdown_flatten(cfg), lock, control, connect_broker=failing)
    skipped = next(ln for ln in _log_lines(buf) if ln["event"] == "core.shutdown_flatten_skipped")
    assert skipped["level"] == "warning"
    assert await _control_value(cfg, "flatten_requested") is None


class FailingSync:
    async def sync_orders(self) -> Any:
        raise ConnectionError("broker down")


class NoLedger:
    async def apply_all(self, result: Any) -> frozenset[str]:
        raise AssertionError("nothing to book after a failed read")


async def test_sync_step_logs_a_failure_and_never_raises() -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    await _sync_step(FailingSync(), NoLedger())  # type: ignore[arg-type]
    failed = next(ln for ln in _log_lines(buf) if ln["event"] == "ledger.sync_failed")
    assert "broker down" in failed["exception"]
