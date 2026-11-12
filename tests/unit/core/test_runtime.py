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
from sqlalchemy import func, select, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from structlog.testing import capture_logs

import tests.unit.risk.test_engine as te
from autotrader.broker.base import (
    Account,
    AssetRules,
    BrokerError,
    BrokerOrder,
    FeeSchedule,
    OrderAck,
    Position,
)
from autotrader.broker.sim import SimBroker
from autotrader.core import runtime
from autotrader.core.admission import AdmissionReadiness
from autotrader.core.bus import CommitUncertain, EventBus
from autotrader.core.config import AppConfig, Settings, load_config, load_profile
from autotrader.core.cycle import CloseReport, CycleRunner
from autotrader.core.feed import BarClosed, Latest, MarketFeed
from autotrader.core.flatten import request_flatten
from autotrader.core.kill import PAUSED_KEY, ControlGate, ControlStatus
from autotrader.core.ledger import CashGuard, CashResult, Ledger
from autotrader.core.lock import JournalLock, acquire
from autotrader.core.logging import configure_logging
from autotrader.core.runtime import ControlPoller, _snapshot_step, _sync_step, run_core
from autotrader.core.scanner import CandidateSignal, Scanner
from autotrader.core.shutdown import EXIT_CLEAN, EXIT_FAILED, ShutdownControl
from autotrader.core.supervise import Supervisor
from autotrader.core.types import Bar, Quote
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade
from autotrader.risk.engine import RiskEngine
from tests.dbhooks import CommitHook, hooked_sessionmaker
from tests.health import seed_baseline
from tests.pm_harness import Harness, make_sim

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


@pytest.mark.parametrize("order", ["before_run_core_saw_the_stop", "after_teardown_began"])
async def test_startup_raising_after_the_stop_won_the_race(
    cfg: AppConfig, lock: JournalLock, monkeypatch: pytest.MonkeyPatch, order: str
) -> None:
    control, _ = make_control()
    teardown_began = asyncio.Event()
    real_teardown = runtime._teardown

    async def teardown(*args: Any, **kwargs: Any) -> list[str]:
        teardown_began.set()
        return await real_teardown(*args, **kwargs)

    # A failed backfill no longer fails startup (WP 1.5d-3); a failing broker check still does.
    async def check_broker(*args: Any) -> Any:
        control.begin("signal")  # the stop request wins the race against the startup
        if order == "after_teardown_began":
            await teardown_began.wait()
        raise RuntimeError("startup broke")

    monkeypatch.setattr("autotrader.core.runtime._teardown", teardown)
    monkeypatch.setattr("autotrader.core.runtime._check_broker", check_broker)
    with capture_logs() as logs:
        if order == "before_run_core_saw_the_stop":
            # The startup task is done (raised) by the time run_core looks: a startup failure.
            with pytest.raises(RuntimeError, match="startup broke"):
                await asyncio.wait_for(start_core(cfg, lock, control), timeout=5)
        else:
            # Teardown awaits the unfinished startup as its first step: that step failed.
            assert await asyncio.wait_for(start_core(cfg, lock, control), timeout=5) == (
                EXIT_FAILED
            )
    events = [e["event"] for e in logs]
    assert teardown_began.is_set() and control.cause == "signal"
    if order == "before_run_core_saw_the_stop":
        (failed,) = [e for e in logs if e["event"] == "core.failed"]
        assert failed["cause"] == "signal" and str(failed["exc_info"]) == "startup broke"
        assert control.failure_logged  # so the CLI (exit 1) does not log it again
        assert "core.shutdown_step_failed" not in events
    else:
        (step,) = [e for e in logs if e["event"] == "core.shutdown_step_failed"]
        assert step["step"] == "startup" and "startup broke" in step["error"]
        assert "core.failed" not in events and not control.failure_logged
        (shutdown_failed,) = [e for e in logs if e["event"] == "core.shutdown_failed"]
        assert shutdown_failed["steps"] == ["startup"]


async def test_stream_death_does_not_also_log_shutdown_failed(
    cfg: AppConfig, lock: JournalLock
) -> None:
    control, _ = make_control()
    with capture_logs() as logs, pytest.raises(ConnectionError, match="stream died"):
        await asyncio.wait_for(
            start_core(cfg, lock, control, source=StreamingSource(fail_stream=True)), timeout=5
        )
    events = [e["event"] for e in logs]
    assert events.count("core.failed") == 1 and control.failure_logged
    assert "core.shutdown_failed" not in events and "core.shutdown_step_failed" not in events
    assert "core.stopped" in events  # the rest of the teardown was clean


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
        return [
            Position(
                symbol="BTC/USD",
                qty=Decimal("0.5"),
                qty_available=Decimal("0.5"),
                avg_entry_price=Decimal(100),
            )
        ]

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


# --- WP 1.5d-3: startup wiring and admission readiness ----------------------------------


class QuotingSource(StreamingSource):
    """Delivers a quote per scripted symbol (bid, ask = bid + 0.01) at stream start, then the
    two live bars (at the symbol's bid, else 1) and idles. `fail_history` makes the backfill
    raise (a market-data REST outage); `warm` returns three hours of 1m bars around 100 with a
    range, so the 5m ATR exists. `streamed` is set once `stream()` is entered."""

    def __init__(
        self,
        bids: dict[str, float] | None = None,
        *,
        fail_history: bool = False,
        warm: bool = False,
    ) -> None:
        super().__init__()
        self.bids = bids or {}
        self.fail_history = fail_history
        self.warm = warm
        self.streamed = False

    async def history(self, symbols: Sequence[str], start: datetime, end: datetime) -> list[Bar]:
        if self.fail_history:
            raise ConnectionError("data API down")
        if not self.warm:
            return []
        bars: list[Bar] = []
        ts = max(start, end - timedelta(hours=3))
        while ts < end:
            for symbol in symbols:
                bars.append(
                    Bar(
                        symbol=symbol,
                        timeframe="1m",
                        ts=ts,
                        open=100,
                        high=101,
                        low=99,
                        close=100,
                        volume=1,
                    )
                )
            ts += timedelta(minutes=1)
        return bars

    async def stream(
        self, on_bar: Callable[[Bar], Awaitable[None]], on_quote: Callable[[Quote], Awaitable[None]]
    ) -> None:
        self.streamed = True
        now = datetime.now(UTC)
        for symbol, bid in self.bids.items():
            await on_quote(Quote(symbol=symbol, ts=now, bid=bid, ask=bid + 0.01))
        ts = now.replace(second=0, microsecond=0)
        for symbol in ("BTC/USD", "ETH/USD"):
            price = self.bids.get(symbol, 1.0)
            await on_bar(
                Bar(
                    symbol=symbol,
                    timeframe="1m",
                    ts=ts,
                    open=price,
                    high=price,
                    low=price,
                    close=price,
                    volume=1,
                )
            )
        await self.stopped.wait()


def sim_connect(broker: object) -> Callable[[Settings], Awaitable[Any]]:
    """An async broker connect returning `broker` (a `SimBroker` or a fake)."""

    async def connect(_: Settings) -> Any:
        return broker

    return connect


async def seed_position(
    cfg: AppConfig, sim: SimBroker, *, stop: str = "95", opened_at: datetime | None = None
) -> int:
    """A booked BTC position on the cfg journal with a filled `sim` holding (Harness). With
    `opened_at`, the row is reopened then with no time limit; otherwise it keeps the Harness's
    2026-10-19 open, so its time exit is overdue."""
    engine = make_engine(cfg.database.url)
    sessions = make_sessionmaker(engine)
    bus = EventBus(sessions)
    try:
        pid = await Harness(sessions, bus, sim=sim).open_position(stop=stop)
        if opened_at is not None:
            async with sessions() as s:
                await s.execute(
                    update(m.Position)
                    .where(m.Position.id == pid)
                    .values(opened_at=opened_at, time_limit_at=None)
                )
                await s.commit()
        return pid
    finally:
        await bus.close()
        await engine.dispose()  # before run_core opens the journal


async def _count(cfg: AppConfig, model: Any, *where: Any) -> int:
    engine = make_engine(cfg.database.url)
    try:
        async with make_sessionmaker(engine)() as s:
            return int(await s.scalar(select(func.count()).select_from(model).where(*where)) or 0)
    finally:
        await engine.dispose()


def _record_bus(monkeypatch: pytest.MonkeyPatch) -> list[EventBus]:
    """The core's bus, to publish candidates on it."""
    buses: list[EventBus] = []

    class RecordingBus(EventBus):
        def __init__(self, *a: Any, **kw: Any) -> None:
            super().__init__(*a, **kw)
            buses.append(self)

    monkeypatch.setattr(runtime, "EventBus", RecordingBus)
    return buses


def _candidate(symbol: str = "BTC/USD", score: float = 0.8) -> CandidateSignal:
    now = datetime.now(UTC)
    return CandidateSignal(
        ts=now,
        symbol=symbol,
        direction="long",
        rule_id="r1",
        score=score,
        timeframe="5m",
        bar_ts=now.replace(second=0, microsecond=0) - timedelta(minutes=5),  # fresh
        features={},
    )


def _named(buf: io.StringIO, event: str) -> list[dict[str, Any]]:
    return [ln for ln in _log_lines(buf) if ln["event"] == event]


def _spy(monkeypatch: pytest.MonkeyPatch, name: str) -> list[int]:
    """Counts the calls of a module-level step in `runtime` (successful steps log nothing)."""
    calls: list[int] = []
    real = getattr(runtime, name)

    async def spy(*a: Any) -> None:
        calls.append(1)
        await real(*a)

    monkeypatch.setattr(runtime, name, spy)
    return calls


class LiveBroker(FakeBroker):
    def mode(self) -> str:
        return "live"


async def test_mode_mismatch_fails_startup(
    cfg: AppConfig, lock: JournalLock, monkeypatch: pytest.MonkeyPatch
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    source = QuotingSource()
    supervised: list[int] = []
    monkeypatch.setattr(Supervisor, "start", lambda self: supervised.append(1))
    control, _ = make_control()
    with pytest.raises(RuntimeError) as raised:
        await asyncio.wait_for(
            start_core(cfg, lock, control, source=source, connect_broker=sim_connect(LiveBroker())),
            timeout=5,
        )
    # Both modes, never a setting value.
    assert str(raised.value) == "broker mode 'live' does not match config mode 'paper'"
    assert control.cause == "startup_failure"
    events = [ln["event"] for ln in _log_lines(buf)]
    assert "core.failed" in events and "core.started" not in events
    assert supervised == [] and not source.streamed


class CountingBroker(FakeBroker):
    """Cannot place orders; counts the calls (the Position Manager asks every step)."""

    def __init__(self) -> None:
        self.can_calls = 0

    def can_place_orders(self) -> bool:
        self.can_calls += 1
        return False


async def test_broker_that_cannot_trade_keeps_admission_disabled(
    cfg: AppConfig, lock: JournalLock, monkeypatch: pytest.MonkeyPatch
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    buses = _record_bus(monkeypatch)
    admits = _spy(monkeypatch, "_admission_step")
    syncs = _spy(monkeypatch, "_sync_step")
    broker = CountingBroker()
    control, _ = make_control()
    task = start_core(
        cfg,
        lock,
        control,
        connect_broker=sim_connect(broker),
        snapshot_s=0.01,
        sync_s=0.01,
        manage_s=0.01,
    )
    try:
        await _wait_for(lambda: bool(_named(buf, "core.started")))
        seen = broker.can_calls
        await _wait_for(lambda: broker.can_calls >= seen + 3)  # positions.manage steps
        await _wait_for(lambda: len(syncs) >= 3 and len(admits) >= 3)
        await buses[0].publish(_candidate())
        await _wait_for(lambda: bool(_named(buf, "cycle.dropped")))
    finally:
        control.begin("stop")
        assert await asyncio.wait_for(task, timeout=5) == EXIT_CLEAN
    (disabled,) = _named(buf, "cycle.admission_disabled")
    assert (disabled["reason"], disabled["level"]) == ("broker cannot place orders", "warning")
    assert _named(buf, "cycle.admission_open") == []
    assert [d["reason"] for d in _named(buf, "cycle.dropped")] == [
        "admission disabled: broker cannot place orders"
    ]


async def _wait_for_sell(cfg: AppConfig) -> None:
    engine = make_engine(cfg.database.url)
    try:
        sessions = make_sessionmaker(engine)
        async with asyncio.timeout(5):
            while True:
                async with sessions() as s:
                    sells = await s.scalar(
                        select(func.count()).select_from(m.Order).where(m.Order.side == "sell")
                    )
                if sells:
                    return
                await asyncio.sleep(0.01)
    finally:
        await engine.dispose()


async def test_recover_failure_keeps_exits_running(
    cfg: AppConfig, lock: JournalLock, monkeypatch: pytest.MonkeyPatch
) -> None:
    sim = make_sim()
    await seed_position(cfg, sim, stop="95", opened_at=datetime.now(UTC))
    cycles, entries = await _count(cfg, m.Cycle), await _count(cfg, m.Order, m.Order.side == "buy")
    buf = io.StringIO()
    configure_logging(stream=buf)
    buses = _record_bus(monkeypatch)

    async def broken(self: CycleRunner) -> None:
        raise OperationalError("SELECT cycles", None, Exception("database is locked"))

    monkeypatch.setattr(CycleRunner, "recover", broken)
    control, _ = make_control()
    task = start_core(
        cfg,
        lock,
        control,
        source=QuotingSource({"BTC/USD": 90.0}),  # below the stop
        connect_broker=sim_connect(sim),
        sync_s=0.01,
        manage_s=0.01,
    )
    try:
        await _wait_for(lambda: bool(_named(buf, "core.started")))
        await _wait_for_sell(cfg)
        await buses[0].publish(_candidate())
        await _wait_for(lambda: bool(_named(buf, "cycle.dropped")))
    finally:
        control.begin("stop")
        assert await asyncio.wait_for(task, timeout=5) == EXIT_CLEAN
    (disabled,) = _named(buf, "cycle.admission_disabled")
    assert (disabled["reason"], disabled["level"]) == ("recover failed", "error")
    assert "database is locked" in disabled["exception"]
    (triggered,) = _named(buf, "positions.exit_triggered")
    assert triggered["reason"] == "stop"
    assert [d["reason"] for d in _named(buf, "cycle.dropped")] == [
        "admission disabled: recover failed"
    ]
    assert await _count(cfg, m.Cycle) == cycles
    assert await _count(cfg, m.Order, m.Order.side == "buy") == entries


def _sim_bar(symbol: str, price: float) -> Bar:
    return Bar(
        symbol=symbol,
        timeframe="1m",
        ts=datetime.now(UTC).replace(second=0, microsecond=0),
        open=price,
        high=price,
        low=price,
        close=price,
        volume=1,
    )


async def _wait_for_settled_sell(cfg: AppConfig) -> None:
    async with asyncio.timeout(5):
        while not await _count(
            cfg, m.Order, m.Order.side == "sell", m.Order.settled_at.is_not(None)
        ):
            await asyncio.sleep(0.01)


async def test_backfill_failure_disables_admission_and_keeps_exits(
    cfg: AppConfig, lock: JournalLock, monkeypatch: pytest.MonkeyPatch
) -> None:
    sim = make_sim()
    await seed_position(cfg, sim)  # opened 2026-10-19: the time exit is overdue
    cycles = await _count(cfg, m.Cycle)
    buf = io.StringIO()
    configure_logging(stream=buf)
    buses = _record_bus(monkeypatch)
    admits = _spy(monkeypatch, "_admission_step")
    ticks: list[int] = []
    real_tick = MarketFeed.tick

    async def tick(self: MarketFeed) -> None:
        ticks.append(1)
        await real_tick(self)

    monkeypatch.setattr(MarketFeed, "tick", tick)
    scanned: list[BarClosed] = []  # the bound method is taken at subscribe time

    async def on_bar_closed(self: Scanner, event: BarClosed) -> None:
        scanned.append(event)

    monkeypatch.setattr(Scanner, "on_bar_closed", on_bar_closed)
    control, _ = make_control()
    task = start_core(
        cfg,
        lock,
        control,
        source=QuotingSource(fail_history=True),
        connect_broker=sim_connect(sim),
        snapshot_s=0.05,  # readiness would get its chance to open within the test
        sync_s=0.01,
        manage_s=0.01,
    )
    try:
        await _wait_for(lambda: bool(_named(buf, "core.started")))
        await _wait_for_sell(cfg)
        seen = len(ticks)
        await _wait_for(lambda: len(ticks) >= seen + 3)  # feed.tick keeps beating
        sim.process_bar(_sim_bar("BTC/USD", 100.0))  # fills the exit
        await _wait_for_settled_sell(cfg)
        # A cash check after the settled exit, then many readiness ticks: with admission
        # enabled, readiness would open here (no position left, nothing to hold).
        seen = len(_named(buf, "risk.snapshot_written"))
        await _wait_for(lambda: len(_named(buf, "risk.snapshot_written")) >= seen + 2)
        seen = len(admits)
        await _wait_for(lambda: len(admits) >= seen + 20)
        await buses[0].publish(_candidate())
        await _wait_for(lambda: bool(_named(buf, "cycle.dropped") or _named(buf, "cycle.admitted")))
    finally:
        control.begin("stop")
        assert await asyncio.wait_for(task, timeout=5) == EXIT_CLEAN
    (failed,) = _named(buf, "feed.backfill_failed")
    assert failed["level"] == "error" and "data API down" in failed["exception"]
    (disabled,) = _named(buf, "cycle.admission_disabled")
    assert (disabled["reason"], disabled["level"]) == ("backfill failed", "error")
    assert _named(buf, "cycle.admission_open") == []
    assert [d["reason"] for d in _named(buf, "cycle.dropped")] == [
        "admission disabled: backfill failed"
    ]
    assert _named(buf, "scanner.subscribed") == []
    # The live bars were announced, and the scanner never received one.
    assert await _count(cfg, m.Event, m.Event.type == "BarClosed") == 2
    assert scanned == []
    assert _named(buf, "feed.tick.task_died") == []
    (triggered,) = _named(buf, "positions.exit_triggered")
    assert triggered["reason"] == "time"
    assert await _count(cfg, m.Order, m.Order.side == "sell") == 1
    assert await _count(cfg, m.BarRow) == 2  # the live bars persist on an empty history
    assert await _count(cfg, m.Cycle) == cycles


UNIVERSE_BIDS = {"BTC/USD": 100.0, "ETH/USD": 100.0, "SOL/USD": 100.0}


def _hold_first_snapshot(monkeypatch: pytest.MonkeyPatch) -> asyncio.Event:
    """The cash check waits for the returned event: readiness cannot open before it."""
    hold = asyncio.Event()
    real_snapshot = runtime._snapshot_step

    async def held_snapshot(*a: Any) -> None:
        await hold.wait()
        await real_snapshot(*a)

    monkeypatch.setattr(runtime, "_snapshot_step", held_snapshot)
    return hold


async def test_candidate_before_readiness_is_dropped_and_after_is_admitted(
    cfg: AppConfig, lock: JournalLock, monkeypatch: pytest.MonkeyPatch
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    buses = _record_bus(monkeypatch)
    hold = _hold_first_snapshot(monkeypatch)
    control, _ = make_control()
    task = start_core(
        cfg,
        lock,
        control,
        source=QuotingSource(UNIVERSE_BIDS),
        connect_broker=sim_connect(make_sim()),
        snapshot_s=0.05,
        sync_s=0.01,
        manage_s=0.01,
    )
    try:
        await _wait_for(lambda: bool(_named(buf, "core.started")))
        await buses[0].publish(_candidate())
        await _wait_for(lambda: bool(_named(buf, "cycle.dropped")))
        hold.set()
        await _wait_for(lambda: bool(_named(buf, "cycle.admission_open")))
        await buses[0].publish(_candidate())
        await _wait_for(lambda: bool(_named(buf, "cycle.admitted")))
    finally:
        control.begin("stop")
        assert await asyncio.wait_for(task, timeout=5) in (EXIT_CLEAN, EXIT_FAILED)
    assert [d["reason"] for d in _named(buf, "cycle.dropped")] == ["startup reconciliation pending"]
    assert _named(buf, "cycle.admission_disabled") == []
    assert await _count(cfg, m.Cycle) == 1  # only its admission is asserted (it expires)


async def test_held_symbol_outside_the_universe_is_logged(
    cfg: AppConfig, lock: JournalLock
) -> None:
    engine = make_engine(cfg.database.url)
    try:
        async with make_sessionmaker(engine)() as s:
            s.add(
                m.Position(
                    symbol="DOGE/USD",
                    side="long",
                    qty=Decimal("10"),
                    entry_price=Decimal("0.1"),
                    opened_at=datetime.now(UTC),
                    fees=Decimal(0),
                    dust_consumed=Decimal(0),
                )
            )
            await s.commit()
    finally:
        await engine.dispose()
    buf = io.StringIO()
    configure_logging(stream=buf)
    control, _ = make_control()
    task = start_core(cfg, lock, control)
    try:
        await _wait_for(lambda: bool(_named(buf, "core.started")))
    finally:
        control.begin("stop")
        assert await asyncio.wait_for(task, timeout=5) == EXIT_CLEAN
    (unquoted,) = _named(buf, "admission.unquoted_holdings")
    assert (unquoted["symbols"], unquoted["level"]) == (["DOGE/USD"], "warning")


class BrokenSessions:
    def __call__(self) -> Any:
        raise OperationalError("SELECT positions", None, Exception("database is locked"))


async def test_unquoted_holdings_read_failure_is_logged_not_raised() -> None:
    with capture_logs() as logs:
        await runtime._unquoted_holdings(BrokenSessions(), ["BTC/USD"])  # type: ignore[arg-type]
    (failed,) = logs
    assert (failed["event"], failed["log_level"]) == (
        "admission.unquoted_holdings_failed",
        "warning",
    )
    assert failed["exc_info"]


async def test_dropping_handler_logs_the_reason_only() -> None:
    with capture_logs() as logs:
        await runtime._dropping("recover failed")(_candidate())
    assert logs == [
        {
            "event": "cycle.dropped",
            "log_level": "info",
            "symbol": "BTC/USD",
            "rule_id": "r1",
            "reason": "admission disabled: recover failed",
        }
    ]


class FakeAdmissionRunner:
    def __init__(self, open_: bool) -> None:
        self.admission_open = open_
        self.opened = 0

    def open_admission(self) -> None:
        self.opened += 1


class FakeReadiness:
    def __init__(self, ready: bool) -> None:
        self.result = ready
        self.calls = 0

    async def ready(self) -> bool:
        self.calls += 1
        return self.result


@pytest.mark.parametrize(
    ("already_open", "ready", "calls", "opened"),
    [(True, True, 0, 0), (False, False, 1, 0), (False, True, 1, 1)],
)
async def test_admission_step_opens_only_when_ready(
    already_open: bool, ready: bool, calls: int, opened: int
) -> None:
    runner, readiness = FakeAdmissionRunner(already_open), FakeReadiness(ready)
    await runtime._admission_step(readiness, runner)  # type: ignore[arg-type]
    assert (readiness.calls, runner.opened) == (calls, opened)


# --- WP 1.5d-3: teardown closes the runner first ---------------------------------------


def _record_close(
    monkeypatch: pytest.MonkeyPatch,
    order: list[str],
    report: CloseReport | None = None,
    began: Callable[[], None] | None = None,
) -> list[float]:
    """Wraps `CycleRunner.close`: records the call in `order` and returns the budgets it got;
    `began` runs once `closing` is set; `report` replaces the real report."""
    budgets: list[float] = []
    real = CycleRunner.close

    async def close(self: CycleRunner, budget_s: float = 20.0, cancel_wait_s: float = 3.0) -> Any:
        order.append("runner.close")
        budgets.append(budget_s)
        closing = asyncio.ensure_future(real(self, budget_s, cancel_wait_s))
        await asyncio.sleep(0)  # `closing` is set before close()'s first await
        if began is not None:
            began()
        result = await closing
        return result if report is None else report

    monkeypatch.setattr(CycleRunner, "close", close)
    return budgets


async def test_teardown_closes_the_runner_before_flatten_and_source_stop(
    cfg: AppConfig, lock: JournalLock, monkeypatch: pytest.MonkeyPatch
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    order: list[str] = []
    budgets = _record_close(monkeypatch, order)
    real_request = request_flatten

    async def request(*a: Any, **kw: Any) -> Any:
        result = await real_request(*a, **kw)
        order.append("flatten.request")  # the request row is written
        return result

    monkeypatch.setattr(runtime, "request_flatten", request)
    source = StreamingSource()
    real_stop = source.stop

    async def stop() -> None:
        order.append("source.stop")
        await real_stop()

    source.stop = stop  # type: ignore[method-assign]
    control, _ = make_control()
    task = start_core(
        _with_shutdown_flatten(cfg), lock, control, source=source, sync_s=0.01, manage_s=0.01
    )
    await _wait_for(lambda: bool(_named(buf, "core.started")))
    control.begin("stop")
    assert await asyncio.wait_for(task, timeout=10) == EXIT_CLEAN
    assert order == ["runner.close", "flatten.request", "source.stop"]
    (budget,) = budgets
    assert 0.0 <= budget <= 5.0 - runtime.CLOSE_MARGIN_S  # below what was left


async def test_abandoned_cycle_makes_the_exit_unclean(
    cfg: AppConfig, lock: JournalLock, monkeypatch: pytest.MonkeyPatch
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    _record_close(monkeypatch, [], report=CloseReport((42,)))
    control, _ = make_control()
    task = start_core(cfg, lock, control)
    await _wait_for(lambda: bool(_named(buf, "core.started")))
    control.begin("stop")
    assert await asyncio.wait_for(task, timeout=5) == EXIT_FAILED
    (forced,) = _named(buf, "core.shutdown_forced")
    assert "cycle:42" in forced["steps"]
    (abandoned,) = _named(buf, "core.shutdown_abandoned")
    assert "cycle:42" in abandoned["steps"]
    assert abandoned["note"] == "left for the startup sequence; not proof that broker work stopped"


class StubbornSubmit(SimBroker):
    """`submit_order` ignores cancellation until `stubborn.release` is set."""

    stubborn: Stubborn
    entered: asyncio.Event

    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        if order.side == "buy":
            self.entered.set()
            await self.stubborn.wait()
        return await super().submit_order(order)


class CooperativeSubmit(te.HangingSubmit):
    entered: asyncio.Event

    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        self.entered.set()
        return await super().submit_order(order)


async def _hang_a_check_and_stop(
    cfg: AppConfig,
    lock: JournalLock,
    monkeypatch: pytest.MonkeyPatch,
    sim: SimBroker,
    entered: asyncio.Event,
    buf: io.StringIO,
) -> int:
    """Admission opens for real, one candidate is admitted and hangs in `check()`'s submit,
    then the stop is requested with a 3 s shutdown budget. Returns the exit code."""
    engine = make_engine(cfg.database.url)
    try:  # a trusted day-start baseline, so check() approves the entry
        await seed_baseline(make_sessionmaker(engine), datetime.now(UTC))
    finally:
        await engine.dispose()
    buses = _record_bus(monkeypatch)
    control, _ = make_control(timeout_s=3.0)
    task = start_core(
        cfg,
        lock,
        control,
        source=QuotingSource(UNIVERSE_BIDS, warm=True),
        connect_broker=sim_connect(sim),
        sync_s=0.01,
        manage_s=0.01,
    )
    try:
        await _wait_for(lambda: bool(_named(buf, "cycle.admission_open")))
        await buses[0].publish(_candidate())
        await asyncio.wait_for(entered.wait(), timeout=5)
    finally:
        control.begin("stop")
        code = await asyncio.wait_for(task, timeout=10)
    return code


def _record_engines(monkeypatch: pytest.MonkeyPatch) -> list[AsyncEngine]:
    """The core's journal engine, to dispose it again after left-behind work ran on it."""
    engines: list[AsyncEngine] = []

    def recording(url: str) -> AsyncEngine:
        engines.append(make_engine(url))
        return engines[-1]

    monkeypatch.setattr(runtime, "make_engine", recording)
    return engines


async def _drain_left_behind(engines: list[AsyncEngine]) -> None:
    """Let the work `runner.close` left behind finish, then close the connections it opened
    after teardown disposed the journal engine."""
    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    if pending:
        await asyncio.wait(pending, timeout=5)
    for engine in engines:
        await engine.dispose()


async def test_abandoned_cycle_is_reported_with_a_short_shutdown_budget(
    cfg: AppConfig, lock: JournalLock, monkeypatch: pytest.MonkeyPatch
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    sim = make_sim(cls=StubbornSubmit)
    assert isinstance(sim, StubbornSubmit)
    sim.stubborn, sim.entered = Stubborn(), asyncio.Event()
    engines = _record_engines(monkeypatch)
    try:
        assert await _hang_a_check_and_stop(cfg, lock, monkeypatch, sim, sim.entered, buf) == (
            EXIT_FAILED
        )
    finally:
        sim.stubborn.release.set()
        await _drain_left_behind(engines)
    (admitted,) = _named(buf, "cycle.admitted")
    (forced,) = _named(buf, "core.shutdown_forced")
    assert f"cycle:{admitted['cycle_id']}" in forced["steps"]  # not only "runner.close"
    assert "runner.close" not in forced["steps"]  # close() returned its report in time
    (abandoned,) = _named(buf, "cycle.task_abandoned")
    assert abandoned["cycle_id"] == admitted["cycle_id"]


async def test_cooperative_hung_check_is_not_abandoned(
    cfg: AppConfig, lock: JournalLock, monkeypatch: pytest.MonkeyPatch
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    sim = make_sim(cls=CooperativeSubmit)
    assert isinstance(sim, CooperativeSubmit)
    sim.entered = asyncio.Event()
    engines = _record_engines(monkeypatch)
    try:
        await _hang_a_check_and_stop(cfg, lock, monkeypatch, sim, sim.entered, buf)
    finally:
        await _drain_left_behind(engines)
    (admitted,) = _named(buf, "cycle.admitted")
    (cancelled,) = _named(buf, "cycle.forced_cancel")
    assert cancelled["cycle_id"] == admitted["cycle_id"]
    assert _named(buf, "cycle.task_abandoned") == []
    for forced in _named(buf, "core.shutdown_forced"):
        assert not any(step.startswith("cycle:") for step in forced["steps"])


async def test_runner_close_is_skipped_when_startup_failed(
    cfg: AppConfig, lock: JournalLock, monkeypatch: pytest.MonkeyPatch
) -> None:
    order: list[str] = []
    _record_close(monkeypatch, order)

    async def failing(_: Settings) -> Any:
        raise BrokerError("auth failed")

    control, _ = make_control()
    with pytest.raises(BrokerError, match="auth failed"):
        await start_core(cfg, lock, control, connect_broker=failing)
    assert order == []


async def test_stop_during_recover_skips_runner_close(
    cfg: AppConfig, lock: JournalLock, monkeypatch: pytest.MonkeyPatch
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    order: list[str] = []
    _record_close(monkeypatch, order)
    in_recover, release = asyncio.Event(), asyncio.Event()

    async def slow_recover(self: CycleRunner) -> None:
        in_recover.set()
        await release.wait()

    monkeypatch.setattr(CycleRunner, "recover", slow_recover)
    control, _ = make_control()
    task = start_core(cfg, lock, control)
    await asyncio.wait_for(in_recover.wait(), timeout=5)
    control.begin("signal")
    await asyncio.sleep(0)
    release.set()  # recover() finishes within the budget, after the stop request
    assert await asyncio.wait_for(task, timeout=5) == EXIT_CLEAN
    assert order == []
    events = [ln["event"] for ln in _log_lines(buf)]
    assert "core.started" not in events and "core.stopped" in events


async def test_readiness_cannot_open_admission_after_close_began(
    cfg: AppConfig, lock: JournalLock, monkeypatch: pytest.MonkeyPatch
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    in_ready, release = asyncio.Event(), asyncio.Event()
    opened: list[int] = []
    answered: list[int] = []

    async def late_ready(self: AdmissionReadiness) -> bool:
        in_ready.set()
        await release.wait()
        if answered:  # True once only: later ticks must not depend on timing
            return False
        answered.append(1)
        return True

    real_open = CycleRunner.open_admission

    def open_admission(self: CycleRunner) -> None:
        opened.append(1)
        real_open(self)

    monkeypatch.setattr(AdmissionReadiness, "ready", late_ready)
    monkeypatch.setattr(CycleRunner, "open_admission", open_admission)
    _record_close(monkeypatch, [], began=release.set)
    control, _ = make_control()
    task = start_core(cfg, lock, control, connect_broker=sim_connect(make_sim()))
    await asyncio.wait_for(in_ready.wait(), timeout=5)
    control.begin("stop")
    assert await asyncio.wait_for(task, timeout=5) == EXIT_CLEAN
    assert opened == [1]  # the late tick reached open_admission() ...
    assert _named(buf, "cycle.admission_open") == []  # ... which was a no-op
