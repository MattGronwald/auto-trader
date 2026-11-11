"""`autotrader run core` (SPEC §12 Phase 0 DoD; WP 1.5a runtime safety; WP 1.5c exits).

Connects the broker, backfills and streams bars for the universe, persists 1m bars, runs the
Signal Scanner on closed bars (WP 1.2) and snapshots equity for the daily-loss/drawdown
checks. The broker is handed to the `RiskEngine`, its only holder from then on (SPEC §0.1).

WP 1.5d-1 (spec 2026-11-09, 5): every `risk.snapshot` step also runs the ledger's cash check
(guard, snapshot, `check_cash`) and drives the engine's cash-reconciliation fault, which rejects
every entry (`cash reconciliation failed`) while set: set at startup (`not yet reconciled`) and
by any stage that raises (sticky until restart for `CommitUncertain`), cleared by a completed
check (`baseline`, `ok`, `discrepancy`; the latter committed the manual pause), unchanged by a
`skipped` one. The engine gets `snapshot_interval_s=snapshot_s`: an entry also needs a loop
snapshot and a day-start baseline within `2 x snapshot_s`.

WP 1.5c (spec 2026-10-19, 4.7-4.8): `exit()` is called by the Position Manager in the
supervised `positions.manage` loop (every 1 s), and the `ledger.sync` loop reads every unsettled
order and books its fills every 2 s through the shared `Ledger`; a hung order read never delays
the Position Manager. With `shutdown.flatten`, teardown runs a bounded `flatten` step after the
startup and `runner.close` steps: it requests a flatten (keeping a pending CLI request) and waits
up to half the remaining budget for the still-running loops to reach flat, then teardown
continues either way.

WP 1.5d-3 (spec 2026-11-11): startup fails when the broker's mode differs from the config's
(`exit()` would reject every exit, so a running core would protect nothing). A failed history
backfill does not stop the core: it logs `feed.backfill_failed`, leaves the scanner unsubscribed
and disables admission until a restart. `start()` builds the Cycle Runner with admission closed
and runs `recover()` before the stream starts; a failing `recover()` disables admission and a
drop-only handler logs every candidate in place of the runner. A broker that cannot place orders
also disables admission. The supervised `admission.ready` loop opens admission once
`AdmissionReadiness` (`core/admission.py`) sees a completed cash check, a later Position Manager
step that evaluated every held position and a passed holdings check. The exits never depend on
admission, the backfill or `recover()`. Teardown closes the runner first (`runner.close`); a
cycle it had to abandon makes the exit unclean (`cycle:<id>`).

WP 1.5a (spec 2026-10-14):
- The caller (the CLI) holds the single-writer journal lock (`core/lock.py`) before the
  migration and passes it in; `run_core` opens the journal through `lock.url` and never
  releases the lock (process death does).
- Feed ticks, kill/control polling, a DB-free kill-file check and equity snapshots run as
  independent supervised loops (`core/supervise.py`): a hung await in one never delays the
  others (they still share the event loop and the journal). Stalls are logged;
  `risk.snapshot_overdue` follows the last snapshot the loop *committed* (never the peak
  snapshot a `check()` writes, spec 2026-11-09 section 4).
- Every exit cause (signal, programmatic stop, startup failure, stream death) goes through
  `ShutdownControl` (`core/shutdown.py`), whose deadline and watchdog are set before any
  async work. Startup races the stop request, so a signal during the broker connect or the
  backfill is bounded too. Teardown is ordered and bounded: a step still running at the
  deadline is cancelled once and left behind (`core.shutdown_forced`), and abandoned work
  stays in the journal for the next start's startup sequence. A startup failure or stream
  death is logged as `core.failed` (with the traceback) exactly once, before teardown: here once
  shutdown began, by the CLI for one raised before it (preconditions, journal engine);
  whoever logs it sets `ShutdownControl.failure_logged`. A startup that raises only after
  teardown began fails the `startup` step instead (`core.shutdown_failed`). The stopped
  stream gets the remaining budget to end on its own, so a bar in flight finishes its journal
  writes; a stream error, including one that lands as the stop is requested, fails the
  `stream` step (`core.shutdown_failed`, exit 1).
"""

import asyncio
import contextlib
from collections.abc import Awaitable, Callable, Collection
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Any, Protocol

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.broker.base import Account
from autotrader.core.admission import AdmissionReadiness, journal_holdings
from autotrader.core.bus import CommitUncertain, EventBus
from autotrader.core.config import AppConfig, Settings, StrategyProfile
from autotrader.core.cycle import CLOSE_BUDGET_S, CycleRunner
from autotrader.core.feed import BarClosed, BarSource, MarketFeed
from autotrader.core.flatten import request_flatten, wait_for_result
from autotrader.core.kill import ControlGate, ControlStatus
from autotrader.core.ledger import CashGuard, CashResult, Ledger
from autotrader.core.lock import JournalLock, canonical_journal
from autotrader.core.position_manager import PositionManager
from autotrader.core.scanner import CandidateSignal, Scanner
from autotrader.core.shutdown import EXIT_CLEAN, EXIT_FAILED, ShutdownControl
from autotrader.core.supervise import Freshness, Loop, Supervisor
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker
from autotrader.risk.engine import RiskEngine

log = structlog.get_logger(__name__)

BrokerConnect = Callable[[Settings], Awaitable[Any]]

TICK_S = 1.0  # feed tick and control poll interval
SNAPSHOT_S = 60.0
SUPERVISE_S = 1.0  # stall/freshness checks and stream supervision
FEED_STALL_S = 30.0
CONTROL_STALL_S = 10.0
SYNC_S = 2.0  # order sync: broker reads and ledger bookings
SYNC_STALL_S = 30.0
MANAGE_S = 1.0  # Position Manager step
MANAGE_STALL_S = 30.0
CLOSE_MARGIN_S = 1.0  # runner.close's budget stays this far below what teardown has left


class _Snapshotter(Protocol):
    async def snapshot_equity(self) -> Account: ...

    def set_cash_fault(self, reason: str, *, sticky: bool = False) -> None: ...

    def cash_reconciled(self) -> None: ...


class _CashChecker(Protocol):
    async def cash_guard(self) -> CashGuard: ...

    async def check_cash(self, account: Account, guard: CashGuard) -> CashResult: ...


class ControlPoller:
    """Polls the kill/pause gate and logs kill changes; the latest status feeds the Position
    Manager (`latest`).

    `file_step` checks the kill file on its own (sync, DB-free) in a separate loop, so a hung
    journal read in `step` never hides a kill file: the file latch is the fallback for a broken
    journal. A file seen there only ever raises the kill; clearing it is `step`'s job, and only
    a successful journal read with no kill file present clears it: an unavailable read proves
    no clearance, so a known kill is kept, with the unavailable reason added."""

    def __init__(self, gate: ControlGate, kill_file: Path) -> None:
        self._gate = gate
        self._kill_file = kill_file
        self.latest: ControlStatus | None = None
        self._kill_reasons: tuple[str, ...] = ()  # of the last confirmed kill

    async def step(self) -> None:
        status = await self._gate.status()
        if not status.killed and self._file_present():  # created during the journal read
            status = self._file_status()
        elif not status.killed and not status.available and self._known_kill():
            status = ControlStatus(True, (*self._kill_reasons, *status.reasons), available=False)
        self._report(status)

    async def file_step(self) -> None:
        if self._file_present() and not (self.latest is not None and self.latest.killed):
            self._report(self._file_status())

    def _known_kill(self) -> bool:
        return self.latest is not None and self.latest.killed

    def _file_present(self) -> bool:
        return ControlGate.kill_file_present(self._kill_file)

    def _file_status(self) -> ControlStatus:
        return ControlStatus(True, (f"kill file present: {self._kill_file}",))

    def _report(self, status: ControlStatus) -> None:
        if status.killed and status.available:
            self._kill_reasons = status.reasons
        previous, self.latest = self.latest, status
        if previous is not None and status.killed == previous.killed:
            return
        if status.killed:
            log.warning("core.kill_detected", reasons=list(status.reasons))
        elif previous is not None:
            log.warning("core.kill_cleared")


def _cash_fault(risk: _Snapshotter, stage: str, error: Exception) -> None:
    """Spec 2026-11-09 5.2, any stage: `CommitUncertain` holds until restart (sticky)."""
    risk.set_cash_fault(
        f"{stage}: {type(error).__name__}", sticky=isinstance(error, CommitUncertain)
    )


async def _snapshot_step(risk: _Snapshotter, ledger: _CashChecker) -> None:
    """One equity snapshot plus the ledger's cash check (spec 2026-11-09 5.1-5.2): the guard
    is read before the account read (D17), `snapshot_equity()` commits the snapshot and
    returns that account, `check_cash()` compares it. Any stage that raises sets the engine's
    cash fault (`<stage>: <ExceptionType>`); `baseline`, `ok` and `discrepancy` clear it,
    `skipped` leaves it. A failed guard read still takes the snapshot (tomorrow's baseline)
    but skips the check. Failures are logged, never raised (the engine bounds its own broker
    calls, `io_timeout_s`, and journal writes are never cancelled); `CancelledError` is not
    caught."""
    guard: CashGuard | None
    try:
        guard = await ledger.cash_guard()
    except Exception as e:
        log.exception("ledger.cash_check_failed", stage="guard")
        _cash_fault(risk, "guard", e)
        guard = None
    try:
        account = await risk.snapshot_equity()
    except TimeoutError as e:
        log.error("risk.snapshot_failed", timeout=True)
        _cash_fault(risk, "snapshot", e)
        return
    except Exception as e:  # it may have committed the snapshot before raising
        log.exception("risk.snapshot_failed")
        _cash_fault(risk, "snapshot", e)
        return
    log.info("risk.snapshot_written")
    if guard is None:  # no guard, no valid comparison
        return
    try:
        result = await ledger.check_cash(account, guard)
    except Exception as e:
        log.exception("ledger.cash_check_failed", stage="check")
        _cash_fault(risk, "check", e)
        return
    if result in ("baseline", "ok", "discrepancy"):  # a discrepancy committed its pause
        risk.cash_reconciled()


async def _sync_step(risk: RiskEngine, ledger: Ledger) -> None:
    """One order sync: read the unsettled orders from the broker, then book them. Failures
    are logged, never raised (the engine bounds its broker reads, `io_timeout_s`)."""
    try:
        await ledger.apply_all(await risk.sync_orders())
    except Exception:
        log.exception("ledger.sync_failed")


@dataclass(frozen=True)
class _Started:
    supervisor: Supervisor
    stream_task: "asyncio.Task[None]"
    runner: CycleRunner


async def _admission_step(readiness: AdmissionReadiness, runner: CycleRunner) -> None:
    """The `admission.ready` step: open admission once readiness holds, then a no-op.
    `open_admission()` itself stays a no-op once `runner.close()` began (spec 2026-11-11 3.1)."""
    if runner.admission_open:
        return
    if await readiness.ready():
        runner.open_admission()


def _dropping(reason: str) -> Callable[[CandidateSignal], Awaitable[None]]:
    """A `CandidateSignal` handler for an unsubscribed runner: it only logs the drop, so no
    candidate is persisted without a drop line, and touches nothing else."""

    async def drop(event: CandidateSignal) -> None:
        log.info(
            "cycle.dropped",
            symbol=event.symbol,
            rule_id=event.rule_id,
            reason=f"admission disabled: {reason}",
        )

    return drop


async def _unquoted_holdings(
    sessions: async_sessionmaker[AsyncSession], universe: Collection[str]
) -> None:
    """Startup: warn once about open positions the feed never quotes (readiness waits for
    them to close). Never raises: only a mode mismatch may stop the exits at startup."""
    try:
        async with sessions() as session:
            positions = await repo.open_positions(session)
    except Exception:
        log.warning("admission.unquoted_holdings_failed", exc_info=True)
        return
    symbols = sorted({p.symbol for p in positions} - set(universe))
    if symbols:
        log.warning("admission.unquoted_holdings", symbols=symbols)


async def _close_runner(runner: CycleRunner, control: ShutdownControl, forced: list[str]) -> None:
    """Teardown's `runner.close` step. Its budget stays `CLOSE_MARGIN_S` below what is left,
    so `bounded` cannot cancel `close()` before it returns its report (D21 note 2026-11-11);
    each abandoned cycle makes the exit unclean (`cycle:<id>`)."""
    budget = max(0.0, min(CLOSE_BUDGET_S, control.remaining() - CLOSE_MARGIN_S))
    report = await runner.close(budget_s=budget)
    forced.extend(f"cycle:{cid}" for cid in report.abandoned)


async def _shutdown_flatten(
    sessions: async_sessionmaker[AsyncSession], control: ShutdownControl
) -> None:
    """Teardown's `flatten` step (spec 4.7): request a flatten, keeping a pending request so
    a waiting CLI still sees its own result, and wait up to half the remaining budget for the
    still-running loops to report flat. Never drives a pass itself and never raises for "not
    flat": positions left open are handled at the next start."""
    request, _ = await request_flatten(
        sessions, source="shutdown", reason=control.cause, now=datetime.now(UTC)
    )
    result = await wait_for_result(sessions, request.ts, wait_s=control.remaining() / 2)
    if result is not None and result.get("flat"):
        log.info("core.shutdown_flattened", request_ts=request.ts, dust=result.get("dust"))
    else:
        log.error(
            "core.shutdown_flatten_incomplete",
            request_ts=request.ts,
            remaining=None if result is None else result.get("remaining"),
        )


async def run_core(
    cfg: AppConfig,
    settings: Settings,
    profile: StrategyProfile,
    profile_yaml: str,
    *,
    source: BarSource,
    connect_broker: BrokerConnect,
    lock: JournalLock,
    control: ShutdownControl,
    tick_s: float = TICK_S,
    snapshot_s: float = SNAPSHOT_S,
    supervise_s: float = SUPERVISE_S,
    sync_s: float = SYNC_S,
    manage_s: float = MANAGE_S,
) -> int:
    """Run until shutdown is requested. Returns EXIT_CLEAN, or EXIT_FAILED when a teardown
    step was forced at the deadline or failed. Startup failures and stream death are logged
    (`core.failed`, setting `control.failure_logged`) and re-raised after teardown. The
    precondition `ValueError`s and a failing journal engine are raised before shutdown
    begins, unlogged: the CLI logs those."""
    if not lock.held:
        raise ValueError("run_core needs the held single-writer journal lock")
    if lock.url != canonical_journal(cfg.database.url)[0]:
        raise ValueError("the journal lock belongs to a different journal")
    engine = make_engine(lock.url)
    sessions = make_sessionmaker(engine)
    bus = EventBus(sessions)

    async def start() -> _Started | None:
        """Startup up to the first loop (spec 2026-11-11 section 2); None if shutdown was
        requested meanwhile."""
        async with sessions() as session:
            strategy = await repo.register_strategy(session, profile, profile_yaml)
        broker = await _check_broker(connect_broker, settings)
        if broker.mode() != cfg.mode:  # exit() would reject every exit: nothing to protect
            raise RuntimeError(
                f"broker mode {broker.mode()!r} does not match config mode {cfg.mode!r}"
            )
        timeframes = [
            profile.timeframes.signal,
            *profile.timeframes.context,
            profile.timeframes.trend,
        ]
        feed = MarketFeed(
            source,
            sessions,
            bus,
            symbols=list(profile.universe),
            timeframes=timeframes,
            stale_alert_s=cfg.feed.stale_alert_s,
        )
        backfilled = True
        try:
            await feed.backfill()  # publishes no events, so the scanner can subscribe after it
        except Exception:  # exits need no history: only entries wait, until a restart (D21.9)
            log.error("feed.backfill_failed", exc_info=True)
            backfilled = False
        gate = ControlGate(cfg.control.kill_file, sessions)
        scanner = Scanner(profile, feed, gate, sessions, bus, trading_day_tz=cfg.trading_day_tz)
        if backfilled:  # no candidates on a cold, gappy history
            bus.subscribe(BarClosed, scanner.on_bar_closed)
            log.info(
                "scanner.subscribed",
                timeframe=profile.timeframes.signal,
                rules=[r.id for r in profile.signal_rules],
            )
        ledger = Ledger(bus, sessions)  # shared by the engine and the sync loop
        risk = RiskEngine(
            broker,
            profile=profile,
            strategy_id=strategy.id,
            mode=cfg.mode,
            gate=gate,
            market=feed,
            sessions=sessions,
            ledger=ledger,
            trading_day_tz=cfg.trading_day_tz,
            snapshot_interval_s=snapshot_s,  # freshness bound and baseline window: 2x this
        )
        log.info("risk.engine_ready", mode=cfg.mode)
        can_trade = risk.can_place_orders()
        await _unquoted_holdings(sessions, profile.universe)
        runner = CycleRunner(  # admission closed until readiness opens it
            profile=profile,
            strategy_id=strategy.id,
            market=feed,
            gate=gate,
            risk=risk,
            bus=bus,
            sessions=sessions,
            trading_day_tz=cfg.trading_day_tz,
        )
        disabled: list[str] = []
        if not can_trade:
            disabled.append("broker cannot place orders")
            log.warning("cycle.admission_disabled", reason="broker cannot place orders")
        if not backfilled:
            disabled.append("backfill failed")
            log.error("cycle.admission_disabled", reason="backfill failed")
        recovered = True
        try:
            await runner.recover()  # before the stream starts: no candidate can reach it earlier
        except Exception:  # entries stay disabled, the exits still start (D21.4)
            disabled.append("recover failed")
            log.error("cycle.admission_disabled", reason="recover failed", exc_info=True)
            recovered = False
        for reason in disabled:
            runner.disable_admission(reason)
        bus.subscribe(
            CandidateSignal, runner.on_candidate if recovered else _dropping("recover failed")
        )
        if control.requested:  # no await from here on: nothing starts after a stop request
            return None
        poller = ControlPoller(gate, cfg.control.kill_file)
        pm = PositionManager(risk, feed, sessions, bus, poller, profile=profile, config=cfg)
        readiness = AdmissionReadiness(
            risk,
            ledger,
            partial(journal_holdings, sessions),
            waiting_after_s=3 * snapshot_s,
            disabled=disabled,
        )
        supervisor = Supervisor(
            [
                Loop("feed.tick", feed.tick, tick_s, FEED_STALL_S),
                Loop("control.poll", poller.step, tick_s, CONTROL_STALL_S, "error"),
                Loop("control.kill_file", poller.file_step, tick_s, CONTROL_STALL_S, "error"),
                Loop(
                    "risk.snapshot",
                    partial(_snapshot_step, risk, ledger),
                    snapshot_s,
                    2 * snapshot_s,
                ),
                Loop("ledger.sync", partial(_sync_step, risk, ledger), sync_s, SYNC_STALL_S),
                Loop(
                    "positions.manage",
                    partial(readiness.manage, pm.step),
                    manage_s,
                    MANAGE_STALL_S,
                    "error",
                ),
                Loop(
                    "admission.ready",
                    partial(_admission_step, readiness, runner),
                    tick_s,
                    SYNC_STALL_S,  # a holdings read is a broker read like ledger.sync's
                ),
            ],
            [Freshness("risk.snapshot", lambda: risk.last_snapshot_at, 2 * snapshot_s)],
        )
        stream_task = asyncio.create_task(source.stream(feed.on_bar, feed.on_quote), name="feed")
        supervisor.start()
        log.info(
            "core.started",
            mode=cfg.mode,
            profile=profile.name,
            profile_hash=profile.hash,
            strategy_id=strategy.id,
            symbols=list(profile.universe),
            timeframes=timeframes,
        )
        return _Started(supervisor, stream_task, runner)

    startup: asyncio.Task[_Started | None] | None = None
    supervisor: Supervisor | None = None
    stream_task: asyncio.Task[None] | None = None
    runner: CycleRunner | None = None
    failure: BaseException | None = None
    stream_died = False  # the supervise loop read the stream's end (and its error) itself
    wakeup = control.attach()  # right before the try: nothing that can raise sits between
    try:
        if not control.requested:
            # Startup races the stop request: a signal during the broker connect or the
            # backfill must not wait for them. An unfinished startup becomes the first
            # teardown step, bounded by the deadline like every other step.
            startup = asyncio.create_task(start(), name="startup")
            waiter = asyncio.create_task(wakeup.wait(), name="startup-stop")
            try:
                await asyncio.wait({startup, waiter}, return_when=asyncio.FIRST_COMPLETED)
            finally:
                waiter.cancel()
            started = startup.result() if startup.done() else None  # raises a startup failure
            if started is not None:
                supervisor, stream_task, runner = (
                    started.supervisor,
                    started.stream_task,
                    started.runner,
                )
            while supervisor is not None and stream_task is not None and not control.requested:
                if stream_task.done():
                    control.begin("stream_death")
                    stream_died = True
                    error = None if stream_task.cancelled() else stream_task.exception()
                    failure = error or RuntimeError("market data stream ended unexpectedly")
                    break
                supervisor.check()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(wakeup.wait(), timeout=supervise_s)
    except Exception as e:
        control.begin("startup_failure")  # a no-op if another cause came first
        failure = e
    if failure is not None:  # logged before teardown, which the watchdog may cut short
        log.error("core.failed", cause=control.cause, exc_info=failure)
        control.failure_logged = True  # the CLI must not log it again
    flatten_step: Callable[[], Awaitable[None]] | None = None
    if cfg.shutdown.flatten:
        if supervisor is not None:  # the Position Manager runs: its loop works the request
            flatten_step = partial(_shutdown_flatten, sessions, control)
        else:
            log.warning("core.shutdown_flatten_skipped", reason="startup never completed")
    forced = await _teardown(
        control,
        source,
        startup,
        supervisor,
        stream_task,
        bus,
        engine,
        stream_died=stream_died,
        flatten_step=flatten_step,
        runner=runner,
    )
    if failure is not None:
        raise failure
    return EXIT_FAILED if forced else EXIT_CLEAN


async def _teardown(
    control: ShutdownControl,
    source: BarSource,
    startup: "asyncio.Task[Any] | None",
    supervisor: Supervisor | None,
    stream_task: "asyncio.Task[None] | None",
    bus: EventBus,
    engine: Any,
    *,
    stream_died: bool = False,
    flatten_step: Callable[[], Awaitable[None]] | None = None,
    runner: CycleRunner | None = None,
) -> list[str]:
    """Ordered, bounded teardown (spec section 4). Each step gets what is left of the
    deadline; a step still running then is cancelled once and left behind (forced). A step
    that raises or is cancelled is failed. Either way the remaining steps still run, and
    the returned list (forced + failed) makes the exit unclean."""
    deadline = control.deadline
    if deadline is None:
        raise RuntimeError("teardown before ShutdownControl.begin()")
    log.info("core.stopping", cause=control.cause)
    forced: list[str] = []  # timed out: cancelled once and left behind
    failed: list[str] = []  # completed, but raised or was cancelled

    async def bounded(name: str, work: Awaitable[object], floor_s: float = 0.0) -> None:
        task = asyncio.ensure_future(work)
        done, _ = await asyncio.wait({task}, timeout=max(control.remaining(), floor_s))
        if not done:
            task.cancel()
            forced.append(name)
        elif task.cancelled():
            failed.append(name)
            log.error("core.shutdown_step_failed", step=name, error="cancelled")
        elif (error := task.exception()) is not None:
            failed.append(name)
            log.error("core.shutdown_step_failed", step=name, error=repr(error))

    # 0. An unfinished startup (stop requested during it) gets what is left of the budget.
    # It checks the request before starting the stream or any loop, so it built neither.
    if startup is not None and not startup.done():
        await bounded("startup", startup)
    # 1. Admission: close the runner before any flatten, so no entry starts while it runs
    # (spec 2026-11-11 section 4). Only a runner that startup returned.
    if runner is not None:
        await bounded("runner.close", _close_runner(runner, control, forced))
    # 2. shutdown.flatten: the supervised loops still run and work the request (spec 4.7).
    if flatten_step is not None:
        await bounded("flatten", flatten_step())
    await bounded("source.stop", source.stop())
    if supervisor is not None:
        forced += [f"loop:{name}" for name in await supervisor.stop(deadline)]
    if stream_task is not None and not stream_died:
        # After `source.stop()` the stream ends on its own once the bar in hand is handled, so
        # it gets the remaining budget instead of an immediate cancel, which could commit a bar
        # without its BarClosed event (PR #23 CI). It is cancelled only at the deadline
        # (forced). A stream that raised, including one that ended while the stop was being
        # requested, before the supervise loop looked, fails the step; a normal return is
        # clean.
        await bounded("stream", stream_task)
    await bounded("bus.close", bus.close(budget_s=control.remaining()))
    # A floor even after the deadline (inside the watchdog's 5 s grace): a forced teardown
    # must still close the journal's connections (dry run, spec revision 2026-10-14).
    await bounded("engine.dispose", engine.dispose(), floor_s=1.0)
    if forced:
        log.error("core.shutdown_forced", steps=forced, cause=control.cause)
        log.warning(
            "core.shutdown_abandoned",
            steps=forced,
            note="left for the startup sequence; not proof that broker work stopped",
        )
    if failed:
        log.error("core.shutdown_failed", steps=failed, cause=control.cause)
    if not forced and not failed:
        log.info("core.stopped", cause=control.cause)
    return forced + failed  # any entry makes the exit unclean


async def _check_broker(connect_broker: BrokerConnect, settings: Settings) -> Any:
    broker = await connect_broker(settings)
    account = await broker.get_account()
    fees = broker.fee_schedule()
    log.info(
        "core.broker_connected",
        mode=broker.mode(),
        equity=str(account.equity),
        cash=str(account.cash),
        maker_bps=str(fees.maker_bps),
        taker_bps=str(fees.taker_bps),
    )
    return broker
