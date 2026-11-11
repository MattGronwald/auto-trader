# Startup Sequence and Cycle Runner Admission (WP 1.5d-3) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `run core` builds, recovers and subscribes the Cycle Runner with admission closed, a
supervised `admission.ready` loop opens it once the journal is proven reconciled with the broker
(cash check, a later Position Manager step over every held position, a one-sided holdings check),
teardown closes the runner first, and the runner/ledger race is pinned by a real-engine
integration test.

**Architecture:** The runner gets a closed-by-default admission flag (`open_admission()`,
`disable_admission()`). `PositionManager.step()` returns a per-position `StepReport`. A new
`core/admission.py` holds `AdmissionReadiness`, a clock-injected tracker that is fed the Position
Manager reports (it wraps the `positions.manage` step) and decides readiness from the engine's cash
fault, those reports and a guarded holdings comparison (new lock-free
`RiskEngine.broker_holdings()`); `run_core` only wires it: a mode check, a non-fatal backfill
failure, a non-fatal `recover()` failure (drop-only handler), the readiness loop, and
`runner.close` as teardown step 1. No schema change, no new config key.

**Tech Stack:** Python 3.12, uv, SQLAlchemy 2 async, pydantic v2, structlog, pytest
(asyncio_mode=auto, `--strict-markers`, `filterwarnings=error`), ruff, mypy strict.

**Spec:** `docs/superpowers/specs/2026-11-11-startup-admission-design.md` (read it first; section
numbers below refer to it). Decisions: PLAN.md D21 (1)-(9).

**Plan calls the spec leaves to the builder (section 9), decided here:**
- `start()` returns a frozen dataclass `_Started(supervisor, stream_task, runner)`.
- Tracker: `AdmissionReadiness` in `core/admission.py`; it wraps the Position Manager step
  (`readiness.manage(pm.step)`) and records the step's start on its own monotonic clock.
- Runner block reasons, in order: `shutting down`, `admission suspended: commit uncertain`,
  `admission disabled: <reasons joined by ", ">`, `startup reconciliation pending`.
- `open_admission()` order of checks: closing -> no-op; `recover()` not completed cleanly ->
  `RuntimeError`; disabled -> no-op; already open -> no-op; else open and log once.
- Report type: `PositionCheck(position_id, symbol, outcome)` in a `StepReport` tuple.
- `broker_holdings()` reads `get_positions` once and sums `qty` per symbol. It does not go
  through `_broker_positions`, which also calls `asset_rules` for each symbol, a call the
  holdings check does not need.
- Tracker log prefix: `admission.` for the tracker's own lines; `cycle.admission_*` for the
  admission state lines (spec section 6 names kept).
- Readiness runs as a supervised `Loop` (`admission.ready`, `tick_s`, stall after
  `CONTROL_STALL_S`, level `warning`).
- **(Claude's call, beyond the spec):** if the cash fault is seen set again after `T_b` and before
  admission opens, the tracker forgets `T_b` and the holdings result; (c) and (d) must then hold
  again after the next clear observation. Reason: `T_b` is the proof that the journal was caught
  up; a later fault means a check stage raised after it. Admission also re-reads the fault right
  before reporting ready.
- **(Claude's call, deviation from spec test 12):** the "candidate before/after readiness" test
  publishes `CandidateSignal` on the real bus into the runner wired by `run_core`'s helper, not
  from a `BarClosed` through the scanner. A real signal needs a full indicator warm-up for one of
  the profile's rules. The scanner -> `CandidateSignal` leg is already covered by
  `tests/integration/test_scanner_feed.py`.

## Global Constraints

- Branch: `wp-1.5d3-impl` off `origin/main` (`git fetch origin && git switch -c wp-1.5d3-impl origin/main`).
- Every commit message ends with the line `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`
  (exactly this, whatever model you are).
- ASCII only in code, comments and docstrings (ruff RUF001-003). No `assert` in `src/` (S101).
- Every task's commit is green: `uv run ruff format && uv run ruff check --fix && uv run ruff check && uv run mypy && uv run pytest`
  (format BEFORE check; format does not wrap docstrings, keep lines <= 100 by hand). Run checks
  into a file and read the real exit status; never trust a piped exit code.
- `risk/` keeps 100 % branch coverage: `uv run pytest --cov=autotrader.risk --cov-branch --cov-report=term-missing tests/unit/risk`.
- Never assert wall-clock timing. Tracker and runner tests use injected clocks; runtime tests poll
  with the existing `_wait_for(predicate)` helper (a bound, not a timing assertion). Log assertions
  use `structlog.testing.capture_logs`. Tests never hit Alpaca or Anthropic.
- **SPEC 0.1:** only `risk/engine.py` holds a `Broker`. The tracker reads holdings through
  `RiskEngine.broker_holdings()`; `run_core` reads `broker.mode()` in `start()` before handing the
  broker to the engine (as `_check_broker` already does).
- Exact values: block reasons `startup reconciliation pending`, `admission disabled: <reason>`;
  disable reasons `broker cannot place orders`, `backfill failed`, `recover failed`; waiting log
  after `3 * snapshot_s` on the tracker's clock; runner close budget
  `min(CLOSE_BUDGET_S, control.remaining())`; abandoned cycles reported as `cycle:<id>`.
- Log names: `cycle.admission_open` (info, runner), `cycle.admission_disabled` (warning for
  `broker cannot place orders`, error otherwise; runtime), `cycle.admission_waiting` (warning,
  once), `cycle.dropped` (info, existing), `admission.holdings_short` (error),
  `admission.holdings_read_failed` (warning, once per failure streak),
  `admission.unquoted_holdings` (warning, startup), `feed.backfill_failed` (error, startup).
- Docs are part of each task: fix every docstring and comment in a touched module that states the
  old behaviour (`rg` for it). SPEC/CLAUDE.md/runbook/PLAN land in Task 8. Dated records (PLAN
  history entries, older specs) are never rewritten; they get dated notes.

## Review Focus

1. **Cash fault clears, then is set again before admission opens** (a guard read raises at the
   next snapshot): readiness must not open on the old `T_b`; a Position Manager report from before
   the new clear does not count. Test in Task 3 (`test_fault_set_again_resets_t_b`).
2. **Dust-only symbol whose coins were sold by hand** (journal `symbol_dust` > 0, no open position,
   broker holds 0): the one-sided rule makes this a shortfall. The expected behaviour is that
   admission stays closed and the shortfall is logged; the runbook then says to fix the
   journal by hand. Test in Task 3 (`test_dust_only_symbol_without_broker_holding_is_short`).
3. **Stop request while `recover()` runs:** the unfinished startup is teardown step 0. It returns
   `None` at the stop check, so no runner is returned, `runner.close` is skipped and the exit is
   clean. Test in Task 6 (`test_stop_during_recover_skips_runner_close`).
4. **A readiness tick racing teardown:** `runner.close()` has begun, and the next `admission.ready`
   step finds the tracker ready. `open_admission()` must stay a no-op, so no candidate is ever
   admitted during the flatten. Tests in Task 1
   (`test_open_admission_after_close_is_a_noop`) and Task 6.
5. **A held symbol outside the universe:** it is never quoted, so its report stays `no_bid`. The
   waiting log must name it with `no_bid` so the operator can act. Test in Task 3
   (`test_waiting_log_names_positions_and_short_symbols`).

---

### Task 1: Cycle Runner admission flag

**Files:**
- Modify: `src/autotrader/core/cycle.py` (`__init__`, `_blocked`, `recover`, new methods, module
  and `recover()` docstrings)
- Modify: `tests/unit/core/test_cycle.py` (`make_runner` becomes async and opens admission by
  default; new tests)
- Modify: `tests/integration/test_cycle_runner.py` (`wire()` recovers and opens)

**Interfaces:**
- Produces:
  - `CycleRunner.disable_admission(reason: str) -> None`: appends `reason` to an ordered,
    deduplicated tuple, logs nothing.
  - `CycleRunner.open_admission() -> None`: checks in this order. Closing: no-op. Not
    `self._recovered`: raise `RuntimeError("open_admission() needs a clean recover()")`. Disabled:
    no-op. Already open: no-op. Otherwise set open and log `cycle.admission_open` (info) once.
  - `CycleRunner.admission_open: bool` (property).
  - `CycleRunner.admission_disabled: tuple[str, ...]` (property).
  - `_blocked()` order: `"shutting down"`, `"admission suspended: commit uncertain"`,
    `"admission disabled: " + ", ".join(disabled)`, `"startup reconciliation pending"` (while not
    open), else `None`.
  - `recover()` sets `self._recovered = True` only after its last write. A raising `recover()`
    leaves it `False`.
  - Test helper: `async def make_runner(..., admit: bool = True) -> CycleRunner`. With `admit`,
    it awaits `recover()` and `open_admission()`.

- [ ] **Step 1: Write the failing tests** in `tests/unit/core/test_cycle.py`:
  - `test_new_runner_drops_with_reconciliation_pending`: `make_runner(..., admit=False)`, then
    `await runner.on_candidate(candidate())`. Assert one `cycle.dropped` with
    `reason == "startup reconciliation pending"` and no row in `cycles`.
  - `test_disabled_runner_drops_with_all_reasons`: `admit=False`, `await runner.recover()`,
    `disable_admission("backfill failed")`, `disable_admission("recover failed")`,
    `disable_admission("backfill failed")`, `open_admission()`. A candidate is dropped with
    `reason == "admission disabled: backfill failed, recover failed"`.
  - `test_open_admission_requires_a_clean_recover`: `open_admission()` before `recover()` raises
    `RuntimeError`. A runner whose `recover()` raised (patch `repo.open_cycles` to raise
    `OperationalError`) also raises.
  - `test_open_admission_is_idempotent_and_logs_once`: after `recover()`, call it twice. Exactly
    one `cycle.admission_open`, and `admission_open is True`.
  - `test_open_admission_after_close_is_a_noop`: `recover()`, `await runner.close(budget_s=1)`,
    `open_admission()`. No `cycle.admission_open` is logged, and a candidate drops with
    `"shutting down"`.
  - `test_recover_after_a_closed_runner_dropped_a_candidate_raises`: `admit=False`, drop one
    candidate, then `recover()` raises `RuntimeError(match="before admission")`.
- [ ] **Step 2: Run** `uv run pytest tests/unit/core/test_cycle.py -k "admission or reconciliation or closed_runner" -v`.
  Expected: FAIL (`AttributeError: open_admission`; `make_runner` not awaitable).
- [ ] **Step 3: Implement the interfaces above** in `cycle.py`. Make `make_runner` async with
  `admit: bool = True` and update every call site (`await make_runner(...)`). Tests that seed
  cycles before building the runner, or that call `recover()` themselves (around current lines
  874-1068), pass `admit=False`. In `tests/integration/test_cycle_runner.py::wire`, await
  `runner.recover()` and call `runner.open_admission()` before returning. Docstrings: the module
  docstring adds "admission starts closed and is opened by `run core`'s readiness loop (WP 1.5d-3)".
  In `recover()`, drop "startup reconciliation is 1.5d"; on failure the caller disables admission
  and does not subscribe the runner.
- [ ] **Step 4: Run** `uv run pytest tests/unit/core/test_cycle.py tests/integration/test_cycle_runner.py -v`.
  Expected: PASS (new and existing).
- [ ] **Step 5: Full check and commit**
  `git add src/autotrader/core/cycle.py tests/unit/core/test_cycle.py tests/integration/test_cycle_runner.py`,
  `git commit -m "feat: cycle runner admission starts closed until readiness opens it"`.

### Task 2: Position Manager step report

**Files:**
- Modify: `src/autotrader/core/position_manager.py` (`step`, `_manage`, `_evaluate`, module and
  `step()` docstrings)
- Test: `tests/unit/core/test_position_manager.py`

**Interfaces:**
- Produces (in `position_manager.py`):
  ```python
  PositionOutcome = Literal["evaluated", "latched", "no_bid", "failed"]

  @dataclass(frozen=True)
  class PositionCheck:
      position_id: int
      symbol: str
      outcome: PositionOutcome

  @dataclass(frozen=True)
  class StepReport:
      positions: tuple[PositionCheck, ...] = ()

      @property
      def complete(self) -> bool:  # every position "evaluated" or "latched"; empty -> True
          ...
  ```
  `async def step(self) -> StepReport`. It still raises when the open-positions read raises (no
  report then).
- Outcome rules, per open position read by the step:
  - the trigger was already latched (`pos.exit_trigger` set): `latched`;
  - otherwise `_evaluate` read the bid. The bid is non-None and the evaluation latched any
    trigger that fired: `evaluated`. The bid is `None` and a trigger fired and was latched (a
    time exit): `latched`. The bid is `None` and nothing was latched: `no_bid`. A trigger fired
    and `_latch` returned `False` (`CommitUncertain`, already latched or closed elsewhere):
    `failed`;
  - `_manage` raised anywhere, including in the exit path after the latch: `failed`. The
    existing `positions.step_failed` log stays.
  `_evaluate` returns `tuple[str | None, PositionOutcome]` and `_manage` returns `PositionOutcome`.

- [ ] **Step 1: Write the failing tests** (use the existing fixtures in
  `tests/unit/core/test_position_manager.py` / `tests/pm_harness.py`):
  - `test_report_evaluated_with_a_bid_and_no_trigger`: a position with a fresh quote inside
    stop/tp gives `report.positions == (PositionCheck(pid, BTC, "evaluated"),)` and
    `report.complete`.
  - `test_report_evaluated_when_a_trigger_fires_and_latches`: a quote below the stop gives
    `evaluated`, and the position is latched in the journal.
  - `test_report_latched_for_an_already_latched_position`: latch first, then step. Outcome
    `latched`.
  - `test_report_no_bid_without_a_trusted_quote`: no quote, time limit not reached. Outcome
    `no_bid` and `not report.complete`.
  - `test_report_latched_for_a_time_exit_without_a_bid`: no quote, clock past `time_limit_at`.
    Outcome `latched`.
  - `test_report_failed_when_manage_raises`: monkeypatch `repo.update_trailing` (or `_exit_path`)
    to raise. Outcome `failed`.
  - `test_report_failed_when_the_latch_is_uncertain`: make `bus.publish_tx` raise
    `CommitUncertain` for the latch unit. Outcome `failed`.
  - `test_report_is_empty_without_positions`: `report == StepReport()` and `report.complete`.
- [ ] **Step 2: Run** `uv run pytest tests/unit/core/test_position_manager.py -k report -v`.
  Expected: FAIL (`ImportError: PositionCheck`).
- [ ] **Step 3: Implement** the types and outcome rules above. Docstring of `step()`: "Returns a
  per-position report for the admission readiness check (WP 1.5d-3); the supervised loop discards
  it." Add one line about the report to the module docstring.
- [ ] **Step 4: Run** `uv run pytest tests/unit/core/test_position_manager.py tests/unit/core/test_position_manager_control.py -v`.
  Expected: PASS.
- [ ] **Step 5: Full check and commit**
  `git add src/autotrader/core/position_manager.py tests/unit/core/test_position_manager.py`,
  `git commit -m "feat: position manager step reports what it evaluated per position"`.

### Task 3: Admission readiness tracker and engine holdings read

**Files:**
- Create: `src/autotrader/core/admission.py`
- Modify: `src/autotrader/risk/engine.py` (new `broker_holdings()`)
- Test: `tests/unit/core/test_admission.py` (new), `tests/unit/risk/test_engine.py`

**Interfaces:**
- Consumes: `StepReport`, `PositionCheck` (Task 2); `CashGuard` (`core/ledger.py`).
- Produces:
  - `RiskEngine.broker_holdings(self) -> dict[str, Decimal]`: lock-free, with its own
    `io_timeout_s` budget (`deadline = self._monotonic() + self._io_timeout`, one
    `self._within(deadline, self._broker.get_positions)`). Returns the total `qty` per symbol,
    dust included, and raises on failure.
  - `async def journal_holdings(sessions: async_sessionmaker[AsyncSession]) -> dict[str, tuple[Decimal, Decimal]]`
    in `core/admission.py`. Per symbol with an open position or a dust balance:
    `(sum of open positions.qty, symbol_dust qty)`, read in one session with
    `repo.open_positions` and `repo.symbol_dust_balances`.
  - In `core/admission.py`:
    ```python
    class ReadinessRisk(Protocol):
        cash_fault: str | None
        async def broker_holdings(self) -> dict[str, Decimal]: ...

    class GuardSource(Protocol):
        async def cash_guard(self) -> CashGuard: ...

    JournalHoldings = Callable[[], Awaitable[Mapping[str, tuple[Decimal, Decimal]]]]

    class AdmissionReadiness:
        def __init__(
            self,
            risk: ReadinessRisk,
            ledger: GuardSource,
            journal: JournalHoldings,
            *,
            waiting_after_s: float,
            disabled: Sequence[str] = (),
            clock: Callable[[], float] = time.monotonic,
        ) -> None: ...
        async def manage(self, step: Callable[[], Awaitable[StepReport]]) -> None: ...
        async def ready(self) -> bool: ...
    ```
- `manage`: `started = clock()`, `report = await step()`, then keep `(started, report)` as the
  last report. If `step` raises, re-raise and keep the previous report.
- `ready()` algorithm, in this order:
  1. `disabled` non-empty: return `False` and log nothing.
  2. `now = clock()`. If `risk.cash_fault is not None`, forget `T_b` and any passed holdings
     result. Else, if `T_b` is unset, set `T_b = now`.
  3. (c) holds if a last report exists with `started > T_b` and `report.complete`.
  4. (d): only when `T_b` is set and the holdings check has not passed yet:
     `g1 = await ledger.cash_guard()`, `broker = await risk.broker_holdings()`,
     `journal = await journal()`, `g2 = await ledger.cash_guard()`. If `g1.unsettled`,
     `g2.unsettled` or `g1.generation != g2.generation`, discard the result silently. Otherwise a
     symbol is short when `open + dust > broker.get(symbol, 0)`. No short symbol: passed (kept
     for the process unless step 2 resets it). Per short symbol, log `admission.holdings_short`
     (error) with `symbol`, `journal_qty`, `dust`, `broker_qty` (as `str`) when its triple
     differs from the last logged one. A raising guard read or broker read logs
     `admission.holdings_read_failed` (warning, with the exception) once per failure streak; a
     success ends the streak.
  5. If (b), (c) and (d) hold and `risk.cash_fault is None` (re-read after the awaits), return
     `True`.
  6. Else, once `now - created >= waiting_after_s` and the waiting line has not been logged yet,
     log `cycle.admission_waiting` (warning) with `unmet: list[str]`. Its entries come from
     `"cash check not completed: <fault>"`, `"no position manager step since the cash check"`,
     `"positions not evaluated"` and `"holdings short"`. Also log `positions`, a list of
     `{position_id, symbol, outcome}` for every non-evaluated/latched position in the last report,
     and `short: list[str]`. Return `False`.
- Test doubles in `test_admission.py`: a `Mono` clock (float, `advance()`); `FakeRisk` with a
  settable `cash_fault` and `holdings`, `holdings_error`; `FakeLedger` returning a queue of
  `CashGuard`s; an async `journal` callable returning a settable mapping.

- [ ] **Step 1: Write the failing tests** (`tests/unit/core/test_admission.py`):
  - `test_opens_only_when_all_conditions_hold`: fault set, so `ready()` is `False`. Clear the
    fault, `ready()` (records `T_b`). Then a `manage()` with a complete report, then
    `ready() is True`.
  - `test_report_from_a_step_started_before_t_b_does_not_count`: a `manage()` whose step
    awaits an event. Clear the fault and call `ready()` while it runs, then finish the step:
    `ready()` is `False`. The next `manage()` gives `True`.
  - `test_incomplete_reports_do_not_count` (parametrize `no_bid`, `failed`) and
    `test_empty_report_counts`.
  - `test_fault_set_again_resets_t_b` (Review Focus 1): clear, complete report, set the fault
    again, `False`. Clear again: the old report does not count, a new `manage()` does.
  - `test_holdings_shortfall_keeps_it_closed_and_logs_once_per_figures`: journal
    `{BTC: (1, 0)}`, broker `{BTC: 0.5}`. Two `ready()` calls give one `admission.holdings_short`.
    Broker becomes `0.4`: a second line. Broker becomes `1`: `True`.
  - `test_broker_holding_above_the_journal_passes`.
  - `test_dust_counts_on_the_journal_side`: journal `{BTC: (1, 0.001)}`, broker `{BTC: 1}`, so
    short.
  - `test_dust_only_symbol_without_broker_holding_is_short` (Review Focus 2): journal
    `{ETH: (0, 0.001)}`, broker `{}`, so short.
  - `test_unsettled_or_moved_guard_discards_silently` (parametrize: `g1.unsettled`,
    `g2.unsettled`, generation moved): `False`, no `admission.holdings_short`, and the next
    clean tick passes.
  - `test_broker_read_failure_logs_once_per_streak`: two failing ticks give one warning; a
    success, then a failure, gives a second warning.
  - `test_waiting_logged_once_after_three_snapshot_intervals`: `waiting_after_s=180`.
    `advance(179)` and `ready()`: no log. `advance(1)`: one `cycle.admission_waiting`. Further
    ticks: no more.
  - `test_waiting_not_logged_when_ready_earlier`.
  - `test_waiting_log_names_positions_and_short_symbols` (Review Focus 5): a report with
    `PositionCheck(7, "DOGE/USD", "no_bid")` and a BTC shortfall. The log has
    `positions == [{"position_id": 7, "symbol": "DOGE/USD", "outcome": "no_bid"}]` and
    `short == ["BTC/USD"]`.
  - `test_disabled_tracker_never_opens_and_never_logs`.
  - `test_journal_holdings_sums_open_positions_and_dust` (real migrated SQLite journal, same
    fixture style as `tests/unit/core/test_ledger.py`): two open BTC positions, one closed, ETH
    dust only. Result `{BTC: (sum, 0), ETH: (0, dust)}`.
  In `tests/unit/risk/test_engine.py`:
  - `test_broker_holdings_sums_total_qty_per_symbol`: positions include a dust-sized one, and the
    result includes it.
  - `test_broker_holdings_raises_on_a_broker_error_and_timeout`: a broker raising `BrokerError`
    makes it raise; a hanging `get_positions` with an advanced `Mono` raises `TimeoutError`.
- [ ] **Step 2: Run** `uv run pytest tests/unit/core/test_admission.py tests/unit/risk/test_engine.py -k "holdings or admission or waiting or t_b or report or guard" -v`.
  Expected: FAIL (`ModuleNotFoundError: autotrader.core.admission`).
- [ ] **Step 3: Implement** `broker_holdings()` (docstring: "the holdings check reads the total
  qty; SPEC 0.1 keeps the broker here") and `core/admission.py` with the module docstring
  (spec 3.2-3.3 in two paragraphs: the conditions, their order, `T_b`, the reset rule).
- [ ] **Step 4: Run** the same tests (PASS), then the risk coverage command (100 %).
- [ ] **Step 5: Full check and commit**
  `git add src/autotrader/core/admission.py src/autotrader/risk/engine.py tests/unit/core/test_admission.py tests/unit/risk/test_engine.py`,
  `git commit -m "feat: admission readiness tracker with a one-sided holdings check"`.

### Task 4: Startup wiring in `run core`

**Files:**
- Modify: `src/autotrader/core/runtime.py` (`start()`, new helpers, module docstring)
- Test: `tests/unit/core/test_runtime.py`

**Interfaces:**
- Consumes: Tasks 1-3.
- Produces (in `runtime.py`):
  - `@dataclass(frozen=True) class _Started: supervisor: Supervisor; stream_task: asyncio.Task[None]; runner: CycleRunner`.
    `start()` returns `_Started | None`.
  - `async def _admission_step(readiness: AdmissionReadiness, runner: CycleRunner) -> None`.
    Return at once if `runner.admission_open`; else, if `await readiness.ready()`, call
    `runner.open_admission()`.
  - `def _dropping(reason: str) -> Callable[[CandidateSignal], Awaitable[None]]`: a handler that
    logs `cycle.dropped` (info) with `symbol`, `rule_id`, `reason=f"admission disabled: {reason}"`
    and touches nothing else.
  - `async def _unquoted_holdings(sessions, universe: Collection[str]) -> None`: logs
    `admission.unquoted_holdings` (warning) once with `symbols` (sorted) when an open position's
    symbol is not in the universe.
- `start()` order (spec section 2). New steps are marked NEW:
  1. Register the strategy. `broker = await _check_broker(...)`. NEW: if
     `broker.mode() != cfg.mode`, raise `RuntimeError(f"broker mode {broker.mode()!r} does not match config mode {cfg.mode!r}")`.
  2. Build the feed. NEW: `try: await feed.backfill()`. On `except Exception`, log
     `feed.backfill_failed` with `exc_info` and set `backfilled = False`.
  3. Gate and scanner. Subscribe the scanner and log `scanner.subscribed` only if `backfilled`.
  4. Ledger and engine (unchanged). Read `can_trade = risk.can_place_orders()` once. NEW: call
     `await _unquoted_holdings(sessions, profile.universe)`.
  5. NEW: `runner = CycleRunner(profile=profile, strategy_id=strategy.id, market=feed, gate=gate, risk=risk, bus=bus, sessions=sessions, trading_day_tz=cfg.trading_day_tz)`.
  6. NEW: `disabled: list[str]`, with `broker cannot place orders` if not `can_trade` and
     `backfill failed` if not `backfilled`. Then `try: await runner.recover()`. On
     `except Exception`, append `recover failed`, log `cycle.admission_disabled` (error,
     `reason="recover failed"`, `exc_info`) and set `recovered = False`. For each of the first
     two reasons that applies, log `cycle.admission_disabled` once (`broker cannot place orders`
     at warning, `backfill failed` at error). Call `runner.disable_admission(r)` for every
     reason.
  7. NEW: `bus.subscribe(CandidateSignal, runner.on_candidate if recovered else _dropping("recover failed"))`.
  8. The existing `control.requested` check returns `None`.
  9. Build the poller, PM and `readiness = AdmissionReadiness(risk, ledger, partial(journal_holdings, sessions), waiting_after_s=3 * snapshot_s, disabled=disabled)`.
     The loops change: `Loop("positions.manage", partial(readiness.manage, pm.step), manage_s, MANAGE_STALL_S, "error")`,
     plus `Loop("admission.ready", partial(_admission_step, readiness, runner), tick_s, CONTROL_STALL_S)`.
     Start the stream and the supervisor and log `core.started`, then return
     `_Started(supervisor, stream_task, runner)`.
- `run_core` unpacks `_Started` and keeps `runner` for teardown (Task 5).
- Verify the feed claim (spec section 2, builder call): after a raising `backfill()`, `tick()`
  and `on_bar`/`on_quote` work on empty history. `backfill` mutates only `_finalized_until`
  before `history()` raises. Pin this with test 11a below; any feed change stays in `feed.py`.
- Test helpers to add in `test_runtime.py`:
  - `QuotingSource(StreamingSource)`: also delivers a `Quote` per symbol (bid/ask settable) at
    stream start. Its option `fail_history=True` makes `history()` raise `ConnectionError`.
  - `sim_connect(sim)`: an async connect returning a given `SimBroker`.
  - `seed_position(cfg, sim, *, stop, opened_at)`: open a booked position on the cfg journal
    with a filled SimBroker holding, using `tests.pm_harness.Harness` on a sessionmaker for
    `cfg.database.url` (`Harness.open_position`, then set `opened_at` / `time_limit_at` on the
    row for an overdue time exit). Close its engine before `run_core` starts.

- [ ] **Step 1: Write the failing tests** (spec tests 9-12, 14):
  - `test_mode_mismatch_fails_startup` (9): a `FakeBroker` subclass with `mode() == "live"`.
    `run_core` raises `RuntimeError` whose message contains `'live'` and `'paper'` and no
    setting value. `core.failed` is logged, and no `core.started`, no supervisor loop and no
    stream run (`source.stream` never called: a flag on the source).
  - `test_broker_that_cannot_trade_keeps_admission_disabled` (10): default `FakeBroker`. The log
    has one `cycle.admission_disabled` with `reason == "broker cannot place orders"` at warning,
    and no `cycle.admission_open` after several `admission.ready` ticks (wait for
    `positions.manage` and `ledger.sync` beats via a `_wait_for` on their log lines or on the
    supervisor). A `CandidateSignal` published on the core's bus is dropped with
    `admission disabled: broker cannot place orders`. To reach the bus, monkeypatch
    `runtime.EventBus` with a subclass that records the instance.
  - `test_recover_failure_keeps_exits_running` (11): `SimBroker` via `sim_connect`, a seeded
    position with stop 95 and `QuotingSource` bid 90. Monkeypatch `CycleRunner.recover` to raise
    `OperationalError`. Expect `cycle.admission_disabled reason="recover failed"`, the core
    keeps running, and an exit order row for the position appears (`_wait_for` on `orders` with
    `side == "sell"`). A published `CandidateSignal` gives
    `cycle.dropped reason="admission disabled: recover failed"`. The `cycles` and entry `orders`
    counts are unchanged.
  - `test_backfill_failure_disables_admission_and_keeps_exits` (11a): `QuotingSource(fail_history=True)`,
    `SimBroker`, a seeded position with an overdue time exit. Startup completes (`core.started`),
    `feed.backfill_failed` and `cycle.admission_disabled reason="backfill failed"` are logged, and
    no `scanner.subscribed`. A sell order row appears, and `feed.tick` keeps beating (the live bar
    persists to `bars`). No cycle is inserted.
  - `test_candidate_before_readiness_is_dropped_and_after_is_admitted` (12, see the deviation
    note): `SimBroker` with no positions, `QuotingSource`, `snapshot_s=0.05`, and the profile's
    universe symbols scripted. Publish a `CandidateSignal` (fresh `bar_ts`) right after
    `core.started`: `cycle.dropped reason="startup reconciliation pending"`. `_wait_for`
    `cycle.admission_open`, then publish another: `cycle.admitted`, and a `cycles` row exists.
    The cycle may then expire or be rejected; only its admission is asserted.
  - `test_held_symbol_outside_the_universe_is_logged` (14): seed an open position on
    `DOGE/USD`, which is not in the universe (journal rows only, `FakeBroker`). One
    `admission.unquoted_holdings` with `symbols == ["DOGE/USD"]`.
- [ ] **Step 2: Run** `uv run pytest tests/unit/core/test_runtime.py -k "mode_mismatch or cannot_trade or recover_failure or backfill_failure or before_readiness or outside_the_universe" -v`.
  Expected: FAIL.
- [ ] **Step 3: Implement** the startup order above. Fix the module docstring: drop "`check()`
  still waits for the Cycle Runner (WP 1.5d-3)" and add a WP 1.5d-3 paragraph covering the
  runner, readiness, the backfill/recover failure modes and the mode check. Fix the existing
  tests that break on the new logs or the `_Started` return. Do not weaken their assertions;
  extend expected log sets where a new line is now correct.
- [ ] **Step 4: Run** `uv run pytest tests/unit/core/test_runtime.py -v`. Expected: PASS.
- [ ] **Step 5: Full check and commit**
  `git add src/autotrader/core/runtime.py tests/unit/core/test_runtime.py`,
  `git commit -m "feat: run core wires the cycle runner behind admission readiness"`.

### Task 5: Teardown closes the runner first

**Files:**
- Modify: `src/autotrader/core/runtime.py` (`run_core` teardown call, `_teardown`, the
  `core.shutdown_abandoned` note, the module docstring's "startup recovery (WP 1.5d)")
- Test: `tests/unit/core/test_runtime.py`

**Interfaces:**
- `_teardown(..., runner: CycleRunner | None = None, ...)`. Steps: 0 unfinished startup. 1 NEW:
  if `runner is not None`, run
  `await bounded("runner.close", _close_runner(runner, control, forced))`. 2 flatten. 3 onwards
  unchanged.
- `async def _close_runner(runner: CycleRunner, control: ShutdownControl, forced: list[str]) -> None`:
  `report = await runner.close(budget_s=min(CLOSE_BUDGET_S, control.remaining()))`, then
  `forced.extend(f"cycle:{cid}" for cid in report.abandoned)`. `bounded` is a closure inside
  `_teardown`, so define `_close_runner` there or pass `forced` in.
- `core.shutdown_abandoned` note becomes `"left for the startup sequence; not proof that broker work stopped"`.
  The step comment becomes "1. Admission: close the runner before any flatten, so no entry starts
  while it runs (spec 2026-11-11 section 4)."

- [ ] **Step 1: Write the failing tests** (spec test 13 and Review Focus 3-4):
  - `test_teardown_closes_the_runner_before_flatten_and_source_stop`: `_with_shutdown_flatten(cfg)`
    and a recording `source.stop`. Monkeypatch `CycleRunner.close` to record the call order.
    The order is `runner.close` < the flatten request row write < `source.stop`. Use timestamps
    from a shared list, not wall time.
  - `test_abandoned_cycle_makes_the_exit_unclean`: monkeypatch `CycleRunner.close` to return
    `CloseReport((42,))`. Result `EXIT_FAILED`, `core.shutdown_forced.steps` contains
    `"cycle:42"` and so does `core.shutdown_abandoned`.
  - `test_runner_close_is_skipped_when_startup_failed`: broker connect fails. No
    `CycleRunner.close` call.
  - `test_stop_during_recover_skips_runner_close` (Review Focus 3): `CycleRunner.recover` awaits
    an event. Request the stop while it waits, then release. Expect `EXIT_CLEAN`, no
    `CycleRunner.close` call, and no `core.started`.
  - `test_readiness_cannot_open_admission_after_close_began` (Review Focus 4): SimBroker that can
    trade and a tracker forced ready late (monkeypatch `AdmissionReadiness.ready` to wait for an
    event, then return `True`). Request the stop; once `runner.close` began, release the event.
    No `cycle.admission_open` is logged.
- [ ] **Step 2: Run** `uv run pytest tests/unit/core/test_runtime.py -k "teardown or abandoned or runner_close or close_began" -v`.
  Expected: FAIL.
- [ ] **Step 3: Implement** the teardown change and the docstring/comment fixes (`rg -n "1.5d" src/autotrader/core/runtime.py`
  must return only the WP 1.5d-1/1.5d-2/1.5d-3 history paragraphs).
- [ ] **Step 4: Run** `uv run pytest tests/unit/core/test_runtime.py -v`. Expected: PASS.
- [ ] **Step 5: Full check and commit**
  `git add src/autotrader/core/runtime.py tests/unit/core/test_runtime.py`,
  `git commit -m "feat: teardown closes the cycle runner before the shutdown flatten"`.

### Task 6: Restart scenarios on the real components

**Files:**
- Create: `tests/integration/test_startup_admission.py`
- Modify (only if a helper is missing): `tests/pm_harness.py`

**Interfaces:**
- Consumes: `Harness` (`tests/pm_harness.py`: `sim`, `engine`, `ledger`, `view`, `clock`,
  `new_pm()`, `quote()`, `bar()`, `sync()`, `open_position()`, `submit_entry()`), Tasks 1-3, and
  `runtime._snapshot_step`, `runtime._sync_step`, `runtime._admission_step`.
- A `restart(h)` helper builds what `start()` builds on the same journal: a new `CycleRunner`
  (`risk=h.engine`, `market=h.view`), `await runner.recover()`, a `PositionManager` via
  `h.new_pm()`, and an `AdmissionReadiness` (`journal=partial(journal_holdings, h.sessions)`,
  `waiting_after_s=180`, `clock=Mono()`). A `tick(order)` helper runs the named steps in the
  given order: `"sync"` -> `_sync_step(h.engine, h.ledger)`, `"snapshot"` ->
  `_snapshot_step(h.engine, h.ledger)`, `"manage"` -> `readiness.manage(pm.step)`, `"admit"` ->
  `_admission_step(readiness, runner)`. Interleavings are explicit; no loop runs. Seed the
  baseline with `tests.health.seed_baseline` where the cash check needs one.

- [ ] **Step 1: Write the tests** (spec tests 15-18):
  - `test_restart_reconciles_before_admission_opens` (15): before the restart, journal (a) a
    `RISK_CHECK` cycle without an order row (`repo.insert_cycle`), (b) `h.submit_entry(ETH, limit=None)`
    and `h.bar(ETH, 100)` (filled at the broker, not booked), and (c) `h.open_position(BTC, stop="95")`.
    Restart, quote both symbols, with BTC bid 90. Run ticks `sync, snapshot, manage, admit` until
    `runner.admission_open`, at most 5 rounds. Assert: (a) is `ERROR`, (b)'s cycle is `FILLED` with
    an open ETH position, BTC has a sell order row, and `cycle.admission_open` is the last of
    those log lines.
  - `test_position_booked_on_a_later_pass_waits_for_its_exit` (16): an entry submitted through
    `h.engine.check()` with `h.sim.fail_submit_after_accept()` armed (the pattern of
    `tests/unit/risk/test_resolver.py::test_lost_response_is_linked_and_booked`), filled by a
    bar, with stop above the next bid. Restart without a quote for the symbol. Order:
    `sync` (links), `manage` (no position yet), `sync` (books), `snapshot` (clears `T_b`),
    `manage` (`no_bid`), `admit`: still closed. Then quote below the stop, `manage`, and
    `admit`: open. A sell order row exists before `cycle.admission_open`.
  - `test_swallowed_evaluation_failure_keeps_admission_closed` (17): two positions. Monkeypatch
    `repo.update_trailing`, or the position's `_evaluate`, to raise for one position id for the
    first two `manage` calls. `admit` stays closed through those, and opens after the next clean
    `manage`.
  - `test_holdings_shortfall_keeps_admission_closed` (18): `h.open_position(BTC, qty="1")`, then
    sell 0.5 BTC at the broker outside the journal (`h.sim.submit_order(BrokerOrder(BTC, "sell", 0.5, "market"))`,
    `h.bar(BTC, 100)`). Restart with a fresh BTC quote inside stop/tp. Several `sync, snapshot,
    manage, admit` rounds keep it closed, with one `admission.holdings_short`
    (`journal_qty="1"`, `broker_qty="0.5"`), and no manual pause is written
    (`control_state` `PAUSED_KEY` absent).
- [ ] **Step 2: Run** `uv run pytest tests/integration/test_startup_admission.py -v`. Expected:
  PASS. These tests pin behaviour that Tasks 1-5 implement. A failure here is a defect in those
  tasks: fix it there and say so in the commit message.
- [ ] **Step 3: Full check and commit**
  `git add tests/integration/test_startup_admission.py` (plus `tests/pm_harness.py` if touched),
  `git commit -m "test: restart scenarios reconcile before admission opens"`.

### Task 7: Runner/ledger race on the real engine (1.5b follow-up c)

**Files:**
- Create: `tests/integration/test_runner_ledger_race.py`

**Interfaces:**
- Consumes: `wire()`, `sim_broker()`, `candidate()`, `five()` from
  `tests/integration/test_cycle_runner.py` (import them, or move them into a shared
  `tests/integration/conftest.py` if import cycles appear), plus `Ledger`, `RiskEngine`, and
  `CycleRunner` (opened by `wire`, Task 1).
- Race doubles:
  - `SyncFirst`: wraps the engine. `check()` awaits the real `check()`, then `sim.process_bar(...)`
    (the entry fills), then `await ledger.apply_all(await engine.sync_orders())`, then returns the
    result. The ledger completes the cycle before the runner writes `SUBMITTED`.
  - `FilledAckSim(SimBroker)`: `submit_order` calls `super()`, processes a bar that fills the
    order, and returns the ack with `status="filled"`.
  - `ZeroFillSim(SimBroker)`: scripts `FillPlan(("cancel",))` for the entry. The ack is `new` or,
    in the runner-first variant, the order ends canceled at the next bar.

- [ ] **Step 1: Write the tests** (spec tests 19-23). Each asserts the `CycleTransitioned`
  events for the cycle (`transitions()` helper pattern from `test_cycle.py`), the `positions`
  count and the `fills` count:
  - `test_sync_completes_filled_before_the_runner_writes_submitted` (19): `SyncFirst`. Expect
    `cycle.completed_elsewhere`, exactly one transition to `FILLED`, one position, and no
    `cycle.stuck`.
  - `test_runner_submitted_new_then_sync_fills` (20): a plain engine. After
    `await runner.join()`, the cycle is `SUBMITTED`. Then `process_bar` and one sync pass: one
    `FILLED` transition, from `SUBMITTED`.
  - `test_runner_filled_and_sync_book_the_same_order_once` (21): `FilledAckSim`. Run a sync pass
    concurrently with the runner's post-check writes. Force it with a `CommitHook` (`tests/dbhooks.py`)
    that runs the sync pass right before the runner's `SUBMITTED` commit. Exactly one `FILLED`
    transition, and the fills count equals the broker's fills (deduplicated by `broker_fill_id`).
  - `test_zero_fill_ends_expired_once` (22), parametrized `sync_first` / `runner_first`: the cycle
    ends `EXPIRED` with exactly one terminal transition and no position.
  - `test_snapshot_step_then_approved_check_through_the_runner` (23): an engine with no manual
    clearing (no `cash_reconciled()` and no `last_snapshot_at` poke in this test's own wiring).
    `seed_baseline`, run `await runtime._snapshot_step(engine, ledger)`, open admission, then
    deliver a candidate. `check()` approves (the cycle reaches `SUBMITTED`).
- [ ] **Step 2: Run** `uv run pytest tests/integration/test_runner_ledger_race.py -v`. Expected:
  PASS. A failure is a real race defect: stop and report it with the failing interleaving. Do not
  change `cycle.py` or `ledger.py` in this task without the owner's go (spec section 5: no ledger
  change).
- [ ] **Step 3: Full check and commit**
  `git add tests/integration/test_runner_ledger_race.py`,
  `git commit -m "test: runner/ledger race on the real engine and ledger"`.

### Task 8: Docs

**Files:**
- Modify: `SPEC.md` (line 7 implementation boundary; line 534 startup sentence), `CLAUDE.md`
  (`run core` command line), `src/autotrader/core/config.py:372` comment, `docs/runbook.md`,
  `PLAN.md` ("Current repo state", 1.5 row dated note)

- [ ] **Step 1: Find the claims.**
  `rg -n "1\.5d-3|1\.5d\b|WP 1\.5d|no entries|does not subscribe|nothing to close|startup recovery|admitting entries|places no entries" SPEC.md CLAUDE.md PLAN.md docs/runbook.md src/`.
  Fix every hit that the change falsified (spec section 8 list). Leave dated history entries and
  older specs untouched.
- [ ] **Step 2: Runbook "Startup and admission" section** (spec section 8): the readiness
  conditions, every log line from Global Constraints, and what to do per cause. Cover an
  incident or unsettled row (`repair` with the core stopped, then restart), a held symbol
  without a trusted bid (feed back; outside the universe: universe fix or manual close), a
  holdings shortfall (investigate; correct the journal by hand; this includes a dust-only symbol
  sold by hand, Review Focus 2), and a sticky cash fault or failed `recover()` (fix and restart).
  Also cover a failed backfill (restart once the data API answers) and a mode mismatch (check
  `mode` against `ALPACA_PAPER`). Replace "Current limits (WP 1.5d-2)": with `SimBroker` the
  runner places entries, and with Alpaca admission stays disabled until 1.6.
- [ ] **Step 3: PLAN.md:** the "Current repo state" line (1.5d-3 done on the branch, not yet
  merged; adjust on merge) and a dated 1.5 row note naming spec, plan and the two Claude's calls
  from this plan's header.
- [ ] **Step 4: Full check** (ruff also on docs-only edits; line length in comments).
- [ ] **Step 5: Commit**
  `git add SPEC.md CLAUDE.md PLAN.md docs/runbook.md src/autotrader/core/config.py`,
  `git commit -m "docs: startup sequence and admission readiness (WP 1.5d-3)"`.
