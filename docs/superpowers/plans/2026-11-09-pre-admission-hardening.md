# Pre-admission Hardening (WP 1.5d-1) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `RiskEngine.check()` fail closed on an untrusted daily-loss baseline, an overdue
equity snapshot, a cash-reconciliation fault and an unsettled sell on the symbol, run the cash
check in `run core`, and land the 1.5a/1.5c follow-ups, before 1.5d-3 wires the Cycle Runner.

**Architecture:** Journal queries in `journal/repo.py`; the pure rejections in `risk/checks.py`
(`faults=` argument, check 3 on a missing baseline, new check 5a); engine state (snapshot
freshness, cash fault, receipt-time snapshot stamps, pre-submit re-check) in `risk/engine.py`;
the snapshot+cash step in `core/runtime.py`. Follow-ups touch `core/shutdown.py`,
`core/supervise.py`, `cli.py`, `core/position_manager.py`, `core/ledger.py`.

**Tech Stack:** Python 3.12, uv, SQLAlchemy 2 async + Alembic (`render_as_batch=True`), pydantic v2,
structlog, pytest (asyncio_mode=auto, `--strict-markers`, `filterwarnings=error`), ruff, mypy strict.

**Spec:** `docs/superpowers/specs/2026-11-09-pre-admission-hardening-design.md` (read it first; section
numbers below refer to it). Decisions: PLAN.md D19.

**Dry run (2026-11-09):** an agent executed tasks 1-11 in an isolated worktree; all green (1065 passed,
`risk/` 100 % branches). Its 12 plan defects (existing-test fallout, barrier mechanics, fixture
locations, a red Task 3 commit, Task 8 ordering, Task 10 margin) are folded into this text.

## Global Constraints

- Branch: `wp-1.5d1-impl` off `origin/main` (`git fetch origin && git switch -c wp-1.5d1-impl origin/main`).
- Every commit message ends with the line `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`
  (exactly this, whatever model you are).
- ASCII only in code, comments and docstrings (no Unicode minus, times, arrows: ruff RUF001-003).
- No `assert` in `src/` (ruff S101); no `eval`/`exec`.
- Every task's commit is green: `uv run ruff format && uv run ruff check --fix && uv run ruff check && uv run mypy && uv run pytest` (format BEFORE check; format does not wrap docstrings, so keep lines <= 100 by hand).
- Never assert wall-clock timing; use injected clocks (`Clock`, `Mono`) and events/barriers.
- Log assertions use `structlog.testing.capture_logs`.
- `risk/` keeps 100 % branch coverage: `uv run pytest --cov=autotrader.risk --cov-branch --cov-report=term-missing tests/unit/risk` shows 100 %.
- Reason strings are exact: `daily-loss baseline unavailable`, `equity snapshot overdue`,
  `cash reconciliation failed`, `unsettled sell on <SYM>`, `trading day changed`.
- None of the new rejections sets a pause (`RiskDecision.pause is None`).
- Freshness bound and baseline window are both `2 * snapshot_interval_s`; default
  `snapshot_interval_s = 60.0` (120 s). `run core` passes its `snapshot_s`.
- Docs are part of each task: when a task changes a behaviour, fix every docstring/comment in the
  touched module that states the old behaviour (`rg` for it). PLAN/runbook land in Task 10.
- Dated records (PLAN history entries, older specs) are never rewritten.

## Review Focus

1. Non-UTC trading-day tz (`Europe/Berlin`, DST): the baseline window is relative to the tz's day
   start in UTC (22:00 or 23:00 UTC). Test in Task 3.
2. A snapshot stamped exactly at `day_start` (inclusive) and exactly `day_start - 120 s`
   (inclusive): both trusted. Test in Task 1.
3. A process restart against a journal with a fresh pre-boundary snapshot: the baseline is
   trusted, but entries still wait for this process's first loop snapshot and completed cash
   check. Test in Task 4.
4. A final-but-unsettled sell (`not_submitted`/`rejected` not yet settled by the ledger) blocks its
   symbol until settled. Covered by the Task 1 query test only: through `check()` the gather's own
   sync settles such rows first, so the gate test in Task 3 uses an id-less `pending` sell.
5. The pre-submit re-check must run after every await of `check()` before the broker call: a fault
   set during the order-row insert (not only during the gate read) still blocks. Test in Task 5.

---

### Task 1: Journal queries and the `events.type` index

**Files:**
- Modify: `src/autotrader/journal/repo.py` (next to `day_start_equity`, ~600; next to `open_entry_orders`, ~436)
- Modify: `src/autotrader/journal/models.py` (`Event.type`, ~248)
- Create: `src/autotrader/journal/migrations/versions/20261109_0006_events_type_index.py`
- Test: `tests/unit/journal/test_repo.py`, `tests/unit/journal/test_db.py` (parity test must pass unchanged)

**Interfaces:**
- Produces: `async def trusted_day_start_equity(session: AsyncSession, day_start: datetime, max_age: timedelta) -> Decimal | None`
- Produces: `async def unsettled_sell_symbols(session: AsyncSession) -> set[str]`
- Produces: index `ix_events_type` on `events.type` (migration revision `0006`, down_revision = the 0005 revision id).

- [ ] **Step 1: Write failing tests** in `tests/unit/journal/test_repo.py` (use the file's existing `session` fixture and `insert_equity_snapshot`; `ts` is unique):

```python
async def test_trusted_day_start_equity_accepts_only_the_pre_boundary_window(session: AsyncSession) -> None:
    start = datetime(2026, 10, 9, tzinfo=UTC)
    window = timedelta(seconds=120)
    assert await repo.trusted_day_start_equity(session, start, window) is None
    for ts, eq in [(start - timedelta(seconds=121), "900"), (start + timedelta(seconds=1), "1100")]:
        await repo.insert_equity_snapshot(session, ts=ts, equity=Decimal(eq), cash=Decimal(eq),
                                          exposure=Decimal(0), hwm=Decimal(eq), daily_pnl=Decimal(0))
    assert await repo.trusted_day_start_equity(session, start, window) is None  # no fallback
    await repo.insert_equity_snapshot(session, ts=start - window, equity=Decimal("950"), ...)
    assert await repo.trusted_day_start_equity(session, start, window) == Decimal("950")  # inclusive
    await repo.insert_equity_snapshot(session, ts=start, equity=Decimal("990"), ...)
    assert await repo.trusted_day_start_equity(session, start, window) == Decimal("990")  # latest, inclusive

async def test_unsettled_sell_symbols_any_status_own_or_external(session: AsyncSession) -> None:
    # rows: BTC sell pending unsettled; ETH sell not_submitted unsettled; SOL sell filled settled;
    # AVAX buy unsettled -> {"BTC/USD", "ETH/USD"}
```

(`...` = the same remaining keyword arguments as the first insert; write them out.) Build the order
rows with `repo.insert_order` as other tests in the file do, and settle the SOL row by setting
`settled_at`.

- [ ] **Step 2:** Run `uv run pytest tests/unit/journal/test_repo.py -k "trusted_day_start or unsettled_sell" -v`. Expected: FAIL (`AttributeError: ... has no attribute 'trusted_day_start_equity'`).
- [ ] **Step 3:** Implement both functions. `trusted_day_start_equity`: `select(EquitySnapshot.equity).where(ts >= day_start - max_age, ts <= day_start).order_by(ts.desc()).limit(1)`. `unsettled_sell_symbols`: `select(Order.symbol).where(Order.side == "sell", Order.settled_at.is_(None))` into a set (skip NULL symbols). Add to `day_start_equity`'s docstring: "Informational only (`equity_snapshots.daily_pnl`); no limit may use it: decisions use `trusted_day_start_equity` (D19)."
- [ ] **Step 4:** Set `index=True` on `Event.type`; write migration 0006 by hand in the style of 0001 (`batch_op.create_index(batch_op.f("ix_events_type"), ["type"], unique=False)`; downgrade drops it). The model/migration parity test in `test_db.py` (`compare_metadata`) is the check; no autogenerate probe.
- [ ] **Step 5:** Run `uv run pytest tests/unit/journal -v`. Expected: PASS (incl. the model/migration parity test in `test_db.py`).
- [ ] **Step 6: Commit** `feat: trusted day-start baseline and unsettled-sell queries, events.type index`.

### Task 2: Pure checks: missing baseline, faults, unsettled sell (check 5a)

**Files:**
- Modify: `src/autotrader/risk/types.py` (`PortfolioState`, ~57)
- Modify: `src/autotrader/risk/checks.py` (`check_entry`, ~56-110)
- Test: `tests/unit/risk/test_checks.py`

**Interfaces:**
- Produces: `PortfolioState.day_start_equity: Dec | None` (no default; stays required), `PortfolioState.unsettled_sells: frozenset[str] = frozenset()`
- Produces: `check_entry(intent, portfolio, market, *, profile, control, mode_matches, faults: tuple[str, ...] = ()) -> RiskDecision`

Order inside `check_entry`: check 1 (control, mode) -> **faults: reject `faults[0]`** -> check 2
(market) -> check 3 (**`day_start_equity is None` -> reject `daily-loss baseline unavailable`**,
else today's loss check) -> check 4 -> **check 5a: `intent.symbol in portfolio.unsettled_sells` ->
reject `f"unsettled sell on {intent.symbol}"`** -> check 5 (unchanged; 5a's symbols are NOT added
to `busy`) -> rest unchanged. All new rejections have `pause=None`.

- [ ] **Step 1: Write failing tests** in `test_checks.py`, reusing its portfolio/market builders:
  - `test_fault_rejects_after_control_without_pause`: `faults=("equity snapshot overdue", "cash reconciliation failed")` -> `reasons == ("equity snapshot overdue",)`, `pause is None`; with a kill in `control` the control reason wins.
  - `test_missing_baseline_rejects_without_pause`: `day_start_equity=None` -> `("daily-loss baseline unavailable",)`, `pause is None`.
  - `test_unsettled_sell_rejects_its_symbol_only`: `unsettled_sells=frozenset({BTC})` -> BTC intent rejected `"unsettled sell on BTC/USD"`; with `max_positions` busy count at limit-1 and `unsettled_sells={"ETH/USD"}`, a BTC entry is still approved (5a symbols don't count toward `max_positions`).
- [ ] **Step 2:** Run `uv run pytest tests/unit/risk/test_checks.py -v`. Expected: FAIL (unexpected keyword `faults` / validation error on `None`).
- [ ] **Step 3:** Implement the type changes and the three rejections as ordered above. Update `check_entry`'s docstring/check list (it says "11 checks") to name the faults step and check 5a.
- [ ] **Step 4:** Run `uv run pytest tests/unit/risk/test_checks.py -v` and the coverage command from Global Constraints for `tests/unit/risk/test_checks.py` + `test_engine.py`. Expected: PASS; checks.py 100 % branches.
- [ ] **Step 5: Commit** `feat: check_entry rejects on faults, a missing baseline and an unsettled sell`.

### Task 3: Engine: trusted baseline, unsettled sells, receipt-time stamps, test baseline seeding

**Files:**
- Modify: `src/autotrader/risk/engine.py` (`__init__` ~120, `_gather_entry` ~215-300, `snapshot_equity` ~561, `_write_snapshot` ~577, module docstring)
- Modify: `tests/unit/risk/test_engine.py` (fixtures ~130-200), `tests/integration/test_cycle_runner.py` (~140)
- Create: `tests/health.py` (shared test helpers)
- Modify: `pyproject.toml` (`[tool.pytest.ini_options].markers`)

**Interfaces:**
- Consumes: Task 1 `repo.trusted_day_start_equity`, `repo.unsettled_sell_symbols`; Task 2 `PortfolioState` fields.
- Produces: `RiskEngine.__init__(..., snapshot_interval_s: float = 60.0)`; attribute stays `last_snapshot_at: float | None`.
- Produces (tests): `tests/health.py`: `async def seed_baseline(sessions: async_sessionmaker[AsyncSession], now: datetime, tz: str = "UTC", equity: Decimal = Decimal("1000")) -> None` inserting one snapshot at `day_start(now, tz) - timedelta(seconds=60)` with `cash=hwm=equity`, `exposure=0`, `daily_pnl=0`.
- Produces (tests): pytest marker `no_baseline` ("test builds its own equity snapshots").

Engine changes:
- `_gather_entry`: baseline from `repo.trusted_day_start_equity(session, day_start(now, tz), timedelta(seconds=2 * self._snapshot_interval_s))`, passed as is (may be `None`); `unsettled_sells=frozenset(await repo.unsettled_sell_symbols(session))` read in the same session block as `open_entry_orders`.
- Receipt-time stamps (spec 3.1): in `_gather_entry`, the peak snapshot is written with `observed = self._clock()` read right after `get_account` returned (not the gather's `now`); in `snapshot_equity`, read `now = self._clock()` after `get_account` returned and use it for both `_write_snapshot` calls (normal and position-read error path). Other uses of the gather's `now` are unchanged.
- `last_snapshot_at`: set only in `snapshot_equity` after each successful `_write_snapshot` (both paths); `_write_snapshot` no longer sets it (spec 4).

- [ ] **Step 1: Test infrastructure.** Create `tests/health.py` with `seed_baseline`. Register marker `"no_baseline: the test builds its own equity snapshots (no seeded day-start baseline)"`. In `test_engine.py` add an autouse fixture `_baseline(request, sessions)` that calls `await seed_baseline(sessions, NOW)` unless the test has the `no_baseline` marker. In `test_cycle_runner.py`, seed in its `sessions` fixture (not in `wire()`: one test calls `wire()` twice and `equity_snapshots.ts` is unique).
- [ ] **Step 2: Write failing tests** in `test_engine.py`:
  - `test_gapped_day_boundary_rejects_baseline_unavailable` (`no_baseline`): snapshots 1000 at day start - 3 h, 1100 at day start + 1 h; engine clock NOW with account equity 1080 (`EquityBroker`) -> rejected `daily-loss baseline unavailable`, no order row.
  - `test_no_pre_start_snapshot_rejects_even_with_later_snapshots` (`no_baseline`): five snapshots after the day start -> rejected.
  - `test_berlin_baseline_window_is_relative_to_the_local_day_start` (`no_baseline`): tz `Europe/Berlin` (NOW is 2026-10-09 12:00 UTC, day start 2026-10-08 22:00 UTC): snapshot at 21:59 UTC -> approved; only one at 21:57 UTC -> rejected.
  - `test_next_day_with_fresh_snapshot_allows_entries` (`no_baseline`): day N rejected; snapshot at day N+1 start - 30 s, clock N+1 noon -> approved.
  - `test_decision_never_uses_the_untrusted_day_start_equity` (`no_baseline`): one snapshot at day start + 1 min equal to current equity; `repo.day_start_equity` returns it, `check()` still rejects `daily-loss baseline unavailable`.
  - `test_unsettled_external_sell_blocks_its_symbol`: insert an id-less `pending` `side="sell"` order row for BTC (a final row would be settled by the gather's own sync first) -> BTC entry rejected `unsettled sell on BTC/USD`; after setting its `settled_at`, approved.
  - `test_account_read_crossing_midnight_is_stamped_at_receipt` (`no_baseline`): a broker whose `get_account` advances the engine `Clock` from `day_start - 1 s` to `day_start + 1 s` before returning; run `snapshot_equity()` -> the row's `ts == day_start + 1 s`; then `check()` at that clock -> rejected `daily-loss baseline unavailable`. Repeat for the peak snapshot written inside `check()` (empty journal, so the first check writes the peak): row stamped `day_start + 1 s`.
  - `test_exits_and_cancels_work_on_a_baseline_unavailable_day` (`no_baseline`): held BTC, `exit()` approved and sent; `cancel_open_entries()` cancels a resting entry.
  - `test_peak_snapshot_in_check_does_not_refresh_last_snapshot_at`: `EquityBroker` with equity 1100 (above the seeded HWM 1000, so `check()` really writes a peak row; assert the row exists) -> `engine.last_snapshot_at is None`. From Task 4 on this test passes `healthy=False`.
- [ ] **Step 3:** Run `uv run pytest tests/unit/risk/test_engine.py -v -x`. Expected: the new tests FAIL; record which pre-existing tests now fail.
- [ ] **Step 4:** Implement the engine changes listed above; update the module docstring (baseline rule, receipt stamps, loop-only `last_snapshot_at`) and the comment at the peak-snapshot write.
- [ ] **Step 5:** Fix pre-existing tests broken by the seeded baseline row (the dry run saw 6 in `test_engine.py`, 1 in `test_cycle_runner.py`), without weakening them:
  - Drawdown tests (`test_equity_peak_seen_by_a_rejected_check_still_bounds_drawdown` and its variants) keep the seeded baseline, so check 4 still runs; only their snapshot-row assertion gains the seed row first (e.g. `[1000 (seed), 1000, 1200]`). Never mark a test `no_baseline` if that makes check 3 reject before the check it is about.
  - Tests about the first snapshot (`test_first_check_snapshots_equity_as_baseline`) and tests that build their own snapshot history get `@pytest.mark.no_baseline`; where they then expected approval, assert the `daily-loss baseline unavailable` rejection and keep their snapshot assertions.
  - `test_last_snapshot_at_tracks_committed_writes` is unchanged in this task (Task 4 adds `healthy=False` to it). Do not change any other assertion.
- [ ] **Step 6:** Run the full check from Global Constraints. Expected: all PASS, `risk/` 100 %.
- [ ] **Step 7: Commit** `feat: engine trusts only a fresh pre-boundary baseline and blocks symbols with unsettled sells`.

### Task 4: Engine: snapshot freshness and cash-fault state as entry faults

**Files:**
- Modify: `src/autotrader/risk/engine.py`
- Modify: `tests/unit/risk/test_engine.py` (`make_engine_`), `tests/integration/test_cycle_runner.py`
- Test: `tests/unit/risk/test_engine.py`

**Interfaces:**
- Consumes: Task 2 `check_entry(..., faults=...)`; Task 3 `snapshot_interval_s`.
- Produces: `RiskEngine.cash_fault: str | None` (initialised `"not yet reconciled"`), `RiskEngine.set_cash_fault(self, reason: str, *, sticky: bool = False) -> None`, `RiskEngine.cash_reconciled(self) -> None` (no-op while sticky; stickiness only ever goes from false to true, so a later plain `set_cash_fault` never downgrades a sticky fault), `RiskEngine.health_faults(self) -> tuple[str, ...]` (order: `"equity snapshot overdue"`, `"cash reconciliation failed"`; overdue when `last_snapshot_at is None` or `self._monotonic() - last_snapshot_at > 2 * snapshot_interval_s`).
- Produces (tests): `make_engine_(..., healthy: bool = True)`: when true, sets `engine.last_snapshot_at` to the engine's monotonic reading at construction (`monotonic()` if given, else `time.monotonic()`) and calls `engine.cash_reconciled()`. Pass `monotonic=time.monotonic if monotonic is None else monotonic` explicitly (the old `**({} if ...)` splat fails mypy `arg-type` once the constructor has a `float` keyword). Tests about `last_snapshot_at` itself (`test_last_snapshot_at_tracks_committed_writes`, Task 3's `test_peak_snapshot_in_check_does_not_refresh_last_snapshot_at`) pass `healthy=False`.

`_gather_entry` returns the faults with its state (computed at the start of the gather, before the
order sync); `check()` passes them as `faults=`. Logging on transitions only: `risk.cash_fault_set`
(warning, `reason`, `sticky`) when the fault goes from clear to set or becomes sticky;
`risk.cash_fault_cleared` (info) when cleared.

- [ ] **Step 1: Failing tests** (all with `healthy=False` unless stated):
  - `test_fresh_engine_rejects_snapshot_overdue_first`: no loop snapshot -> `("equity snapshot overdue",)`.
  - `test_loop_snapshot_older_than_two_intervals_rejects`: `Mono`; `snapshot_equity()` at t=0; t=120 -> approved after `cash_reconciled()`; t=121 -> rejected `equity snapshot overdue`.
  - `test_startup_cash_fault_rejects_until_reconciled`: loop snapshot fresh, fault untouched -> `cash reconciliation failed`; `cash_reconciled()` -> approved.
  - `test_sticky_cash_fault_survives_reconciliation`: `set_cash_fault("CommitUncertain", sticky=True)`; `cash_reconciled()` -> still rejected; a plain `set_cash_fault("x")` then `cash_reconciled()` on a new engine -> approved.
  - `test_plain_fault_never_downgrades_a_sticky_one`: `set_cash_fault("a", sticky=True)`, then `set_cash_fault("b")`, then `cash_reconciled()` -> still rejected `cash reconciliation failed`.
  - `test_restart_with_fresh_journal_baseline_still_waits_for_loop_and_cash` (Review Focus 3): seeded baseline, new engine -> rejected `equity snapshot overdue`; after `snapshot_equity()` -> `cash reconciliation failed`; after `cash_reconciled()` -> approved.
  - `test_fault_transitions_are_logged_once`: two `set_cash_fault` calls -> one `risk.cash_fault_set`; `cash_reconciled()` twice -> one `risk.cash_fault_cleared`.
- [ ] **Step 2:** Run them. Expected: FAIL (`unexpected keyword 'healthy'` / attribute errors).
- [ ] **Step 3:** Implement the engine state, `health_faults`, the gather wiring and `make_engine_(healthy=True)`. In `test_cycle_runner.py` set `engine.last_snapshot_at = time.monotonic()` and call `engine.cash_reconciled()` after construction.
- [ ] **Step 4:** Full check. Expected: PASS, `risk/` 100 %.
- [ ] **Step 5: Commit** `feat: entries fail closed while snapshots are overdue or cash is unreconciled`.

### Task 5: Engine: pre-submit health re-check

**Files:**
- Modify: `src/autotrader/risk/engine.py` (`check()`, between the pre-submit `gate.status()` re-check and `self._submit(...)`)
- Test: `tests/unit/risk/test_engine.py`

**Interfaces:**
- Consumes: Task 4 `health_faults()`.
- Produces: nothing new public.

Right after the existing gate re-check (and only if it allowed), compute
`late = (*self.health_faults(), *(("trading day changed",) if day_start(self._clock(), self._tz) != day_start(now, self._tz) else ()))`.
If non-empty: `await self._set_status(row.id, "not_submitted")`, `log.warning("risk.entry_not_submitted", reasons=list(late))`, return `OrderResult(decision, row.id, "not_submitted")`. No await between this re-check and `self._submit(...)`.

- [ ] **Step 1: Failing tests.** Barriers: (a) a `Gate` whose second `status()` call (the pre-submit re-check) runs a callback before returning allowed; (b) for the order-row insert, a `RiskEngine` test subclass that overrides `_persist` to call `super()._persist(...)` and then arms `hook.block_before` (`tests/dbhooks.py`, `hooked_sessionmaker`), so the *next* commit, the order-row insert, waits on an event the test controls. For the exit-during-barrier case, the exit needs a priced, held second symbol (ETH with a `FakeView.prices` entry and a broker position), otherwise check 8 rejects every entry and the barrier is never reached.
  - `test_cash_fault_set_after_gather_stops_the_submit` (parametrize sticky False/True): the callback calls `engine.set_cash_fault(...)` -> `status == "not_submitted"`, row `not_submitted`, `broker_order_id is None`, the SimBroker received no order, decision persisted approved.
  - `test_cash_fault_set_during_order_insert_stops_the_submit` (Review Focus 5): fault set while the order-row insert commit is blocked -> `not_submitted`, no broker order.
  - `test_snapshot_ageing_past_the_bound_during_gather_stops_the_submit`: `Mono` at 119 s age at gather start; the gate callback advances it to 121 s -> `not_submitted`.
  - `test_trading_day_change_between_gather_and_submit_stops_the_submit`: engine `Clock` at 23:59:59; the gate callback moves it to next day 00:00:01 -> `not_submitted`, log `risk.entry_not_submitted` with `["trading day changed"]` (snapshot/cash state otherwise healthy; the start clock 23:59:59 is on NOW's date, so the autouse baseline applies).
  - `test_exit_goes_through_while_entry_submit_is_blocked`: in each of the cases above, an `exit()` started while the entry waits at the barrier completes `approved`.
- [ ] **Step 2:** Run them. Expected: FAIL (`status == "new"`).
- [ ] **Step 3:** Implement the re-check; update the `check()` part of the module docstring ("asks the control gate and re-checks engine health right before submitting").
- [ ] **Step 4:** Full check. Expected: PASS, `risk/` 100 %.
- [ ] **Step 5: Commit** `feat: re-check engine health right before an entry is submitted`.

### Task 6: `run core` snapshot step runs the cash check and drives the fault

**Files:**
- Modify: `src/autotrader/core/runtime.py` (`_Snapshotter` ~77, `_snapshot_step` ~130, `RiskEngine(...)` ~229, `Loop("risk.snapshot", ...)` ~250, module docstring)
- Test: `tests/unit/core/test_runtime.py`

**Interfaces:**
- Consumes: Task 4 `set_cash_fault`, `cash_reconciled`; `Ledger.cash_guard() -> CashGuard`, `Ledger.check_cash(account: Account, guard: CashGuard) -> CashResult` (`core/ledger.py`); `CommitUncertain` (`core/bus.py`).
- Produces: `async def _snapshot_step(risk: _Snapshotter, ledger: _CashChecker) -> None` with protocols:
  - `_Snapshotter`: `async def snapshot_equity(self) -> Account`, `def set_cash_fault(self, reason: str, *, sticky: bool = False) -> None`, `def cash_reconciled(self) -> None`
  - `_CashChecker`: `async def cash_guard(self) -> CashGuard`, `async def check_cash(self, account: Account, guard: CashGuard) -> CashResult`

Step order and outcomes (spec 5.1-5.2):
Every stage classifies its exception the same way (spec 5.2, any stage):
`set_cash_fault(f"<stage>: {type(e).__name__}", sticky=isinstance(e, CommitUncertain))`.
1. `guard = await ledger.cash_guard()`; on `Exception`: log `ledger.cash_check_failed` (`stage="guard"`, exc_info), set the fault as above, `guard = None`, continue (the snapshot is still taken).
2. `account = await risk.snapshot_equity()`; `TimeoutError` -> `risk.snapshot_failed` `timeout=True`; other `Exception` -> `risk.snapshot_failed` with exc_info; both set the fault as above (stage `snapshot`) and return. Success -> `risk.snapshot_written`.
3. If `guard is None`: return. `result = await ledger.check_cash(account, guard)`; on `Exception` -> `ledger.cash_check_failed` (`stage="check"`) and the fault as above; result in `("baseline", "ok", "discrepancy")` -> `cash_reconciled()`; `"skipped"` -> nothing.
No exception leaves the step (`CancelledError` is not caught). `run_core` passes `snapshot_interval_s=snapshot_s` to `RiskEngine` and `partial(_snapshot_step, risk, ledger)` to the loop.

- [ ] **Step 1: Failing tests** with fakes for both protocols (record calls; no DB), each asserting fault calls and log events:
  - `test_snapshot_step_runs_guard_snapshot_check_in_order_and_clears_on_ok` (also `baseline`, `discrepancy` via parametrize).
  - `test_skipped_cash_check_leaves_the_fault_unchanged`.
  - `test_failed_guard_still_takes_the_snapshot_and_skips_the_check`.
  - `test_snapshot_failure_after_commit_sets_the_fault_and_skips_the_check` (fake raises `ConnectionError` from `snapshot_equity`).
  - `test_commit_uncertain_in_any_stage_sets_a_sticky_fault`, parametrized over `guard`, `snapshot`, `check` (the fake raises `CommitUncertain` in that stage): the fault is sticky; a following step whose stages all succeed and whose check returns `ok` leaves the fault set (the fake engine records `cash_reconciled()` as a no-op while sticky, or use a real `RiskEngine` with fake broker/ledger); only a new engine starts clear of it.
  - `test_plain_error_in_any_stage_sets_a_recoverable_fault`, parametrized over the three stages: a plain `ConnectionError` sets a non-sticky fault; a following `ok` step clears it.
  - Replace `test_snapshot_step_logs_failures_and_successes` with a version over the new signature (same three log outcomes).
  - Fixtures for the two integration tests: a sync fixture that runs `upgrade(url)` (it calls `asyncio.run` and cannot run inside an async test) and yields the URL; reuse the engine builders by `import tests.unit.risk.test_engine as te` (`te.make_engine_`, `te.FakeView`, `te.PROFILE`, `te.five`, `te.RULES`, `te.FEES`) rather than copying them.
  - `test_failed_discrepancy_commit_keeps_entries_closed` (real `RiskEngine` + `Ledger` + SimBroker over `hooked_sessionmaker` from `tests/dbhooks.py`; spec 11 cases 10 and 14): step 1 -> `baseline`, fault cleared; change SimBroker cash by -19.84; arm `hook.fail_before = 1` only after the snapshot committed (wrap the ledger's `check_cash` in the test so it arms the hook before delegating; arming earlier fails the snapshot's own commit and yields a plain fault); advance the engine and ledger clocks by 60 s before each step (`equity_snapshots.ts` is unique); step 2 -> `check_cash` raises `CommitUncertain` (verify finds nothing) -> `paused` unset in the journal, `engine.cash_fault` set and sticky, `check()` rejects `cash reconciliation failed`; step 3 (hook clear) -> `discrepancy` commits the pause, the fault stays (sticky). Separate test `test_discrepancy_clears_a_plain_fault_and_the_pause_rejects`: plain fault set, step -> `discrepancy` -> fault cleared, `check()` rejected by the control gate (pause).
- [ ] **Step 2:** Run `uv run pytest tests/unit/core/test_runtime.py -v`. Expected: FAIL (`_snapshot_step()` signature).
- [ ] **Step 3:** Implement; update the runtime module docstring (snapshot step runs the cash check; the fault).
- [ ] **Step 4:** Full check. Expected: PASS (including the existing hung-snapshot and kill-detection runtime tests).
- [ ] **Step 5: Commit** `feat: run core reconciles cash every snapshot and fails entries closed on a fault`.

### Task 7: Watchdog thread pre-started at handler install

**Files:**
- Modify: `src/autotrader/core/shutdown.py` (`Watchdog`, ~50-110)
- Modify: `src/autotrader/cli.py` (~118-120)
- Test: `tests/unit/core/test_shutdown.py`, `tests/unit/test_cli.py`, `tests/unit/core/test_runtime.py` (if it builds a `Watchdog`)

**Interfaces:**
- Produces: `Watchdog(*, exit=os._exit, stderr=None, log_wait_s=0.5, clock=time.monotonic, thread_factory: Callable[[Callable[[], None]], _Thread] = <daemon threading.Thread named "watchdog">, sleep=time.sleep, poll_s=0.1)`
- Produces: `Watchdog.start(self) -> None` (idempotent; starts the polling thread), `Watchdog.poll(self) -> bool` (one check; fires and returns True when `clock() >= deadline`), `Watchdog.arm(self, seconds: float) -> None`, property `armed: bool` (deadline set), property `started: bool`.
- `_Thread` protocol: `daemon: bool`, `def start(self) -> None`. The `timer_factory` parameter is removed.

`arm()` only assigns `self._deadline = clock() + max(0.0, seconds - log_wait_s)` if not already set
(first wins): no lock, no thread start, no `Event`. If `not started`, it first writes the
`core.watchdog_late_start` JSON line to stderr (best-effort, like `_write_line`) and calls
`start()` (fallback, spec 9.1). The thread target loops `while not self.poll(): sleep(poll_s)`.
`_fire` and exit code 70 are unchanged. The CLI calls `dog.start()` before `install(...)`.

- [ ] **Step 1: Failing tests** (fake thread factory records the target and never runs it; tests drive `poll()` with a fake clock):
  - `test_arm_assigns_a_deadline_without_starting_a_thread`: started watchdog; `arm(35)` -> factory called once (by `start`), not again; `poll()` false before `35 - log_wait_s`, true at it, exit 70.
  - `test_arm_keeps_the_first_deadline`.
  - `test_arm_on_an_unstarted_watchdog_starts_it_and_says_so`: stderr gets `core.watchdog_late_start`, `started` true.
  - `test_cli_starts_the_watchdog_before_installing_the_signal_handler` (`test_cli.py`): the injected `install` spy asserts `dog.started` when called.
  - Port the existing watchdog tests (stderr raising/blocking, log-wait bound, signal arming while the loop is blocked) to `thread_factory`/`poll`; their assertions stay.
- [ ] **Step 2:** Run `uv run pytest tests/unit/core/test_shutdown.py tests/unit/test_cli.py -v`. Expected: FAIL.
- [ ] **Step 3:** Implement; update the `Watchdog` and module docstrings.
- [ ] **Step 4:** Full check. Expected: PASS.
- [ ] **Step 5: Commit** `fix: pre-start the watchdog thread so arming it is signal-safe`.

### Task 8: 1.5a follow-ups: dead tasks at stop, `core.failed` once, missing tests

**Files:**
- Modify: `src/autotrader/core/supervise.py` (`Supervisor.stop`, ~102)
- Modify: `src/autotrader/core/shutdown.py` (`ShutdownControl`), `src/autotrader/core/runtime.py` (~303), `src/autotrader/cli.py` (~131-145)
- Test: `tests/unit/core/test_supervise.py`, `tests/unit/core/test_runtime.py`, `tests/unit/test_cli.py`, `tests/unit/core/test_lock.py`

**Interfaces:**
- Produces: `ShutdownControl.failure_logged: bool = False` (set by whoever logs `core.failed`).

- [ ] **Step 1: Failing tests:**
  - `test_stop_logs_tasks_that_died_before_it` (`test_supervise.py`): a loop task already `done()` with an exception when `stop()` runs -> one `<name>.task_died` with the error.
  - `test_failure_after_an_earlier_stop_request_is_logged_once` (`test_cli.py`): a signal during the migration (existing pattern in `test_signal_during_migration_arms_the_watchdog_and_skips_startup`), then the migration raises -> exactly one `core.failed`.
  - Replace the existing `test_failure_run_core_already_logged_is_not_logged_twice` (`test_cli.py`): its fake `run_core` must now set `control.failure_logged = True` where it logs, and the test asserts the CLI adds no second `core.failed`.
  - The three missing 1.5a tests: `test_startup_raising_after_the_stop_won_the_race` (`test_runtime.py`), parametrized over both orders, using events to fix the order: (i) the startup task raises before `run_core` observed the stop -> `core.failed` once, the error re-raised (exit 1 via the CLI); (ii) teardown already began, then the startup step raises -> `core.shutdown_step_failed` with `step="startup"`, `EXIT_FAILED`, no `core.failed`; `test_restart_waits_for_and_then_acquires_the_lock` (`test_lock.py`: a holder releases after the waiter started waiting -> acquired, no error; use events, no timing asserts); `test_stream_death_does_not_also_log_shutdown_failed` (`test_runtime.py`).
- [ ] **Step 2:** Run them. Expected: the new behaviour tests FAIL; the three missing-coverage tests may already pass (they pin existing behaviour; if one fails, fix the code it exposes and say so in the commit).
- [ ] **Step 3:** Implement: `Supervisor.stop()` logs `task_died` for done, non-cancelled tasks before waiting on the running ones (same fields as `check()`); `run_core` sets `control.failure_logged = True` where it logs `core.failed`; the CLI logs `core.failed` only `if not control.failure_logged` and then sets it, replacing `first = not control.requested`. Update the comment there.
- [ ] **Step 4:** Full check. Expected: PASS.
- [ ] **Step 5: Commit** `fix: log dead loops at stop and core.failed exactly once`.

### Task 9: 1.5c follow-ups: log fields, cancel backoff, anomaly log after commit

**Files:**
- Modify: `src/autotrader/core/position_manager.py` (`_alert_unsettled` ~593-617, `_cancel_entries` ~226, its caller ~212)
- Modify: `src/autotrader/risk/engine.py` (`cancel_open_entries` log calls ~544-547)
- Modify: `src/autotrader/core/ledger.py` (`_anomaly` ~636-680)
- Test: `tests/unit/core/test_position_manager.py`, `tests/unit/core/test_position_manager_control.py`, `tests/unit/risk/test_engine.py`, `tests/unit/core/test_ledger.py`

**Interfaces:**
- Consumes: `Backoff` (`core/exit_rules.py`: `due(now)`, `fail(now) -> bool` alert, `reset()`, `failures`).
- Produces: `PositionManager._cancel_entries(self, now: datetime) -> None` (caller passes the step-start `now`).

Behaviour:
- `positions.exit_unresolved` / `positions.exit_stuck`: replace `key=str(key)` with `**_key_fields(key)` (drop the explicit `external=` argument, `_key_fields` supplies it) and remove `_alert_unsettled`'s then-unused `external` parameter and its callers' argument.
- `risk.cancel_failed` / `risk.entry_cancel_requested`: `order_row=row.id` instead of `order_id=row.id`.
- Cancel backoff: `self._cancel_backoff: dict[int, Backoff]`; `exclude = self._cancel_requested | {rid for rid, b in self._cancel_backoff.items() if not b.due(now)}`; for each `result.failed` row: `if self._cancel_backoff.setdefault(rid, Backoff()).fail(now): log.error("positions.cancel_failed_repeatedly", order_row=rid, failures=...)`; for each `result.requested` row: pop its backoff.
- `Ledger._anomaly`: log `ledger.anomaly` / `anomaly_repaused` / `anomaly_persists` only after `publish_tx` returned; if it raises, log `ledger.anomaly_unrecorded` (error; `kind`, `order_row`, `detail`, `error=type(e).__name__`) and re-raise. Fix the comment that says it is logged even when the commit raises.

- [ ] **Step 1: Failing tests:**
  - exit alert logs carry `position_id` (own) / `symbol` + `external=True` (external) and no `key`.
  - cancel logs carry `order_row`, no `order_id`.
  - `test_failed_cancel_backs_off`: a cancel that keeps failing with the PM clock stepped 1 s at a time is attempted at t=0, 1, 3, 7 s (1, 2, 4 s delays), `positions.cancel_failed_repeatedly` at the 3rd failure; after an accepted cancel the row is excluded as before.
  - `test_anomaly_repause_not_logged_when_commit_fails`: `hooked_sessionmaker` `fail_before=1` on the re-pause unit -> no `ledger.anomaly_repaused`, one `ledger.anomaly_unrecorded`, the exception propagates; update the existing assertion at `test_ledger.py:~1034` only if its scenario changes.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3:** Implement as above; adjust existing tests that asserted the old field names (`risk.cancel_failed` count test at `test_position_manager_control.py:~372` keeps its count).
- [ ] **Step 4:** Full check. Expected: PASS.
- [ ] **Step 5: Commit** `fix: exit alert fields, cancel backoff, anomaly logs after commit`.

### Task 10: Flaky budget tests

**Files:**
- Test: `tests/unit/risk/test_engine.py` (`test_hung_entry_submit_is_unknown` ~856, `test_only_network_calls_count_against_the_budget` ~1183)

- [ ] **Step 1: Confirm the cause** (superpowers:systematic-debugging). With `Mono` frozen, `_within` gives every call `io_timeout_s` real seconds, so with 0.05 a non-hanging SimBroker call over 50 ms (CI load) times out the gather first. Reproduce in a scratch run: monkeypatch `SimBroker.get_account` to `await asyncio.sleep(0.06)` before delegating -> `test_hung_entry_submit_is_unknown` fails with `state unavailable: TimeoutError`. Record the result in the commit message. If it does not reproduce, stop and report instead of changing the tests.
- [ ] **Step 2:** Fix: `io_timeout_s=1.0` in both tests (the hanging call still times out; non-hanging calls get 20x headroom over the observed failure); the journal case sleeps `2 * budget` (2 s) so it still proves journal awaits are outside `wait_for`; raise the outer `asyncio.wait_for` timeouts (2 in the first test, 5 in the second) to `10`. No other assertion changes. This is a real-time margin, not a guarantee: `_within` hands every call real seconds by design, so a deterministic fix would need a fake event-loop clock, which is out of proportion here. Spec 9.3 is corrected accordingly (dated note in this plan PR).
- [ ] **Step 3:** Run `uv run pytest tests/unit/risk/test_engine.py -k "hung_entry_submit or only_network_calls" -v` and once with the 60 ms scratch delay applied to a non-hanging call. Expected: PASS both times. Remove the scratch patch.
- [ ] **Step 4:** Full check. Expected: PASS.
- [ ] **Step 5: Commit** `test: give non-hanging calls headroom in the budget tests`.

### Task 11: Docs

**Files:**
- Modify: `PLAN.md` (1.5 row: a dated "<merge date>, 1.5d-1 done" note with spec/plan paths, using the real date; "Current repo state" line)
- Modify: `docs/runbook.md`
- Check: `src/autotrader/risk/engine.py`, `core/runtime.py`, `core/shutdown.py` docstrings (done per task), `docs/superpowers/specs/*` (do not rewrite dated specs)

- [ ] **Step 1:** Runbook: a section "Entries rejected" listing each new reason, what clears it, and that a `daily-loss baseline unavailable` day is expected after downtime across 00:00 UTC; the cash-discrepancy procedure (inspect the `CashDiscrepancy` event, explain the change, resume with the existing manual-pause SQL); `ledger.anomaly_unrecorded` and `positions.cancel_failed_repeatedly` meanings.
- [ ] **Step 2:** `rg -n "day_start_equity|snapshot_overdue|cash check|1\.5d|last snapshot at or before" src docs/runbook.md CLAUDE.md` and fix every claim this WP falsified (not dated history). Most `1.5d` hits stay true (they point at 1.5d-2/-3); the known stale one is the runbook's "WP 1.5d must block or defer such an entry" (now done by check 5a).
- [ ] **Step 3:** Full check (ruff also runs on docs-adjacent code comments). Expected: PASS.
- [ ] **Step 4: Commit** `docs: 1.5d-1 runbook and plan status`.
