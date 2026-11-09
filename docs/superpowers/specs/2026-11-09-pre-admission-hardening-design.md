# Pre-admission hardening design (WP 1.5d-1)

Date: 2026-11-09. Status: decisions approved by the owner as PLAN D19 (PR #29, revised twice after
Codex review). This written spec is in review. Choices made here that D19 does not fix are marked
**(builder call)**.

## 1. Scope

1.5d is split into three PRs (PLAN 1.5 row, 2026-11-09). 1.5d-1 makes `check()` safe to call from
the Cycle Runner before 1.5d-3 wires it. It delivers:

1. a trustworthy daily-loss baseline, or no entries (D19 item 1);
2. entry rejection while equity snapshots are overdue (D19 item 2);
3. entry rejection on a symbol with an unsettled sell (D19 item 3; 1.5c final-review requirement);
4. the cash check in `run core`, with a fail-closed cash-reconciliation fault (D19 item 4);
5. an index on `events.type` (D19 item 5);
6. the 1.5a and 1.5c follow-ups and the two flaky tests (D19 item 6).

Out of scope: the uncertain-order resolver and journal-repair tooling (1.5d-2); the startup
sequence, Cycle Runner wiring and admission, the runner/ledger race test (1.5d-3). `run core` still
calls no `check()` after this WP; everything here is exercised by tests and becomes reachable in
1.5d-3. Alpaca still cannot place orders (1.6).

## 2. Entry gate overview

After this WP, `check()` rejects an entry (persisted `RiskDecision`, no order row) when any of these
hold, in addition to today's checks:

| Reason text | Condition | Cleared by |
|---|---|---|
| `daily-loss baseline unavailable` | no snapshot in `[day_start - 2 x interval, day_start]` | the next trading day with such a snapshot |
| `equity snapshot overdue` | the snapshot loop's last committed snapshot is older than 2 x interval, or none yet in this process | a snapshot loop step that commits |
| `cash reconciliation failed` | the cash-reconciliation fault is set (section 5) | a completed cash check |
| `unsettled sell on <SYM>` | the intent's symbol has an order row with `side='sell'` and `settled_at IS NULL` | the ledger settling that sell |

None of these rejections sets a pause (`decision.pause` stays `None`): they clear themselves when
the condition ends. Exits (`exit()`), cancels and the Position Manager are untouched by all of
them.

`interval` is the snapshot loop interval, `SNAPSHOT_S = 60` in `run core`; the engine takes it as a
new constructor argument `snapshot_interval_s: float = 60.0` and derives both 2 x bounds from it, so
the bounds and the `risk.snapshot_overdue` warning cannot drift apart.

## 3. Daily-loss baseline (D19 item 1)

New repo function `trusted_day_start_equity(session, day_start, max_age) -> Decimal | None`: the
equity of the latest snapshot with `day_start - max_age <= ts <= day_start`, else `None`. There is
no fallback to a later snapshot or to current equity.

`_gather_entry` reads it with `max_age = 2 x snapshot_interval_s` (120 s). `PortfolioState.
day_start_equity` becomes `Dec | None`. `check_entry` check 3: if it is `None`, reject
`daily-loss baseline unavailable` with no pause; otherwise today's comparison and its
`until_next_day` pause are unchanged.

Consequences the owner accepted (PR #29): the first day of operation has no entries; a UTC day
whose boundary the process did not observe (down, asleep, or two failed snapshots right before
00:00) has no entries. Later snapshots never clear it.

The existing `repo.day_start_equity` (with its first-snapshot-after fallback) stays only for the
informational `equity_snapshots.daily_pnl` column, which no limit reads; its docstring says so
**(builder call: keeps the column populated on the first day; renaming is not worth the churn)**.
No code path used for a decision may call it; a test asserts `check()` rejects in a state where
`day_start_equity` would return a value but `trusted_day_start_equity` returns `None`.

The PLAN D14 text ("start-of-day = last snapshot at/before the trading-day start") is superseded by
D19 and is not rewritten (dated record); the engine module docstring and any comment stating the
old rule are corrected.

## 4. Snapshot freshness (D19 item 2)

`RiskEngine.last_snapshot_at` (monotonic) today is set by every `_write_snapshot`, including the
peak snapshot `check()` writes when equity exceeds the stored HWM. **(builder call)** Only the
snapshot loop's `snapshot_equity()` updates it after this WP: otherwise entries that keep making
new peaks would keep the gate fresh while the snapshot loop (and with it the cash check, section 5)
is hung. The `risk.snapshot_overdue` warning reads the same attribute, so it follows the loop too.
This is stricter than a literal reading of D19 item 2 ("last committed equity snapshot"), so this
PR adds a dated clarification to D19 saying freshness counts snapshot-loop snapshots only
(decision records must not trail the code).

`_gather_entry` computes the age with the engine's `monotonic` clock at the start of the gather;
`None` (no loop snapshot yet in this process) or an age `> 2 x snapshot_interval_s` adds the fault
reason `equity snapshot overdue` (section 6 for how faults reach `check_entry`).

A snapshot step that commits its snapshot and then raises (position read failed) still refreshes
`last_snapshot_at`; the cash fault (section 5) covers that case.

## 5. Cash check in `run core` and the cash-reconciliation fault (D19 item 4)

### 5.1 The step

`_snapshot_step` in `core/runtime.py` becomes one step over the risk engine and the ledger:

```
guard = await ledger.cash_guard()          # before the account read (D17)
account = await risk.snapshot_equity()      # commits the snapshot; may raise after committing
result = await ledger.check_cash(account, guard)
```

If `cash_guard()` raises, the step still runs `snapshot_equity()` and skips `check_cash()` (no
guard, no valid comparison): the fault is set, but the snapshot is taken, so a journal-read error
right before 00:00 cannot also cost the next day's baseline **(builder call)**. If
`snapshot_equity()` raises, `check_cash()` is skipped (no account).

Same loop, same interval and stall bound as today. Logging: `risk.snapshot_failed` when
`snapshot_equity()` raises (as today, with `timeout=True` for `TimeoutError`),
`ledger.cash_check_failed` (error, with the exception) when `cash_guard()` or `check_cash()` raises,
`risk.snapshot_written` as today after a committed snapshot. No exception leaves the step.

### 5.2 The fault

State on the engine, in memory, per process:

- `cash_fault: str | None`, initialised to `"not yet reconciled"` in `__init__`;
- `cash_fault_sticky: bool`, initialised `False`.

Engine methods used only by the step: `set_cash_fault(reason: str, *, sticky: bool = False)` and
`cash_reconciled()`. `cash_reconciled()` clears the fault unless it is sticky.

Rules, applied by the step:

| Outcome of the step | Effect on the fault |
|---|---|
| any part raises `CommitUncertain` | `set_cash_fault(..., sticky=True)`: holds until restart (parity with D15) |
| any part raises another `Exception` (including `TimeoutError`) | `set_cash_fault(<exception type>)` |
| `check_cash` returns `baseline`, `ok` or `discrepancy` | `cash_reconciled()` |
| `check_cash` returns `skipped` | unchanged |

"Any part" is `cash_guard()`, `snapshot_equity()` and `check_cash()` (D19 item 4, second review):
`snapshot_equity()` can commit a fresh snapshot and then raise on the position read, so
`check_cash()` never runs. `asyncio.CancelledError` is not an `Exception` and is not caught (a
cancelled step at shutdown changes nothing).

A `discrepancy` clears the fault because its unit committed the manual pause together with the
`CashDiscrepancy` event; the pause then rejects entries until an operator resumes. A `skipped`
result (an order was unsettled at either read, or the generation moved) compared nothing, so it
neither sets nor clears the fault: after a restart entries wait until a check completes, however
long orders stay unsettled.

While `cash_fault` is set, `_gather_entry` adds the fault reason `cash reconciliation failed`
(section 6). The fault reason text is not exposed in the decision reason **(builder call: one
stable reason string; the detail is in the step's log line)**.

### 5.3 A hung step

The supervisor runs the step serially; a step hung after its snapshot committed starts no further
step, so `last_snapshot_at` ages and section 4 rejects entries within 2 x interval (120 s). This is
the bound on a hang; the fault itself is set only by exceptions. Journal awaits are not inside
`wait_for` (D14) and SQLite raises after `busy_timeout` (5 s), so a journal hang is itself
unexpected.

### 5.4 Alpaca paper today

`run core` connects `AlpacaBroker`, so the cash check runs against the paper account from this WP
on. A manual trade in the paper account, or a repeat of the unexplained -19.84 USD drop, is a
`discrepancy`: manual pause + `CashDiscrepancy` (D17, intended). The runbook gets the operator
procedure: inspect the `CashDiscrepancy` event, explain the change, then resume with the runbook's
existing manual-pause SQL (there is no resume command until the Phase 3 API); re-baselining already
happened in the same unit. One `CashChecked` event is written
per completed check (about one a minute), which is one reason for section 8.

## 6. How the new conditions reach `check_entry`

**(builder call)** `check_entry` gains a keyword argument `faults: tuple[str, ...] = ()`. After
check 1 (control and mode), if `faults` is non-empty it rejects with `faults[0]` as the reason
(no pause). The engine builds `faults` in `_gather_entry` in this order: `equity snapshot overdue`,
`cash reconciliation failed`. Keeping them in the pure function keeps the decision persisted and
audited the same way as every other rejection, and keeps `risk/` at 100 % branch coverage with
plain unit tests.

The baseline (section 3) stays in check 3 via `day_start_equity is None`, and the unsettled sell
(section 7) is its own check. `_gather_entry` still runs fully when a fault is present (order sync
and booking are useful either way); a gather that raises keeps today's `state unavailable`
rejection.

## 7. Unsettled-sell guard (D19 item 3)

New repo function `unsettled_sell_symbols(session) -> set[str]`: symbols of order rows with
`side='sell'` and `settled_at IS NULL`, any status, own or external. `_gather_entry` reads it in the
same session block as `open_entry_orders`, after the order sync and booking.

`PortfolioState` gains `unsettled_sells: frozenset[str] = frozenset()`. New check 5a in
`check_entry`, right before check 5: if `intent.symbol in unsettled_sells`, reject `unsettled sell
on <SYM>` (no pause). It does not add the symbol to check 5's `busy` set, so an unsettled external
sell never counts toward `max_positions` (D19).

This closes the 1.5c final-review gap (spec 2026-10-19 4.4.1, 2026-10-29 correction): a buy can no
longer open a position while an external sell on that symbol is unsettled, so external fills can't
land in a trade's `realized_pnl`. An id-less `pending`/`unknown` sell stays unsettled until the
1.5d-2 resolver (or the runbook procedure) settles it, and blocks its symbol's entries until then;
that is intended.

Sells placed outside the system (the Alpaca UI) have no order row; they show up as a cash
`discrepancy`, not here.

## 8. Index on `events.type` (D19 item 5)

`Event.type` gets `index=True` in `journal/models.py`; migration `0006` creates `ix_events_type`
(autogenerated name convention as in earlier migrations, `render_as_batch`). The model/migration
parity test in `tests/unit/journal/test_db.py` must pass.

## 9. Follow-ups (D19 item 6)

### 9.1 From 1.5a (PR #23 final review)

1. **Watchdog thread pre-started.** Today `Watchdog.arm()` creates and starts a `threading.Timer`
   inside the first-signal handler; `Thread.start()` there can deadlock when the signal lands
   while the main thread is itself inside `Thread.start()`. New: a `Watchdog.start()` called when
   the signal handler is installed (CLI, before `install(...)`) starts one daemon thread. `arm()`
   only assigns a deadline (a float attribute; no lock, no thread start, no `Event.set`), so it is
   safe in a signal handler. The thread polls the deadline every 0.1 s **(builder call)** and calls
   `_fire()` when it passes, keeping today's `log_wait_s` early firing and exit code 70. `arm()`
   stays idempotent (first deadline wins). The injectable `timer_factory` is replaced by an
   injectable thread/clock seam so tests stay deterministic (no wall-clock asserts). A watchdog
   must never be silently off: `arm()` on a watchdog that was never started falls back to
   starting its thread itself (today's behaviour, with today's risk) and logs
   `core.watchdog_late_start` to stderr; a CLI test asserts the watchdog is started before the
   signal handler is installed **(builder call: raising inside a signal handler is worse than
   the old risk)**.
2. **`Supervisor.stop()` logs dead tasks.** A loop task already `done()` (not cancelled) when
   `stop()` runs logs `<loop>.task_died` with its exception, as `check()` does, instead of being
   skipped silently.
3. **`core.failed` logged exactly once.** `ShutdownControl` gains `failure_logged: bool`. Whoever
   logs `core.failed` (run_core during teardown, or the CLI) sets it; the CLI logs only if it is
   not set, replacing the `first = not control.requested` gate. Fixes: a failure after an earlier
   stop request was logged only as the stderr summary.
4. **Missing tests:** startup raising after the stop request won the race; the restart waits for
   and then acquires the journal lock; a stream death does not also log `core.shutdown_failed`.

### 9.2 From 1.5c (PR #28 final review)

1. `positions.exit_unresolved` and `positions.exit_stuck` log `**_key_fields(key)`
   (`position_id`/`symbol`, `external`) instead of `key=str(key)`.
2. `risk.cancel_failed` and `risk.entry_cancel_requested` log `order_row=row.id` (the name used by
   the ledger and the Position Manager) instead of `order_id=row.id`, which reads like a broker id.
3. **Cancel backoff.** A failing entry cancel is retried on every 1 s Position Manager step today.
   The manager keeps a `Backoff` (`core/exit_rules.py`, 1, 2, 4 ... 30 s) per failed order row,
   skips rows whose backoff is not due via the existing `exclude` argument, resets it when the
   cancel is accepted, and logs `positions.cancel_failed_repeatedly` when `Backoff.fail` says
   alert. `risk.cancel_failed` stays one line per attempt. Entries are marketable IOC orders (D14),
   so a resting entry needing a cancel is already an anomaly; the backoff trades nothing material.
4. **`ledger.anomaly_repaused` only after the commit.** `Ledger._anomaly` logs `anomaly`,
   `anomaly_repaused` or `anomaly_persists` only after `publish_tx` returned. If it raises, it logs
   `ledger.anomaly_unrecorded` (error, with kind, row, detail and the exception type) and re-raises,
   so a warning never claims a pause that may not have committed.

### 9.3 Flaky tests

`test_hung_entry_submit_is_unknown` and `test_only_network_calls_count_against_the_budget[journal]`
(`tests/unit/risk/test_engine.py`) both run the engine with `io_timeout_s=0.05` real seconds while
the fake `Mono` clock keeps the shared budget from shrinking, so every *non-hanging* broker call
gets a 50 ms real `wait_for`. Hypothesis: under CI load one of those calls (or the thread hop of a
SimBroker call) exceeds 50 ms, the gather times out first, and the result is `state unavailable`
instead of the asserted outcome. The builder confirms the cause before fixing (e.g. reproduce under
load or by injecting a 60 ms delay into a non-hanging call) and fixes it so that only the call
meant to hang can exhaust the budget, without wall-clock assertions (WP workflow lesson). The fix
must not weaken what each test proves.

## 10. Docs

Same PR as the code:
- PLAN: 1.5 row (1.5d-1 done), "Current repo state" line.
- `risk/engine.py` module docstring: the new rejections, the cash fault, `last_snapshot_at` loop-only.
- `core/runtime.py` module docstring: the snapshot step runs the cash check; the fault.
- `docs/runbook.md`: entries rejected with each new reason and what clears it; the cash
  discrepancy procedure (5.4); a baseline-unavailable day is expected after downtime across 00:00
  UTC.
- `rg` the touched concepts for stale absolutes (`day_start_equity`, `snapshot_overdue`,
  `cash check`, `1.5d`) in code comments, specs and the runbook; dated specs and PLAN history
  entries are not rewritten.

## 11. Acceptance tests

From D19 item 7, plus the cases this spec adds. All use SimBroker, injected clocks, no wall-clock
asserts.

Baseline:
1. Gapped boundary: snapshots stale pre-start 1000 (more than 120 s before 00:00), none near the
   boundary, first post-start 1100, now 1080: `check()` rejects `daily-loss baseline unavailable`.
2. No pre-start snapshot at all (first day): rejects, even with many snapshots after the start.
3. A snapshot 119 s before the day start: baseline used; 121 s: rejected (boundary).
4. Day N rejected for the baseline; day N+1 with a fresh pre-start snapshot: allowed.
5. Exits and cancels still work on a baseline-unavailable day.

Snapshot freshness:
6. No loop snapshot yet in the process: rejects `equity snapshot overdue`.
7. Last loop snapshot 121 s old (fake monotonic): rejects; a peak snapshot written by `check()`
   does not make it fresh.

Cash fault:
8. Fresh engine after one loop step whose cash check returned `skipped` (so the snapshot is fresh
   and only the startup fault remains): rejects `cash reconciliation failed`; after a step that
   completes with `baseline`, allowed.
9. Fresh snapshot, then `check_cash` raises: rejects.
10. The `CashDiscrepancy` unit's commit fails (`CommitUncertain`): rejects, and a later `ok` check
    does not clear it (sticky); the probe from PR #29: -19.84 USD change, no pause committed,
    entries still rejected.
11. `snapshot_equity()` commits its snapshot, then the position read raises: `check_cash` is not
    called, entries rejected although the snapshot is fresh.
12. A later fresh snapshot with a `skipped` cash check does not clear the fault.
13. Fault set by a plain exception, then an `ok` check: entries allowed again.
14. `discrepancy`: fault cleared, manual pause set, entries rejected by the pause.

Unsettled sell:
15. External sell on BTC unsettled: BTC entry rejected `unsettled sell on BTC/USD`; an ETH entry
    still allowed, and the BTC sell doesn't count toward `max_positions`.
16. After the ledger settles that sell: BTC entry allowed.

Follow-ups: each item in section 9 has a regression test (watchdog armed from a signal handler
without starting a thread; dead task logged at stop; `core.failed` once after an earlier stop
request; the three 1.5a tests; log fields; cancel backoff timing with an injected clock; no
`anomaly_repaused` when the commit fails).

Existing tests: every test that expects an approved entry from a real `RiskEngine`
(`tests/unit/risk/test_engine.py` via `make_engine_`/`RecordingEngine`, and
`tests/integration/test_cycle_runner.py` through the Cycle Runner) now hits the new gates. The
plan adds one shared fixture/helper that seeds a snapshot inside the 120 s before the day start,
marks a loop snapshot fresh and clears the cash fault (a `baseline` cash check), and the existing
tests use it; their assertions do not change.

`risk/` stays at 100 % branch coverage; mypy strict on `core/`, `risk/`, `broker/`.
