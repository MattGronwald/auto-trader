# Startup sequence and Cycle Runner admission design (WP 1.5d-3)

Date: 2026-11-11. Status: owner decisions in chat 2026-11-11, recorded as PLAN D21; this written
spec is in review. Choices made here that D21 does not fix are marked **(builder call)**.
Revised 2026-11-11 after a cold builder check against the code, owner go: a mode mismatch fails
startup (D21.6, section 2), the Position Manager readiness step needs a trusted bid for every
held symbol and must start after a completed sync (D21.7, section 3.2), `_sync_step` reports
success (3.2, 5). Calls the spec author made that the owner did not are marked **(Claude's call)**.

## 1. Scope and intent

Today `run core` runs the feed, the scanner, the order-sync loop (with the 1.5d-2 resolver), the
Position Manager and the snapshot/cash loop, but it never builds the Cycle Runner
([runtime.py `start()`](../../../src/autotrader/core/runtime.py)). `CandidateSignal` events are
persisted and nobody acts on them. `check()` has no caller in production, and teardown step 1 is
a placeholder ("WP 1.5d: close admission, runner.close()").

1.5d-3 delivers (PLAN 1.5 row, 2026-11-09 split):

1. **The safe startup sequence** (PLAN 3.2 item 1): recover cycles, reconcile broker orders and
   fills, act on overdue exits, complete a cash check, and only then admit entries (sections 2-3).
2. **Cycle Runner wiring:** the runner is built, recovered and subscribed in `run core`, and closed
   first in teardown (sections 2, 4).
3. **The runner/ledger race integration test** (1.5b follow-up c), with the real engine, ledger and
   runner on `SimBroker` (section 6).

Why: this is the last piece before entries can flow. Every per-entry gate already exists in
`check()` (1.5d-1: mode, cash fault, snapshot and baseline freshness, unsettled sells, failed
syncs, incidents). What is missing is a defined point after a (re)start at which the journal has
been reconciled with the broker, and before which no cycle can start.

**Non-goals:**
- Alpaca order placement (1.6). With `AlpacaBroker`, `can_place_orders()` is false, so admission
  stays closed in production until 1.6 (PLAN 1.5 row: "keep Alpaca admission disabled until 1.6").
- Per-symbol readiness. Per-symbol faults (failed rows, incidents, unsettled sells) stay with
  `check()`; they do not hold back admission for every symbol.
- Re-closing admission after it opened. Later faults are handled per entry by `check()` and the
  gate, as today.
- Persistent account/mode binding and cross-host ownership (3.5); the LLM Decision Maker (Phase 2).
  The runner keeps the stub decider.

**Anti-goals** (a technically working result that would still be a failure):
- a cycle admitted before a sync-and-book pass, a Position Manager step that saw a trusted price
  for every held symbol, and a completed cash check have run in this process;
- a core that keeps running while the broker mode differs from the configured mode (its exits would
  be rejected too, so it would only look protected), or admission opening with a broker that
  cannot place orders;
- `recover()` running after any admission (it could move a live cycle's `RISK_CHECK` to `ERROR`);
- a candidate dropped during startup without a log line saying why;
- the exits (Position Manager, flatten) depending on admission: they run whether or not admission
  ever opens.

## 2. Startup order (D21.1, D21.4)

Inside `run_core`'s `start()`, in this order (new steps in bold):

1. register the strategy; connect the broker (`_check_broker`); **if `broker.mode() != cfg.mode`,
   raise** (a `RuntimeError` naming both modes, never a key) so startup fails through the existing
   path: `core.failed`, teardown, non-zero exit (D21.6). Reason: `exit()` rejects every exit on a
   mode mismatch ([engine.py `exit()`](../../../src/autotrader/risk/engine.py), `check_exit(...,
   mode_matches=...)`), so a running core would protect nothing; a mismatch means the keys belong
   to the wrong account type, and stopping loudly is the safe answer. This check runs before the
   backfill, so no loop and no stream start;
2. build the feed and backfill it;
3. build the gate and the scanner, subscribe the scanner to `BarClosed`;
4. build the ledger and the engine;
5. **build the `CycleRunner`** (profile, strategy id, `market=feed`, `gate`, `risk=engine`, bus,
   sessions, `trading_day_tz`), with admission closed (section 3.1);
6. **`await runner.recover()`.** If it raises, startup fails: `core.failed` is logged and the core
   stops, as for any startup failure (D21.4). This keeps the existing contract of `recover()`
   ("the caller must then not subscribe the runner") without a second, degraded mode;
7. **subscribe `runner.on_candidate` to `CandidateSignal`**;
8. the existing stop-request check (`control.requested` -> return `None`);
9. build the poller and the Position Manager, start the supervisor (with the new
   `admission.ready` loop, section 3.2) and the stream; log `core.started`.

Steps 5-7 run before the stream starts, so no `BarClosed`, hence no `CandidateSignal`, can reach
the runner before `recover()` has finished. A stop request during step 6 is handled like one during
the backfill: the unfinished startup is the first teardown step.

`start()` returns the runner along with the supervisor and the stream task (shape is a builder
call: a small dataclass is fine).

## 3. Admission readiness

### 3.1 Runner: closed until opened (D21.1)

The runner gets an admission flag that starts **closed**:

- `_blocked()` returns `"startup reconciliation pending"` while it is closed. It is checked where
  `_blocked()` is checked today (before the gate read, inside the admission lock, inside the
  `publish_tx` build), so a closed runner drops the candidate with `cycle.dropped` and
  `reason="startup reconciliation pending"`. The candidate event itself is already persisted by
  the scanner, so the drop is auditable.
- New `open_admission()`: raises `RuntimeError` unless `recover()` completed without raising in
  this runner instance; otherwise sets the flag and logs `cycle.admission_open` once. Idempotent.
- The order of precedence in `_blocked()` is: shutting down, then suspended (`CommitUncertain`),
  then startup pending **(builder call)**.
- A dropped candidate still sets `_admitting_started` (it already does for every `_admit` call),
  so `recover()` cannot run after one, closed or not. This is the existing guard and stays.
- The closed default is the safe default for every caller. Existing runner tests that admit cycles
  call `recover()` and `open_admission()` in their fixture **(builder call: fixture shape)**.

### 3.2 The `admission.ready` loop (D21.2)

A new supervised loop, `admission.ready`, interval `tick_s`, stall level `warning`. Its step opens
admission once all of these hold, then does nothing on later steps:

(The mode check is not a readiness condition: a mismatch already failed startup, section 2.)

| | Condition | How it is known | If it fails |
|---|---|---|---|
| a | the broker can place orders | `risk.can_place_orders()` read once in `start()` | permanent for this process |
| b | one order-sync pass completed after startup | `_sync_step` returns `True` (below) | retried every tick |
| c | one Position Manager step that **started after (b)** completed **with a trusted bid for every symbol with an open position** | readiness wrapper around `pm.step()` (below) | retried every tick |
| d | the cash fault is clear | `risk.cash_fault is None` (a completed cash check: `baseline`, `ok` or `discrepancy`) | retried every tick |

- **(b): `_sync_step` reports success** (its one behaviour change). Today it catches every
  exception and only logs `ledger.sync_failed` ([runtime.py `_sync_step`](../../../src/autotrader/core/runtime.py)).
  It now returns `True` when `sync_orders()` and `apply_all()` both returned, `False` when it
  logged a failure; it still never raises. The loop's step adapter discards the value; the
  readiness record sets (b) on the first `True`. Per-row failures inside a pass that completed
  count, including a pass in which every broker read failed (`sync_orders` records those per row
  instead of raising): they are `check()`'s per-symbol concern, and `check()` rejects entries on
  any symbol whose row failed to sync **(Claude's call)**.
- **(c): ordered after (b), and price-aware** (D21.7). A position whose entry filled while the core
  was down exists only after the first sync books it, so a Position Manager step that started
  before (b) never saw it and does not count **(Claude's call)**. Stop, take-profit and trailing
  triggers need a trusted bid (`_trusted_bid`: a quote that is present, not stale, and younger than
  `feed.stale_alert_s`); `backfill()` loads no quotes, so the first steps usually run without one
  and would fire only time exits. A step therefore counts for (c) only if, at its start, every
  symbol with an open journal position had a trusted bid. With no open positions any step that
  started after (b) counts. The check reads the same market view and the same staleness rule as
  the Position Manager, computed by the readiness wrapper from the positions the step read
  **(builder call: expose the step's position symbols, or re-read them; either way the symbols must
  be those the step evaluated)**. This is what makes "act on overdue exits" (PLAN 3.2 item 1) hold
  before admission: exit state is durable (migration 0005), and that step fires any stop, TP,
  trailing or time trigger that came due while the core was down. It also satisfies SPEC 4.5: the
  Position Manager is active before entry admission is enabled.
  Cost, accepted by the owner: while the feed for a held symbol gives no trusted bid, admission
  stays closed for every symbol.
- (d) is checked at the moment the loop evaluates; it is the same fault `check()` rejects on. A
  sticky fault (`CommitUncertain`) or a cash check that keeps returning `skipped` (unsettled rows,
  incidents; spec 2026-11-10 3.2 note) keeps admission closed. That matches what `check()` would
  do anyway and is accepted. After a restart with unsettled rows the first cash check is usually
  `skipped`, so admission typically waits one more snapshot interval (60 s); accepted, no separate
  pre-admission snapshot cadence **(Claude's call)**.
- (a) is static for the process. If it fails, `start()` logs `cycle.admission_disabled
  reason="broker cannot place orders"` (warning) once and the `admission.ready` step never opens
  admission. No retry: it cannot change without a restart.
- While (b)-(d) are not yet met, the step logs nothing per tick. After the loop has been waiting
  for longer than 3 x `snapshot_s` (so an ordinary restart that waits one snapshot interval does
  not warn), it logs `cycle.admission_waiting` (warning) once, with the unmet conditions (and, for
  (c), the held symbols without a trusted bid), and `cycle.admission_open` when it opens.
- Once admission is open it stays open for the process (D21.3). `check()` and the gate cover every
  later fault per entry.

### 3.3 What happens to exits

Nothing changes for the order-sync loop, the resolver, the Position Manager, `flatten` or
`shutdown.flatten`: they start as today, before and independently of admission. If admission never
opens (Alpaca until 1.6, a sticky cash fault, a held symbol without a trusted bid), protective
exits keep running (PLAN 3.2 item 1: "failure keeps entries disabled; protective exits remain
available"). A mode mismatch is the exception: there the engine rejects exits too, so the core does
not start at all (section 2).

## 4. Teardown (D21.5)

The placeholder step 1 becomes **`runner.close`**, and it moves before the `shutdown.flatten`
step, so no entry can start while a flatten is running:

0. an unfinished startup (unchanged);
1. **`runner.close`:** `bounded("runner.close", runner.close(budget_s=min(CLOSE_BUDGET_S,
   control.remaining())))`, only if `start()` built a runner. `close()` sets `closing` (admission
   is closed for good), cancels cycles before `check()`, drains `check()` calls, and returns the
   abandoned cycle ids. Each abandoned id is added to the forced list as `cycle:<id>`, so the exit
   is unclean and `core.shutdown_abandoned` names it;
2. `flatten` (unchanged, `shutdown.flatten` only). It now gets half of what is left after
   `runner.close`; with the 30 s default and a `check()` in flight that can be under 10 s. The
   flatten request is persisted and is picked up again after a restart, so a short window delays a
   flatten but does not lose it;
3. onwards unchanged: `source.stop`, supervisor, stream, `bus.close`, `engine.dispose`.

The comment and the `core.shutdown_abandoned` note lose their "WP 1.5d" pointers: abandoned work
is now recovered by the startup sequence in section 2.

## 5. Things that do not change

- `check()`, the ledger, the resolver, the Position Manager and the snapshot/cash step: no behaviour
  change. `_sync_step` only gains its success return value (3.2); its existing test
  (`test_sync_step_logs_a_failure_and_never_raises`) also asserts `False`. The runner's FSM, caps
  and `close()` semantics: unchanged apart from section 3.1.
- `shutdown.timeout_s` validation: unchanged (the default 30 s stays above `CLOSE_BUDGET_S`; a
  smaller configured value just shortens `runner.close` through `min(...)`).
- No schema change, no new config key, no profile hash change.

## 6. Acceptance tests (SimBroker, no real API)

Runner unit tests (`tests/unit/core/test_cycle.py`):
1. A new runner drops a candidate with `reason="startup reconciliation pending"` and inserts no
   cycle.
2. `open_admission()` before `recover()` raises; after a `recover()` that raised it still raises;
   after a clean `recover()` it opens, and a second call is a no-op (one `cycle.admission_open`).
3. `recover()` after a dropped (closed-runner) candidate raises (existing guard, now on the closed
   path).

Runtime tests (`tests/unit/core/test_runtime.py`):
4. With a `SimBroker` that can place orders and matching mode, admission opens only after a sync
   pass, then a Position Manager step started after it, plus a completed cash check; each
   condition is held back in turn with an injected step (events, not wall-clock timing) and
   admission stays closed until it is released. A Position Manager step that completed but
   started before the first successful sync does not count.
5. Mode mismatch: startup fails (`core.failed`, non-zero exit), no loop and no stream start, the
   error names both modes and no key.
6. `can_place_orders()` false: admission never opens, `cycle.admission_disabled reason="broker
   cannot place orders"` is logged once, and the Position Manager and sync loops still run.
7. `_sync_step` returns `False` when `sync_orders` or `apply_all` raises, and that pass does not
   count for (b); a later clean pass (`True`) does.
8. A sticky cash fault keeps admission closed.
8a. An open position whose symbol has no quote: Position Manager steps without a trusted bid do
    not count for (c); admission opens only after a step that had one, and that step fired the
    position's overdue stop (an exit order exists) before `cycle.admission_open`. With no open
    positions, a step without any quote counts.
8b. `cycle.admission_waiting` is logged once (not per tick) after 3 x `snapshot_s` with the unmet
    conditions and the held symbols lacking a trusted bid; not logged when admission opens
    within one snapshot interval.
9. `recover()` raising fails startup (`core.failed`, exit via the startup-failure path), and the
   runner is never subscribed.
10. A candidate published before readiness is dropped and logged; one published after it is
    admitted (end to end: `BarClosed` -> scanner -> `CandidateSignal` -> cycle).
11. Teardown: `runner.close` runs before `flatten` and before `source.stop`; an abandoned cycle is
    reported as `cycle:<id>` in `core.shutdown_forced` / `core.shutdown_abandoned` and makes the
    exit unclean; with no runner (startup failed before step 5) the step is skipped.
12. Restart: a journal left with a `RISK_CHECK` cycle without an order row, a `SUBMITTED` cycle
    whose entry filled at the broker while the core was down, and a position whose stop came due
    while down. With quotes delivered for both held symbols, after startup: the first is
    `ERROR`, the second `FILLED` with its position open, the third has an exit order, all before
    `cycle.admission_open`.

Integration (`tests/integration/test_runner_ledger_race.py`, 1.5b follow-up c), real
`RiskEngine` + `Ledger` + `CycleRunner` on `SimBroker`, interleavings forced with injected events
(no sleeps, no timing asserts):
13. The sync pass books the entry's fill and completes the cycle `FILLED` **before** the runner
    writes `SUBMITTED`: the runner logs `cycle.completed_elsewhere`, the cycle has exactly one
    `FILLED` transition, one position.
14. The runner writes `SUBMITTED` (status `new`) **before** the sync books the fill: the ledger
    completes it `FILLED`; exactly one terminal transition.
15. The runner writes `SUBMITTED` then `FILLED` (status `filled`) while a sync pass books the same
    order: exactly one `FILLED` transition, fills booked once (deduplicated by `broker_fill_id`).
16. A zero-fill final outcome (IOC canceled, nothing filled) in both orders: the cycle ends
    `EXPIRED` once (D15).
17. The 1.5d-1 handoff: `_snapshot_step` (snapshot plus cash check) followed by an approved
    `check()` through the runner, on the real engine (a follow-up noted at the 1.5d-1 merge).

## 7. Docs (same PR as each change)

Claims this change falsifies, to correct (search for `1.5d`, `1.5d-3`, `no entries`, `does not
subscribe`, `nothing to close`, `startup recovery`):
- `SPEC.md` line 7 (implementation boundary: "does not subscribe the Cycle Runner or trade");
- `src/autotrader/core/runtime.py` module docstring ("`check()` still waits for the Cycle Runner
  (WP 1.5d-3)", "startup recovery (WP 1.5d)"), the teardown step-1 comment and the
  `core.shutdown_abandoned` note;
- `src/autotrader/core/cycle.py`: `recover()` docstring ("startup reconciliation is 1.5d"), module
  docstring (admission now also waits for startup readiness);
- `src/autotrader/core/config.py:372` comment ("wired in WP 1.5d");
- `docs/runbook.md` "Current limits (WP 1.5d-2)" and the lines saying nothing places entries before
  1.5d-3 (17, 40, 326): with SimBroker the runner now places entries; with Alpaca admission stays
  disabled until 1.6;
- `PLAN.md` "Current repo state" line and the 1.5 row (dated note, never rewriting earlier notes).

New surface to document in `docs/runbook.md`: a "Startup and admission" section with the readiness
conditions (section 3.2), the log lines (`cycle.admission_open`, `cycle.admission_waiting`,
`cycle.admission_disabled`, `cycle.dropped reason="startup reconciliation pending"`), and what to
do when admission does not open (read the unmet conditions; a held symbol without a trusted bid
needs its feed back; a sticky cash fault needs a restart). A mode mismatch now stops the core at
startup: fix `mode` in `config.yaml` or the keys in `.env`.

Dated specs of earlier WPs are not rewritten.

## 8. Left to the builder

- Shape of `start()`'s return value and where the readiness flags live (section 2, 3.2).
- Precedence of the closed reason in `_blocked()` (3.1) and the runner test fixture shape.
- How the readiness wrapper learns the symbols the Position Manager step evaluated (3.2 (c)).
- Whether the readiness evaluation is a supervised `Loop` (as specified) or a check in the existing
  supervise loop of `run_core`; the loop is preferred because it is covered by stall detection.
