# Startup sequence and Cycle Runner admission design (WP 1.5d-3)

Date: 2026-11-11. Status: owner decisions in chat 2026-11-11, recorded as PLAN D21; this written
spec is in review. Choices made here that D21 does not fix are marked **(builder call)**; calls the
spec author made that the owner did not are marked **(Claude's call)**.
Revised 2026-11-11 after a cold builder check against the code, owner go: a mode mismatch fails
startup (D21.6), the readiness Position Manager step must cover every held position with a
trusted bid (D21.7).
Revised 2026-11-11 after an independent review of PR #37, owner go: readiness is ordered cash check
first, then the Position Manager step, and the separate sync-pass condition is dropped (3.2); the
Position Manager step reports per position what it evaluated (3.2, 5); a failing `recover()` no
longer stops the core (D21.4 reversed, section 2); a one-sided holdings check joins readiness
(D21.8, 3.3); the mode check is a backstop, not an account check (section 2); readiness is a
small clock-injected class (3.2, 6).

## 1. Scope and intent

Today `run core` runs the feed, the scanner, the order-sync loop (with the 1.5d-2 resolver), the
Position Manager and the snapshot/cash loop, but it never builds the Cycle Runner
([runtime.py `start()`](../../../src/autotrader/core/runtime.py)). `CandidateSignal` events are
persisted and nobody acts on them. `check()` has no caller in production, and teardown step 1 is
a placeholder ("WP 1.5d: close admission, runner.close()").

1.5d-3 delivers (PLAN 1.5 row, 2026-11-09 split):

1. **The safe startup sequence** (PLAN 3.2 item 1): recover cycles, reconcile the journal with the
   broker's orders, fills and holdings, act on overdue exits, and only then admit entries
   (sections 2-3).
2. **Cycle Runner wiring:** the runner is built, recovered and subscribed in `run core`, and closed
   first in teardown (sections 2, 4).
3. **The runner/ledger race integration test** (1.5b follow-up c), with the real engine, ledger and
   runner on `SimBroker` (section 7).

Why: this is the last piece before entries can flow. Every per-entry gate already exists in
`check()` (1.5d-1: mode, cash fault, snapshot and baseline freshness, unsettled sells, failed
syncs, incidents). What is missing is a defined point after a (re)start at which the journal has
been reconciled with the broker, and before which no cycle can start.

**Non-goals:**
- Alpaca order placement (1.6). With `AlpacaBroker`, `can_place_orders()` is false, so admission
  stays closed in production until 1.6 (PLAN 1.5 row: "keep Alpaca admission disabled until 1.6").
- Per-symbol readiness. Readiness is global: a condition that fails on one symbol keeps admission
  closed for every symbol. Some per-symbol faults block readiness through the cash check, notably
  incidents and any other unsettled row (3.2 (b)); others (a symbol whose sync failed after
  admission opened) stay with `check()`.
- Re-closing admission after it opened. Later faults are handled per entry by `check()` and the
  gate, as today.
- A full two-sided holdings reconciliation with a fee/rounding allowance (WP 3.1 / 1.6); the
  holdings check here is one-sided and exact (3.3).
- Persistent account/mode binding and cross-host ownership (3.5); the LLM Decision Maker (Phase 2).
  The runner keeps the stub decider.

**Anti-goals** (a technically working result that would still be a failure):
- a cycle admitted before a completed cash check (which proves no order row is unsettled), a later
  Position Manager step that evaluated every held position with a trusted bid, and a holdings check
  have run in this process;
- admission opening with a broker that cannot place orders, or after a failed `recover()`;
- `recover()` running after any admission (it could move a live cycle's `RISK_CHECK` to `ERROR`);
- a candidate dropped during startup without a log line saying why;
- the exits (Position Manager, flatten) depending on admission or on `recover()`: they run whether
  or not admission ever opens. The only startup failure that stops them is one where they could
  not work anyway (a mode mismatch, section 2).

## 2. Startup order (D21.1, D21.4, D21.6)

Inside `run_core`'s `start()`, in this order (new steps in bold):

1. register the strategy; connect the broker (`_check_broker`); **if `broker.mode() != cfg.mode`,
   raise** (a `RuntimeError` naming both modes, never a key) so startup fails through the existing
   path: `core.failed`, teardown, non-zero exit (D21.6). This is a backstop, not an account check:
   `load_config` already rejects `mode` against `ALPACA_PAPER`
   ([config.py `load_config`](../../../src/autotrader/core/config.py)), and `AlpacaBroker.mode()`
   echoes that same flag ([alpaca.py `mode()`](../../../src/autotrader/broker/alpaca.py)), so with
   Alpaca it cannot fire today; it guards a future broker or wiring whose mode is derived
   differently. Stopping is the right answer for a mismatch because `exit()` rejects every exit
   then ([engine.py `exit()`](../../../src/autotrader/risk/engine.py), `check_exit(...,
   mode_matches=...)`), so a running core would protect nothing. It runs before the backfill, so
   no loop and no stream start;
2. build the feed and backfill it;
3. build the gate and the scanner, subscribe the scanner to `BarClosed`;
4. build the ledger and the engine;
5. **build the `CycleRunner`** (profile, strategy id, `market=feed`, `gate`, `risk=engine`, bus,
   sessions, `trading_day_tz`), with admission closed (section 3.1);
6. **`await runner.recover()`.** If it raises (D21.4, revised): log `cycle.admission_disabled
   reason="recover failed"` (error, with the exception), do **not** subscribe the runner, and
   continue startup; readiness then never opens admission (`open_admission()` refuses, 3.1). The
   loops, and with them every exit, still start. Reason: PLAN 3.2 item 1 ("failure keeps entries
   disabled; protective exits remain available"); one bad cycle row or a transient SQLite lock
   while the operator runs `kill`/`flatten` must not take the stops down. Recovery: fix the cause
   and restart;
7. **subscribe `runner.on_candidate` to `CandidateSignal`** (only after a clean `recover()`);
8. the existing stop-request check (`control.requested` -> return `None`);
9. build the poller, the Position Manager and the readiness tracker; start the supervisor (with
   the new `admission.ready` loop, 3.2) and the stream; log `core.started`.

Steps 5-7 run before the stream starts, so no `BarClosed`, hence no `CandidateSignal`, can reach
the runner before `recover()` has finished. A stop request during step 6 is handled like one during
the backfill: the unfinished startup is the first teardown step. No await sits between step 8 and
the loop start, so a stop request after `recover()` starts nothing.

`start()` returns the runner along with the supervisor and the stream task (shape is a builder
call: a small dataclass is fine).

At startup, also log `admission.unquoted_holdings` (warning) once for any symbol with an open
journal position that is not in `profile.universe` (e.g. the universe was edited between runs):
the feed never quotes it, so readiness (c) cannot pass until that position closes (3.2).

## 3. Admission readiness

### 3.1 Runner: closed until opened (D21.1)

The runner gets an admission flag that starts **closed**, and a disabled reason:

- `_blocked()` returns, in this order **(builder call: relative order with the two existing
  reasons)**: `"shutting down"`, `"admission suspended: commit uncertain"`, then
  `"admission disabled: <reason>"` when admission was disabled (broker cannot place orders, recover
  failed), else `"startup reconciliation pending"` while closed. It is checked where `_blocked()`
  is checked today (before the gate read, inside the admission lock, inside the `publish_tx`
  build), so a closed runner drops the candidate with `cycle.dropped` and that reason. The
  candidate event is already persisted by the scanner, so the drop is auditable.
- New `disable_admission(reason)`: records the reason, logs nothing itself (the caller logs
  `cycle.admission_disabled`). New `open_admission()`: raises `RuntimeError` unless `recover()`
  completed without raising in this runner instance; is a no-op while closing (after `close()`
  began) or disabled; otherwise sets the flag and logs `cycle.admission_open` once. Idempotent.
- A dropped candidate still sets `_admitting_started` (it already does for every `_admit` call),
  so `recover()` cannot run after one, closed or not. This is the existing guard and stays.
- Dropped candidates still count toward the scanner's `max_candidates_per_symbol_per_hour` (it
  counts persisted `CandidateSignal` events, [scanner.py](../../../src/autotrader/core/scanner.py)),
  so a symbol that signalled often while admission was closed can be rate-limited for up to an hour
  after it opens. Accepted: the limit is conservative in that direction **(Claude's call)**.
- The closed default is the safe default for every caller. Existing tests that admit cycles, unit
  (`tests/unit/core/test_cycle.py`) and integration (`tests/integration/test_cycle_runner.py`),
  call `recover()` and `open_admission()` in their fixture **(builder call: fixture shape)**.

### 3.2 Readiness conditions (D21.2, D21.7, D21.8)

Readiness is a small class in `core/` (`AdmissionReadiness`, name a builder call) with an injected
monotonic clock. It holds the conditions below and decides; `run_core` only feeds it and opens the
runner. It is evaluated by a new supervised loop, `admission.ready`, interval `tick_s`, stall level
`warning`, whose step calls the tracker and, when it says ready, `runner.open_admission()`; after
that the step does nothing.

| | Condition | How it is known | If it fails |
|---|---|---|---|
| a | admission not disabled: the broker can place orders and `recover()` succeeded | `risk.can_place_orders()` read once in `start()`; step 6 | permanent for this process |
| b | the cash fault is clear | `risk.cash_fault is None`, observed by the tracker; the first observation time `T_b` is recorded | retried every tick |
| c | a Position Manager step that **started after `T_b`** evaluated every open position with a trusted bid, or found it already latched | the step's report (below) | retried every tick |
| d | the holdings check passed, taken after `T_b` (3.3) | the readiness step runs it | retried every tick; a shortfall is logged |

Order and why (finding of the PR #37 review, owner go): (b), then (c) and (d).

- **(b) proves the journal is caught up.** The cash check completes (`baseline`, `ok` or
  `discrepancy`, the only results that clear the fault) only if no order row is unsettled at its
  guard read and the order/fill generation did not move during it
  ([ledger.py `_guard`, `check_cash`](../../../src/autotrader/core/ledger.py)). So at `T_b` every
  order the journal knows of is settled and booked, including rows the resolver linked (a linked
  row is booked by a later sync pass, `apply_all` docstring; until then it is unsettled and the
  check is `skipped`). After `T_b` no position can appear except through an entry, and admission
  is closed. This replaces the earlier "one sync pass completed" condition, which a resolver link
  could satisfy before the linked fill was booked; `_sync_step` stays unchanged **(Claude's call,
  owner go)**. A sticky fault (`CommitUncertain`) or a check that keeps returning `skipped` keeps
  admission closed. In particular **any incident row** (it stays unsettled until `repair`) keeps
  admission closed for every symbol after a restart; this is the 1.5d-2 restart behaviour (spec
  2026-11-10 3.2 note), now also a readiness fact. After a restart with unsettled rows the first
  check is usually `skipped`, so admission typically waits one more snapshot interval (60 s);
  accepted **(Claude's call)**. A `discrepancy` clears the fault but commits the manual pause, so
  the gate still blocks every entry after admission opens; nothing extra is needed.
- **(c) proves overdue exits were acted on** (D21.7; PLAN 3.2 item 1 "act on overdue exits"; SPEC
  4.5: the Position Manager is active before entry admission). Exit state is durable (migration
  0005). Stop, take-profit and trailing triggers need a trusted bid (`_trusted_bid`: a quote that
  is present, not stale, and younger than `feed.stale_alert_s`), and `backfill()` loads no quotes,
  so early steps fire only time exits. A "completed" step is not enough either: `step()` swallows
  per-position exceptions, a latch can return without latching (`CommitUncertain`, a concurrent
  close), and the bid is read per position at the decision point, after the step's control-phase
  awaits ([position_manager.py `step`, `_manage`, `_latch`](../../../src/autotrader/core/position_manager.py)).
  So **`PositionManager.step()` returns a report** (its one behaviour change, section 5): per open
  position it read, one of `evaluated` (`evaluate()` ran with a non-None bid, whatever it decided,
  and any fired trigger was latched), `latched` (an exit trigger was already latched before or by
  this step), `no_bid`, or `failed` (an exception, or a latch that did not commit). The supervised
  loop's step adapter discards the report; the readiness wrapper keeps the last one. (c) holds for
  a step that started after `T_b` and whose report has every position `evaluated` or `latched`. An
  empty report (no open positions) counts. Cost, accepted by the owner: while a held symbol has no
  trusted bid, admission stays closed for every symbol; a held symbol outside the universe never
  gets one (section 2 warning), so admission waits for that position's time exit (or a manual
  close and restart).
- **(d)** is section 3.3.
- (a) is static for the process. If it fails, `start()` logs `cycle.admission_disabled` once
  (`reason="broker cannot place orders"` at warning level, `reason="recover failed"` at error
  level) and calls `runner.disable_admission(reason)`; the readiness step never opens admission.
  No retry: neither can change without a restart.
- While (b)-(d) are not yet met, the step logs nothing per tick. After the tracker has been waiting
  longer than 3 x `snapshot_s` (so an ordinary restart that waits one snapshot interval does not
  warn), it logs `cycle.admission_waiting` (warning) once, with the unmet conditions (for (c), the
  positions not `evaluated`/`latched` and why; for (d), the short symbols), and
  `cycle.admission_open` when it opens. The thresholds and the transitions are unit-tested on the
  tracker with an injected clock (section 7).
- Once admission is open it stays open for the process (D21.3). `check()` and the gate cover every
  later fault per entry.

### 3.3 Holdings check (D21.8)

PLAN 3.2 item 1 asks to reconcile positions, and SPEC 4.7 to compare broker positions against
fills. Nothing does that at runtime today: the per-symbol identity Σ open qty + `symbol_dust` =
broker holding is only a test invariant (fill-accounting spec 2026-10-15, section 4.4 invariant), and the cash check
catches a missing holding only indirectly. 1.5d-3 adds a **one-sided** check to readiness:

- **Rule:** for every symbol with an open journal position or a `symbol_dust` balance,
  `Σ open positions.qty + symbol_dust.qty <= broker total qty` (the held qty, not
  `qty_available`, dust included). A broker holding larger than the journal is legitimate (manual
  coins in a traded symbol, spec 2026-10-19 4.4.1) and is not checked. Exact comparison: SimBroker
  books exactly; Alpaca cannot admit before 1.6, which must revisit this rule with its coin-fee
  rounding (recorded for 1.6 in the PLAN note).
- **Broker read:** a new lock-free engine method, `broker_holdings() -> dict[str, Decimal]`, with
  its own `io_timeout_s` budget, returning the total qty of every broker position (dust included).
  The broker stays inside the engine (SPEC 0.1). Built on the existing `_broker_positions`
  **(builder call)**.
- **When:** in the readiness step, after `T_b`. To keep the comparison free of in-flight orders,
  the step reads the ledger's cash guard before the broker read and again after the journal read;
  if either shows an unsettled row or the generation moved, the result is discarded and retried on
  the next tick (no log). A Position Manager exit placed after `T_b` makes the guard unsettled until
  it books, which only delays the check.
- **On a shortfall:** admission stays closed; `admission.holdings_short` (error) is logged once per
  symbol, with journal qty, dust and broker qty, and re-logged only if the figures change. No
  manual pause is committed **(Claude's call)**: admission is already closed, and the check
  re-runs every tick, so a holding that reappears (e.g. a late broker update) opens admission
  without an operator step. A broker read that raises is a retry, logged at warning once per
  failure streak.
- Once passed, the check is not repeated (admission stays open, D21.3).

### 3.4 What happens to exits

Nothing changes for the order-sync loop, the resolver, `flatten` or `shutdown.flatten`, and the
Position Manager only gains its report: they start as today, before and independently of
admission. If admission never opens (Alpaca until 1.6, a failed `recover()`, a sticky cash fault,
an incident, a held symbol without a trusted bid, a holdings shortfall), protective exits keep
running (PLAN 3.2 item 1). A mode mismatch is the exception: there the engine rejects exits too, so
the core does not start at all (section 2).

## 4. Teardown (D21.5)

The placeholder step 1 becomes **`runner.close`**, and it moves before the `shutdown.flatten`
step, so no entry can start while a flatten is running:

0. an unfinished startup (unchanged);
1. **`runner.close`:** `bounded("runner.close", runner.close(budget_s=min(CLOSE_BUDGET_S,
   control.remaining())))`, only if `start()` returned a runner (it did not if startup failed or
   was still running). `close()` sets `closing` (admission is closed for good, and
   `open_admission()` becomes a no-op, 3.1), cancels cycles before `check()`, drains `check()`
   calls, and returns the abandoned cycle ids. Each abandoned id is added to the forced list as
   `cycle:<id>`, so the exit is unclean and `core.shutdown_abandoned` names it. A runner whose
   `recover()` failed has no tasks; its close returns at once;
2. `flatten` (unchanged, `shutdown.flatten` only). It waits for half of what is left
   (`_shutdown_flatten`), so with the 30 s default and a `check()` that took the whole 20 s close
   budget it gets about 5 s. The flatten request is persisted and is picked up again after a
   restart, so a short window delays a flatten but does not lose it;
3. onwards unchanged: `source.stop`, supervisor (the `admission.ready` loop stops with the
   others), stream, `bus.close`, `engine.dispose`.

The comment and the `core.shutdown_abandoned` note lose their "WP 1.5d" pointers: abandoned work
is now recovered by the startup sequence in section 2.

## 5. Things that change outside the new code, and things that do not

Changes:
- `PositionManager.step()` returns its per-position report (3.2 (c)); its decisions, retries, kill
  and flatten handling are unchanged. The supervised loop discards the value.
- `RiskEngine.broker_holdings()` is new (3.3); no existing engine method changes.
- `CycleRunner`: closed/disabled admission and the two new methods (3.1).

No change: `check()`, `exit()`, the ledger (including `_sync_step` and the cash check), the
resolver; the runner's FSM, caps and `close()` semantics apart from 3.1; `shutdown.timeout_s`
validation (the default 30 s stays above `CLOSE_BUDGET_S`; a smaller value just shortens
`runner.close` through `min(...)`). No schema change, no new config key, no profile hash change.

## 6. Logs

New: `cycle.admission_open` (info), `cycle.admission_waiting` (warning, once),
`cycle.admission_disabled` (warning or error, once), `cycle.dropped` with the new reasons,
`admission.holdings_short` (error), `admission.unquoted_holdings` (warning, startup). Names of the
logger prefix for the tracker (`admission.` vs `cycle.`) are a **builder call** as long as the
runbook lists the final names.

## 7. Acceptance tests (SimBroker, no real API; no wall-clock timing)

Runner unit tests (`tests/unit/core/test_cycle.py`):
1. A new runner drops a candidate with `reason="startup reconciliation pending"` and inserts no
   cycle; a disabled runner drops with `"admission disabled: <reason>"`.
2. `open_admission()` before `recover()` raises; after a `recover()` that raised it still raises;
   after a clean `recover()` it opens, and a second call is a no-op (one `cycle.admission_open`);
   after `close()` began, or after `disable_admission()`, it is a no-op.
3. `recover()` after a dropped (closed-runner) candidate raises (existing guard, now on the closed
   path).

Readiness tracker unit tests (injected clock and inputs, no loops):
4. Opens only when (a)-(d) hold; a Position Manager report from a step that started before `T_b`
   does not count, one that started after it does.
5. A report with any `no_bid` or `failed` position does not count; all `evaluated`/`latched`
   counts; an empty report counts.
6. Holdings check: a shortfall keeps it closed and logs once per symbol (again only when the
   figures change); a broker holding above the journal passes; dust counts on the journal side; a
   guard showing an unsettled row or a moved generation discards the result silently.
7. `cycle.admission_waiting` is logged once after 3 x `snapshot_s` on the injected clock, with the
   unmet conditions; not logged when the tracker opens earlier.

Position Manager unit tests:
8. The report: `evaluated` with a bid (both when a trigger fires and when none does), `latched`
   for an already-latched position, `no_bid` without a trusted quote, `failed` when `_manage`
   raises and when the latch hits `CommitUncertain`.

Runtime tests (`tests/unit/core/test_runtime.py`, wiring only; steps monkeypatched or driven by
events where needed):
9. Mode mismatch: startup fails (`core.failed`, non-zero exit), no loop and no stream start, the
   error names both modes and no key.
10. `can_place_orders()` false: admission never opens, `cycle.admission_disabled reason="broker
    cannot place orders"` once, candidates dropped with the disabled reason, and the Position
    Manager and sync loops still run.
11. `recover()` raising: the runner is not subscribed, `cycle.admission_disabled reason="recover
    failed"` is logged, the core keeps running, and an overdue stop on a held position is still
    exited by the Position Manager.
12. A candidate published before readiness is dropped and logged; one published after it is
    admitted (end to end: `BarClosed` -> scanner -> `CandidateSignal` -> cycle).
13. Teardown: `runner.close` runs before `flatten` and before `source.stop`; an abandoned cycle is
    reported as `cycle:<id>` in `core.shutdown_forced` / `core.shutdown_abandoned` and makes the
    exit unclean; with no runner returned (startup failed or unfinished) the step is skipped; the
    readiness loop cannot open admission after `runner.close` began.
14. A held symbol outside the universe logs `admission.unquoted_holdings` at startup.

Restart scenarios (real engine, ledger, Position Manager and runner on `SimBroker`; quotes injected):
15. A journal left with a `RISK_CHECK` cycle without an order row, a `SUBMITTED` cycle whose entry
    filled at the broker while the core was down, and a position whose stop came due while down.
    With quotes delivered for both held symbols, after startup: the first is `ERROR`, the second
    `FILLED` with its position open, the third has an exit order, all before
    `cycle.admission_open`.
16. **Position booked on a later pass** (the PR #37 review case): an `unknown` entry row without a
    broker id whose order filled while the core was down. The resolver links it on pass 1, a
    Position Manager step runs before pass 2 books it, the new position's stop is overdue and its
    symbol has no quote yet. Admission does not open until a quote arrives and the stop has been
    latched and its exit sent.
17. **Swallowed failure:** one held position's evaluation raises on the first steps; admission
    stays closed until a step evaluates it.
18. **Holdings shortfall:** a journal position whose broker holding was removed while down keeps
    admission closed (`admission.holdings_short`), while its exit attempts continue as today.

Integration (`tests/integration/test_runner_ledger_race.py`, 1.5b follow-up c), real
`RiskEngine` + `Ledger` + `CycleRunner` on `SimBroker`, interleavings forced with injected events:
19. The sync pass books the entry's fill and completes the cycle `FILLED` **before** the runner
    writes `SUBMITTED`: the runner logs `cycle.completed_elsewhere`, the cycle has exactly one
    `FILLED` transition, one position.
20. The runner writes `SUBMITTED` (status `new`) **before** the sync books the fill: the ledger
    completes it `FILLED`; exactly one terminal transition.
21. The runner writes `SUBMITTED` then `FILLED` (status `filled`) while a sync pass books the same
    order: exactly one `FILLED` transition, fills booked once (deduplicated by `broker_fill_id`).
22. A zero-fill final outcome (IOC canceled, nothing filled) in both orders: the cycle ends
    `EXPIRED` once (D15).
23. The 1.5d-1 handoff: `_snapshot_step` (snapshot plus cash check) followed by an approved
    `check()` through the runner, on the real engine (a follow-up noted at the 1.5d-1 merge).

## 8. Docs (same PR as each change)

Claims this change falsifies, to correct (search for `1.5d`, `1.5d-3`, `no entries`, `does not
subscribe`, `nothing to close`, `startup recovery`, `admitting entries`):
- `SPEC.md` line 7 (implementation boundary: "does not subscribe the Cycle Runner or trade") and
  line 534 ("WP 1.5d resolves the previous writer's SQLite work before admitting entries": now
  name the startup sequence and readiness);
- `CLAUDE.md` `run core` command line (add the runner and readiness-gated admission; still no
  orders with Alpaca until 1.6);
- `src/autotrader/core/runtime.py` module docstring ("`check()` still waits for the Cycle Runner
  (WP 1.5d-3)", "startup recovery (WP 1.5d)"), the teardown step-1 comment and the
  `core.shutdown_abandoned` note;
- `src/autotrader/core/cycle.py`: `recover()` docstring ("startup reconciliation is 1.5d"; the
  caller now disables admission instead of failing), module docstring (admission also waits for
  readiness);
- `src/autotrader/core/position_manager.py` `step()` docstring (it returns a report);
- `src/autotrader/core/config.py:372` comment ("wired in WP 1.5d");
- `docs/runbook.md` "Current limits (WP 1.5d-2)" and the lines saying nothing places entries before
  1.5d-3 (17, 40, and the sentence at 325-326): with SimBroker the runner now places entries; with
  Alpaca admission stays disabled until 1.6;
- `PLAN.md` "Current repo state" line and the 1.5 row (dated note, never rewriting earlier notes).

New surface in `docs/runbook.md`, a "Startup and admission" section:
- the readiness conditions (3.2, 3.3) and the log lines (section 6);
- what to do when admission does not open: read `cycle.admission_waiting`; an incident or other
  unsettled row needs its `repair` (core stopped) and a restart; a held symbol without a trusted
  bid needs its feed back, or, if it is outside the universe, a universe fix or a manual close; a
  holdings shortfall needs investigation of the missing coins (journal correction by hand, as for
  other anomalies); a sticky cash fault or a failed `recover()` needs its cause fixed and a
  restart;
- a mode mismatch stops the core at startup: check `mode` in `config.yaml` against
  `ALPACA_PAPER` in `.env` (normally `check-config` and `load_config` already catch it).

Dated specs of earlier WPs are not rewritten.

## 9. Left to the builder

- Shape of `start()`'s return value; the tracker's name, module and API (3.2).
- Relative order of the runner's block reasons (3.1) and the runner test fixture shape.
- How the report is typed (a small dataclass or tuple per position) and how the readiness wrapper
  records a step's start time.
- `broker_holdings()` implementation on top of `_broker_positions` (3.3).
- Log prefix of the tracker's lines (section 6).
- Whether the readiness evaluation is a supervised `Loop` (as specified) or a check in the existing
  supervise loop of `run_core`; the loop is preferred because it is covered by stall detection.
