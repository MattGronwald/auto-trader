# Cycle Runner design (WP 1.4)

Date: 2026-10-11. Status: approved in conversation, pending written-spec review.
Revised 2026-10-11 after the first written-spec review: orders after an `ERROR` cycle,
the bus contract, bounded shutdown, SimBroker test scope, signal freshness, stub rounding,
and `canceled` ≠ nothing filled. Revised again after the second review: commit-and-enqueue
survives caller cancellation, with read-back on an uncertain commit; `publish_tx` callbacks
return their event; shutdown budgets and abandoned tasks; reconciliation-only edges.
Third review: only a positive read-back confirms a commit, and any uncertain commit
suspends admission until restart; delivery and edge wording corrected. Approved
2026-10-11 with three implementation notes, folded in: suspension is checked inside the
admission lock; the bus-owned unit reports `CommitUncertain` itself (`on_uncertain`); the
Postgres caveat for recovery goes to WP 1.6.
Sources: SPEC §0, §4.3, §4.4, §5, §7, §11; PLAN G14, D13, D14.
Decisions taken while designing are recorded as PLAN **D15**.

## 1. Purpose and success criteria

The Cycle Runner turns each delivered `CandidateSignal` into at most one audited cycle and
drives it through a deterministic FSM. In Phase 1 a rule-based stub stands in for the
agents. It is the first caller of `RiskEngine.check()`.

Done means:
- `max_concurrent_cycles` and "one open cycle per symbol" hold exactly, even when
  candidates for several symbols arrive together (G14).
- Every state change is persisted with its timestamp and details in the same transaction
  as the `cycles` row update, and events are delivered in the order they were persisted.
- `check()` is called at most once per cycle and is never retried.
- A crash, a DB failure or a shutdown never loses track of an order. Every order that may
  exist at the broker has a durable journal row (`pending`/`new`/`unknown`), whatever
  state its cycle ends in.

**Identity.** "One cycle per candidate" holds under the current delivery model: the bus
delivers each event at most once, in process, and nothing replays events after a restart.
There is no durable candidate identity. If replay is ever added, it must add one (e.g. the
candidate's event id on the cycle row, unique).

Out of scope:
- **Wiring into `run core`.** Entries are IOC limits without a bracket (D14), so stops
  and time exits exist only once the Position Manager lands. WP 1.5 subscribes the runner.
- **Resolving submitted orders.** That covers fills, cancellations, zero versus partial
  fills, and orders left behind by an `ERROR` cycle; all of it is WP 1.5/1.6
  reconciliation (§8).
- LLM agents, the cost meter and the budget check (Phase 2).

## 2. Approach

The `CandidateSignal` handler only **admits**. It checks freshness, the gate and the caps,
and inserts the cycle row. It then starts one `asyncio.Task` per cycle that walks the FSM.
The bus worker stays fast, so a slow cycle (an LLM call in Phase 2) never delays admitting
the next candidate.

Rejected alternatives:
- **Running the cycle inline in the bus handler.** Cycles would run strictly one at a
  time: `max_concurrent_cycles` becomes meaningless, and Phase 2 would need a rewrite.
- **A persisted work queue with a poller that resumes open cycles.** Steps are not
  idempotent (LLM calls cost money; `check()` must never be retried), so resuming is the
  wrong goal.

## 3. Structure

| File | Content |
|---|---|
| `core/cycle.py` (new) | `CycleState`, the allowed-transition table, `CycleTransitioned` event, `CycleRunner` (admission, per-cycle task, recovery, shutdown). |
| `agents/stub.py` (new) | `decide(candidate, snapshot, limits) -> StubBuy \| StubSkip`. Pure function with module constants. Phase 1 only; deleted in Phase 2. |
| `core/bus.py` | `publish_tx(build, verify)`, `CommitUncertain`, bounded `close(budget_s)` (§6, §8). |
| `journal/repo.py` | `append_event(..., commit=True)` gains a no-commit mode for `publish_tx`; `insert_cycle`, `transition_cycle`, `count_open_cycles`, `open_cycles` (for recovery), `has_order_for_cycle`, `open_entry_symbols`. |

No schema change and no migration. `cycles` already has `state`, `outcome`,
`reject_reason` and `finished_at`. The candidate's `timeframe` and `bar_ts` go into
`signal_features` under reserved keys `_timeframe` and `_bar_ts`, next to the feature
values (the column is JSON; feature names never start with `_`).

## 4. FSM

```
NEW → ENRICHING → ANALYZING → DECIDING → RISK_CHECK → SUBMITTED → FILLED
                                                                → REJECTED
ENRICHING → EXPIRED            (stale symbol, no ask, ATR missing or ≤ 0)
DECIDING  → REJECTED           (stub skip)
RISK_CHECK → REJECTED          (risk rejection)
any non-terminal state → ERROR

Shared edges (runner and WP 1.5 reconciliation):
SUBMITTED  → FILLED            (runner: immediate `filled` ack; reconciliation: broker
                                filled qty > 0, full or partial)
RISK_CHECK → EXPIRED           (runner: signal stale on entry, §5, without calling `check()`;
                                reconciliation: a cycle left in RISK_CHECK with an order
                                row whose broker status is final with filled qty = 0)

Reconciliation-only edges (taken by WP 1.5, never by the runner):
SUBMITTED  → EXPIRED           (broker final with filled qty = 0)
RISK_CHECK → FILLED            (cycle left in RISK_CHECK with an order row, filled qty > 0)
```

The transition table is defined in WP 1.4, including the reconciliation-only edges, so
WP 1.5 uses the same `transition_cycle` and audit trail. The runner never takes the
reconciliation-only edges. On the shared edges the two sources differ only in `details`:
the runner's immediate ack carries no filled qty, while reconciliation records
`filled_qty`, `ordered_qty` and `partial`.

Terminal states are `FILLED`, `REJECTED`, `EXPIRED` and `ERROR` (`repo.TERMINAL_CYCLE_STATES`
is unchanged). There is no SKIPPED state: a skip is `REJECTED` with a `decision:` reason.
On a terminal transition, `outcome` is set to the terminal state, `finished_at` to the
transition time, and `reject_reason` to the reason (for `ERROR`/`REJECTED`/`EXPIRED`).
The transition table is explicit. A transition that isn't in it raises `ValueError` (a
bug), which ends the cycle in `ERROR`.

| State | What happens | Exits |
|---|---|---|
| `NEW` | Row inserted at admission (§5). | → `ENRICHING` |
| `ENRICHING` | Build a frozen `CycleSnapshot` from the feed: last price, bid/ask, `atr14` on the signal timeframe via `compute_features` (the same source the Risk Engine uses for check 6), stale flag. | stale symbol, no ask, or ATR missing / non-finite / ≤ 0 → `EXPIRED` (reason names which); else → `ANALYZING` |
| `ANALYZING` | Phase 1: no analyst. The transition is recorded with details `{"skipped": "no analyst in phase 1"}` so Phase 2 keeps the same graph. | → `DECIDING` |
| `DECIDING` | `agents/stub.decide(...)`; the resulting intent fields are the transition details. | skip → `REJECTED` (`decision: skip (<why>)`); buy → `RISK_CHECK` |
| `RISK_CHECK` | First re-check signal freshness (§5): stale → `EXPIRED` (`signal stale`) without calling `check()`. Otherwise `RiskEngine.check(EntryIntent)`, called **exactly once**, never retried. | exception → `ERROR`; `approved=False` → `REJECTED` (first reason); approved → `SUBMITTED` |
| `SUBMITTED` | Mapped from `OrderResult.status` (below). | `filled` → `FILLED`; `rejected` / `not_submitted` → `REJECTED`; anything else → **stays `SUBMITTED`** |

**Status mapping after submit.** Only outcomes that prove the executed quantity are
mapped. `filled` means fully filled. `rejected` (the broker refused it) and
`not_submitted` (never sent) mean nothing was executed. `new`, `unknown` and `canceled`
stay `SUBMITTED`: an IOC that ends `canceled` may have filled partly first, and
`OrderResult` carries no filled quantity. WP 1.5 reconciliation completes these cycles
from the broker's order status. It moves a cycle to `EXPIRED` only when the broker
reports a final status with filled quantity 0. **Any filled quantity > 0, full or partial,
→ `FILLED`**, with `details` carrying `filled_qty`, `ordered_qty` and `partial`
(`filled_qty < ordered_qty`). `FILLED` therefore means "a position was acquired"; the
`positions` row carries the real quantity, and the Position Manager manages it. There is no
separate PARTIAL state: SPEC §4.3 lists none and nothing downstream branches on it.

Against the real `SimBroker`, every accepted order acks `new` (fills and IOC
cancellation happen on a later bar), so in WP 1.4 a SimBroker cycle ends in `SUBMITTED`.

### Timeouts

There is one timeout per step for `ENRICHING`, `ANALYZING` and `DECIDING` (constructor
argument, default 30 s per SPEC §4.3, injectable for tests). Expiry → `ERROR`
(`<state>: timeout`), never retried.

`RISK_CHECK` gets **no** per-step timeout while running. Cancelling `check()` mid-submit
would turn a real order into an `unknown` row. Shutdown is the only exception (§8).
`SUBMITTED` gets no timeout either, for the reason above.

## 5. Admission (G14) and freshness

**Freshness rule** (the same as the Scanner's obsolete-bar pre-filter, D13): a candidate
is stale when `now > bar_ts + 2 × period(timeframe)`, i.e. more than one signal period
after its bar ended. A fresh quote doesn't make an old queued signal fresh. The rule is
checked at admission and again on entering `RISK_CHECK`. In Phase 2 that is after the
agent calls, which is where queueing delay matters.

`CycleRunner.on_candidate(event: CandidateSignal)`:
1. If the runner is closing → drop (`shutting down`).
2. Stale (freshness rule) → drop (`signal stale`).
3. Ask `ControlGate.status()`. Entries not allowed, or the gate raises → drop (the gate
   fails closed).
4. Under the runner's admission lock:
   - re-check `closing` → drop (`shutting down`) and `admission_suspended` → drop
     (`admission suspended: commit uncertain`). Both flags are checked here, inside the
     lock, not only before it;
   - `bus.publish_tx(build, verify)` (§6), where `build(session)`:
     - counts the non-terminal cycles. If ≥ `scanner.max_concurrent_cycles` → returns
       `None` (drop, `max_concurrent_cycles`);
     - if a non-terminal cycle on the symbol exists → `None` (`open cycle on symbol`);
     - if an entry order in `pending`/`new`/`unknown` on the symbol exists → `None`
       (`open entry order on symbol`). The Risk Engine would reject it at check 5 anyway;
       dropping here avoids spending agent money first (Phase 2);
     - otherwise inserts the cycle (state `NEW`), flushes to obtain its id, and returns
       `CycleTransitioned(cycle_id=<new id>, None → NEW)`. The drop reason is recorded in
       a local variable that `build` closes over;
   - after `publish_tx` returns, check `closing` again. If it is set, don't start the task:
     the cycle stays `NEW` and the next `recover()` marks it `ERROR` (`interrupted`).
     Otherwise create the cycle task and add it to the task set, still under the lock.
5. Return.

A drop logs `cycle.dropped` with the symbol, rule and reason, and writes no row; the
`CandidateSignal` event already records the signal. If admission raises (for example the DB
is down), the candidate is dropped and logged. The handler never raises, so the bus worker
survives.

**Assumption:** one core process per journal database. The admission lock serialises
admission only within one process. PLAN 1.6 already requires one journal per account and
mode; this adds "and one core process".

## 6. Persistence of transitions (bus contract)

`EventBus.publish(event)` is unchanged: persist in its own transaction, then deliver.

`EventBus.publish_tx(build, verify, *, on_uncertain=None) -> DomainEvent | None` is the
transactional path:
- `build: Callable[[AsyncSession], Awaitable[DomainEvent | None]]` does its writes in
  the given session without committing (flushing is fine, e.g. to obtain an id). It
  returns the event to persist, or `None`, meaning "nothing to do": the transaction is
  rolled back and nothing is written or delivered.
- `verify: Callable[[AsyncSession, DomainEvent], Awaitable[bool]]` answers, in a fresh
  session, whether the event's transaction is visible. For cycle events this means "the
  cycle row exists in `to_state`" (the event row and the cycle row are committed
  together, so one implies the other).

**Commit-and-enqueue is owned by the bus, not by the caller.** `publish_tx` starts an
internal task that does the whole unit of work and awaits it through `asyncio.shield`.
Cancelling the caller therefore never interrupts it. The caller's own `CancelledError`
still propagates, and the unit finishes on its own. The internal task:
1. takes the bus lock (the same lock `publish` uses);
2. opens a session, awaits `build(session)`, appends the returned event
   (`repo.append_event(..., commit=False)`), and commits;
3. enqueues the event for subscribers, still under the lock.

If `build` raises, the transaction is rolled back and the exception propagates; nothing
was written.

**Uncertain commit.** A database can commit and then fail to acknowledge it (e.g. a
connection drop during `COMMIT` on Postgres). An exception raised by the commit itself is
therefore not proof that nothing persisted. On a commit exception the internal task,
still under the lock, calls `verify` once in a fresh session:
- `True` → it was committed: enqueue the event and return it as if nothing went wrong
  (logged `bus.commit_recovered`);
- `False` or `verify` raises → raise `CommitUncertain` (logged, chained to the original
  exception). **Before raising, the unit itself calls `on_uncertain(error)`** (a plain
  synchronous callback; the runner passes the method that sets `admission_suspended`).
  Suspension therefore never depends on the original caller still being there to catch
  the exception: a cancelled caller never sees it. A negative read-back does **not** prove a rollback: after a lost
  acknowledgement the original `COMMIT` may still be in flight and become visible later.
  The bus never claims an outcome it can't establish. The caller must assume either
  outcome (§9).

Every event is persisted and enqueued inside the one bus lock, so the global order of
event ids equals the delivery order. Delivery guarantee: a unit whose commit is
confirmed (directly, or by a positive read-back) enqueues its event exactly once, even if
its caller was cancelled after the commit. An uncertain unit (`CommitUncertain`) or one
abandoned at `close()` may be persisted **without** being delivered. The `events` table
stays the record, and `recover()` resolves the cycle on the next start. The internal tasks
are tracked by the bus; `close()` waits for them within its budget (§8).

**Lock order:** runner admission lock, then the bus lock; never the reverse. `build` and
`verify` only touch the session they are given. They never publish or await other locks.

Transitions use `publish_tx` with `build` doing the guarded update in
`repo.transition_cycle(session, cycle_id, from_, to, details, now)`:
`UPDATE … WHERE id = :id AND state = :from_`. Zero rows updated → `RuntimeError` (a lost
race or a bug), so no event is written. The update also sets `outcome`, `finished_at`
and `reject_reason` on terminal states. `build` returns the `CycleTransitioned` event.
Admission's `build` is described in §5.

`CycleTransitioned(DomainEvent)` fields: `cycle_id` (base field), `from_state`
(`str | None`), `to_state`, `details` (JSON-safe dict: Decimals as strings). The field is
not called `payload`, because `DomainEvent.payload()` is the method the bus serialises
with.

## 7. Stub Decision Maker (`agents/stub.py`)

These are module constants (owner decision, D15) and are not part of the profile, so the
profile hash is unchanged:

| Constant | Value | Use |
|---|---|---|
| `MIN_SCORE` | 0.5 | `candidate.score < MIN_SCORE` → skip (`score <x> < 0.5`) |
| `STOP_ATR` | 1.5 | `est_entry = ask × (1 + entry_slippage_bps / 10⁴)`; `stop = est_entry − 1.5 × atr14` |
| `TP_R` | 2 | `take_profit = est_entry + 2 × (est_entry − stop)` |
| `SIZE_FRACTION` | 1.0 | passed through; the engine sizes |
| confidence | `min(1, default_win_rate_prior + 0.10)` | the engine caps it at win rate + 10 pp anyway |

Inputs are guaranteed by ENRICHING: `ask > 0` and a finite `atr14 > 0`. If `stop ≤ 0` →
skip (`stop <x> ≤ 0`). That is an explicit outcome, not an incidental `EntryIntent`
validation error.

**Stop band guarantee.** The runner can't see the price increment: `AssetRules` come from
the broker, and only the engine holds it. The engine's entry is
`entry_limit(ask, slip, tick)`, which rounds `est_entry` **up** by less than one tick.
For the same ask, the stop distance the engine measures is therefore in
`[1.5 × ATR, 1.5 × ATR + tick)`. It lies within `[min_stop_atr, max_stop_atr] × ATR` when
`min_stop_atr ≤ 1.5` (always, with the current profile's 0.8) and
`tick < (max_stop_atr − 1.5) × ATR`. Otherwise check 6 rejects, which is correct. Quote or
bar movement between DECIDING and `check()` is not covered; the engine prices from its own
fresh quote (D14).

**Known consequence (fee vs. timeframe).** With Alpaca's ~25 bps taker fee on both legs,
check 11 needs roughly 0.5 R > 2 × fee, so R ≳ 1 % of price, i.e. a 5m ATR ≳ 0.67 %. BTC/ETH
rarely reach that, so most live-paper stub cycles will end `REJECTED: expectancy`. That is
the Risk Engine working as designed. Whether the strategy's timeframe or targets fit the
fee level is a strategy question, tracked in PLAN next to D10.

## 8. Orders, recovery and shutdown

**Orders are reconciled by order row, not by cycle state.** An order may exist at the
broker while its cycle is `ERROR`. Example: the broker accepts, then the engine's
acknowledgement write fails, so `check()` raises and the row stays `pending`. WP 1.5
reconciliation therefore scans every entry order in `pending`/`new`/`unknown` (PLAN 1.5
contract, by `client_order_id`), whatever its cycle's state. While unresolved, such an
order:
- keeps reserving its symbol and a position slot at the Risk Engine (check 5 counts open
  entry orders), and at admission (§5);
- does **not** hold a cycle slot once its cycle is terminal. The cycle cap limits work in
  flight (agent spend), not exposure.

**Recovery** (`CycleRunner.recover()`, called once before subscribing; it raises `RuntimeError` if admission has started or a cycle task exists, and fails closed: the first failed write aborts it and the caller must then not subscribe the runner):
- open cycles in `NEW`, `ENRICHING`, `ANALYZING`, `DECIDING` → `ERROR` (`interrupted`).
  Nothing was sent to the broker in those states;
- `RISK_CHECK` with no `orders` row for the cycle → `ERROR` (`interrupted`). `check()`
  inserts the order row before it submits, so no row means nothing was submitted;
- `RISK_CHECK` with an order row, and every `SUBMITTED` cycle → left open and logged
  (`cycle.awaiting_reconciliation`). WP 1.5 completes them from the order's broker status.

**Shutdown is best-effort bounded in process; the hard deadline belongs to the process.**
A coroutine blocked in a driver call (aiosqlite runs statements in a thread) cannot be
interrupted, and `Task.cancel()` only *requests* cancellation. So no in-process step can
promise to stop everything by a deadline. What each step promises:

`CycleRunner.close(budget_s) -> CloseReport` (default budget 20 s, injectable). The budget
bounds the **whole call**, measured from entry:
1. Set `closing` **without** taking the admission lock, so `close()` never waits behind a
   stalled admission. Admission checks the flag after taking its lock and again after
   `publish_tx` returns (§5). A cycle committed after the flag is set gets no task; it
   stays `NEW`, and the next `recover()` marks it `ERROR`.
2. Cancel every task that is not inside `check()`. Their cycles stay in their last
   committed state; the next `recover()` marks them `ERROR`.
3. **Graceful drain:** wait until `deadline − cancel_wait_s` (`cancel_wait_s` default 3 s)
   for tasks inside `check()`. The `check()` call runs under `asyncio.shield`, so step 2's
   cancel can't reach it. A task that finishes in time records its transition normally.
4. **Forced termination:** cancel the shielded `check()` calls that are still running and
   log `cycle.forced_cancel` per cycle.
5. Wait for all cancelled tasks until the deadline.
6. Any task still running is logged `cycle.task_abandoned` and returned in
   `CloseReport.abandoned`. The runner no longer owns or awaits it. It may still touch a
   session or publish; the bus refuses publishes once closed (`BusClosedError`, which the
   task logs). Process exit ends it.

Forcing and abandoning are safe because `check()` writes the `pending` order row before it
submits. An order that may have reached the broker is therefore durable, `recover()`
leaves its cycle open, and WP 1.5 reconciles it.

`EventBus.close(budget_s)` gets the same shape (it already exists; its drain is
unbounded today):
1. mark closed, without waiting for the bus lock longer than the budget allows; new
   publishes raise `BusClosedError`;
2. wait for in-flight `publish_tx` units and the subscriber backlog until the deadline;
3. cancel the workers and wait for them until the deadline;
4. log survivors as `bus.task_abandoned`.
An in-flight `publish_tx` unit that is abandoned may still commit. Its cycle and event
are then consistent in the database, and `recover()` handles the cycle on the next start.

**Shutdown order in `run core`** (wired in WP 1.5): stop the feed stream (no new bars);
`runner.close(budget)`, which drops further candidates immediately; then
`bus.close(budget)`. The runner is closed before the bus is drained, so draining the
scanner's backlog can no longer admit new cycles, and nothing waits on the bus before the
runner's budget starts.

**Process-level hard deadline** (WP 1.5, with the wiring): after the first SIGINT/SIGTERM
starts the shutdown above, a second signal, or a watchdog at `shutdown.timeout_s`
(> runner + bus budgets), logs `core.forced_exit` and calls `os._exit(1)`. Once the core
runs under a supervisor (container stop timeout, systemd `TimeoutStopSec`), that is the
outer bound. Until then the watchdog is. The spec promises bounded *in-process* steps and
a hard process deadline, not a clean in-process stop of every task.

## 9. Error handling

- An exception in any step → `ERROR` with `reject_reason = "<state>: <ExcType>"`, logged
  inside `bind_cycle(cycle_id)`. `CancelledError` is never treated as an error.
- **General rule:** if a transition write fails, the runner makes one attempt to write
  `ERROR` (`transition write failed`). If that fails too, it logs `cycle.stuck` and the
  row keeps its last committed state; `recover()` resolves it.
- **Exception: once `check()` has returned or raised, no `ERROR` write is attempted for a
  failed transition write.** The cycle keeps its last committed state (`RISK_CHECK`, or
  `SUBMITTED` when `RISK_CHECK → SUBMITTED` committed but `SUBMITTED → FILLED`/`REJECTED`
  failed) and logs `cycle.stuck`. The outcome is already in `risk_decisions`/`orders`, so
  the order row decides what happens. If `check()` itself raised, the transition to `ERROR` is the normal
  transition, not a fallback.
- **`CommitUncertain`** (§6) counts as a failed transition write. However, the runner
  doesn't know which state is committed. So it never attempts a fallback `ERROR` write
  (the guarded update would hit an unknown `from_`). It logs `cycle.stuck`
  (`commit uncertain`) and ends the task. The row is in either the old state or the new
  one, both consistent with its event. `recover()` resolves it on the next start.
  At admission, `CommitUncertain` → no task is started. If the insert did commit
  (possibly later), the cycle stays `NEW` until `recover()` marks it `ERROR`.
- **Any `CommitUncertain`, at admission or on a transition, suspends admission** for the
  rest of the process: the runner sets `admission_suspended`, logs
  `cycle.admission_suspended` once at error level, and drops every later candidate
  (`admission suspended: commit uncertain`). Reason: an uncertain admission insert may
  become visible only after the next admission has counted the open cycles, which would
  exceed `max_concurrent_cycles` (G14). Strictly, only admission inserts can make the count
  too low (a transition that lands late can only make it look higher), but suspending on
  both is simpler and still conservative. Cycles already running continue, so exits and
  orders are unaffected. The flag is set by the bus-owned unit through `on_uncertain`
  (§6), not by the caller's `except` clause. **Resolution is a restart:** `recover()`
  reads the final state through a fresh connection pool. On SQLite (this WP) a fresh
  process is enough. On Postgres a fresh pool does not prove that the old server-side
  `COMMIT` has finished. WP 1.6 must make startup recovery establish that the previous
  process's transactions have resolved (e.g. no backend of the old process left in
  `pg_stat_activity`, or terminating them first) before admission reopens. No in-process
  re-verification with timing heuristics: an outcome-unknown commit is rare, and failing
  closed on entries is the right trade.
- A task done-callback removes the task from the runner's set and logs anything that
  escaped. This is a backstop; step code already catches.

## 10. Testing

pytest + hypothesis on a temporary SQLite database. Clocks, timeouts and blocking points
are injected; no wall-clock asserts.

- **Admission/G14:**
  - concurrent candidates on two symbols with cap 1 → exactly one row and one
    `cycle.dropped`;
  - the same symbol twice → one admitted;
  - an open entry order on the symbol (cycle already `ERROR`) → dropped;
  - stale candidate → dropped;
  - kill/pause or a raising gate → dropped;
  - DB error at admission → dropped, and the handler doesn't raise;
  - the interleaving is driven explicitly (the first admission is held inside `build`
    by an event);
  - the admission event's `cycle_id` equals the inserted row's id; a drop (`build`
    returns `None`) writes neither a row nor an event.
- **FSM:**
  - **Real `RiskEngine` + `SimBroker`:** approved entry → `SUBMITTED`, with every
    transition in `events` (from/to/details, in order), `orders.cycle_id` set and status
    `new`;
  - stub skip → `REJECTED`; risk rejection → `REJECTED` (first reason);
  - stale symbol / no ask / ATR ≤ 0 → `EXPIRED`;
  - signal stale on entering `RISK_CHECK` (clock advanced while DECIDING blocks) →
    `EXPIRED`, and `check()` is not called.
- **`check()` contract (fake engine returning controlled `OrderResult`s):**
  - `filled` → `FILLED`; `rejected`/`not_submitted` → `REJECTED`;
  - `new`/`unknown`/`canceled` → stays `SUBMITTED`;
  - raises → `ERROR`, called exactly once;
  - step timeouts in ENRICHING/DECIDING → `ERROR`, not retried.
- **Broker accepts, then a DB failure:** real engine with a SimBroker whose ack write fails
  (failing session hook) → the engine raises → cycle `ERROR`, the order row stays
  `pending` with its `client_order_id`, and a following candidate on the symbol is dropped
  at admission.
- **Transition write failures:**
  - a failure before `check()` → `ERROR` via the fallback;
  - a failure after `check()` returned → stays `RISK_CHECK`, no `ERROR` write attempted,
    `cycle.stuck` logged;
  - `RISK_CHECK → SUBMITTED` commits, then `SUBMITTED → FILLED` fails (fake engine acks
    `filled`) → stays `SUBMITTED`, no `ERROR` write attempted, `cycle.stuck` logged;
  - `CommitUncertain` on a transition → no fallback write, `cycle.stuck`, the task ends,
    admission suspended; at admission → no task started, admission suspended;
  - **late-landing admission:** the commit raises, `verify` returns false, then the
    transaction becomes visible (the fake commits after an event is released) → the next
    candidate is dropped (`admission suspended`), and the open-cycle count never exceeds
    the cap; cycles already running still reach their terminal states.
- **Shutdown:**
  - `close()` while an admission is held inside `build` → `close()` does not wait for the
    admission lock; the admission commits, sees `closing`, starts no task; the cycle
    stays `NEW`, and `recover()` → `ERROR`. A candidate after the flag → dropped
    (`shutting down`);
  - `close()` during DECIDING → cancelled and awaited; `recover()` → `ERROR`;
  - `close()` while `check()` is blocked and releases before the drain deadline → result
    recorded;
  - `check()` blocked beyond the drain deadline → forced cancel, `cycle.forced_cancel`
    logged, the cycle stays `RISK_CHECK`, the order row stays `pending`; `recover()`
    leaves the cycle open;
  - a task that ignores cancellation (blocks on an event that swallows `CancelledError`
    until released) → `close()` returns by its deadline (injected clock) with the task in
    `CloseReport.abandoned` and `cycle.task_abandoned` logged.
- **Recovery:** seeded rows for every open state, both `RISK_CHECK` variants → the
  expected outcome.
- **Transitions:**
  - guarded update with the wrong `from_` → raises, no event row;
  - an illegal transition → `ERROR`;
  - terminal fields set.
- **Bus `publish_tx`:**
  - `build` raises → no event row, no cycle write, no delivery, exception propagates;
  - `build` returns `None` → rollback, nothing written or delivered;
  - commit → delivered once;
  - **ordering:** a `publish_tx` held inside `build` while a plain `publish` is attempted
    → the plain publish waits, and event ids and delivery order agree;
  - caller cancelled while `build` runs → the unit still completes (commit + enqueue)
    under the bus; the caller sees `CancelledError`;
  - **caller cancelled after the DB committed but before it got confirmation** (commit
    wrapped to succeed, then block on an event while the caller is cancelled) → the row
    and the event are persisted and the event is delivered exactly once;
  - commit raises but the data is committed (fake commit that commits, then raises) →
    `verify` true → delivered once, `bus.commit_recovered` logged;
  - commit raises and `verify` false → `CommitUncertain` (chained), nothing delivered;
  - commit raises and `verify` raises → `CommitUncertain`, nothing delivered;
  - `close(budget)` while a `publish_tx` is in flight → it finishes and delivers within
    the budget; a later publish → `BusClosedError`; a unit blocked past the budget →
    `close()` returns by its deadline, `bus.task_abandoned` logged;
  - `CycleTransitioned` round-trips through `payload()` (guards against field shadowing);
  - existing bus tests unchanged.
- **Stub:**
  - skip below `MIN_SCORE`; skip on `stop ≤ 0`;
  - BUY field math;
  - hypothesis property, using the real `entry_limit()`: for any `ask > 0`, `ATR > 0`,
    `tick > 0` with `tick < (max_stop_atr − 1.5) × ATR`, the distance
    `entry_limit(ask, slip, tick) − stop` lies in `[1.5 × ATR, 1.5 × ATR + tick)` and within
    `[min_stop_atr, max_stop_atr] × ATR`. A regression case covers ask `100.003`, ATR
    `0.067`, tick `0.01`.

Gates: mypy strict (covers `core/`), ruff. There is no coverage gate on `core/cycle.py`
(SPEC demands 100 % only for `risk/`), but the tests above are meant to cover every row of
the transition table.

## 11. Docs alignment (same PR)

- PLAN:
  - 1.4 row → Done; **D15**;
  - 1.5 row gains:
    - subscribe the Cycle Runner in `run core` after `recover()`, with the §8 shutdown
      order (runner closed before the bus is drained, budgets from config);
    - process-level hard deadline: second signal or `shutdown.timeout_s` watchdog →
      `os._exit(1)`;
    - complete `SUBMITTED` and `RISK_CHECK`-with-order cycles from the order's broker
      status via the reconciliation edges (`EXPIRED` only on a final status with
      filled qty 0; any fill, full or partial → `FILLED` + managed position);
    - reconcile open entry orders of any cycle state;
    - SimBroker next-bar fill/cancel integration tests;
  - 1.6 row gains "one core process per journal" and the Postgres recovery caveat (§9:
    establish that the old process's server-side transactions have resolved before
    reopening admission); fee-vs-ATR note next to D10.
- Scanner module docstring: G14 is now enforced by the Cycle Runner.
- `core/bus.py` docstring: `publish_tx`, bus-owned commit-and-enqueue, uncertain-commit
  read-back, ordering guarantee, bounded `close()`.
- SPEC §4.3: dated note:
  - no timeout on `RISK_CHECK`/`SUBMITTED` except forced shutdown;
  - any fill, full or partial → `FILLED`;
  - skip = `REJECTED`;
  - `canceled` stays `SUBMITTED` until reconciliation;
  - orders are reconciled by order row.
- CLAUDE.md: the events rule mentions `publish_tx(build, verify)` for writes that must be
  atomic with their event.
