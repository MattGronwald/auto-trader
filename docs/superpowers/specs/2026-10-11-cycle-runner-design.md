# Cycle Runner design (WP 1.4)

Date: 2026-10-11. Status: approved in conversation, pending written-spec review.
Revised 2026-10-11 after the first written-spec review: orders after an `ERROR` cycle,
the bus contract, bounded shutdown, SimBroker test scope, signal freshness, stub rounding,
and `canceled` ≠ nothing filled.
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
| `core/bus.py` | `publish(event, *, with_tx=None)` (§6). |
| `journal/repo.py` | `append_event(..., commit=True)` gains a no-commit mode for `with_tx`; `insert_cycle`, `transition_cycle`, `count_open_cycles`, `open_cycles` (for recovery), `has_order_for_cycle`, `open_entry_symbols`. |

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
RISK_CHECK → EXPIRED           (signal stale on entry, §5)
RISK_CHECK → REJECTED          (risk rejection)
any non-terminal state → ERROR
```

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
reports filled quantity 0, and treats any filled quantity > 0 as a position to manage.

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
   - re-check `closing` (it is set under this lock, §8);
   - in the bus transaction (§6):
     - count the non-terminal cycles. If ≥ `scanner.max_concurrent_cycles` → drop
       (`max_concurrent_cycles`);
     - if a non-terminal cycle on the symbol exists → drop (`open cycle on symbol`);
     - if an entry order in `pending`/`new`/`unknown` on the symbol exists → drop
       (`open entry order on symbol`). The Risk Engine would reject it at check 5 anyway;
       dropping here avoids spending agent money first (Phase 2);
     - otherwise insert the cycle (state `NEW`) and publish `CycleTransitioned(None → NEW)`;
   - register the cycle task in the runner's task set **before releasing the lock**.
5. Return.

A drop logs `cycle.dropped` with the symbol, rule and reason, and writes no row; the
`CandidateSignal` event already records the signal. If admission raises (for example the DB
is down), the candidate is dropped and logged. The handler never raises, so the bus worker
survives.

**Assumption:** one core process per journal database. The admission lock serialises
admission only within one process. PLAN 1.6 already requires one journal per account and
mode; this adds "and one core process".

## 6. Persistence of transitions (bus contract)

`EventBus.publish(event, *, with_tx=None)`:
- Without `with_tx`, behaviour is unchanged (persist, then deliver).
- With `with_tx: Callable[[AsyncSession], Awaitable[None]]`, the bus opens its session
  **under its own lock** and does three things:
  1. awaits `with_tx(session)`, which does its writes without committing;
  2. appends the event row (`repo.append_event(..., commit=False)`), then commits once;
  3. enqueues the event for subscribers, still under the lock.
- If `with_tx` raises, or the commit fails, the transaction rolls back and nothing is
  delivered; the exception propagates to the caller.
- Cancellation inside `with_tx` or the commit also rolls back, because the session context
  exits without a commit.

So every published event (with or without `with_tx`) is persisted and enqueued inside the
one bus lock. The global order of event ids equals the delivery order, and no delivery is
pending outside the lock that `close()` could lose. `close()` takes the same lock, so it
waits for an in-flight `with_tx` publish to finish (commit and enqueue, or rollback).

**Lock order:** runner admission lock, then the bus lock; never the reverse.
`with_tx` callbacks only touch the given session. They never publish or await other
locks.

Transitions use `with_tx` for the guarded update in
`repo.transition_cycle(session, cycle_id, from_, to, details, now)`:
`UPDATE … WHERE id = :id AND state = :from_`. Zero rows updated → `RuntimeError` (a lost
race or a bug), so the event is not written. The update also sets `outcome`, `finished_at`
and `reject_reason` on terminal states. Admission's count + insert also runs inside
`with_tx`; a drop is signalled by a sentinel exception that the runner catches and logs.

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

**Recovery** (`CycleRunner.recover()`, called once before subscribing):
- open cycles in `NEW`, `ENRICHING`, `ANALYZING`, `DECIDING` → `ERROR` (`interrupted`).
  Nothing was sent to the broker in those states;
- `RISK_CHECK` with no `orders` row for the cycle → `ERROR` (`interrupted`). `check()`
  inserts the order row before it submits, so no row means nothing was submitted;
- `RISK_CHECK` with an order row, and every `SUBMITTED` cycle → left open and logged
  (`cycle.awaiting_reconciliation`). WP 1.5 completes them from the order's broker status.

**Shutdown** (`CycleRunner.close(grace_s)`), in order:
1. Take the admission lock and set `closing`. An admission already inside the lock
   finishes, including registering its task, before the flag is set. Every later
   candidate is dropped (`shutting down`).
2. Cancel every task that is not inside `check()`. Their cycles stay in their last
   committed state; the next `recover()` marks them `ERROR`.
3. **Graceful drain:** wait up to `grace_s` (default 15 s, injectable) for tasks inside
   `check()`. The `check()` call is wrapped in `asyncio.shield`, so step 2's cancel can't
   reach it. A task that finishes in time records its transition normally.
4. **Forced termination:** cancel the shielded `check()` calls that are still running,
   log `cycle.forced_cancel` per cycle, and return. The engine's io budget does not bound
   its lock wait or its journal writes, so without this step shutdown would have no upper
   bound. Forcing is safe because `check()` writes the `pending` order row before
   submitting. An order that may have reached the broker is therefore durable, `recover()`
   leaves its cycle open, and WP 1.5 reconciles it.

**Shutdown order in `run core`** (wired in WP 1.5): stop the feed stream (no new bars),
drain the bus subscribers (scanner, runner admission), `runner.close(grace_s)`, then
`bus.close()`.

## 9. Error handling

- An exception in any step → `ERROR` with `reject_reason = "<state>: <ExcType>"`, logged
  inside `bind_cycle(cycle_id)`. `CancelledError` is never treated as an error.
- **General rule:** if a transition write fails, the runner makes one attempt to write
  `ERROR` (`transition write failed`). If that fails too, it logs `cycle.stuck` and the
  row keeps its last committed state; `recover()` resolves it.
- **Exception: once `check()` has returned or raised, no `ERROR` write is attempted for a
  failed transition write.** The cycle stays `RISK_CHECK` and logs `cycle.stuck`. The
  outcome is already in `risk_decisions`/`orders`, so the order row decides what
  happens. If `check()` itself raised, the transition to `ERROR` is the normal
  transition, not a fallback.
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
  - the interleaving is driven explicitly (the first admission is held inside `with_tx`
    by an event).
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
    `cycle.stuck` logged.
- **Shutdown:**
  - `close()` while an admission is held inside `with_tx` → that cycle is registered and
    then cancelled or drained; a candidate after the flag → dropped (`shutting down`);
  - `close()` during DECIDING → cancelled; `recover()` → `ERROR`;
  - `close()` while `check()` is blocked and releases within the grace period → result
    recorded;
  - `check()` blocked beyond the grace period → forced cancel, `cycle.forced_cancel`
    logged, the cycle stays `RISK_CHECK`, the order row stays `pending`; `recover()`
    leaves the cycle open.
- **Recovery:** seeded rows for every open state, both `RISK_CHECK` variants → the
  expected outcome.
- **Transitions:**
  - guarded update with the wrong `from_` → raises, no event row;
  - an illegal transition → `ERROR`;
  - terminal fields set.
- **Bus `with_tx`:**
  - `with_tx` raises → no event row, no cycle write, no delivery;
  - commit → delivered once;
  - **ordering:** a `with_tx` publish held inside its callback while a plain `publish` is
    attempted → the plain publish waits, and event ids and delivery order agree;
  - cancellation inside `with_tx` → rollback, no delivery;
  - `close()` while a `with_tx` publish is in flight → `close()` waits for it, which then
    commits and delivers; a later publish → `BusClosedError`;
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
      order;
    - complete `SUBMITTED` and `RISK_CHECK`-with-order cycles from the order's broker
      status (`EXPIRED` only on filled qty 0; any fill → managed position);
    - reconcile open entry orders of any cycle state;
    - SimBroker next-bar fill/cancel integration tests;
  - 1.6 row gains "one core process per journal"; fee-vs-ATR note next to D10.
- Scanner module docstring: G14 is now enforced by the Cycle Runner.
- `core/bus.py` docstring: `with_tx` and the ordering guarantee.
- SPEC §4.3: dated note:
  - no timeout on `RISK_CHECK`/`SUBMITTED` except bounded forced shutdown;
  - skip = `REJECTED`;
  - `canceled` stays `SUBMITTED` until reconciliation;
  - orders are reconciled by order row.
- CLAUDE.md: the events rule mentions `publish(event, with_tx=...)` for atomic writes.
