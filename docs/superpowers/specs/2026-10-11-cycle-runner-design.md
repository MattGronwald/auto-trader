# Cycle Runner design (WP 1.4)

Date: 2026-10-11. Status: approved in conversation, pending written-spec review.
Sources: SPEC §0, §4.3, §4.4, §5, §7, §11; PLAN G14, D13, D14.
Decisions taken while designing are recorded as PLAN **D15**.

## 1. Purpose and success criteria

The Cycle Runner turns each `CandidateSignal` into at most one audited cycle and drives it
through a deterministic FSM to a terminal state. In Phase 1 a rule-based stub stands in
for the agents. It is the first caller of `RiskEngine.check()`.

Done means:
- `max_concurrent_cycles` and "one open cycle per symbol" hold exactly, even when
  candidates for several symbols arrive together (G14).
- Every state change is persisted with its timestamp and payload, atomically with the
  `cycles` row.
- `check()` is called at most once per cycle. Any exception from it ends the cycle in
  `ERROR`.
- A crash or shutdown never leaves an order that the journal doesn't know about, and
  never causes a step to be retried.

Out of scope:
- **Wiring into `run core`.** Entries are IOC limits without a bracket (D14), so stops
  and time exits exist only once the Position Manager lands. WP 1.5 subscribes the runner.
- Resolving `new`/`unknown` orders (WP 1.5/1.6 reconciliation).
- LLM agents, the cost meter and the budget check (Phase 2).

## 2. Approach

The `CandidateSignal` handler only **admits**. It checks the caps and inserts the cycle
row in one transaction, then starts one `asyncio.Task` per cycle that walks the FSM. The
bus worker stays fast, so a slow cycle (an LLM call in Phase 2) never delays admitting the
next candidate.

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
| `core/bus.py` | `publish(event, *, session=None)`. Without a session, behaviour is unchanged. With one, the event row is appended inside the caller's transaction and delivery waits for the caller to commit (see §6). |
| `journal/repo.py` | `admit_cycle`, `transition_cycle`, `open_cycles` (for recovery), `has_order_for_cycle`. |

No schema change and no migration. `cycles` already has `state`, `outcome`,
`reject_reason` and `finished_at`.

## 4. FSM

```
NEW → ENRICHING → ANALYZING → DECIDING → RISK_CHECK → SUBMITTED → FILLED
                                                                → REJECTED
                                                                → EXPIRED
any non-terminal state → ERROR; ENRICHING → EXPIRED; DECIDING → REJECTED; RISK_CHECK → REJECTED
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
| `ENRICHING` | Build a frozen `CycleSnapshot` from the feed: last price, bid/ask, `atr14` on the signal timeframe via `compute_features` (the same source the Risk Engine uses for check 6), stale flag. | stale symbol, no ask, or no finite ATR → `EXPIRED` (reason names which); else → `ANALYZING` |
| `ANALYZING` | Phase 1: no analyst. The transition is recorded with payload `{"skipped": "no analyst in phase 1"}` so Phase 2 keeps the same graph. | → `DECIDING` |
| `DECIDING` | `agents/stub.decide(...)`; the resulting intent fields are the transition payload. | skip → `REJECTED` (`decision: skip (score …)`); buy → `RISK_CHECK` |
| `RISK_CHECK` | `RiskEngine.check(EntryIntent)`, called **exactly once**, never retried. | exception → `ERROR`; `approved=False` → `REJECTED` (first reason); approved → `SUBMITTED` |
| `SUBMITTED` | Mapped from `OrderResult.status`. | `filled` → `FILLED`; `canceled` (IOC got nothing) → `EXPIRED`; `rejected` / `not_submitted` → `REJECTED`; `new` / `unknown` → **stays `SUBMITTED`** |

A cycle that stays `SUBMITTED` keeps blocking its symbol and a concurrency slot until WP
1.5/1.6 reconciliation resolves its order. That is deliberate (fail safe).

### Timeouts

There is one timeout per step for `ENRICHING`, `ANALYZING` and `DECIDING` (constructor
argument, default 30 s per SPEC §4.3, injectable for tests). Expiry → `ERROR`
(`<state>: timeout`), never retried.

`RISK_CHECK` gets **no** runner-side timeout. Cancelling `check()` mid-submit would turn a
real order into an `unknown` row, and the engine already bounds its network calls with
its `io_timeout_s` budget. `SUBMITTED` gets no timeout either, for the reason above.

## 5. Admission (G14)

`CycleRunner.on_candidate(event: CandidateSignal)`:
1. Ask `ControlGate.status()`. Entries not allowed, or the gate raises → drop (the gate
   fails closed).
2. Under the runner's `asyncio.Lock`, in one session/transaction:
   - count the non-terminal cycles. If ≥ `scanner.max_concurrent_cycles` → drop
     (`max_concurrent_cycles`);
   - if a non-terminal cycle on the symbol exists → drop (`open cycle on symbol`);
   - otherwise insert the cycle (`strategy_id`, `symbol`, `rule_id`,
     `signal_features = event.features`, state `NEW`) and append
     `CycleTransitioned(None → NEW)` in the same transaction; commit; deliver.
3. Start the cycle task and return.

A drop logs `cycle.dropped` with the symbol, rule and reason, and writes no row; the
`CandidateSignal` event already records the signal. If admission raises (for example the DB
is down), the candidate is dropped and logged. The handler never raises, so the bus worker
survives.

**Assumption:** one core process per journal database. The runner lock serialises
admission only within one process. PLAN 1.6 already requires one journal per account and
mode; this adds "and one core process".

## 6. Persistence of transitions

`repo.transition_cycle(session, cycle_id, from_, to, payload, now)` updates the row with a
guard (`WHERE id = :id AND state = :from_`; zero rows updated → `RuntimeError`, which
catches a lost race or a bug). It also sets `outcome`, `finished_at` and `reject_reason` on
terminal states.

The runner appends the `CycleTransitioned` event in the same session through
`bus.publish(event, session=session)`, then commits. So `cycles.state` and the `events`
audit trail cannot disagree.

Bus semantics with `session=`:
- `publish` appends the event row in the caller's session and does not commit.
- Delivery is queued on a per-session pending list and runs only after the caller calls
  `bus.deliver_pending(session)` after its commit. On rollback the caller discards it
  (`bus.discard_pending(session)`), and nothing is delivered.
- The existing path (`session=None`) keeps persist-then-deliver in its own transaction.

`CycleTransitioned(DomainEvent)` fields: `cycle_id` (base field), `from_state`
(`str | None`), `to_state`, `payload` (JSON-safe dict: Decimals as strings).

## 7. Stub Decision Maker (`agents/stub.py`)

These are module constants (owner decision, D15) and are not part of the profile, so the
profile hash is unchanged:

| Constant | Value | Use |
|---|---|---|
| `MIN_SCORE` | 0.5 | `candidate.score < MIN_SCORE` → skip |
| `STOP_ATR` | 1.5 | `stop = ask − 1.5 × atr14` (mid-band in `[min_stop_atr 0.8, max_stop_atr 3.0]`, so small quote or ATR drift before `check()` doesn't flip check 6) |
| `TP_R` | 2 | `take_profit = ask + 2 × (ask − stop)` |
| `SIZE_FRACTION` | 1.0 | passed through; the engine sizes |
| confidence | `min(1, default_win_rate_prior + 0.10)` | the engine caps it at win rate + 10 pp anyway |

The stub only proposes. The Risk Engine re-reads the quote and prices the entry itself
(D14).

**Known consequence (fee vs. timeframe).** With Alpaca's ~25 bps taker fee on both legs,
check 11 needs roughly 0.5 R > 2 × fee, so R ≳ 1 % of price, i.e. a 5m ATR ≳ 0.67 %. BTC/ETH
rarely reach that, so most live-paper stub cycles will end `REJECTED: expectancy`. That is
the Risk Engine working as designed. End-to-end flow is proven on SimBroker (here and in
WP 1.8). Whether the strategy's timeframe or targets fit the fee level is a strategy
question, tracked in PLAN next to D10.

## 8. Recovery and shutdown

**Recovery** (`CycleRunner.recover()`, called once before subscribing):
- open cycles in `NEW`, `ENRICHING`, `ANALYZING`, `DECIDING` → `ERROR` (`interrupted`).
  Nothing was sent to the broker in those states;
- `RISK_CHECK` with no `orders` row for the cycle → `ERROR` (`interrupted`). `check()`
  inserts the order row before it submits, so no row means nothing was submitted;
- `RISK_CHECK` with an order row, and every `SUBMITTED` cycle → left open and logged
  (`cycle.awaiting_reconciliation`); WP 1.5 resolves them.

**Shutdown** (`CycleRunner.close()`): cancel all cycle tasks and await them. A task in
`ENRICHING`…`DECIDING` is cancelled (the next `recover()` marks it `ERROR`). The
`check()` call runs under `asyncio.shield` and the task awaits the shielded future even
when cancelled, then records the result before it exits. A SIGINT can therefore never turn
a real order into an unrecorded one, and the engine's io budget bounds the wait.

## 9. Error handling

- An exception in any step → `ERROR` with `reject_reason = "<state>: <ExcType>"`, logged
  inside `bind_cycle(cycle_id)`. `CancelledError` is never treated as an error.
- If a transition write fails, the runner makes one attempt to write `ERROR`
  (`transition write failed`). If that fails too, it logs `cycle.stuck` and the row keeps
  its last committed state; `recover()` and WP 1.5 resolve it.
- If the write after `check()` returns fails, the outcome is already in
  `risk_decisions`/`orders`, and the cycle stays `RISK_CHECK` with an order row (the 1.5
  reconciliation case). It is never retried.
- A task done-callback removes the task from the runner's set and logs anything that
  escaped. This is a backstop; step code already catches.

## 10. Testing

pytest + hypothesis on a temporary SQLite database. Clocks, timeouts and blocking points
are injected; no wall-clock asserts.

- **Admission/G14:**
  - concurrent candidates on two symbols with cap 1 → exactly one row and one
    `cycle.dropped`;
  - the same symbol twice → one admitted;
  - kill/pause or a raising gate → dropped;
  - DB error at admission → dropped, and the handler doesn't raise;
  - the interleaving is driven explicitly (the first admission is held inside its
    transaction by an event).
- **FSM on SimBroker + real `RiskEngine`:**
  - happy path → `FILLED`, with every transition in `events` (from/to/payload) and
    `orders.cycle_id` set;
  - stub skip → `REJECTED`;
  - risk rejection → `REJECTED` (first reason);
  - IOC with no fill → `EXPIRED`;
  - stale symbol / no ask / no ATR → `EXPIRED`.
- **`check()` contract (fake engine):**
  - raises → `ERROR`, called exactly once;
  - `new`/`unknown` → stays `SUBMITTED`;
  - `rejected`/`not_submitted` → `REJECTED`;
  - step timeouts in ENRICHING/DECIDING → `ERROR`, not retried.
- **Shutdown:**
  - `close()` during DECIDING → cancelled; `recover()` → `ERROR`;
  - `close()` while `check()` is blocked → `check()` completes and its result is recorded.
- **Recovery:** seeded rows for every open state, both `RISK_CHECK` variants → the
  expected outcome.
- **Transitions:**
  - guarded update with the wrong `from_` → raises;
  - an illegal transition → `ERROR`;
  - terminal fields set.
- **Bus `publish(session=)`:**
  - rollback → no row, no delivery;
  - commit + `deliver_pending` → delivered once;
  - existing bus tests unchanged.
- **Stub:**
  - skip below `MIN_SCORE`;
  - BUY field math;
  - hypothesis property: for any positive ask and ATR with
    `ask × entry_slippage_bps / 10⁴ ≤ (max_stop_atr − STOP_ATR) × ATR`, the stop distance
    measured from the engine's entry price lies within `[min_stop_atr, max_stop_atr] × ATR`.
    Below that ATR (≈ 6.7 bps of price with the current profile), check 6 rejects. That is
    correct: the slippage alone would exceed the allowed stop band.

Gates: mypy strict (covers `core/`), ruff. There is no coverage gate on `core/cycle.py`
(SPEC demands 100 % only for `risk/`), but the tests above are meant to cover every row of
the transition table.

## 11. Docs alignment (same PR)

- PLAN: 1.4 row → Done; **D15**; 1.5 row gains "subscribe the Cycle Runner in `run core`
  (after `recover()`)" and "reconcile `RISK_CHECK`-with-order and `SUBMITTED` cycles";
  1.6 row gains "one core process per journal"; fee-vs-ATR note next to D10.
- Scanner module docstring: G14 is now enforced by the Cycle Runner.
- SPEC §4.3: dated note (no timeout on `RISK_CHECK`/`SUBMITTED`; skip = `REJECTED`; shielded
  `check()` on shutdown).
- CLAUDE.md: the events rule mentions `publish(event, session=...)` for atomic writes.
