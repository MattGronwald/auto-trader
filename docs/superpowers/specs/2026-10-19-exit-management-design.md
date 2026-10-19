# Exit management design (WP 1.5c)

Date: 2026-10-19. Status: design approved in chat by the owner (decisions 1–12, all as recommended; 10–12 after the builder check). This written spec is in review.

## 1. Scope

WP 1.5c delivers PLAN §3.2 items 2 (exit contract) and 5 (retry and kill/flatten). It also takes the parts of SPEC §4.7 and §0.6 that 1.5a and 1.5b left open:

- a deterministic Position Manager (`core/position_manager.py`) for stop, take-profit, time, trailing and stale exits;
- durable exit state on the position: the original stop, TP, initial risk, the hold deadline, the trailing high-water mark and the trailing stop;
- retries for exits that never reached the broker or that the broker refused;
- handling for a kill: cancel outstanding entry orders, then flatten if `kill.flatten` is set;
- `autotrader flatten` and flatten on graceful stop (`shutdown.flatten`, D7);
- per-symbol failure isolation in `Ledger.apply_all` (1.5b final-review follow-up a).

It also wires two loops into `run core` (decision 8): an order-sync loop that books fills, and the Position Manager loop.

Out of scope, staying in 1.5d:
- startup reconciliation before admission (§3.2 item 1);
- the bounded resolver for `pending`/`unknown` orders without a broker id, and for orders the broker no longer knows (§3.2 item 4);
- the cash check in `run core`;
- Cycle Runner wiring and admission;
- the deferred 1.5a items.

Out of scope, staying in 1.6: Alpaca order methods. In 1.5c the new loops run against Alpaca, but its order calls are still stubs, and nothing places entries until 1.5d/1.6. All end-to-end behaviour is proven on SimBroker.

Optional LLM exit review (`exit_review`, off in the PoC) is not part of this WP.

## 2. Owner decisions (2026-10-19, recorded as D18)

1. **Trigger price:** the current **bid** from the feed's latest quote. A market sell receives the bid, not the last trade.
2. **No retroactive bar triggers.** Live triggers use the current bid only. A stop that a 1m bar dipped through, with the price since recovered, does not fire.

   For bar-based evaluation (SimBroker replay tests, WP 1.7 backtests), a bar whose low reaches the stop and whose high reaches the TP counts as a **stop first**, the conservative reading.
3. **Trailing:**
   - **Activation:** once the bid reaches `entry_price + trigger_r × R`, where `R = entry_price − original stop` (per unit; D16, R stays the price distance).
   - **High-water mark:** the highest bid since activation.
   - **Trailing stop:** `high-water − trail_atr × ATR`, using the signal-timeframe ATR14 **frozen at entry**.
   - **Bounds:** the trailing stop never moves down and never sits below the original stop.
4. **Time exit:** first fill time (`positions.opened_at`) + `risk.max_hold_min`. The agent-supplied `time_limit_min` waits for Phase 2.
5. **Kill without flatten** (the paper default): outstanding entry orders are cancelled, and open positions keep their normal stop/TP/time/trailing management.
6. **Flatten also sets the manual pause** (`paused`), so the next scanner signal cannot re-enter right after.
7. **Exit retries never give up.** A failed exit is retried with backoff of 1, 2, 4, … s, capped at 30 s, with an alert after 3 consecutive failures.
8. **1.5c wires into `run core`:** an order-sync loop that books fills every 2 s, and the Position Manager loop every 1 s. 1.5d keeps startup reconciliation, the resolver and Cycle Runner wiring.
9. **`shutdown.flatten`** (D7) is implemented in 1.5c through the same flatten path.

Added 2026-10-19 after the builder check (owner, as recommended):

10. **A broker that can't place orders.** Alpaca's order methods stay stubs until 1.6, and today their `BrokerError` makes an exit `unknown`.
    - The stubs raise a new `OrderMethodsUnavailable(BrokerError)`.
    - `RiskEngine._submit` records that as `not_submitted`: nothing left the process.
    - The Position Manager asks the engine once whether the broker can place orders (`RiskEngine.can_place_orders()`, backed by a new `Broker.can_place_orders()`; SimBroker true, Alpaca false until 1.6). When it can't:
      - it attempts no exits and no cancels;
      - it logs `positions.broker_cannot_trade` once;
      - triggers are still evaluated and latched, so the logs show what it would have done;
      - flatten reports `pending` with `broker cannot place orders`.
11. **Unknown exits until 1.5d.** An exit row left `pending`/`unknown` without a broker id is not retried.
    - `positions.exit_unresolved` repeats every 60 s while the row is unresolved.
    - The row is listed as unresolved in `flatten_result`.
    - A manual recovery procedure is documented: check the order at the broker, then record the outcome on the row (operator runbook in `docs/`).
    - The gap is shipped as a documented known limitation until the 1.5d resolver.
12. **Feed outage.** D6 stands for 1.5c: with no trusted bid, only time and (when enabled) stale exits fire. The PLAN 1.6 row gets a note: before live, decide whether held symbols need a REST quote fallback.

## 3. Data model (migration 0005)

New `positions` columns, all nullable:

| Column | Type | Meaning |
|---|---|---|
| `atr_at_entry` | `Money` | Signal-timeframe ATR14 from the approved entry decision (decision 3). NULL means no trailing for this position. |
| `trail_high` | `Money` | Highest bid since trailing activated. NULL means not activated. |
| `trail_stop` | `Money` | Current trailing stop. Only rises; never below `stop`. |
| `exit_trigger` | `String(16)` | Latched exit reason (an `ExitReason` value). NULL means no trigger has fired. |
| `exit_triggered_at` | `UTCDateTime` | When the trigger latched. |

`positions.stop`, `tp`, `initial_risk` and `opened_at` already exist (1.5b). `time_limit_at` already exists: the Position Manager writes `opened_at + max_hold_min` the first time it sees a position whose value is NULL. Restarts therefore never move the deadline.

**Entry decision:** check 7 adds `"atr": str(atr)` to the approved entry decision's `adjusted`, next to `initial_risk`. The ledger copies it into `positions.atr_at_entry` when it opens the position, the same way it copies `initial_risk` today (`_initial_risk` becomes a reader for both keys).

**Upgrade:** the migration backfills nothing. Positions opened before 0005 have `atr_at_entry` NULL, so they get no trailing.

## 4. Components

### 4.1 Ledger: per-symbol failure isolation (follow-up a)

`Ledger.apply_all(result)` today aborts the whole pass when one row raises a non-anomaly exception (`IntegrityError`, `LookupError`, `CommitUncertain`). After this change:

- The exception is caught per row and logged as `ledger.row_failed` (order row, symbol, error).
- The row's symbol is held back for the rest of the pass, using the same mechanism as a deferred row.
- Other symbols continue.
- `apply_all` returns the set of symbols that failed (`frozenset[str]`).
- `CancelledError` is never caught.

`RiskEngine.check()` treats a non-empty result as an incomplete sync: it raises `SyncIncomplete` and rejects with `state unavailable`. That is the same fail-closed outcome as today, now for every symbol rather than only the failed one, because a booking failure anywhere puts the journal in doubt for exposure.

### 4.2 Order-sync loop (`ledger.sync`, every 2 s)

A supervised loop in `run core` runs `risk.sync_orders()` then `ledger.apply_all(result)`, and logs failures.

- It is a separate loop from the Position Manager, so a hung broker read (bounded by `io_timeout_s`) never delays exit triggers on positions that are already booked.
- `sync_orders` already takes no lock, so it never delays an exit.
- Runs that overlap a `check()` sync are safe: the ledger is idempotent and serialised by the bus lock.
- When there are no unsettled order rows, `sync_orders` makes no broker call (it reads the journal and returns).

### 4.3 Position Manager (`core/position_manager.py`)

`PositionManager(risk, market, sessions, bus, controls, *, profile, config, clock)`.

**Dependencies:**
- `risk` is the `RiskEngine` and is used only for `exit()` plus the two new engine methods in 4.5. The Position Manager never holds a broker.
- `market` is the feed view: `quote()`, `is_stale()`, and the new `price_age_s()` from 4.6.
- `controls` gives the latest `ControlStatus` (`ControlPoller.latest`) and reads the flatten request (4.7).

**`step()`, run by the supervised `positions.manage` loop every 1 s:**

1. **Control:**
   - **Kill handling:** on the kill edge, and while killed, cancel open entries (4.4). With `kill_flatten`, every open position exits with reason `kill`, including a late entry fill.
   - **Flatten request:** if one is pending, run one flatten pass (4.7).
2. **Positions:** read the open journal positions (`closed_at IS NULL`), then process each one independently. A failure on one position is logged as `positions.step_failed` and does not stop the others.
   - **Bookkeeping:** write `time_limit_at` if it is NULL. If the position has no `stop`, log `positions.unprotected` (warning, once per position per process). Time and stale exits still apply to it.
   - **Exit pending:** if `exit_trigger` is set, go to the exit path (4.4).
   - **Otherwise:** evaluate the triggers (4.3.1). If one fires, latch it and go to the exit path.
3. **Stale-held alert:** a held symbol that is stale past `feed.stale_alert_s` logs `positions.stale_held` once per stale episode.

#### 4.3.1 Trigger evaluation (pure function)

`evaluate(state, bid, price_age_s, now, cfg) -> (ExitReason | None, TrailUpdate | None)`.

- **No trusted price:** there is no usable bid when the quote is missing, `is_stale(symbol)` is true, or the quote itself is older than `feed.stale_alert_s` (by `Quote.ts`). The feed counts fresh bars as observed, so a symbol can read as not stale while its quote is old. Then only time and stale can fire. *(Builder's call after the builder check.)*
- **Effective stop:** `max(stop, trail_stop)`, ignoring NULLs.
- **Fire conditions:**
  - **stop / trailing:** `bid <= effective stop`. The reason is `trailing` when `trail_stop` is set and `trail_stop > stop`, otherwise `stop`.
  - **take_profit:** `tp` is set and `bid >= tp`.
  - **time:** `now >= time_limit_at`. This needs no price.
  - **stale:** `stale_exit_enabled` and `price_age_s > feed.stale_exit_s`. The price age is measured from the newest valid quote or bar, the same clock the feed uses for `is_stale`. A symbol the feed has never priced since start counts as infinitely old only after `stale_exit_s` has passed since the Position Manager started, so a cold start doesn't immediately stale-exit.
- **Precedence** when several fire in one evaluation: stop > trailing > take_profit > time > stale. It only changes the recorded reason; the action is the same full close.
- **Trailing update:** applies only when `risk.trailing.enabled`, `atr_at_entry` is set and `R > 0`.
  - Activation (or continuation) needs `bid >= entry_price + trigger_r × R`, or `trail_high` already set.
  - The new high-water is `max(trail_high, bid)`.
  - The new trailing stop is `max(trail_stop, stop, new_high − trail_atr × atr_at_entry)`.
  - The update is returned only when a value rises, and is persisted with a guarded `UPDATE … WHERE id = ? AND closed_at IS NULL`, touching only these two columns.
  - The trailing stop is evaluated against the bid before the update, so a single tick never both raises the stop and triggers it on the same price.
- **Values:** prices are compared as `Decimal`. The quote's float bid is converted with `Decimal(str(bid))`, as the engine already does.

**Same-bar rule for bar-based callers** (tests, WP 1.7): `evaluate_bar(state, bar)` treats `low <= effective stop` as a stop before `high >= tp`, so stop wins. The live loop never calls it.

#### 4.3.2 Latching a trigger

- **The latch write:** in one `publish_tx` unit, set `exit_trigger` and `exit_triggered_at` and publish `ExitTriggered(position_id, symbol, reason, bid, ts)`. The update is guarded by `exit_trigger IS NULL AND closed_at IS NULL`.
- **Once latched:** a trigger is never re-evaluated, even if the price recovers. The exit keeps being retried until the position closes, and a restart honours it.
- **Commit verification:** uses the event's own unit id, the 1.5b `_verify` pattern.

### 4.4 Exit path, retries and in-flight exits

For a position whose trigger is latched, the Position Manager looks at the symbol's sell orders in the journal and acts on their state:

| Latest unsettled or recent sell order for the symbol | Action |
|---|---|
| None, or the last one is final with the position still open (e.g. a late entry fill grew it) | Attempt an exit if the backoff allows. |
| `not_submitted` or `rejected` (proven not working) | Count a failure and retry after backoff. |
| `pending` / `unknown` **without** a broker id | Do not retry; another close might still land (§3.2 items 4–5). Log `positions.exit_unresolved` (error) every 60 s while it stays unresolved (decision 11); this waits for the 1.5d resolver or the manual procedure. |
| Has a broker id and is not final | In flight; wait for the sync loop. If it has been in flight for more than 60 s, log `positions.exit_stuck` (error), then every 60 s. No cancel-and-resubmit in 1.5c. *(Builder's call.)* |
| Has a broker id and is final (`filled`/`canceled`), with the order row still unsettled | Wait for the ledger to settle it. |

**Attempting an exit:** call `risk.exit(ExitRequest(symbol, reason=exit_trigger, cycle_id=position.cycle_id))`. The engine re-reads the broker position on every attempt. Outcomes:

- **Order submitted** (any non-error status with a broker id): success. Reset the backoff.
- **`not_submitted` / `rejected`:** failure.
- **Dust** (`OrderResult.dust`): the broker holds only dust. Log `positions.exit_dust` and don't retry. The ledger closes the position as dust once its orders settle (1.5b), or it stays open with an unsettled entry (the 1.5c follow-up: unexitable, no retry storm).

  Today `check_exit` rejects a dust-only holding as `no position to exit`, and `OrderResult.dust` is set only when `close_position` raises `DustRemainder`. 1.5c therefore makes `check_exit` return a typed dust rejection (reason `dust only in <SYM>`), and `exit()` sets `OrderResult.dust` for it. This is a `risk/` change and keeps 100 % branch coverage. *(Builder's call.)*
- **Rejected with "no position to exit"** (the broker holds nothing): a failure with backoff. Typical cause: the booking lags the broker (an exit fill not yet synced). If it persists, the journal and broker disagree; the `positions.exit_failing` alerts are the escalation, and 1.5d reconciliation resolves it. Each attempt persists an exit decision (at most one per 30 s per position at the cap); that volume is accepted.
- **`unknown`:** handled by the table above on the next step.

**Backoff:** in memory, per position: 1, 2, 4, 8, 16, then 30 s for every later attempt. It resets on success and on restart; a restart re-attempts at once, as D7 requires (enforce overdue exits immediately). After 3 consecutive failures, log `positions.exit_failing` (error) with the reasons, then again every 10th failure. Every attempt is audited anyway as a persisted exit decision.

**Kill handling (decision 5):**
- The Position Manager reads `controls.latest`. When it is `killed`, including on the first step after startup with a kill already set, it calls `risk.cancel_open_entries()` (4.5) for every unsettled buy order with a broker id that it has not yet asked to cancel. This runs again on later steps for entries that appear afterwards, such as an in-flight submit acked after the kill.
- Buy orders without a broker id (`pending`/`unknown`) can't be cancelled. They are logged `positions.entry_unresolved` and left for 1.5d.
- If `kill_flatten` is set, every open position with no latched trigger is latched with `kill`, and every broker-held non-dust position without a journal position is exited with reason `kill`.
- Without `kill_flatten`, management continues normally.
- Clearing the kill stops the cancel/flatten actions. Triggers already latched stay latched.

### 4.5 Engine additions (the broker stays in `risk/engine.py`)

- **`cancel_open_entries() -> CancelResult`:** under the **exit** lock, so it never waits for an entry, with its own `io_timeout_s` budget.
  - It reads unsettled `side == "buy"` order rows with a broker id whose status is not final, and calls `broker.cancel_order(id)` for each, with failures collected per order.
  - It never cancels a sell.
  - It writes nothing to the order rows: the sync loop observes the resulting `canceled`/`filled` status and the ledger books it.
  - It returns the order rows it asked to cancel and those that failed.
- **`sellable_positions() -> tuple[HeldPosition, ...]`:** the broker's non-dust positions (`_broker_positions`, unpriced). Used by flatten and by the kill-flatten of positions the journal doesn't know.
- **`can_place_orders() -> bool`:** delegates to the new `Broker.can_place_orders()` (decision 10). `_submit` maps `OrderMethodsUnavailable` to `not_submitted`, before the generic `unknown` branch.
- **Check 7:** records `atr` in the decision's `adjusted` (section 3).
- **`check()`:** treats failed symbols from `apply_all` as an incomplete sync (4.1).

### 4.6 Feed addition

`MarketFeed.price_age_s(symbol) -> float | None`: seconds since the newest valid quote or bar for the symbol, on the same clock and data as `is_stale`. None means never observed. It is read-only. The `RiskMarketView`-style protocol used by the Position Manager adds it.

### 4.7 Flatten (decisions 6 and 9)

**Request:** `autotrader flatten [--reason …]` writes `control_state.flatten_requested = {source: "cli", reason, ts}` and sets `paused = true`, in one transaction. A DB is required; there is no file fallback, because flattening needs the core anyway.

The CLI then checks whether a core holds the single-writer lock (a non-blocking probe of the lock file):
- If none does, it prints that the request is queued until the next `run core`, and exits 0.
- Otherwise it waits up to 60 s for `control_state.flatten_result` with a matching request timestamp, prints the outcome, and exits 0 when flat and 1 when still pending.

**Flatten pass** (Position Manager step, while `flatten_requested` is set):
1. `cancel_open_entries()`.
2. Latch `flatten` on every open journal position that has no trigger yet. The exit path (4.4) does the rest, with its retries.
3. Exit, with reason `flatten`, every broker-held non-dust position that has no open journal position (`sellable_positions()`).
4. **Flat check:** flat means all of:
   - no non-dust broker position;
   - no open journal position with qty ≥ min_qty;
   - no unsettled buy order;
   - no unresolved (`pending`/`unknown`) sell.

   When flat:
   - write `flatten_result = {request_ts, flat: true, dust: {symbol: qty}, ts}`;
   - clear `flatten_requested`;
   - publish `FlattenCompleted`;
   - log `positions.flat` with the dust per symbol.

   Otherwise:
   - log `positions.flatten_pending` every 10 s with what remains (positions, open entries, unresolved orders);
   - refresh `flatten_result` with `flat: false` and the remaining items.
- The manual pause stays set after flatten; resuming is the operator's call.
- **Cancelling a request:** `autotrader flatten --cancel` clears `flatten_requested` and records `flatten_result = {request_ts, flat: false, cancelled: true}`. It does not lift the pause and does not un-latch triggers already set. Without this, a request that can never finish would latch `flatten` on every new position after a resume. *(Builder's call.)*

**`shutdown.flatten`:**
- **When it runs:** if set and the Position Manager was started (startup completed), teardown runs a new bounded step, `flatten`, after the startup step and before `source.stop()`. This applies to every shutdown cause, including stream death, because exits need no price. After a startup failure there is no Position Manager, so the step is skipped and logged. Admission is closed in 1.5d; in 1.5c nothing places entries.
- **What it does:** it writes the flatten request and the pause (as the CLI does) and waits for the still-running `positions.manage` and `ledger.sync` loops to reach flat, polling `flatten_result`, until flat or until half the remaining shutdown budget is spent. The loops are stopped afterwards by `supervisor.stop()` as usual. The step never drives passes itself, so there is only one flatten actor. *(Builder's call.)*
- **On timeout:** it logs `core.shutdown_flatten_incomplete` with what remains. Teardown continues, because the second signal and the watchdog bounds still apply, and positions left open are handled at the next start.

### 4.8 `run core` wiring

New supervised loops in `start()`:

| Loop | Interval | Stall threshold | Notes |
|---|---|---|---|
| `ledger.sync` | 2 s | 30 s | 4.2 |
| `positions.manage` | 1 s | 30 s, `error` | 4.3 |

`ControlPoller` is passed to the Position Manager as its control source. The docstring of `runtime.py` ("Nothing calls `check()`/`exit()` until 1.5c/1.5d…") is updated: `exit()` is now called by the Position Manager, and `check()` still waits for 1.5d.

## 5. Error handling summary

| Situation | Outcome |
|---|---|
| Ledger row raises (non-anomaly) | Only that symbol is held back for the pass (`ledger.row_failed`); `check()` fails closed. |
| Broker read hangs in the sync loop | Bounded by `io_timeout_s`. The Position Manager loop is unaffected. |
| No trusted bid (missing or stale quote) | Price triggers don't fire. Time fires. Stale fires after `stale_exit_s` if enabled. `positions.stale_held` alert. |
| Position without a stop | `positions.unprotected` warning. Time, stale, kill and flatten still apply. |
| Exit `not_submitted` / `rejected` / no broker position | Retry with backoff (1→30 s). `positions.exit_failing` after 3. |
| Exit `pending` / `unknown` without a broker id | No retry. `positions.exit_unresolved` every 60 s (1.5d resolver, or the manual procedure). |
| Broker can't place orders (Alpaca until 1.6) | `OrderMethodsUnavailable` becomes `not_submitted`. The Position Manager attempts no exits or cancels and logs `positions.broker_cannot_trade` once; flatten stays `pending`. |
| Dust only | No retry. `positions.exit_dust`. The ledger closes the position as dust once settled. |
| Cancel fails | Logged per order and retried on the next killed or flatten step. |
| Latch commit uncertain | `CommitUncertain` from the bus. Logged; the next step re-reads the position and either sees the latch or latches again (guarded update). |
| Flatten not done in time at shutdown | `core.shutdown_flatten_incomplete`. Teardown continues. |

## 6. Testing

- **Pure evaluator:**
  - every trigger, at and on both sides of its boundary;
  - the precedence order;
  - trailing activation at exactly `trigger_r × R`, high-water and trailing-stop monotonicity, the trailing stop never below `stop`, `R ≤ 0` or missing ATR (no trailing), and `trailing.enabled: false`;
  - the same-bar rule (`evaluate_bar`, stop wins);
  - no price: only time and stale; the cold-start stale grace;
  - Decimal conversion from float bids.
- **Position Manager on SimBroker with a scripted quote feed:**
  - a stop exit end to end (booked, settled, closed with reason `stop` and P&L);
  - a TP exit; a time exit at the deadline; a trailing exit after activation and a pullback;
  - a stale exit only when enabled;
  - a partial entry fill protected at once, with a late fill growing the position, then the exit closing everything;
  - a latched stop with a recovered price still exits;
  - the restart cases: a fresh Position Manager on the same DB honours the latch and the persisted `time_limit_at`, and re-attempts at once.
- **Retries:**
  - budget spent → `not_submitted` → retry after backoff;
  - broker rejection → retry;
  - backoff sequence and cap, with an injected clock;
  - `positions.exit_failing` after 3;
  - `pending`/`unknown` without a broker id → no retry;
  - dust → no retry.
- **Kill:**
  - an open IOC entry (SimBroker made to leave an order open) is cancelled; a sell is never cancelled;
  - with `kill_flatten`, positions close with reason `kill`, including a late entry fill after the kill;
  - without it, positions keep normal management;
  - a kill already set at startup.
- **Flatten:**
  - the CLI writes the request and the pause;
  - the core flattens, writes `flatten_result` flat with a dust report, and clears the request;
  - flat is not reported while an entry is unsettled or an exit is unresolved;
  - the CLI exit codes for "queued (no core)", "flat" and "pending";
  - `shutdown.flatten` on graceful stop, and a timeout logging `core.shutdown_flatten_incomplete`.
- **Broker that can't trade:** an exit against a broker raising `OrderMethodsUnavailable` records `not_submitted`, never `unknown`; with `can_place_orders()` false the Position Manager latches but attempts nothing, and flatten stays pending with that reason. An unresolved exit repeats `positions.exit_unresolved` every 60 s (injected clock).
- **Ledger isolation:** a row raising `IntegrityError` holds back its symbol only; other symbols book; `apply_all` returns the failed symbol; `check()` rejects `state unavailable`.
- **Runtime:** both loops are started, supervised, stopped at teardown, and a hung sync doesn't delay `positions.manage` (injected hang, no wall-clock assertions).
- **Coverage:** `risk/` stays at 100 % branch coverage. Tests never assert wall-clock timing.

## 7. Documentation alignment (same PR)

- **SPEC §4.7:** the 1.5c decisions:
  - trigger price;
  - no retroactive bar triggers and the same-bar rule;
  - the trailing formula;
  - the time exit;
  - the retry policy;
  - kill and flatten.

  Also replace the sentence "WP 1.5 must specify trigger price, same-bar stop/TP ambiguity, and durable hold-time/initial-risk metadata before implementation" with a dated note.
- **SPEC §7:** the `positions` columns from migration 0005; `control_state` keys `flatten_requested`, `flatten_result`.
- **SPEC §0.6 and §4.11 (kill):** the 1.5c behaviour (dated note).
- **PLAN:**
  - D18;
  - a dated status note in the 1.5 row, marking follow-up (a) done;
  - the runtime docstring;
  - the `cli.py` `flatten` stub replaced;
  - `CLAUDE.md` commands: `autotrader flatten`.
- **Operator runbook** (`docs/`, decision 11): resolving an unresolved exit by hand, and `flatten` / `flatten --cancel`. Check `docs/` for an existing operations file before creating one.
- **PLAN 1.6 row:** the REST quote fallback question (decision 12); `AlpacaBroker.can_place_orders()` becomes true with the order methods.
- **Comment sweep:** `rg` for claims this falsifies, for example "until WP 1.5c", "Position Manager (WP 1.5)", "nothing calls exit()", "flatten … 1.5".
