# Operator runbook

Procedures for running the core by hand. It covers what `run core`, `autotrader flatten` and the
Position Manager (WP 1.5c) do today, their limits, and how to recover when something is stuck.
Requirements live in `SPEC.md`, delivery status in `PLAN.md`.

The examples assume the default `config.yaml` (journal `data/autotrader.db`, SQLite). Run SQL with
`sqlite3 data/autotrader.db`. Never delete journal rows; change only the columns a procedure names.

## Current limits (WP 1.6a)

- `run core` connects `AlpacaBroker`, which cannot place orders until WP 1.6d: its order writes
  (submit, cancel) raise `OrderMethodsUnavailable` and `can_place_orders()` is false. Single-order
  reads and the client-id lookup work since WP 1.6a; `order_fills` waits for 1.6b. Until then
  a linked or read order with a fill the row has not booked yet cannot settle: the sync pass
  reads executions whenever the broker reports more filled than the row has booked, so such a
  row fails every pass (`risk.order_sync_failed`, `OrderMethodsUnavailable`) and stays unsettled
  (see "Resolving an order incident", step 3). The Position Manager still
  evaluates and latches triggers (`positions.exit_triggered`) and logs
  `positions.broker_cannot_trade` once, but sends no exits and no cancels. A flatten stays pending
  with `reason: "broker cannot place orders"`.
- With `SimBroker` the Cycle Runner places entries once admission opens; with `AlpacaBroker`
  admission stays disabled until WP 1.6d (`cycle.admission_disabled`, reason `broker cannot place
  orders`), so `run core` places no entries there. The rejection reasons in "Entries rejected"
  below are enforced by `RiskEngine.check()` for every admitted candidate. How admission opens
  is in "Startup and admission".
- An order the engine cannot account for (no broker id, or one the broker no longer knows) is
  looked up by its client id for 80 s, then becomes an incident. The resolver never marks
  `not_submitted` by itself and never retries the order: the operator repairs it with
  `autotrader repair` (see "Resolving an order incident"). On `AlpacaBroker` the lookup and `repair link` work since WP 1.6a; no order rows
  exist there before 1.6d enables writes, so no such incident arises in production yet.
- Recovering from an `oversell` anomaly is `autotrader repair oversell` (see "Manual coins in a
  traded symbol").
- An external sale that failed is retried only after the order-sync loop has settled the failed
  row, so external retries run every max(backoff, about 2 s).
- An entry on a symbol with an unsettled sell row (an external sale included) is rejected by
  `check()` (`unsettled sell on <SYM>`, see "Entries rejected"), so a partly filled external
  sale can no longer merge into a new trade's P&L.
- The flatten-state guards rely on SQLite's write lock. Postgres needs `SELECT ... FOR UPDATE`
  (WP 3.5 ownership work).

## Startup and admission

`run core` builds the Cycle Runner with admission closed, runs `recover()` (it settles cycles the
previous process left open) before the feed stream starts, and subscribes the runner to
`CandidateSignal`. Exits, kill polling and the order sync never depend on admission. A supervised
loop, `admission.ready` (every second), opens admission, once and for good, when all of these hold:

1. Nothing disabled it (see "Admission disabled" below).
2. The ledger cash check completed and no cash fault is set (the same condition as
   `cash reconciliation failed` in "Entries rejected"). The first clear observation starts the
   proof; a fault seen again before admission opens restarts it.
3. A Position Manager step that started after that observation evaluated every open position
   (`evaluated` or `latched`; none open also counts).
4. The holdings check passed: for every symbol with an open journal position or a dust balance,
   journal qty + dust is at most the broker's total qty. A larger broker holding is fine.
   It runs only until admission opens; the cash check keeps running every 60 s regardless.

Open admission logs `cycle.admission_open` (info). If it is still closed 3 x the snapshot interval
after the start (3 min by default), `cycle.admission_waiting` (warning) logs `unmet` (the open
conditions), `positions` (the open positions that are not `evaluated` or `latched`, with their
outcome) and `short` (symbols failing the holdings check), then repeats every 10 min while
admission stays closed. It is never logged while admission is disabled (`ready()` returns
before any logging); the `cycle.admission_disabled` startup line is the signal then. Until it
opens, a candidate is dropped with `cycle.dropped`
(`admission disabled: ...` or `startup reconciliation pending`).

### Admission disabled

These disable admission until the next restart; the exits keep running. Each logs
`cycle.admission_disabled` with the reason:

| Reason | Level | What to do |
|---|---|---|
| `broker cannot place orders` | warning | Expected with `AlpacaBroker` before WP 1.6d. Nothing to fix. |
| `backfill failed` | error | The history backfill raised (`feed.backfill_failed`) and the scanner is not subscribed. Restart once the data API answers. |
| `recover failed` | error | `recover()` raised (traceback in the log). Fix the cause (usually the journal or a hung database), then restart. Candidates are logged as dropped meanwhile. |

A broker whose mode differs from `mode` in `config.yaml` stops the startup with
`broker mode '...' does not match config mode '...'`: with a mismatch `exit()` would reject every
exit. Check `mode` against `ALPACA_PAPER` in `.env`; nothing runs until they agree.

### Admission waiting: what to do per cause

- **Cash fault, `cash check not completed: <fault>`** (`risk.cash_fault_set`,
  `ledger.cash_check_failed`): see `cash reconciliation failed` in "Entries rejected". A sticky
  fault (a `CommitUncertain` error) holds until the core is restarted: fix the cause and restart.
- **`unsettled order rows`:** an order row with `settled_at` NULL keeps admission closed. After a
  restart it keeps the cash check skipped, so the line names it next to the cash fault. After the
  cash check (an exit the Position Manager placed, or one that never settles) the holdings check
  is skipped while the row is unsettled at its first guard read, without reading the broker, or
  discarded after the broker read when a row is unsettled at its second guard read or the
  generation moved. The line then names `unsettled order rows` instead of a holdings cause. A
  discard on a moved generation with nothing left unsettled (an order settled during the check)
  carries the same label for one tick. A row that settles clears it on the next tick. A resolver incident stays until `autotrader repair` settles it (see "Resolving an
  order incident"; stop the core, repair, restart).
- **`positions not evaluated`** (`positions` lists each position with `no_bid` or `failed`):
  - `no_bid`: no trusted bid for a held symbol. If the feed is down or stale, wait for it to
    return. A symbol outside the profile's universe is never quoted; `admission.unquoted_holdings`
    (warning, at startup) names such symbols. The clean fix is to add the symbol to the universe
    and restart. Without a bid only the time exit (`max_hold_min`) and, when enabled, the stale
    exit can fire; they latch without a price, and a latched position counts as evaluated, so
    the blocker clears on its own once that exit latches (it still has to be sent and settled,
    which `unsettled order rows` below covers). Closing the position by hand is not enough: the
    ledger books only sells the journal placed, so the position stays open and the next holdings
    check reports `holdings short`; that needs the manual journal correction described there.
  - `failed`: the position's step raised (`positions.step_failed`, with the traceback) or its
    trigger could not be latched. A position whose exit keeps raising stays `failed` and keeps
    admission closed until it is exited or the cause is fixed. An exit that is only REJECTED
    (`exit()` returns `rejected` or `not_submitted`) does not raise: the position stays
    `latched`, the Position Manager backs off and logs `positions.exit_failing`, and admission
    is not blocked by it.
- **`no position manager step since the cash check`** (`unmet`): transient, clears on the next
  `positions.manage` step. If it persists, look for `positions.manage` stalls or
  `positions.step_failed`.
- **`holdings short`** (`admission.holdings_short`, error: `symbol`, `journal_qty`, `dust`,
  `broker_qty`): the journal claims more than the broker holds. Investigate (a manual sale, a
  missed fill, a withdrawal) and correct the journal by hand; this includes a dust-only symbol
  whose coins were sold by hand (no open position, `broker_qty` 0). Nothing flattens or sells
  automatically. A symbol is logged once per distinct set of figures and again if it stops being
  short and comes back; after a cash-fault reset the same shortfall is not logged again, but the
  waiting log still names it under `short`.
- **`holdings check not completed`:** a read of the holdings check failed: the broker, journal or
  cash-guard read raised (`admission.holdings_read_failed`, warning, once per failure streak) and
  is retried every tick. Check the broker connection (and the journal database) if the line
  persists. An order in flight is not reported here but as `unsettled order rows`. Without a
  read-failed line it can also appear for one tick after a passed check was dropped because an
  order changed the journal; it re-runs on the next tick.

Known costs (revisited in WP 1.6):

- An exit left unsettled after the cash check (an incident, or a sell that never settles) keeps
  admission closed until it settles or is repaired.
- A latched position whose exit attempts keep raising reports `failed` and keeps admission
  closed. One whose exits are rejected (`rejected` or `not_submitted`, logged
  `positions.exit_failing`) reports `latched` and does not block admission.
- `cycle.admission_waiting` repeats every 10 min and names the current causes, including
  unsettled order rows before and after the cash check; the per-cause lines
  (`admission.holdings_short`, `admission.holdings_read_failed`, `positions.*`,
  `risk.cash_fault_set`) have the details.
- The holdings check reads the broker's positions every tick until it passes, except while an
  order row is unsettled at its first guard read. A cash fault set and cleared between two ticks
  is not seen.

## Entries rejected

`RiskEngine.check()` rejects an entry with one of these reasons. Each reason in the table
below is a gather-time rejection: a persisted risk decision, no pause is set and no order row is
written (the pre-submit case after the table is different). Exits are never affected. With
`AlpacaBroker` `run core` calls no `check()` until WP 1.6d (admission is disabled), so there these
describe what the engine will do.

| Reason | Meaning | Clears when |
|---|---|---|
| `daily-loss baseline unavailable` | No equity snapshot in [trading-day start - 120 s, trading-day start] (2 x the 60 s snapshot interval). | The next trading day that has such a snapshot. Nothing to fix. |
| `equity snapshot overdue` | The snapshot loop has not committed a snapshot yet in this process, or the last one is older than 120 s. | A snapshot loop commit (within 60 s once the loop is healthy). |
| `cash reconciliation failed` | The ledger cash check has not completed since startup, or its guard, snapshot or check stage raised. | A completed cash check with result baseline, ok or discrepancy (not `skipped`). After a `CommitUncertain` error it holds until the core is restarted. A completed check can also be `skipped`: the ledger skips the comparison when any order row has `settled_at` NULL (at either guard read) or the order/fill generation (`max(orders.id)`, fill watermark) moved between the reads. A skipped check neither sets nor clears the fault and logs no `ledger.cash_check_failed`, so after a restart one stuck unsettled row keeps entries rejected, and keeps admission closed (see "Startup and admission"). Find it with `SELECT id, symbol, side, status FROM orders WHERE settled_at IS NULL;` and settle it (see below). |
| `unsettled sell on <SYM>` | An order row with side `sell` and `settled_at` NULL exists on that symbol. | The ledger settles the row. A `pending`/`unknown` sell without a broker id is linked by the resolver, or becomes an incident that stays until `autotrader repair` (see "Resolving an order incident") settles it. |

Pre-submit re-check: a fault, an overdue snapshot or `trading day changed` that appears after
the gather but before the broker call marks the order row `not_submitted` (log
`risk.entry_not_submitted` with `reasons`); nothing is sent. This differs from the table: an order
row exists and is marked. `trading day changed` means the trading day rolled over between the
gather and the submit. It is per attempt, with nothing to clear: the next `check()` runs on the
new day, which itself needs a trusted baseline (`daily-loss baseline unavailable` otherwise).

`daily-loss baseline unavailable` is expected for the whole trading day after downtime across the
trading-day start (00:00 in `trading_day_tz`, UTC by default; the core was down, so no snapshot fell in the window). It is not a
fault; entries resume the next day.

### Cash discrepancy

The snapshot step of `run core` runs the ledger cash check against the Alpaca paper account every
60 s. A discrepancy commits the manual pause and a `CashDiscrepancy` event, and re-baselines in
the same unit, so the next check compares from the new level.

1. Read the event: `SELECT ts, payload FROM events WHERE type = 'CashDiscrepancy' ORDER BY id;`
   (`expected`, `actual`, `diff`, `n_fills`, `baseline_ts`).
2. Explain the change (a manual transfer or trade, a fee posted outside the fills, a missed fill)
   and record it. Do not resume while it is unexplained.
3. Resume with the manual-pause SQL in "Resuming after a pause".

### Log lines to act on

- `ledger.anomaly_unrecorded` (error): the pause/event commit of a ledger anomaly raised; the
  error is re-raised and the anomaly may not be recorded. Check `control_state.paused` and the
  `LedgerAnomaly` events; set the pause by hand if it is missing, then investigate the anomaly.
- `positions.cancel_failed_repeatedly`: an entry cancel failed 3 times; retries back off 1, 2,
  4 ... up to 30 s. Check the order at the broker; the retries continue on their own. The line
  repeats at the 13th failure and every 10th after (23rd, 33rd, ...) while the cancel keeps failing.
- `ledger.cash_check_failed`: a stage of the cash step raised (`stage` field); entries stay
  rejected with `cash reconciliation failed` until a check completes.
- `core.watchdog_late_start` (stderr JSON): the shutdown watchdog was armed without having been
  started up front, so it started its thread inside the stop path (signal-safety risk). It still
  guards the stop; report it as a bug.

## Resuming after a pause

These set the manual pause (`control_state.paused = true`):
- flatten (`autotrader flatten`, `shutdown.flatten`);
- a ledger anomaly (`ledger.anomaly`), re-asserted on the next sync pass after a resume while it
  persists (another `LedgerAnomaly` event, `ledger.anomaly_repaused` warning), so fix the journal
  first;
- an unexplained cash change (`CashDiscrepancy`; `run core` checks cash every 60 s, see "Entries
  rejected");
- the Risk Engine on the max-drawdown check (`risk.paused_manual_resume`).

The daily-loss check sets `control_state.paused_until` instead (`risk.paused_until`): it blocks
entries until the start of the next trading day and then expires on its own, with no operator
action.

There is no resume command until the Phase 3 API. Resume the manual pause only after the cause is
fixed:

```sql
UPDATE control_state SET value = 'false' WHERE key = 'paused';
```

## Flatten

```
uv run autotrader flatten [--reason TEXT]
uv run autotrader flatten --cancel
```

`flatten` writes `control_state.flatten_requested = {source: "cli", reason, ts}` and sets the
manual pause in one transaction. If a request is already pending, it keeps that request, says so,
and waits for that request's result instead. A database is required; there is no file fallback.

Then it probes the core's journal lock:

| Situation | Output | Exit |
|---|---|---|
| No core running | `no core is running: the request is queued until the next run core` | 0 |
| Core reports flat within 60 s | `flat (dust: {...})` | 0 |
| Still pending after 60 s | `still pending after 60 s: {remaining}` | 1 |
| No result for this request within 60 s (the core has not written one yet) | `still pending after 60 s: no result yet` | 1 |
| Cancelled by someone else while waiting | `flatten cancelled` | 1 |
| Invalid config | `invalid config: ...` on stderr | 1 |
| Journal or other error | `flatten failed (...)` on stderr | 1 |

Exit 1 on a timeout does not cancel the request: the core keeps working it. The pause stays set
in every case; resuming is the operator's call.

What the core does while a request is pending (every Position Manager step, 1 s):
1. cancels open entry orders (never a sell);
2. latches `flatten` on every open journal position without a trigger and exits it, retrying with
   backoff (1, 2, 4 ... s, capped at 30 s; `positions.exit_failing` after 3 failures);
3. sells broker holdings that have no journal position as external sales (see below);
4. reports flat only when no non-dust broker holding, no open journal position and no unsettled
   order row of either side remain. An unsettled row on any symbol, however old, blocks flat.

While not flat it logs `positions.flatten_pending` and refreshes `flatten_result` every 10 s with
`{request_ts, flat: false, remaining, ts}` (plus `reason: "broker cannot place orders"` when the
broker cannot trade). `remaining` holds:
- `positions`: `[{position_id, symbol, qty}]`, the open journal positions;
- `holdings`: `[{symbol, qty}]`, the non-dust broker holdings, or `"unavailable"` when the broker
  read failed;
- `orders`: `[{order_row_id, symbol, side, status, age_s, unresolved, incident, anomaly}]`, every unsettled
  order row. `unresolved` is true for a `pending`/`unknown` row without a broker id and for any
  order incident; `incident` is the row's incident reason or null; `anomaly` is the row's latest
  ledger anomaly kind, or null.

Read it with:

```sql
SELECT value FROM control_state WHERE key IN ('flatten_requested', 'flatten_result');
```

On flat, the core writes `flatten_result = {request_ts, flat: true, dust, external_sales, ts}`,
clears the request, publishes `FlattenCompleted` and logs `positions.flat`.

`flatten --cancel` clears a pending request and records `flatten_result = {request_ts, flat:
false, cancelled: true}` (exit 0; `no flatten request pending` if there was none). It does not
lift the pause and does not un-latch triggers already set. Cancel a request that cannot finish
(for example with Alpaca before WP 1.6d): otherwise it latches `flatten` on every new position
after a resume.

`shutdown.flatten: true` in `config.yaml` runs the same request on every graceful stop after a
completed startup. It runs after `runner.close` (up to 20 s, so no entry starts during the
flatten) and waits up to half the remaining shutdown budget: with the 30 s default and a close that
used its full 20 s, about 5 s. On timeout it logs
`core.shutdown_flatten_incomplete` and teardown continues; whatever is left is handled at the next
start.

## Kill

`autotrader kill` (or `touch data/control/KILL`) blocks entries. In the core the Position Manager
then cancels outstanding entry orders. With `kill.flatten` (default: true in live, false in paper)
it also latches `kill` on every open position, including one opened by a late fill, and sells
external holdings. Without it, positions keep normal stop/TP/time/trailing management. After
`kill --clear` no further cancels are sent; exits already latched still complete.

## Resolving an order incident

Every order carries a client id, so the engine can find it at the broker even when the submit
response was lost. Entries use `at-{jid}-c{cycle_id}-e`, exits `at-{jid}-o{order_row_id}`; `jid`
is the journal's own 8-hex id (`control_state.journal_id`), so ids of another journal or the
legacy `at-c<cycle>-e` form are never taken as ours. Exits are sent with `submit_order` for the
free qty (`qty_available`) of the broker holding; a holding fully
reserved by pending sells rejects the exit (`reserved by pending sells in <SYM>`) until they
settle. A journal without a `journal_id` row (hand-deleted) fails entries closed
(`state unavailable`); exits still go out without a client id and log `risk.journal_id_missing`.

### What the resolver does

The ledger's 2 s sync pass (`RiskEngine.sync_orders`) resolves two kinds of unsettled row
(`check()` runs its own order read before an entry, without the resolver):
- (a) `pending`/`unknown` with no broker id and no submit call in flight;
- (b) a row with a broker id whose `get_order` raised `OrderNotFound` or `UnknownOrderStatus` (the
  broker reported a status we do not map).

A row without a client id of this journal becomes a `no_ownership_proof` incident at once (no
lookup, no bound). Otherwise it looks the order up by client id. For (a), a match on symbol, side,
qty, order type, limit price, time in force (the one our own submits use for that type) and
creation time (no earlier than 5 minutes before `submitted_at`) links the row (`risk.order_resolved`, `OrderResolved`
event; the next read books its fills and settles it; on `AlpacaBroker` before WP 1.6b only
when there is no unbooked fill, see "Current limits"). For (b), a match links nothing: the row
keeps its clock and becomes `lookup_inconsistent` at the bound. A zero match or a failed lookup
waits: the bound is 80 s from `submitted_at`
for (a), or from the first `OrderNotFound` / `UnknownOrderStatus` (persisted as `resolve_since`, restart-safe) for (b).
Past it, or at once on a mismatch, the row becomes an incident: `risk.order_incident` (error),
an `OrderIncident` event, and a reminder `risk.order_incident_open` every 60 s while it is
unsettled. While a (b) row is within its bound, entries fail closed on all symbols; afterwards
the incident itself blocks only its own symbol, like an (a) incident. That holds only while the
core keeps running: after a restart (and every repair needs one, the core must be stopped) the
cash check is `skipped` while any row is unsettled, so `cash reconciliation failed` rejects
entries on **every** symbol until all incidents are repaired, their rows settled and one cash
check completes (see "Entries rejected"). Symptoms you may also see:
`positions.exit_unresolved`, `positions.entry_unresolved`, `"unresolved": true` in
`flatten_result.remaining.orders`.

The resolver never sets `not_submitted`: a lookup that finds nothing is no proof that an order
can never be accepted or become visible (PLAN.md section 3.2 item 4).

| Incident reason | Meaning | Repair |
|---|---|---|
| `no_ownership_proof` | The row has no client id of this journal (NULL or legacy form), so no lookup was made. A (b) row (it has a broker id, e.g. an exit sent without a client id) gets this reason on its first `OrderNotFound` or unmapped-status read, without the 80 s window; `risk.order_status_unknown` before it shows that an unmapped status was the cause (a known gap until WP 1.6d, PLAN 1.6 row). | Manual procedure below. If the cause was an unmapped status, bring the order to a final state at the broker first: the hand-written link only holds once the status is mapped, otherwise the next read raises the incident again. |
| `client_id_mismatch` | The broker has an order under our client id that differs from the row (symbol, side, qty, order type, limit price, time in force, the `created_at` bound, or broker id). The order exists; a broker inconsistency. | Find out which is right; `repair link` only if it matches after all, else escalate ("Repair commands", step 4). Never `repair not-submitted` (it refuses this reason). |
| `not_found` | Zero match until the bound. The order may still exist. | Investigate at the broker, then `repair link` (found) or `repair not-submitted` (authoritative evidence). |
| `lookup_failed` | The lookups raised until the bound (or the budget ran out), so nothing was learned. | Same as `not_found`. |
| `lookup_inconsistent` | (b) only: `get_order` says not found while the client-id lookup finds the order, until the bound. | `repair link` with the broker id the lookup reports. |
| `unknown_status` | (a) or (b): the broker reported an order status we do not map (`held`, `suspended`, `replaced`, ...; the log line `risk.order_status_unknown` names the status): the order exists but we cannot book it. (a): a client-id lookup met the status during the resolution window; this happens e.g. when the submit ack itself carried the status (`alpaca.ack_status_unknown` names the broker order id). (b): a direct read met it. Either way the evidence is kept in memory for the window, and any inconclusive outcome at the bound (`not_found`, `lookup_failed`, `lookup_inconsistent`) of such a row is recorded as this reason. `no_ownership_proof` and `client_id_mismatch` still win. | Recovery below: bring the order to a final state at the broker, then `repair link`. Never `repair not-submitted` (it refuses this reason). |

The reason is not always the result of a final lookup. A row past its bound whose lookup the
pass budget (10 s, shared by every unsettled row) did not reach becomes an incident in that
pass anyway, with the outcome of its last lookup in this process, or `lookup_failed` if there
was none (e.g. the first pass after a restart). So with many unsettled rows or a slow broker a
`not_found` or `lookup_failed` incident may be stale: always check at the broker before
`repair not-submitted`; `repair link` does its own fresh lookup.

An open incident keeps the row's reservation and blocks flat. The ledger books nothing on the
incident row itself, even fills the broker reported before the incident was written
(`ledger.exit_deferred` names the row), and it holds back every later booking on its symbol
until repaired, the fills of a later exit included: the broker may have
sold while the journal position stays open and the cash check reports `skipped`. This is
deliberate (booking a later exit before an unresolved earlier order gives anomalies or a wrong
P&L). It books nothing wrong.

### Repair commands

Stop the core first (Ctrl-C). Each command takes the journal lock and refuses with `core is
running` (exit 1) otherwise.

```
uv run autotrader repair link <order_row> <broker_order_id> --evidence "..."
uv run autotrader repair not-submitted <order_row> --evidence "..."
uv run autotrader repair oversell <order_row> --evidence "..."
```

All take `--evidence TEXT` (required: what you checked and when), `--yes` (skip the confirmation)
and `--config/-c`. Each prints the row before and after, asks to confirm, then writes the row
change and an `OperatorRepair` event (`action`, `order_row_id`, `before`, `after`, `evidence`,
`operator`, and for `link` `broker_view`: the broker order that proved the match) in one unit.
Exit 0 done, 1 refused (reason printed), 2 usage. A repair never resumes the manual pause and
never migrates: it refuses a journal path that does not exist (`no journal at <path>`, check
`--config`) and a journal whose schema is not at head (run `autotrader db upgrade` first).

1. Read the row and its incident:
   ```sql
   SELECT id, symbol, side, qty, status, submitted_at, client_order_id, broker_order_id,
          incident, incident_at, external
   FROM orders WHERE id = <row>;
   SELECT ts, payload FROM events WHERE type = 'OrderIncident' ORDER BY id;
   ```
2. Look for the order at the broker (its full order history, open and closed): by client id, or
   for a row without one by symbol, side, qty and a time close to `submitted_at`. Absence is not
   proof: most of these rows are `unknown`, which the engine records after a timeout or a network
   error, exactly the case where the order may have reached the broker and is not visible yet.
   Wait at least 15 minutes after `submitted_at` before concluding anything (an investigation
   delay, nothing more). Also check fills and activities for the symbol from `submitted_at` on,
   and the broker position qty against the journal (the open journal position's qty plus any
   `symbol_dust`). If the position changed by the row's qty, the order executed: find its id.
3. **Found at the broker:** `repair link <row> <broker_order_id>`. It reads the order (read-only)
   and requires the broker order's client id to equal the row's, plus symbol, side, qty and the
   5-minute `created_at` check, plus the order type, limit price and time in force our own
   submits use. It accepts id-less `pending`/`unknown` rows and (b) incident rows
   (`not_found`, `lookup_failed`, `lookup_inconsistent`, `client_id_mismatch`, `unknown_status`;
   the broker id is then replaced, `before` keeps the old one). For an `unknown_status` row it also
   requires the broker order to be **final** (`filled`, `canceled` or `rejected`) and refuses with
   `broker order is not final: <status>` otherwise (see "Recovering an `unknown_status` incident"). It refuses lookup misses, mismatches (the
   differences are printed) and rows without ownership proof; there is no `--force`. It writes
   `broker_order_id` and clears the incident fields. On `AlpacaBroker` it works since WP 1.6a
   (reads only; no order is written). **Until WP 1.6b, linking settles only orders with no fill
   left to book:** the next sync pass reads the order's executions whenever the broker reports
   more filled than the row has booked, and `AlpacaBroker.order_fills` raises until 1.6b. So a
   linked order that filled, or a partially filled order that ended `canceled`, fails every sync
   pass (`risk.order_sync_failed`) and stays unsettled, holding back its symbol and flatten. In 1.6a
   only a final order with nothing filled beyond the row's booked qty (e.g. `canceled` or
   `rejected` with zero fill) settles after the link. Keep the manual pause in place until 1.6b
   can reconcile the fills (the link still records the broker id and your evidence).
4. **Not found, with authoritative evidence:** `repair not-submitted <row>`, only for an unsettled
   `pending`/`unknown` row without a broker id. It refuses an `unknown_status` or a
   `client_id_mismatch` incident: each reason is positive evidence the order exists (see
   "Recovering an `unknown_status` incident"). A `client_id_mismatch` row whose broker order
   still differs from the row after step 2 fits neither command: leave it an incident (its
   symbol stays blocked, the fail-closed outcome), record the differences `repair link` prints,
   and escalate (owner; Alpaca support with the request id from the `alpaca.http` line). Do
   not mark it `not_submitted` by SQL either. Use `not-submitted` only with what PLAN.md section 3.2 item 4
   requires: the original worker is quiescent (the core is stopped) **and** authoritative broker
   evidence rules out that the order was or will ever be accepted or become visible, such as a
   broker-confirmed final rejection. A grace window plus a zero-match lookup does not qualify,
   and neither does an HTTP timeout. The command cannot check the evidence; it records it. The
   mark is final: the next sync pass settles the row with zero fill and never reads it again,
   even if a broker id is added later. A wrong mark books permanently wrong (an exit that filled
   leaves the journal position open against a flat broker, so `positions.exit_failing` repeats
   and flat never completes; an entry that filled makes its coins look like an external holding
   that the next flatten sells with no P&L).
5. **No ownership proof (`no_ownership_proof`):** the CLI refuses `link` here (a wrong link books
   a foreign order); the operator's own evidence is the proof. With the core stopped and the
   broker order identified by symbol, side, qty and time, set its id by hand:
   ```sql
   UPDATE orders SET broker_order_id = '<broker id>', incident = NULL, incident_at = NULL
   WHERE id = <row>;
   ```
   This writes no `OperatorRepair` event, so nothing in the journal records why: put the
   evidence (broker order id, what matched, who, when) in your ticket or operator notes.
   (`not_submitted` for such a row is `repair not-submitted`, same evidence rule as step 4.)
6. Start the core again. Confirm the row settles (`settled_at` set) and the incident reminders
   stop. On `AlpacaBroker` before WP 1.6b a linked row with an unbooked fill does not settle
   (step 3): it fails every sync pass instead; keep the pause until 1.6b. A row left unrepaired keeps reminding every 60 s and holding back its symbol; that is
   expected. Resume the manual pause only after this (see "Resuming after a pause").

Undoing `repair not-submitted` is manual and only safe with the core stopped, when the mark was
wrong and the broker order is identified. First check for a later order row on the same symbol
that settled after the mark (`SELECT id, side, status, settled_at FROM orders WHERE symbol =
'<SYM>' AND id > <row> AND settled_at IS NOT NULL;`); if one exists, do not undo: the journal
booked on top of the mark, so escalate. Otherwise back up `data/autotrader.db` first, then set the broker
id and the status back (`pending` or `unknown`, as the `OperatorRepair` event's `before` shows):

```sql
UPDATE orders SET broker_order_id = '<broker id>', status = '<before status>', settled_at = NULL
WHERE id = <row>;
```

This does not undo bookings made after the mark: check the symbol's position, fills and cash
against the broker by hand and expect to correct them.

### Recovering an `unknown_status` incident

The broker holds the order in a state the adapter does not map, so nothing can be booked and the
row keeps its symbol blocked (the fail-closed outcome). The row may have a broker id (type (b))
or none (type (a), e.g. the submit ack carried the unmapped status). `repair not-submitted`
refuses every `unknown_status` row (the order exists), and a plain `repair link` refuses a
non-final order:

1. Stop the core (Ctrl-C). Note the status in the `risk.order_status_unknown` log line, and the
   Alpaca request id from the matching `alpaca.http` line, for a support ticket if needed. For a
   row without a broker id, `alpaca.ack_status_unknown` (if the ack carried the status) names
   the broker order id.
2. Look the order up at the broker by its broker id, or for a row without one by its client
   order id (`client_order_id` column). **Before linking, check its status there yourself**, for
   both kinds of row. The evidence that this was an unmapped status is weak: for either kind of
   row it is kept in memory only, so after a restart inside the 80 s resolution window an
   inconclusive lookup can end the row as `lookup_failed` (or `not_found`) instead of
   `unknown_status`. If the order's status is unmapped, treat the row as `unknown_status`
   whatever reason it carries, and do not use `repair not-submitted` on it.
3. Bring the order to a final state: cancel it in the Alpaca dashboard, or wait until it finishes
   (`filled`, `canceled`, `rejected`).
4. `repair link <row> <broker id> --evidence "..."` (the row's own broker id, or for an id-less
   row the id the broker shows for its client order id). It refuses while the order is not final
   (`broker order is not final: <status>`). Do not use it to get past that; the check keeps a row
   from being linked while the order can still move.
5. Start the core: the ledger books the row's fills from the final state on the next pass. On
   `AlpacaBroker` before WP 1.6b that holds only for an order with no fill left to book (e.g.
   `canceled` with zero fill); a filled or partially filled one fails every sync pass and stays
   unsettled until 1.6b can read its fills ("Resolving an order incident", step 3). Keep the
   manual pause in place until then.

While Alpaca keeps the order in the unmapped state (`held`, `suspended`, ...), escalate to Alpaca
support; the row stays an incident and keeps reminding every 60 s.

### Alpaca request log

Every trading-API call that returns or raises writes one `alpaca.http` line: `method`, `path` (the template, e.g.
`/orders/{id}`), `order_id` for a single-order call, `status` or `error` (the exception type
name), `elapsed_ms`, and `request_id` (Alpaca's `X-Request-ID`); a non-2xx adds Alpaca's
`message`. It is `info` for a 2xx and `warning` otherwise; headers and keys are never logged. The
request id is also in the text of every `BrokerError` raised from a response, so the incident or
`risk.*` line carries it too: quote it when asking Alpaca about a call. A call cancelled while in
flight (the engine's I/O budget ran out, or a shutdown cut it) writes **no** `alpaca.http` line
until WP 1.6d adds one (PLAN 1.6 row): for such a call, e.g. a submit that ended `unknown` on a
timeout, the engine's `risk.*` line is the only record, and there is no request id.

Broker order ids must be Alpaca's UUID form (`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`, hex). The
adapter refuses any other id before the request, and an off-pattern id in a response, with
`refusing an unsafe order id: <withheld: N chars>`. `repair link` cannot store such an id: it
links only when the typed id equals the id the broker's lookup returns, which is a UUID (copy it
exactly as Alpaca shows it, lower case). A broker id set by hand with SQL (step 5 above) that is
not a UUID fails every read of the row with that message, as in "A linked order whose read keeps
failing" below; correct the id with the same SQL. Upper-case hex reads, but `repair link`
compares the id exactly.

### A linked order whose read keeps failing

Symptom: `risk.order_sync_failed` for the same `order_row`, with a `BrokerError` traceback, every
sync pass (about every 2 s) without end, and no `risk.order_incident` for the row. Entries are
rejected on every symbol (`state unavailable: SyncIncomplete`, `order rows not synced: [<row>]`)
and the unsettled row holds back flatten.

Meaning: the row has a broker id and Alpaca keeps answering `get_order` with a 2xx the adapter
refuses as malformed: a status contradicted by `filled_qty` (`alpaca order status 'filled' with
no filled_qty`, `alpaca order reports a negative filled_qty`), a value out of range or not a
plain decimal string, another order's id, or a missing field (`unexpected alpaca get_order
response: <type> (request ...)`), or an id that is not a UUID (`refusing an unsafe order id`).
A transient failure (5xx, timeout) looks the same but stops on its own; this one does not. Unlike
`OrderNotFound` or an unmapped status, it starts no resolution clock and never becomes an
incident, so `repair link` and `repair not-submitted` both refuse the row (it has a broker id and
no incident). Nothing is booked from the malformed answer (fail-closed). A bounded path with an
incident is a precondition of WP 1.6d (PLAN 1.6 row).

Recovery:
1. Keep the manual pause; stop the core (Ctrl-C) if the repeating traceback floods the log.
2. Note the request id from the `BrokerError` text and the `alpaca.http` line, and look the order
   up in the Alpaca dashboard by the row's `broker_order_id` (step 1 of "Repair commands").
3. If the id itself is wrong (set by hand, not a UUID or not the order you meant), correct it
   with the step 5 SQL and record your evidence. Otherwise there is no supported repair before
   WP 1.6d: escalate (owner; Alpaca support with the request id). Do not mark the row
   `not_submitted` or clear it by SQL to get entries going: the order exists at the broker.

### Positions read refused (unknown `asset_class` or symbol)

Symptom: every broker positions read fails with `unexpected alpaca positions response: not a list
of positions with a known asset_class (request ...)`, `refusing an unsafe symbol: <withheld: N
chars>` or `cannot map position symbol to BASE/QUOTE: <withheld: N chars>`, or the generic
`unexpected alpaca positions response: ValueError (request ...)` for an impossible value. You see
it in `risk.snapshot_failed` (the snapshot loses its exposure),
`positions.flatten_read_failed` (external sales and the flatten check cannot run, so a flatten
never completes), `risk.exit_state_unavailable` for every exit attempt on any symbol (an exit
reads the positions first; `positions.exit_failing` follows after repeated failures), and
admission's holdings check keeps failing.

Meaning: the adapter fails the whole read closed rather than hide a holding: a row whose
`asset_class` is missing or not one of `crypto`, `us_equity`, `us_option`, `crypto_perp`, a crypto
symbol it cannot map to `BASE/QUOTE` or that is off the config's symbol pattern, or (Fable review
2 M2) an impossible value (a negative qty, `qty_available` above `qty`, two rows for one symbol).
No exit can be sent while it lasts; the kill switch and the manual pause still work.

Recovery:
1. Check the positions in the Alpaca dashboard. A holding you did not expect (a new asset class,
   a transferred coin, an option) is the usual cause.
2. If a position must be closed now, close it in the Alpaca dashboard; the bot cannot.
3. A new asset class Alpaca introduced needs a code change adding it to `_ASSET_CLASSES` in
   `broker/alpaca.py` (spot `crypto` stays the only one held); an odd symbol or value is a broker
   inconsistency to escalate with the request id. Once the read succeeds again the loops recover
   on their own.

A submit the transport proved unsent is marked `not_submitted` and logged
`risk.order_not_submitted` with `reason="request not sent: <message>"`. Only a connect error, a
connect timeout, a pool-wait timeout or a call on a closed client count (the connection never
existed, or none was handed out); the message is `alpaca <METHOD> <path template> not sent:
<cause type name>` (or `client closed`), with no request id (no response exists) and never the
cause's text. It needs no repair: nothing reached Alpaca. Any other failure, and a submit that
times out inside the engine's I/O budget, may have been written: the row becomes `unknown` and goes
to the resolver as before. A cancelled caller is not mapped to either; the cancellation propagates.

With `AlpacaBroker` no order is ever placed before 1.6d (`can_place_orders()` is false and
admission is disabled), so none of this arises in production yet.

## Accounting for an external sale

Flatten and kill-flatten sell broker holdings that have no journal position (for example coins
bought by hand). The ledger books the sell's fills, cash effect and settlement on an order with
`orders.external = true`, but writes no position and no P&L: the journal has no cost basis for
those coins. It publishes `ExternalHoldingSold` (`order_row_id`, `symbol`, `qty`,
`dust_consumed`, `external_qty`, `proceeds_usd` gross, `fees_usd`), logs `positions.external_sold`,
and lists the sale in `flatten_result.external_sales`.

Account for it by hand: take the qty and proceeds from `flatten_result.external_sales` or from
the event, and the cost basis from your own records of the purchase.

```sql
SELECT ts, payload FROM events WHERE type = 'ExternalHoldingSold' ORDER BY id;
```

## Manual coins in a traded symbol (`oversell`)

**Prevention rule: never hold manual coins in a symbol the bot trades.**

A normal exit sells the whole free (`qty_available`) broker-reported holding, so it also sells
manual coins in that symbol. The ledger then sees a sell beyond the position's open qty: the `oversell` anomaly rolls
the booking back, sets the manual pause (one `LedgerAnomaly` event, `ledger.anomaly` error log) and
re-checks the row on every sync pass; while the pause stays set, the repeats write nothing. Resuming
alone does not recover: the next pass pauses again and writes another `LedgerAnomaly` event with the
same kind and detail (`ledger.anomaly_repaused` warning), and the row keeps blocking flat.

Recover with `repair oversell` (stop the core first):

```
uv run autotrader repair oversell <order_row> --evidence "..."
```

It needs a sell row that is unsettled and whose latest `LedgerAnomaly` is `oversell`. It sets
`orders.excess_external` and writes an `OperatorRepair` event; no balance is edited. On the next
core run the ledger books each fill of that row to the position up to what it can still absorb
(open qty plus carried dust), and the rest as external: the split is stored per fill
(`fills.external_qty`). The position closes, the external part is published as
`ExternalHoldingSold` (account for it by hand, see above), and no new anomaly is raised. The
manual pause stays set: resume it (see "Resuming after a pause") only after the row settled and
you recorded the broker-side facts (order, fills, how many manual coins).
