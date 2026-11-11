# Operator runbook

Procedures for running the core by hand. It covers what `run core`, `autotrader flatten` and the
Position Manager (WP 1.5c) do today, their limits, and how to recover when something is stuck.
Requirements live in `SPEC.md`, delivery status in `PLAN.md`.

The examples assume the default `config.yaml` (journal `data/autotrader.db`, SQLite). Run SQL with
`sqlite3 data/autotrader.db`. Never delete journal rows; change only the columns a procedure names.

## Current limits (WP 1.5d-2)

- `run core` connects `AlpacaBroker`, which cannot place orders until WP 1.6: its order methods
  raise `OrderMethodsUnavailable` and `can_place_orders()` is false. The Position Manager still
  evaluates and latches triggers (`positions.exit_triggered`) and logs
  `positions.broker_cannot_trade` once, but sends no exits and no cancels. A flatten stays pending
  with `reason: "broker cannot place orders"`.
- `run core` places no entries until the Cycle Runner is wired in WP 1.5d-3 (it calls no
  `check()` yet). The rejection reasons in "Entries rejected" below are enforced by
  `RiskEngine.check()` and apply once it is wired.
- An order the engine cannot account for (no broker id, or one the broker no longer knows) is
  looked up by its client id for 80 s, then becomes an incident. The resolver never marks
  `not_submitted` by itself and never retries the order: the operator repairs it with
  `autotrader repair` (see "Resolving an order incident"). On `AlpacaBroker` the lookup, and so
  `repair link`, is unavailable until WP 1.6; no such rows exist before then.
- Recovering from an `oversell` anomaly is `autotrader repair oversell` (see "Manual coins in a
  traded symbol").
- An external sale that failed is retried only after the order-sync loop has settled the failed
  row, so external retries run every max(backoff, about 2 s).
- An entry on a symbol with an unsettled sell row (an external sale included) is rejected by
  `check()` (`unsettled sell on <SYM>`, see "Entries rejected"), so a partly filled external
  sale can no longer merge into a new trade's P&L.
- The flatten-state guards rely on SQLite's write lock. Postgres needs `SELECT ... FOR UPDATE`
  (WP 3.5 ownership work).

## Entries rejected

`RiskEngine.check()` rejects an entry with one of these reasons. Each reason in the table
below is a gather-time rejection: a persisted risk decision, no pause is set and no order row is
written (the pre-submit case after the table is different). Exits are never affected. `run core` calls
no `check()` until WP 1.5d-3, so today these describe what the engine will do.

| Reason | Meaning | Clears when |
|---|---|---|
| `daily-loss baseline unavailable` | No equity snapshot in [trading-day start - 120 s, trading-day start] (2 x the 60 s snapshot interval). | The next trading day that has such a snapshot. Nothing to fix. |
| `equity snapshot overdue` | The snapshot loop has not committed a snapshot yet in this process, or the last one is older than 120 s. | A snapshot loop commit (within 60 s once the loop is healthy). |
| `cash reconciliation failed` | The ledger cash check has not completed since startup, or its guard, snapshot or check stage raised. | A completed cash check with result baseline, ok or discrepancy (not `skipped`). After a `CommitUncertain` error it holds until the core is restarted. A completed check can also be `skipped`: the ledger skips the comparison when any order row has `settled_at` NULL (at either guard read) or the order/fill generation (`max(orders.id)`, fill watermark) moved between the reads. A skipped check neither sets nor clears the fault and logs no `ledger.cash_check_failed`, so after a restart one stuck unsettled row keeps entries rejected. Find it with `SELECT id, symbol, side, status FROM orders WHERE settled_at IS NULL;` and settle it (see below). |
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
(for example with Alpaca before WP 1.6): otherwise it latches `flatten` on every new position
after a resume.

`shutdown.flatten: true` in `config.yaml` runs the same request on every graceful stop after a
completed startup and waits up to half the remaining shutdown budget. On timeout it logs
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
free qty (`qty_available`) of the broker holding, not with `close_position`; a holding fully
reserved by pending sells rejects the exit (`reserved by pending sells in <SYM>`) until they
settle. A journal without a `journal_id` row (hand-deleted) fails entries closed
(`state unavailable`); exits still go out without a client id and log `risk.journal_id_missing`.

### What the resolver does

The ledger's 2 s sync pass (`RiskEngine.sync_orders`) resolves two kinds of unsettled row
(`check()` runs its own order read before an entry, without the resolver):
- (a) `pending`/`unknown` with no broker id and no submit call in flight;
- (b) a row with a broker id whose `get_order` raised `OrderNotFound`.

A row without a client id of this journal becomes a `no_ownership_proof` incident at once (no
lookup, no bound). Otherwise it looks the order up by client id. For (a), a match on symbol, side,
qty and creation time (5 minutes of skew) links the row (`risk.order_resolved`, `OrderResolved`
event; the next read books its fills and settles it). For (b), a match links nothing: the row
keeps its clock and becomes `lookup_inconsistent` at the bound. A zero match or a failed lookup
waits: the bound is 80 s from `submitted_at`
for (a), or from the first `OrderNotFound` (persisted as `resolve_since`, restart-safe) for (b).
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
| `no_ownership_proof` | The row has no client id of this journal (NULL or legacy form), so no lookup was made. | Manual procedure below. |
| `client_id_mismatch` | The broker has an order under our client id that differs from the row (symbol, side, qty, time, or broker id). A broker inconsistency. | Find out which is right; `repair link` only if it matches after all, else escalate. |
| `not_found` | Zero match until the bound. The order may still exist. | Investigate at the broker, then `repair link` (found) or `repair not-submitted` (authoritative evidence). |
| `lookup_failed` | The lookups raised until the bound (or the budget ran out), so nothing was learned. | Same as `not_found`. |
| `lookup_inconsistent` | (b) only: `get_order` says not found while the client-id lookup finds the order, until the bound. | `repair link` with the broker id the lookup reports. |

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
   5-minute `created_at` check. It accepts id-less `pending`/`unknown` rows and (b) incident rows
   (`not_found`, `lookup_failed`, `lookup_inconsistent`, `client_id_mismatch`; the broker id is
   then replaced, `before` keeps the old one). It refuses lookup misses, mismatches (the
   differences are printed) and rows without ownership proof; there is no `--force`. It writes
   `broker_order_id` and clears the incident fields. On `AlpacaBroker` it fails with
   `broker lookup failed: OrderMethodsUnavailable` until 1.6.
4. **Not found, with authoritative evidence:** `repair not-submitted <row>`, only for an unsettled
   `pending`/`unknown` row without a broker id. Use it only with what PLAN.md section 3.2 item 4
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
   stop. A row left unrepaired keeps reminding every 60 s and holding back its symbol; that is
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

With `AlpacaBroker` in 1.5d-2 no order is ever placed (`can_place_orders()` is false, nothing
places entries before 1.5d-3), so none of this arises in production yet.

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
