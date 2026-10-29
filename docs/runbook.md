# Operator runbook

Procedures for running the core by hand. It covers what `run core`, `autotrader flatten` and the
Position Manager (WP 1.5c) do today, their limits, and how to recover when something is stuck.
Requirements live in `SPEC.md`, delivery status in `PLAN.md`.

The examples assume the default `config.yaml` (journal `data/autotrader.db`, SQLite). Run SQL with
`sqlite3 data/autotrader.db`. Never delete journal rows; change only the columns a procedure names.

## Current limits (WP 1.5c)

- `run core` connects `AlpacaBroker`, which cannot place orders until WP 1.6: its order methods
  raise `OrderMethodsUnavailable` and `can_place_orders()` is false. The Position Manager still
  evaluates and latches triggers (`positions.exit_triggered`) and logs
  `positions.broker_cannot_trade` once, but sends no exits and no cancels. A flatten stays pending
  with `reason: "broker cannot place orders"`.
- `run core` places no entries until the Cycle Runner is wired in WP 1.5d.
- An exit or entry left `pending`/`unknown` without a broker order id is never retried and never
  resolved automatically until the 1.5d resolver. Use the manual procedure below.
- Recovering from an `oversell` anomaly needs journal-repair tooling that WP 1.5d owns (see below).
- An external sale that failed is retried only after the order-sync loop has settled the failed
  row, so external retries run every max(backoff, about 2 s).
- An entry on a symbol whose external sale is partly filled and still unsettled would merge that
  sale into the new trade's P&L, with no `ExternalHoldingSold`. It cannot happen in 1.5c (no
  entries are placed); WP 1.5d must block or defer such an entry before the Cycle Runner is wired.
- The flatten-state guards rely on SQLite's write lock. Postgres needs `SELECT ... FOR UPDATE`
  (WP 3.5 ownership work).

## Resuming after a pause

These set the manual pause (`control_state.paused = true`):
- flatten (`autotrader flatten`, `shutdown.flatten`);
- a ledger anomaly (`ledger.anomaly`), re-asserted on every sync pass while it persists
  (`ledger.anomaly_repaused`), so fix the journal first;
- an unexplained cash change (`CashDiscrepancy`);
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
- `orders`: `[{order_row_id, symbol, side, status, age_s, unresolved, anomaly}]`, every unsettled
  order row. `unresolved` is true for a `pending`/`unknown` row without a broker id; `anomaly` is
  the row's latest ledger anomaly kind, or null.

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

## Resolving an unresolved order by hand

Symptoms: `positions.exit_unresolved` (every 60 s, with `order_rows`), `positions.entry_unresolved`,
or `"unresolved": true` in `flatten_result.remaining.orders`. The row is `pending` or `unknown`
with no `broker_order_id`: the request may or may not have reached the broker. The order-sync loop
skips such rows and the Position Manager never retries them, so until the 1.5d resolver exists an
operator decides.

1. Stop the core (Ctrl-C) so nothing writes while you work.
2. Read the row:
   ```sql
   SELECT id, symbol, side, qty, status, submitted_at, client_order_id, external
   FROM orders WHERE id = <row>;
   ```
3. Look for the order at the broker. Entries carry `client_order_id`; exits carry none
   (`close_position` takes no id), so match an exit by symbol, side `sell`, qty and a time close
   to `submitted_at`.
4. Record the outcome on the row:
   - **Found at the broker:** set its id and leave the status; the next sync pass reads the order,
     books its fills and settles it.
     ```sql
     UPDATE orders SET broker_order_id = '<broker id>' WHERE id = <row>;
     ```
   - **Not found at the broker:** leave the row unresolved (`pending` / `unknown`, no
     `broker_order_id`) and escalate it as an operator incident. Absence is not proof: most of
     these rows are `unknown`, which the engine records after a timeout or a network error,
     exactly the case where the order may have reached the broker and is not visible yet. Waiting
     alone does not change that. Wait at least 15 minutes after `submitted_at` before concluding
     anything (an investigation delay, nothing more), then record what you checked, each with
     the time you checked it: the broker's full order history, open and closed (entries by
     `client_order_id`, exits by symbol, side, qty and time); fills and activities for the symbol
     from `submitted_at` on; the broker position qty against the journal (the open journal
     position's qty plus any `symbol_dust`). If the broker position changed by the row's qty,
     the order executed: find its broker id and use the **Found** branch.

     The row may be marked `not_submitted` only with what PLAN.md section 3.2 item 4 requires:
     the original worker is quiescent (the core is stopped, step 1) **and** authoritative broker
     evidence rules out that the order was or will ever be accepted or become visible, such as a
     broker-confirmed final rejection or non-acceptance status for that request. A grace window
     plus a zero-match lookup does not qualify, and neither does an HTTP timeout. A
     broker-confirmed final status normally comes with a broker order id, which is the
     **Found** branch, not this one. The mark is final and irreversible: the sync loop settles
     the row at once with zero fill (`settled_at` set) and never reads it again, even if a broker
     id is added later; undoing it needs the 1.5d journal-repair tooling. A wrong mark books
     permanently wrong (an exit that filled leaves the journal position open against a flat
     broker, so `positions.exit_failing` repeats and flat never completes; an entry that filled
     makes its coins look like an external holding that the next flatten sells with no P&L).
     Only with that authoritative evidence in hand:
     ```sql
     -- ONLY with authoritative broker evidence of non-acceptance (PLAN.md 3.2 item 4)
     UPDATE orders SET status = 'not_submitted' WHERE id = <row>;
     ```

     The cost of leaving the row unresolved, stated plainly: until the 1.5d resolver exists it
     blocks flat, retries for that position and later bookings on that symbol. It books nothing
     wrong, and reservations stay in place.
5. Start the core again. After a **Found** update (or the fenced mark), confirm the row settles
   (`settled_at` set) and the alert stops. A row left unresolved keeps alerting every 60 s until
   the incident is resolved; that is expected.

With `AlpacaBroker` in 1.5c this cannot arise in production: `can_place_orders()` is false, so
the Position Manager attempts no exits or cancels, and nothing places entries before 1.5d. A
direct engine call would still fail with `OrderMethodsUnavailable` before anything is sent and be
recorded `not_submitted`. The sync loop cannot read Alpaca orders yet either.

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

A normal exit closes the whole broker-reported position, so it also sells manual coins in that
symbol. The ledger then sees a sell beyond the position's open qty: the `oversell` anomaly rolls
the booking back, sets the manual pause (one `LedgerAnomaly` event, `ledger.anomaly` error log) and
re-asserts the pause on every sync pass. Resuming alone does not recover: the next pass pauses again
(`ledger.anomaly_repaused`), and the row keeps blocking flat.

WP 1.5c ships no repair for this. The journal-repair tooling is WP 1.5d's (PLAN 1.5 row, 1.5b
follow-up b). Until then, leave the pause set, keep the core's other symbols running, and record
the broker-side facts (order, fills, how many manual coins) for the repair.
