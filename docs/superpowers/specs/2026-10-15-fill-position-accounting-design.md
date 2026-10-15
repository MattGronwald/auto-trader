# Fill and position accounting design (WP 1.5b)

Date: 2026-10-15. Status: design approved in chat by the owner (sections 1–4); written spec awaiting review.

Sources: PLAN §3.2 items 3 and 6, the PLAN 1.5 row follow-ups (2026-10-13 WP 1.R2 final review, 2026-10-14 PR #21 review, 2026-10-14 fee re-query), D10, D14, D15, G10; SPEC §4.5 checks 5 and 7, §4.7, §7; `risk/engine.py`, `broker/base.py`, `broker/sim.py`, `core/cycle.py`, `core/bus.py`, `journal/models.py`, `journal/repo.py`.

WP 1.5 is split into four sub-PRs (owner, 2026-10-14): 1.5a runtime safety (done), **1.5b fill/position accounting (this spec)**, 1.5c exit management, 1.5d startup recovery/reconciliation and Cycle Runner wiring.

## 1. Scope

In scope:
- PLAN §3.2 item 3: settle orders whose fill effects are unapplied, regardless of cycle state; complete eligible cycles per D15 (any fill → `FILLED`, final zero-fill → `EXPIRED`); keep reservations until effects are applied.
- PLAN §3.2 item 6: positions and estimated fee-adjusted realized P&L on close; persist the initial-risk denominator; unexplained discrepancies pause entries.
- 1.R2 follow-ups on the PLAN 1.5 row: (a) open-entry notional = unfilled remainder; (b) a typed dust signal; (c) persist fill qty/prices by value; the partial-IOC dust case; flag an unexplained cash change and pause entries.

Out of scope (owner of each):
- Stop/TP/time/trailing/stale exits, exit retry, kill/flatten reporting: **1.5c**.
- Resolving `pending`/`unknown`/missing orders (§3.2 item 4), startup reconciliation, a periodic sync loop, the cash check in `run core`, Cycle Runner wiring: **1.5d**.
- Alpaca `order_fills`, `get_order`, fill ids: **1.6**.
- Posted `CFEE` activity reconciliation, R-multiple, MAE/MFE: **3.1**. Deposits/withdrawals (cash-flow discontinuities): **3.5**.

1.5b ships as library code plus engine methods, tested against SimBroker. Nothing new runs in `run core`; until 1.5d, `check()` is the only caller of the order sync.

## 2. Owner decisions (2026-10-15, recorded as D17)

1. **Unexplained cash change → manual-resume pause** (`control_state.paused = true`), not `paused_until`, not warn-only in paper. Cash that can't be explained means sizing and the daily-loss baseline can't be trusted; a new trading day fixes nothing. Exits stay allowed.
2. **Dust is written off at close.** A position closes when its remainder is below `min_qty`; realized P&L is the cash it actually returned minus the cash it cost, so the dust's cost is a loss at once. Dust later sold as part of another trade adds to that trade's proceeds. No journal position stays open for dust.
3. **Pull per order.** Fills are pulled per unsettled order (`get_order` for status and `filled_qty`, `order_fills` for executions), deduplicated by a unique broker fill id. Live operation and restart catch-up share one path; `stream_fills()` stays unused (it may become a latency optimisation later, never a second source of truth).

## 3. Data model (migration 0004)

`fills`:
- add `broker_fill_id` (String(64), **unique, not null**), `side` (`buy`/`sell`), `fee_asset` (`base`/`quote`, check constraints).
- `fee` stays in the asset that paid it, exactly as the broker reported it.
- `qty`, `price`, `fee` are written **by value**: normalised before the write, compared by value on redelivery, so `Decimal("1.50")` and `Decimal("1.5")` are the same fill. (The `Money` column type stays as it is.)
- The table is empty in every journal today (nothing writes it), so `not null` needs no backfill. The migration still refuses to run if rows exist, rather than invent ids.

`orders`:
- add `filled_qty` (Money, not null, default 0): the sum of **applied** fill qty, never the broker's figure.
- add `settled_at` (UTCDateTime, nullable): set when the order's effects are fully applied (section 4.3). NULL = unsettled = still reserving.
- add `position_id` (FK `positions.id`, nullable): set when the ledger first applies a fill of the order. Entry and exit orders of one trade share it; kill/flatten exits have no cycle, so cycle id can't link them.
- add `exit_reason` (String(16), nullable): `ExitRequest.reason`, written by `exit()`.
- Existing final rows (`filled`/`canceled`/`rejected`/`not_submitted`) from before the migration: rows with no `broker_order_id` get `settled_at = updated_at`; rows with a broker id stay unsettled so the first sync books them (paper rows from runs before 1.5b could not exist with fills applied, since nothing applied fills).

`positions`:
- add `initial_risk` (Money, nullable): from the entry decision's `adjusted["initial_risk"]`; never changed by trailing (§3.2 item 6). Nullable for positions not opened by a `check()` decision.
- add `dust_qty` (Money, nullable): the remainder written off at close.
- `stop`/`tp` are the entry order's `stop_price`/`tp_price` (original values).
- `exit_reason` vocabulary: `ExitReason` (`stop`, `take_profit`, `time`, `trailing`, `kill`, `flatten`, `stale`) plus `dust`, which only the ledger writes.

SPEC §7 gets a dated note listing these additions.

## 4. Components

### 4.1 Broker interface

- `Broker.order_fills(order_id: str) -> list[Fill]`: every execution of one order seen so far, in any order. SimBroker serves it from `_fill_log`. `AlpacaBroker` raises "not yet" until 1.6 (like its other order methods).
- `DustRemainder(OrderRejected)` in `broker/base.py` with `qty` and `min_qty` attributes. SimBroker's `close_position` raises it instead of the message-text rejection. Because it subclasses `OrderRejected`, existing handlers stay correct.
- `Fill.fill_id` already exists (WP 1.R2) and maps to `fills.broker_fill_id`.

### 4.2 `RiskEngine.sync_orders()` (read-only)

- Reads every unsettled order row with a `broker_order_id` (entries and exits). For each: `get_order`; if the broker's `filled_qty` exceeds the row's applied `filled_qty`, also `order_fills`; and `asset_rules(symbol)` once per symbol touched, for `min_qty`.
- Returns `OrderUpdate(order_row_id, status: OrderStatus, fills: list[Fill], min_qty)` per order.
- Own `io_timeout_s` budget via `_within`; takes **neither** the entry nor the exit lock, so it never delays an exit.
- A failure for one order (including `OrderNotFound`) is logged (`risk.order_sync_failed`) and that order is skipped: it stays unsettled and keeps reserving. Resolving it is 1.5d. A spent budget raises `TimeoutError` to the caller.
- Rows without a broker id are not queried. `not_submitted` rows, and `rejected` rows with no broker id, are handed to the ledger as zero-fill final updates (no broker call). `pending`/`unknown` rows without a broker id are left alone (1.5d resolver).

### 4.3 `core/ledger.py`: `Ledger.apply(update)`

One transaction per order update through `EventBus.publish_tx`. `build` re-reads the order row, its applied fills and its position **inside the transaction** and computes the delta there, so the result does not depend on what the caller saw. The bus unit lock serialises all `publish_tx` units, so two concurrent `apply()` calls for the same order can't both apply a fill; the unique `broker_fill_id` constraint is the backstop (an `IntegrityError` rolls the unit back and the order is retried on the next pass).

Per fill not yet in the journal:
- insert the `fills` row (by value);
- **buy fill of an entry order:** if the order has no position yet, open one: `cycle_id`, `symbol`, `side="long"`, `stop`/`tp` from the order, `initial_risk` from the approved entry decision of the order's cycle (latest approved `kind="entry"` decision for that cycle), `opened_at` = fill ts. Then `qty += net` (net = `qty − fee` for `fee_asset="base"`, else `qty`), `entry_price` = gross VWAP of the order's buy fills (Alpaca's convention, informational), `fees += fee in USD` (a coin fee × its fill price);
- **sell fill:** attach to the symbol's open journal position (check 5 guarantees at most one), set `orders.position_id`, `qty -= fill qty` (floored at 0, section 4.4), `fees += fee`;
- `orders.filled_qty += fill qty`.

A fill id already in the journal with equal values is skipped. Settling:
- An order is **settled** when the broker status is final (`filled`, `canceled`, `rejected`; or local `not_submitted`) **and** applied `filled_qty` equals the broker's `filled_qty`. Then `settled_at = now` and the row's `status` is the broker status.
- Not yet final, or fills not yet visible (`order_fills` returned less than `filled_qty`): apply what is there, update `status`, stay unsettled. Not an anomaly; the next pass retries.

On settling an **entry** order:
- cycle in `RISK_CHECK` or `SUBMITTED`: applied qty > 0 → `FILLED` with `details = {filled_qty, ordered_qty, partial}`; 0 → `EXPIRED` (`reason = final status`). Both via `check_edge(..., reconciliation=True)` and `repo.transition_cycle`.
- cycle terminal (`ERROR`, or already `FILLED` by the runner's immediate ack, …): no transition, ever (D15); the fills are still booked.
- the position's net qty < `min_qty`: close it immediately as dust (section 4.4, `exit_reason="dust"`). This is the tiny partial IOC case (PLAN 1.5 row, PR #21 review).

On settling an **exit** order: if the position's remaining qty < `min_qty`, close it (section 4.4) with `exit_reason` from the order. Otherwise it stays open with the reduced qty; 1.5c exits again.

Events: the unit commits one or more events atomically. `publish_tx`'s `build` may now return a tuple of events (first event drives `verify`, since all commit together): always `OrderSettled` or `FillsApplied` (`order_row_id`, `fills`, `position_id`, `status`), plus `CycleTransitioned` when the cycle moves, plus `DustLeft` (`symbol`, `position_id`, `dust_qty`, `min_qty`) when a close leaves dust. Each event carries the cycle id where there is one.

### 4.4 Closing a position (decision 2)

- `closed_at` = ts of the last applied fill; `exit_reason` as above.
- `realized_pnl = Σ over the position's sell fills (notional − USD fee) − Σ over its buy fills (notional + USD fee)`. Coin buy fees reduce qty, not cash, so they are not subtracted again (G10: never charge a coin deduction as a cash fee). The dust's cost is therefore a loss now.
- `dust_qty` = remaining qty, floored at 0; emit `DustLeft` if > 0.
- **Carried dust:** after an earlier close left dust, the broker merges it into the next position on that symbol, and a full close sells it too. The later position's remaining qty would go negative; it is floored at 0 and the extra proceeds count toward that trade.
- `strategy_trade_stats()` is unchanged: closed rows carry `realized_pnl`; a dust-only close is a losing trade.

### 4.5 Anomalies

Each of these logs an error, rolls back the update, leaves the order unsettled, and, in a separate transaction, sets `paused = true` and publishes `LedgerAnomaly {kind, order_row_id, detail}`:
- `fill_mismatch`: a known `broker_fill_id` with different values;
- `overfilled`: applied qty would exceed the broker's `filled_qty`;
- `sell_without_position`: a sell fill with no open journal position for its symbol.

### 4.6 `Ledger.check_cash(account, unsettled_before, unsettled_after)` (decision 1)

- Baseline in `control_state["cash_check"] = {cash, fill_watermark, ts}` (`fill_watermark` = max `fills.id` at the baseline). The first call only writes it.
- **No false alarms from lag:** the caller reads "any unsettled order?" before and after the account read; the check compares only if both are false. A `pending` row is written before submit and a fill can't precede its submit, so every fill reflected in broker cash is then applied. Otherwise it returns `skipped` and keeps the baseline.
- Expected cash = baseline cash + Σ cash effects of fills with `id > fill_watermark`: buy `−notional` (and `−fee` if `fee_asset="quote"`), sell `+notional − fee`. Coin fees don't move cash.
- Tolerance `max(0.01, 0.01 × n_fills)` USD (Alpaca truncates cash to cents per fill; SimBroker is exact).
- Within tolerance: re-baseline to the actual cash (rounding doesn't accumulate).
- Outside: in one transaction set `paused = true`, publish `CashDiscrepancy {expected, actual, diff, n_fills, baseline_ts}`, and re-baseline to actual cash. The alert fires once per incident; the pause holds until an operator clears it.
- Not modelled: deposits/withdrawals (they also pause, correct until 3.5) and posted `CFEE` activities (the sell `CFEE` is already netted in the post-sell cash; 3.1).
- `RiskEngine.snapshot_equity()` returns the `Account` it read so a caller can feed this without a second broker call. Wiring the check into `run core` is 1.5d.

### 4.7 Engine changes

- `RiskEngine` takes a `Ledger`. `_refresh_open_entries` becomes: `sync_orders()` → `ledger.apply(u)` for each update → read the reserving entry orders back. Any exception in that step makes `check()` reject with `state unavailable` (fail closed, as today). Sync still runs before the positions read, so a fill the sync observed is booked and also in the positions read (never neither position nor entry).
- **Reservation (check 5 and exposure):** an entry order reserves while unsettled: `pending`, `unknown`, `new`, `partially_filled`, and final rows with `settled_at IS NULL`. Its `OpenEntry.notional = (qty − broker filled_qty) × limit_price`, where broker filled_qty is the sync's figure (falling back to the applied `filled_qty` for rows the sync skipped or could not query). A final unsettled row has notional 0 but still blocks its symbol. This fixes the 1.R2 double count (a `partially_filled` entry counted at full notional while its filled part is also a priced position).
- `exit()` writes `exit_reason` on the order row and maps `DustRemainder` to `OrderResult(..., status="rejected", dust=True)`. It never syncs.
- `OrderResult` gains `dust: bool = False`.
- `risk/` keeps 100 % branch coverage.

## 5. Error handling summary

| Situation | Behaviour |
|---|---|
| `get_order`/`order_fills` fails for one order | skip it, log, order keeps reserving |
| sync budget spent | `TimeoutError` → `check()` rejects `state unavailable` |
| ledger unit fails (DB error, `IntegrityError`) | `check()` rejects `state unavailable`; order retried next pass |
| `CommitUncertain` from the bus | propagates; the order stays unsettled until a later pass sees the committed state |
| fills not yet visible | apply what is there, stay unsettled |
| anomaly (4.5) | roll back, manual pause, `LedgerAnomaly` |
| cash outside tolerance | manual pause, `CashDiscrepancy`, re-baseline |

## 6. Testing

Ledger (SimBroker with `FillPlan`s): full entry → position + `FILLED`; partial IOC then cancel → `FILLED` with `partial`; zero-fill cancel → `EXPIRED`; tiny partial → dust close + `DustLeft`; full exit → closed with exact P&L for `base` and `quote` buy fees; partial exit stays open; carried dust adds to the next trade; `ERROR` cycle with a fill stays `ERROR` and the fill is booked; runner-`FILLED` cycle gets no second transition; `not_submitted` settles without broker calls; redelivered fill (also with a different exponent) applied once; each anomaly pauses and leaves the order unsettled; invisible fills settle on a later pass; two racing `apply()` calls on one update apply it once (explicit interleaving).

Engine: `partially_filled` entry reserves its remainder only; final unsettled order reserves 0 notional and blocks its symbol; one order's `get_order` failure doesn't abort the sync; a ledger failure rejects `state unavailable`; `exit()` returns `dust=True` on `DustRemainder` and writes `exit_reason`.

Cash check: first call baselines; within tolerance re-baselines; outside pauses + event + re-baselines; skipped while any order is unsettled; interleaving: an order submitted between the two reads → skipped.

Property (hypothesis): for random fill plans on SimBroker, the broker cash change equals the ledger's Σ cash effects (the check never fires on SimBroker), and journal open qty + written-off dust equals broker holdings.

Migration 0004: upgrade/downgrade like the existing migration tests, including the refusal on a non-empty `fills` table and the settled backfill of id-less final rows.

## 7. Documentation alignment (same PR)

- PLAN: D17 (decisions above); dated 1.5b entry on the 1.5 row; 1.6 row: Alpaca must implement `order_fills` and populate `broker_fill_id`; 1.5 row follow-ups (a)–(c) marked done when implemented.
- SPEC §7: dated note on the new columns; the "WP 1.6 adds durable broker fill identity" sentence becomes "1.5b adds the column and the SimBroker path, 1.6 the Alpaca path".
- Stale claims to correct: `_refresh_open_entries` docstring ("WP 1.6 adds the fill stream"), the engine module docstring on late fills, `core/cycle.py` comments "WP 1.5 reconciles", the SimBroker docstring's dust message text, `broker/base.py` `Fill` docstring ("a consumer applies each fill once (WP 1.5)"). `rg` the claim wording, not only feature names.
