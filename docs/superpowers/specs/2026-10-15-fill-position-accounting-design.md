# Fill and position accounting design (WP 1.5b)

Date: 2026-10-15. Status: design approved in chat by the owner (sections 1–4); written spec in review (PR #24).

Revised 2026-10-15 after the first Codex review (PR #24), owner go:
- an order that can't be synced makes the sync incomplete, and `check()` rejects `state unavailable` (fail closed, PLAN §3.2 item 4) instead of skipping it;
- status and fill history are separate broker reads: fills are applied as seen, settlement needs a final status that matches, and `overfilled` is only a contradiction against a final status or the order qty;
- the cash check compares only across an unchanged order/fill generation, re-checked in the baseline transaction;
- a position closes only once its entry order is settled; updates are applied per symbol in order-row id order, and a missing earlier update defers the later ones;
- carried dust is a durable per-symbol balance that a later oversell consumes; an oversell beyond it is an anomaly.

Revised 2026-10-15 after the second Codex review (PR #24), owner go: an earlier order can have an update and still miss fills, so a sell's excess over the open qty is resolved (dust or `oversell`) only once every earlier order on the symbol is settled; until then the exit's update is deferred, not an anomaly.

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

1. **Unexplained cash change → manual-resume pause** (`control_state.paused = true`), not `paused_until`, not warn-only in paper. Cash that can't be explained means sizing and the daily-loss baseline can't be trusted; a new trading day fixes nothing. Exits stay allowed. The check compares only across a stable order/fill generation (section 4.6).
2. **Dust is written off at close.** A position closes when its remainder is below `min_qty` and its entry order is settled; realized P&L is the cash it actually returned minus the cash it cost, so the dust's cost is a loss at once. The dust goes into a per-symbol balance; dust later sold as part of another trade is consumed from that balance and its proceeds add to that trade. No journal position stays open for dust.
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
- add `dust_qty` (Money, nullable): the remainder written off at close (historical, never reduced).
- add `dust_consumed` (Money, not null, default 0): carried dust from earlier trades that this position's sells consumed (section 4.4).

`symbol_dust` (new table): `symbol` (PK), `qty` (Money, not null, ≥ 0), `updated_at`. The physical dust the journal believes the broker still holds per symbol. A close adds its `dust_qty`; a later oversell consumes from it in the same transaction.
- `stop`/`tp` are the entry order's `stop_price`/`tp_price` (original values).
- `exit_reason` vocabulary: `ExitReason` (`stop`, `take_profit`, `time`, `trailing`, `kill`, `flatten`, `stale`) plus `dust`, which only the ledger writes.

SPEC §7 gets a dated note listing these additions.

## 4. Components

### 4.1 Broker interface

- `Broker.order_fills(order_id: str) -> list[Fill]`: every execution of one order seen so far, in any order. SimBroker serves it from `_fill_log`. `AlpacaBroker` raises "not yet" until 1.6 (like its other order methods).
- `DustRemainder(OrderRejected)` in `broker/base.py` with `qty` and `min_qty` attributes. SimBroker's `close_position` raises it instead of the message-text rejection. Because it subclasses `OrderRejected`, existing handlers stay correct.
- `Fill.fill_id` already exists (WP 1.R2) and maps to `fills.broker_fill_id`.

### 4.2 `RiskEngine.sync_orders()` (read-only)

- Reads every unsettled order row with a `broker_order_id` (entries and exits), in order-row id order. For each: `get_order`; if the broker's `filled_qty` exceeds the row's applied `filled_qty`, also `order_fills`; and `asset_rules(symbol)` once per symbol touched, for `min_qty`.
- Returns `SyncResult(updates: list[OrderUpdate], failed: list[int])`, with `OrderUpdate(order_row_id, status: OrderStatus, fills: list[Fill], min_qty)` per order and `complete = not failed`.
- `status` and `fills` are **two separate broker reads**, not one snapshot: an execution can land between them, so the fills may be ahead of the status. Section 4.3 is written for that.
- Own `io_timeout_s` budget via `_within`; takes **neither** the entry nor the exit lock, so it never delays an exit.
- A failure for one order (including `OrderNotFound`) is logged (`risk.order_sync_failed`), recorded in `failed`, and the sync continues with the other orders. A spent budget records the remaining orders as failed and stops. The sync never raises for a per-order failure; the **caller** decides: `check()` applies the updates it got and then rejects `state unavailable` whenever `complete` is false (PLAN §3.2 item 4: an unavailable or missing broker order fails new entries closed). Resolving the failed order is 1.5d; until then entries stay blocked and exits stay available.
- Rows without a broker id are not queried. `not_submitted` rows, and `rejected` rows with no broker id, are handed to the ledger as zero-fill final updates (no broker call). `pending`/`unknown` rows without a broker id are left alone (1.5d resolver); they keep reserving.

### 4.3 `core/ledger.py`: `Ledger.apply_all(sync_result)` and `Ledger.apply(update)`

**Order of application.** `apply_all` groups the updates by symbol and applies each symbol's updates in order-row id order (an entry row always precedes the exits that sell its qty). If an earlier order row on the same symbol is unsettled and has no update in this pass (it failed in the sync, or it is a `pending`/`unknown` row without a broker id), every later update for that symbol is **deferred** to the next pass. A restart that sees an exit's history before its entry's history therefore never applies the exit first. An earlier update can also be present but **incomplete** (its fills not all visible yet); the sell-fill readiness rule below covers that case.

**One transaction per order update** through `EventBus.publish_tx`. `build` re-reads the order row, its applied fills, its position and the symbol's dust balance **inside the transaction** and computes the delta there, so the result does not depend on what the caller saw. The bus unit lock serialises all `publish_tx` units, so two concurrent `apply()` calls for the same order can't both apply a fill; the unique `broker_fill_id` constraint is the backstop (an `IntegrityError` rolls the unit back and the order is retried on the next pass).

**Status and fills are separate reads (section 4.2).** The ledger treats the fills as the record of what was applied and the status only as evidence of finality:
- every fill id not yet in the journal is applied as seen, even if the fills sum to more than the status's `filled_qty` (the status read is older than the fill read);
- the row's `status` only moves forward (`new` → `partially_filled` → final); an older update never moves `status` or `settled_at` back, and an update for an already settled row is dropped as stale;
- the order **settles** only when the status is final (`filled`, `canceled`, `rejected`; or local `not_submitted`) **and** its `filled_qty` equals the applied qty. Then `settled_at = now` and `status` = the final status.
- A final status with `filled_qty` above the applied qty means fills not yet visible: apply what is there, stay unsettled, retry next pass. Not an anomaly.

Per fill not yet in the journal:
- insert the `fills` row (by value); `orders.filled_qty += fill qty`;
- **buy fill of an entry order:** if the order has no position yet, open one: `cycle_id`, `symbol`, `side="long"`, `stop`/`tp` from the order, `initial_risk` from the approved entry decision of the order's cycle (latest approved `kind="entry"` decision for that cycle), `opened_at` = fill ts, and set `orders.position_id`. Then `qty += net` (net = `qty − fee` for `fee_asset="base"`, else `qty`), `entry_price` = gross VWAP of the order's buy fills (Alpaca's convention, informational), `fees += fee in USD` (a coin fee × its fill price). The entry's position stays open until the entry is settled (below), so a late entry fill always lands on an open position;
- **sell fill:** attach to the symbol's open journal position (check 5 guarantees at most one), set `orders.position_id`, `fees += fee`. A sell within the remaining qty applies at once (so a protective exit against a still-open partial entry is booked). If the fill qty **exceeds** the remaining qty, the excess is resolved only when every earlier order row on the symbol is **settled** (its full history applied):
  - **readiness not met** (an earlier order is unsettled, e.g. its status is final but some of its fills are not visible yet): the whole update is rolled back as **deferred** (logged `ledger.exit_deferred`, no anomaly, no dust touched) and retried on the next pass. Only the booking waits; the broker exit already happened. While deferred the exit stays unsettled (so the cash check skips) and the unsettled earlier entry keeps blocking its symbol. If the missing fills never appear, the earlier order never settles: 1.5d resolver / operator, not a pause;
  - **readiness met:** the excess is carried dust from earlier trades (section 4.4): consume it from `symbol_dust`, add it to `positions.dust_consumed`, and set the remaining qty to 0. An excess beyond the available `symbol_dust.qty` is an anomaly (`oversell`).

**Closing.** A position closes when **both** hold: its entry order is settled, and its remaining qty is below `min_qty`. The check runs whenever an order of that position settles:
- entry settles with no sells (the tiny partial IOC case, PLAN 1.5 row, PR #21 review): close as dust, `exit_reason="dust"`;
- exit settles while the entry is settled: close with the exit's `exit_reason`;
- exit settles while the entry is still unsettled: the position stays open (qty may be ~0); when the entry settles later, the close happens then, with the `exit_reason` of the latest settled exit. A late entry fill in between raises the open qty instead, and 1.5c protects it.
- remaining qty at or above `min_qty`: stays open; 1.5c exits again.

On settling an **entry** order the cycle completes per D15:
- cycle in `RISK_CHECK` or `SUBMITTED`: applied qty > 0 → `FILLED` with `details = {filled_qty, ordered_qty, partial}`; 0 → `EXPIRED` (`reason = final status`). Both via `check_edge(..., reconciliation=True)` and `repo.transition_cycle`.
- cycle terminal (`ERROR`, or already `FILLED` by the runner's immediate ack, …): no transition, ever (D15); the fills are still booked. Position lifetime is independent of cycle state.

Events: the unit commits one or more events atomically. `publish_tx`'s `build` may now return a tuple of events (first event drives `verify`, since all commit together): always `OrderSettled` or `FillsApplied` (`order_row_id`, `fills`, `position_id`, `status`), plus `CycleTransitioned` when the cycle moves, plus `DustLeft` (`symbol`, `position_id`, `dust_qty`, `min_qty`) when a close leaves dust. Each event carries the cycle id where there is one.

### 4.4 Closing a position and carried dust (decision 2)

- `closed_at` = ts of the last applied fill; `exit_reason` as in section 4.3.
- `realized_pnl = Σ over the position's sell fills (notional − USD fee) − Σ over its buy fills (notional + USD fee)`. Coin buy fees reduce qty, not cash, so they are not subtracted again (G10: never charge a coin deduction as a cash fee). The dust's cost is therefore a loss now.
- `dust_qty` = remaining qty at close (historical, never reduced later); add it to `symbol_dust.qty` in the same transaction; emit `DustLeft` if > 0.
- **Carried dust:** the broker merges dust into the next position on that symbol, and a full close sells it too. That sell's excess over the open qty is consumed from `symbol_dust` (section 4.3) and recorded in `dust_consumed`; its proceeds stay in that trade's sells, so they count toward that trade, as decided. An excess beyond the balance is `oversell`, never silently floored.
- Invariant: Σ open positions' qty + `symbol_dust.qty` = broker holdings, per symbol (exact on SimBroker).
- `strategy_trade_stats()` is unchanged: closed rows carry `realized_pnl`; a dust-only close is a losing trade.
- 1.6 note: Alpaca's coin fee and rounding must be known exactly (or a bounded allowance defined) before this invariant and `oversell` are applied to Alpaca fills (G10).

### 4.5 Anomalies

Each of these logs an error, rolls back the update, leaves the order unsettled, and, in a separate transaction, sets `paused = true` and publishes `LedgerAnomaly {kind, order_row_id, detail}`. Each is a contradiction against an authoritative bound, never a lag between two reads:
- `fill_mismatch`: a known `broker_fill_id` with different values;
- `overfilled`: the applied qty would exceed the order's qty, or a **final** status reports less `filled_qty` than is already applied (a final status is final);
- `fill_after_settle`: a new fill id for an order that is already settled;
- `sell_without_position`: a sell fill with no open journal position for its symbol, when no earlier unsettled order on that symbol exists (otherwise the update is deferred, section 4.3);
- `oversell`: a sell beyond the open qty plus the available `symbol_dust`, established only once every earlier order on the symbol is settled (otherwise deferred, section 4.3).

### 4.6 Cash check: `Ledger.cash_guard()` and `Ledger.check_cash(account, guard)` (decision 1)

- Baseline in `control_state["cash_check"] = {cash, fill_watermark, ts}` (`fill_watermark` = max `fills.id` at the baseline).
- **Generation guard.** `cash_guard()` returns `Guard(generation=(max orders.id, max fills.id), unsettled=any unsettled order)`. The caller reads it **before** the account read and passes it in. `check_cash` runs as one `publish_tx` unit and re-reads the guard **inside that transaction**; it compares (or writes the first baseline) only if the generation is unchanged and neither read saw an unsettled order. Otherwise it returns `skipped` and keeps the baseline.
- Why this is enough: an order that is submitted, filled and settled between the two reads still inserts an order row (its `pending` row precedes the submit), so `max orders.id` changes; a fill applied in between changes `max fills.id`. Ledger units after the second read can't interleave, because the bus lock serialises them with this unit; an order row inserted after the second read was created after the account read, so its fills are above the watermark and are not in the account's cash.
- Expected cash = baseline cash + Σ cash effects of fills with `id > fill_watermark`: buy `−notional` (and `−fee` if `fee_asset="quote"`), sell `+notional − fee`. Coin fees don't move cash.
- Tolerance `max(0.01, 0.01 × n_fills)` USD (Alpaca truncates cash to cents per fill; SimBroker is exact).
- Within tolerance: re-baseline to the actual cash and the current fill watermark (rounding doesn't accumulate).
- Outside: in the same transaction set `paused = true`, publish `CashDiscrepancy {expected, actual, diff, n_fills, baseline_ts}`, and re-baseline. The alert fires once per incident; the pause holds until an operator clears it.
- Not modelled: deposits/withdrawals (they also pause, correct until 3.5) and posted `CFEE` activities (the sell `CFEE` is already netted in the post-sell cash; 3.1).
- `RiskEngine.snapshot_equity()` returns the `Account` it read so a caller can feed this without a second broker call. Wiring the check into `run core` is 1.5d.

### 4.7 Engine changes

- `RiskEngine` takes a `Ledger`. `_refresh_open_entries` becomes: `sync_orders()` → `ledger.apply_all(result)` → read the reserving entry orders back → if `result.complete` is false, raise (so `check()` rejects `state unavailable`, PLAN §3.2 item 4). Any exception in that step also rejects `state unavailable` (fail closed, as today). Sync still runs before the positions read, so a fill the sync observed is booked and also in the positions read (never neither position nor entry).
- **Reservation (check 5 and exposure):** an entry order reserves while unsettled: `pending`, `unknown`, `new`, `partially_filled`, and final rows with `settled_at IS NULL`. Its `OpenEntry.notional = (qty − filled) × limit_price`, where `filled` is the larger of the sync's broker `filled_qty` and the applied `filled_qty` (a sync can only reach here complete, so every unsettled broker-id row has a current figure). A final unsettled row has notional 0 but still blocks its symbol. This fixes the 1.R2 double count (a `partially_filled` entry counted at full notional while its filled part is also a priced position).
- `exit()` writes `exit_reason` on the order row and maps `DustRemainder` to `OrderResult(..., status="rejected", dust=True)`. It never syncs.
- `OrderResult` gains `dust: bool = False`.
- `risk/` keeps 100 % branch coverage.
- 1.5c note: a position can be open with a qty below `min_qty` while its entry is unsettled; the Position Manager must treat that as unexitable (no exit retries on it) until the entry settles.

## 5. Error handling summary

| Situation | Behaviour |
|---|---|
| `get_order`/`order_fills` fails for one order | recorded as failed, other orders still synced; `check()` rejects `state unavailable`; exits unaffected |
| sync budget spent | remaining orders failed; `check()` rejects `state unavailable` |
| earlier order on a symbol has no update | later updates on that symbol deferred to the next pass |
| sell exceeds the open qty while an earlier order on the symbol is unsettled | exit update deferred (no dust consumed, no anomaly), retried next pass |
| fills ahead of the status / fills not yet visible | apply what is there, stay unsettled |
| stale update after a newer one | no regression of `status`/`settled_at`; settled rows dropped |
| ledger unit fails (DB error, `IntegrityError`) | `check()` rejects `state unavailable`; order retried next pass |
| `CommitUncertain` from the bus | propagates; the order stays unsettled until a later pass sees the committed state |
| anomaly (4.5) | roll back, manual pause, `LedgerAnomaly` |
| cash generation changed or an order unsettled | cash check skipped, baseline kept |
| cash outside tolerance | manual pause, `CashDiscrepancy`, re-baseline |

## 6. Testing

Ledger (SimBroker with `FillPlan`s): full entry → position + `FILLED`; partial IOC then cancel → `FILLED` with `partial`; zero-fill cancel → `EXPIRED`; tiny partial → dust close + `DustLeft` + `symbol_dust`; full exit → closed with exact P&L for `base` and `quote` buy fees; partial exit stays open; `ERROR` cycle with a fill stays `ERROR` and the fill is booked; runner-`FILLED` cycle gets no second transition; `not_submitted` settles without broker calls; redelivered fill (also with a different exponent) applied once; each anomaly pauses and leaves the order unsettled; invisible fills settle on a later pass; two racing `apply()` calls on one update apply it once (explicit interleaving).

Snapshot consistency (explicit interleavings): a fill lands between `get_order` and `order_fills` (fills ahead of status) → applied, not settled, no anomaly, settles next pass; a stale update applied after a newer one → no status/settlement regression.

Position lifetime: partial entry → protective exit settles → late entry fill → position still open with the late qty, closes only after the entry settles; restart catch-up where the exit's update is available but the entry's lookup fails → exit deferred, no `sell_without_position`; with both available, applied in id order. Separately: successful but incomplete entry history (status final with `filled_qty` 1, only 0.5 visible) followed by a fully visible exit selling 1, with an existing old-dust balance → first pass: exit deferred, no anomaly, dust balance unchanged; after the missing 0.5 is released → both settle, exact P&L, old-dust balance still unchanged.

Carried dust: close/re-entry/resale repeated over several trades keeps the invariant and assigns resale proceeds to the selling trade; an excess sell beyond the dust balance → `oversell`.

Engine: `partially_filled` entry reserves its remainder only; final unsettled order reserves 0 notional and blocks its symbol; one order's lookup fails, another succeeds → the successful update is booked and an entry on a **different** symbol is still rejected `state unavailable`; a ledger failure rejects `state unavailable`; `exit()` still works while the sync fails, returns `dust=True` on `DustRemainder` and writes `exit_reason`.

Cash check: first call baselines (only with a stable generation); within tolerance re-baselines; outside pauses + event + re-baselines; skipped while any order is unsettled; a complete submit → fill → settle between the guard and the check → skipped (generation changed), no false incident.

Property (hypothesis): for random fill plans on SimBroker, the broker cash change equals the ledger's Σ cash effects (the check never fires on SimBroker), and per symbol Σ open qty + `symbol_dust` equals broker holdings.

Migration 0004: upgrade/downgrade like the existing migration tests, including the refusal on a non-empty `fills` table and the settled backfill of id-less final rows.

## 7. Documentation alignment (same PR)

- PLAN: D17 (decisions above, including the generation-guarded cash check and the dust balance); dated 1.5b entry on the 1.5 row; 1.6 row: Alpaca must implement `order_fills` and populate `broker_fill_id`; 1.5 row follow-ups (a)–(c) marked done when implemented.
- SPEC §7: dated note on the new columns; the "WP 1.6 adds durable broker fill identity" sentence becomes "1.5b adds the column and the SimBroker path, 1.6 the Alpaca path".
- Stale claims to correct: `_refresh_open_entries` docstring ("WP 1.6 adds the fill stream"), the engine module docstring on late fills, `core/cycle.py` comments "WP 1.5 reconciles", the SimBroker docstring's dust message text, `broker/base.py` `Fill` docstring ("a consumer applies each fill once (WP 1.5)"). `rg` the claim wording, not only feature names.
