# Order resolver and journal repair design (WP 1.5d-2)

Date: 2026-11-10. Status: owner decisions in chat 2026-11-10, recorded as PLAN D20; this written
spec is in review. Choices made here that D20 does not fix are marked **(builder call)**.
Revised 2026-11-10 after the first Codex review (PR #33), owner go: total vs free qty (2.4),
journal-namespaced client ids as the ownership proof (2.3), the type (b) clock survives client-id
hits (3.7), one per-fill oversell allocation (5.4). A cold
builder check against the code ran before review; its findings are folded in (3.1, 3.2, 3.6, 3.8,
5.4).
Revised 2026-11-10 after the plan dry run and the PR #34 review (owner go, D20.10-11): the
incident hold-back is kept (3.6), clock clears and type (b) incidents are conditional on the clock
they observed (3.6, 3.7), `repair link` accepts type (b) `lookup_failed` (5.2, 8).

## 1. Scope and intent

Today an order row that is `pending`/`unknown` without a broker id is skipped by every order-sync
pass ([engine.py `_sync`](../../../src/autotrader/risk/engine.py)) and never retried. A row whose
`get_order` raises (including `OrderNotFound`) fails the sync on every pass. Both block entries on
the symbol, block flat, and hold reservations indefinitely. The only way out is the manual SQL
procedure in `docs/runbook.md` ("Resolving an unresolved order by hand"). An `oversell` anomaly
has no recovery at all: it re-pauses after every resume.

1.5d-2 delivers (PLAN 3.2 item 4, 1.5b follow-up b):

1. **Every order we send carries our `client_order_id`**, exits included (owner decision D20.1).
   Exits stop using `Broker.close_position` (section 2).
2. **A bounded resolver** in the existing order-sync pass that finds uncertain orders by client id,
   links them, and turns what it cannot resolve within the bound into an explicit operator incident
   (section 3).
3. **Repair CLI commands** for the operator: `repair link`, `repair not-submitted`,
   `repair oversell` (section 5; owner decision D20.3: CLI, not documented SQL).

Why: before 1.5d-3 lets the Cycle Runner place entries, a lost submit response must not freeze a
symbol forever or require hand-written SQL against the live journal. The design keeps the PLAN 3.2
item 4 rule: absence is never proof of non-submission, so nothing is ever auto-expired.

**Non-goals:**
- automatic `not_submitted` (section 3.5);
- repair commands for the other five anomaly kinds (`fill_mismatch`, `overfilled`,
  `fill_after_settle`, `sell_without_position`, `ambiguous_position`): they stay "investigate,
  then fix by hand" (D20.4);
- undoing a `not_submitted` mark (manual procedure only, section 6);
- the Alpaca implementation of the new broker method (1.6, with the other order methods);
- the startup sequence, Cycle Runner wiring, admission (1.5d-3); a resume command (Phase 3 API).

**Anti-goals** (a technically working result that would still be a failure):
- any path that marks an order `not_submitted`, or releases its reservation, on time plus a
  zero-match lookup alone;
- linking an order on a client-id hit without checking that it is the same order (a reused id from
  an older journal against the same broker account must not be booked);
- a repair command that edits balances, fills or positions directly instead of recording a decision
  the ledger then applies through its normal path.

## 2. Exits carry client ids (D20.1)

### 2.1 Engine

`RiskEngine.exit()` inserts the order row as today, then sets
`client_order_id = f"at-{jid}-o{row.id}"` (2.3) in the same transaction (flush to get the id),
and submits

```
submit_order(BrokerOrder(symbol, side="sell", qty=decision.qty, type="market",
                         time_in_force="gtc", client_order_id=row.client_order_id))
```

instead of `close_position(symbol)`. `decision.qty` is the position's **free** qty (2.4), not its
total. The 1.5c behaviour around it is unchanged: dust is still rejected by `check_exit` before
any row is written, a broker `OrderRejected` still records `rejected`, and the Position Manager's
retry rules still apply.

`close_position` is no longer called by the engine **(builder call: no fallback; I said "fallback
only" in chat, but `check_exit` already rejects dust before the call and the paper probe accepted
the exact 9-decimal net qty, so there is no case left for it to cover)**. It stays in the `Broker`
protocol (`close_all` and tests use it). `DustRemainder` handling in `_submit` stays: SimBroker's
`submit_order` raises it for a full-holding sell below `min_qty` (2.2).

Rationale and its limit: the Risk Engine spec (2026-10-11, section 5) chose `close_position`
because it "sells exactly what the broker holds" (coin-fee-reduced qty, precision). The 2026-10-12
paper probe sold the exact net `qty_available` (0.000117817 BTC, 9 decimals) with a plain market
sell and closed cleanly with no dust (`docs/alpaca-notes.md`, step 2). That is one BTC sample.
**WP 1.6 must confirm it per traded symbol on Alpaca paper** and map Alpaca's `qty` to
`Position.qty` and `qty_available` to `Position.qty_available` (2.4). Remaining difference from
`close_position`: the qty is read a moment before the send; if the position grows in between (a
late entry fill), the sell leaves a remainder the Position Manager already retries.

### 2.2 SimBroker

SimBroker currently rejects an off-increment sell unless it came from `close_position`
(`full_close`). Change: a `submit_order` sell whose qty equals the full free holding
(`held - pending sells`) is treated as a full close (increment check skipped, `full_close=True`,
`DustRemainder` if below `min_qty`), mirroring the paper probe. Any other off-increment sell is
still rejected.

### 2.3 Client id format: a journal namespace (revised after PR #33 review)

Row and cycle ids are unique only within one journal. A second journal (a reset, a test copy)
trading on the same broker account produces the same ids, and a reused id from the other journal
can match on symbol, side, qty and time, so a shape check cannot prove the order is ours (anti-goal
in section 1). Therefore:

- **Journal id:** a random 8-hex-char id generated once per journal and stored in `control_state`
  (`journal_id`). Migration 0007 writes it **(builder call: generated in the migration with
  `secrets.token_hex(4)`; never regenerated; 32 bits is ample for the handful of journals one
  account sees)**.
- **Format:** exits `at-{jid}-o{row_id}`, entries `at-{jid}-c{cycle_id}-e` (replacing D14's
  `at-c{cycle_id}-e`). Both fit the 64-char column.
- **Ownership proof:** a broker order belongs to a row only if its client id equals the row's
  client id exactly **and** that id carries this journal's `jid`. A row whose client id is NULL or
  not in this journal's namespace (legacy format) has no ownership proof (3.4, 5.2).

No real legacy rows exist: Alpaca has never placed an order through the bot (1.6), so old-format
rows exist only in SimBroker journals.

### 2.4 Total versus free qty (revised after PR #33 review)

Coins reserved by a pending sell are still held: they count toward exposure until the sell fills.
So the total and the free qty are kept apart:

- `Position` (broker type) gains `qty_available: Dec` (free to sell); `qty` stays the total held.
  SimBroker: `qty_available = held - pending sell remainders`. AlpacaBroker (1.6): Alpaca `qty`
  and `qty_available` one-to-one.
- `HeldPosition` gains `qty_available`. Exposure, the dust test (`is_dust`) and every entry check
  keep using `qty` (total), unchanged.
- `check_exit` sizes the exit with `qty_available`. If the holding is not dust but
  `qty_available < min_qty` (an earlier sell reserves it), `check_exit` rejects with reason
  `reserved by pending sells in <SYM>`: no order row, not dust, not `no_position`; the Position
  Manager's backoff retries **(builder call)**.

## 3. Resolver (PLAN 3.2 item 4)

### 3.1 Where it runs

Only in `sync_orders()` (the 2 s `ledger.sync` pass), under that pass's `io_timeout_s` budget,
after the normal reads. `_sync` is also called by `check()`'s `_gather_entry` under the entry
lock; that caller does **not** run the resolver, so an entry check never spends its budget on
client-id lookups and the two callers can never write the same link or incident concurrently
**(builder call: split `_sync` into the read step both callers use and a resolve step only
`sync_orders` runs)**. The link and incident writes are still idempotent: each is a conditional
update (`WHERE broker_order_id IS NULL` / `incident IS NULL`; type (a) writes also require
status `pending`/`unknown`, added 2026-11-11 after the PR #35 review) whose `publish_tx` verify step
publishes the event only if this unit changed the row. No new loop, no new lock. A row the
resolver cannot reach within the budget is "not looked up this pass", never a zero match.

### 3.2 Which rows

An unsettled order row enters resolution when either holds:
- **(a)** status `pending` or `unknown` and `broker_order_id IS NULL`, and the row is **not in
  flight** (3.3);
- **(b)** it has a `broker_order_id` and `get_order` raised `OrderNotFound` on this pass.

Other `get_order` exceptions (transport, auth, timeout) on a row with a broker id keep today's
behaviour (`failed`, entries fail closed) and are not resolution: the order is known, the read
failed.

Rows already marked as an incident (3.6) are skipped by the resolver **and by the normal
`get_order` read** (no more `risk.order_sync_failed` every pass for them).

**Blocking scope (owner decision D20.5):** while a type (b) row is still within its bound it stays
in `SyncResult.failed`, so `check()` rejects entries on every symbol (today's behaviour). Once it
becomes an incident it leaves `failed` and blocks only its own symbol, like a type (a) incident:
the existing unsettled-row rules (open entry, unsettled sell) cover it. Type (a) rows were never in
`failed` and keep blocking only their symbol.

**Note 2026-11-11 (PR #35 review):** the per-symbol scope holds only within one process. The cash
check is `skipped` while any row is unsettled (spec 2026-11-09 4.6), and a skipped check never
clears the engine's startup cash fault (`not yet reconciled`). So after a restart, which every
repair requires (5.1, core stopped), any open incident keeps `check()` rejecting entries on every
symbol (`cash reconciliation failed`) until all incidents are repaired, settled and one cash check
completes. This fails closed and is accepted as is; per-symbol blocking across restarts would need
a different cash check and is not in this WP.

### 3.3 In flight

`pending` is also the status of a row whose submit call is running. The engine keeps an in-memory
set of row ids between row insert and `_submit` returning (entries and exits), and the resolver
skips them. After a restart the set is empty, so every `pending` row is eligible: nothing in this
process is sending it.

### 3.4 Lookup and match

New broker method `get_order_by_client_id(client_order_id) -> BrokerOrderView | None` (section 4).
`None` means the broker answered and has no such order; any exception means "don't know".

- Row without ownership proof (client id NULL or not in this journal's namespace, 2.3): no lookup;
  it becomes an incident at once, reason `no_ownership_proof`.
- Found: ownership comes from the namespaced client id (2.3); the view must also match the row:
  same `symbol`, `side`, `qty`, and the broker's `created_at` no earlier than
  `row.submitted_at - 5 min` **(builder call: the skew bound, a consistency check, not the
  ownership proof)**. For a type (b) row the broker order id must also equal the row's. Match →
  link (3.7). Mismatch → incident at once, reason `client_id_mismatch` (a broker inconsistency).
  Never link a mismatch.
- `None`: zero match this pass. Keep waiting (3.5).
- Exception: not looked up this pass. Keep waiting.

### 3.5 The bound, and why there is no automatic `not_submitted`

Deadline: `row.submitted_at + order_resolution_timeout_s` (80 s: 10 s transport + 60 s visibility
allowance + 10 s final lookup, PLAN 3.2 item 4) for type (a) rows. For type (b) rows the clock
starts at the first `OrderNotFound`, persisted as `orders.resolve_since` so a restart cannot reset
it. Type (a) rows use `submitted_at`, already persisted. **(builder call: both bounds are
`RiskEngine` constructor parameters, `order_visibility_allowance_s=60`,
`order_resolution_timeout_s=80`, like `io_timeout_s`; not in `config.yaml` until 1.6 validates
them against Alpaca.)**

Before the visibility allowance (`submitted_at + 70 s`) a zero match is expected and logged at
debug. From then until the deadline the resolver keeps looking up every pass.

At or after the deadline, if the row is still unlinked (zero match, lookups failing, or never
reached), it becomes an incident, reason `not_found` or `lookup_failed` (the last outcome seen).

**No automatic `not_submitted`** (owner decision D20.2, from PLAN 3.2 item 4): a zero-match lookup
may establish `not_submitted` only with authoritative broker evidence that the order can never be
accepted or become visible. No broker we target offers that, and a lookup that finds nothing is
not such evidence. So the resolver never marks `not_submitted`; only an operator can, through
`repair not-submitted` (5.3).

### 3.6 Incident

Marking an incident writes, in one `publish_tx` unit: `orders.incident = <reason>` and
`orders.incident_at`, plus an `OrderIncident` event (`order_row_id`, `symbol`, `side`, `qty`,
`client_order_id`, `broker_order_id`, `reason`, `submitted_at`). Log `risk.order_incident`
(error) when it is marked, then the resolve step logs `risk.order_incident_open` (error,
`order_rows` with their reasons) every 60 s while any incident row is unsettled **(builder call:
the reminder lives in the engine's resolve step, not in the Position Manager; today
`positions.exit_unresolved` covers only id-less sells and `positions.entry_unresolved` logs once,
so neither is a reminder for every incident)**. `flatten_result.remaining.orders` gains the row's
`incident` reason, and its `unresolved` flag becomes true for any incident row.

The row keeps its status (`pending`/`unknown`, or its last known status for type (b)), stays
unsettled, keeps its reservation and keeps blocking entries on the symbol and flat. Nothing is
retried. The resolver does not look it up again: automatic investigation is bounded; the operator
takes over (5.2, 5.3).

**Note 2026-11-10 (plan dry run; owner decision: keep, D20.10):** because an incident row is never read
again, `Ledger.apply_all` holds back every later row on its symbol until the operator repairs it,
a later exit's fills included (the broker sold, the journal position stays open, the cash check
stays `skipped`). The hold-back predates 1.5d-2 (id-less rows); incidents make it indefinite.
It is kept (booking a later exit before an unresolved earlier entry gives anomalies or a wrong
P&L), pinned with a test and documented in the runbook.

### 3.7 Link

Linking a type (a) row writes `broker_order_id` and publishes `OrderResolved` (`order_row_id`,
`broker_order_id`, `client_order_id`, `how="resolver"`) in one unit, then logs
`risk.order_resolved`. The row is then read by the normal sync path on the same pass or the next,
which books its fills and settles it.

For a type (b) row a match writes nothing: it only proves the broker still knows the order under
our client id. **`resolve_since` is kept** (revised after PR #33 review). It is cleared only by a
successful normal `get_order` read of the row, by the row becoming an incident, or by a repair
command; a client-id hit never clears or restarts it. (2026-11-10, PR #34 review: a clear and a
type (b) incident each apply only while `resolve_since` still holds the value their decision
observed, checked in the write itself; a late clear never erases a newer clock, and an incident
decided on a clock that was cleared meanwhile is not written.) So if `get_order` keeps raising
`OrderNotFound` while the client-id lookup keeps finding the order, the row becomes an incident at
`resolve_since + order_resolution_timeout_s`, reason `lookup_inconsistent`, exactly once, across
restarts.

### 3.8 Effects elsewhere

- `cancel_open_entries` still returns id-less entry rows as `unresolved`; once linked they are
  cancelled normally.
- The Position Manager's exit logic needs no change: once linked and settled, its existing retry
  rules see the real outcome. `classify_sells` already holds exits back while any sell on the
  position is `unresolved` or `in_flight`, so a new exit is normally not sent while an earlier sell
  still reserves qty. If one is (flatten racing a pending sell), the exit is sized from
  `qty_available` (2.4) and so never oversells; a fully reserved holding is rejected before any
  row is written and the existing backoff retries once the earlier sell settles.
- `check()` keeps failing closed on the symbol while any row is unsettled (existing D19 and
  open-entry rules).

## 4. Broker protocol

```python
class BrokerOrderView(_Frozen):
    order_id: str
    client_order_id: str | None
    symbol: str
    side: Side
    qty: Dec
    status: OrderState
    filled_qty: Dec
    created_at: AwareDatetime

class Broker(Protocol):
    ...
    async def get_order_by_client_id(self, client_order_id: str) -> BrokerOrderView | None: ...
```

- SimBroker: implements it from `_orders`. Test hooks: `hide(client_order_id, until)` keeps an
  accepted order invisible to both lookups until the sim clock passes `until`; `fail_submit_after_accept`
  makes the next `submit_order` accept the order and then raise (a lost response). **(builder call:
  hook names and shape.)**
- AlpacaBroker: raises `OrderMethodsUnavailable` like the other order methods until 1.6. 1.6 maps
  it to Alpaca's lookup by client order id and must document whether Alpaca rejects a duplicate
  `client_order_id` on submit.

`repair link` also uses `get_order` (to read a broker id given by the operator), which needs the
symbol, side, qty and creation time too: it uses `get_order_by_client_id` when the row has a client
id, and otherwise a new `get_order_view(order_id) -> BrokerOrderView` **(builder call: or extend
`OrderStatus`; pick whichever touches fewer call sites)**.

## 5. Repair CLI (D20.3, D20.4)

### 5.1 Common rules

`autotrader repair <command> <order_row> --evidence "<text>" [--config config.yaml]`.

- **Core must be stopped.** Each command takes the single-writer journal lock
  (`core/lock.py`) for its whole run and fails with exit 1 (`core is running`) if it cannot. This
  is the "original worker is quiescent" condition of PLAN 3.2 item 4 and keeps the ledger from
  racing the repair.
- `--evidence` is required and non-empty: what the operator checked and when.
- Each command validates preconditions, prints the row before and the change it will make, and
  asks for confirmation (`--yes` skips the prompt) **(builder call)**.
- Each command writes its row change and an `OperatorRepair` event (`action`, `order_row_id`,
  `before`, `after`, `evidence`, `operator` = the local OS user name) in one `publish_tx` unit.
- Exit codes: 0 done, 1 refused (precondition or lock), 2 usage. Refusals print the reason.
- Repair never resumes the manual pause. The operator resumes it (runbook) after the next core run
  shows the row settled.

### 5.2 `repair link <row> <broker_order_id>`

For an unsettled row that either has no broker id and is `pending`/`unknown` (incident or not),
or is a type (b) incident (`not_found`, `lookup_failed`, `lookup_inconsistent`, `client_id_mismatch`
on a row with a broker id), whose broker id is then replaced (D20.5; `lookup_failed` added
2026-11-10 after the PR #34 review, D20.11: without it a type (b) row whose client-id lookups
failed until the deadline had no CLI repair at all). Connects the broker (read-only), reads the
order, and requires the ownership proof of 2.3 (the broker order's client id equals the row's
namespaced client id) plus the symbol, side, qty and 5-minute `created_at` checks of 3.4. A row
without ownership proof (`no_ownership_proof`: NULL or legacy client id) is refused: it goes to the
runbook's manual procedure, where the operator's own evidence is the proof (revised after PR #33
review). On match: writes `broker_order_id` (`before` keeps the old one),
clears `incident`, `incident_at`, `resolve_since`; `OperatorRepair(action="link")`. On mismatch it
refuses and prints the differences; there is no override **(builder call: no `--force`; a wrong
link books a foreign order)**. The next core run settles the row through the normal sync.

With `AlpacaBroker` this fails until 1.6 (`OrderMethodsUnavailable`), which is fine: Alpaca places
no orders before 1.6, so no such rows exist.

### 5.3 `repair not-submitted <row>`

For a row that is unsettled, has no broker id, and is `pending`/`unknown`. Sets status
`not_submitted` and clears the incident fields; `OperatorRepair(action="not_submitted")`. The next
sync pass settles it with zero fill through the existing `_LOCAL_FINAL` path, releasing the
reservation.

The prompt restates the PLAN 3.2 item 4 condition: only with authoritative broker evidence that
the order was never and will never be accepted; a zero-match search or a timeout does not qualify,
and the mark is final. The command cannot check the evidence; it records it.

### 5.4 `repair oversell <row>`

For a sell row whose latest `LedgerAnomaly` is `oversell` and that is still unsettled. Sets a new
flag `orders.excess_external = true`; `OperatorRepair(action="oversell")`.

Ledger change: **one per-fill allocation** for proceeds, fees, P&L and the event (revised after
PR #33 review; an order-wide ratio and a per-fill fee rule disagree when fills differ in price).
The split is persisted per fill when the fill is booked, never recomputed from memory or from a
final ratio:

- Fills are allocated in booking order (the order the ledger already books them). Each fill goes
  to the position first, up to what the position can still absorb: its open qty plus the carried
  dust (`SymbolDust`). Whatever of that fill is beyond it, and every later fill of the row, is
  external. The external part of each fill is stored in a new column `fills.external_qty`
  (0 for every ordinary fill).
- In `_sell`, with `excess_external` set on the row, the fill that crosses the position's capacity
  consumes all carried dust (as today: `dust_consumed`, `SymbolDust` reduced), sets
  `position.qty = 0`, and writes its `external_qty` instead of raising `oversell`. Later fills of
  the row are fully external (`external_qty = qty`).
- Every fill's position share is `qty - external_qty`. Fees: `_sell` adds to `position.fees` only
  `fee * (qty - external_qty) / qty` of each fill (today it adds the whole fee before the excess
  check). P&L: `_maybe_close` sums each linked fill's cash effect scaled by the same factor.
- At the row's settlement the ledger publishes `ExternalHoldingSold` from the same stored split:
  `external_qty = sum(fills.external_qty)`, `proceeds_usd = sum(price * external_qty)`,
  `fees_usd = sum(fee_usd * external_qty / qty)`, `dust_consumed = 0` (the dust went to the
  position). It does **not** call `_external_sale` (that path consumes `SymbolDust` itself and runs
  only for rows without a position; calling it here would take the dust twice).

Each fill's position part plus its external part equals the fill exactly (same rounding rule as
the ledger's other Decimal splits; any rounding residue goes to the external part **(builder
call)**).

No balance is edited by the command. The next sync pass after restart applies it; the pause stays
set until the operator resumes.

## 6. Schema (migration 0007)

`orders`:
- `incident` `String(24)` nullable: `no_ownership_proof`, `client_id_mismatch`, `not_found`,
  `lookup_failed`, `lookup_inconsistent`;
- `incident_at` `UTCDateTime` nullable;
- `resolve_since` `UTCDateTime` nullable (type b clock);
- `excess_external` bool, default false.

`fills`:
- `external_qty` Decimal, default 0 (5.4).

`control_state`: the `journal_id` row (2.3), written by the migration.

New event types: `OrderIncident`, `OrderResolved`, `OperatorRepair`.

Undoing a wrong `not_submitted` mark (its effects are already settled) stays manual: the runbook
gets the procedure, and the claim that "undoing it needs the 1.5d journal-repair tooling" is
corrected (section 7).

## 7. Docs (same PR as each change)

- `docs/runbook.md`: replace "Resolving an unresolved order by hand" with the resolver behaviour
  and the `repair link` / `repair not-submitted` procedure; replace the oversell "WP 1.5c ships no
  repair" paragraph with `repair oversell`; correct "until the 1.5d resolver exists" and "undoing it
  needs the 1.5d journal-repair tooling" claims; update "Current limits".
- Risk Engine spec (2026-10-11) section 5 and `exit()`'s docstring ("carries no client_order_id"):
  dated update note, exits now use `submit_order`.
- D14's entry client id `at-c{cycle_id}-e` (PLAN D14, `risk/checks.py`, Risk Engine spec): dated
  note that new ids carry the journal namespace (2.3).
- PLAN 1.5 row and D20; CLAUDE.md command list (`autotrader repair ...`).
- `docs/alpaca-notes.md`: 1.6 must verify the exact-qty sell per symbol and duplicate client-id
  behaviour.

## 8. Acceptance tests (SimBroker, no real API)

1. An exit sends `submit_order` with `client_order_id = at-<jid>-o<row>` and the broker's free
   qty; an off-increment full-holding sell after a coin-fee buy is accepted and leaves no dust.
   Entries use `at-<jid>-c<cycle>-e`; `jid` is stable across restarts of one journal.
2. Lost response (submit accepted, then raised): the row is `unknown`; the next pass finds it by
   client id, links it (`OrderResolved`), books its fills and settles it.
3. Hidden order: invisible for 50 s, then visible: linked, no incident.
4. Never visible: an incident at `submitted_at + 80 s` (reason `not_found`), not earlier; the row
   stays unsettled with its reservation and keeps blocking entries on the symbol.
5. Lookup raising until the deadline: incident `lookup_failed`; a raising lookup never counts as
   zero match before it.
6. Restart: a `pending` row from a previous process is resolved; the deadline still counts from
   `submitted_at` (type a) or `resolve_since` (type b).
7. In-flight row (submit still running) is never looked up.
8. Client-id hit with a different qty or a `created_at` before the bound: incident
   `client_id_mismatch`, never linked.
9. Rows without ownership proof (NULL client id, legacy `at-o<row>` / `at-c<cycle>-e` id):
   incident `no_ownership_proof` on the first pass, no lookup; `repair link` refuses them.
9a. Two journals on one SimBroker account (different `jid`), identical symbol/side/qty orders
    inside the 5-minute window, the second journal's submit response lost: the first journal's
    order is never linked, by the resolver or by `repair link`.
9b. Total vs free qty: an appreciated holding fully reserved by a pending sell still counts at its
    total qty in gross exposure, so a different-symbol entry that would exceed the exposure cap is
    rejected; an exit on the reserved symbol is rejected `reserved by pending sells` with no row.
10. Type (b): `get_order` raises `OrderNotFound` for a known id; `resolve_since` persisted;
    entries on every symbol rejected until the deadline; incident at the deadline, after which
    only its own symbol is blocked (until a restart, see the 3.2 note of 2026-11-11) and
    `get_order` is no longer called for it; `repair link`
    replaces its broker id. With client-id lookups failing through the deadline the incident is
    `lookup_failed`; after the broker recovers, `repair link` links it and the next sync settles it
    (added 2026-11-10, PR #34).
10a. Type (b) with `get_order` always `OrderNotFound` and the client-id lookup always matching,
    across the full deadline and a restart: `resolve_since` never resets; exactly one
    `lookup_inconsistent` incident.
11. `repair` refuses while the core holds the lock, with empty evidence, on a settled row, and on a
    link mismatch; each successful command writes exactly one `OperatorRepair` event with the row
    change in the same unit.
12. `repair not-submitted` then a sync pass: row settled with zero fill, reservation released.
13. `repair oversell`: position closes, dust consumed once, `ExternalHoldingSold` for the excess,
    no further `LedgerAnomaly`, pause still set. With two fills at different prices (one-coin
    position, sell 1 @ 100 then 1 @ 200): the first fill is the position's, the second external;
    position fees, P&L and the event's `proceeds_usd`/`fees_usd` each assert their own amounts
    (not only that cash is conserved). Booking the first fill, restarting, then booking the second
    gives the same amounts.
14. No path anywhere sets `not_submitted` from a zero-match lookup (grep-level check in review plus
    tests 4 and 5).

`risk/` keeps 100 % branch coverage.

## 9. Left to the builder

Everything marked **(builder call)** above, plus: exact log field names, CLI output wording,
whether `repair` lives in `cli.py` or a `cli_repair.py` module, and test file layout.
