# Order resolver and journal repair design (WP 1.5d-2)

Date: 2026-11-10. Status: owner decisions in chat 2026-11-10, recorded as PLAN D20; this written
spec is in review. Choices made here that D20 does not fix are marked **(builder call)**. A cold
builder check against the code ran before review; its findings are folded in (3.1, 3.2, 3.6, 3.8,
5.4).

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
`client_order_id = f"at-o{row.id}"` in the same transaction (flush to get the id) **(builder
call: the format; it only has to be unique per journal and fit 64 chars)**, and submits

```
submit_order(BrokerOrder(symbol, side="sell", qty=decision.qty, type="market",
                         time_in_force="gtc", client_order_id=row.client_order_id))
```

instead of `close_position(symbol)`. `decision.qty` stays `check_exit`'s broker position qty. The
1.5c behaviour around it is unchanged: dust is still rejected by `check_exit` before any row is
written, an `OrderRejected` (for example "qty exceeds free qty" because an earlier sell still
reserves it) still records `rejected`, and the Position Manager's retry rules still apply.

`close_position` is no longer called by the engine **(builder call: no fallback; I said "fallback
only" in chat, but `check_exit` already rejects dust before the call and the paper probe accepted
the exact 9-decimal net qty, so there is no case left for it to cover)**. It stays in the `Broker`
protocol (`close_all` and tests use it). `DustRemainder` handling in `_submit` stays: SimBroker's
`submit_order` raises it for a full-holding sell below `min_qty` (2.2).

Rationale and its limit: the Risk Engine spec (2026-10-11, section 5) chose `close_position`
because it "sells exactly what the broker holds" (coin-fee-reduced qty, precision). The 2026-10-12
paper probe sold the exact net `qty_available` (0.000117817 BTC, 9 decimals) with a plain market
sell and closed cleanly with no dust (`docs/alpaca-notes.md`, step 2). That is one BTC sample.
**WP 1.6 must confirm it per traded symbol on Alpaca paper** and map `Position.qty` from Alpaca's
`qty_available` (not `qty`). Remaining difference from `close_position`: the qty is read a moment
before the send; if the position grows in between (a late entry fill), the sell leaves a remainder
the Position Manager already retries.

### 2.2 SimBroker

SimBroker currently rejects an off-increment sell unless it came from `close_position`
(`full_close`). Change: a `submit_order` sell whose qty equals the full free holding
(`held - pending sells`) is treated as a full close (increment check skipped, `full_close=True`,
`DustRemainder` if below `min_qty`), mirroring the paper probe. Any other off-increment sell is
still rejected.

### 2.3 Entries

Entries keep `at-c{cycle_id}-e` (D14). No change.

## 3. Resolver (PLAN 3.2 item 4)

### 3.1 Where it runs

Only in `sync_orders()` (the 2 s `ledger.sync` pass), under that pass's `io_timeout_s` budget,
after the normal reads. `_sync` is also called by `check()`'s `_gather_entry` under the entry
lock; that caller does **not** run the resolver, so an entry check never spends its budget on
client-id lookups and the two callers can never write the same link or incident concurrently
**(builder call: split `_sync` into the read step both callers use and a resolve step only
`sync_orders` runs)**. The link and incident writes are still idempotent: each is a conditional
update (`WHERE broker_order_id IS NULL` / `incident IS NULL`) whose `publish_tx` verify step
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

### 3.3 In flight

`pending` is also the status of a row whose submit call is running. The engine keeps an in-memory
set of row ids between row insert and `_submit` returning (entries and exits), and the resolver
skips them. After a restart the set is empty, so every `pending` row is eligible: nothing in this
process is sending it.

### 3.4 Lookup and match

New broker method `get_order_by_client_id(client_order_id) -> BrokerOrderView | None` (section 4).
`None` means the broker answered and has no such order; any exception means "don't know".

- Row without a client id (legacy exits from before this WP): no lookup possible; it becomes an
  incident at once, reason `no_client_id`.
- Found: the view must match the row: same `symbol`, `side`, `qty`, and the broker's `created_at`
  no earlier than `row.submitted_at - 5 min` **(builder call: the skew bound)**. For a type (b) row
  the broker order id must also equal the row's. Match → link (3.7). Mismatch → incident at once,
  reason `client_id_mismatch` (a reused id from another journal on the same account, or a broker
  inconsistency). Never link a mismatch.
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

### 3.7 Link

Linking writes `broker_order_id` (type a), clears `resolve_since`, and publishes `OrderResolved`
(`order_row_id`, `broker_order_id`, `client_order_id`, `how="resolver"`) in one unit, then logs
`risk.order_resolved`. The row is then read by the normal sync path on the same pass or the next,
which books its fills and settles it.

For a type (b) row a match only proves the broker still knows the order under our client id. The
row then reads through `get_order` as usual; if `get_order` keeps raising `OrderNotFound` while
the client-id lookup finds it, the row becomes an incident at its deadline (reason
`lookup_inconsistent`) **(builder call)**.

### 3.8 Effects elsewhere

- `cancel_open_entries` still returns id-less entry rows as `unresolved`; once linked they are
  cancelled normally.
- The Position Manager's exit logic needs no change: once linked and settled, its existing retry
  rules see the real outcome. `classify_sells` already holds exits back while any sell on the
  position is `unresolved` or `in_flight`, so a new exit is never sent while an earlier sell still
  reserves qty. If one still is (flatten racing a pending sell), the broker rejects the
  oversized sell, the row is `rejected`, and the existing backoff retries once the earlier sell
  settles **(builder call: no pending-sell subtraction in the engine)**.
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
or is a type (b) incident (`not_found`, `lookup_inconsistent`, `client_id_mismatch` on a row with a
broker id), whose broker id is then replaced (D20.5). Connects the broker (read-only), reads the
order, and checks symbol, side, qty, client id (if the row has one) and the 5-minute `created_at`
bound as in 3.4. On match: writes `broker_order_id` (`before` keeps the old one),
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

Ledger change **(builder call: the allocation rule below)**. The split is persisted on the row,
derived from booked fills, never from memory:

- In `_sell`, when a fill exceeds the open qty plus carried dust and the row has
  `excess_external`, the ledger consumes all carried dust (as today: `dust_consumed`,
  `SymbolDust` reduced), sets `position.qty = 0`, and adds the remainder to a new column
  `orders.external_qty` instead of raising `oversell`. The position's share of the sell is its
  open qty plus the carried dust it consumed, exactly what an ordinary close books today.
- `_sell` currently adds the whole fill's fee to `position.fees` before the excess check. With
  `excess_external` it adds only the position's share, pro rata by qty.
- `_maybe_close` computes `realized_pnl` from the linked fills as today, except that each fill of
  an order with `external_qty > 0` contributes its cash effect scaled by
  `(order filled qty - external_qty) / order filled qty`.
- At the row's settlement the ledger publishes `ExternalHoldingSold` with
  `external_qty = orders.external_qty`, `dust_consumed = 0` (the dust went to the position),
  `proceeds_usd` and `fees_usd` scaled by `external_qty / filled qty`. It does **not** call
  `_external_sale` (that path consumes `SymbolDust` itself and runs only for rows without a
  position; calling it here would take the dust twice).

No balance is edited by the command. The next sync pass after restart applies it; the pause stays
set until the operator resumes.

## 6. Schema (migration 0007)

`orders`:
- `incident` `String(24)` nullable: `no_client_id`, `client_id_mismatch`, `not_found`,
  `lookup_failed`, `lookup_inconsistent`;
- `incident_at` `UTCDateTime` nullable;
- `resolve_since` `UTCDateTime` nullable (type b clock);
- `excess_external` bool, default false;
- `external_qty` `Money`-style Decimal, default 0 (5.4).

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
- PLAN 1.5 row and D20; CLAUDE.md command list (`autotrader repair ...`).
- `docs/alpaca-notes.md`: 1.6 must verify the exact-qty sell per symbol and duplicate client-id
  behaviour.

## 8. Acceptance tests (SimBroker, no real API)

1. An exit sends `submit_order` with `client_order_id = at-o<row>` and the broker qty; an
   off-increment full-holding sell after a coin-fee buy is accepted and leaves no dust.
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
9. Legacy exit row without client id: incident `no_client_id` on the first pass.
10. Type (b): `get_order` raises `OrderNotFound` for a known id; `resolve_since` persisted;
    entries on every symbol rejected until the deadline; incident at the deadline, after which
    only its own symbol is blocked and `get_order` is no longer called for it; `repair link`
    replaces its broker id.
11. `repair` refuses while the core holds the lock, with empty evidence, on a settled row, and on a
    link mismatch; each successful command writes exactly one `OperatorRepair` event with the row
    change in the same unit.
12. `repair not-submitted` then a sync pass: row settled with zero fill, reservation released.
13. `repair oversell`: position closes with P&L on its qty only, dust consumed,
    `ExternalHoldingSold` for the excess, no further `LedgerAnomaly`, pause still set.
14. No path anywhere sets `not_submitted` from a zero-match lookup (grep-level check in review plus
    tests 4 and 5).

`risk/` keeps 100 % branch coverage.

## 9. Left to the builder

Everything marked **(builder call)** above, plus: exact log field names, CLI output wording,
whether `repair` lives in `cli.py` or a `cli_repair.py` module, and test file layout.
