# Order Resolver and Journal Repair (WP 1.5d-2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every order carries a journal-namespaced `client_order_id` (exits via `submit_order`),
a bounded resolver in the order-sync pass links lost orders or turns them into operator
incidents, and `autotrader repair link|not-submitted|oversell` replaces hand-written SQL.

**Architecture:** Pure helpers (ownership, match, client-id formats) in a new `risk/resolve.py`.
The engine decides (it alone holds the broker): `sync_orders()` = the read step both callers use
plus a resolve step only it runs; it returns its decisions as `SyncResult.resolutions`. The
ledger writes them (it alone holds the bus): `apply_all()` applies resolutions as conditional
`publish_tx` units before booking. The oversell split lives in the ledger's `_sell` /
`_maybe_close`. Repair = journal-side `core/repair.py` (prepare, then commit one unit) plus a
`repair` typer sub-app in `cli_repair.py`.

**Tech Stack:** Python 3.12, uv, SQLAlchemy 2 async + Alembic (`render_as_batch=True`), pydantic v2,
structlog, typer, pytest (asyncio_mode=auto, `--strict-markers`, `filterwarnings=error`), ruff, mypy strict.

**Spec:** `docs/superpowers/specs/2026-11-10-order-resolver-repair-design.md` (read it first;
section numbers below refer to it). Decisions: PLAN.md D20.

**Dry run (2026-11-10):** an agent executed tasks 1-10 in an isolated worktree; every commit green
(1144 passed, `risk/` 100 % branches). Its 18 findings are folded into this text: missing SimBroker
`unforget`, existing-test fallout per task, a coverage gap at Tasks 3 and 6, the `_in_flight` add
point, an exact `_split` for ordinary fills, broker failures in `repair link`, a benign refusal
logged as `bus.unit_failed`, a JSON-typed migration insert, SimBroker `created_at` sitting on the
skew bound, and two design points: a stale type (b) incident (fixed in Tasks 5-6) and incident
rows holding back later bookings on their symbol (**owner decision, see "Open decision"**).

**Open decision (dry-run finding, not in the spec):** an incident row is never read again, so
`Ledger.apply_all` holds back every later row on its symbol until the operator repairs it,
including a later exit's fills (the broker sells, the journal position stays open, the cash
check stays `skipped`). That hold-back predates this WP (id-less rows), but incidents make it
indefinite. Relaxing it would book an exit before an unresolved earlier entry (anomalies, or
a wrong P&L). This plan keeps the hold-back (fail safe) and makes it visible: a test pins it
(Task 6), `risk.order_incident_open` lists the held-back rows, and the runbook says an incident
must be repaired before anything later on its symbol books (Task 11). Spec 3.6 gets a dated note
in this plan PR.

## Global Constraints

- Branch: `wp-1.5d2-impl` off `origin/main` (`git fetch origin && git switch -c wp-1.5d2-impl origin/main`).
- Every commit message ends with the line `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`
  (exactly this, whatever model you are).
- ASCII only in code, comments and docstrings (ruff RUF001-003). No `assert` in `src/` (S101).
- Every task's commit is green: `uv run ruff format && uv run ruff check --fix && uv run ruff check && uv run mypy && uv run pytest`
  (format BEFORE check; format does not wrap docstrings, keep lines <= 100 by hand).
- `risk/` keeps 100 % branch coverage: `uv run pytest --cov=autotrader.risk --cov-branch --cov-report=term-missing tests/unit/risk`.
- Never assert wall-clock timing; drive time with the tests' `Clock` (engine/ledger) and `Mono`
  (budget). Log assertions use `structlog.testing.capture_logs`. Tests never hit Alpaca.
- **SPEC 0.1:** only `risk/engine.py` holds a `Broker`. The repair CLI connects the broker and
  hands it straight to a function in `risk/engine.py` (as `run core` hands it to `RiskEngine`);
  `core/repair.py` only ever receives a lookup callable.
- **No path sets `not_submitted` from a lookup.** The resolver writes only `broker_order_id`
  (link), `incident`/`incident_at` and `resolve_since`. `not_submitted` comes only from
  `repair not-submitted` (and the existing pre-submit paths).
- Exact values: client ids `at-{jid}-c{cycle_id}-e` (entries), `at-{jid}-o{row_id}` (exits);
  `jid` = 8 lowercase hex chars (`secrets.token_hex(4)`), `control_state` key `journal_id`;
  `order_visibility_allowance_s = 60.0`, `order_resolution_timeout_s = 80.0` (RiskEngine
  constructor parameters, not config); `created_at` skew 5 min; incident reminder every 60 s;
  incident reasons `no_ownership_proof`, `client_id_mismatch`, `not_found`, `lookup_failed`,
  `lookup_inconsistent`; exit rejection reason `reserved by pending sells in <SYM>`.
- Docs are part of each task: fix every docstring/comment in a touched module that states the
  old behaviour (`rg` for it). Runbook/PLAN/CLAUDE.md land in Task 11. Dated records (PLAN
  history entries, older specs) are never rewritten; they get dated notes.

## Review Focus

1. A lost-response order that **fills before it is linked**: the link must not lose or double-book
   those fills; the normal read books all of them once. Test in Task 6 (`test_lost_response_...`).
2. A journal **without a `journal_id` row** (hand-deleted): entries fail closed
   (`state unavailable`), exits still go out with `client_order_id=None` (reduce-only exits must
   stay possible) and log `risk.journal_id_missing`. Test in Task 4.
3. A row marked `excess_external` whose position **can absorb the whole sell** (carried dust
   arrived, or the mark was unnecessary): no external part, no `ExternalHoldingSold`, books
   like any sell. Test in Task 8.
4. **Budget spent mid-resolve**: a row before its deadline that was not reached stays waiting
   (never a zero match); one past its deadline becomes an incident with its last seen outcome
   (`lookup_failed` if none). Test in Task 6.
5. A type (b) clock **cleared by a successful `get_order`** read, then a later `OrderNotFound`:
   the clock restarts at the new miss, not at the old `resolve_since`. A type (b) incident
   decided before such a clear is not written (dry-run race: `check()`'s own read clears the
   clock between `sync_orders` deciding and `apply_all` writing). Tests in Tasks 5 and 6.

---

### Task 1: Schema 0007 and the journal id

**Files:**
- Create: `src/autotrader/journal/migrations/versions/20261110_0007_order_resolver.py`
- Modify: `src/autotrader/journal/models.py` (`Order` ~113, `Fill` ~145, `ControlState` docstring ~287)
- Modify: `src/autotrader/journal/repo.py` (next to `get_control`, ~343)
- Test: `tests/unit/journal/test_db.py`, `tests/unit/journal/test_repo.py`

**Interfaces:**
- Produces: `Order.incident: str | None` (`String(24)`), `Order.incident_at: datetime | None`,
  `Order.resolve_since: datetime | None` (both `UTCDateTime` like the other order times),
  `Order.excess_external: bool` (default/server_default false); `Fill.external_qty: Decimal`
  (default 0, server_default "0").
- Produces: `repo.JOURNAL_ID_KEY = "journal_id"`, `async def journal_id(session: AsyncSession) -> str | None`.

Migration 0007 (revision `"0007"`, down `"0006"`, style of 0006): add the columns in batch mode;
then `op.execute` an insert of `("journal_id", json "<token_hex(4)>")` into `control_state` only if
the key is absent (`INSERT ... WHERE NOT EXISTS`). Bind the value with a JSON-typed parameter
(`sa.bindparam("value", value=<id>, type_=sa.JSON)`), never a quoted literal or a plain text
parameter: Postgres (asyncpg) would type a text parameter as `text`, not `json`. Downgrade drops
the columns and deletes the key.

- [ ] **Step 1: Failing tests:**
  - `test_db.py::test_migration_writes_a_journal_id_once`: after `upgrade(url)`, `journal_id` matches `^[0-9a-f]{8}$`; a fresh journal gets a different id (probabilistic but 2^-32). The `WHERE NOT EXISTS` guard is exercised by downgrading to `0006`, inserting a `journal_id` row by hand, upgrading again: the hand-written value is kept (a second `upgrade` at head is a no-op and proves nothing).
  - The existing model/migration parity test must pass with the new columns.
  - `test_repo.py::test_journal_id_reads_the_control_row`: returns the migrated value; `None` after deleting the row.
- [ ] **Step 2:** `uv run pytest tests/unit/journal -v`. Expected: FAIL (no revision / attribute).
- [ ] **Step 3:** Implement models, migration, `repo.journal_id`. Add `journal_id` to the `ControlState` key list.
- [ ] **Step 4:** Full check. Expected: PASS.
- [ ] **Step 5: Commit** `feat: schema for the order resolver and a per-journal id (migration 0007)`.

### Task 2: Broker protocol: order view, free qty, SimBroker hooks

**Files:**
- Modify: `src/autotrader/broker/base.py`, `src/autotrader/broker/sim.py`, `src/autotrader/broker/alpaca.py`
- Test: `tests/unit/broker/test_sim.py`, `tests/unit/broker/test_alpaca.py`, `tests/unit/core/test_runtime.py` (~1102: its `Position(...)` gains `qty_available`). All test broker fakes subclass `SimBroker`, so none needs the new method.

**Interfaces:**
- Produces: `Position.qty_available: Dec` (required).
- Produces: `class BrokerOrderView(_Frozen)` exactly as spec 4 (`order_id`, `client_order_id`,
  `symbol`, `side`, `qty`, `status`, `filled_qty`, `created_at: AwareDatetime`).
- Produces: `Broker.get_order_by_client_id(self, client_order_id: str) -> BrokerOrderView | None`.
  No `get_order_view`: `repair link` only accepts rows with a namespaced client id (spec 5.2), so
  the client-id lookup always applies (resolves the spec 4 builder call).
- Produces (SimBroker test hooks):
  - `hide(client_order_id: str, visible: Callable[[], bool]) -> None`: while `visible()` is false the
    order is invisible to `get_order`, `order_fills` (raise `OrderNotFound`) and
    `get_order_by_client_id` (returns `None`). A predicate, not a datetime, so tests drive it with
    the engine's `Clock` (5 m sim bars cannot express 50 s).
  - `fail_submit_after_accept() -> None`: the next `submit_order` stores the order, then raises
    `ConnectionError("response lost")`.
  - `forget(order_id: str) -> None`: `get_order`/`order_fills` raise `OrderNotFound` for it;
    `get_order_by_client_id` still returns it under the same id. `unforget(order_id: str) -> None`
    undoes it (Review Focus 5 needs a successful read after a miss).
  - `rekey(order_id: str) -> str`: moves the order to a new id (`sim-<n>`, same seq counter, fill
    ids keep the old prefix); the old id raises `OrderNotFound`; returns the new id.
- `submit_order` rejects a non-None `client_order_id` already used by any stored order
  (`OrderRejected("duplicate client_order_id")`) so a lookup is unambiguous **(builder call)**.

SimBroker changes (spec 2.2, 2.4): `get_positions` sets `qty_available = held - pending sell
remainders`; `submit_order` treats a sell whose qty equals that free qty as a full close
(`full_close=True`, increment check skipped; below `min_qty` raises `DustRemainder`). Any other
off-increment sell is still rejected. `get_order_by_client_id` builds the view from `_orders`
(`created_at = submitted_at`). AlpacaBroker: `qty_available=Decimal(p["qty_available"])`
(the read side exists today; fixture `fee_check_btcusd_20261012.json` has the field);
`get_order_by_client_id` -> `_not_yet(...)`.

- [ ] **Step 1: Failing tests** (`test_sim.py`):
  - `test_qty_available_excludes_pending_sells`: hold 1, open sell 0.4 -> `qty=1`, `qty_available=0.6`.
  - `test_full_free_sell_off_increment_is_accepted_after_coin_fee_buy`: base-fee buy leaves an off-increment holding; `submit_order(sell, qty=qty_available)` accepted; after the fill no holding remains. A different off-increment qty is still rejected. A free holding below `min_qty` raises `DustRemainder`.
  - `test_get_order_by_client_id_returns_view_or_none`; `test_duplicate_client_id_rejected`.
  - `test_hide_until_visible`, `test_fail_submit_after_accept_stores_the_order`, `test_forget_keeps_client_lookup` (and `unforget` restores reads), `test_rekey_moves_the_order`.
  - `test_alpaca.py`: positions map `qty_available` (the positions fixture gains the field); `get_order_by_client_id` joins the unavailable-methods list.
- [ ] **Step 2:** `uv run pytest tests/unit/broker -v`. Expected: FAIL.
- [ ] **Step 3:** Implement. Update the sim module docstring (`close_position` paragraph: a full free sell via `submit_order` behaves the same; test hooks). Existing test fallout: `test_sim.py::test_close_sells_an_off_increment_holding_exactly` asserts that `submit_order` rejects the full off-increment qty, which is now a full close (spec 2.2): make it use a different off-increment qty.
- [ ] **Step 4:** Full check. Expected: PASS.
- [ ] **Step 5: Commit** `feat: broker order view by client id, free qty, SimBroker lost-order hooks`.

### Task 3: Risk helpers, namespaced entry ids, free-qty exits (pure checks + minimal engine wiring)

**Files:**
- Create: `src/autotrader/risk/resolve.py`
- Modify: `src/autotrader/risk/types.py` (`HeldPosition`), `src/autotrader/risk/checks.py` (`check_entry` ~56, `check_exit` ~228)
- Modify: `src/autotrader/risk/engine.py` (`_gather_entry`, `_broker_positions` ~449, `exit()` rejection branch ~581): the new required fields would otherwise leave this commit red
- Test: create `tests/unit/risk/test_resolve.py`; modify `tests/unit/risk/test_checks.py`, `tests/unit/risk/test_engine.py`, `tests/integration/test_cycle_runner.py` (~204-268), `tests/unit/core/test_cycle.py` (~639, ~861)

**Interfaces:**
- Consumes: Task 1 `repo.journal_id`; Task 2 `Position.qty_available`, `BrokerOrderView`.
- Produces: `RiskEngine._journal_id(self) -> Awaitable[str | None]` (read once, cached when found). In `_gather_entry`, `None` raises `RuntimeError("journal has no journal_id")` (-> `state unavailable: RuntimeError`); the gather returns `jid` and `check()` passes `journal_id=jid`.
- Engine: `_broker_positions` maps `qty_available=p.qty_available`; `exit()` returns `OrderResult(decision)` (a failure: not dust, not `no_position`) when it rejects a non-dust position (the reserved case).
- Produces (`risk/resolve.py`):
  - `IncidentReason = Literal["no_ownership_proof", "client_id_mismatch", "not_found", "lookup_failed", "lookup_inconsistent"]`
  - `CREATED_AT_SKEW = timedelta(minutes=5)`
  - `def entry_client_id(journal_id: str, cycle_id: int) -> str` -> `at-{jid}-c{cycle_id}-e`
  - `def exit_client_id(journal_id: str, row_id: int) -> str` -> `at-{jid}-o{row_id}`
  - `def owns(client_order_id: str | None, journal_id: str) -> bool`: true iff it starts with `at-{jid}-`.
  - `def mismatches(view: BrokerOrderView, *, symbol: str, side: str, qty: Decimal, submitted_at: datetime, broker_order_id: str | None) -> tuple[str, ...]`: one human-readable line per differing field (`symbol`, `side`, `qty`, `created_at < submitted_at - 5 min`, and `order_id` only when `broker_order_id` is not None); empty = match. Ownership is NOT checked here (the caller checks `owns` and exact client-id equality).
- Produces: `HeldPosition.qty_available: Dec` (required; exposure and `is_dust` keep using `qty`).
- Produces: `check_entry(..., journal_id: str)` (required keyword) building the order's client id with `entry_client_id`.
- `check_exit`: after the dust test, if `position.qty_available < position.min_qty` -> `_reject(f"reserved by pending sells in {symbol}")`; else approve with `qty=position.qty_available`.

- [ ] **Step 1: Failing tests:**
  - `test_resolve.py`: formats; `owns` true for own jid, false for other jid, `None`, legacy `at-o5`, `at-c7-e`, and a prefix collision (`at-{jid}x-...`); `mismatches` empty on a match, one entry per differing field, the `created_at` bound inclusive at exactly -5 min, `order_id` compared only when given.
  - `test_checks.py`: `check_entry(..., journal_id="0123abcd")` -> `client_order_id == "at-0123abcd-c7-e"` (replaces the `at-c7-e` assertion); `test_exit_sizes_from_free_qty`; `test_fully_reserved_exit_rejects_without_dust` (qty 1, qty_available 0, min_qty 0.0001 -> reason `reserved by pending sells in BTC/USD`, `qty is None`); exposure test (spec 8 test 9b, pure half): a holding with `qty_available=0` still counts its full market value in gross exposure.
  - `test_engine.py`: `test_entry_client_id_carries_the_journal_id` (replaces the `at-c{cycle_id}-e` assertion ~250): `row.client_order_id == f"at-{jid}-c{cycle_id}-e"` with `jid` from `repo.journal_id`; a second engine on the same journal uses the same `jid`. `test_missing_journal_id_rejects_entries` (delete the row -> `state unavailable: RuntimeError`; needed here for 100 % coverage, Task 4 extends it to exits). `test_exit_on_a_fully_reserved_holding_is_a_plain_failure` (spec 8 test 9b, engine half): an open sell reserving the whole holding -> `dust is False`, `no_position is False`, no new order row. `test_entry_exposure_counts_reserved_qty` (9b): BTC fully reserved by a pending sell, an ETH entry that only fits if BTC were counted at its free qty -> rejected by the exposure cap.
- [ ] **Step 2:** `uv run pytest tests/unit/risk -v`. Expected: FAIL.
- [ ] **Step 3:** Implement. Update the `checks.py` module docstring (exit sizing from free qty). Existing test fallout: tests pinning `at-c{cycle}-e` (`test_checks.py` ~109 and its `run()`/`held()` helpers, `test_cycle_runner.py` 3 assertions, `test_cycle.py` 2 fixtures: build ids with `entry_client_id`). Free-qty sizing also changes two engine tests, correctly: `test_concurrent_exits_on_one_symbol_sell_once` (the second exit is now rejected `reserved by pending sells` with no row, status `None`) and `test_retried_unknown_exit_never_sells_twice[False]` (the retry is rejected before any row: `["unknown"]`, not `["unknown", "rejected"]`). No other assertion changes.
- [ ] **Step 4:** Full check + risk coverage. Expected: PASS, 100 %.
- [ ] **Step 5: Commit** `feat: namespaced entry ids, ownership and match checks, free-qty exits`.

### Task 4: Engine: exits via `submit_order` with a namespaced client id; in-flight rows

**Files:**
- Modify: `src/autotrader/risk/engine.py` (`__init__`, `check()`, `exit` ~563, `_submit` ~505 comment, module docstring)
- Modify: `src/autotrader/journal/repo.py` (`insert_order`)
- Modify: `src/autotrader/core/ledger.py` (comment at ~359 only)
- Test: `tests/unit/risk/test_engine.py`, `tests/unit/core/test_position_manager*.py` (any `close_position` expectation)

**Interfaces:**
- Consumes: Task 3 `exit_client_id`, `_journal_id`.
- Produces: `repo.insert_order(..., client_order_id_for: Callable[[int], str] | None = None)`: when given, flush, set `row.client_order_id = client_order_id_for(row.id)`, then commit (one transaction).
- Produces: `RiskEngine._in_flight: set[int]`: row ids from the insert until `check()`/`exit()` returns. Add the id inside the `async with self._sessions()` block right after `insert_order` returns (closing the session is an await); discard it in a `finally` covering the gate re-check, the health re-check and `_submit`. Task 6 reads it. A gap remains that no in-memory set closes: the row is visible to another task once committed, before the inserting task resumes. It is harmless: a miss is never acted on before the 80 s bound, and a link writes the id the ack would.

Behaviour:
- `exit()`: insert the row with `client_order_id_for=partial(exit_client_id, jid)` when `jid` is known; when `None`, `client_order_id=None` and `log.error("risk.journal_id_missing", symbol=...)` (the exit still goes out). Submit `BrokerOrder(symbol, side="sell", qty=decision.qty, type="market", time_in_force="gtc", client_order_id=row.client_order_id)` via `submit_order`. `close_position` is no longer called anywhere in `src/` outside the brokers.
- Docstrings/comments: `exit()` ("carries no client_order_id") and the module docstring; `_submit`'s `DustRemainder` comment ("full free sell"); the ledger comment "An exit is a close_position" -> exits now sell an exact qty; the sell-side overfill exemption stays (rows from earlier journals).

- [ ] **Step 1: Failing tests** (`test_engine.py`):
  - `test_exit_submits_a_namespaced_market_sell_of_the_free_qty` (spec 8 test 1; replaces the `client_order_id is None` assertion ~728): SimBroker sees `submit_order` with `client_order_id == f"at-{jid}-o{row.id}"`, `qty == qty_available`, `type == "market"`; no `close_position` call (spy). After a base-fee buy (off-increment holding) the exit is accepted and, after the next bar, no holding and no dust remain.
  - `test_missing_journal_id_rejects_entries_but_not_exits` (Review Focus 2): delete the `journal_id` row -> `check()` rejected `state unavailable: RuntimeError`; `exit()` approved, row `client_order_id is None`, log `risk.journal_id_missing`.
  - `test_in_flight_set_covers_insert_to_return`: a `HangingSubmit`-style broker blocked on an event -> the row id is in `engine._in_flight` while blocked, gone after release; also gone after a gate rejection (`not_submitted`).
- [ ] **Step 2:** `uv run pytest tests/unit/risk/test_engine.py -v -x`. Expected: the new tests FAIL; note which existing tests break.
- [ ] **Step 3:** Implement. Existing test fallout (13 tests fail before this): every fake that overrides `close_position` now tests nothing; retarget each to override `submit_order` for sells, same assertions: `test_engine.py` `HangingClose`, `LostCloseAck`, `DustBroker`, `UnavailableBroker` (drop the dead `SlowBroker.close_position`), `test_exit_goes_through_while_killed` (now expects the client id); `test_position_manager.py` `RejectingClose`, `UnavailableClose`, `LostClose`, `DustClose`; `test_position_manager_control.py` `EthCloseRejected`, `EthAckLost`, `BtcAckLost`. `rg -n "def close_position" tests` must then list no fake that the engine path relies on. Do not change other assertions.
- [ ] **Step 4:** Full check + risk coverage. Expected: PASS, 100 %.
- [ ] **Step 5: Commit** `feat: every order carries a journal-namespaced client id; exits use submit_order`.

### Task 5: Resolution types and the ledger's resolution writes

**Files:**
- Modify: `src/autotrader/risk/types.py` (`SyncResult` ~110)
- Modify: `src/autotrader/core/ledger.py` (events next to `LedgerAnomaly`; `apply_all` ~265; the conditional updates live here, no repo helper)
- Test: `tests/unit/core/test_ledger.py`

**Interfaces:**
- Produces (`risk/types.py`):
  ```python
  class OrderResolution(_Frozen):
      order_row_id: int
      action: Literal["link", "incident", "start_clock", "clear_clock"]
      at: AwareDatetime
      broker_order_id: str | None = None  # link only
      reason: IncidentReason | None = None  # incident only
      clock: AwareDatetime | None = None  # type (b) incident only: the resolve_since it was decided on
  ```
  and `SyncResult.resolutions: tuple[OrderResolution, ...] = ()`.
- Produces (`core/ledger.py`): `class OrderResolved(LedgerEvent)`: `order_row_id`, `broker_order_id`, `client_order_id: str | None`, `how: Literal["resolver"] = "resolver"`; `class OrderIncident(LedgerEvent)`: `order_row_id`, `symbol`, `side`, `qty: Decimal`, `client_order_id`, `broker_order_id`, `reason: str`, `submitted_at: datetime`.
- Produces: `Ledger.apply_resolutions(self, resolutions: Sequence[OrderResolution]) -> None`; `apply_all` calls it first, then books as today.

Writes, each its own unit; a unit whose conditional update matched no row returns `None` (no event):
- `link`: `UPDATE orders SET broker_order_id=?, updated_at=at WHERE id=? AND broker_order_id IS NULL AND settled_at IS NULL AND incident IS NULL`, event `OrderResolved`; after commit `log.info("risk.order_resolved", order_row=..., broker_order_id=...)`.
- `incident`: `... SET incident=reason, incident_at=at, resolve_since=NULL WHERE id=? AND incident IS NULL AND settled_at IS NULL`, plus, when `clock` is set (type (b)), the row's `resolve_since` must still equal `clock` (compare in Python after reading the row in the unit; datetimes are text on SQLite). A stale type (b) decision, whose clock `check()`'s own read cleared (`clear_clock`) or restarted in between, writes nothing (dry-run race). Event `OrderIncident` (row fields read in the unit); after commit `log.error("risk.order_incident", order_row=..., symbol=..., reason=...)`.
- `start_clock`: `... SET resolve_since=at WHERE id=? AND resolve_since IS NULL AND incident IS NULL`; `clear_clock`: `... SET resolve_since=NULL WHERE id=? AND resolve_since IS NOT NULL`. Plain session writes, no event, no bus (bookkeeping like `update_order`).
- A resolution that raises is logged `ledger.resolution_failed` (`order_row`, `action`, exc_info) and does not stop the others or the booking. `CancelledError` propagates.

- [ ] **Step 1: Failing tests** (`test_ledger.py`, rows built like the file's other order rows):
  - `test_link_writes_once_and_publishes_once`: two identical `link` resolutions in one call -> `broker_order_id` set, exactly one `OrderResolved` event; a `link` on a row that already has a broker id or an incident writes nothing.
  - `test_incident_writes_once_clears_the_clock_and_logs`: `start_clock` then `incident` -> `incident`, `incident_at` set, `resolve_since is None`, one `OrderIncident` with the row's fields, one `risk.order_incident` log; a second `incident` with another reason changes nothing.
  - `test_start_clock_never_moves_an_existing_clock`; `test_clear_clock`.
  - `test_stale_type_b_incident_is_not_written` (Review Focus 5): row with `resolve_since=t0`; an `incident` with `clock=t0` applied after a `clear_clock` -> nothing written, no event; with `clock=t0` while `resolve_since=t0` -> written.
  - `test_failed_resolution_does_not_block_booking`: a `link` whose unit raises (`hooked_sessionmaker` `fail_before=1`) is logged `ledger.resolution_failed`, the same pass still books another symbol's update.
- [ ] **Step 2:** `uv run pytest tests/unit/core/test_ledger.py -v -k "link or incident or clock or resolution"`. Expected: FAIL.
- [ ] **Step 3:** Implement; extend the ledger module docstring (resolutions are applied first, each conditional).
- [ ] **Step 4:** Full check. Expected: PASS.
- [ ] **Step 5: Commit** `feat: the ledger records resolver links, incidents and type (b) clocks`.

### Task 6: Engine resolver in `sync_orders`

**Files:**
- Modify: `src/autotrader/risk/engine.py` (`__init__`, `sync_orders` ~400, `_sync` ~405, `_read_order`, module docstring)
- Test: create `tests/unit/risk/test_resolver.py` (own `db_url`/`sessions` fixtures; builders via `import tests.unit.risk.test_engine as te`: `te.make_engine_`, `te.Clock`, `te.Mono`, `te.FakeView`, `te.five`, `te.RULES`, `te.FEES`, `te.PROFILE`)

**Interfaces:**
- Consumes: Task 2 `get_order_by_client_id` and SimBroker hooks; Task 3 `owns`, `mismatches`, `_journal_id`; Task 4 `_in_flight`; Task 5 `OrderResolution`, `Ledger.apply_all`.
- Produces: `RiskEngine.__init__(..., order_visibility_allowance_s: float = 60.0, order_resolution_timeout_s: float = 80.0)`; `make_engine_` passes them through as keyword arguments with these defaults.
- `check()` keeps calling the read step only; `sync_orders()` runs read then resolve under ONE `io_timeout_s` budget and returns one `SyncResult`.

Read step (both callers; spec 3.2): skip rows with `incident` set (no update, not failed); id-less
local finals as today; id-less `pending`/`unknown` -> type (a) candidates; for rows with a broker id,
`OrderNotFound` from the read -> type (b) candidate (and in `failed`), other exceptions -> `failed` as
today; a successful read of a row with `resolve_since` set adds `clear_clock`.

Resolve step (`sync_orders` only), `now = self._clock()` once, `jid = await self._journal_id()`:
- type (a) rows in `self._in_flight` are skipped (checked at lookup time, after any await).
- `jid is None` (hand-deleted row): resolve nothing this pass, `log.error("risk.journal_id_missing")`
  (marking every row `no_ownership_proof` would turn a journal fault into permanent incidents).
- no ownership proof (`not owns(row.client_order_id, jid)`) -> `incident` `no_ownership_proof` at once, no lookup.
- type (b): `since = row.resolve_since` or, if None, `now` plus a `start_clock` resolution (emitted
  before any incident for the row, also for `no_ownership_proof`). Every type (b) incident carries
  `clock=since` (Task 5 writes it only if the clock is unchanged).
- lookup `get_order_by_client_id` via `_within(deadline, ...)`; a spent budget = not looked up.
- found: `row.client_order_id == view.client_order_id` and `mismatches(...)` empty (type (b) passes `broker_order_id=row.broker_order_id`) -> type (a): `link`; type (b): nothing (clock kept, spec 3.7). Otherwise -> `incident` `client_id_mismatch` at once.
- `None` -> outcome `not_found`; exception -> `lookup_failed`; remember it in `self._last_outcome: dict[int, IncidentReason]`.
- deadline: type (a) `row.submitted_at + order_resolution_timeout_s`, type (b) `since + order_resolution_timeout_s`. At or after it and not linked -> `incident` with reason: type (b) with a match -> `lookup_inconsistent`; else this pass's outcome, else the remembered one, else `lookup_failed`.
- log: a miss before `submitted_at + io_timeout_s + order_visibility_allowance_s` -> `risk.order_resolve_pending` debug; after -> info.
- type (b) rows that get an `incident` in this result are removed from `failed` (D20.5: they then block only their symbol).
- reminder: if any unsettled row carries an incident (from the journal or this pass) and `self._monotonic() - last >= 60` (or never logged) -> `log.error("risk.order_incident_open", order_rows=[{"order_row": id, "reason": r}, ...])`.

Tests run the real pair `ledger.apply_all(await engine.sync_orders())` with SimBroker; the
engine `Clock` is advanced by hand; SimBroker hides use `visible=lambda: clock.now >= ...`.
Fixture timing: SimBroker stamps `created_at` with its last bar time, which trails the engine
clock (with `te.five(29)` it is `NOW - 5 min`, exactly on the inclusive skew bound, so a 1 s
clock advance before an entry turns a match into `client_id_mismatch`). The resolver fixtures
process a bar stamped at the engine's `NOW` before any submit, so matches have 5 min of headroom;
the mismatch test (8) moves the row's `submitted_at` past the bound on purpose.

- [ ] **Step 1: Failing tests** (`test_resolver.py`; spec 8 numbers):
  - `test_lost_response_is_linked_and_booked` (2, Review Focus 1): `fail_submit_after_accept()`; entry -> row `unknown`; process a bar so the order fills BEFORE the next pass; pass 1 -> linked (`OrderResolved`), pass 2 -> fills booked once, row settled, position open.
  - `test_order_hidden_for_50s_is_linked_without_incident` (3).
  - `test_never_visible_becomes_not_found_at_80s_not_earlier` (4): passes at +79 s: no incident; +80 s: incident `not_found`; row unsettled, still pending/unknown; `check()` on BTC rejected, on ETH not by this row.
  - `test_raising_lookup_becomes_lookup_failed_never_zero_match` (5): a SimBroker subclass whose `get_order_by_client_id` raises; no incident before +80 s; `lookup_failed` at +80 s.
  - `test_restart_resolves_pending_rows_with_persisted_clocks` (6): pending row from a "previous process" (insert the row directly, broker has the order) -> a new engine links it; type (b) after a restart keeps `resolve_since` (deadline from the persisted value).
  - `test_in_flight_row_is_never_looked_up` (7): `HangingSubmit`-style broker blocked on an event; run `sync_orders()` concurrently; spy on `get_order_by_client_id` -> not called for that row.
  - `test_client_id_hit_with_wrong_qty_or_old_created_at_is_a_mismatch` (8): incident `client_id_mismatch`, never linked.
  - `test_rows_without_ownership_proof_are_incidents_without_lookup` (9): `NULL`, `at-o5`, `at-c7-e` client ids -> `no_ownership_proof` on pass 1, lookup spy not called.
  - `test_other_journals_order_is_never_linked` (9a): two journals (two `db_url`s, different `jid`) on one SimBroker; journal A's order exists; journal B's identical-shape submit response is lost (`fail_submit_after_accept`) -> B links its own order only, never A's (assert B's `broker_order_id` != A's). A regression guard: the lookup is by B's own namespaced id, so A's order is never even returned.
  - `test_type_b_incident_after_check_cleared_the_clock_is_dropped` (Review Focus 5, end to end): `sync_orders()` decides a type (b) incident at the deadline; before `apply_all`, `unforget` and run `check()` (its read emits `clear_clock`); then apply -> no incident, row readable and booked normally.
  - `test_incident_holds_back_later_rows_on_its_symbol` (Open decision): BTC entry incident, then a later BTC sell row that fills -> its fills stay unbooked while the incident is open (`ledger.update_deferred`); ETH books normally; after `repair` (link) the BTC rows book in order.
  - `test_generic_read_failure_stays_failed_not_type_b` and `test_resolver_idles_without_journal_id` (coverage: a non-`OrderNotFound` read error is not a type (b) candidate; `jid is None` resolves nothing and logs `risk.journal_id_missing`).
  - `test_type_b_blocks_all_until_deadline_then_only_its_symbol` (10): `rekey(id)` + `hide(new client id)` -> `resolve_since` persisted; ETH entry rejected `state unavailable: SyncIncomplete` until +80 s; then incident `not_found`, ETH approved, BTC rejected; `get_order` spy no longer called for it.
  - `test_type_b_inconsistent_lookup_is_one_incident_across_restart` (10a): `forget(id)`; passes up to +40 s, restart, passes to +80 s -> `resolve_since` unchanged throughout, exactly one `OrderIncident` with `lookup_inconsistent`.
  - `test_spent_budget_never_counts_as_zero_match` (Review Focus 4): `Mono` that the first lookup advances past the budget; two rows, one at +30 s, one at +85 s with remembered `not_found` -> first: no incident; second: incident `not_found`.
  - `test_cleared_clock_restarts_at_the_next_miss` (Review Focus 5): `forget` -> `resolve_since=t0`; un-forget (a successful read) -> `clear_clock`; `forget` again at t1 -> `resolve_since == t1`.
  - `test_incident_reminder_every_60s` (`capture_logs`; monotonic driven by `Mono`).
  - `test_resolver_never_sets_not_submitted` (14): across all scenarios above, no row ends `not_submitted`; additionally `rg -n "not_submitted" src/autotrader/risk/engine.py` lists only the pre-submit paths (record the grep in the commit message).
- [ ] **Step 2:** `uv run pytest tests/unit/risk/test_resolver.py -v`. Expected: FAIL.
- [ ] **Step 3:** Implement. Update the module docstring (`sync_orders` resolves; `check()` only reads; incidents skipped by both; the bound; no automatic `not_submitted`) and remove the "the 1.5d resolver's case" comment in `_sync`; also `core/cycle.py:14` ("waits for the 1.5d resolver"). Existing test fallout: `make_engine_` gains the two bound parameters; `test_engine.py::test_sync_orders_reads_every_unsettled_row_once` asserted `failed == (ghost,)`, but its rows have NULL client ids, so `sync_orders` now marks the ghost and the id-less `unknown` row `no_ownership_proof` and they leave `failed`: assert that, and assert separately that the read step alone (`_sync`) still reports the ghost as failed.
- [ ] **Step 4:** Full check + risk coverage. Expected: PASS, 100 %.
- [ ] **Step 5: Commit** `feat: bounded order resolver in the sync pass; unresolved orders become incidents`.

### Task 7: Flatten result shows incidents

**Files:**
- Modify: `src/autotrader/core/position_manager.py` (`remaining["orders"]` ~365, module docstring if it describes `unresolved`)
- Test: `tests/unit/core/test_position_manager_control.py`

**Interfaces:** each order entry gains `"incident": o.incident`; `"unresolved"` becomes
`o.incident is not None or (o.broker_order_id is None and o.status in ("pending", "unknown"))`.

- [ ] **Step 1: Failing test** `test_flatten_result_reports_incident_rows`: an unsettled sell row with a broker id and `incident="not_found"` -> its entry has `incident == "not_found"`, `unresolved is True`; an ordinary open row has `incident is None`, `unresolved is False`.
- [ ] **Step 2:** Run it. Expected: FAIL (`KeyError: 'incident'`).
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Full check. Expected: PASS.
- [ ] **Step 5: Commit** `feat: flatten_result names incident rows`.

### Task 8: Ledger: per-fill external split for an operator-marked oversell

**Files:**
- Modify: `src/autotrader/core/ledger.py` (`_book` settlement block ~404-411, `_sell` ~455, `_maybe_close` ~587, module docstring)
- Test: `tests/unit/core/test_ledger.py`

**Interfaces:**
- Consumes: Task 1 `Order.excess_external`, `Fill.external_qty`.
- Produces: `def _split(amount: Decimal, qty: Decimal, external_qty: Decimal) -> tuple[Decimal, Decimal]` -> `(position_part, external_part)`: `(amount, 0)` unchanged when `external_qty == 0` (at 28-digit precision `a * q / q != a` in ~18 % of cases, so ordinary trades would pick up rounding residue); otherwise `position_part = amount * (qty - external_qty) / qty` and `external_part = amount - position_part` (exact sum; residue in the external part).
- Produces: `Ledger._excess_sale(self, session, order, now) -> ExternalHoldingSold | None` (None when `sum(external_qty) == 0`).

Algorithm (spec 5.4): `_sell` receives the `FillRow` it just added (so it can set `external_qty`).
Capacity = `position.qty` + carried dust. If `fill.qty <= position.qty`: as today. Else, after the
existing `_earlier_unsettled` deferral: if `order.excess_external` is false, today's path (consume
dust or `oversell`). If true: `consumed = min(excess, dust)`, `external = fill.qty - position.qty - consumed`;
consume the dust as today (`dust.qty`, `position.dust_consumed`), `position.qty = 0`,
`fill_row.external_qty = external`. A later fill of the row then has capacity 0 and is fully external.
Fees: `position.fees += _split(fee_usd, fill.qty, external)[0]` (moved after the split; ordinary
fills keep `external = 0`, so today's totals are unchanged). `_maybe_close`: `realized_pnl = sum(_split(_cash_effect(r), r.qty, r.external_qty)[0])`.
Settlement: `if order.side == "sell" and order.excess_external` -> `unit.external = await self._excess_sale(...)`
with `qty = external_qty = sum(external_qty)`, `dust_consumed = 0`, `proceeds_usd = sum(_split(r.qty * r.price, r.qty, r.external_qty)[1])`,
`fees_usd = sum(_split(r.fee, r.qty, r.external_qty)[1])`. Never calls `_external_sale`.

- [ ] **Step 1: Failing tests** (`test_ledger.py`):
  - `test_marked_oversell_splits_per_fill` (spec 8 test 13): one-coin position bought @ 100 with known fee; sell row qty 2 marked `excess_external`; fills 1 @ 100 (fee f1) then 1 @ 200 (fee f2) -> position closed, `fees == buy_fee + f1`, `realized_pnl == 100 - f1 - 100 - quote buy fee`, `fills.external_qty == [0, 1]`; one `ExternalHoldingSold` with `qty == external_qty == 1`, `dust_consumed == 0`, `proceeds_usd == 200`, `fees_usd == f2`; no `LedgerAnomaly`; `paused` still true (set beforehand).
  - `test_marked_oversell_is_restart_safe`: book fill 1, new `Ledger` instance on the same journal, book fill 2 -> same amounts as above.
  - `test_marked_oversell_consumes_carried_dust_once`: `SymbolDust` 0.3, position 1, sell 1.5 in one fill -> `dust_consumed == 0.3`, `external_qty == 0.2`, `SymbolDust` 0, event `dust_consumed == 0`.
  - `test_mark_without_excess_books_normally` (Review Focus 3): marked row, sell 1 of a 1-coin position -> `external_qty == 0`, no `ExternalHoldingSold`, totals as an unmarked sell.
  - `test_split_is_exact`: `_split(D("1"), D("3"), D("1"))` parts sum to exactly 1; `_split(a, q, D(0)) == (a, D(0))` returns `a` itself for a value where `a * q / q != a` (e.g. `a=D("0.1")`, `q=D("3")`: check the inequality in the test first).
- [ ] **Step 2:** `uv run pytest tests/unit/core/test_ledger.py -v -k "oversell or split"`. Expected: FAIL.
- [ ] **Step 3:** Implement; module docstring paragraph on the marked oversell.
- [ ] **Step 4:** Full check (all existing ledger tests unchanged). Expected: PASS.
- [ ] **Step 5: Commit** `feat: ledger books an operator-marked oversell with a persisted per-fill split`.

### Task 9: Repair core (journal side)

**Files:**
- Create: `src/autotrader/core/repair.py`
- Modify: `src/autotrader/risk/engine.py` (module-level lookup function)
- Test: create `tests/unit/core/test_repair.py`

**Interfaces:**
- Produces (`risk/engine.py`): `async def read_order_by_client_id(broker: Broker, client_order_id: str, *, timeout_s: float) -> BrokerOrderView | None` (one `asyncio.wait_for`).
- Produces (`core/repair.py`):
  ```python
  class RepairRefused(Exception): ...
  Action = Literal["link", "not_submitted", "oversell"]
  @dataclass(frozen=True)
  class RepairPlan:
      action: Action
      order_row_id: int
      before: dict[str, str | bool | None]
      after: dict[str, str | bool | None]   # column -> new value; also the event's `after`
  class OperatorRepair(LedgerEvent):  # verify_unit works unchanged
      action: Action; order_row_id: int; before: dict[str, Any]; after: dict[str, Any]
      evidence: str; operator: str
  Lookup = Callable[[str], Awaitable[BrokerOrderView | None]]
  async def prepare_link(sessions, row_id: int, broker_order_id: str, lookup: Lookup) -> RepairPlan
  async def prepare_not_submitted(sessions, row_id: int) -> RepairPlan
  async def prepare_oversell(sessions, row_id: int) -> RepairPlan
  async def commit(bus: EventBus, plan: RepairPlan, *, evidence: str, operator: str, now: datetime) -> None
  ```
- Preconditions (each failure raises `RepairRefused` with the reason printed by the CLI):
  - all: the row exists and `settled_at IS NULL`.
  - link (spec 5.2): (`broker_order_id IS NULL` and status in `pending`/`unknown`) or (`broker_order_id` set and `incident in {"not_found", "lookup_inconsistent", "client_id_mismatch"}`); `owns(row.client_order_id, jid)` (else refuse "no ownership proof: use the runbook's manual procedure"); `lookup(row.client_order_id)` -> `None` refuses; `view.client_order_id != row.client_order_id`, `view.order_id != broker_order_id` or non-empty `mismatches(..., broker_order_id=None)` refuses with every difference. `after = {broker_order_id: <given>, incident: None, incident_at: None, resolve_since: None}`. No `--force`.
  - not_submitted (5.3): `broker_order_id IS NULL`, status `pending`/`unknown`. `after = {status: "not_submitted", incident: None, incident_at: None}`.
  - oversell (5.4): `side == "sell"`, latest `LedgerAnomaly` (`repo.last_anomalies`) has `kind == "oversell"`, `excess_external` false. `after = {excess_external: True}`.
- `commit`: one `publish_tx` unit that re-reads the row; unless every `before` column still holds its value, `build` records the refusal in a holder and returns `None` (no write, no event), and `commit` raises `RepairRefused("row changed since it was read")` after `publish_tx` returned. (Raising inside `build` would make the bus log `bus.unit_failed` at error level for a benign refusal.) Otherwise writes `after` (plus `updated_at = now`) and returns `OperatorRepair`. Never touches positions, fills, balances or `paused`.

- [ ] **Step 1: Failing tests** (`test_repair.py`, journal + SimBroker lookup via `read_order_by_client_id`):
  - per command: refuses a settled row; refuses its wrong-state rows; success writes the row change and exactly one `OperatorRepair` (`action`, `before`, `after`, `evidence`, `operator`) in one unit (`hooked_sessionmaker` `fail_before=1` -> neither the row change nor the event lands).
  - link: refuses NULL/legacy/other-journal client ids (spec 8 test 9, 9a), a lookup miss, a different `order_id`, and a qty mismatch (lists the difference); links a type (b) incident row to a rekeyed id (spec 8 test 10, last clause).
  - `test_not_submitted_then_sync_settles_with_zero_fill` (spec 8 test 12): commit, then `ledger.apply_all(await engine.sync_orders())` -> row settled, `filled_qty == 0`, the symbol's entry no longer blocked by it.
  - `test_oversell_repair_then_sync_closes_the_position` (spec 8 test 13, end to end): ledger reports `oversell` and pauses; `prepare_oversell` + `commit`; next pass books it (`ExternalHoldingSold`), no new `LedgerAnomaly`, `paused` still true.
  - `test_commit_refuses_a_row_changed_since_prepare`: `RepairRefused`, nothing written, no `bus.unit_failed` log.
- [ ] **Step 2:** `uv run pytest tests/unit/core/test_repair.py -v`. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Full check + risk coverage (the new engine function needs a test in `tests/unit/risk`: found / `None` / timeout). Expected: PASS, 100 %.
- [ ] **Step 5: Commit** `feat: journal repair operations with an OperatorRepair audit event`.

### Task 10: `autotrader repair` CLI

**Files:**
- Create: `src/autotrader/cli_repair.py`
- Modify: `src/autotrader/cli.py` (register `repair_app` next to `db_app` ~52)
- Test: `tests/unit/test_cli_repair.py`

**Interfaces:**
- Produces: `autotrader repair link <order_row> <broker_order_id>`, `repair not-submitted <order_row>`, `repair oversell <order_row>`, each with `--evidence TEXT` (required), `--yes`, `--config/-c` (default `config.yaml`).
- Produces: `def _run_repair(cfg: AppConfig, settings: Settings, action: Action, row_id: int, *, broker_order_id: str | None, evidence: str, yes: bool, acquire=journal_lock.acquire, upgrade=db.upgrade, connect_broker=AlpacaBroker.connect, confirm=typer.confirm, operator=getpass.getuser, clock=lambda: datetime.now(UTC)) -> int` (the commands only parse and call it).

Flow: blank `--evidence` (after strip) -> exit 2. `acquire(cfg.database.url, wait_s=0)`;
`JournalLockError` -> print `core is running (<holder>)`, exit 1. `upgrade(lock.url)` (as `run core`).
Open the journal through `lock.url`; for `link` only, `connect_broker(settings)` and
`lookup = partial(read_order_by_client_id, broker, timeout_s=10.0)`. Prepare -> print the row
(`before`) and the change (`after`); for `not-submitted` also print the PLAN 3.2 item 4 condition
(spec 5.3: only with authoritative broker evidence that the order was never and will never be
accepted; a zero-match search or a timeout does not qualify; the mark is final). Unless `--yes`,
`confirm("Apply?")`; declined -> exit 1. Commit; print `done; resume entries per docs/runbook.md
once the next run core shows the row settled`. `RepairRefused` -> print the reason, exit 1. A
broker connect or lookup that raises (`OrderMethodsUnavailable` on Alpaca until 1.6,
`BrokerError`, `TimeoutError`) -> print `refused: broker lookup failed: <type>: <msg>`, exit 1,
no traceback, nothing written. The
lock is held for the whole run (process exit releases it; tests call `release()`).

- [ ] **Step 1: Failing tests** (`typer.testing.CliRunner` for parsing, `_run_repair` with fakes for the rest; spec 8 test 11):
  - refuses while another holder has the lock (exit 1, `core is running`); blank evidence -> exit 2; a settled row -> exit 1 with the reason; a link mismatch -> exit 1 listing the difference.
  - `--yes` applies without calling `confirm`; a declined `confirm` writes nothing (no `OperatorRepair`).
  - `not-submitted` output contains the evidence condition text.
  - `link` with a broker whose connect raises `OrderMethodsUnavailable` (and one whose lookup raises `BrokerError`) -> exit 1, `broker lookup failed`, no `OperatorRepair`.
  - `operator` is recorded in the event.
  - `autotrader repair --help` lists the three commands.
- [ ] **Step 2:** `uv run pytest tests/unit/test_cli_repair.py -v`. Expected: FAIL.
- [ ] **Step 3:** Implement.
- [ ] **Step 4:** Full check. Expected: PASS.
- [ ] **Step 5: Commit** `feat: autotrader repair link / not-submitted / oversell`.

### Task 11: Docs

**Files:**
- Modify: `docs/runbook.md`, `PLAN.md` (1.5 row note, "Current repo state"), `CLAUDE.md` (command list), `docs/alpaca-notes.md`
- Modify (dated notes only): `docs/superpowers/specs/2026-10-11-risk-engine-design.md` section 5 (or the Risk Engine spec's actual filename; `ls docs/superpowers/specs/`), PLAN D14 (dated note, not a rewrite)

- [ ] **Step 1:** Runbook (spec 7; dry-run hits: runbook ~21-22, ~43, ~181-229 incl. ~190-191 "exits carry none", ~206, ~219, ~268): replace "Resolving an unresolved order by hand" with the resolver behaviour (bound, incident reasons, `risk.order_incident` / `risk.order_incident_open`, what each reason means, and that an open incident holds back every later booking on its symbol, a later exit's fills included, until repaired) and the `repair link` / `repair not-submitted` procedure; keep a short manual procedure for `no_ownership_proof` rows (operator evidence is the proof); replace the oversell "WP 1.5c ships no repair" paragraph with `repair oversell`; correct "until the 1.5d resolver exists" and "undoing it needs the 1.5d journal-repair tooling" (undo stays manual: give the procedure); update "Current limits".
- [ ] **Step 2:** `rg -n "close_position|client_order_id|at-c|1\.5d resolver|journal-repair|resolver|not_submitted" src docs/runbook.md CLAUDE.md PLAN.md docs/alpaca-notes.md` and fix every claim this WP falsified (not dated history). Add to CLAUDE.md's command block: `uv run autotrader repair link|not-submitted|oversell <row> --evidence "..."  # journal repair with core stopped (docs/runbook.md)`. alpaca-notes: 1.6 must verify the exact-qty sell per traded symbol, map `qty_available`, and document duplicate `client_order_id` behaviour on submit.
- [ ] **Step 3:** PLAN: 1.5 row dated note "<real date>, 1.5d-2 done" with spec/plan paths; "Current repo state" line; dated notes on §3.2 item 5 ("budget expired before `close_position`", ~106) and D14 (~193). Risk Engine spec dated notes at the `close_position` / no-client-id / `at-c` passages (~127, 148, 153, 176, 191-192).
- [ ] **Step 4:** Full check. Expected: PASS.
- [ ] **Step 5: Commit** `docs: 1.5d-2 resolver and repair in the runbook, plan status`.
