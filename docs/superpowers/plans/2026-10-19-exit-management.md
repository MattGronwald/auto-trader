# Exit Management (WP 1.5c) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Protect every open position with deterministic client-side exits (stop, take profit, time, trailing, stale), retry failed exits until they succeed, act on a kill and on a flatten request (CLI and graceful stop), and run the order-sync and Position Manager loops in `run core`.

**Architecture:** A pure evaluator (`core/exit_rules.py`) decides triggers from the current bid. A new `core/position_manager.py` latches a trigger durably on the position (migration 0005) and drives `RiskEngine.exit()` with in-memory backoff. It also cancels entries on a kill and runs flatten passes requested through `control_state` (`core/flatten.py`). The ledger isolates failures per symbol and books sells of external holdings without a position. Two supervised loops in `run core` connect it all: `ledger.sync` (2 s) and `positions.manage` (1 s).

**Tech Stack:** Python 3.12, uv, SQLAlchemy 2 async + Alembic (SQLite dev), pydantic v2, structlog, typer, pytest + pytest-asyncio + hypothesis.

**Spec:** `docs/superpowers/specs/2026-10-19-exit-management-design.md` (merged in PR #26; decisions D1-D13 recorded as D18 in `PLAN.md`). Read it before starting any task. Section numbers below (4.3, 4.4.1, ...) refer to it.

## Global Constraints

- ASCII only in code, comments, docstrings and test names (ruff RUF001-003 fail on `−`, `×`, `→`, `≥`). Markdown docs may use Unicode.
- `Decimal` for every money/qty value. Quote prices are floats: convert with `Decimal(str(x))`. Money columns are text on SQLite: compare Decimals in Python, never in SQL.
- No `assert` in `src/` (ruff S101). No `eval`/`exec`.
- mypy strict must pass on `src` and `tests`. Import a name from the module that defines it, not from one that merely imports it.
- `src/autotrader/risk/` keeps 100 % branch coverage. A new pure type added before its consumer needs a test in the same task.
- Tests never assert wall-clock timing: inject clocks (`Clock` objects, fixed `T0`). `filterwarnings = error` is on: close engines, buses and subprocess pipes you open.
- Only `risk/engine.py` holds a `Broker`. `core/position_manager.py`, `core/ledger.py` and `core/flatten.py` never import or receive one.
- Never catch `asyncio.CancelledError` (it is a `BaseException`; `except Exception` is fine).
- Events subclass `DomainEvent`; events that must survive a commit error carry a `unit_id` (subclass `LedgerEvent`) and are written with `EventBus.publish_tx(build, verify_unit)`.
- Every commit ends with the trailer line `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>` (subagents: exactly this line, not your own model name).
- Each task's commit leaves the full check green. Run `ruff format` before `ruff check --fix` (snippets may exceed the line length). `ruff format` does not wrap docstrings or comments: wrap those by hand at 100 columns. pytest keeps only the last `-k`: combine with `or`.

Full check, used at the end of every task:

```bash
uv run ruff format && uv run ruff check --fix && uv run ruff format && uv run mypy && uv run pytest -q --cov && uv run coverage report --include='src/autotrader/risk/*' --fail-under=100 --show-missing
```

**Plan dry run (2026-10-19):** all 13 tasks were applied verbatim in a throwaway worktree, running every check after each task: all green, 982 tests, `risk/` at 100 %. This revision folds in the 7 snippet bugs and 8 design findings it reported:
- clarifications 15-17;
- the owner's choices on findings 3 and 6 (2026-10-19: keep the spec's qualification rule; add the lock-first write);
- the decisions it had to make where the plan was silent (clarification 18).

The `position_manager.py` paths no test reaches (about 93 % covered: `settling`, the `qty == 0` skip, an invalid trigger, `CommitUncertain` on completion, a failed holdings read or cancel, a `pending`/`unknown` external outcome) get one test each in Tasks 9-10.

## Spec clarifications made while planning (flag in the PR description)

1. **External-sale dust allocation is durable** (Codex plan note on PR #26). It happens once, in the external sell's **settlement** unit, computed from the order's booked `fills` rows, never from memory. Partial fills and restarts therefore can't double-consume `symbol_dust`. `ExternalHoldingSold` is published only when the settled order sold something (a zero-fill external order settles silently).
2. **`flatten_result.external_sales` is derived from the journal**: settled `external` orders with `settled_at >= request ts`, joined with their `ExternalHoldingSold` payloads (`repo.external_sales_since`).
3. **`flatten_result.dust` reports the journal's `symbol_dust` balances** (> 0), not a broker read: `sellable_positions()` returns non-dust holdings only.
4. **The flat check requires no open journal position at all** (spec: "qty ≥ min_qty"). The Position Manager holds no asset rules. The ledger closes a position below `min_qty` as soon as all its orders settle, so an open position below `min_qty` always has an unsettled order, and that already blocks flat.
5. **A failure is counted once, from the attempt's outcome.** On later steps an unsettled `not_submitted`/`rejected` sell row only means "may attempt again when the backoff is due" (spec 4.4 table, row 2).
6. **No attempt while the journal position's qty is 0.** All coins are sold and the position waits for its other orders to settle. A late fill raises the qty and makes the exit attemptable again.
7. **`OrderResult` gains `min_qty` and `no_position`.** The Position Manager needs `min_qty` for the dust rules in 4.4 (re-arm, journal ≥ `min_qty` counts as a failure), and the external-sale rule needs to tell "no position to exit" apart from other rejections. Both are set by `exit()` from data it already read.
8. **`request_flatten()` keeps an already pending request**, for the CLI as well as for `shutdown.flatten`. The CLI says so and waits for that request's result. It still sets the pause.
9. **`lock.core_running(url)`** probes the core's journal lock with a non-blocking `flock` and writes nothing (unlike `acquire()`, which rewrites the holder line).
10. **The flat check runs at the end of the step**, after the per-position exit work, so one pass can latch, sell and see the result of a previous pass's sale.
11. **`cancel_open_entries(exclude=...)`**: the Position Manager passes the rows whose cancel the broker already accepted. Rows that failed are retried on the next killed or flatten step. Rows without a broker id are returned as `unresolved`; the Position Manager logs `positions.entry_unresolved` once per row per process.
12. **`ledger._verify` becomes the public `verify_unit`**, and the Position Manager's events subclass `LedgerEvent` (unit-exact commit verification, the 1.5b pattern).
13. **No trailing update on a step where a trigger fires.** The trigger is evaluated against the stored values first (spec 4.3.1).
14. **`positions.flatten_pending` and the non-flat `flatten_result` refresh both run every 10 s** (the first at once), not every step.
15. **External retries wait for the sync loop** (dry run, owner 2026-10-19: spec rule kept). A failed external attempt leaves an id-less `rejected` (or `not_submitted`) row, and that symbol does not qualify again until the ledger settles the row (spec 4.4.1: no unsettled order of either side). In `run core` that takes up to 2 s, so external retries run every max(backoff, about 2 s).
16. **Lock-first writes for the flatten state** (dry run, owner 2026-10-19). On SQLite (pysqlite's legacy mode), `BEGIN` is only issued at the first DML, so a guard's `SELECT` would run before the write lock. Every read-check-write of `flatten_requested`/`flatten_result` therefore starts with `flatten.lock_flatten_state(session)`, a no-op `UPDATE control_state SET key = key WHERE key = 'flatten_requested'`. It takes SQLite's write lock before the guard reads. The writes that need it:
    - the completion unit;
    - the non-flat refresh;
    - `request_flatten`;
    - `cancel_flatten`.

    A concurrent `flatten --cancel` from another process then either commits before the guard reads (and the guard sees it) or waits for our commit. Postgres needs `SELECT ... FOR UPDATE` instead; the Postgres ownership work in WP 3.5 owns that.
17. **Error containment beyond positions** (dry run). The whole control phase is wrapped (`positions.control_failed`), and so is each external `exit()` call (`positions.external_failed`, which counts a backoff failure). A raising external sale or cancel never stops the position exits on the same step (spec 4.3).
18. **Decisions the dry run had to make:**
    - `positions.entry_unresolved` is only logged when cancels run (the broker can trade).
    - A latch whose guard matched nothing skips the position for this step.
    - Cleared control values are written as JSON null, not deleted rows.
    - External-sale backoff is a separate per-symbol dict.
    - `core_running` probes with `LOCK_SH | LOCK_NB` on an `O_RDONLY` descriptor.
    - Log assertions use `structlog.testing.capture_logs`.

## Review Focus

1. **Quote older than `feed.stale_alert_s` on a symbol the feed reports fresh (bars keep arriving):** no price trigger fires on that bid (Task 9, `test_an_old_quote_on_a_fresh_symbol_triggers_nothing`).
2. **Bid exactly at the stop, the TP or the trailing activation price:** each fires or activates; one tick below the boundary does not (Task 7, parametrized boundary tests).
3. **Restart while an exit is in flight:** a fresh Position Manager never sends a second sell for a latched position whose sell has a broker id and is still open (Task 9, `test_a_restart_honours_the_latch_and_never_double_sells`).
4. **Kill cleared while a cancel had failed:** after the clear, no further cancels are sent, and triggers already latched with `kill` stay latched and are exited (Task 10, `test_clearing_the_kill_stops_cancels_and_keeps_latched_exits`).
5. **`exit()` raising for one position** (journal error, programming error): logged `positions.step_failed`; the other positions are still evaluated and exited on the same step (Task 9, `test_a_failing_position_never_stops_the_others`).

---

## File Structure

- `src/autotrader/broker/base.py` (modify): `OrderMethodsUnavailable`, `Broker.can_place_orders()`.
- `src/autotrader/broker/sim.py`, `src/autotrader/broker/alpaca.py` (modify): `can_place_orders()`; Alpaca stubs raise `OrderMethodsUnavailable`.
- `src/autotrader/risk/types.py` (modify): `ExitRequest.external`, `CancelResult`.
- `src/autotrader/risk/checks.py` (modify): typed dust rejection in `check_exit`, `atr` in check 7's `adjusted`.
- `src/autotrader/risk/engine.py` (modify): `OrderResult.min_qty/no_position`, `can_place_orders()`, `cancel_open_entries()`, `sellable_positions()`, `Accounting.apply_all -> frozenset[str]`, `_submit` maps `OrderMethodsUnavailable`.
- `src/autotrader/journal/models.py` (modify) + `migrations/versions/20261019_0005_exit_management.py` (create).
- `src/autotrader/journal/repo.py` (modify): `insert_order(external=)` and the Position Manager's reads/writes.
- `src/autotrader/core/ledger.py` (modify): per-symbol isolation, `atr_at_entry`, external sales, `verify_unit`.
- `src/autotrader/core/feed.py` (modify): `price_age_s()`.
- `src/autotrader/core/exit_rules.py` (create): pure trigger evaluation, sell-row classification, backoff.
- `src/autotrader/core/flatten.py` (create): flatten request/result in `control_state`.
- `src/autotrader/core/lock.py` (modify): `core_running()`.
- `src/autotrader/core/position_manager.py` (create): `PositionManager`, `ExitTriggered`, `FlattenCompleted`.
- `src/autotrader/core/runtime.py` (modify): two loops, the `flatten` teardown step.
- `src/autotrader/cli.py` (modify): `autotrader flatten`.
- Tests: `tests/pm_harness.py` (create; shared by the two Position Manager test files), `tests/unit/core/test_exit_rules.py`, `tests/unit/core/test_flatten.py`, `tests/unit/core/test_position_manager.py`, `tests/unit/core/test_position_manager_control.py` (create); existing broker, risk, journal, ledger, feed, lock, runtime and CLI test modules.
- Docs: `SPEC.md`, `PLAN.md`, `CLAUDE.md`, `docs/runbook.md` (create; `docs/` has no operations file today), stale comments (Task 13).

---

### Task 1: A broker that cannot place orders

**Files:**
- Modify: `src/autotrader/broker/base.py`, `src/autotrader/broker/sim.py`, `src/autotrader/broker/alpaca.py`, `src/autotrader/risk/engine.py`
- Test: `tests/unit/broker/test_alpaca.py`, `tests/unit/broker/test_sim.py`, `tests/unit/risk/test_engine.py`, `tests/unit/core/test_runtime.py` (fake broker only)

**Interfaces:**
- Produces: `OrderMethodsUnavailable(BrokerError)`; `Broker.can_place_orders() -> bool` (sync); `SimBroker.can_place_orders()` is `True`, `AlpacaBroker.can_place_orders()` is `False` until WP 1.6; `RiskEngine.can_place_orders() -> bool` delegates. `RiskEngine._submit` records `OrderMethodsUnavailable` as `not_submitted`.

- [ ] **Step 1: Write the failing tests**

`tests/unit/broker/test_alpaca.py` (import `OrderMethodsUnavailable` from `autotrader.broker.base`, `Awaitable`/`Callable` from `collections.abc`):

```python
async def test_order_methods_raise_the_typed_unavailable_error() -> None:
    broker = await connect(FakeClient())
    assert broker.can_place_orders() is False
    calls: list[Callable[[], Awaitable[object]]] = [  # lazy: no un-awaited coroutine on failure
        lambda: broker.close_position("BTC/USD"),
        lambda: broker.cancel_order("x"),
        lambda: broker.get_order("x"),
    ]
    for call in calls:
        with pytest.raises(OrderMethodsUnavailable, match=r"is not implemented until WP 1\.6"):
            await call()
```

`tests/unit/broker/test_sim.py`:

```python
def test_sim_can_place_orders() -> None:
    assert make_sim().can_place_orders() is True
```

`tests/unit/risk/test_engine.py` (import `OrderMethodsUnavailable`):

```python
class UnavailableBroker(SimBroker):
    async def close_position(self, symbol: str) -> OrderAck:
        raise OrderMethodsUnavailable("AlpacaBroker.close_position is not implemented until WP 1.6")

    def can_place_orders(self) -> bool:
        return False


async def test_an_exit_on_a_broker_that_cannot_trade_is_not_submitted(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    broker = _sim(UnavailableBroker)
    await _held_btc(broker)
    engine = make_engine_(broker, sessions, gate)
    assert engine.can_place_orders() is False
    result = await engine.exit(ExitRequest(symbol=BTC, reason="stop"))
    assert (result.status, result.broker_order_id) == ("not_submitted", None)
    (row,) = await orders(sessions)
    assert row.status == "not_submitted"  # never "unknown": nothing left the process
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/broker/test_alpaca.py tests/unit/broker/test_sim.py tests/unit/risk/test_engine.py -q -k "unavailable or can_place or cannot_trade"`
Expected: FAIL (`ImportError: cannot import name 'OrderMethodsUnavailable'`).

- [ ] **Step 3: Implement**

- `base.py`: `class OrderMethodsUnavailable(BrokerError)`, docstring: the adapter cannot place, cancel or read orders yet (Alpaca until WP 1.6); nothing was sent, so callers record `not_submitted` (D18 decision 10). Add `def can_place_orders(self) -> bool: ...` to `Broker`.
- `sim.py`: `can_place_orders()` returns `True`.
- `alpaca.py`: `can_place_orders()` returns `False` (comment: true with the WP 1.6 order methods); `_not_yet` raises `OrderMethodsUnavailable` with the unchanged message, so the existing `match=` tests keep passing.
- `engine.py`: `can_place_orders()` returns `self._broker.can_place_orders()`. In `_submit`, add `except OrderMethodsUnavailable` (log `risk.order_not_submitted` with `reason="broker cannot place orders"`, status `not_submitted`) after the `OrderRejected` branch and before the generic `except Exception`.
- `tests/unit/core/test_runtime.py`: give `FakeBroker` `def can_place_orders(self) -> bool: return False` (the Position Manager calls it from Task 12; adding it now keeps the fake complete).

- [ ] **Step 4: Run the full check** (command above). Expected: all green, `risk/` 100 %.

- [ ] **Step 5: Commit**

```bash
git add src/autotrader/broker src/autotrader/risk/engine.py tests/unit/broker tests/unit/risk/test_engine.py tests/unit/core/test_runtime.py
git commit -m "feat: typed OrderMethodsUnavailable and Broker.can_place_orders (D18 decision 10)"
```

---

### Task 2: Migration 0005 and external sells

**Files:**
- Create: `src/autotrader/journal/migrations/versions/20261019_0005_exit_management.py`
- Modify: `src/autotrader/journal/models.py`, `src/autotrader/journal/repo.py`, `src/autotrader/risk/types.py`, `src/autotrader/risk/engine.py`
- Test: `tests/unit/journal/test_db.py`, `tests/unit/risk/test_engine.py`

**Interfaces:**
- Produces: `Position.atr_at_entry`, `trail_high`, `trail_stop` (`Mapped[Decimal | None]`), `Position.exit_trigger` (`Mapped[str | None]`, `String(16)`), `Position.exit_triggered_at` (`Mapped[datetime | None]`); `Order.external: Mapped[bool]` (default False, `server_default=sqlalchemy.false()`); `ExitRequest.external: bool = False`; `repo.insert_order(..., external: bool = False)`. `exit()` writes `request.external` to the order row.

- [ ] **Step 1: Write the failing tests**

`tests/unit/journal/test_db.py` (reuse `_ORDER_0003` and `_run_sql`; the 0003-shaped insert still works after downgrading to 0004):

```python
def test_0005_marks_existing_orders_not_external(db_url: str) -> None:
    downgrade(db_url, "0004")
    _run_sql(db_url, _ORDER_0003.format(id=1, broker="'b-1'", status="filled"))
    upgrade(db_url)

    async def external() -> list[bool]:
        engine = make_engine(db_url)
        try:
            async with make_sessionmaker(engine)() as s:
                return [o.external for o in await s.scalars(select(m.Order))]
        finally:
            await engine.dispose()

    assert asyncio.run(external()) == [False]
```

`test_migration_matches_models` (existing) then guards the model/migration match.

`tests/unit/risk/test_engine.py`:

```python
async def test_an_external_exit_marks_its_order_row(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    await _held_btc(sim)
    result = await make_engine_(sim, sessions, gate).exit(
        ExitRequest(symbol=BTC, reason="flatten", external=True)
    )
    assert result.status == "new"
    (row,) = await orders(sessions)
    assert (row.external, row.exit_reason, row.cycle_id) == (True, "flatten", None)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/journal/test_db.py tests/unit/risk/test_engine.py -q -k "0005 or external_exit or matches_models"`
Expected: FAIL (`ValidationError` on `external` for `ExitRequest`, `AttributeError: external` on the order row).

- [ ] **Step 3: Implement**

- Migration `0005` (revises `0004`), following 0004's style. Batch-add the five nullable `positions` columns (`Money()` for the three prices, `sa.String(length=16)`, `UTCDateTime(timezone=True)`) and `orders.external` (`sa.Boolean()`, `server_default=sa.false()`, `nullable=False`). No backfill (spec section 3). `downgrade()` drops them in reverse.
- `models.py`: the columns above, with a comment block like 1.5b's (WP 1.5c: durable exit state; `external` = sell of a broker holding with no journal position, decision 13). Update `ControlState`'s docstring: keys `paused, paused_until, kill, mode, cash_check, flatten_requested, flatten_result`.
- `risk/types.py`: `ExitRequest.external: bool = False`, docstring: a sell of a broker holding with no journal position (flatten/kill-flatten only).
- `repo.insert_order`: keyword `external: bool = False`, stored on the row.
- `engine.exit()`: pass `external=request.external`.

- [ ] **Step 4: Run the full check.** Expected: green.

- [ ] **Step 5: Commit**

```bash
git add src/autotrader/journal src/autotrader/risk tests/unit/journal/test_db.py tests/unit/risk/test_engine.py
git commit -m "feat: migration 0005: durable exit state and external sells (D18)"
```

---

### Task 3: Exit outcomes the Position Manager can act on, ATR at entry

**Files:**
- Modify: `src/autotrader/risk/checks.py`, `src/autotrader/risk/engine.py`, `src/autotrader/core/ledger.py`
- Test: `tests/unit/risk/test_checks.py`, `tests/unit/risk/test_engine.py`, `tests/unit/core/test_ledger.py`

**Interfaces:**
- Produces: `check_exit` rejects a dust-only holding with reason `f"dust only in {symbol}"` (a missing one stays `f"no position to exit in {symbol}"`); `OrderResult` gains `min_qty: Decimal | None = None` and `no_position: bool = False`. `exit()` sets `dust`/`min_qty`/`no_position` on a `check_exit` rejection from the broker position it read, only when the mode matches, and sets `dust=True, min_qty=e.min_qty` on `DustRemainder`. Check 7 adds `"atr": str(market.atr)` to `adjusted`. The ledger copies `adjusted["atr"]` into `positions.atr_at_entry`.

- [ ] **Step 1: Write the failing tests**

`tests/unit/risk/test_checks.py`: in `test_exit_rejections`, change the dust row to `([held(BTC, qty="0.0005")], True, "dust only in BTC/USD")`, and add:

```python
def test_an_approved_entry_records_the_atr_it_was_sized_with() -> None:
    d = run()
    assert d.approved and d.adjusted["atr"] == "2"
```

`tests/unit/risk/test_engine.py`:

```python
async def test_exit_outcomes_say_whether_the_broker_held_dust_or_nothing(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    nothing = await make_engine_(_sim(), sessions, gate).exit(ExitRequest(symbol=BTC, reason="stop"))
    assert (nothing.no_position, nothing.dust, nothing.min_qty) == (True, False, None)

    dusty = SimBroker(  # base fee: a 0.0001 buy nets 0.00009975 < min_qty
        cash=D("1000"),
        fees=FeeSchedule(maker_bps=D(15), taker_bps=D(25), buy_fee_asset="base"),
        slippage_bps=D(0),
        asset_rules={BTC: RULES.model_copy(update={"min_notional": D(0)})},  # 0.01 USD buy
        timeframe="5m",
    )
    dusty.process_bar(five(29))
    await dusty.submit_order(BrokerOrder(symbol=BTC, side="buy", qty=D("0.0001"), type="market"))
    dusty.process_bar(five(30))
    dust = await make_engine_(dusty, sessions, gate).exit(ExitRequest(symbol=BTC, reason="stop"))
    assert (dust.no_position, dust.dust, dust.min_qty) == (False, True, RULES.min_qty)
    assert dust.decision.reasons == ("dust only in BTC/USD",) and dust.order_row_id is None


async def test_a_mode_mismatch_is_neither_dust_nor_no_position(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    await _held_btc(sim)
    sim.broker_mode = "live"
    result = await make_engine_(sim, sessions, gate).exit(ExitRequest(symbol=BTC, reason="stop"))
    assert (result.dust, result.no_position, result.min_qty) == (False, False, None)
```

Extend `test_a_dust_close_is_reported_as_dust` with `assert result.min_qty == RULES.min_qty`.

`tests/unit/core/test_ledger.py`: in `test_full_entry_opens_the_position_and_fills_the_cycle` the approved decision is written with `adjusted={"initial_risk": "5"}`. Add a test next to it that writes `{"initial_risk": "5", "atr": "2"}` and asserts `pos.atr_at_entry == D("2")`. Extend the existing test with `assert pos.atr_at_entry is None` (no `atr` key: no trailing).

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/risk tests/unit/core/test_ledger.py -q -k "exit_rejections or records_the_atr or dust or mode_mismatch or atr_at_entry or full_entry"`
Expected: FAIL.

- [ ] **Step 3: Implement**

- `check_exit`: split the `position is None or position.is_dust` branch into the two reasons.
- `engine.exit()`: on rejection, look up the requested symbol in `held` and return `OrderResult(decision, dust=..., min_qty=..., no_position=...)`. All three keep their defaults when the mode does not match (a mismatch is a failure, not dust). In `_submit`'s `DustRemainder` branch, add `min_qty=e.min_qty`.
- `checks._size_and_finish`: add `"atr": str(market.atr)` to `adjusted` (check 6 already rejected a `None` ATR).
- `ledger.py`: replace `_initial_risk` with `async def _entry_adjusted(session, cycle_id) -> dict[str, Any]` (the same query; `{}` when there is no cycle or decision). `_buy` reads `initial_risk` and `atr` from it through one small `_opt_dec(value: object) -> Decimal | None` helper and sets `atr_at_entry`.

- [ ] **Step 4: Run the full check.** Expected: green, `risk/` 100 %.

- [ ] **Step 5: Commit**

```bash
git add src/autotrader/risk src/autotrader/core/ledger.py tests/unit/risk tests/unit/core/test_ledger.py
git commit -m "feat: typed dust/no-position exit outcomes and ATR frozen at entry (D18)"
```

---

### Task 4: Per-symbol failure isolation in the ledger (1.5b follow-up a)

**Files:**
- Modify: `src/autotrader/core/ledger.py`, `src/autotrader/risk/engine.py`
- Test: `tests/unit/core/test_ledger.py`, `tests/unit/risk/test_engine.py`

**Interfaces:**
- Produces: `Ledger.apply_all(result: SyncResult) -> frozenset[str]`, the symbols whose booking raised on this pass (spec 4.1). `Accounting.apply_all` has the same signature. `check()` raises `SyncIncomplete` when the sync is incomplete **or** that set is non-empty.

- [ ] **Step 1: Write the failing tests**

`tests/unit/core/test_ledger.py` (add `ETH = "ETH/USD"`, a `make_sim` variant with rules for both symbols if the module has none, and `from sqlalchemy.exc import IntegrityError`):

```python
class FailingRowLedger(Ledger):
    def __init__(self, *args: Any, failing: int, **kw: Any) -> None:
        super().__init__(*args, **kw)
        self.failing = failing

    async def apply(self, update: OrderUpdate) -> Any:
        if update.order_row_id == self.failing:
            raise IntegrityError("INSERT", None, Exception("boom"))
        return await super().apply(update)


async def test_a_failing_row_holds_back_its_symbol_only(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    sim = make_sim()  # with BTC and ETH rules and a first bar for each
    btc, _ = await place(sessions, sim, "buy", "1")
    eth, _ = await place(sessions, sim, "buy", "1", symbol=ETH)
    sim.process_bar(bar(1))
    sim.process_bar(bar(1, symbol=ETH))
    btc_exit, _ = await place(sessions, sim, "sell", "1")  # a later BTC row: held back too
    ledger = FailingRowLedger(bus, sessions, clock=lambda: T0, failing=btc)

    failed = await ledger.apply_all(await sync(sessions, sim))

    assert failed == frozenset({BTC})
    by_id = {o.id: o for o in await orders(sessions)}
    assert by_id[eth].settled_at is not None  # the other symbol booked
    assert by_id[btc].settled_at is None and by_id[btc_exit].settled_at is None
```

(If `place`/`bar` take no `symbol` yet, add a `symbol: str = BTC` keyword to both helpers in this task.)

`tests/unit/risk/test_engine.py`: change `FailingLedger.apply_all` to return `frozenset[str]`, and add:

```python
class OneSymbolFailed:
    async def apply_all(self, result: SyncResult) -> frozenset[str]:
        return frozenset({"ETH/USD"})


async def test_a_symbol_the_ledger_could_not_book_fails_every_entry_closed(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    engine = make_engine_(sim, sessions, gate, ledger=OneSymbolFailed())
    result = await engine.check(intent(cycle_id))  # a BTC entry
    assert result.decision.reasons == ("state unavailable: SyncIncomplete",)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/core/test_ledger.py tests/unit/risk/test_engine.py -q -k "failing_row or could_not_book or failing_ledger"`
Expected: FAIL (the `IntegrityError` propagates out of `apply_all`; the engine ignores the returned set).

- [ ] **Step 3: Implement**

- `apply_all`: wrap `await self.apply(update)` in `try/except Exception`. On an exception, `log.exception("ledger.row_failed", order_row=row.id, symbol=symbol)`, add the symbol to `held` and to the failed set, and continue. Return `frozenset(failed)`. Update the docstring (spec 4.1: one row's failure holds back its symbol only; `CancelledError` is never caught).
- `engine.py`: `Accounting.apply_all(...) -> frozenset[str]`. In `_gather_entry`: `failed = await self._ledger.apply_all(synced)`, then raise `SyncIncomplete` when `not synced.complete or failed`, naming both in the message. Update the module docstring paragraph on the sync accordingly.

- [ ] **Step 4: Run the full check.** Expected: green.

- [ ] **Step 5: Commit**

```bash
git add src/autotrader/core/ledger.py src/autotrader/risk/engine.py tests/unit/core/test_ledger.py tests/unit/risk/test_engine.py
git commit -m "fix: a ledger row failure holds back its symbol only; check() fails closed on it"
```

---

### Task 5: Ledger books external sales (decision 13, spec 4.4.1)

**Files:**
- Modify: `src/autotrader/core/ledger.py`
- Test: `tests/unit/core/test_ledger.py`

**Interfaces:**
- Consumes: `Order.external` (Task 2).
- Produces: `ExternalHoldingSold(LedgerEvent)` with `order_row_id: int`, `symbol: str`, `qty: Decimal`, `dust_consumed: Decimal`, `external_qty: Decimal`, `proceeds_usd: Decimal`, `fees_usd: Decimal`. The public name `verify_unit` replaces `_verify` (same body; update the `LedgerEvent` docstring: also used by the Position Manager's events). `Ledger.apply()` logs `positions.external_sold` (warning) after an external sale's settlement unit committed.

- [ ] **Step 1: Write the failing tests**

Give the `place()` helper an `external: bool = False` keyword passed to `repo.insert_order`. Add a helper that buys at the sim with no journal row (an external holding):

```python
async def hold_externally(sim: SimBroker, qty: str, minute: int = 1) -> None:
    """A holding the journal doesn't know: bought at the sim with no order row."""
    await sim.submit_order(BrokerOrder(symbol=BTC, side="buy", qty=D(qty), type="market"))
    sim.process_bar(bar(minute))


async def set_dust(sessions: async_sessionmaker[AsyncSession], qty: str) -> None:
    async with sessions() as s:
        s.add(m.SymbolDust(symbol=BTC, qty=D(qty), updated_at=T0))
        await s.commit()


async def test_an_external_sale_books_fills_and_cash_but_no_position(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    await hold_externally(sim, "1")
    await set_dust(sessions, "0.0003")
    await place(sessions, sim, "close", "1", exit_reason="flatten", external=True)
    sim.process_bar(bar(2))

    assert await ledger.apply_all(await sync(sessions, sim)) == frozenset()

    (order,) = await orders(sessions)
    assert order.settled_at is not None and order.position_id is None
    assert await positions(sessions) == []
    assert [f.side for f in await fills(sessions)] == ["sell"]
    assert await dust_balance(sessions) == D(0)
    assert not await paused(sessions)
    (sold,) = [e.payload for e in await event_rows(sessions) if e.type == "ExternalHoldingSold"]
    assert (D(sold["qty"]), D(sold["dust_consumed"]), D(sold["external_qty"])) == (
        D("1"),
        D("0.0003"),
        D("0.9997"),
    )
    assert (D(sold["proceeds_usd"]), D(sold["fees_usd"])) == (D("100"), D("0.25"))


async def test_a_dust_balance_above_the_sold_qty_is_left_as_a_residual(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    await hold_externally(sim, "1")
    await set_dust(sessions, "2")
    await place(sessions, sim, "close", "1", exit_reason="flatten", external=True)
    sim.process_bar(bar(2))
    await ledger.apply_all(await sync(sessions, sim))
    assert await dust_balance(sessions) == D("1")  # consumed min(1, 2); 1.5d corrects the rest


async def test_external_dust_is_consumed_once_across_partial_fills(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    await hold_externally(sim, "1")
    await set_dust(sessions, "0.0003")
    sim.script(BTC, "sell", FillPlan((D("0.5"),)))
    await place(sessions, sim, "close", "1", exit_reason="flatten", external=True)
    sim.process_bar(bar(2))
    await ledger.apply_all(await sync(sessions, sim))  # 0.5 booked, not settled
    assert await dust_balance(sessions) == D("0.0003")
    sim.process_bar(bar(3))
    await ledger.apply_all(await sync(sessions, sim))  # rest booked, settled
    await ledger.apply_all(await sync(sessions, sim))  # a repeated pass changes nothing
    assert await dust_balance(sessions) == D(0)
    assert (await event_types(sessions)).count("ExternalHoldingSold") == 1


async def test_a_marked_sell_against_an_open_position_books_normally_and_oversells(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    cycle = await new_cycle(sessions)
    await place(sessions, sim, "buy", "1", cycle_id=cycle)
    sim.process_bar(bar(1))
    await ledger.apply_all(await sync(sessions, sim))
    await hold_externally(sim, "1", minute=2)  # broker now holds 2, the journal position 1
    await place(sessions, sim, "close", "2", exit_reason="flatten", external=True)
    sim.process_bar(bar(3))
    await ledger.apply_all(await sync(sessions, sim))
    assert anomaly_kind(await event_rows(sessions)) == "oversell"
    (pos,) = await positions(sessions)
    assert pos.realized_pnl is None  # no external proceeds in a trade's P&L
```

`test_a_marked_sell_against_an_open_position_books_normally_and_oversells` is a **regression pin**: it already passes before this task (it pins the 1.5b path that marked sells must keep), so Step 2 expects the other three to fail and this one to pass. The existing `test_a_sell_without_any_position_is_an_anomaly` and `test_a_sell_without_a_position_waits_for_an_earlier_unsettled_order` keep pinning the unmarked behaviour.

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/core/test_ledger.py -q -k "external or residual or marked_sell"`
Expected: FAIL (`sell_without_position` anomaly).

- [ ] **Step 3: Implement**

- `_sell` returns `Position | None`. In the no-candidate branch: deferral first (unchanged), then `if order.external: return None` (decision 13: booked as an external sale), else the `sell_without_position` anomaly.
- In `_book`'s settlement block: when `order.side == "sell" and order.external and order.position_id is None`, call `await self._external_sale(session, order, now)` and append a non-None result to `extra`; also store it on `unit.external` (new `_Unit` field `external: ExternalHoldingSold | None = None`).
- `_external_sale(session, order, now) -> ExternalHoldingSold | None`. It flushes, reads the order's `FillRow`s and computes `sold = sum(qty)`, returning `None` if `sold == 0`. It then reads `SymbolDust`, sets `consumed = min(sold, balance)` and lowers the balance by `consumed` (normalised, `updated_at = now`). When `balance > sold` it logs `ledger.dust_residual(symbol, residual=str(balance - sold))`. Finally it returns the event with `proceeds_usd = _norm(sum(qty * price))`, `fees_usd = _norm(sum(fee))` and `external_qty = sold - consumed`.
- `apply()`: after the unit committed, `if unit.external is not None: log.warning("positions.external_sold", order_row=..., symbol=..., external_qty=..., dust_consumed=...)`.
- Rename `_verify` to `verify_unit` throughout the module. Optionally route `_guard`'s generation through the Task 10 repo helper later; leave `_guard` as is here.
- Module docstring: one paragraph on external sales (no position, no P&L, dust consumed at settlement).

- [ ] **Step 4: Run the full check.** Expected: green.

- [ ] **Step 5: Commit**

```bash
git add src/autotrader/core/ledger.py tests/unit/core/test_ledger.py
git commit -m "feat: ledger books external sales without a position or P&L (D18 decision 13)"
```

---

### Task 6: Engine cancel and holdings reads, feed price age

**Files:**
- Modify: `src/autotrader/risk/types.py`, `src/autotrader/risk/engine.py`, `src/autotrader/core/feed.py`
- Test: `tests/unit/risk/test_engine.py`, `tests/unit/core/test_feed.py`

**Interfaces:**
- Produces:
  - `CancelResult(_Frozen)` in `risk/types.py`: `requested: tuple[int, ...] = ()` (rows whose cancel the broker accepted), `failed: tuple[int, ...] = ()`, `unresolved: tuple[int, ...] = ()` (non-final buys without a broker id).
  - `RiskEngine.cancel_open_entries(exclude: Collection[int] = ()) -> CancelResult`: under the exit lock with its own `io_timeout_s` budget. It reads `repo.open_entry_orders`, skips ids in `exclude` and rows whose status is in `FINAL`, and lists id-less rows as `unresolved`. For each other row it calls `broker.cancel_order` through `_within`, logging `risk.entry_cancel_requested` on success and `risk.cancel_failed` (exception) on failure. It never touches sells or order rows.
  - `RiskEngine.sellable_positions() -> tuple[HeldPosition, ...]`: the non-dust result of `_broker_positions` with its own budget; lock-free; raises on failure.
  - `MarketFeed.price_age_s(symbol: str) -> float | None`: `(clock() - observed).total_seconds()`, `None` if never observed (the same `_observed` map `is_stale` uses).

- [ ] **Step 1: Write the failing tests**

`tests/unit/risk/test_engine.py` (`Collection` is not needed in tests):

```python
class CancelSpy(SimBroker):
    def __init__(self, *args: Any, **kw: Any) -> None:
        super().__init__(*args, **kw)
        self.cancelled: list[str] = []
        self.fail: set[str] = set()

    async def cancel_order(self, order_id: str) -> None:
        if order_id in self.fail:
            raise OrderRejected("cannot cancel")
        self.cancelled.append(order_id)
        await super().cancel_order(order_id)


async def _entry_row(
    sessions: async_sessionmaker[AsyncSession], status: str, broker_id: str | None, side: str = "buy"
) -> int:
    async with sessions() as s:
        row = await repo.insert_order(
            s, cycle_id=None, client_order_id=None, symbol=BTC, mode="paper", type_="limit",
            side=side, qty=D("1"), limit_price=D("50"), stop_price=None, tp_price=None,
            status="pending", now=NOW,
        )
        await repo.update_order(s, row.id, status=status, now=NOW, broker_order_id=broker_id)
        return row.id


async def test_cancel_open_entries_cancels_open_buys_only(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    broker = _sim(CancelSpy)
    assert isinstance(broker, CancelSpy)
    await _held_btc(broker)
    resting = await broker.submit_order(
        BrokerOrder(symbol=BTC, side="buy", qty=D("1"), type="limit", limit_price=D("50"))
    )
    failing = await broker.submit_order(
        BrokerOrder(symbol=BTC, side="buy", qty=D("1"), type="limit", limit_price=D("51"))
    )
    broker.fail.add(failing.order_id)
    sell = await broker.close_position(BTC)
    open_row = await _entry_row(sessions, "new", resting.order_id)
    failed_row = await _entry_row(sessions, "new", failing.order_id)
    unacked = await _entry_row(sessions, "unknown", None)
    await _entry_row(sessions, "not_submitted", None)  # final: skipped
    await _entry_row(sessions, "new", sell.order_id, side="sell")  # never a sell
    skipped = await _entry_row(sessions, "new", "sim-999")
    engine = make_engine_(broker, sessions, gate)

    result = await engine.cancel_open_entries(exclude={skipped})

    assert result == CancelResult(requested=(open_row,), failed=(failed_row,), unresolved=(unacked,))
    assert broker.cancelled == [resting.order_id]
    assert (await broker.get_order(sell.order_id)).status == "new"


async def test_sellable_positions_leave_out_dust(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    engine = make_engine_(sim, sessions, gate)
    assert await engine.sellable_positions() == ()
    await _held_btc(sim)
    (held,) = await engine.sellable_positions()
    assert (held.symbol, held.qty, held.market_value) == (BTC, D("1"), None)
```

Add one dust case to the second test with a base-fee `SimBroker` whose only holding nets `0.00009975`: it returns `()`. Also add a budget test: `cancel_open_entries` on an engine with `io_timeout_s=0.0` and an open buy row lists the row as `failed` (the spent budget raises `TimeoutError` inside `_within`).

`tests/unit/core/test_feed.py`, using the module's existing feed factory and clock helpers:

```python
async def test_price_age_is_seconds_since_the_newest_valid_quote_or_bar(...) -> None:
    # never observed -> None; after a quote at t, at t + 5 s -> 5.0;
    # a newer bar end moves it; a rejected (crossed) quote does not.
```

Write it with exact equality on floats built from whole seconds.

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/risk/test_engine.py tests/unit/core/test_feed.py -q -k "cancel_open or sellable or price_age"`
Expected: FAIL (`ImportError: cannot import name 'CancelResult'`; `AttributeError: price_age_s`).

- [ ] **Step 3: Implement** the three methods and `CancelResult` as specified in Interfaces. Module docstring of `engine.py`: one sentence on `cancel_open_entries` (exit lock, own budget) and `sellable_positions`.

- [ ] **Step 4: Run the full check.** Expected: green, `risk/` 100 % (cover `exclude`, `FINAL`, id-less, failure, success).

- [ ] **Step 5: Commit**

```bash
git add src/autotrader/risk src/autotrader/core/feed.py tests/unit/risk/test_engine.py tests/unit/core/test_feed.py
git commit -m "feat: engine cancel_open_entries/sellable_positions and feed price age (D18)"
```

---

### Task 7: Pure exit rules (`core/exit_rules.py`)

**Files:**
- Create: `src/autotrader/core/exit_rules.py`
- Test: `tests/unit/core/test_exit_rules.py`

**Interfaces:**
- Produces:
  - `ExitState` (frozen dataclass): `entry_price: Decimal`, `stop: Decimal | None`, `tp: Decimal | None`, `time_limit_at: datetime | None`, `atr_at_entry: Decimal | None`, `trail_high: Decimal | None`, `trail_stop: Decimal | None`.
  - `TrailUpdate` (frozen dataclass): `trail_high: Decimal`, `trail_stop: Decimal`.
  - `ExitRules` (frozen dataclass): `trailing: Trailing` (from `core/config.py`), `stale_exit_enabled: bool`, `stale_exit_s: float`.
  - `evaluate(state: ExitState, bid: Decimal | None, price_age_s: float | None, now: datetime, rules: ExitRules) -> tuple[ExitReason | None, TrailUpdate | None]`. `bid` None means no trusted price; `price_age_s` None means unknown age (never stale).
  - `evaluate_bar(state: ExitState, bar: Bar) -> ExitReason | None`: bar-based callers only (tests, WP 1.7).
  - `SellState = Literal["attempt", "unresolved", "in_flight", "settling"]` and `classify_sells(rows: Sequence[SellRow]) -> SellState`, where `SellRow` is a Protocol with **read-only properties** `broker_order_id -> str | None` and `status -> str`. Plain protocol attributes would not be satisfied by SQLAlchemy `Mapped` attributes or a frozen dataclass; with properties, an `Order` row and the tests' `Row` both satisfy it. `rows` are the symbol's unsettled sell rows.
  - `Backoff` (mutable dataclass): `failures: int = 0`, `next_at: datetime | None = None`; `due(now) -> bool`; `fail(now) -> bool` (records a failure, returns True when an alert is due); `reset()`.

- [ ] **Step 1: Write the failing tests**

```python
"""Pure exit rules (spec 2026-10-19 section 4.3.1, 4.4)."""

from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal as D

import pytest

from autotrader.core.config import Trailing
from autotrader.core.exit_rules import (
    Backoff, ExitRules, ExitState, TrailUpdate, classify_sells, evaluate, evaluate_bar,
)
from autotrader.core.types import Bar

T0 = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)
ON = ExitRules(Trailing(enabled=True, trigger_r=D("1"), trail_atr=D("1.5")), False, 300)
STATE = ExitState(  # R = 5, activation at 105, trail distance 1.5 x 2 = 3
    entry_price=D("100"), stop=D("95"), tp=D("110"), time_limit_at=T0 + timedelta(hours=8),
    atr_at_entry=D("2"), trail_high=None, trail_stop=None,
)


@pytest.mark.parametrize(
    ("bid", "reason"),
    [("95", "stop"), ("95.01", None), ("110", "take_profit"), ("109.99", None), ("94", "stop")],
)
def test_price_triggers_fire_at_their_boundary(bid: str, reason: str | None) -> None:
    assert evaluate(STATE, D(bid), 0.0, T0, ON)[0] == reason


def test_trailing_activates_at_exactly_trigger_r_and_only_rises() -> None:
    assert evaluate(STATE, D("104.99"), 0.0, T0, ON) == (None, None)
    assert evaluate(STATE, D("105"), 0.0, T0, ON) == (None, TrailUpdate(D("105"), D("102")))
    up = replace(STATE, trail_high=D("106"), trail_stop=D("103"))
    assert evaluate(up, D("104"), 0.0, T0, ON) == (None, None)  # no lower high, no lower stop
    assert evaluate(up, D("103"), 0.0, T0, ON) == ("trailing", None)


def test_the_trailing_stop_never_sits_below_the_stop() -> None:
    wide = replace(STATE, atr_at_entry=D("10"))  # 105 - 15 = 90 < 95
    assert evaluate(wide, D("105"), 0.0, T0, ON) == (None, TrailUpdate(D("105"), D("95")))
    # trail_stop == stop: the reason stays "stop"
    assert evaluate(replace(wide, trail_high=D("105"), trail_stop=D("95")), D("95"), 0.0, T0, ON)[
        0
    ] == "stop"


@pytest.mark.parametrize(
    "state",
    [replace(STATE, atr_at_entry=None), replace(STATE, stop=None), replace(STATE, stop=D("100"))],
)
def test_no_trailing_without_atr_stop_or_positive_r(state: ExitState) -> None:
    assert evaluate(state, D("108"), 0.0, T0, ON) == (None, None)  # above 105, below the TP


def test_trailing_disabled_by_config() -> None:
    off = replace(ON, trailing=Trailing(enabled=False, trigger_r=D("1"), trail_atr=D("1.5")))
    assert evaluate(STATE, D("108"), 0.0, T0, off) == (None, None)


def test_precedence_and_time_without_a_price() -> None:
    late = replace(STATE, time_limit_at=T0)
    assert evaluate(late, D("95"), 0.0, T0, ON)[0] == "stop"  # stop beats time
    assert evaluate(late, D("110"), 0.0, T0, ON)[0] == "take_profit"  # TP beats time
    assert evaluate(late, None, None, T0, ON)[0] == "time"
    assert evaluate(late, None, None, T0 - timedelta(seconds=1), ON)[0] is None


def test_stale_fires_only_when_enabled_and_past_the_threshold() -> None:
    stale_on = replace(ON, stale_exit_enabled=True)
    assert evaluate(STATE, None, 300.0, T0, stale_on)[0] is None
    assert evaluate(STATE, None, 300.5, T0, stale_on)[0] == "stale"
    assert evaluate(STATE, None, 10_000.0, T0, ON)[0] is None
    assert evaluate(STATE, None, None, T0, stale_on)[0] is None
    assert evaluate(replace(STATE, time_limit_at=T0), None, 999.0, T0, stale_on)[0] == "time"


def test_no_trailing_update_on_a_firing_step() -> None:
    assert evaluate(replace(STATE, tp=D("105")), D("105"), 0.0, T0, ON) == ("take_profit", None)


def _bar(low: float, high: float) -> Bar:
    return Bar(symbol="BTC/USD", timeframe="1m", ts=T0, open=100, high=high, low=low, close=100,
               volume=1)


def test_a_bar_through_stop_and_tp_is_a_stop_first() -> None:
    assert evaluate_bar(STATE, _bar(94, 111)) == "stop"
    assert evaluate_bar(STATE, _bar(96, 111)) == "take_profit"
    assert evaluate_bar(STATE, _bar(96, 109)) is None


@dataclass(frozen=True)
class Row:  # satisfies SellRow
    broker_order_id: str | None
    status: str


@pytest.mark.parametrize(
    ("rows", "state"),
    [
        ([], "attempt"),
        ([Row(None, "not_submitted")], "attempt"),
        ([Row(None, "rejected")], "attempt"),
        ([Row("b", "rejected")], "attempt"),
        ([Row(None, "pending")], "unresolved"),
        ([Row(None, "unknown"), Row("b", "new")], "unresolved"),
        ([Row("b", "new")], "in_flight"),
        ([Row("b", "partially_filled"), Row("c", "filled")], "in_flight"),
        ([Row("b", "filled")], "settling"),
        ([Row("b", "canceled"), Row(None, "not_submitted")], "settling"),
    ],
)
def test_sell_rows_classify_by_precedence(rows: list[Row], state: str) -> None:
    assert classify_sells(rows) == state


def test_backoff_doubles_to_a_30_s_cap_and_alerts_at_3_then_every_10th() -> None:
    b = Backoff()
    now = T0
    delays: list[float] = []
    alerts: list[int] = []
    for n in range(1, 25):
        assert b.due(now)
        if b.fail(now):
            alerts.append(n)
        assert b.next_at is not None
        delays.append((b.next_at - now).total_seconds())
        assert not b.due(now)
        now = b.next_at
    assert delays[:7] == [1, 2, 4, 8, 16, 30, 30]
    assert alerts == [3, 13, 23]
    b.reset()
    assert (b.failures, b.next_at, b.due(T0)) == (0, None, True)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/core/test_exit_rules.py -q`
Expected: FAIL (`ModuleNotFoundError: autotrader.core.exit_rules`).

- [ ] **Step 3: Implement `core/exit_rules.py`**

The module docstring states the D18 rules it implements (decisions 1-4, 7) and that it is pure. `ExitReason` is imported from `autotrader.risk.types`. The two non-obvious bodies:

```python
def evaluate(state, bid, price_age_s, now, rules):
    reason: ExitReason | None = None
    if bid is not None:
        stops = [s for s in (state.stop, state.trail_stop) if s is not None]
        if stops and bid <= max(stops):
            trailing = state.trail_stop is not None and (
                state.stop is None or state.trail_stop > state.stop
            )
            reason = "trailing" if trailing else "stop"
        elif state.tp is not None and bid >= state.tp:
            reason = "take_profit"
    if reason is None and state.time_limit_at is not None and now >= state.time_limit_at:
        reason = "time"
    if (
        reason is None
        and rules.stale_exit_enabled
        and price_age_s is not None
        and price_age_s > rules.stale_exit_s
    ):
        reason = "stale"
    if reason is not None or bid is None:
        return reason, None
    return None, _trail(state, bid, rules.trailing)


def _trail(state, bid, cfg) -> TrailUpdate | None:
    if not cfg.enabled or state.atr_at_entry is None or state.stop is None:
        return None
    r = state.entry_price - state.stop
    if r <= 0:
        return None
    if state.trail_high is None and bid < state.entry_price + cfg.trigger_r * r:
        return None
    high = bid if state.trail_high is None else max(state.trail_high, bid)
    floor = [state.stop, high - cfg.trail_atr * state.atr_at_entry]
    stop = max([*floor, state.trail_stop] if state.trail_stop is not None else floor)
    if (high, stop) == (state.trail_high, state.trail_stop):
        return None
    return TrailUpdate(trail_high=high, trail_stop=stop)
```

`classify_sells`: an id-less row is `attempt` when its status is `not_submitted`/`rejected`, else `unresolved`. A row with an id is `in_flight` when its status is not in `FINAL`, `attempt` when `rejected`, else `settling`. The result is the highest of the set by precedence `unresolved > in_flight > settling > attempt` (`attempt` for no rows).
`Backoff.fail`: `failures += 1`; `next_at = now + timedelta(seconds=min(2 ** (failures - 1), 30))`; alert when `failures == 3 or (failures > 3 and (failures - 3) % 10 == 0)`.
`evaluate_bar` uses `Decimal(str(bar.low))` / `Decimal(str(bar.high))` and the same effective stop and reason rule; TP only when the stop did not fire.

- [ ] **Step 4: Run the full check.** Expected: green.

- [ ] **Step 5: Commit**

```bash
git add src/autotrader/core/exit_rules.py tests/unit/core/test_exit_rules.py
git commit -m "feat: pure exit rules: triggers, trailing, same-bar rule, sell states, backoff (D18)"
```

---

### Task 8: Flatten request/result and the core probe

**Files:**
- Create: `src/autotrader/core/flatten.py`
- Modify: `src/autotrader/core/lock.py`
- Test: `tests/unit/core/test_flatten.py`, `tests/unit/core/test_lock.py`

**Interfaces:**
- Produces (`core/flatten.py`):
  - `FLATTEN_REQUESTED_KEY = "flatten_requested"`, `FLATTEN_RESULT_KEY = "flatten_result"`.
  - `FlattenRequest` (frozen dataclass): `source: str`, `reason: str | None`, `ts: str` (ISO 8601; the request's identity); `as_json() -> dict[str, str | None]`.
  - `parse_request(value: object) -> FlattenRequest | None`: None for None; a malformed value is logged `flatten.request_malformed` (error) and returns None.
  - `async def read_request(session: AsyncSession) -> FlattenRequest | None`; `async def read_result(session: AsyncSession) -> dict[str, Any] | None`.
  - `async def request_flatten(sessions, *, source: str, reason: str | None, now: datetime) -> tuple[FlattenRequest, bool]`: one transaction. It sets `paused = True` and writes a new request unless one is pending, then returns `(the pending or new request, created)` (clarification 8).
  - `async def cancel_flatten(sessions, *, now: datetime) -> FlattenRequest | None`: one transaction. It clears the request and writes `flatten_result = {"request_ts", "flat": False, "cancelled": True, "ts"}`. The pause is never touched. Returns the cancelled request, or None if none was pending (then it writes nothing).
  - `async def wait_for_result(sessions, request_ts: str, *, wait_s: float, poll_s: float = 0.5) -> dict[str, Any] | None`: polls `read_result` until a result for `request_ts` is `flat` or `cancelled`, or `wait_s` (event-loop time) runs out. Returns the last result for that request seen, or None. It never sleeps past the deadline: `await asyncio.sleep(min(poll_s, deadline - loop.time()))`. A full `poll_s` sleep overran the shutdown budget in the dry run and forced the teardown.
  - `async def lock_flatten_state(session: AsyncSession) -> None`: executes `update(ControlState).where(ControlState.key == FLATTEN_REQUESTED_KEY).values(key=ControlState.key)` and does not commit. On SQLite this takes the write lock before the caller's guard reads (clarification 16). `request_flatten`, `cancel_flatten`, and the Position Manager's completion and non-flat refresh (Task 10) call it first.
- Produces (`core/lock.py`): `core_running(url: str) -> bool`. It probes with a non-blocking `flock` on the lock file and writes nothing. A non-file journal (`canonical_journal` raises) is False, since no core can run on it. A missing lock file is False.

- [ ] **Step 1: Write the failing tests**

`tests/unit/core/test_flatten.py` (migrated SQLite fixtures as in `test_ledger.py`):

```python
async def test_a_request_sets_the_pause_and_keeps_a_pending_one(sessions) -> None:
    first, created = await request_flatten(sessions, source="cli", reason="r", now=T0)
    again, created_again = await request_flatten(sessions, source="shutdown", reason=None, now=T1)
    assert (created, created_again, again) == (True, False, first)
    assert first == FlattenRequest(source="cli", reason="r", ts=T0.isoformat())
    async with sessions() as s:
        assert await repo.get_control(s, PAUSED_KEY) is True
        assert await read_request(s) == first


async def test_cancel_clears_the_request_and_keeps_the_pause(sessions) -> None:
    req, _ = await request_flatten(sessions, source="cli", reason=None, now=T0)
    assert await cancel_flatten(sessions, now=T1) == req
    assert await cancel_flatten(sessions, now=T1) is None
    async with sessions() as s:
        assert await read_request(s) is None
        assert await read_result(s) == {
            "request_ts": req.ts, "flat": False, "cancelled": True, "ts": T1.isoformat(),
        }
        assert await repo.get_control(s, PAUSED_KEY) is True


async def test_wait_returns_the_matching_final_result(sessions) -> None:
    req, _ = await request_flatten(sessions, source="cli", reason=None, now=T0)
    async with sessions() as s:
        await repo.set_control(s, FLATTEN_RESULT_KEY, {"request_ts": "other", "flat": True})
    assert await wait_for_result(sessions, req.ts, wait_s=0.0, poll_s=0.0) is None

    async def complete() -> None:
        await asyncio.sleep(0)
        async with sessions() as s:
            await repo.set_control(s, FLATTEN_RESULT_KEY, {"request_ts": req.ts, "flat": True})

    waiter = asyncio.create_task(wait_for_result(sessions, req.ts, wait_s=5.0, poll_s=0.0))
    await complete()
    assert await waiter == {"request_ts": req.ts, "flat": True}


async def test_the_lock_first_write_blocks_other_writers_until_commit(sessions, db_url) -> None:
    other = create_async_engine(db_url, connect_args={"timeout": 0.05})  # a second process
    try:
        async with sessions() as s:
            await lock_flatten_state(s)  # holds SQLite's write lock, uncommitted
            with pytest.raises(OperationalError, match="locked"):
                async with async_sessionmaker(other)() as o:
                    await repo.set_control(o, FLATTEN_REQUESTED_KEY, None)
            await s.commit()
        async with async_sessionmaker(other)() as o:  # released: the write goes through
            await repo.set_control(o, FLATTEN_REQUESTED_KEY, None)
    finally:
        await other.dispose()


def test_wait_never_sleeps_past_its_deadline() -> None:
    # wait_for_result(wait_s=0.01, poll_s=10.0) on a journal without a result returns None
    # well before poll_s: assert the loop time it took is < 1.0 (an upper bound far from the
    # value under test, not a timing assertion on the deadline itself).
    ...


async def test_a_malformed_request_reads_as_none(sessions) -> None:
    async with sessions() as s:
        await repo.set_control(s, FLATTEN_REQUESTED_KEY, ["junk"])
        assert await read_request(s) is None
```

(Annotate `sessions: async_sessionmaker[AsyncSession]` in the real file.)

`tests/unit/core/test_lock.py`:

```python
def test_core_running_probes_without_writing(tmp_path: Path) -> None:
    url = f"sqlite+aiosqlite:///{tmp_path / 'j.db'}"
    assert core_running(url) is False
    held = acquire(url, wait_s=0)
    try:
        before = held.lock_path.read_text()
        assert core_running(url) is True
        assert held.lock_path.read_text() == before
    finally:
        held.release()
    assert core_running(url) is False
    assert core_running("sqlite+aiosqlite:///:memory:") is False
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/core/test_flatten.py tests/unit/core/test_lock.py -q`
Expected: FAIL (`ModuleNotFoundError`, `ImportError: core_running`).

(Imports for the lock test: `create_async_engine` and `async_sessionmaker` from `sqlalchemy.ext.asyncio`, `OperationalError` from `sqlalchemy.exc`. Write the elided wait test as an async test from its comment.)

- [ ] **Step 3: Implement** both modules per Interfaces. `request_flatten`/`cancel_flatten` first call `lock_flatten_state`, then read, `session.merge(ControlState(...))` and one `commit()` (`repo.set_control` commits per key, so it can't make two keys atomic). The `core/flatten.py` docstring covers the protocol (spec 4.7): request identity is `ts`; the core clears the request on completion; `--cancel` never lifts the pause.

- [ ] **Step 4: Run the full check.** Expected: green.

- [ ] **Step 5: Commit**

```bash
git add src/autotrader/core/flatten.py src/autotrader/core/lock.py tests/unit/core/test_flatten.py tests/unit/core/test_lock.py
git commit -m "feat: flatten request/result in control_state and a read-only core probe (D18)"
```

---

### Task 9: Position Manager: triggers, latch and the exit path

**Files:**
- Create: `src/autotrader/core/position_manager.py`, `tests/pm_harness.py`, `tests/unit/core/test_position_manager.py`
- Modify: `src/autotrader/journal/repo.py`
- Test: `tests/unit/journal/test_repo.py` (repo helpers), the new test module

**Interfaces:**
- Consumes: Tasks 1-8 (`ExitRequest`, `OrderResult` with `dust/min_qty/no_position`, `RiskEngine.can_place_orders`, `exit_rules`, `verify_unit`, `LedgerEvent`, `MarketFeed.price_age_s`).
- Produces (`repo.py`):
  - `open_positions(session) -> list[Position]`: `closed_at IS NULL`, id order, `populate_existing`.
  - `set_time_limit(session, position_id: int, at: datetime) -> None`: a guarded `UPDATE ... WHERE id = ? AND time_limit_at IS NULL`; commits.
  - `update_trailing(session, position_id: int, *, trail_high: Decimal, trail_stop: Decimal) -> None`: guarded by `closed_at IS NULL`; touches only these two columns; commits.
  - `latch_exit(session, position_id: int, *, reason: str, at: datetime) -> bool`: guarded by `exit_trigger IS NULL AND closed_at IS NULL`; **no commit** (runs inside `publish_tx`); returns `rowcount == 1`.
- Produces (`core/position_manager.py`):
  - `ExitTriggered(LedgerEvent)`: `position_id: int`, `symbol: str`, `reason: str`, `bid: Decimal | None` (`cycle_id` = the position's).
  - Protocols `ExitEngine` (`exit`, `cancel_open_entries(exclude: Collection[int] = ())`, `sellable_positions`, `can_place_orders`), `ExitMarketView` (`quote`, `is_stale`, `price_age_s`) and `ControlSource`. `ControlSource` declares `latest` as a `@property` returning `ControlStatus | None`, so `ControlPoller`'s plain attribute and the harness dataclass both satisfy it.
  - `_as_reason(value: str) -> ExitReason`: `Position.exit_trigger` is a `str` column and `ExitRequest.reason` the `ExitReason` Literal (mypy rejects passing one as the other). It checks the value against `typing.get_args(ExitReason)` and `cast`s; an unknown value raises `ValueError`, which the per-position `try` logs as `positions.step_failed`.
  - `PositionManager(risk: ExitEngine, market: ExitMarketView, sessions, bus: EventBus, controls: ControlSource, *, profile: StrategyProfile, config: AppConfig, clock: Callable[[], datetime] = lambda: datetime.now(UTC))`; `async def step(self) -> None`. It records `clock()` at construction as its start time (cold-start stale grace).
  - Log events: `positions.broker_cannot_trade` (warning, once per process), `positions.unprotected` (warning, once per position), `positions.stale_held` (warning, once per stale episode), `positions.exit_triggered` (warning), `positions.latch_uncertain` (error), `positions.exit_dust` (warning), `positions.exit_failing` (error), `positions.exit_unresolved` (error, every 60 s), `positions.exit_stuck` (error, after 60 s in flight, then every 60 s), `positions.step_failed` (exception).

**Step behaviour this task implements** (kill and flatten come in Task 10; leave a clearly named no-op hook `_control_phase` and `_flat_check` call site for them):

1. `can_trade = risk.can_place_orders()`; if false, log `positions.broker_cannot_trade` once.
2. Read `repo.open_positions` and `repo.unsettled_orders` in one session. For each position, inside `try/except Exception` (log `positions.step_failed` with `position_id`, `symbol`), run `_manage(pos, unsettled, now, can_trade)`:
   - If `time_limit_at` is None: compute `opened_at + timedelta(minutes=profile.risk.max_hold_min)`, write it with `set_time_limit`, and use it in this step.
   - If `stop` is None: log `positions.unprotected` once per position id.
   - If `exit_trigger` is None: build `ExitState`, compute the trusted bid and the price age (below), and call `evaluate`. Persist a `TrailUpdate` with `update_trailing`. On a reason, latch: `publish_tx` with `build` calling `latch_exit` and returning `ExitTriggered` (or None when the guard matched nothing), verified with `verify_unit`. Log `positions.exit_triggered`. On `CommitUncertain`, log `positions.latch_uncertain` and stop processing this position for this step.
   - If a trigger is latched (now or earlier) and `can_trade`: `_exit_path`.
3. Stale-held alert: for each open position's symbol, a symbol that `is_stale()` or whose `price_age_s()` exceeds `feed.stale_alert_s` logs `positions.stale_held` on the transition into staleness; leaving it re-arms the alert.

**Trusted bid:** `quote(symbol)` must exist, `is_stale(symbol)` must be false and `(now - quote.ts).total_seconds() <= feed.stale_alert_s`; then `Decimal(str(quote.bid))`, else None.
**Price age:** `price_age_s(symbol)`. When that is None, use `math.inf` once `(now - started).total_seconds() > feed.stale_exit_s`, else None.
**ExitRules:** `ExitRules(profile.risk.trailing, config.feed_stale_exit_enabled, config.feed.stale_exit_s)`.

**`_exit_path(pos, sells, now)`** (`sells` = the unsettled sell rows of the position's symbol):
- `state = classify_sells(sells)`.
  - `unresolved`: log `positions.exit_unresolved` (with the row ids) when 60 s have passed since the last such log for this position (the first one at once).
  - `in_flight`: log `positions.exit_stuck` when the oldest in-flight row's `submitted_at` is more than 60 s old, again at most every 60 s.
  - `settling`: wait.
  - `attempt`: go on below.
- Skip when `pos.qty == 0` (clarification 6). Skip when a dust hold for this position is armed: `(held_qty, min_qty)` stays armed until `pos.qty > held_qty and pos.qty >= min_qty`, which disarms it. Skip when the position's `Backoff` is not `due(now)`.
- `result = await risk.exit(ExitRequest(symbol=pos.symbol, reason=_as_reason(pos.exit_trigger), cycle_id=pos.cycle_id))`:
  - `result.dust`: if `result.min_qty is not None and pos.qty >= result.min_qty`, it is a failure (journal and broker disagree). Otherwise arm the dust hold with `(pos.qty, result.min_qty or pos.qty)` and log `positions.exit_dust`; no failure is counted.
  - `result.broker_order_id is not None and result.status not in ("rejected", "not_submitted")`: success; `backoff.reset()`.
  - `result.status in ("pending", "unknown")`: nothing (the next step classifies the row).
  - anything else (`not_submitted`, `rejected`, a decision rejection such as `no position to exit` or `state unavailable`): `backoff.fail(now)`. When that returns True, log `positions.exit_failing` with `position_id`, `symbol`, `failures` and `reasons=list(result.decision.reasons)` plus `status`.

- [ ] **Step 1: Write the harness** `tests/pm_harness.py` (shared with Task 10):

```python
"""Position Manager test harness: SimBroker + RiskEngine + Ledger + a scripted quote view."""

from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal as D
from pathlib import Path
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.broker.base import AssetRules, BrokerOrder, FeeSchedule
from autotrader.broker.sim import SimBroker
from autotrader.core.bus import EventBus
from autotrader.core.config import AppConfig, StrategyProfile, load_profile
from autotrader.core.kill import ControlStatus
from autotrader.core.ledger import Ledger
from autotrader.core.position_manager import PositionManager
from autotrader.core.types import Bar, Quote
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.risk.engine import RiskEngine

REPO = Path(__file__).resolve().parents[1]
PROFILE: StrategyProfile = load_profile(REPO / "strategies" / "fast_momentum_v1.yaml")
BTC, ETH = "BTC/USD", "ETH/USD"
T0 = datetime(2026, 10, 19, 12, 0, tzinfo=UTC)
RULES = AssetRules(
    min_qty=D("0.0001"), qty_increment=D("0.0001"), min_notional=D("1"), price_increment=D("0.01")
)
QUOTE_FEES = FeeSchedule(maker_bps=D("15"), taker_bps=D("25"), buy_fee_asset="quote")
BASE_FEES = FeeSchedule(maker_bps=D("15"), taker_bps=D("25"), buy_fee_asset="base")


def app_config(
    *, kill_flatten: bool | None = None, stale_exit: bool | None = None,
    shutdown_flatten: bool = False,
) -> AppConfig:
    return AppConfig.model_validate({
        "mode": "paper", "strategy": "s.yaml", "trading_day_tz": "UTC",
        "database": {"url": "sqlite+aiosqlite:///unused.db"},
        "control": {"kill_file": "KILL"}, "kill": {"flatten": kill_flatten},
        "feed": {"stale_alert_s": 60, "stale_exit_s": 300, "stale_exit_enabled": stale_exit},
        "shutdown": {"flatten": shutdown_flatten, "timeout_s": 30},
    })


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


@dataclass
class View:
    """ExitMarketView: quotes, stale symbols and explicit price ages."""

    clock: Clock
    quotes: dict[str, Quote] = field(default_factory=dict)
    stale: set[str] = field(default_factory=set)
    ages: dict[str, float] = field(default_factory=dict)

    def quote(self, symbol: str) -> Quote | None:
        return self.quotes.get(symbol)

    def is_stale(self, symbol: str) -> bool:
        return symbol in self.stale

    def price_age_s(self, symbol: str) -> float | None:
        if symbol in self.ages:
            return self.ages[symbol]
        q = self.quotes.get(symbol)
        return None if q is None else (self.clock.now - q.ts).total_seconds()


@dataclass
class Controls:
    latest: ControlStatus | None = None


class OpenGate:
    async def status(self, now: datetime | None = None) -> ControlStatus:
        return ControlStatus(False, ())


class NoPrices:  # RiskMarketView: exits never read the feed
    def history(self, symbol: str, timeframe: str) -> list[Bar]:
        return []

    def quote(self, symbol: str) -> Quote | None:
        return None

    def last_price(self, symbol: str) -> float | None:
        return None

    def is_stale(self, symbol: str) -> bool:
        return True


class Harness:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
        bus: EventBus,
        *,
        sim: SimBroker | None = None,
        config: AppConfig | None = None,
        profile: StrategyProfile = PROFILE,
        engine_cls: type[RiskEngine] = RiskEngine,
    ) -> None:
        self.sessions, self.bus = sessions, bus
        self.clock = Clock(T0)
        self.sim = sim or make_sim()
        self.minute = 0
        self.ledger = Ledger(bus, sessions, clock=self.clock)
        self.view = View(self.clock)
        self.controls = Controls()
        self.config = config or app_config()
        self.engine = engine_cls(
            self.sim, profile=profile, strategy_id=1, mode="paper", gate=OpenGate(),
            market=NoPrices(), sessions=sessions, ledger=self.ledger, clock=self.clock,
        )
        self.pm = self.new_pm(profile)

    def new_pm(self, profile: StrategyProfile = PROFILE) -> PositionManager:
        """A fresh manager on the same journal: a restart."""
        return PositionManager(
            self.engine, self.view, self.sessions, self.bus, self.controls,
            profile=profile, config=self.config, clock=self.clock,
        )

    def bar(self, symbol: str, price: float) -> None:
        """The next minute's bar: fills what is open for `symbol`."""
        self.minute += 1
        self.sim.process_bar(_bar(symbol, self.minute, price))

    def quote(self, symbol: str, bid: float, *, age_s: float = 0.0) -> None:
        ts = self.clock.now - timedelta(seconds=age_s)
        self.view.quotes[symbol] = Quote(symbol=symbol, ts=ts, bid=bid, ask=bid + 0.01)

    async def sync(self) -> None:
        await self.ledger.apply_all(await self.engine.sync_orders())

    async def open_position(
        self, symbol: str = BTC, qty: str = "1", *, stop: str | None = "95", tp: str | None = "110",
        atr: str | None = "2",
    ) -> int:
        """Journal an entry like check() does, fill it at 100 on the next bar, book it and
        return the position id. A FillPlan scripted before the call shapes the fill."""
        row_id, _ = await self.submit_entry(symbol, qty, limit=None, stop=stop, tp=tp, atr=atr)
        self.bar(symbol, 100.0)
        await self.sync()
        ...  # return the id of the position whose cycle is the entry row's cycle

    async def submit_entry(
        self, symbol: str = BTC, qty: str = "1", *, limit: str | None = "50", ack: bool = True,
        stop: str | None = "95", tp: str | None = "110", atr: str | None = "2",
    ) -> tuple[int, str]:
        """An entry at the broker, not booked: a GTC limit buy at `limit` rests (never fills
        at 100); `limit=None` is a market buy that fills on the symbol's next bar. With
        `ack=False` the journal row stays `pending` without a broker id (an un-acked submit).
        Returns (order row id, broker order id)."""
        ...  # cycle row (SUBMITTED); approved entry RiskDecision for it (adjusted:
        # initial_risk, plus atr when given); sim.submit_order; repo.insert_order with
        # stop_price/tp_price; if ack: repo.update_order(status=ack.status, broker_order_id=...)

    async def position(self, position_id: int) -> m.Position:
        async with self.sessions() as s:
            row = await s.get(m.Position, position_id, populate_existing=True)
            if row is None:
                raise LookupError(position_id)
            return row

    async def sells(self, symbol: str = BTC) -> list[m.Order]:
        async with self.sessions() as s:
            rows = await s.scalars(
                select(m.Order).where(m.Order.symbol == symbol, m.Order.side == "sell")
                .order_by(m.Order.id)
            )
            return list(rows)


def make_sim(fees: FeeSchedule = QUOTE_FEES, cls: type[SimBroker] = SimBroker) -> SimBroker:
    sim = cls(cash=D("100000"), fees=fees, slippage_bps=D(0), asset_rules={BTC: RULES, ETH: RULES})
    for symbol in (BTC, ETH):
        sim.process_bar(_bar(symbol, 0, 100.0))
    return sim


def _bar(symbol: str, minute: int, price: float) -> Bar:
    return Bar(symbol=symbol, timeframe="1m", ts=T0 + timedelta(minutes=minute), open=price,
               high=price, low=price, close=price, volume=1.0)


async def event_payloads(sessions: async_sessionmaker[AsyncSession], type_: str) -> list[Any]:
    async with sessions() as s:
        return [e.payload for e in await s.scalars(select(m.Event).where(m.Event.type == type_))]
```

Fill in the two elided bodies from their comments (the shape of `place()` in `test_ledger.py`). The docstrings are the contract. The dry run's choices, now fixed:
- `submit_entry` registers `PROFILE` as the strategy and creates the cycle as `SUBMITTED`.
- It writes `adjusted.initial_risk = qty * (100 - stop)` only when a stop is given, and `atr` only when one is given.
- It submits to the sim before inserting the journal row.
- `open_position` returns the entry row's `position_id`.

The harness config is fixed at construction: a test that needs another config sets `h.config` and builds `h.new_pm()`. Log assertions use `structlog.testing.capture_logs()`. Log assertions use `configure_logging(stream=buf)` and parse JSON lines, as in `test_runtime.py`.

- [ ] **Step 2: Write the failing tests** in `tests/unit/core/test_position_manager.py`. Fixtures `sessions` and `bus` as in `test_ledger.py`. Each test builds `h = Harness(sessions, bus, ...)`. Prices: entry 100, stop 95, TP 110, ATR 2, R 5, activation 105, trail distance 3, quote fees 25 bps.

```python
async def test_a_stop_exit_end_to_end(sessions, bus) -> None:
    h = Harness(sessions, bus)
    pid = await h.open_position()
    h.quote(BTC, 95.0)
    await h.pm.step()
    pos = await h.position(pid)
    assert (pos.exit_trigger, pos.exit_triggered_at) == ("stop", T0)
    assert pos.time_limit_at == pos.opened_at + timedelta(minutes=PROFILE.risk.max_hold_min)
    h.bar(BTC, 95.0)
    await h.sync()
    pos = await h.position(pid)
    assert (pos.closed_at is not None, pos.exit_reason, pos.realized_pnl) == (
        True, "stop", D("-5.4875"),  # -100.25 + 95 - 0.2375
    )
    (triggered,) = await event_payloads(sessions, "ExitTriggered")
    assert (triggered["reason"], D(triggered["bid"])) == ("stop", D("95"))
```

Further tests, each with the decisive assertions:

- `test_a_take_profit_exit`: quote 110 → latched `take_profit`; after the bar and sync, closed with `exit_reason == "take_profit"`.
- `test_a_time_exit_needs_no_price`: no quote. One step writes `time_limit_at` and latches nothing. Then `h.clock.now = pos.time_limit_at - timedelta(seconds=1)` and a step → nothing; `h.clock.now = pos.time_limit_at` and a step → latched `time`.
- `test_a_trailing_exit_after_activation_and_pullback`: quotes 106 → `(trail_high, trail_stop) == (D("106"), D("103"))`, no trigger; 104 → unchanged; 103 → latched `trailing`.
- `test_a_stale_exit_only_when_enabled`: `view.ages[BTC] = 301.0`; with `app_config()` (paper default off) nothing latches; with `app_config(stale_exit=True)` it latches `stale`.
- `test_a_never_priced_symbol_gets_the_cold_start_grace`: `stale_exit=True`, no quote, no age; the step at T0 latches nothing; after `h.clock.advance(301)` → `stale` (TP/stop untouched, time not reached).
- `test_an_old_quote_on_a_fresh_symbol_triggers_nothing` (Review Focus 1): `h.quote(BTC, 95.0, age_s=61)` (stale_alert_s 60), symbol not stale → no latch; `age_s=60` → `stop`.
- `test_a_partial_entry_is_protected_and_a_late_fill_is_exited`: `h.sim.script(BTC, "buy", FillPlan((D("0.5"),)))`, open (qty 0.5, entry unsettled), quote 95, step → one sell for 0.5. The next bar fills the entry remainder and the sell, then sync (position qty 0.5, open), step → second sell. After the bar and sync the position is closed with `exit_reason "stop"` and `len(await h.sells()) == 2`.
- `test_a_latched_stop_still_exits_after_the_price_recovers`: `h.sim.script(BTC, "sell", FillPlan(("reject",)))`; quote 95, step; bar + sync (the sell is rejected and settled); quote 100, step → a second sell; bar + sync → closed `stop`.
- `test_a_restart_honours_the_latch_and_never_double_sells` (Review Focus 3): quote 95, step (sell in flight, status `new`); `pm2 = h.new_pm(profile=<copy with max_hold_min=10>)`; `await pm2.step()` twice → still one sell row, `time_limit_at` unchanged; bar + sync → closed.
- `test_a_restart_re_attempts_at_once`: a broker whose `close_position` raises `OrderRejected` (subclass of `SimBroker`, via `make_sim(cls=...)`); one step → failure (backoff 1 s); `h.new_pm().step()` at the same clock → a second attempt (two exit rows).
- `test_failed_exits_back_off_and_alert_after_three`: close raising `OrderMethodsUnavailable`; quote 95; steps at t = 0, 0.5, 1, 2, 3 s after the first (`h.clock.advance`) → exit order rows after each step `[1, 1, 2, 2, 3]`; exactly one `positions.exit_failing` line, emitted at the third failure, with `failures == 3`.
- `test_an_unknown_exit_without_a_broker_id_is_never_retried`: close raising `RuntimeError` → row `unknown`; steps at +1, +30, +61 s → still one sell row; two `positions.exit_unresolved` lines (+1 and +61).
- `test_an_in_flight_exit_alerts_when_stuck`: a sell that never fills (no bars); steps at +30 and +61 s → one `positions.exit_stuck`, at +61.
- `test_dust_holds_back_until_a_late_fill_rearms_the_latched_exit`: `make_sim(BASE_FEES)` and `h.sim.script(BTC, "buy", FillPlan((D("0.0001"),)))` with qty 1. The position opens at `D("0.00009975")`. Quote 95, step → no sell row, one `positions.exit_dust`, one exit decision. Another step → still one exit decision. The bar fills the remainder; sync; step → one sell (no new trigger). One more step before its bar → still one sell. Bar + sync → closed `stop`.
- `test_dust_while_the_journal_holds_a_sellable_qty_counts_as_a_failure`: close raising `DustRemainder(BTC, D("0.00005"), RULES.min_qty)` on a 1 BTC position; three due attempts → one `positions.exit_failing`.
- `test_a_broker_that_cannot_trade_latches_but_attempts_nothing`: sim subclass with `can_place_orders()` False; quote 95, two steps → latched `stop`, no order rows, one `positions.broker_cannot_trade`.
- `test_an_unprotected_position_is_reported_once_and_still_times_out`: `open_position(stop=None, tp=None)`; two steps → one `positions.unprotected`; at `time_limit_at` → `time`.
- `test_stale_held_alerts_once_per_episode`: `view.stale = {BTC}`, two steps → one line; clear, one step, stale again → two lines in total.
- `test_a_failing_position_never_stops_the_others` (Review Focus 5): `engine_cls` = a `RiskEngine` subclass whose `exit` raises `RuntimeError` for BTC and delegates otherwise; BTC and ETH positions, both quoted at 95; one step → one `positions.step_failed` (symbol BTC), and an ETH sell row exists.
- `test_an_uncertain_latch_commit_is_retried_next_step`: build the harness on `hooked_sessionmaker(engine, hook)` (`tests/dbhooks.py`); step once at quote 100 (writes `time_limit_at`); quote 95, `hook.fail_before = 1`, step → `positions.latch_uncertain`, `exit_trigger` None; step → `stop`.

Coverage for paths the dry run left unreached (one short test each):
- `test_a_settling_exit_waits_for_the_ledger`: the sell filled at the sim, no sync → step makes no new sell.
- `test_a_fully_sold_position_is_not_exited_again`: script the buy with `FillPlan((D("0.5"), D("0.00001")))`. The second step's slice rounds to 0, so on that bar the entry fills nothing and stays open. Latch at 95; the sell fills 0.5 on the next bar; sync → position qty 0 with the entry unsettled → a step makes no new exit decision.
- `test_an_unknown_trigger_value_is_a_step_failure`: write `exit_trigger = "bogus"` directly → `positions.step_failed`, the other positions are still processed.

`tests/unit/journal/test_repo.py`: `open_positions` excludes closed rows; `set_time_limit` never overwrites a value; `update_trailing` does nothing on a closed position; `latch_exit` returns False on a latched or closed position and True once otherwise.

- [ ] **Step 3: Run them to verify they fail**

Run: `uv run pytest tests/unit/core/test_position_manager.py tests/unit/journal/test_repo.py -q`
Expected: FAIL (`ModuleNotFoundError: autotrader.core.position_manager`).

- [ ] **Step 4: Implement** the repo helpers and `core/position_manager.py` per the behaviour above. State kept in memory per process: `_backoff: dict[int, Backoff]`, `_dust: dict[int, tuple[Decimal, Decimal]]`, `_last_unresolved_log`/`_last_stuck_log: dict[int, datetime]`, `_unprotected: set[int]`, `_stale_held: set[str]`, `_cannot_trade_logged: bool`, `_started: datetime`. Module docstring: spec sections 4.3-4.4, decisions 1-5, 7, 10-12; what is durable (the latch, `time_limit_at`, the trailing values) and what is in memory (backoff, dust hold, alert throttles). Keep `step()` short by delegating to `_control_phase(...)` (Task 10 fills it; here it returns at once), `_manage(...)` and `_flat_check(...)` (Task 10; here not called).

- [ ] **Step 5: Run the full check.** Expected: green.

- [ ] **Step 6: Commit**

```bash
git add src/autotrader/core/position_manager.py src/autotrader/journal/repo.py tests/pm_harness.py tests/unit/core/test_position_manager.py tests/unit/journal/test_repo.py
git commit -m "feat: Position Manager: triggers, durable latch, exit retries and dust re-arm (D18)"
```

---

### Task 10: Position Manager: kill, external sales and flatten

**Files:**
- Modify: `src/autotrader/core/position_manager.py`, `src/autotrader/journal/repo.py`, `src/autotrader/core/ledger.py` (`_guard` uses the new generation helper)
- Test: `tests/unit/core/test_position_manager_control.py` (create), `tests/unit/journal/test_repo.py`

**Interfaces:**
- Consumes: `flatten` module (Task 8), `CancelResult` (Task 6), `ExternalHoldingSold` payloads (Task 5).
- Produces (`repo.py`): `order_fill_generation(session) -> tuple[int, int]` (`max(orders.id)`, `max(fills.id)`, 0 for none); `symbol_dust_balances(session) -> dict[str, Decimal]` (balances > 0, compared in Python); `external_sales_since(session, since: datetime) -> list[dict[str, Any]]` (settled `external` orders with `settled_at >= since`, id order; each is `{"order_row_id", "symbol", "external_qty", "dust_consumed", "proceeds_usd"}` from its `ExternalHoldingSold` payload, as strings); `last_anomaly_kinds(session, order_ids: Collection[int]) -> dict[int, str]` (latest `LedgerAnomaly.kind` per order row).
- Produces (`position_manager.py`): `FlattenCompleted(LedgerEvent)` with `request_ts: str`, `dust: dict[str, str]`, `external_sales: list[dict[str, Any]]`. Log events: `positions.entry_unresolved` (error, once per row), `positions.flatten_pending` (warning, every 10 s), `positions.flat` (info), `positions.flatten_read_failed` (exception), `positions.control_failed` (exception), `positions.external_failed` (exception), and `positions.exit_failing` with `external=True` for external sales.

**Control phase** (start of `step()`, after `can_trade` is known):
1. `request = flatten.read_request(...)`. `killed = controls.latest is not None and controls.latest.killed`.
2. If `(killed or request) and can_trade`: `result = await risk.cancel_open_entries(exclude=self._cancel_requested)`. Add `result.requested` to `_cancel_requested`, and log `positions.entry_unresolved` for each new id in `result.unresolved`. A raising call is logged and the step goes on.
3. Latch reasons: `kill` when `killed and config.kill_flatten`, else `flatten` when `request`; the kill takes precedence. With a reason, latch it (Task 9's latch) on every open position without a trigger, even when `can_trade` is false (decision 10).
4. External sales with the same reason, only when `can_trade`. Read `sellable_positions()` (the pass's first broker read; the flat check makes the second); a raising call is logged as `positions.flatten_read_failed`, and nothing is sold this step. A holding qualifies when its symbol has no open journal position and no unsettled order row of either side (4.4.1). For a qualifying symbol whose per-symbol `Backoff` (key `("ext", symbol)`) is due, call `risk.exit(ExitRequest(symbol=..., reason=reason, external=True))`:
   - `dust` or `no_position`: ends the attempt, no failure counted;
   - a broker id with an accepted status: `reset()`;
   - `pending`/`unknown`: nothing;
   - anything else: `fail(now)`, and on an alert log `positions.exit_failing` with `external=True`.
Containment (clarification 17): the whole control phase runs inside `try/except Exception` (log `positions.control_failed`). Each external `exit()` call has its own `try` (log `positions.external_failed` with the symbol, then `backoff.fail(now)`). A raising sale or cancel never keeps the position loop below from running.

5. The position loop (Task 9) runs next. Then, if `request`: `await self._flat_check(request, now, can_trade)`.

**`_flat_check(request, now, can_trade)`** (spec 4.7 step 4):
1. `g1 = order_fill_generation` (own session).
2. `holdings = await risk.sellable_positions()` (on an exception: log `positions.flatten_read_failed`, `holdings = None`).
3. In one session: `open_positions`, `unsettled_orders`, `last_anomaly_kinds` of those rows.
4. Flat iff `holdings == ()` and there are no open positions and no unsettled rows (clarification 4).
5. **Flat:** `publish_tx(build, verify_unit)`. `build` first calls `flatten.lock_flatten_state(session)` (clarification 16), then re-reads the generation and the current request, and returns None unless `generation == g1` and `read_request(...).ts == request.ts`. Otherwise it merges `flatten_result = {"request_ts", "flat": True, "dust": {symbol: str(qty)}, "external_sales": [...], "ts"}` and `flatten_requested = None`, and returns `FlattenCompleted`. Log `positions.flat` with the dust after a committed event; a `CommitUncertain` is logged and retried by the next step.
6. **Not flat**, at most every 10 s per request: build `remaining` with these keys:
   - `positions`: `[{position_id, symbol, qty}]`;
   - `holdings`: `[{symbol, qty}]`, or `"unavailable"`;
   - `orders`: `[{order_row_id, symbol, side, status, age_s, unresolved, anomaly}]`, where `unresolved` means no broker id and status `pending`/`unknown`.

   Log `positions.flatten_pending` with it, then write `flatten_result = {"request_ts", "flat": False, "remaining", "ts"}` plus `"reason": "broker cannot place orders"` when not `can_trade`. The write happens only if the request read in the same session, after `lock_flatten_state(session)`, still has `ts == request.ts`.

- [ ] **Step 1: Write the failing tests** in `tests/unit/core/test_position_manager_control.py` (harness from Task 9; `KILLED = ControlStatus(True, ("kill set",))`). Two test doubles, defined in this module:
- `CancelSpy(SimBroker)` records `cancels: list[str]` and raises `OrderRejected` for ids in `fail: set[str]` (the Task 6 pattern).
- `RacingEngine(RiskEngine)` has `callback: Callable[[], Awaitable[None]] | None` and `fire_on_call: int`. Its `sellable_positions` counts calls in the current test and awaits the callback once, right after the read, on call number `fire_on_call`. With a pending request and `can_place_orders()` true, a pass reads twice (external sales, then the flat check), so `fire_on_call=2` races the flat check. With `can_place_orders()` false, the flat check is the only read: `fire_on_call=1`.

```python
async def test_a_kill_cancels_open_entries_once_and_never_a_sell(sessions, bus) -> None:
    sim = make_sim(cls=CancelSpy)
    assert isinstance(sim, CancelSpy)
    h = Harness(sessions, bus, sim=sim)
    await h.open_position()
    h.quote(BTC, 95.0)
    await h.pm.step()  # a stop exit in flight (sell, status new)
    _, entry_id = await h.submit_entry(ETH)  # a resting limit buy at the broker
    h.controls.latest = KILLED
    await h.pm.step()
    await h.pm.step()
    (sell,) = await h.sells()
    assert sim.cancels == [entry_id]  # once, never the sell
    assert sell.broker_order_id is not None
    assert (await sim.get_order(sell.broker_order_id)).status == "new"
    assert (await sim.get_order(entry_id)).status == "canceled"
```

The remaining tests, each with the decisive assertions:

- `test_a_kill_already_set_at_startup_cancels_on_the_first_step`: `controls.latest = KILLED` before `h.new_pm().step()` → the entry is canceled at the sim.
- `test_kill_flatten_exits_every_position_with_reason_kill`: `app_config(kill_flatten=True)`, killed → latched `kill`; bar + sync → closed `kill`.
- `test_kill_flatten_exits_a_late_entry_fill`: `kill_flatten=True`, kill with no position, and `h.submit_entry(BTC, limit=None, ack=False)` (at the sim, journal row `pending` without a broker id). One step → one `positions.entry_unresolved`. Then ack it (`repo.update_order` with the broker id, status `new`), bar, sync → the next step latches `kill` and sells it.
- `test_without_kill_flatten_positions_keep_normal_management`: killed (paper default) → no latch at quote 100; quote 95 → `stop`.
- `test_clearing_the_kill_stops_cancels_and_keeps_latched_exits` (Review Focus 4): `kill_flatten=True`, the entry's cancel raises once (`sim.fail`), killed step → `kill` latched, cancel failed. Clear (`controls.latest = ControlStatus(False, ())`), step → no further cancel call; the position's sell exists; bar + sync → closed `kill`.
- `test_flatten_sells_positions_and_external_holdings_then_reports_flat`: BTC position, plus ETH held at the sim with no journal row (a `hold_externally` helper like Task 5's). Set `symbol_dust` BTC to `0.00005`. `request_flatten(sessions, source="cli", reason="t", now=T0)`; step → BTC latched `flatten`, an ETH sell with `external=True`. Bars for both, sync, step → in one transaction `flatten_result.flat is True` and `flatten_requested` is None. Its `dust` is `{"BTC/USD": "0.00005"}` and its `external_sales` has one ETH item with `external_qty == "1"`. `paused` stays True; one `FlattenCompleted` event; no `LedgerAnomaly`.
- `test_kill_flatten_sells_an_external_holding`: `kill_flatten=True`, killed, ETH held externally → ETH sell `external=True`, reason `kill`; after its bar, sync and a step, there is no second ETH sell, and an `ExternalHoldingSold` event exists.
- `test_an_external_holding_waits_for_an_unsettled_entry_on_its_symbol`: ETH held externally plus an id-less `pending` ETH buy row. Flatten step → no ETH sell; `flatten_result.remaining.orders` lists the row with `unresolved: true`. Mark the row `not_submitted` (`repo.update_order`), sync (it settles), step → the ETH sell; after its bar and sync, no `oversell` anomaly.
- `test_external_failures_back_off_per_symbol_and_alert_after_three`: a sim whose `close_position` raises `OrderRejected` for ETH; flatten request. Run `await h.sync()` before each step, at 0, 1 and 3 s: each failed attempt leaves an unsettled `rejected` row that blocks the symbol until the ledger settles it (clarification 15). Expect three ETH exit rows and one `positions.exit_failing` with `external: true`. Without the syncs only one row appears; assert that too, in the same test.
- `test_a_failing_external_sale_never_stops_position_exits` (clarification 17): flatten request, a BTC position, and an ETH external holding whose `exit()` raises `RuntimeError` (a `RiskEngine` subclass raising for ETH when `request.external`). One step → one `positions.external_failed`, and BTC is latched `flatten` with a sell row.
- Coverage of unreached paths, one short test each:
  - a `CommitUncertain` on the completion (`hook.fail_before = 1` just before the completing step) is logged, and the next step completes;
  - `sellable_positions` raising → `positions.flatten_read_failed` and `holdings: "unavailable"` in the refresh;
  - `cancel_open_entries` raising → `positions.control_failed`, and the position loop still runs;
  - an external exit returning `unknown` → no failure counted.
- `test_flat_is_not_reported_while_an_entry_is_unsettled_or_an_exit_unresolved`: an open entry (cancel requested, not yet synced) → not flat on that step. A position whose close raised `RuntimeError` (unknown, id-less) → not flat, with its row listed `unresolved: true`.
- `test_a_fill_landing_between_the_flat_check_reads_delays_completion`: `engine_cls=RacingEngine`, `fire_on_call=2`, with `h.submit_entry(ETH, limit=None, ack=False)` placed before the request. The callback acks that id-less pending ETH entry (broker id), bars it and syncs it (booked, settled, position open). No completion that step; the next steps latch `flatten` on the new position and exit it, and only then is `flat` reported. (This race is visible to the journal reads, so it never reaches the generation guard; the next test is the one that pins the guard.)
- `test_a_changed_generation_blocks_a_completion_that_looks_flat`: nothing held or open, `fire_on_call=2`. The callback inserts a local-final `not_submitted` order row and sets its `settled_at` directly in a session (nothing open) → no completion on that pass (`flatten_requested` still set); the next pass completes.
- `test_a_completion_never_overwrites_a_cancelled_request`: nothing held, `fire_on_call=2`; the callback calls `cancel_flatten` → `flatten_result` is the cancel record afterwards (with the request's ts), no `FlattenCompleted`.
- `test_a_pending_refresh_never_overwrites_a_cancel_record`: an unflattenable BTC position (sim with `can_place_orders()` False), `fire_on_call=1`. The callback cancels during the pass → after the step `flatten_result.cancelled is True`.
- `test_a_broker_that_cannot_trade_keeps_flatten_pending_with_a_reason`: `can_place_orders` False, a BTC position, request → latched `flatten`, no order rows, `flatten_result == {..., "flat": False, "reason": "broker cannot place orders", ...}`, one `positions.flatten_pending`; a second step 5 s later logs nothing new; one at +10 s logs again.

`tests/unit/journal/test_repo.py`: the four new helpers (generation with no rows is `(0, 0)`; dust balances drop zero rows; external sales filter by `settled_at` and `external`; anomaly kinds keep the latest per row).

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/core/test_position_manager_control.py tests/unit/journal/test_repo.py -q`
Expected: FAIL.

- [ ] **Step 3: Implement** per the behaviour above; `ledger._guard` calls `repo.order_fill_generation` for its generation (one definition of "generation"). Extend the module docstring with spec 4.4 kill handling, 4.4.1 and 4.7.

- [ ] **Step 4: Run the full check.** Expected: green.

- [ ] **Step 5: Commit**

```bash
git add src/autotrader/core/position_manager.py src/autotrader/core/ledger.py src/autotrader/journal/repo.py tests/unit/core/test_position_manager_control.py tests/unit/journal/test_repo.py
git commit -m "feat: Position Manager kill handling, external sales and guarded flatten passes (D18)"
```

---

### Task 11: `autotrader flatten`

**Files:**
- Modify: `src/autotrader/cli.py`
- Test: `tests/unit/test_cli.py`

**Interfaces:**
- Consumes: `request_flatten`, `cancel_flatten`, `wait_for_result` (Task 8), `lock.core_running` (Task 8).
- Produces: the `flatten` command, with options `--config/-c`, `--reason TEXT`, `--cancel` and a hidden `--wait-s FLOAT` (default 60.0). Exit codes:
  - 0: queued with no core running, flat, or `--cancel` done (including "nothing pending");
  - 1: still pending at the timeout, cancelled by someone else while waiting, or a database error.

- [ ] **Step 1: Write the failing tests**

Remove `["flatten"]` from `test_stub_commands_fail_loudly`'s parameters and update its comment, which also names `flatten`. `test_log_level_option_configures_json_logging` uses `flatten` as its stub and expects exit 2: change it to invoke `["--log-level", "debug", "review-now"]`. Then:

```python
def test_flatten_without_a_core_queues_the_request_and_pauses(tmp_path: Path) -> None:
    config = _tmp_config(tmp_path)
    assert runner.invoke(app, ["db", "upgrade", "-c", str(config)]).exit_code == 0

    result = runner.invoke(app, ["flatten", "-c", str(config), "--reason", "test"])

    assert result.exit_code == 0, result.output
    assert "queued" in result.output
    request, paused = _read_flatten_state(config)  # small asyncio.run helper over the journal
    assert request is not None and request["reason"] == "test" and paused is True


def test_flatten_with_a_running_core_waits_and_reports_pending(tmp_path: Path) -> None:
    config = _tmp_config(tmp_path)
    runner.invoke(app, ["db", "upgrade", "-c", str(config)])
    cfg = load_config(config, Settings(_env_file=None))
    held = journal_lock.acquire(cfg.database.url, wait_s=0)  # stands in for a core
    try:
        result = runner.invoke(app, ["flatten", "-c", str(config), "--wait-s", "0"])
    finally:
        held.release()
    assert result.exit_code == 1
    assert "pending" in result.output


def test_flatten_cancel_clears_the_request_and_keeps_the_pause(tmp_path: Path) -> None:
    # request (exit 0), then --cancel (exit 0, "cancelled"), then --cancel again
    # (exit 0, "no flatten request"); the journal keeps paused = True and
    # flatten_result.cancelled is True.
    ...


def test_flatten_twice_keeps_the_first_request(tmp_path: Path) -> None:
    # the second invocation prints "already pending" and leaves the first request's ts.
    ...
```

Write the two elided bodies from their comments. The "flat" exit path is covered end to end by `wait_for_result` (Task 8) and the runtime test in Task 12.

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/test_cli.py -q -k "flatten or stub"`
Expected: FAIL (exit code 2, "not implemented").

- [ ] **Step 3: Implement** the command (function name `flatten_command`, `@app.command("flatten")`; import the module as `from autotrader.core import flatten as flatten_ops`). Flow:
  1. Load the config.
  2. In one `asyncio.run`, open an engine and a sessionmaker (dispose in `finally`).
  3. With `--cancel`, run `cancel_flatten` and print either `flatten request cancelled (<ts>); entries stay paused` or `no flatten request pending`.
  4. Otherwise run `request_flatten(source="cli")` and print `flatten requested (<ts>); entries paused`, or `a flatten request is already pending (<ts>)` when one was.
  5. If `not core_running(url)`, print `no core is running: the request is queued until the next run core` and exit 0.
  6. Otherwise run `wait_for_result(wait_s)` and print one of:
     - `flat` with the dust;
     - `flatten cancelled`;
     - `still pending after <n> s:` followed by the result's `remaining` as JSON (or `no result yet`).

  Any exception prints `flatten failed (<Type>: <msg>)` and exits 1. The CLI module docstring sentence about stubs: `flatten` is no longer one.

- [ ] **Step 4: Run the full check.** Expected: green.

- [ ] **Step 5: Commit**

```bash
git add src/autotrader/cli.py tests/unit/test_cli.py
git commit -m "feat: autotrader flatten [--reason] [--cancel] (D18 decisions 6, 9)"
```

---

### Task 12: `run core` wiring and `shutdown.flatten`

**Files:**
- Modify: `src/autotrader/core/runtime.py`
- Test: `tests/unit/core/test_runtime.py`

**Interfaces:**
- Consumes: `PositionManager` (Tasks 9-10), `Ledger.apply_all -> frozenset[str]`, `flatten.request_flatten` / `wait_for_result`.
- Produces:
  - `run_core(..., sync_s: float = SYNC_S, manage_s: float = MANAGE_S)` with `SYNC_S = 2.0`, `MANAGE_S = 1.0`, `SYNC_STALL_S = 30.0` and `MANAGE_STALL_S = 30.0`.
  - Loops `Loop("ledger.sync", partial(_sync_step, risk, ledger), sync_s, SYNC_STALL_S)` and `Loop("positions.manage", pm.step, manage_s, MANAGE_STALL_S, "error")`.
  - `async def _sync_step(risk: RiskEngine, ledger: Ledger) -> None` runs `sync_orders` and then `apply_all`, and logs `ledger.sync_failed` (exception) on an error.
  - `async def _shutdown_flatten(sessions, control: ShutdownControl) -> None`.
  - `_teardown(..., flatten_step: Callable[[], Awaitable[None]] | None = None)` runs `bounded("flatten", flatten_step())` after the startup step and before `source.stop()`.

`_shutdown_flatten`:
1. Run `request, _ = await request_flatten(sessions, source="shutdown", reason=control.cause, now=datetime.now(UTC))`, which keeps a pending CLI request.
2. Run `wait_for_result(sessions, request.ts, wait_s=control.remaining() / 2)`.
3. Log `core.shutdown_flattened` (info, with the dust) when the result is flat. Otherwise log `core.shutdown_flatten_incomplete` (error, with `remaining`). It never raises for "not flat".

In `run_core`, set `flatten_step` only when `cfg.shutdown.flatten` and the supervisor exists. When `cfg.shutdown.flatten` is set but startup never completed, log `core.shutdown_flatten_skipped` (warning) instead.

The `Ledger` instance is shared by the engine and the sync loop. The Position Manager gets `feed` as its market, `poller` as its controls and `cfg` as its config. Build it right after the poller, still with no `await` after the `control.requested` check.

- [ ] **Step 1: Write the failing tests** (`tests/unit/core/test_runtime.py`)

`FakeBroker.can_place_orders` (Task 1) returns False and keeps no state. Add:

```python
class HangingSyncBroker(FakeBroker):
    """An unsettled order makes the sync loop read it; every read hangs."""

    def __init__(self) -> None:
        self.can_calls = 0
        self.order_reads = 0

    def can_place_orders(self) -> bool:
        self.can_calls += 1
        return False

    async def get_order(self, order_id: str) -> Any:
        self.order_reads += 1
        await asyncio.Event().wait()


async def test_a_hung_order_sync_never_delays_the_position_manager(cfg, lock) -> None:
    await _insert_unsettled_order(cfg)  # one buy row with broker id "b-1", status new
    broker = HangingSyncBroker()

    async def connect(_: Settings) -> Any:
        return broker

    control, _ = make_control(timeout_s=0.5)
    task = start_core(cfg, lock, control, connect_broker=connect, sync_s=0.01, manage_s=0.01)
    try:
        await _wait_for(lambda: broker.order_reads >= 1)  # the sync step is hanging now
        seen = broker.can_calls
        await _wait_for(lambda: broker.can_calls >= seen + 3)  # manage keeps stepping
    finally:
        control.begin("stop")
        assert await asyncio.wait_for(task, timeout=15) in (EXIT_CLEAN, EXIT_FAILED)
```

Further tests, each with the decisive assertions:

- `test_run_core_persists_bars_and_responds_to_kill_file` (existing) still exits `EXIT_CLEAN` with both new loops running and stopped.
- `test_shutdown_flatten_reports_flat_on_a_graceful_stop`: `cfg` copied with `shutdown.flatten=True` (`cfg.model_copy(update={"shutdown": cfg.shutdown.model_copy(update={"flatten": True})})`). FakeBroker holds nothing, so the core flattens to flat on stop → `core.shutdown_flattened` is logged, `flatten_result.flat is True`, and the exit code is `EXIT_CLEAN`.
- `test_shutdown_flatten_times_out_and_teardown_continues`: a broker that holds BTC (`get_positions` and `asset_rules` answer), `can_place_orders` False, `timeout_s=0.5` → `core.shutdown_flatten_incomplete` is logged, then `core.stopped`, with `EXIT_CLEAN`.
- `test_shutdown_flatten_keeps_a_pending_cli_request`: `request_flatten(source="cli")` before the start. Use a broker that holds BTC (answering `get_positions` and `asset_rules`) until `control.requested`, then holds nothing. Otherwise the core completes the CLI request on its first step, and the test depends on timing. After the stop, `flatten_result.request_ts` is the CLI request's ts and `flat is True`.
- `test_shutdown_flatten_is_skipped_after_a_startup_failure`: the failing broker connect with `shutdown.flatten=True` → `core.shutdown_flatten_skipped` is logged, and the error propagates as today.

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/core/test_runtime.py -q`
Expected: FAIL (unexpected keyword `sync_s`, missing log lines).

- [ ] **Step 3: Implement** per Interfaces. Update `runtime.py`'s module docstring. `exit()` is now called by the Position Manager (`positions.manage`), and the `ledger.sync` loop books fills every 2 s. `check()` still waits for 1.5d. `shutdown.flatten` runs as the `flatten` teardown step. Replace the sentence "Nothing calls `check()`/`exit()` until WP 1.5c/1.5d ...". `ControlPoller`'s docstring: the latest status now feeds the Position Manager.

- [ ] **Step 4: Run the full check.** Expected: green.

- [ ] **Step 5: Commit**

```bash
git add src/autotrader/core/runtime.py tests/unit/core/test_runtime.py
git commit -m "feat: run core runs the order-sync and Position Manager loops; shutdown.flatten (D18)"
```

---

### Task 13: Documentation alignment (spec section 7)

**Files:**
- Modify: `SPEC.md` (§0.6, §4.6, §4.7, §4.11, §7), `PLAN.md` (repo-state line, 1.5 row, 1.6 row), `CLAUDE.md` (commands), stale code comments
- Create: `docs/runbook.md`

- [ ] **Step 1: Sweep for falsified claims** (job 1 of the docs rule; fix every hit, code comments included):

```bash
rg -n --hidden "1\.5c|WP 1\.5\b|WP [0-9.]+/1\.5|Position Manager \(WP|until WP 1\.5|nothing calls|flatten.*1\.5|is a stub|stubs?\b.*flatten|close_all|not implemented|does not (trade|place orders)|Implementation boundary" src SPEC.md PLAN.md CLAUDE.md docs tests
```

Known hits to fix:
- `runtime.py` and `ControlPoller` docstrings (if not done in Task 12);
- `engine.py`'s module docstring ("the Position Manager manages it (WP 1.5c)" becomes present tense);
- `kill.py`'s docstring ("Acting on a kill ... is the core's job once the broker exists (WP 0.6/1.5)" becomes: the Position Manager cancels entries and, with `kill.flatten`, exits, WP 1.5c);
- `cli.py`'s docstring;
- `SPEC.md` §4.6 ("`AlpacaBroker` is currently read-only; its order methods are stubs": they raise `OrderMethodsUnavailable`, and `can_place_orders()` is false until 1.6);
- the `ledger.py` comment "a late fill can still arrive: stay open (1.5c protects it)" (now done: the Position Manager manages it);
- `PLAN.md`'s repo-state line ("`flatten` ... stubs", "Next: WP 1.5c");
- `SPEC.md` line 7 ("Implementation boundary": `run core` does not trade). It now runs the Position Manager's exits; add a dated update, since nothing places entries until 1.5d/1.6.

- [ ] **Step 2: Document the new surface**

- `SPEC.md` §4.7: the D18 decisions (trigger price, no retroactive bar triggers and the same-bar rule, the trailing formula, the time exit, retries, kill and flatten). Replace the sentence "WP 1.5 must specify trigger price, same-bar stop/TP ambiguity, and durable hold-time/initial-risk metadata before implementation" with a dated note (2026-10-19, WP 1.5c: specified in D18 and implemented).
- `SPEC.md` §7: the `positions` columns and `orders.external` (migration 0005); the `control_state` keys `flatten_requested` and `flatten_result`.
- `SPEC.md` §0.6 and §4.11: a dated note on the 1.5c kill behaviour.
- `PLAN.md`: a dated status note in the 1.5 row (1.5c done; follow-up (a) done; the plan clarifications above in one sentence each); the 1.6 row gets the REST quote fallback question (decision 12) and "`AlpacaBroker.can_place_orders()` becomes true with the order methods".
- `CLAUDE.md` Commands: `uv run autotrader flatten [--reason ...] / flatten --cancel   # reduce-only flatten via the running core; sets the manual pause`.
- `docs/runbook.md` (new; operator procedures, spec section 7):
  - resolving an unresolved exit by hand: check the order at the broker, then record the outcome on the row (status, broker id) until the 1.5d resolver exists;
  - `flatten` / `flatten --cancel`: queued vs waiting, exit codes, and that the pause stays set;
  - accounting for an external sale: no cost basis in the journal; take it from `flatten_result.external_sales` / `ExternalHoldingSold`;
  - manual coins in a traded symbol: a normal exit sells them too, and the `oversell` anomaly rolls the booking back and re-fires on every sync pass, so resuming alone does not recover. 1.5c ships no repair, and the journal-repair tooling is 1.5d's (PLAN 1.5 row, 1.5b follow-up b). Prevention rule: never hold manual coins in a symbol the bot trades.

- [ ] **Step 3: Run the full check** (ruff/format also run on doc-only changes). Expected: green.

- [ ] **Step 4: Commit**

```bash
git add SPEC.md PLAN.md CLAUDE.md docs/runbook.md src
git commit -m "docs: align SPEC/PLAN/CLAUDE and add the operator runbook for WP 1.5c"
```

Report the checked scope in the PR description: the `rg` command, every hit fixed and those found already aligned.
