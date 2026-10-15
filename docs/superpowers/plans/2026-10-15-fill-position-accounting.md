# Fill and Position Accounting (WP 1.5b) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Book broker fills into the journal exactly once, keep positions with fee-adjusted realized P&L, complete cycles from their entry order's outcome, and pause entries on accounting contradictions or unexplained cash changes.

**Architecture:** `RiskEngine.sync_orders()` (the only broker holder) pulls status and executions per unsettled order and returns a `SyncResult`. A new `core/ledger.py` `Ledger` books each `OrderUpdate` in one `EventBus.publish_tx` unit (fills, order settlement, position, cycle). `check()` syncs and books before reading positions, fails closed on an incomplete sync, and reserves only the unfilled remainder of open entries. A generation-guarded `Ledger.check_cash()` compares broker cash with the booked fills.

**Tech Stack:** Python 3.12, uv, SQLAlchemy 2 async + Alembic (SQLite dev), pydantic v2, structlog, pytest + pytest-asyncio + hypothesis.

**Spec:** `docs/superpowers/specs/2026-10-15-fill-position-accounting-design.md` (merged in PR #24; decisions D17 in `PLAN.md`). Read it before starting any task.

## Global Constraints

- ASCII only in code, comments, docstrings and test names (ruff RUF001-003 fail on `−`, `×`, `→`, `≥`). Markdown docs may use Unicode.
- `Decimal` for every money/qty value; never `float` outside indicator math. Money columns are strings on SQLite: compare Decimals in Python, never in SQL (`WHERE qty > ...` is a string compare there).
- No `assert` in `src/` (ruff S101). No `eval`/`exec`.
- mypy strict must pass on `src` and `tests` (`uv run mypy`).
- `src/autotrader/risk/` keeps 100 % branch coverage.
- Tests never assert wall-clock timing; use fixed clocks (`T0`/`NOW`). `filterwarnings = error` is on: close engines, buses and subprocess pipes you open.
- Only `risk/engine.py` holds a `Broker`. `core/ledger.py` never imports or receives a broker.
- Every commit ends with the trailer line `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>` (subagents: use exactly this line, not your own model name).
- Each task's commit must leave `uv run ruff check`, `uv run ruff format --check`, `uv run mypy` and `uv run pytest` green. Run `uv run ruff format` first (plan snippets may exceed the line length), then `uv run ruff check --fix` (import sorting, I001). `ruff format` does not wrap docstrings or comments: wrap those by hand at 100 columns.

Full check, used at the end of every task:

```bash
uv run ruff format && uv run ruff check --fix && uv run ruff format && uv run mypy && uv run pytest -q --cov && uv run coverage report --include='src/autotrader/risk/*' --fail-under=100 --show-missing
```

**Plan dry run (2026-10-15):** all eight tasks were applied verbatim in a throwaway worktree with every check after each task: all green, 868 tests, property test ~2 s for 25 examples (400 examples also passed). The nine snippet bugs and three design points it found are fixed in this revision (clarifications 5-7 below).

## Spec clarifications made while planning (flag in the PR description)

1. **Close rule is stricter than spec §4.3's "entry settled":** a position closes only when **every** order linked to it is settled and its remainder is below `min_qty`. Otherwise an exit whose fills are booked but whose status lags would let the entry's settlement close the position as `dust` with the wrong reason (Review Focus 5).
2. **`overfilled` against the order qty applies to buy orders only.** An exit is a `close_position`: the broker sells what it holds, which can exceed the qty recorded when `exit()` read the position (a late entry fill in between). A sell's bound is the open qty plus carried dust (`oversell`).
3. **Runner/ledger race (not in the spec):** another cycle's `check()` can sync and complete a cycle (`RISK_CHECK -> FILLED/EXPIRED`) before that cycle's runner writes `SUBMITTED`. The runner's write then fails with `StaleTransition`. `CycleRunner._after_check` logs `cycle.completed_elsewhere` (info) instead of `cycle.stuck` for that cause. A not-submitted entry completed this way ends `EXPIRED` (D15's reconciliation edge) rather than the runner's `REJECTED`.
4. **`positions.qty` is the remaining (open) qty**, reduced by sells; the acquired qty is derivable from the position's buy fills.
5. **Deterministic outcome for orders that never reached the broker** (dry-run finding): the runner writes `SUBMITTED` even for a `not_submitted` result, then `REJECTED`; another check's sync could settle that row in between. The ledger therefore completes a zero-fill order **without a broker id** as `REJECTED` with reason `order <status>` (the runner's own edge and wording; D15's reconciliation may use runner edges), and only a broker-final zero fill as `EXPIRED`. Whoever writes first, the outcome is the same.
6. **The bus no longer logs `StaleTransition` as `bus.unit_failed`** (error level): the runner always logs it itself (`cycle.completed_elsewhere`), so an expected race emits no error line.
7. **Accepted, documented:** after the sync budget is spent, every remaining row is still attempted and fails at once (one `risk.order_sync_failed` line each; same result as stopping). Ledger booking inside `check()` consumes the entry's monotonic budget, so heavy contention can reject an entry `state unavailable` (fail closed). "At most one open position per symbol" can be broken by a position kept open only by a lagging exit while its remainder is broker dust (check 5 ignores dust); the readiness rule then defers the later sell instead of mis-booking it.
8. **Known limitation kept:** until 1.5d adds a periodic sync loop, `check()` is the only caller of the sync, so an unsettled order blocks its symbol's admission until some other entry check runs. This is the same as today's `_refresh_open_entries`.

## Review Focus

1. **Runner write races the ledger:** a cycle completed by another check's sync before its runner writes `SUBMITTED` must end in the ledger's state and log `cycle.completed_elsewhere`, never `cycle.stuck` (Task 7).
2. **Exit fill above its recorded order qty** (position grew between `exit()` and the fill): booked, no `overfilled` anomaly (Task 5).
3. **Quantities crossing digit counts** (`"2"` vs `"10"` compare wrongly as strings): an order of 10 filled 2 + 8 settles with no false anomaly (Task 4).
4. **The same fill id twice in one update** (paginated history overlap): booked once (Task 4).
5. **Exit fills booked while the exit's status lags, then the entry settles:** the position stays open and later closes with the exit's reason, not `dust` (Task 5).

---

## File Structure

- `src/autotrader/broker/base.py` (modify): `DustRemainder`, `Broker.order_fills`.
- `src/autotrader/broker/sim.py` (modify): `order_fills`, raise `DustRemainder`.
- `src/autotrader/broker/alpaca.py` (modify): `order_fills` stub until 1.6.
- `src/autotrader/journal/models.py` (modify): new columns, `SymbolDust`.
- `src/autotrader/journal/migrations/versions/20261015_0004_fill_position_accounting.py` (create).
- `src/autotrader/journal/repo.py` (modify): `insert_order(exit_reason=)`, `unsettled_orders`, `open_entry_orders` = unsettled buys.
- `src/autotrader/core/bus.py` (modify): `publish_tx` build may return a tuple of events.
- `src/autotrader/risk/types.py` (modify): `OrderUpdate`, `SyncResult`, `SyncIncomplete`.
- `src/autotrader/core/ledger.py` (create): `Ledger`, ledger events, cash check.
- `src/autotrader/risk/engine.py` (modify): `Accounting` protocol, `sync_orders`, booking in `check()`, remainder reservation, `exit_reason`, dust result, `snapshot_equity` returns `Account`.
- `src/autotrader/core/cycle.py` (modify): tolerate a cycle completed by the ledger.
- `src/autotrader/core/runtime.py` (modify): construct `Ledger`, pass it to `RiskEngine`.
- Tests: `tests/unit/broker/test_sim.py`, `tests/unit/broker/test_alpaca.py`, `tests/unit/journal/test_db.py`, `tests/unit/journal/test_repo.py`, `tests/unit/core/test_bus.py`, `tests/unit/core/test_ledger.py` (create), `tests/unit/risk/test_engine.py`, `tests/unit/core/test_cycle.py`, `tests/integration/test_cycle_runner.py`.
- Docs: `SPEC.md` §7, `PLAN.md` (1.5 and 1.6 rows, repo state line), stale comments listed in Task 8.

---

### Task 1: Broker interface: `order_fills` and `DustRemainder`

**Files:**
- Modify: `src/autotrader/broker/base.py`
- Modify: `src/autotrader/broker/sim.py` (`close_position`, new `order_fills`, module docstring)
- Modify: `src/autotrader/broker/alpaca.py`
- Test: `tests/unit/broker/test_sim.py`, `tests/unit/broker/test_alpaca.py`

**Interfaces:**
- Produces: `Broker.order_fills(order_id: str) -> list[Fill]` (every execution of one order so far, oldest first; unknown id raises `OrderNotFound`); `DustRemainder(OrderRejected)` with attributes `symbol: str`, `qty: Decimal`, `min_qty: Decimal` and message `"dust below minimum: {qty} < min_qty {min_qty}"`.

- [ ] **Step 1: Write the failing tests** (append to `tests/unit/broker/test_sim.py`; add `DustRemainder` to the `autotrader.broker.base` import list)

```python
async def test_order_fills_lists_every_execution_of_one_order() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    sim.script(BTC, "buy", FillPlan((D("0.5"),)))
    ack = await sim.submit_order(market("buy", "1"))
    other = await sim.submit_order(market("buy", "0.1"))
    sim.process_bar(bar(1, 100.0))
    sim.process_bar(bar(2, 100.0))

    fills = await sim.order_fills(ack.order_id)
    assert [f.fill_id for f in fills] == [f"{ack.order_id}:1", f"{ack.order_id}:2"]
    assert sum(f.qty for f in fills) == (await sim.get_order(ack.order_id)).filled_qty
    assert [f.fill_id for f in await sim.order_fills(other.order_id)] == [f"{other.order_id}:1"]


async def test_order_fills_of_an_unknown_order_raises() -> None:
    with pytest.raises(OrderNotFound):
        await make_sim().order_fills("sim-404")


async def test_dust_close_raises_the_typed_dust_signal() -> None:
    sim = make_sim(fees=BASE)
    sim.process_bar(bar(0, 20000.0))
    await sim.submit_order(market("buy", "0.0001"))
    sim.process_bar(bar(1, 20000.0))  # net 0.00009975 < min_qty 0.0001

    with pytest.raises(DustRemainder) as caught:
        await sim.close_position(BTC)
    assert (caught.value.symbol, caught.value.qty, caught.value.min_qty) == (
        BTC,
        D("0.00009975"),
        D("0.0001"),
    )
    assert isinstance(caught.value, OrderRejected)  # existing handlers stay correct
```

Append to `tests/unit/broker/test_alpaca.py` (same construction as the existing not-yet tests there):

```python
async def test_order_fills_is_not_implemented_until_wp_1_6() -> None:
    broker = await connect(FakeClient())
    with pytest.raises(BrokerError, match=r"order_fills is not implemented until WP 1\.6"):
        await broker.order_fills("any")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/broker/test_sim.py tests/unit/broker/test_alpaca.py -k "order_fills or typed_dust" -q`
Expected: FAIL (`AttributeError: 'SimBroker' object has no attribute 'order_fills'`, `ImportError: cannot import name 'DustRemainder'`).

- [ ] **Step 3: Implement**

In `src/autotrader/broker/base.py`, after `class OrderRejected(BrokerError)`:

```python
class DustRemainder(OrderRejected):
    """A full close was refused: what is left is below the minimum order qty (dust).

    Typed so kill/flatten can report dust explicitly (PLAN 3.2 item 5, WP 1.5b) instead of
    matching message text. It is an `OrderRejected`, so existing handlers stay correct."""

    def __init__(self, symbol: str, qty: Decimal, min_qty: Decimal) -> None:
        super().__init__(f"dust below minimum: {qty} < min_qty {min_qty}")
        self.symbol = symbol
        self.qty = qty
        self.min_qty = min_qty
```

In the `Broker` protocol, after `get_order`:

```python
    async def order_fills(self, order_id: str) -> list[Fill]: ...
```

Replace the `Fill` docstring with (wrapped at 100 columns):

```python
    """One broker execution. `fill_id` is unique and stable per execution, so the ledger
    books each fill once (WP 1.5b, `fills.broker_fill_id`). `qty` is the gross qty traded
    at `price`; `fee` is denominated in `fee_asset`: a buy with
    `FeeSchedule.buy_fee_asset == "base"` pays it in the coin (net coins received =
    qty - fee), every sell and every "quote" buy pays it in USD (G10, D10,
    docs/alpaca-notes.md)."""
```

In `src/autotrader/broker/sim.py`: add `DustRemainder` to the `autotrader.broker.base` import; in `close_position` replace

```python
            raise OrderRejected(f"dust below minimum: {qty} < min_qty {rules.min_qty}")
```

with

```python
            raise DustRemainder(symbol, qty, rules.min_qty)
```

and add after `get_order`:

```python
    async def order_fills(self, order_id: str) -> list[Fill]:
        """Every execution of `order_id` so far, oldest first (the WP 1.5b pull path).
        Unknown ids raise `OrderNotFound`, like `get_order`."""
        self._get(order_id)
        return [f for f in self._fill_log.values() if f.order_id == order_id]
```

In the module docstring, replace the two lines ending the `close_position` bullet with:

```
  remainder below `min_qty` is dust and is rejected with `DustRemainder` (spec section 4).
  Whether Alpaca rejects a sell below `min_order_size` is unverified (WP 1.6).
```

In `src/autotrader/broker/alpaca.py`, after `get_order`:

```python
    async def order_fills(self, order_id: str) -> list[Fill]:
        _not_yet("order_fills")
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/broker -q`
Expected: PASS (the two existing `match="dust below minimum"` tests still pass: the message is unchanged).

- [ ] **Step 5: Full check, then commit**

Run the full check from Global Constraints. Expected: all green.

```bash
git add src/autotrader/broker/base.py src/autotrader/broker/sim.py src/autotrader/broker/alpaca.py tests/unit/broker/test_sim.py tests/unit/broker/test_alpaca.py
git commit -m "WP 1.5b: broker order_fills pull path and typed DustRemainder

The ledger pulls executions per order (D17) instead of consuming the fill
stream, and kill/flatten need dust as a type, not message text.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Journal schema 0004, models and repo helpers

**Files:**
- Modify: `src/autotrader/journal/models.py`
- Create: `src/autotrader/journal/migrations/versions/20261015_0004_fill_position_accounting.py`
- Modify: `src/autotrader/journal/repo.py` (`insert_order`, new `unsettled_orders`)
- Modify: `SPEC.md` §7
- Test: `tests/unit/journal/test_db.py`, `tests/unit/journal/test_repo.py`

**Interfaces:**
- Produces (models): `Fill.broker_fill_id: str` (unique), `Fill.side: str`, `Fill.fee_asset: str`; `Order.filled_qty: Decimal` (default 0), `Order.settled_at: datetime | None`, `Order.position_id: int | None`, `Order.exit_reason: str | None`; `Position.initial_risk: Decimal | None`, `Position.dust_qty: Decimal | None`, `Position.dust_consumed: Decimal` (default 0); `SymbolDust(symbol: str PK, qty: Decimal, updated_at: datetime)`.
- Produces (repo): `insert_order(..., exit_reason: str | None = None) -> Order`; `unsettled_orders(session) -> list[Order]` (rows with `settled_at IS NULL`, id order).
- `open_entry_orders` is NOT changed in this task (Task 7 does it together with the engine, so every commit stays green).

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/journal/test_db.py` (add `current_revision` to the `autotrader.journal.db` import):

```python
def _run_sql(url: str, *statements: str) -> None:
    async def run() -> None:
        engine = make_engine(url)
        try:
            async with engine.begin() as conn:
                for statement in statements:
                    await conn.execute(text(statement))
        finally:
            await engine.dispose()

    asyncio.run(run())


_ORDER_0003 = (
    "INSERT INTO orders (id, broker_order_id, mode, type, side, qty, status, submitted_at,"
    " updated_at) VALUES ({id}, {broker}, 'paper', 'limit', 'buy', '1', '{status}',"
    " '2026-10-09 12:00:00', '2026-10-09 12:00:00')"
)


def test_0004_refuses_to_invent_broker_fill_ids(db_url: str) -> None:
    downgrade(db_url, "0003")
    _run_sql(
        db_url,
        _ORDER_0003.format(id=1, broker="'b-1'", status="filled"),
        "INSERT INTO fills (order_id, qty, price, fee, ts) VALUES (1, '1', '100', '0',"
        " '2026-10-09 12:00:00')",
    )
    with pytest.raises(RuntimeError, match="fills has rows"):
        upgrade(db_url)
    assert current_revision(db_url) == "0003"


def test_0004_settles_final_rows_that_never_reached_the_broker(db_url: str) -> None:
    downgrade(db_url, "0003")
    _run_sql(
        db_url,
        _ORDER_0003.format(id=1, broker="NULL", status="not_submitted"),
        _ORDER_0003.format(id=2, broker="NULL", status="rejected"),
        _ORDER_0003.format(id=3, broker="'b-3'", status="filled"),
        _ORDER_0003.format(id=4, broker="NULL", status="unknown"),
    )
    upgrade(db_url)

    async def rows() -> list[tuple[int, object, object]]:
        engine = make_engine(db_url)
        try:
            async with make_sessionmaker(engine)() as s:
                found = await s.scalars(select(m.Order).order_by(m.Order.id))
                return [(o.id, o.settled_at is not None, o.filled_qty) for o in found]
        finally:
            await engine.dispose()

    assert asyncio.run(rows()) == [
        (1, True, Decimal(0)),
        (2, True, Decimal(0)),
        (3, False, Decimal(0)),
        (4, False, Decimal(0)),
    ]


async def test_broker_fill_id_is_unique(session: AsyncSession) -> None:
    order = m.Order(
        mode="paper", type="market", side="buy", qty=Decimal("1"), status="new",
        submitted_at=T0, updated_at=T0,
    )
    session.add(order)
    await session.flush()
    for _ in range(2):
        session.add(
            m.Fill(
                order_id=order.id, broker_fill_id="f-1", side="buy", fee_asset="quote",
                qty=Decimal("1"), price=Decimal("100"), fee=Decimal("0.25"), ts=T0,
            )
        )
    with pytest.raises(IntegrityError):
        await session.commit()
```

Append to `tests/unit/journal/test_repo.py`:

```python
async def test_insert_order_records_exit_reason_and_starts_unsettled(session: AsyncSession) -> None:
    row = await repo.insert_order(
        session,
        cycle_id=None,
        client_order_id=None,
        symbol="BTC/USD",
        mode="paper",
        type_="market",
        side="sell",
        qty=Decimal("1"),
        limit_price=None,
        stop_price=None,
        tp_price=None,
        status="pending",
        now=T0,
        exit_reason="stop",
    )
    assert (row.exit_reason, row.filled_qty, row.settled_at, row.position_id) == (
        "stop",
        Decimal(0),
        None,
        None,
    )


async def test_unsettled_orders_are_every_row_without_settled_at(session: AsyncSession) -> None:
    open_ = [await _order(session, status=s) for s in ("new", "filled", "not_submitted")]
    done = await _order(session, status="filled", side="sell")
    await session.execute(update(m.Order).where(m.Order.id == done.id).values(settled_at=T0))
    await session.commit()
    assert [o.id for o in await repo.unsettled_orders(session)] == [o.id for o in open_]
```

Add `from sqlalchemy import update` and `from autotrader.journal import models as m` to `test_repo.py` if they are not already imported (check the existing import block first).

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/journal -q`
Expected: FAIL (`TypeError: ... unexpected keyword argument 'broker_fill_id'`, `... 'exit_reason'`, no revision `0004`).

- [ ] **Step 3: Implement the models**

In `src/autotrader/journal/models.py`:

`Order`, after `updated_at`:

```python
    # WP 1.5b (spec 2026-10-15): the applied fill qty (never the broker's figure), the
    # settlement time (NULL = effects unapplied = still reserving), the trade it belongs to
    # (kill/flatten exits have no cycle) and, for exits, why.
    filled_qty: Mapped[Decimal] = mapped_column(default=Decimal(0), server_default="0")
    settled_at: Mapped[datetime | None]
    position_id: Mapped[int | None] = mapped_column(ForeignKey("positions.id"))
    exit_reason: Mapped[str | None] = mapped_column(String(16))
```

`Fill`, replace the class with:

```python
class Fill(Base):
    """One broker execution, booked once (`broker_fill_id`, WP 1.5b). `fee` stays in the
    asset that paid it (`fee_asset`); qty/price/fee are written by value."""

    __tablename__ = "fills"
    __table_args__ = (
        CheckConstraint("side IN ('buy', 'sell')", name="side"),
        CheckConstraint("fee_asset IN ('base', 'quote')", name="fee_asset"),
    )

    id: Mapped[int] = mapped_column(primary_key=True)
    order_id: Mapped[int] = mapped_column(ForeignKey("orders.id"))
    broker_fill_id: Mapped[str] = mapped_column(String(64), unique=True)
    side: Mapped[str] = mapped_column(String(8))
    fee_asset: Mapped[str] = mapped_column(String(8))
    qty: Mapped[Decimal]
    price: Mapped[Decimal]
    fee: Mapped[Decimal]
    ts: Mapped[datetime]
```

`Position`: add after `benchmark_return`:

```python
    # WP 1.5b: `qty` is the open (remaining) qty. The R denominator at entry (never changed
    # by trailing), the remainder written off at close, and carried dust of earlier trades
    # that this trade's sells consumed (D17).
    initial_risk: Mapped[Decimal | None]
    dust_qty: Mapped[Decimal | None]
    dust_consumed: Mapped[Decimal] = mapped_column(default=Decimal(0), server_default="0")
```

New class after `Position`:

```python
class SymbolDust(Base):
    """Dust the broker still holds per symbol after write-off closes (WP 1.5b, D17). A close
    adds its remainder; a later sell beyond its open qty consumes it."""

    __tablename__ = "symbol_dust"

    symbol: Mapped[str] = mapped_column(String(32), primary_key=True)
    qty: Mapped[Decimal]
    updated_at: Mapped[datetime]
```

- [ ] **Step 4: Write migration 0004**

Create `src/autotrader/journal/migrations/versions/20261015_0004_fill_position_accounting.py`:

```python
"""fill/position accounting (WP 1.5b, spec 2026-10-15-fill-position-accounting-design.md)

Fill identity and fee asset, order settlement, position dust/initial risk, and the
per-symbol dust balance (D17).

Revision ID: 0004
Revises: 0003
Create Date: 2026-10-15 20:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

import autotrader.journal.types

revision: str = "0004"
down_revision: str | Sequence[str] | None = "0003"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

Money = autotrader.journal.types.Money
UTCDateTime = autotrader.journal.types.UTCDateTime


def upgrade() -> None:
    # broker_fill_id is NOT NULL and nothing wrote fills before WP 1.5b: refuse instead of
    # inventing ids for rows that should not exist.
    if op.get_bind().execute(sa.text("SELECT COUNT(*) FROM fills")).scalar():
        raise RuntimeError("fills has rows: migration 0004 cannot invent broker_fill_id values")
    op.create_table(
        "symbol_dust",
        sa.Column("symbol", sa.String(length=32), nullable=False),
        sa.Column("qty", Money(), nullable=False),
        sa.Column("updated_at", UTCDateTime(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint("symbol", name=op.f("pk_symbol_dust")),
    )
    with op.batch_alter_table("fills") as batch:
        batch.add_column(sa.Column("broker_fill_id", sa.String(length=64), nullable=False))
        batch.add_column(sa.Column("side", sa.String(length=8), nullable=False))
        batch.add_column(sa.Column("fee_asset", sa.String(length=8), nullable=False))
        batch.create_unique_constraint(op.f("uq_fills_broker_fill_id"), ["broker_fill_id"])
        batch.create_check_constraint(op.f("ck_fills_side"), "side IN ('buy', 'sell')")
        batch.create_check_constraint(
            op.f("ck_fills_fee_asset"), "fee_asset IN ('base', 'quote')"
        )
    with op.batch_alter_table("positions") as batch:
        batch.add_column(sa.Column("initial_risk", Money(), nullable=True))
        batch.add_column(sa.Column("dust_qty", Money(), nullable=True))
        batch.add_column(
            sa.Column("dust_consumed", Money(), server_default="0", nullable=False)
        )
    with op.batch_alter_table("orders") as batch:
        batch.add_column(sa.Column("filled_qty", Money(), server_default="0", nullable=False))
        batch.add_column(sa.Column("settled_at", UTCDateTime(timezone=True), nullable=True))
        batch.add_column(sa.Column("position_id", sa.Integer(), nullable=True))
        batch.add_column(sa.Column("exit_reason", sa.String(length=16), nullable=True))
        batch.create_foreign_key(
            op.f("fk_orders_position_id_positions"), "positions", ["position_id"], ["id"]
        )
    # Final rows that never reached the broker have no effects to apply: settled already.
    op.execute(
        sa.text(
            "UPDATE orders SET settled_at = updated_at"
            " WHERE broker_order_id IS NULL AND status IN ('not_submitted', 'rejected')"
        )
    )


def downgrade() -> None:
    with op.batch_alter_table("orders") as batch:
        batch.drop_constraint(op.f("fk_orders_position_id_positions"), type_="foreignkey")
        batch.drop_column("exit_reason")
        batch.drop_column("position_id")
        batch.drop_column("settled_at")
        batch.drop_column("filled_qty")
    with op.batch_alter_table("positions") as batch:
        batch.drop_column("dust_consumed")
        batch.drop_column("dust_qty")
        batch.drop_column("initial_risk")
    with op.batch_alter_table("fills") as batch:
        batch.drop_constraint(op.f("ck_fills_fee_asset"), type_="check")
        batch.drop_constraint(op.f("ck_fills_side"), type_="check")
        batch.drop_constraint(op.f("uq_fills_broker_fill_id"), type_="unique")
        batch.drop_column("fee_asset")
        batch.drop_column("side")
        batch.drop_column("broker_fill_id")
    op.drop_table("symbol_dust")
```

- [ ] **Step 5: Repo helpers**

In `src/autotrader/journal/repo.py`: add `exit_reason: str | None = None` as the last keyword parameter of `insert_order` and pass `exit_reason=exit_reason` to `Order(...)`. Add after `open_entry_orders`:

```python
async def unsettled_orders(session: AsyncSession) -> list[Order]:
    """Every order whose effects are not fully applied yet (WP 1.5b), in id order: the
    ledger books a symbol's orders oldest first."""
    rows = await session.scalars(
        select(Order)
        .where(Order.settled_at.is_(None))
        .order_by(Order.id)
        .execution_options(populate_existing=True)
    )
    return list(rows)
```

- [ ] **Step 6: SPEC §7**

In `SPEC.md` §7 replace the three table lines:

```
orders            (id, cycle_id, broker_order_id, client_order_id, symbol, mode, type, side, qty, limit_price, stop_price, tp_price, status, submitted_at, updated_at, filled_qty, settled_at, position_id, exit_reason)
fills             (id, order_id, broker_fill_id, side, fee_asset, qty, price, fee, ts)
positions         (id, cycle_id, symbol, side, qty, entry_price, stop, tp, time_limit_at, opened_at, closed_at, exit_reason, realized_pnl, fees, mae, mfe, r_multiple, llm_cost_usd, benchmark_return, initial_risk, dust_qty, dust_consumed)
```

and add after the `positions` line:

```
symbol_dust       (symbol, qty, updated_at)   -- dust the broker still holds per symbol (WP 1.5b)
```

In the paragraph below the block, replace "WP 1.5 must preserve position exit metadata; WP 1.6 adds durable broker fill identity;" with "WP 1.5 must preserve position exit metadata. **2026-10-15 (WP 1.5b):** migration `0004` adds fill identity (`broker_fill_id`, unique) and fee asset, order settlement (`filled_qty`, `settled_at`, `position_id`, `exit_reason`), position `initial_risk`/`dust_qty`/`dust_consumed` and `symbol_dust`; 1.5b books fills from SimBroker, WP 1.6 adds the Alpaca fill path;". Leave "(head `0003` at this review)" as it is (a dated review statement).

- [ ] **Step 7: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/journal -q`
Expected: PASS, including `test_migration_matches_models` and `test_downgrade_removes_tables`.

- [ ] **Step 8: Full check, then commit**

```bash
git add src/autotrader/journal SPEC.md tests/unit/journal
git commit -m "WP 1.5b: journal schema 0004 for fill identity, settlement and dust

Fills need a unique broker id to be booked once, orders a settlement mark to
stop reserving, and positions the dust and initial-risk fields D17 decided.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: `publish_tx` commits several events atomically

**Files:**
- Modify: `src/autotrader/core/bus.py`
- Test: `tests/unit/core/test_bus.py`

**Interfaces:**
- Produces: `Build = Callable[[AsyncSession], Awaitable[DomainEvent | tuple[DomainEvent, ...] | None]]`. `publish_tx` persists every returned event in order in the same transaction, verifies with the first, enqueues all after commit, and returns the first event (`None` for `None` or `()`).

- [ ] **Step 1: Write the failing tests** (append to `tests/unit/core/test_bus.py`)

```python
async def test_publish_tx_commits_several_events_atomically_in_order(
    bus: EventBus, sessions: async_sessionmaker[AsyncSession]
) -> None:
    got: list[DomainEvent] = []

    async def handler(event: DomainEvent) -> None:
        got.append(event)

    bus.subscribe(DomainEvent, handler)

    async def build(session: AsyncSession) -> tuple[DomainEvent, ...]:
        session.add(m.ControlState(key="k", value=1))
        return (Ping(ts=T0, n=1), Pong(ts=T0, text="two"))

    async def verify(session: AsyncSession, event: DomainEvent) -> bool:
        return False

    out = await bus.publish_tx(build, verify)
    await bus.drain()
    assert isinstance(out, Ping) and out.n == 1
    assert [e.type for e in await _events(sessions)] == ["Ping", "Pong"]
    assert [type(e).__name__ for e in got] == ["Ping", "Pong"]
    async with sessions() as s:
        assert await repo.get_control(s, "k") == 1


async def test_publish_tx_empty_tuple_writes_and_delivers_nothing(
    bus: EventBus, sessions: async_sessionmaker[AsyncSession]
) -> None:
    async def build(session: AsyncSession) -> tuple[DomainEvent, ...]:
        session.add(m.ControlState(key="k", value=1))
        return ()

    async def verify(session: AsyncSession, event: DomainEvent) -> bool:
        return False

    assert await bus.publish_tx(build, verify) is None
    assert await _events(sessions) == []
    async with sessions() as s:
        assert await repo.get_control(s, "k") is None
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/core/test_bus.py -k "several_events or empty_tuple" -q`
Expected: FAIL (`AttributeError: 'tuple' object has no attribute 'payload'`) and a mypy error on the build's return type.

- [ ] **Step 3: Implement**

In `src/autotrader/core/bus.py` change the alias:

```python
Build = Callable[[AsyncSession], Awaitable[DomainEvent | tuple[DomainEvent, ...] | None]]
```

Replace `_unit` with:

```python
    async def _unit(
        self, build: Build, verify: Verify, on_uncertain: OnUncertain | None
    ) -> DomainEvent | None:
        async with self._lock:
            async with self._sessions() as session:
                built = await build(session)
                events = built if isinstance(built, tuple) else (() if built is None else (built,))
                if not events:
                    await session.rollback()
                    return None
                event = events[0]  # all commit together, so verifying one verifies all
                event_type = type(event).__name__
                for each in events:
                    await repo.append_event(
                        session,
                        type(each).__name__,
                        each.payload(),
                        cycle_id=each.cycle_id,
                        ts=each.ts,
                        commit=False,
                    )
                try:
                    await session.commit()
                except Exception as e:
                    # On SQLite a failed COMMIT may still hold a lock that blocks the
                    # read-back in the fresh session below.
                    try:
                        await session.rollback()
                    except Exception:
                        log.exception("bus.rollback_failed", event_type=event_type)
                    if not await self._confirmed(verify, event):
                        error = CommitUncertain(f"{event_type} may or may not be committed")
                        log.error(
                            "bus.commit_uncertain",
                            event_type=event_type,
                            cycle_id=event.cycle_id,
                            error=repr(e),
                        )
                        if on_uncertain is not None:
                            try:
                                on_uncertain(error)
                            except Exception:
                                log.exception("bus.on_uncertain_failed")
                        raise error from e
                    log.warning(
                        "bus.commit_recovered", event_type=event_type, cycle_id=event.cycle_id
                    )
            for each in events:
                self._enqueue(each)
        return event
```

Replace the module docstring paragraph on `publish_tx` from "committing and returns the event" to the paragraph's end with:

```
committing and returns the event, a tuple of events (all committed in the same
transaction, verified by the first; WP 1.5b), or `None`/`()`: roll back, nothing to do;
the bus appends the event(s), commits once and enqueues. The unit runs in a bus-owned
task the caller awaits through `asyncio.shield`, so cancelling the caller never
interrupts a commit or loses a committed event's delivery. A commit that raises is not
proof of rollback: the bus asks `verify` once in a fresh session; only a positive answer
confirms (deliver), anything else raises `CommitUncertain` after calling `on_uncertain`
from the unit itself. Delivery is exactly once for confirmed commits; an uncertain or
abandoned unit may be persisted without delivery (the `events` table stays the record).
```

and the `publish_tx` method docstring with:

```python
        """Commit `build`'s writes and the event(s) it returns together, then deliver them in
        order; returns the first."""
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/core/test_bus.py tests/unit/core/test_cycle.py -q`
Expected: PASS (the cycle runner's single-event builds are unchanged).

- [ ] **Step 5: Full check, then commit**

```bash
git add src/autotrader/core/bus.py tests/unit/core/test_bus.py
git commit -m "WP 1.5b: publish_tx commits several events in one transaction

A ledger unit settles an order, completes its cycle and reports dust at once;
the events must commit together with the rows they describe.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Ledger, entry side: fills, settlement, cycle completion, anomalies

**Files:**
- Modify: `src/autotrader/risk/types.py`
- Create: `src/autotrader/core/ledger.py`
- Test: create `tests/unit/core/test_ledger.py`

**Interfaces:**
- Consumes: `Broker.order_fills`/SimBroker (Task 1, tests only), models and `repo.unsettled_orders`/`insert_order(exit_reason=)` (Task 2), tuple builds (Task 3), `check_edge`, `CycleState`, `CycleTransitioned` from `core/cycle.py`, `repo.transition_cycle`, `repo.cycle_state`, `PAUSED_KEY` from `core/kill.py`.
- Produces (`risk/types.py`): `OrderUpdate(order_row_id: int, status: str, filled_qty: Dec, fills: tuple[Fill, ...] = (), min_qty: Dec | None = None)`; `SyncResult(updates: tuple[OrderUpdate, ...] = (), failed: tuple[int, ...] = ())` with property `complete: bool`; `class SyncIncomplete(RuntimeError)`.
- Produces (`core/ledger.py`): `Ledger(bus: EventBus, sessions: async_sessionmaker[AsyncSession], *, clock: Callable[[], datetime] = ...)` with `async apply(update: OrderUpdate) -> Outcome` and `async apply_all(result: SyncResult) -> None` (the sell side and the per-symbol ordering are completed in Task 5); events `FillsApplied`, `OrderSettled`, `DustLeft`, `LedgerAnomaly`; `Outcome = Literal["applied", "noop", "deferred", "anomaly"]`; `FINAL`; module helpers `_norm`, `_cash_effect`, `_verify`.

- [ ] **Step 1: Types** (no test of their own; exercised below)

Append to `src/autotrader/risk/types.py` (add `Fill` to the `autotrader.broker.base` import):

```python
class OrderUpdate(_Frozen):
    """One order's broker view for the ledger (WP 1.5b, spec section 4.2). `status` and
    `fills` are two separate broker reads, so the fills may be ahead of `filled_qty`."""

    order_row_id: int
    status: str  # a broker OrderState, or the local finals "not_submitted" / "rejected"
    filled_qty: Dec  # the broker's cumulative filled qty (0 for local finals)
    fills: tuple[Fill, ...] = ()
    min_qty: Dec | None = None  # None only for local finals: no broker call, no fills


class SyncResult(_Frozen):
    """One sync pass. `failed` lists order rows whose broker state could not be read."""

    updates: tuple[OrderUpdate, ...] = ()
    failed: tuple[int, ...] = ()

    @property
    def complete(self) -> bool:
        return not self.failed


class SyncIncomplete(RuntimeError):
    """An unsettled order's broker state could not be read: entries fail closed (PLAN 3.2
    item 4) until it is resolved."""
```

- [ ] **Step 2: Write the failing tests**

Create `tests/unit/core/test_ledger.py`:

```python
"""Ledger (WP 1.5b; spec 2026-10-15-fill-position-accounting-design.md) on SimBroker and a
migrated SQLite journal."""

import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal as D
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.broker.base import AssetRules, BrokerOrder, FeeSchedule
from autotrader.broker.sim import FillPlan, SimBroker
from autotrader.core.bus import EventBus
from autotrader.core.config import load_profile
from autotrader.core.kill import PAUSED_KEY
from autotrader.core.ledger import Ledger
from autotrader.core.types import Bar
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade
from autotrader.risk.types import OrderUpdate, SyncResult

REPO = Path(__file__).resolve().parents[3]
PROFILE = load_profile(REPO / "strategies" / "fast_momentum_v1.yaml")
BTC = "BTC/USD"
T0 = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
RULES = AssetRules(
    min_qty=D("0.0001"), qty_increment=D("0.0001"), min_notional=D("1"), price_increment=D("0.01")
)
QUOTE = FeeSchedule(maker_bps=D("15"), taker_bps=D("25"), buy_fee_asset="quote")
BASE = FeeSchedule(maker_bps=D("15"), taker_bps=D("25"), buy_fee_asset="base")


def bar(i: int, price: float = 100.0) -> Bar:
    return Bar(
        symbol=BTC,
        timeframe="1m",
        ts=T0 + timedelta(minutes=i),
        open=price,
        high=price,
        low=price,
        close=price,
        volume=1.0,
    )


def make_sim(fees: FeeSchedule = QUOTE, cash: str = "1000") -> SimBroker:
    sim = SimBroker(cash=D(cash), fees=fees, slippage_bps=D(0), asset_rules={BTC: RULES})
    sim.process_bar(bar(0))
    return sim


@pytest.fixture
def db_url(tmp_path: Path) -> str:
    url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    upgrade(url)
    return url


@pytest.fixture
async def sessions(db_url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = make_engine(db_url)
    yield make_sessionmaker(engine)
    await engine.dispose()


@pytest.fixture
async def bus(sessions: async_sessionmaker[AsyncSession]) -> AsyncIterator[EventBus]:
    b = EventBus(sessions)
    yield b
    await b.close()


@pytest.fixture
def ledger(bus: EventBus, sessions: async_sessionmaker[AsyncSession]) -> Ledger:
    return Ledger(bus, sessions, clock=lambda: T0)


async def new_cycle(sessions: async_sessionmaker[AsyncSession], state: str = "SUBMITTED") -> int:
    async with sessions() as s:
        strategy = await repo.register_strategy(s, PROFILE, "yaml")
        row = m.Cycle(
            strategy_id=strategy.id,
            symbol=BTC,
            rule_id="r",
            signal_features={},
            state=state,
            created_at=T0,
        )
        s.add(row)
        await s.commit()
        return row.id


async def place(
    sessions: async_sessionmaker[AsyncSession],
    sim: SimBroker,
    side: str,
    qty: str,
    *,
    cycle_id: int | None = None,
    exit_reason: str | None = None,
    limit: str | None = None,
) -> tuple[int, str]:
    """Submit like the Risk Engine does and journal the row. `side`: "buy" (market, or an
    IOC limit with `limit`), "sell" (market) or "close" (close_position). `qty` is what
    the row records. Returns (order row id, broker order id)."""
    if side == "close":
        ack = await sim.close_position(BTC)
    elif limit is None:
        ack = await sim.submit_order(
            BrokerOrder(symbol=BTC, side="buy" if side == "buy" else "sell", qty=D(qty), type="market")
        )
    else:
        ack = await sim.submit_order(
            BrokerOrder(
                symbol=BTC,
                side="buy",
                qty=D(qty),
                type="limit",
                limit_price=D(limit),
                time_in_force="ioc",
            )
        )
    buy = side == "buy"
    async with sessions() as s:
        row = await repo.insert_order(
            s,
            cycle_id=cycle_id,
            client_order_id=None,
            symbol=BTC,
            mode="paper",
            type_="market" if limit is None else "limit",
            side="buy" if buy else "sell",
            qty=D(qty),
            limit_price=None if limit is None else D(limit),
            stop_price=D("95") if buy else None,
            tp_price=D("110") if buy else None,
            status="pending",
            now=T0,
            exit_reason=exit_reason,
        )
        await repo.update_order(s, row.id, status=ack.status, now=T0, broker_order_id=ack.order_id)
    return row.id, ack.order_id


async def update(
    sim: SimBroker, row_id: int, broker_id: str, *, visible: int | None = None
) -> OrderUpdate:
    """What the engine's sync reads for one order; `visible` hides later executions."""
    status = await sim.get_order(broker_id)
    found = await sim.order_fills(broker_id)
    return OrderUpdate(
        order_row_id=row_id,
        status=status.status,
        filled_qty=status.filled_qty,
        fills=tuple(found if visible is None else found[:visible]),
        min_qty=RULES.min_qty,
    )


async def sync(sessions: async_sessionmaker[AsyncSession], sim: SimBroker) -> SyncResult:
    async with sessions() as s:
        rows = await repo.unsettled_orders(s)
    return SyncResult(
        updates=tuple(
            [await update(sim, r.id, r.broker_order_id) for r in rows if r.broker_order_id]
        )
    )


async def orders(sessions: async_sessionmaker[AsyncSession]) -> list[m.Order]:
    async with sessions() as s:
        return list(await s.scalars(select(m.Order).order_by(m.Order.id)))


async def fills(sessions: async_sessionmaker[AsyncSession]) -> list[m.Fill]:
    async with sessions() as s:
        return list(await s.scalars(select(m.Fill).order_by(m.Fill.id)))


async def positions(sessions: async_sessionmaker[AsyncSession]) -> list[m.Position]:
    async with sessions() as s:
        return list(await s.scalars(select(m.Position).order_by(m.Position.id)))


async def event_rows(sessions: async_sessionmaker[AsyncSession]) -> list[m.Event]:
    async with sessions() as s:
        return list(await s.scalars(select(m.Event).order_by(m.Event.id)))


async def event_types(sessions: async_sessionmaker[AsyncSession]) -> list[str]:
    return [e.type for e in await event_rows(sessions)]


async def paused(sessions: async_sessionmaker[AsyncSession]) -> bool:
    async with sessions() as s:
        return await repo.get_control(s, PAUSED_KEY) is True


async def cycle_state(sessions: async_sessionmaker[AsyncSession], cycle_id: int) -> str | None:
    async with sessions() as s:
        return await repo.cycle_state(s, cycle_id)


async def dust_balance(sessions: async_sessionmaker[AsyncSession]) -> D | None:
    async with sessions() as s:
        row = await s.get(m.SymbolDust, BTC)
        return None if row is None else row.qty


def anomaly_kind(rows: list[m.Event]) -> Any:
    return [e.payload["kind"] for e in rows if e.type == "LedgerAnomaly"][-1]


# --- entries ------------------------------------------------------------------------


async def test_full_entry_opens_the_position_and_fills_the_cycle(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    cycle = await new_cycle(sessions)
    async with sessions() as s:
        await repo.record_risk_decision(
            s,
            cycle_id=cycle,
            kind="entry",
            approved=True,
            reasons=[],
            adjusted={"initial_risk": "5"},
            computed_size=D("1"),
            ts=T0,
        )
    row, oid = await place(sessions, sim, "buy", "1", cycle_id=cycle)
    sim.process_bar(bar(1))

    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    (pos,) = await positions(sessions)
    assert (pos.cycle_id, pos.symbol, pos.qty, pos.entry_price, pos.fees) == (
        cycle,
        BTC,
        D("1"),
        D("100"),
        D("0.25"),
    )
    assert (pos.stop, pos.tp, pos.initial_risk, pos.closed_at) == (D("95"), D("110"), D("5"), None)
    (order,) = await orders(sessions)
    assert (order.status, order.filled_qty, order.position_id) == ("filled", D("1"), pos.id)
    assert order.settled_at is not None
    (fill,) = await fills(sessions)
    assert (fill.broker_fill_id, fill.side, fill.fee_asset) == (f"{oid}:1", "buy", "quote")
    assert await cycle_state(sessions, cycle) == "FILLED"
    assert await event_types(sessions) == ["OrderSettled", "CycleTransitioned"]
    settled = (await event_rows(sessions))[0].payload
    assert (settled["filled_qty"], settled["settled"], settled["status"]) == ("1", True, "filled")


async def test_partial_fill_then_cancel_fills_the_cycle_as_partial(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    cycle = await new_cycle(sessions)
    sim.script(BTC, "buy", FillPlan((D("0.5"), "cancel")))
    row, oid = await place(sessions, sim, "buy", "1", cycle_id=cycle)
    sim.process_bar(bar(1))
    sim.process_bar(bar(2))

    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    (order,) = await orders(sessions)
    assert (order.status, order.filled_qty) == ("canceled", D("0.5"))
    assert await cycle_state(sessions, cycle) == "FILLED"
    moved = [e.payload for e in await event_rows(sessions) if e.type == "CycleTransitioned"]
    assert moved[-1]["details"] == {"filled_qty": "0.5", "ordered_qty": "1", "partial": True}


async def test_a_final_zero_fill_expires_the_cycle(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    cycle = await new_cycle(sessions, state="RISK_CHECK")
    sim.script(BTC, "buy", FillPlan(("cancel",)))
    row, oid = await place(sessions, sim, "buy", "1", cycle_id=cycle)
    sim.process_bar(bar(1))

    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    assert await cycle_state(sessions, cycle) == "EXPIRED"
    assert await positions(sessions) == []
    (order,) = await orders(sessions)
    assert order.settled_at is not None


async def test_a_never_submitted_entry_settles_and_is_rejected_like_the_runner_does(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    cycle = await new_cycle(sessions, state="RISK_CHECK")
    async with sessions() as s:
        row = await repo.insert_order(
            s,
            cycle_id=cycle,
            client_order_id=None,
            symbol=BTC,
            mode="paper",
            type_="limit",
            side="buy",
            qty=D("1"),
            limit_price=D("100"),
            stop_price=None,
            tp_price=None,
            status="not_submitted",
            now=T0,
        )
    update_ = OrderUpdate(order_row_id=row.id, status="not_submitted", filled_qty=D(0))
    assert await ledger.apply(update_) == "applied"
    (order,) = await orders(sessions)
    assert order.settled_at is not None
    assert await cycle_state(sessions, cycle) == "REJECTED"  # as the runner would write it
    async with sessions() as s:
        (cycle_row,) = list(await s.scalars(select(m.Cycle).where(m.Cycle.id == cycle)))
    assert cycle_row.reject_reason == "order not_submitted"


async def test_terminal_cycles_are_never_reopened_but_their_fills_are_booked(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    errored = await new_cycle(sessions, state="ERROR")
    filled = await new_cycle(sessions, state="FILLED")  # the runner saw an immediate fill
    first, first_oid = await place(sessions, sim, "buy", "1", cycle_id=errored)
    sim.process_bar(bar(1))
    assert await ledger.apply(await update(sim, first, first_oid)) == "applied"
    second, second_oid = await place(sessions, sim, "buy", "1", cycle_id=filled)
    sim.process_bar(bar(2))
    assert await ledger.apply(await update(sim, second, second_oid)) == "applied"

    assert await cycle_state(sessions, errored) == "ERROR"
    assert await cycle_state(sessions, filled) == "FILLED"
    assert await event_types(sessions) == ["OrderSettled", "OrderSettled"]
    assert len(await fills(sessions)) == 2


def test_a_sync_is_complete_only_without_failed_orders() -> None:
    # Keeps risk/ at 100 % branch coverage from this task on (the engine uses it in Task 7).
    assert SyncResult().complete
    assert not SyncResult(failed=(1,)).complete


# --- idempotency and status/fill skew ---------------------------------------------------


async def test_a_redelivered_fill_is_booked_once_whatever_its_exponent(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    sim.script(BTC, "buy", FillPlan((D("0.5"),)))
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    sim.process_bar(bar(2))
    again = await update(sim, row, oid)
    respelled = (again.fills[0].model_copy(update={"qty": D("0.50000")}), *again.fills[1:])

    assert await ledger.apply(again.model_copy(update={"fills": respelled})) == "applied"
    assert len(await fills(sessions)) == 2
    (pos,) = await positions(sessions)
    assert pos.qty == D("1")


async def test_the_same_fill_twice_in_one_update_is_booked_once(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    once = await update(sim, row, oid)

    assert await ledger.apply(once.model_copy(update={"fills": once.fills + once.fills})) == "applied"
    assert len(await fills(sessions)) == 1
    (pos,) = await positions(sessions)
    assert pos.qty == D("1")


async def test_quantities_compare_by_value_across_digit_counts(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    # "2" > "10" as strings: Money is text on SQLite, so a SQL-side compare would be wrong.
    sim = make_sim(cash="2000")
    sim.script(BTC, "buy", FillPlan((D("0.2"),)))
    row, oid = await place(sessions, sim, "buy", "10")
    sim.process_bar(bar(1))
    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    sim.process_bar(bar(2))
    assert await ledger.apply(await update(sim, row, oid)) == "applied"

    (order,) = await orders(sessions)
    assert (order.filled_qty, order.settled_at is not None) == (D("10"), True)
    assert not await paused(sessions)


async def test_fills_ahead_of_the_status_apply_without_settling(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    current = await update(sim, row, oid)
    older_status = current.model_copy(update={"status": "partially_filled", "filled_qty": D("0.5")})

    assert await ledger.apply(older_status) == "applied"
    (order,) = await orders(sessions)
    assert (order.filled_qty, order.status, order.settled_at) == (D("1"), "partially_filled", None)
    assert not await paused(sessions)
    assert await ledger.apply(current) == "applied"
    (order,) = await orders(sessions)
    assert (order.status, order.settled_at is not None) == ("filled", True)


async def test_fills_not_yet_visible_settle_on_a_later_pass(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    sim.script(BTC, "buy", FillPlan((D("0.5"),)))
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    sim.process_bar(bar(2))

    assert await ledger.apply(await update(sim, row, oid, visible=1)) == "applied"
    (order,) = await orders(sessions)
    assert (order.filled_qty, order.status, order.settled_at) == (D("0.5"), "filled", None)
    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    (order,) = await orders(sessions)
    assert (order.filled_qty, order.settled_at is not None) == (D("1"), True)


async def test_an_older_update_never_moves_the_status_back(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    sim.script(BTC, "buy", FillPlan((D("0.5"),)))
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    current = await update(sim, row, oid)
    assert await ledger.apply(current) == "applied"

    older = current.model_copy(update={"status": "new", "filled_qty": D(0), "fills": ()})
    assert await ledger.apply(older) == "noop"
    (order,) = await orders(sessions)
    assert (order.status, order.filled_qty) == ("partially_filled", D("0.5"))


async def test_two_racing_applies_book_an_update_once(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    once = await update(sim, row, oid)

    outcomes = await asyncio.gather(ledger.apply(once), ledger.apply(once))
    assert sorted(outcomes) == ["applied", "noop"]
    assert len(await fills(sessions)) == 1


# --- anomalies ---------------------------------------------------------------------------


async def test_a_known_fill_with_other_values_is_an_anomaly(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    sim.script(BTC, "buy", FillPlan((D("0.5"),)))
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    first = await update(sim, row, oid)
    assert await ledger.apply(first) == "applied"

    changed = (first.fills[0].model_copy(update={"price": D("101")}),)
    assert await ledger.apply(first.model_copy(update={"fills": changed})) == "anomaly"
    assert await paused(sessions)
    assert anomaly_kind(await event_rows(sessions)) == "fill_mismatch"
    (order,) = await orders(sessions)
    assert order.filled_qty == D("0.5")


async def test_a_final_status_below_the_applied_qty_is_overfilled(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    lying = (await update(sim, row, oid)).model_copy(
        update={"status": "canceled", "filled_qty": D("0.5")}
    )

    assert await ledger.apply(lying) == "anomaly"
    assert anomaly_kind(await event_rows(sessions)) == "overfilled"
    assert await fills(sessions) == [] and await positions(sessions) == []
    assert await paused(sessions)


async def test_buy_fills_beyond_the_order_qty_are_overfilled(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    once = await update(sim, row, oid)
    extra = once.fills[0].model_copy(update={"fill_id": f"{oid}:99"})

    too_much = once.model_copy(update={"fills": (*once.fills, extra), "filled_qty": D("2")})
    assert await ledger.apply(too_much) == "anomaly"
    assert anomaly_kind(await event_rows(sessions)) == "overfilled"


async def test_a_new_fill_on_a_settled_order_is_an_anomaly_and_a_stale_update_a_noop(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    once = await update(sim, row, oid)
    assert await ledger.apply(once) == "applied"

    assert await ledger.apply(once) == "noop"
    late = once.fills[0].model_copy(update={"fill_id": "late"})
    assert await ledger.apply(once.model_copy(update={"fills": (*once.fills, late)})) == "anomaly"
    assert anomaly_kind(await event_rows(sessions)) == "fill_after_settle"
```


- [ ] **Step 3: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/core/test_ledger.py -q`
Expected: FAIL (`ModuleNotFoundError: No module named 'autotrader.core.ledger'`).

- [ ] **Step 4: Implement `core/ledger.py`** (entry side; the sell branch raises until Task 5)

Create `src/autotrader/core/ledger.py`:

```python
"""Fill and position accounting (WP 1.5b, D17; spec 2026-10-15-fill-position-accounting-
design.md).

The Risk Engine's order sync reads each unsettled order's broker status and executions;
`Ledger.apply_all()` books them here, one `EventBus.publish_tx` unit per order: the fills
(idempotent by `broker_fill_id`), the order's applied qty, status and settlement, the
position (open, grow, reduce, close) and, when an entry settles, its cycle (D15: any fill
-> FILLED, a final zero fill -> EXPIRED). Pure journal code: the broker stays with the
Risk Engine (SPEC 0.1).

Status and fills are separate broker reads, so the fills may be ahead of the status: fills
are applied as seen, and an order settles only on a final status whose `filled_qty`
equals the applied qty (spec 4.3). Only a contradiction against an authoritative bound is
an anomaly: the update is rolled back and entries are paused until an operator resumes
them (spec 4.5).

Money columns are text on SQLite, so Decimal values are compared in Python, never in SQL.
"""

from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Literal

import structlog
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.broker.base import Fill
from autotrader.core.bus import DomainEvent, EventBus
from autotrader.core.cycle import CycleState, CycleTransitioned, check_edge
from autotrader.core.kill import PAUSED_KEY
from autotrader.journal import repo
from autotrader.journal.models import ControlState, Order, Position, RiskDecision, SymbolDust
from autotrader.journal.models import Fill as FillRow
from autotrader.risk.types import OrderUpdate, SyncResult

log = structlog.get_logger(__name__)

FINAL = frozenset({"filled", "canceled", "rejected", "not_submitted"})
_RANK = {"new": 1, "partially_filled": 2}  # pending / unknown: 0, final: 3
_D0 = Decimal(0)

AnomalyKind = Literal[
    "fill_mismatch", "overfilled", "fill_after_settle", "sell_without_position", "oversell"
]
Outcome = Literal["applied", "noop", "deferred", "anomaly"]


class FillsApplied(DomainEvent):
    """Fills or a status change of one order were booked; the order is not settled yet."""

    order_row_id: int
    fill_ids: list[str]
    position_id: int | None
    status: str
    filled_qty: Decimal  # the applied qty after this unit
    settled: bool = False


class OrderSettled(FillsApplied):
    """The order's effects are fully applied (spec 4.3): it no longer reserves."""

    settled: bool = True


class DustLeft(DomainEvent):
    symbol: str
    position_id: int
    dust_qty: Decimal
    min_qty: Decimal


class LedgerAnomaly(DomainEvent):
    kind: AnomalyKind
    order_row_id: int
    detail: str


class _Deferred(Exception):
    """Not an error: the update waits for earlier history on its symbol (spec 4.3)."""


class _Anomaly(Exception):
    def __init__(self, kind: AnomalyKind, detail: str) -> None:
        super().__init__(f"{kind}: {detail}")
        self.kind: AnomalyKind = kind
        self.detail = detail


@dataclass
class _Unit:
    """What a unit's `build` reports to its caller (`build` itself only returns events)."""

    error: _Deferred | _Anomaly | None = None
    cycle_id: int | None = None


def _rank(status: str) -> int:
    return 3 if status in FINAL else _RANK.get(status, 0)


def _norm(value: Decimal) -> Decimal:
    """Written by value (spec 3): 1.50 and 1.5 are one quantity, stored without exponent."""
    return Decimal(format(value.normalize(), "f"))


def _net_qty(fill: Fill) -> Decimal:
    """Coins a buy delivers: a coin fee comes out of the fill (G10, D10)."""
    return fill.qty - fill.fee if fill.fee_asset == "base" else fill.qty


def _fee_usd(fee: Decimal, fee_asset: str, price: Decimal) -> Decimal:
    return fee * price if fee_asset == "base" else fee


def _cash_effect(row: FillRow) -> Decimal:
    """Cash one fill moved: a coin fee reduces coins, never cash (G10)."""
    notional = row.qty * row.price
    if row.side == "buy":
        return -notional - (row.fee if row.fee_asset == "quote" else _D0)
    return notional - row.fee


def _same(row: FillRow, fill: Fill) -> bool:
    return (row.side, row.fee_asset, row.qty, row.price, row.fee) == (
        fill.side,
        fill.fee_asset,
        fill.qty,
        fill.price,
        fill.fee,
    )


async def _verify(session: AsyncSession, event: DomainEvent) -> bool:
    """Read-back after a commit error (D15): did this unit's writes land?"""
    if isinstance(event, FillsApplied):
        order = await session.get(Order, event.order_row_id)
        return (
            order is not None
            and order.filled_qty == event.filled_qty
            and order.status == event.status
            and (order.settled_at is not None) == event.settled
        )
    if isinstance(event, LedgerAnomaly):
        return bool(await repo.get_control(session, PAUSED_KEY) is True)
    raise TypeError(f"cannot verify {type(event).__name__}")


async def _initial_risk(session: AsyncSession, cycle_id: int | None) -> Decimal | None:
    """The R denominator from the approved entry decision (D16 `adjusted`)."""
    if cycle_id is None:
        return None
    adjusted = await session.scalar(
        select(RiskDecision.adjusted)
        .where(
            RiskDecision.cycle_id == cycle_id,
            RiskDecision.kind == "entry",
            RiskDecision.approved.is_(True),
        )
        .order_by(RiskDecision.id.desc())
        .limit(1)
    )
    value = None if adjusted is None else adjusted.get("initial_risk")
    return None if value is None else Decimal(str(value))


async def _earlier_unsettled(session: AsyncSession, order: Order) -> bool:
    n = await session.scalar(
        select(func.count())
        .select_from(Order)
        .where(Order.symbol == order.symbol, Order.id < order.id, Order.settled_at.is_(None))
    )
    return bool(n)


class Ledger:
    def __init__(
        self,
        bus: EventBus,
        sessions: async_sessionmaker[AsyncSession],
        *,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._bus = bus
        self._sessions = sessions
        self._clock = clock

    # --- orders ------------------------------------------------------------------------

    async def apply_all(self, result: SyncResult) -> None:
        """Book one sync pass (spec 4.3): every update, in order-row id order."""
        updates = {u.order_row_id: u for u in result.updates}
        async with self._sessions() as session:
            rows = await repo.unsettled_orders(session)
        for row in rows:
            update = updates.get(row.id)
            if update is not None:
                await self.apply(update)

    async def apply(self, update: OrderUpdate) -> Outcome:
        """Book one order's update in one transaction. Never raises for a deferral or an
        anomaly; journal errors propagate (the caller fails closed)."""
        unit = _Unit()

        async def build(session: AsyncSession) -> tuple[DomainEvent, ...] | None:
            try:
                return await self._book(session, update, unit)
            except (_Deferred, _Anomaly) as e:
                unit.error = e
                return None

        event = await self._bus.publish_tx(build, _verify)
        if isinstance(unit.error, _Anomaly):
            await self._anomaly(update.order_row_id, unit.cycle_id, unit.error)
            return "anomaly"
        if isinstance(unit.error, _Deferred):
            log.info("ledger.exit_deferred", order_row=update.order_row_id, reason=str(unit.error))
            return "deferred"
        return "noop" if event is None else "applied"

    async def _book(
        self, session: AsyncSession, u: OrderUpdate, unit: _Unit
    ) -> tuple[DomainEvent, ...] | None:
        now = self._clock()
        order = await session.get(Order, u.order_row_id)
        if order is None:
            raise LookupError(f"no order row {u.order_row_id}")
        unit.cycle_id = order.cycle_id
        booked = {
            r.broker_fill_id: r
            for r in await session.scalars(select(FillRow).where(FillRow.order_id == order.id))
        }
        new: list[Fill] = []
        for fill in sorted(u.fills, key=lambda f: (f.ts, f.fill_id)):
            row = booked.get(fill.fill_id)
            if row is not None:
                if not _same(row, fill):
                    raise _Anomaly("fill_mismatch", f"fill {fill.fill_id} differs from the booked one")
            elif all(f.fill_id != fill.fill_id for f in new):
                new.append(fill)
        if order.settled_at is not None:
            if new:
                raise _Anomaly("fill_after_settle", f"new fill {new[0].fill_id} on a settled order")
            return None  # a stale update for an order that is already settled
        applied = order.filled_qty + sum((f.qty for f in new), _D0)
        final = u.status in FINAL
        # An exit is a close_position: the broker sells what it holds, which can exceed the
        # qty recorded when exit() read the position. Only an entry's own qty bounds it.
        if order.side == "buy" and applied > order.qty:
            raise _Anomaly("overfilled", f"applied {applied} > ordered {order.qty}")
        if final and u.filled_qty < applied:
            raise _Anomaly(
                "overfilled", f"final {u.status} reports {u.filled_qty} < applied {applied}"
            )
        position = None if order.position_id is None else await session.get(Position, order.position_id)
        for fill in new:
            session.add(
                FillRow(
                    order_id=order.id,
                    broker_fill_id=fill.fill_id,
                    side=fill.side,
                    fee_asset=fill.fee_asset,
                    qty=_norm(fill.qty),
                    price=_norm(fill.price),
                    fee=_norm(fill.fee),
                    ts=fill.ts,
                )
            )
            if order.side == "buy":
                position = await self._buy(session, order, position, fill)
            else:
                position = await self._sell(session, order, position, fill, now)
        changed = bool(new)
        if new:
            order.filled_qty = _norm(applied)
            if order.side == "buy" and position is not None:
                await self._reprice(session, order, position)
        if _rank(u.status) > _rank(order.status):
            order.status = u.status
            changed = True
        extra: list[DomainEvent] = []
        settled = final and u.filled_qty == applied
        if settled:
            order.settled_at = now
            changed = True
            if order.side == "buy":
                moved = await self._complete_cycle(session, order, applied, now)
                if moved is not None:
                    extra.append(moved)
            if position is not None:
                dust = await self._maybe_close(session, position, u.min_qty, now)
                if dust is not None:
                    extra.append(dust)
        if not changed:
            return None
        order.updated_at = now
        head_type = OrderSettled if settled else FillsApplied
        head = head_type(
            ts=now,
            cycle_id=order.cycle_id,
            order_row_id=order.id,
            fill_ids=[f.fill_id for f in new],
            position_id=order.position_id,
            status=order.status,
            filled_qty=order.filled_qty,
        )
        return (head, *extra)

    async def _buy(
        self, session: AsyncSession, order: Order, position: Position | None, fill: Fill
    ) -> Position:
        if position is None:
            position = Position(
                cycle_id=order.cycle_id,
                symbol=order.symbol or fill.symbol,
                side="long",
                qty=_D0,
                entry_price=fill.price,
                stop=order.stop_price,
                tp=order.tp_price,
                opened_at=fill.ts,
                fees=_D0,
                initial_risk=await _initial_risk(session, order.cycle_id),
                dust_consumed=_D0,
            )
            session.add(position)
            await session.flush()  # assigns position.id
            order.position_id = position.id
        position.qty = _norm(position.qty + _net_qty(fill))
        position.fees = _norm((position.fees or _D0) + _fee_usd(fill.fee, fill.fee_asset, fill.price))
        return position

    async def _sell(
        self,
        session: AsyncSession,
        order: Order,
        position: Position | None,
        fill: Fill,
        now: datetime,
    ) -> Position:
        raise NotImplementedError("sell fills are booked from WP 1.5b Task 5 on")

    async def _reprice(self, session: AsyncSession, order: Order, position: Position) -> None:
        """`entry_price` = gross VWAP of the entry's buy fills (Alpaca's convention)."""
        await session.flush()
        rows = list(await session.scalars(select(FillRow).where(FillRow.order_id == order.id)))
        qty = sum((r.qty for r in rows), _D0)
        vwap = sum((r.qty * r.price for r in rows), _D0) / qty
        position.entry_price = _norm(vwap.quantize(Decimal("1e-12")))

    async def _complete_cycle(
        self, session: AsyncSession, order: Order, applied: Decimal, now: datetime
    ) -> CycleTransitioned | None:
        """D15 from the order's final outcome; a terminal cycle is never reopened."""
        cycle_id = order.cycle_id
        if cycle_id is None:
            return None
        state = await repo.cycle_state(session, cycle_id)
        if state is None or state not in (CycleState.RISK_CHECK, CycleState.SUBMITTED):
            return None
        from_ = CycleState(state)
        reason: str | None
        if applied > 0:
            to, reason = CycleState.FILLED, None
            details: dict[str, object] = {
                "filled_qty": str(_norm(applied)),
                "ordered_qty": str(_norm(order.qty)),
                "partial": applied < order.qty,
            }
        elif order.broker_order_id is None:
            # Never reached the broker: the runner's own edge and wording, so the outcome
            # does not depend on who writes first (plan clarification 5).
            to, reason = CycleState.REJECTED, f"order {order.status}"
            details = {"reason": reason}
        else:
            to, reason = CycleState.EXPIRED, f"order {order.status} with no fill"
            details = {"reason": reason}
        check_edge(from_, to, reconciliation=True)
        await repo.transition_cycle(
            session, cycle_id, from_state=from_, to_state=to, now=now, reason=reason
        )
        return CycleTransitioned(
            ts=now, cycle_id=cycle_id, from_state=from_, to_state=to, details=details
        )

    async def _maybe_close(
        self, session: AsyncSession, position: Position, min_qty: Decimal | None, now: datetime
    ) -> DustLeft | None:
        """Close once every order of the position is settled and less than `min_qty` is
        left (spec 4.3/4.4; stricter than "entry settled", see the plan's clarification 1).
        Realized P&L is cash in minus cash out, so dust is written off (D17)."""
        if position.closed_at is not None or min_qty is None or position.qty >= min_qty:
            return None
        await session.flush()
        linked = list(
            await session.scalars(
                select(Order).where(Order.position_id == position.id).order_by(Order.id)
            )
        )
        if any(o.settled_at is None for o in linked):
            return None  # a late fill can still arrive: stay open (1.5c protects it)
        rows = list(
            await session.scalars(select(FillRow).where(FillRow.order_id.in_([o.id for o in linked])))
        )
        exits = [o for o in linked if o.side == "sell"]
        position.closed_at = max(r.ts for r in rows)
        position.exit_reason = (exits[-1].exit_reason or "exit") if exits else "dust"
        position.realized_pnl = _norm(sum((_cash_effect(r) for r in rows), _D0))
        position.dust_qty = position.qty
        log.info(
            "ledger.position_closed",
            position_id=position.id,
            exit_reason=position.exit_reason,
            realized_pnl=str(position.realized_pnl),
        )
        if position.qty == 0:
            return None
        dust = await session.get(SymbolDust, position.symbol)
        if dust is None:
            session.add(SymbolDust(symbol=position.symbol, qty=position.qty, updated_at=now))
        else:
            dust.qty = _norm(dust.qty + position.qty)
            dust.updated_at = now
        return DustLeft(
            ts=now,
            cycle_id=position.cycle_id,
            symbol=position.symbol,
            position_id=position.id,
            dust_qty=position.qty,
            min_qty=min_qty,
        )

    async def _anomaly(self, order_row_id: int, cycle_id: int | None, error: _Anomaly) -> None:
        """Pause entries for an operator in a unit of its own: the update rolled back."""
        log.error("ledger.anomaly", kind=error.kind, order_row=order_row_id, detail=error.detail)
        event = LedgerAnomaly(
            ts=self._clock(),
            cycle_id=cycle_id,
            kind=error.kind,
            order_row_id=order_row_id,
            detail=error.detail,
        )

        async def build(session: AsyncSession) -> DomainEvent:
            await session.merge(ControlState(key=PAUSED_KEY, value=True))
            return event

        await self._bus.publish_tx(build, _verify)
```

Notes for the implementer:
- `position.qty` is the remaining qty (plan clarification 4).
- If ruff flags a line over the length limit, wrap it; `uv run ruff format` does most of this.
- `_D0` and `_norm` are reused by Task 6; keep them module-level.

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/core/test_ledger.py -q`
Expected: PASS (all entry-side tests).

- [ ] **Step 6: Full check, then commit**

```bash
git add src/autotrader/risk/types.py src/autotrader/core/ledger.py tests/unit/core/test_ledger.py
git commit -m "WP 1.5b: ledger books entry fills, settles orders and completes cycles

Fills are applied once by broker id and as seen; an order settles only on a
matching final status, and only contradictions against a final status or the
order qty pause entries (spec 4.3, 4.5).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Ledger, sell side: exits, closes, carried dust, readiness and per-symbol order

**Files:**
- Modify: `src/autotrader/core/ledger.py` (`_sell`, `apply_all`)
- Test: `tests/unit/core/test_ledger.py`

**Interfaces:**
- Consumes: Task 4's `Ledger`, helpers and test helpers (`place`, `update`, `sync`, `positions`, ...).
- Produces: `Ledger.apply_all` holds back a symbol's later rows when an earlier unsettled row has no update or was deferred/anomalous (logs `ledger.update_deferred`); `_sell` per spec 4.3/4.4.

- [ ] **Step 1: Write the failing tests** (append to `tests/unit/core/test_ledger.py`; add `Fill` to its `autotrader.broker.base` import)

```python
# --- exits -------------------------------------------------------------------------------


async def settle_entry(
    sessions: async_sessionmaker[AsyncSession],
    ledger: Ledger,
    sim: SimBroker,
    n: int,
    qty: str = "1",
) -> int:
    """Buy `qty` at market, fill it on bar `n` and book it settled."""
    row, oid = await place(sessions, sim, "buy", qty)
    sim.process_bar(bar(n))
    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    return row


async def test_a_full_exit_closes_with_exact_pnl_on_quote_fees(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    await settle_entry(sessions, ledger, sim, 1)
    row, oid = await place(sessions, sim, "close", "1", exit_reason="take_profit")
    sim.process_bar(bar(2, 110.0))

    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    (pos,) = await positions(sessions)
    # in: 110 - 0.275 = 109.725; out: 100 + 0.25 = 100.25
    assert (pos.exit_reason, pos.realized_pnl, pos.fees, pos.dust_qty) == (
        "take_profit",
        D("9.475"),
        D("0.525"),
        D("0"),
    )
    assert pos.closed_at == T0 + timedelta(minutes=2)
    assert await dust_balance(sessions) is None


async def test_a_full_exit_closes_with_exact_pnl_on_coin_fees(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim(BASE)
    await settle_entry(sessions, ledger, sim, 1)  # net 0.9975 coins for 100 USD
    row, oid = await place(sessions, sim, "close", "0.9975", exit_reason="time")
    sim.process_bar(bar(2, 110.0))

    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    (pos,) = await positions(sessions)
    # in: 0.9975 * 110 = 109.725 - fee 0.2743125; out: 100 (the coin fee is not cash)
    assert (pos.realized_pnl, pos.fees) == (D("9.4506875"), D("0.5243125"))


async def test_a_tiny_partial_ioc_entry_closes_at_once_as_dust(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim(BASE)
    cycle = await new_cycle(sessions)
    sim.script(BTC, "buy", FillPlan((D("0.00005"),)))  # slice floors to 0.0001
    row, oid = await place(sessions, sim, "buy", "2.997", cycle_id=cycle, limit="100")
    sim.process_bar(bar(1))

    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    (pos,) = await positions(sessions)
    assert (pos.exit_reason, pos.dust_qty, pos.realized_pnl) == ("dust", D("0.00009975"), D("-0.01"))
    assert await dust_balance(sessions) == D("0.00009975")
    assert await event_types(sessions) == ["OrderSettled", "CycleTransitioned", "DustLeft"]
    assert await cycle_state(sessions, cycle) == "FILLED"


async def test_a_partial_exit_keeps_the_position_open(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    await settle_entry(sessions, ledger, sim, 1)
    row, oid = await place(sessions, sim, "sell", "0.4", exit_reason="time")
    sim.process_bar(bar(2))

    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    (pos,) = await positions(sessions)
    assert (pos.qty, pos.closed_at) == (D("0.6"), None)


async def test_the_position_closes_only_after_its_entry_settles(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    sim.script(BTC, "buy", FillPlan((D("0.5"), "cancel")))
    entry, entry_oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))  # 0.5 filled, the rest still open
    assert await ledger.apply(await update(sim, entry, entry_oid)) == "applied"
    exit_row, exit_oid = await place(sessions, sim, "close", "0.5", exit_reason="stop")
    sim.process_bar(bar(2, 95.0))  # the entry is canceled, the exit sells 0.5

    assert await ledger.apply(await update(sim, exit_row, exit_oid)) == "applied"
    (pos,) = await positions(sessions)
    assert (pos.qty, pos.closed_at) == (D("0"), None)  # entry not settled yet: still open
    assert await ledger.apply(await update(sim, entry, entry_oid)) == "applied"
    (pos,) = await positions(sessions)
    # in: 47.5 - 0.11875; out: 50 + 0.125
    assert (pos.exit_reason, pos.realized_pnl) == ("stop", D("-2.74375"))


async def test_a_late_entry_fill_after_a_protective_exit_lands_on_the_open_position(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    sim.script(BTC, "buy", FillPlan((D("0.5"),)))  # the rest fills on the next bar
    entry, entry_oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    assert await ledger.apply(await update(sim, entry, entry_oid)) == "applied"
    exit_row, exit_oid = await place(sessions, sim, "close", "0.5", exit_reason="stop")
    sim.process_bar(bar(2))  # the entry fills its other 0.5, the exit sells 0.5

    assert await ledger.apply(await update(sim, exit_row, exit_oid)) == "applied"
    assert await ledger.apply(await update(sim, entry, entry_oid)) == "applied"
    (pos,) = await positions(sessions)
    assert (pos.qty, pos.closed_at) == (D("0.5"), None)
    (held,) = await sim.get_positions()
    assert held.qty == D("0.5")


async def test_a_lagging_exit_status_keeps_the_position_open_until_the_exit_settles(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    sim.script(BTC, "buy", FillPlan((D("0.5"), "cancel")))
    entry, entry_oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    assert await ledger.apply(await update(sim, entry, entry_oid, visible=1)) == "applied"
    exit_row, exit_oid = await place(sessions, sim, "close", "0.5", exit_reason="stop")
    sim.process_bar(bar(2))
    exit_now = await update(sim, exit_row, exit_oid)

    assert await ledger.apply(exit_now.model_copy(update={"status": "new"})) == "applied"
    assert await ledger.apply(await update(sim, entry, entry_oid)) == "applied"  # entry settles
    (pos,) = await positions(sessions)
    assert pos.closed_at is None  # not closed as "dust" while the exit is unsettled
    assert await ledger.apply(exit_now) == "applied"
    (pos,) = await positions(sessions)
    assert pos.exit_reason == "stop"


async def test_an_exit_filling_more_than_its_recorded_qty_is_not_overfilled(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    await settle_entry(sessions, ledger, sim, 1)
    row, oid = await place(sessions, sim, "close", "0.5", exit_reason="kill")  # sells all 1
    sim.process_bar(bar(2))

    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    (pos,) = await positions(sessions)
    assert (pos.exit_reason, pos.qty) == ("kill", D("0"))
    assert not await paused(sessions)


# --- ordering and readiness ---------------------------------------------------------------


async def test_catch_up_defers_an_exit_whose_entry_lookup_failed(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    entry, _ = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    exit_row, exit_oid = await place(sessions, sim, "close", "1", exit_reason="stop")
    sim.process_bar(bar(2))

    exit_only = SyncResult(updates=(await update(sim, exit_row, exit_oid),), failed=(entry,))
    await ledger.apply_all(exit_only)
    assert await fills(sessions) == [] and not await paused(sessions)
    await ledger.apply_all(await sync(sessions, sim))
    (pos,) = await positions(sessions)
    assert pos.exit_reason == "stop"
    assert all(o.settled_at is not None for o in await orders(sessions))


async def test_an_oversell_waits_for_incomplete_entry_history_and_keeps_old_dust(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    async with sessions() as s:
        s.add(m.SymbolDust(symbol=BTC, qty=D("0.00005"), updated_at=T0))
        await s.commit()
    sim.script(BTC, "buy", FillPlan((D("0.5"),)))
    entry, entry_oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    sim.process_bar(bar(2))  # final, filled 1 in two executions
    exit_row, exit_oid = await place(sessions, sim, "close", "1", exit_reason="stop")
    sim.process_bar(bar(3))

    first = SyncResult(
        updates=(
            await update(sim, entry, entry_oid, visible=1),  # status final 1, only 0.5 shown
            await update(sim, exit_row, exit_oid),
        )
    )
    await ledger.apply_all(first)
    exit_order = (await orders(sessions))[1]
    assert (exit_order.filled_qty, exit_order.settled_at) == (D(0), None)
    assert not await paused(sessions)
    assert await dust_balance(sessions) == D("0.00005")

    await ledger.apply_all(await sync(sessions, sim))
    (pos,) = await positions(sessions)
    assert (pos.exit_reason, pos.realized_pnl, pos.dust_consumed) == ("stop", D("-0.5"), D(0))
    assert await dust_balance(sessions) == D("0.00005")


async def test_carried_dust_is_consumed_by_the_next_trade(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim(BASE)
    sim.script(BTC, "buy", FillPlan((D("0.00005"),)))
    first, first_oid = await place(sessions, sim, "buy", "2.997", limit="100")
    sim.process_bar(bar(1))
    assert await ledger.apply(await update(sim, first, first_oid)) == "applied"  # dust
    await settle_entry(sessions, ledger, sim, 2)  # net 0.9975
    (held,) = await sim.get_positions()
    assert held.qty == D("0.99759975")  # the broker merged the dust in
    row, oid = await place(sessions, sim, "close", str(held.qty), exit_reason="time")
    sim.process_bar(bar(3))

    assert await ledger.apply(await update(sim, row, oid)) == "applied"
    _, second = await positions(sessions)
    # in: 99.759975 - 0.2493999375; out: 100
    assert (second.dust_consumed, second.dust_qty, second.realized_pnl) == (
        D("0.00009975"),
        D("0"),
        D("-0.4894249375"),
    )
    assert await dust_balance(sessions) == D("0")
    assert await sim.get_positions() == []


async def test_a_sell_beyond_open_qty_and_dust_is_an_oversell(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    await settle_entry(sessions, ledger, sim, 1)
    row, oid = await place(sessions, sim, "close", "1", exit_reason="stop")
    sim.process_bar(bar(2))
    real = await update(sim, row, oid)
    bigger = (real.fills[0].model_copy(update={"qty": D("1.5")}),)

    lying = real.model_copy(update={"fills": bigger, "filled_qty": D("1.5")})
    assert await ledger.apply(lying) == "anomaly"
    assert anomaly_kind(await event_rows(sessions)) == "oversell"
    (pos,) = await positions(sessions)
    assert (pos.qty, pos.closed_at) == (D("1"), None)


def _sell_fill(fill_id: str) -> Fill:
    return Fill(
        fill_id=fill_id,
        order_id="x",
        symbol=BTC,
        side="sell",
        qty=D("1"),
        price=D("100"),
        fee=D("0.25"),
        fee_asset="quote",
        ts=T0,
    )


async def _sell_row(sessions: async_sessionmaker[AsyncSession], status: str = "new") -> int:
    async with sessions() as s:
        row = await repo.insert_order(
            s,
            cycle_id=None,
            client_order_id=None,
            symbol=BTC,
            mode="paper",
            type_="market",
            side="sell",
            qty=D("1"),
            limit_price=None,
            stop_price=None,
            tp_price=None,
            status=status,
            now=T0,
            exit_reason="flatten",
        )
        return row.id


async def test_a_sell_without_any_position_is_an_anomaly(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    row = await _sell_row(sessions)
    lone = OrderUpdate(
        order_row_id=row, status="filled", filled_qty=D("1"), fills=(_sell_fill("x:1"),), min_qty=RULES.min_qty
    )
    assert await ledger.apply(lone) == "anomaly"
    assert anomaly_kind(await event_rows(sessions)) == "sell_without_position"


async def test_a_sell_without_a_position_waits_for_an_earlier_unsettled_order(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    async with sessions() as s:  # an entry whose submit outcome is unknown (no broker id)
        await repo.insert_order(
            s,
            cycle_id=None,
            client_order_id="c-1",
            symbol=BTC,
            mode="paper",
            type_="limit",
            side="buy",
            qty=D("1"),
            limit_price=D("100"),
            stop_price=None,
            tp_price=None,
            status="unknown",
            now=T0,
        )
    row = await _sell_row(sessions)
    lone = OrderUpdate(
        order_row_id=row, status="filled", filled_qty=D("1"), fills=(_sell_fill("x:1"),), min_qty=RULES.min_qty
    )
    assert await ledger.apply(lone) == "deferred"
    assert not await paused(sessions)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/core/test_ledger.py -q`
Expected: the new exit tests FAIL with `NotImplementedError: sell fills are booked from WP 1.5b Task 5 on`; `test_catch_up_defers_an_exit_whose_entry_lookup_failed` fails because `apply_all` books the exit without holding it back.

- [ ] **Step 3: Implement `_sell`** (replace the Task 4 stub)

```python
    async def _sell(
        self,
        session: AsyncSession,
        order: Order,
        position: Position | None,
        fill: Fill,
        now: datetime,
    ) -> Position:
        """Reduce the symbol's open position. A sell beyond the open qty is carried dust of
        earlier trades, resolved only once every earlier order on the symbol is settled
        (spec 4.3): until then the whole update is deferred, never an anomaly."""
        if position is None:
            position = await session.scalar(
                select(Position)
                .where(Position.symbol == order.symbol, Position.closed_at.is_(None))
                .order_by(Position.id)
                .limit(1)
            )
            if position is None:
                if await _earlier_unsettled(session, order):
                    raise _Deferred("no open position yet; an earlier order is unsettled")
                raise _Anomaly(
                    "sell_without_position", f"sell fill {fill.fill_id}: no open {order.symbol}"
                )
            order.position_id = position.id
        position.fees = _norm((position.fees or _D0) + _fee_usd(fill.fee, fill.fee_asset, fill.price))
        excess = fill.qty - position.qty
        if excess <= 0:
            position.qty = _norm(position.qty - fill.qty)
            return position
        if await _earlier_unsettled(session, order):
            raise _Deferred(f"sells {excess} beyond the open qty; an earlier order is unsettled")
        dust = await session.get(SymbolDust, position.symbol)
        available = _D0 if dust is None else dust.qty
        if dust is None or excess > available:
            raise _Anomaly("oversell", f"sells {excess} beyond the open qty, carried dust {available}")
        dust.qty = _norm(available - excess)
        dust.updated_at = now
        position.dust_consumed = _norm(position.dust_consumed + excess)
        position.qty = _D0
        return position
```

- [ ] **Step 4: Implement the per-symbol ordering in `apply_all`** (replace the Task 4 version)

```python
    async def apply_all(self, result: SyncResult) -> None:
        """Book one sync pass (spec 4.3): per symbol in order-row id order. An unsettled row
        without an update (failed lookup, or pending/unknown with no broker id), and an
        update that was deferred or anomalous, hold back the later rows of its symbol, so a
        catch-up never books an exit before its entry."""
        updates = {u.order_row_id: u for u in result.updates}
        async with self._sessions() as session:
            rows = await repo.unsettled_orders(session)
        held: set[str] = set()
        for row in rows:
            symbol = row.symbol or ""
            update = updates.get(row.id)
            if symbol in held:
                if update is not None:
                    log.info("ledger.update_deferred", order_row=row.id, symbol=symbol)
                continue
            if update is None or await self.apply(update) in ("deferred", "anomaly"):
                held.add(symbol)
```

- [ ] **Step 5: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/core/test_ledger.py -q`
Expected: PASS (all entry and exit tests).

- [ ] **Step 6: Full check, then commit**

```bash
git add src/autotrader/core/ledger.py tests/unit/core/test_ledger.py
git commit -m "WP 1.5b: ledger books exits, closes positions and tracks carried dust

A position closes only when all its orders are settled; a sell beyond the open
qty consumes the per-symbol dust balance once earlier history is settled and is
otherwise deferred, so visibility lag never pauses entries (spec 4.3, 4.4).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Cash check

**Files:**
- Modify: `src/autotrader/core/ledger.py`
- Test: `tests/unit/core/test_ledger.py`

**Interfaces:**
- Consumes: Task 4/5 ledger, `_cash_effect`, `_verify`.
- Produces: `CASH_CHECK_KEY = "cash_check"`; `CashGuard(generation: tuple[int, int], unsettled: bool)`; `CashResult = Literal["baseline", "ok", "discrepancy", "skipped"]`; `Ledger.cash_guard() -> CashGuard`; `Ledger.check_cash(account: Account, guard: CashGuard) -> CashResult`; events `CashChecked(result, expected, actual, n_fills)`, `CashDiscrepancy(expected, actual, diff, n_fills, baseline_ts)`. Wiring into `run core` is 1.5d.

- [ ] **Step 1: Write the failing tests**

Append to `tests/unit/core/test_ledger.py` (add `Account` to its `autotrader.broker.base` import and `CASH_CHECK_KEY` to its `autotrader.core.ledger` import):

```python
# --- cash check (D17, spec 4.6) -----------------------------------------------------------


async def baseline_of(sessions: async_sessionmaker[AsyncSession]) -> dict[str, object]:
    async with sessions() as s:
        value = await repo.get_control(s, CASH_CHECK_KEY)
    assert isinstance(value, dict)
    return value


async def check(ledger: Ledger, account: Account) -> str:
    return await ledger.check_cash(account, await ledger.cash_guard())


async def test_the_first_check_only_records_a_baseline(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    assert await check(ledger, await sim.get_account()) == "baseline"
    baseline = await baseline_of(sessions)
    assert (baseline["cash"], baseline["fill_watermark"]) == ("1000", 0)
    assert await event_types(sessions) == ["CashChecked"]


async def test_cash_explained_by_booked_fills_is_ok(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    assert await check(ledger, await sim.get_account()) == "baseline"
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    assert await ledger.apply(await update(sim, row, oid)) == "applied"

    assert await check(ledger, await sim.get_account()) == "ok"
    assert not await paused(sessions)
    assert (await baseline_of(sessions))["cash"] == "899.75"


async def test_an_unexplained_change_pauses_once_and_rebaselines(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    assert await check(ledger, await sim.get_account()) == "baseline"
    sim.cash -= D("19.84")  # the unexplained paper drop of 2026-10-14

    assert await check(ledger, await sim.get_account()) == "discrepancy"
    assert await paused(sessions)
    (event,) = [e for e in await event_rows(sessions) if e.type == "CashDiscrepancy"]
    assert (event.payload["diff"], event.payload["n_fills"]) == ("-19.84", 0)
    assert await check(ledger, await sim.get_account()) == "ok"  # measured from the new cash


async def test_the_check_skips_while_an_order_is_unsettled(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    assert await check(ledger, await sim.get_account()) == "baseline"
    before = await baseline_of(sessions)
    await place(sessions, sim, "buy", "1")  # unsettled until booked

    assert await check(ledger, await sim.get_account()) == "skipped"
    assert await baseline_of(sessions) == before


async def test_an_order_settled_between_guard_and_check_skips_the_check(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    assert await check(ledger, await sim.get_account()) == "baseline"
    guard = await ledger.cash_guard()
    account = await sim.get_account()  # cash 1000
    row, oid = await place(sessions, sim, "buy", "1")  # submitted, filled and settled
    sim.process_bar(bar(1))
    assert await ledger.apply(await update(sim, row, oid)) == "applied"

    assert await ledger.check_cash(account, guard) == "skipped"  # not a false incident
    assert not await paused(sessions)
    assert await check(ledger, await sim.get_account()) == "ok"


async def test_the_tolerance_is_a_cent_per_fill(
    sessions: async_sessionmaker[AsyncSession], ledger: Ledger
) -> None:
    sim = make_sim()
    assert await check(ledger, await sim.get_account()) == "baseline"
    row, oid = await place(sessions, sim, "buy", "1")
    sim.process_bar(bar(1))
    assert await ledger.apply(await update(sim, row, oid)) == "applied"  # expected 899.75

    truncated = Account(equity=D("999.74"), cash=D("899.74"))  # one fill: 0.01 allowed
    assert await check(ledger, truncated) == "ok"
    assert await check(ledger, Account(equity=D("999.72"), cash=D("899.72"))) == "discrepancy"
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/core/test_ledger.py -k cash -q`
Expected: FAIL (`ImportError: cannot import name 'CASH_CHECK_KEY'`).

- [ ] **Step 3: Implement**

In `src/autotrader/core/ledger.py`: add `from autotrader.broker.base import Account, Fill` (replacing the `Fill`-only import), and after the existing constants:

```python
CASH_CHECK_KEY = "cash_check"
CENT = Decimal("0.01")
CashResult = Literal["baseline", "ok", "discrepancy", "skipped"]
```

After `LedgerAnomaly`:

```python
class CashChecked(DomainEvent):
    result: Literal["baseline", "ok"]
    expected: Decimal | None
    actual: Decimal
    n_fills: int


class CashDiscrepancy(DomainEvent):
    expected: Decimal
    actual: Decimal
    diff: Decimal
    n_fills: int
    baseline_ts: str


@dataclass(frozen=True)
class CashGuard:
    """The order/fill generation and whether any order was unsettled at one read (4.6)."""

    generation: tuple[int, int]
    unsettled: bool


async def _guard(session: AsyncSession) -> CashGuard:
    last_order = await session.scalar(select(func.max(Order.id)))
    last_fill = await session.scalar(select(func.max(FillRow.id)))
    open_orders = await session.scalar(
        select(func.count()).select_from(Order).where(Order.settled_at.is_(None))
    )
    return CashGuard((last_order or 0, last_fill or 0), bool(open_orders))
```

Extend `_Unit` with `result: CashResult = "skipped"` and `_verify` with (before the final `raise`):

```python
    if isinstance(event, CashChecked | CashDiscrepancy):
        baseline = await repo.get_control(session, CASH_CHECK_KEY)
        return isinstance(baseline, dict) and baseline.get("ts") == event.ts.isoformat()
```

Add to `Ledger`:

```python
    # --- cash (D17) ----------------------------------------------------------------------

    async def cash_guard(self) -> CashGuard:
        """Read BEFORE the account read and pass it to `check_cash`."""
        async with self._sessions() as session:
            return await _guard(session)

    async def check_cash(self, account: Account, guard: CashGuard) -> CashResult:
        """Compare broker cash with the cash effects of fills booked since the baseline
        (spec 4.6). Compares only if the generation is unchanged from `guard` to the
        re-read inside this unit and no order was unsettled at either read: an order that
        came and went in between still changed `max(orders.id)`. Outside the tolerance:
        manual pause, `CashDiscrepancy`, re-baseline (alert once, nothing absorbed)."""
        now = self._clock()
        unit = _Unit()

        async def build(session: AsyncSession) -> DomainEvent | None:
            again = await _guard(session)
            if guard.unsettled or again.unsettled or again.generation != guard.generation:
                return None
            baseline = await repo.get_control(session, CASH_CHECK_KEY)
            await session.merge(
                ControlState(
                    key=CASH_CHECK_KEY,
                    value={
                        "cash": str(_norm(account.cash)),
                        "fill_watermark": again.generation[1],
                        "ts": now.isoformat(),
                    },
                )
            )
            if baseline is None:
                unit.result = "baseline"
                return CashChecked(
                    ts=now, result="baseline", expected=None, actual=account.cash, n_fills=0
                )
            rows = list(
                await session.scalars(
                    select(FillRow).where(FillRow.id > int(baseline["fill_watermark"]))
                )
            )
            expected = Decimal(baseline["cash"]) + sum((_cash_effect(r) for r in rows), _D0)
            diff = account.cash - expected
            if abs(diff) <= max(CENT, CENT * len(rows)):
                unit.result = "ok"
                return CashChecked(
                    ts=now, result="ok", expected=expected, actual=account.cash, n_fills=len(rows)
                )
            await session.merge(ControlState(key=PAUSED_KEY, value=True))
            unit.result = "discrepancy"
            log.error(
                "ledger.cash_discrepancy",
                expected=str(expected),
                actual=str(account.cash),
                diff=str(diff),
                n_fills=len(rows),
            )
            return CashDiscrepancy(
                ts=now,
                expected=expected,
                actual=account.cash,
                diff=diff,
                n_fills=len(rows),
                baseline_ts=str(baseline["ts"]),
            )

        await self._bus.publish_tx(build, _verify)
        return unit.result
```

Note: SimBroker's exact arithmetic gives `Decimal("899.75000")` after the buy (the fee keeps the bps division's exponent); the baseline stores cash by value (`_norm`), so it reads `"899.75"`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `uv run pytest tests/unit/core/test_ledger.py -q`
Expected: PASS.

- [ ] **Step 5: Full check, then commit**

```bash
git add src/autotrader/core/ledger.py tests/unit/core
git commit -m "WP 1.5b: generation-guarded cash check pauses on unexplained cash

Broker cash is compared with the booked fills only across an unchanged
order/fill generation, so an order settling mid-check never raises a false
incident; outside a cent per fill entries pause for an operator (D17).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Engine integration, reservations, runner tolerance and wiring

**Files:**
- Modify: `src/autotrader/risk/engine.py`
- Modify: `src/autotrader/journal/repo.py` (`open_entry_orders` = unsettled buys; drop `OPEN_ENTRY_STATUSES`)
- Modify: `src/autotrader/core/cycle.py` (`_after_check`)
- Modify: `src/autotrader/core/bus.py` (`_unit_done`)
- Modify: `src/autotrader/core/runtime.py` (construct `Ledger`; `_Snapshotter` returns `object`)
- Test: `tests/unit/risk/test_engine.py`, `tests/unit/journal/test_repo.py`, `tests/unit/core/test_cycle.py`, `tests/integration/test_cycle_runner.py`

**Interfaces:**
- Consumes: `SyncResult`, `OrderUpdate`, `SyncIncomplete` (Task 4), `Ledger` (Tasks 4-6), `Broker.order_fills`, `DustRemainder` (Task 1), `repo.unsettled_orders`, `insert_order(exit_reason=)` (Task 2).
- Produces: `RiskEngine(..., ledger: Accounting, ...)` (keyword, required); `class Accounting(Protocol): async def apply_all(self, result: SyncResult) -> None`; `RiskEngine.sync_orders() -> SyncResult`; `OrderResult.dust: bool = False`; `RiskEngine.snapshot_equity() -> Account`; `repo.open_entry_orders` returns unsettled buy rows.

- [ ] **Step 1: Write the failing tests**

In `tests/unit/risk/test_engine.py`:
- add imports: `from typing import Any`; `from autotrader.broker.base import DustRemainder, Fill` (into the existing import list); `from autotrader.broker.sim import FillPlan`; `from autotrader.core.bus import EventBus`; `from autotrader.core.ledger import Ledger`; `import autotrader.risk.engine as engine_module`; `from autotrader.risk.checks import check_entry`; `from autotrader.risk.engine import Accounting` (next to `Gate, RiskEngine`); `from autotrader.risk.types import EntryIntent, ExitRequest, MarketSnapshot, OpenEntry, PortfolioState, RiskDecision, SyncResult`.
- change `make_engine_` to take `ledger: Accounting | None = None` and pass
  `ledger=ledger or Ledger(EventBus(sessions), sessions, clock=clock or Clock(NOW)),` to `cls(...)`.

Append:

```python
def spy_portfolios(monkeypatch: pytest.MonkeyPatch) -> list[PortfolioState]:
    seen: list[PortfolioState] = []
    real = check_entry  # mypy: risk.engine does not re-export it

    def spy(
        intent: EntryIntent, portfolio: PortfolioState, market: MarketSnapshot, **kw: Any
    ) -> RiskDecision:
        seen.append(portfolio)
        return real(intent, portfolio, market, **kw)

    monkeypatch.setattr(engine_module, "check_entry", spy)
    return seen


async def test_a_partially_filled_entry_reserves_only_its_remainder(
    sessions: async_sessionmaker[AsyncSession],
    gate: ControlGate,
    cycle_id: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = _sim(PartialFillBroker)  # reports the canceled IOC as still partially filled
    broker.script(BTC, "buy", FillPlan((D("0.5"),)))
    engine = make_engine_(broker, sessions, gate)
    await engine.check(intent(cycle_id))  # 2.997 at limit 100.10
    broker.process_bar(five(30))  # fills 1.4985
    seen = spy_portfolios(monkeypatch)

    await engine.check(intent(await new_cycle(sessions)))
    assert seen[-1].open_entries == (
        OpenEntry(symbol=BTC, notional=(D("2.997") - D("1.4985")) * D("100.10")),
    )


class HiddenFillsBroker(SimBroker):
    """Reports the order final but none of its executions yet (visibility lag)."""

    async def order_fills(self, order_id: str) -> list[Fill]:
        return []


async def test_a_final_unsettled_entry_reserves_nothing_but_blocks_its_symbol(
    sessions: async_sessionmaker[AsyncSession],
    gate: ControlGate,
    cycle_id: int,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    broker = _sim(HiddenFillsBroker)
    engine = make_engine_(broker, sessions, gate)
    await engine.check(intent(cycle_id))
    broker.process_bar(five(30))  # filled at the broker, fills not visible
    seen = spy_portfolios(monkeypatch)

    result = await engine.check(intent(await new_cycle(sessions)))
    assert seen[-1].open_entries == (OpenEntry(symbol=BTC, notional=D(0)),)
    assert "already has a position or open entry" in result.decision.reasons[0]
    first = (await orders(sessions))[0]
    assert (first.status, first.settled_at) == ("filled", None)


async def test_an_unreadable_order_fails_entries_closed_but_the_rest_is_booked(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    engine = make_engine_(sim, sessions, gate)
    await engine.check(intent(cycle_id))
    sim.process_bar(five(30))  # the BTC entry fills
    async with sessions() as s:  # an order the broker no longer knows, on another symbol
        ghost = await repo.insert_order(
            s,
            cycle_id=None,
            client_order_id=None,
            symbol="ETH/USD",
            mode="paper",
            type_="limit",
            side="buy",
            qty=D("1"),
            limit_price=D("10"),
            stop_price=None,
            tp_price=None,
            status="pending",
            now=NOW,
        )
        await repo.update_order(s, ghost.id, status="new", now=NOW, broker_order_id="sim-404")

    result = await engine.check(intent(await new_cycle(sessions)))
    assert result.decision.reasons == ("state unavailable: SyncIncomplete",)
    async with sessions() as s:
        (pos,) = list(await s.scalars(select(m.Position)))
    assert pos.qty == D("2.997")  # the readable order was still booked
    exited = await engine.exit(ExitRequest(symbol=BTC, reason="stop"))  # exits never sync
    assert exited.decision.approved and exited.status == "new"


class FailingLedger:
    async def apply_all(self, result: SyncResult) -> None:
        raise RuntimeError("journal down")


async def test_a_failing_ledger_fails_entries_closed(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    result = await make_engine_(sim, sessions, gate, ledger=FailingLedger()).check(intent(cycle_id))
    assert result.decision.reasons == ("state unavailable: RuntimeError",)


async def test_sync_orders_reads_every_unsettled_row_once(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    engine = make_engine_(sim, sessions, gate)
    await engine.check(intent(cycle_id))  # row 1: acked "new", nothing filled yet
    direct = await sim.submit_order(BrokerOrder(symbol=BTC, side="buy", qty=D("0.01"), type="market"))
    async with sessions() as s:

        async def row(status: str, broker_id: str | None) -> int:
            r = await repo.insert_order(
                s,
                cycle_id=None,
                client_order_id=None,
                symbol=BTC,
                mode="paper",
                type_="market",
                side="buy",
                qty=D("0.01"),
                limit_price=None,
                stop_price=None,
                tp_price=None,
                status=status,
                now=NOW,
            )
            if broker_id is not None:
                await repo.update_order(s, r.id, status=status, now=NOW, broker_order_id=broker_id)
            return r.id

        unknown = await row("unknown", None)  # 1.5d's resolver: not read
        never = await row("not_submitted", None)  # local final: no broker call
        ghost = await row("new", "sim-404")  # the broker does not know it
        same_symbol = await row("new", direct.order_id)  # second BTC row: rules cached

    first = await engine.sync_orders()
    assert first.failed == (ghost,)
    by_row = {u.order_row_id: u for u in first.updates}
    assert unknown not in by_row
    assert (by_row[never].status, by_row[never].min_qty) == ("not_submitted", None)
    assert by_row[1].fills == () and by_row[1].min_qty == RULES.min_qty
    sim.process_bar(five(30))
    second = await engine.sync_orders()
    filled = {u.order_row_id: u for u in second.updates}
    assert len(filled[1].fills) == 1 and len(filled[same_symbol].fills) == 1


class DustBroker(SimBroker):
    async def close_position(self, symbol: str) -> OrderAck:
        raise DustRemainder(symbol, D("0.00005"), RULES.min_qty)


async def test_a_dust_close_is_reported_as_dust(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    broker = _sim(DustBroker)
    await _held_btc(broker)
    result = await make_engine_(broker, sessions, gate).exit(ExitRequest(symbol=BTC, reason="kill"))
    assert (result.status, result.dust) == ("rejected", True)
    (row,) = await orders(sessions)
    assert (row.status, row.exit_reason) == ("rejected", "kill")


async def test_snapshot_equity_returns_the_account_it_read(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    account = await make_engine_(sim, sessions, gate).snapshot_equity()
    assert account.cash == D("1000")
```

Row id `1` in `test_sync_orders_reads_every_unsettled_row_once` is the engine's entry (the first `orders` row in a fresh journal). If the first-row id assumption fails in the dry run, read it from `(await orders(sessions))[0].id` instead.

In `tests/unit/journal/test_repo.py`, replace `test_open_entry_orders_and_update` with:

```python
async def test_open_entry_orders_are_the_unsettled_buys(session: AsyncSession) -> None:
    keep = [
        await _order(session, status=s)
        for s in ("pending", "new", "partially_filled", "unknown", "filled", "canceled")
    ]
    done = await _order(session, status="filled")
    await session.execute(update(m.Order).where(m.Order.id == done.id).values(settled_at=T0))
    await session.commit()
    await _order(session, status="new", side="sell")
    assert {o.id for o in await repo.open_entry_orders(session)} == {o.id for o in keep}
```

In `tests/unit/core/test_cycle.py`, append:

```python
class LedgerFirstRisk(FakeRisk):
    """check() returns after another check's sync already completed this cycle (WP 1.5b)."""

    def __init__(self, sessions: async_sessionmaker[AsyncSession]) -> None:
        super().__init__(approved("new"))
        self.sessions = sessions

    async def check(self, intent: EntryIntent) -> OrderResult:
        result = await super().check(intent)
        async with self.sessions() as s:
            await repo.transition_cycle(
                s, intent.cycle_id, from_state="RISK_CHECK", to_state="FILLED", now=NOW
            )
            await s.commit()
        return result


async def test_a_cycle_the_ledger_completed_first_is_not_stuck(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: ControlGate,
    strategy_id: int,
) -> None:
    runner = make_runner(sessions, bus, gate, LedgerFirstRisk(sessions), strategy_id)
    cid = await new_cycle(sessions, strategy_id)
    with capture_logs() as logs:
        await runner._run(cid, candidate())
    assert (await cycle_row(sessions, cid)).state == "FILLED"
    names = [e["event"] for e in logs]
    assert "cycle.completed_elsewhere" in names
    assert "cycle.stuck" not in names and "bus.unit_failed" not in names
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/risk/test_engine.py tests/unit/journal/test_repo.py tests/unit/core/test_cycle.py -q`
Expected: FAIL (`TypeError: RiskEngine.__init__() got an unexpected keyword argument 'ledger'`, `cannot import name 'Accounting'`, the repo test, and `cycle.stuck` logged in the runner test).

- [ ] **Step 3: Implement the engine**

In `src/autotrader/risk/engine.py`:

Imports: `from autotrader.broker.base import Account, Broker, DustRemainder, Fill, Mode, OrderAck, OrderRejected`; add `OrderUpdate, SyncIncomplete, SyncResult` to the `autotrader.risk.types` import. Delete `_BROKER_OPEN` and add:

```python
# Final rows that never reached the broker: nothing to read, the ledger settles them.
_LOCAL_FINAL = ("not_submitted", "rejected")


class Accounting(Protocol):
    """The ledger (`core/ledger.py`, WP 1.5b): books a sync pass. Journal code only."""

    async def apply_all(self, result: SyncResult) -> None: ...
```

`OrderResult` gains `dust: bool = False` (last field).

`__init__`: add the keyword parameter `ledger: Accounting,` after `sessions` and store `self._ledger = ledger`.

In `_gather_entry` replace

```python
        # Refresh our own entries BEFORE reading positions: a fill the refresh observes
        # is then already in the positions read below (never neither-position-nor-entry).
        open_rows = await self._refresh_open_entries(deadline)
```

with

```python
        # Book our own orders BEFORE reading positions: a fill the sync observed is then in
        # the journal and in the positions read below (never neither position nor entry).
        synced = await self._sync(deadline)
        await self._ledger.apply_all(synced)
        if not synced.complete:  # PLAN 3.2 item 4: an unreadable order fails entries closed
            raise SyncIncomplete(f"order rows not synced: {list(synced.failed)}")
        async with self._sessions() as session:
            open_rows = await repo.open_entry_orders(session)
        broker_filled = {u.order_row_id: u.filled_qty for u in synced.updates}
```

and the `open_entries=` argument with

```python
            open_entries=tuple(
                # symbol is always set by insert_order; "" only guards legacy NULL rows and
                # still counts toward max_positions (conservative). The notional is the
                # unfilled remainder: the filled part is already a priced position.
                OpenEntry(
                    symbol=o.symbol or "",
                    notional=max(
                        o.qty - max(broker_filled.get(o.id, o.filled_qty), o.filled_qty),
                        Decimal(0),
                    )
                    * (o.limit_price or Decimal(0)),
                )
                for o in open_rows
            ),
```

Replace `_refresh_open_entries` with:

```python
    async def sync_orders(self) -> SyncResult:
        """Read-only broker view of every unsettled order for the ledger (spec 4.2). Takes
        no lock (never delays an exit) and has its own `io_timeout_s` budget."""
        return await self._sync(self._monotonic() + self._io_timeout)

    async def _sync(self, deadline: float) -> SyncResult:
        """Per-order failures (incl. `OrderNotFound` and a spent budget) are collected,
        never raised: the caller decides, and `check()` then fails closed."""
        async with self._sessions() as session:
            rows = await repo.unsettled_orders(session)
        updates: list[OrderUpdate] = []
        failed: list[int] = []
        min_qty: dict[str, Decimal] = {}
        for row in rows:
            broker_id = row.broker_order_id
            if broker_id is None:
                if row.status in _LOCAL_FINAL:
                    updates.append(
                        OrderUpdate(order_row_id=row.id, status=row.status, filled_qty=Decimal(0))
                    )
                continue  # pending / unknown without an id: the 1.5d resolver's case
            try:
                updates.append(await self._read_order(row, broker_id, min_qty, deadline))
            except Exception:
                log.exception("risk.order_sync_failed", order_row=row.id)
                failed.append(row.id)
        return SyncResult(updates=tuple(updates), failed=tuple(failed))

    async def _read_order(
        self, row: Order, broker_id: str, min_qty: dict[str, Decimal], deadline: float
    ) -> OrderUpdate:
        """Status, then (only if more filled than booked) executions: two reads, so the
        executions may be ahead of the status (the ledger handles that)."""
        status = await self._within(deadline, partial(self._broker.get_order, broker_id))
        fills: list[Fill] = []
        if status.filled_qty > row.filled_qty:
            fills = await self._within(deadline, partial(self._broker.order_fills, broker_id))
        symbol = row.symbol or ""
        if symbol not in min_qty:
            rules = await self._within(deadline, partial(self._broker.asset_rules, symbol))
            min_qty[symbol] = rules.min_qty
        return OrderUpdate(
            order_row_id=row.id,
            status=status.status,
            filled_qty=status.filled_qty,
            fills=tuple(fills),
            min_qty=min_qty[symbol],
        )
```

In `_submit`, add before `except OrderRejected as e:`

```python
        except DustRemainder as e:  # close_position only: report dust as a type (1.5c)
            log.warning("risk.exit_dust", order_row=row_id, qty=str(e.qty), min_qty=str(e.min_qty))
            await self._set_status(row_id, "rejected")
            return OrderResult(decision, row_id, "rejected", dust=True)
```

In `exit()`, pass `exit_reason=request.reason,` to `repo.insert_order`.

`snapshot_equity`: annotate `-> Account` and `return account` at the end (after `_write_snapshot`). Update its docstring: "Returns the account it read, so a caller can feed the ledger's cash check (1.5d) without a second broker call."

Module docstring: replace "a late IOC fill after a kill-flatten can happen with or without a shared lock (fills are broker-side); the Position Manager handles it (WP 1.5)." with "a late IOC fill after a kill-flatten can happen with or without a shared lock (fills are broker-side); the ledger books it (WP 1.5b) and the Position Manager manages it (WP 1.5c)." and add a paragraph: "`check()` first syncs every unsettled order (`sync_orders`) and books it through the ledger (`core/ledger.py`, WP 1.5b); an unreadable order rejects the entry (`state unavailable: SyncIncomplete`). Exits never sync."

- [ ] **Step 4: Repo, runner and runtime**

`src/autotrader/journal/repo.py`: delete `OPEN_ENTRY_STATUSES` and its comment block; replace `open_entry_orders` with

```python
async def open_entry_orders(session: AsyncSession) -> list[Order]:
    """Entry orders that still reserve their symbol: every buy not yet settled by the
    ledger (WP 1.5b), whatever its status. A final but unsettled row (fills not booked
    yet) keeps reserving until the ledger settles it."""
    rows = await session.scalars(
        select(Order)
        .where(Order.side == "buy", Order.settled_at.is_(None))
        .order_by(Order.id)
        .execution_options(populate_existing=True)
    )
    return list(rows)
```

`src/autotrader/core/cycle.py` `_after_check`:

```python
        try:
            await self._advance(cur, to, details, reason)
            return True
        except asyncio.CancelledError:
            raise
        except _WriteFailed as e:
            if isinstance(e.__cause__, repo.StaleTransition):
                # Another check()'s sync booked this cycle's order first and the ledger
                # completed the cycle (WP 1.5b): its state is final, nothing is stuck.
                log.info("cycle.completed_elsewhere", state=cur.state, to_state=to)
            else:
                log.exception("cycle.stuck", state=cur.state, to_state=to)
            return False
        except Exception:  # incl. CommitUncertain: the order row decides what happens
            log.exception("cycle.stuck", state=cur.state, to_state=to)
            return False
```

`src/autotrader/core/bus.py` `_unit_done`: do not log the expected race at error level (plan clarification 6):

```python
        if exc is not None and not isinstance(exc, CommitUncertain | repo.StaleTransition):
            # The caller may be gone (cancelled): without this the failure would vanish.
            # CommitUncertain is logged by the unit; StaleTransition by its caller (the
            # runner logs cycle.completed_elsewhere or cycle.stuck).
            log.error("bus.unit_failed", error=repr(exc))
```

`src/autotrader/core/runtime.py`: `from autotrader.core.ledger import Ledger`; pass `ledger=Ledger(bus, sessions),` to `RiskEngine(...)`; change `_Snapshotter.snapshot_equity` to `async def snapshot_equity(self) -> object: ...`.

`tests/integration/test_cycle_runner.py` `wire()`: create `bus = EventBus(sessions)` before the engine and pass `ledger=Ledger(bus, sessions, clock=lambda: NOW)` to `RiskEngine(...)`; import `Ledger`.

- [ ] **Step 5: Run the tests**

Run: `uv run pytest tests/unit/risk tests/unit/journal tests/unit/core tests/integration -q`
Expected: PASS. If an existing engine or integration test fails, read why before changing it:
- a test that expected an IOC row to stop blocking after the broker canceled it now needs the ledger to settle it: it does inside the next `check()`; adjust only the assertion on intermediate status text, never weaken the blocking/approval assertion;
- a test asserting a reason like `state unavailable: TimeoutError` for a hanging `get_order` now gets `state unavailable: SyncIncomplete`: update the expected reason (the behaviour, fail closed, is unchanged).
Record every such change in the commit message body.

- [ ] **Step 6: Full check including risk coverage, then commit**

Run the full check. Expected: green, `risk/` at 100 % branch coverage. If a branch in `_sync`/`_read_order` is reported missing, add a focused test for it rather than a `pragma: no cover`.

```bash
git add src/autotrader/risk/engine.py src/autotrader/journal/repo.py src/autotrader/core/cycle.py src/autotrader/core/bus.py src/autotrader/core/runtime.py tests/unit/risk/test_engine.py tests/unit/journal/test_repo.py tests/unit/core/test_cycle.py tests/integration/test_cycle_runner.py
git commit -m "WP 1.5b: check() books every order through the ledger and reserves remainders

Entries now sync and book all unsettled orders before reading positions, fail
closed when any order is unreadable, and reserve only the unfilled remainder.
Exits record their reason and report dust as a type; the runner treats a cycle
the ledger completed first as done, not stuck.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Property test and documentation alignment

**Files:**
- Test: `tests/unit/core/test_ledger.py` (property test)
- Modify: `PLAN.md`, `src/autotrader/core/cycle.py` (docstrings/comments), `src/autotrader/risk/engine.py` (if anything stale remains), `src/autotrader/broker/sim.py` (docstring), `CLAUDE.md` only if a command or hard rule changed (none expected)

**Interfaces:**
- Consumes: everything above.

- [ ] **Step 1: Write the property test** (append to `tests/unit/core/test_ledger.py`; add `import tempfile`, `from hypothesis import given, settings`, `from hypothesis import strategies as st`, `from autotrader.broker.base import OrderRejected` and `from autotrader.core.ledger import _cash_effect`)

```python
_trades = st.lists(
    st.tuples(
        st.integers(min_value=2, max_value=40),  # qty in 0.01 BTC
        st.sampled_from(["full", "half", "sliver"]),
        st.floats(min_value=90, max_value=110, allow_nan=False),
        st.floats(min_value=90, max_value=110, allow_nan=False),
    ),
    min_size=1,
    max_size=6,
)


async def _replay(url: str, trades: list[tuple[int, str, float, float]], fee_asset: str) -> None:
    engine = make_engine(url)
    sessions = make_sessionmaker(engine)
    bus = EventBus(sessions)
    ledger = Ledger(bus, sessions, clock=lambda: T0)
    sim = make_sim(QUOTE.model_copy(update={"buy_fee_asset": fee_asset}))
    i = 0
    try:
        for n, plan, price_in, price_out in trades:
            qty = D(n) / 100
            if plan == "half":
                sim.script(BTC, "buy", FillPlan((D("0.5"), "cancel")))
            elif plan == "sliver":
                sim.script(BTC, "buy", FillPlan((D("0.0001") / qty, "cancel")))
            await place(sessions, sim, "buy", str(qty))
            for _ in range(2):  # the fill bar, then the scripted cancel (if any)
                i += 1
                sim.process_bar(bar(i, price_in))
            await ledger.apply_all(await sync(sessions, sim))
            held = await sim.get_positions()
            if not held or held[0].qty < RULES.min_qty:
                continue  # all dust: nothing to close
            try:
                await place(sessions, sim, "close", str(held[0].qty), exit_reason="time")
            except OrderRejected:
                break  # below min notional: open but not closable; one trade per symbol
            i += 1
            sim.process_bar(bar(i, price_out))
            await ledger.apply_all(await sync(sessions, sim))
        async with sessions() as s:
            booked = list(await s.scalars(select(m.Fill)))
            open_qty = sum(
                (p.qty for p in await s.scalars(select(m.Position).where(m.Position.closed_at.is_(None)))),
                D(0),
            )
            dust = await s.get(m.SymbolDust, BTC)
            assert await repo.get_control(s, PAUSED_KEY) is None
        assert abs(D("1000") + sum((_cash_effect(f) for f in booked), D(0)) - sim.cash) < D("1e-18")
        held_qty = sum((p.qty for p in await sim.get_positions()), D(0))
        assert open_qty + (D(0) if dust is None else dust.qty) == held_qty
    finally:
        await bus.close()
        await engine.dispose()


@given(_trades, st.sampled_from(["base", "quote"]))
@settings(max_examples=25, deadline=None)
def test_the_ledger_reproduces_broker_cash_and_holdings(
    trades: list[tuple[int, str, float, float]], fee_asset: str
) -> None:
    with tempfile.TemporaryDirectory() as tmp:
        url = f"sqlite+aiosqlite:///{tmp}/ledger.db"
        upgrade(url)  # sync: Alembic runs its own event loop
        asyncio.run(_replay(url, trades, fee_asset))
```

- [ ] **Step 2: Run it**

Run: `uv run pytest tests/unit/core/test_ledger.py -k reproduces -q`
Expected: PASS. A failure here is a real accounting bug: shrink the example hypothesis prints, add it as an `@example`, and fix the ledger (not the test) after diagnosing it with superpowers:systematic-debugging.

- [ ] **Step 3: Correct stale claims** (search the claim wording, not only the feature names)

Run these searches and fix every hit that the change falsified:

```bash
rg -n "WP 1\.5 reconcil|until WP 1\.5|WP 1\.6 adds the fill stream|_refresh_open_entries|OPEN_ENTRY_STATUSES|dust below minimum|applies each fill once|Position Manager handles it" src tests docs/alpaca-notes.md SPEC.md CLAUDE.md
```

Known hits and their replacements:
- `src/autotrader/core/cycle.py` module docstring: replace "proven outcomes are mapped; `new` / `unknown` / `canceled` stay SUBMITTED until WP 1.5 reconciles the order." with (wrapped at 100 columns) "proven outcomes are mapped; `new` / `canceled` stay SUBMITTED until the ledger (WP 1.5b) settles the order from the broker's filled qty; an `unknown` row without a broker id waits for the 1.5d resolver."
- `src/autotrader/risk/checks.py` (the check 7 comment): "A small partial IOC fill can still leave dust (WP 1.5 reconciles)" -> "A small partial IOC fill can still leave dust (the ledger writes it off at close, WP 1.5b)".
- `core/cycle.py` comment above `RECONCILIATION_ONLY_EDGES`: "Taken only by WP 1.5 reconciliation" -> "Taken only by the ledger (WP 1.5b) when an entry order settles".
- `core/cycle.py` `_risk_check` comment "WP 1.5 reconciles from the broker's fill qty" -> "the ledger settles them from the broker's fill qty (WP 1.5b)".
- `core/cycle.py` `recover()` docstring "leave the rest to WP 1.5" -> "leave the rest to the ledger (WP 1.5b; startup reconciliation is 1.5d)".
- Test-file comments that say "dust below minimum" stay (they match the message, which is unchanged).
Leave dated history in `PLAN.md` and specs untouched.

- [ ] **Step 4: PLAN.md**

- 1.5 row: append "**2026-10-DD, 1.5b done:** ledger (`core/ledger.py`), order sync in `RiskEngine.sync_orders()`, migration 0004, cash check (not yet run by `run core`: 1.5d); follow-ups (a) remainder notional, (b) `DustRemainder`, (c) fills by value are done. Plan clarifications: a position closes only when all its orders are settled; `overfilled` against the order qty applies to buys only; the runner logs `cycle.completed_elsewhere` when the ledger completed its cycle first; an order that never reached the broker completes its cycle `REJECTED` (runner wording) whoever writes first. Spec `docs/superpowers/specs/2026-10-15-fill-position-accounting-design.md`, plan `docs/superpowers/plans/2026-10-15-fill-position-accounting.md`." Replace `DD` with the actual date the PR is opened.
- 1.6 row: append "**2026-10-15 (1.5b):** implement `Broker.order_fills` for Alpaca (activities API, paginated) populating `fills.broker_fill_id`; establish Alpaca's coin fee and rounding exactly (or a bounded allowance) before the ledger's `oversell` check applies to Alpaca fills (spec 2026-10-15 section 4.4)."
- The "Current repo state" paragraph: "WP 1.5a (runtime safety) done. Next: WP 1.5b (fill/position accounting) and WP 1.6 (Alpaca execution)." -> "WP 1.5a (runtime safety) and WP 1.5b (fill/position accounting: `core/ledger.py`) done. Next: WP 1.5c (exit management), then 1.5d and 1.6."

- [ ] **Step 5: Full check, then commit**

Run the full check (including `ruff format --check` after the doc-only edits).

```bash
git add tests/unit/core/test_ledger.py PLAN.md src
git commit -m "WP 1.5b: property test for ledger cash and holdings; align docs

The ledger must reproduce broker cash and holdings for any fill sequence on
SimBroker; docs and comments now name the ledger where they promised WP 1.5.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Self-review checklist (done while writing; re-run after edits)

- Spec 4.1 -> Task 1; 3 -> Task 2; bus tuple (4.3 events) -> Task 3; 4.2 -> Task 7; 4.3/4.4/4.5 -> Tasks 4-5; 4.6 -> Task 6; 4.7 -> Task 7; 6 (property) -> Task 8; 7 (docs) -> Tasks 2 and 8.
- Names used across tasks: `OrderUpdate`, `SyncResult.complete`, `SyncIncomplete`, `Ledger.apply`/`apply_all`/`cash_guard`/`check_cash`, `CashGuard`, `CASH_CHECK_KEY`, `Accounting`, `RiskEngine.sync_orders`, `OrderResult.dust`, `repo.unsettled_orders`, `repo.open_entry_orders`, `insert_order(exit_reason=)`, `DustRemainder`, `Broker.order_fills`, `SymbolDust`.
