# Risk Engine Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Risk Engine (WP 1.3): 11 pure entry checks, a reduce-only exit check, and a `RiskEngine` that is the sole holder of the `Broker`, persists every decision and order, and records equity snapshots.

**Architecture:** `risk/checks.py` holds pure functions over frozen inputs from `risk/types.py`. `risk/engine.py` gathers state from broker, feed view and journal under one lock, calls the pure checks, persists `risk_decisions`/`orders`, applies pause side effects, re-checks the control gate, and submits. `run core` hands the broker to the engine and snapshots equity every 60 s.

**Tech Stack:** Python 3.12, pydantic v2, SQLAlchemy 2 async + Alembic, pytest + hypothesis, uv.

**Spec:** `docs/superpowers/specs/2026-10-11-risk-engine-design.md` (read it first; this plan argues from it).

## Global Constraints

- Only `src/autotrader/risk/engine.py` may hold a `Broker` reference after startup (SPEC §0.1, CLAUDE.md). `run core` connects it and hands it over.
- All money and quantities are `Decimal`; floats are rejected at the boundary (`Dec` from `broker/base.py`). Bar/quote floats convert via `Decimal(str(x))`.
- `src/autotrader/risk/` must reach 100 % branch coverage; CI enforces it.
- `orders.status` is `String(16)`: status values are `pending`, `new`, `filled`, `canceled`, `rejected`, `unknown`, `not_submitted`.
- Exits are never blocked by kill, pause or rate limits (G1).
- Commands: `uv run pytest`, `uv run ruff check`, `uv run ruff format --check`, `uv run mypy`. Run **all four** before every commit (a doc-only edit once broke lint on PR #11).
- Commit messages end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **Acked entry orders never leave status `new`** (order sync is WP 1.6), so they would block their symbol forever. Expected: the engine refreshes its own `new` entry orders from `broker.get_order()` while gathering. Pinned in Task 6 (`test_acked_entry_is_refreshed_from_broker`).
2. **A kill lands between gathering and submit.** Expected: the order row ends `not_submitted`, nothing reaches the broker. Pinned in Task 6.
3. **Submit times out.** Expected: status `unknown`, and the next entry check counts it as open (blocks the symbol, counts toward rate and exposure). Pinned in Task 6.
4. **No equity snapshot exists yet** (first ever check). Expected: the engine snapshots first and uses current equity as the day-start baseline, never rejecting for a missing baseline. Pinned in Task 6.
5. **Exit while killed or paused.** Expected: the exit goes through. Pinned in Task 7.

## Spec amendments found while planning

Apply these to the spec in Task 8 (and they are already reflected in the tasks):
- The engine refreshes its own non-final entry orders (`new` with a broker id) via `broker.get_order()` during gathering. Without it, acked IOC orders would count as open forever until WP 1.6.
- Exit orders carry no `client_order_id`: `Broker.close_position(symbol)` takes none. Exits are identified by `broker_order_id`.

## File structure

| File | Responsibility |
|---|---|
| `src/autotrader/broker/base.py` | add `BrokerOrder.time_in_force`, `AssetRules.price_increment` |
| `src/autotrader/broker/sim.py` | IOC semantics (taker fee, cancel if not filled on next bar) |
| `src/autotrader/broker/alpaca.py` | map `price_increment` |
| `src/autotrader/core/config.py` | `RiskLimits.entry_slippage_bps` + validation |
| `strategies/fast_momentum_v1.yaml` | `entry_slippage_bps: 10` |
| `src/autotrader/core/tradingday.py` | `day_start()`, `next_day_start()` |
| `src/autotrader/core/feed.py` | `quote()`, `last_price()` accessors |
| `src/autotrader/journal/models.py` + migration `0002` | `orders.client_order_id` |
| `src/autotrader/journal/repo.py` | risk decision, order, snapshot, stats queries |
| `src/autotrader/risk/types.py` | frozen inputs/outputs |
| `src/autotrader/risk/checks.py` | `entry_limit`, `check_entry`, `check_exit` |
| `src/autotrader/risk/engine.py` | `RiskEngine` |
| `src/autotrader/core/runtime.py`, `cli.py` | wiring, 60 s snapshots |
| `.github/workflows/ci.yml` | risk coverage gate |

---

### Task 1: Broker types, SimBroker IOC, Alpaca price increment

**Files:**
- Modify: `src/autotrader/broker/base.py` (`BrokerOrder`, `AssetRules`)
- Modify: `src/autotrader/broker/sim.py` (`process_bar`, `_try_fill`, `_estimated_cost`, module docstring)
- Modify: `src/autotrader/broker/alpaca.py:122-129`
- Test: `tests/unit/broker/test_sim.py`, `tests/unit/broker/test_alpaca.py`

**Interfaces:**
- Produces: `BrokerOrder.time_in_force: Literal["gtc", "ioc"] = "gtc"`; `AssetRules.price_increment: Dec (gt=0)` (required).

- [ ] **Step 1: Update fixtures and write failing tests**

In `tests/unit/broker/test_sim.py`, add `price_increment=Decimal("0.01")` to every `AssetRules(...)` (lines 23, 26, 395). Append:

```python
def ioc_buy(qty: str, limit: str, symbol: str = BTC) -> BrokerOrder:
    return BrokerOrder(
        symbol=symbol, side="buy", qty=Decimal(qty), type="limit",
        limit_price=Decimal(limit), time_in_force="ioc",
    )


async def test_ioc_limit_fills_through_limit_with_taker_fee() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100))
    ack = await sim.submit_order(ioc_buy("1", "100.1"))
    sim.process_bar(bar(1, 100.05, h=100.2, low=99.9))
    status = await sim.get_order(ack.order_id)
    assert status.status == "filled"
    assert status.avg_fill_price == Decimal("100.05")  # better of open and limit
    fill = await anext(sim.stream_fills())
    assert fill.fee == Decimal("100.05") * Decimal("25") / Decimal(10_000)  # taker


async def test_ioc_limit_not_reached_on_next_bar_is_canceled() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100))
    ack = await sim.submit_order(ioc_buy("1", "100.1"))
    sim.process_bar(bar(1, 101, h=101.5, low=100.5))
    assert (await sim.get_order(ack.order_id)).status == "canceled"
    sim.process_bar(bar(2, 99, h=99, low=98))  # would have filled a gtc order
    assert (await sim.get_order(ack.order_id)).status == "canceled"
    assert sim.cash == Decimal("1000")


async def test_ioc_cash_reservation_uses_taker_fee() -> None:
    sim = make_sim(cash="100.35")  # 100.1 * 1.0025 = 100.35025 > cash
    sim.process_bar(bar(0, 100))
    with pytest.raises(OrderRejected, match="insufficient cash"):
        await sim.submit_order(ioc_buy("1", "100.1"))
```

In `tests/unit/broker/test_alpaca.py`, extend the expected value at line 106 with `price_increment=Decimal("0.000000001")` (the fixture's raw asset already has `"price_increment": "0.000000001"`).

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/unit/broker -q`
Expected: FAIL (`price_increment` / `time_in_force` unknown fields: `extra="forbid"`).

- [ ] **Step 3: Implement**

`broker/base.py`:

```python
TimeInForce = Literal["gtc", "ioc"]


class BrokerOrder(_Frozen):
    symbol: str
    side: Side
    qty: Dec = Field(gt=0)
    type: OrderType
    limit_price: Dec | None = Field(default=None, gt=0)
    time_in_force: TimeInForce = "gtc"
    client_order_id: str | None = None
    # (keep the existing _limit_price_matches_type validator)


class AssetRules(_Frozen):
    """Per-symbol order constraints used by sizing and entry pricing (G4, D14)."""

    min_qty: Dec = Field(gt=0)
    qty_increment: Dec = Field(gt=0)
    min_notional: Dec = Field(ge=0)
    price_increment: Dec = Field(gt=0)
```

`broker/sim.py` — in `process_bar`, replace the fill loop:

```python
        for o in [o for o in self._orders.values() if o.status == "new"]:
            if o.order.symbol == bar.symbol and bar.ts > o.submitted_at:
                self._try_fill(o, bar)
                if o.status == "new" and o.order.time_in_force == "ioc":
                    o.status = "canceled"  # IOC: only the first bar after submit counts
```

In `_try_fill`, the limit branch's fee line becomes:

```python
            # An IOC limit crosses the book at submit (marketable limit): taker, not maker.
            ioc = order.time_in_force == "ioc"
            fee_bps = self.fees.taker_bps if ioc else self.fees.maker_bps
```

`_estimated_cost` limit branch:

```python
        if order.limit_price is not None:
            fee_bps = self.fees.taker_bps if order.time_in_force == "ioc" else self.fees.maker_bps
            return order.limit_price * order.qty * (1 + fee_bps / _BPS)
```

Add to the module docstring's fill model list: `- IOC limit: evaluated on the next processed bar only; fills like a limit but pays the taker fee (it crosses the book), otherwise canceled.`

`broker/alpaca.py` `asset_rules`: add `price_increment=Decimal(raw["price_increment"]),`.

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/unit/broker -q`
Expected: PASS.

- [ ] **Step 5: Full checks and commit**

```bash
uv run pytest -q && uv run ruff check && uv run ruff format --check && uv run mypy
git add src/autotrader/broker tests/unit/broker
git commit -m "Broker: IOC time in force and price increment for marketable entries (D14)"
```

---

### Task 2: Profile setting `entry_slippage_bps`

**Files:**
- Modify: `src/autotrader/core/config.py` (`RiskLimits`)
- Modify: `strategies/fast_momentum_v1.yaml` (risk block)
- Test: `tests/unit/core/test_config.py`

**Interfaces:**
- Produces: `RiskLimits.entry_slippage_bps: Decimal (ge 0)`, validated `entry_slippage_bps / 100 < max_price_deviation_pct`.

- [ ] **Step 1: Failing tests**

Add to the `test_invalid_profile_rejected` parametrize list:

```python
        ("risk.entry_slippage_bps", 100, "entry_slippage_bps"),  # 1 % = max deviation
        ("risk.entry_slippage_bps", -1, "entry_slippage_bps"),
```

And a new test:

```python
def test_entry_slippage_loaded_exactly(profile_data: dict[str, Any]) -> None:
    assert parse_profile(profile_data).risk.entry_slippage_bps == Decimal("10")
```

(`Decimal` import if missing: `from decimal import Decimal`.)

- [ ] **Step 2: Run** `uv run pytest tests/unit/core/test_config.py -q` — Expected: FAIL.

- [ ] **Step 3: Implement**

In `RiskLimits` add `entry_slippage_bps: Annotated[Decimal, Field(ge=0)]` after `max_price_deviation_pct`, and extend `_stop_band` (rename to `_consistent`):

```python
    @model_validator(mode="after")
    def _consistent(self) -> Self:
        if self.min_stop_atr >= self.max_stop_atr:
            raise ValueError("min_stop_atr must be below max_stop_atr")
        # Our own entry allowance must never trip the price-sanity check (D14).
        if self.entry_slippage_bps / 100 >= self.max_price_deviation_pct:
            raise ValueError("entry_slippage_bps must stay below max_price_deviation_pct")
        return self
```

In the YAML risk block after `max_price_deviation_pct: 1.0`:

```yaml
  entry_slippage_bps: 10                      # D14: marketable IOC limit = ask + this
```

- [ ] **Step 4: Run** `uv run pytest -q` — Expected: PASS (all profile-loading tests pick up the new field).

- [ ] **Step 5: Full checks and commit** `git add src/autotrader/core/config.py strategies tests/unit/core/test_config.py` — message: `Profile: risk.entry_slippage_bps for marketable entries (D14)`.

---

### Task 3: Trading-day helpers, journal migration and repo functions

**Files:**
- Create: `src/autotrader/core/tradingday.py`, `tests/unit/core/test_tradingday.py`
- Modify: `src/autotrader/journal/models.py` (`Order`: `client_order_id`, `symbol`)
- Create: `src/autotrader/journal/migrations/versions/20261011_0002_orders_client_order_id.py`
- Modify: `src/autotrader/journal/repo.py`
- Test: `tests/unit/journal/test_repo.py`

**Interfaces:**
- Produces (`core/tradingday.py`): `day_start(now: datetime, tz: str) -> datetime`, `next_day_start(now: datetime, tz: str) -> datetime` (both UTC-aware).
- Produces (`journal/repo.py`):
  - `OPEN_ENTRY_STATUSES = ("pending", "new", "unknown")`
  - `async record_risk_decision(session, *, cycle_id: int | None, kind: str, approved: bool, reasons: list[str], adjusted: dict[str, str], computed_size: Decimal | None, ts: datetime) -> RiskDecision`
  - `async insert_order(session, *, cycle_id, client_order_id, symbol, mode, type_, side, qty, limit_price, stop_price, tp_price, status, now) -> Order`
  - `async update_order(session, order_id: int, *, status: str, now: datetime, broker_order_id: str | None = None) -> None`
  - `async open_entry_orders(session) -> list[Order]`
  - `async count_entry_orders(session, since: datetime, *, inclusive: bool) -> int`
  - `async insert_equity_snapshot(session, *, ts, equity, cash, exposure, hwm, daily_pnl) -> None`
  - `async latest_snapshot(session) -> EquitySnapshot | None`
  - `async day_start_equity(session, day_start: datetime) -> Decimal | None`
  - `async strategy_trade_stats(session, strategy_id: int) -> tuple[int, int]` (closed trades, wins)

- [ ] **Step 1: Failing tests — trading day**

`tests/unit/core/test_tradingday.py`:

```python
from datetime import UTC, datetime

from autotrader.core.tradingday import day_start, next_day_start


def test_utc_day_boundaries() -> None:
    now = datetime(2026, 10, 9, 15, 30, tzinfo=UTC)
    assert day_start(now, "UTC") == datetime(2026, 10, 9, tzinfo=UTC)
    assert next_day_start(now, "UTC") == datetime(2026, 10, 10, tzinfo=UTC)


def test_berlin_day_on_the_25_hour_dst_day() -> None:
    now = datetime(2026, 10, 25, 22, 30, tzinfo=UTC)  # 23:30 CET on the 25th
    assert day_start(now, "Europe/Berlin") == datetime(2026, 10, 24, 22, tzinfo=UTC)  # CEST
    assert next_day_start(now, "Europe/Berlin") == datetime(2026, 10, 25, 23, tzinfo=UTC)
```

- [ ] **Step 2: Failing tests — repo** (append to `tests/unit/journal/test_repo.py`; add imports `from decimal import Decimal`)

```python
async def _cycle(session: AsyncSession) -> int:
    strategy = await repo.register_strategy(session, load_profile(PROFILE_PATH), "yaml")
    row = m.Cycle(strategy_id=strategy.id, symbol="BTC/USD", rule_id="r", signal_features={},
                  state="RISK_CHECK", created_at=T0)
    session.add(row)
    await session.commit()
    return row.id


async def _order(session: AsyncSession, *, status: str, side: str = "buy",
                 at: datetime = T0, cycle_id: int | None = None) -> m.Order:
    return await repo.insert_order(
        session, cycle_id=cycle_id, client_order_id=None, symbol="BTC/USD", mode="paper",
        type_="limit",
        side=side, qty=Decimal("1"), limit_price=Decimal("100"), stop_price=None,
        tp_price=None, status=status, now=at,
    )


async def test_risk_decision_roundtrip(session: AsyncSession) -> None:
    cycle_id = await _cycle(session)
    row = await repo.record_risk_decision(
        session, cycle_id=cycle_id, kind="entry", approved=False, reasons=["x"],
        adjusted={"entry": "100.1"}, computed_size=None, ts=T0,
    )
    assert (row.kind, row.reasons, row.adjusted) == ("entry", ["x"], {"entry": "100.1"})


async def test_client_order_id_is_unique(session: AsyncSession) -> None:
    cycle_id = await _cycle(session)
    kw: dict[str, Any] = dict(
        cycle_id=cycle_id, client_order_id="at-c1-e", symbol="BTC/USD", mode="paper",
        type_="limit", side="buy",
        qty=Decimal("1"), limit_price=Decimal("1"), stop_price=None, tp_price=None,
        status="pending", now=T0,
    )
    await repo.insert_order(session, **kw)
    with pytest.raises(IntegrityError):
        await repo.insert_order(session, **kw)


async def test_open_entry_orders_and_update(session: AsyncSession) -> None:
    keep = [await _order(session, status=s) for s in ("pending", "new", "unknown")]
    for s in ("filled", "canceled", "rejected", "not_submitted"):
        await _order(session, status=s)
    await _order(session, status="new", side="sell")
    assert {o.id for o in await repo.open_entry_orders(session)} == {o.id for o in keep}
    await repo.update_order(session, keep[1].id, status="filled", now=T0, broker_order_id="b1")
    assert {o.id for o in await repo.open_entry_orders(session)} == {keep[0].id, keep[2].id}


async def test_count_entry_orders_window(session: AsyncSession) -> None:
    await _order(session, status="filled", at=T0)
    await _order(session, status="rejected", at=T0 + timedelta(minutes=30))
    await _order(session, status="not_submitted", at=T0 + timedelta(minutes=40))  # never sent
    await _order(session, status="filled", side="sell", at=T0 + timedelta(minutes=40))  # exit
    assert await repo.count_entry_orders(session, T0, inclusive=True) == 2
    assert await repo.count_entry_orders(session, T0, inclusive=False) == 1


async def test_snapshots_and_day_start_equity(session: AsyncSession) -> None:
    assert await repo.latest_snapshot(session) is None
    assert await repo.day_start_equity(session, T0) is None
    for minutes, equity in ((-10, "990"), (5, "995")):
        await repo.insert_equity_snapshot(
            session, ts=T0 + timedelta(minutes=minutes), equity=Decimal(equity),
            cash=Decimal(equity), exposure=Decimal(0), hwm=Decimal("1000"), daily_pnl=Decimal(0),
        )
    assert await repo.day_start_equity(session, T0) == Decimal("990")  # last at/before
    assert await repo.day_start_equity(session, T0 - timedelta(hours=1)) == Decimal("990")  # first after
    latest = await repo.latest_snapshot(session)
    assert latest is not None and latest.equity == Decimal("995")


async def test_strategy_trade_stats_counts_closed_trades_of_that_strategy(
    session: AsyncSession,
) -> None:
    cycle_id = await _cycle(session)
    cycle = await session.get(m.Cycle, cycle_id)
    assert cycle is not None
    for pnl, closed in (("5", True), ("-3", True), ("1", True), ("9", False)):
        session.add(m.Position(cycle_id=cycle_id, symbol="BTC/USD", side="long",
                               qty=Decimal(1), entry_price=Decimal(1), opened_at=T0,
                               closed_at=T0 if closed else None, realized_pnl=Decimal(pnl)))
    await session.commit()
    assert await repo.strategy_trade_stats(session, cycle.strategy_id) == (3, 2)
    assert await repo.strategy_trade_stats(session, cycle.strategy_id + 1) == (0, 0)
```

(Imports to add at the top: `from typing import Any`, `from sqlalchemy.exc import IntegrityError`.)

- [ ] **Step 3: Run** `uv run pytest tests/unit/core/test_tradingday.py tests/unit/journal -q` — Expected: FAIL (missing module/functions).

- [ ] **Step 4: Implement `core/tradingday.py`**

```python
"""Trading-day boundaries (G6, D5): 00:00 in `trading_day_tz`, returned as UTC."""

from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo


def day_start(now: datetime, tz: str) -> datetime:
    zone = ZoneInfo(tz)
    return datetime.combine(now.astimezone(zone).date(), time(), tzinfo=zone).astimezone(UTC)


def next_day_start(now: datetime, tz: str) -> datetime:
    zone = ZoneInfo(tz)
    tomorrow = now.astimezone(zone).date() + timedelta(days=1)
    return datetime.combine(tomorrow, time(), tzinfo=zone).astimezone(UTC)
```

- [ ] **Step 5: Model + migration**

`models.py` `Order`: after `broker_order_id` add

```python
    client_order_id: Mapped[str | None] = mapped_column(String(64), unique=True)  # D14
    # SPEC §7 has no symbol on orders, but exits from a kill/flatten have no cycle to
    # take it from, and open entry orders must block their symbol (check 5).
    symbol: Mapped[str | None] = mapped_column(String(32))
```

`migrations/versions/20261011_0002_orders_client_order_id.py`:

```python
"""orders.client_order_id (WP 1.3, D14)

Revision ID: 0002
Revises: 0001
Create Date: 2026-10-11 15:00:00
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | Sequence[str] | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    with op.batch_alter_table("orders") as batch:
        batch.add_column(sa.Column("client_order_id", sa.String(length=64), nullable=True))
        batch.add_column(sa.Column("symbol", sa.String(length=32), nullable=True))
        batch.create_unique_constraint(op.f("uq_orders_client_order_id"), ["client_order_id"])


def downgrade() -> None:
    with op.batch_alter_table("orders") as batch:
        batch.drop_constraint(op.f("uq_orders_client_order_id"), type_="unique")
        batch.drop_column("symbol")
        batch.drop_column("client_order_id")
```

- [ ] **Step 6: Repo functions** (`journal/repo.py`; extend the models import with `EquitySnapshot, Order, RiskDecision`; update the module docstring's first paragraph to say WP 1.3 adds the risk-decision, order and equity-snapshot functions)

```python
# --- risk decisions / orders / equity (WP 1.3) ---------------------------------------

# Entry orders that may still fill: not yet acked, acked and not final, or unknown
# after an ambiguous submit failure (WP 1.5 reconciles those by client_order_id).
OPEN_ENTRY_STATUSES = ("pending", "new", "unknown")


async def record_risk_decision(
    session: AsyncSession,
    *,
    cycle_id: int | None,
    kind: str,
    approved: bool,
    reasons: list[str],
    adjusted: dict[str, str],
    computed_size: Decimal | None,
    ts: datetime,
) -> RiskDecision:
    row = RiskDecision(cycle_id=cycle_id, kind=kind, approved=approved, reasons=reasons,
                       adjusted=adjusted, computed_size=computed_size, ts=ts)
    session.add(row)
    await session.commit()
    return row


async def insert_order(
    session: AsyncSession,
    *,
    cycle_id: int | None,
    client_order_id: str | None,
    symbol: str,
    mode: str,
    type_: str,
    side: str,
    qty: Decimal,
    limit_price: Decimal | None,
    stop_price: Decimal | None,
    tp_price: Decimal | None,
    status: str,
    now: datetime,
) -> Order:
    row = Order(cycle_id=cycle_id, client_order_id=client_order_id, symbol=symbol, mode=mode,
                type=type_,
                side=side, qty=qty, limit_price=limit_price, stop_price=stop_price,
                tp_price=tp_price, status=status, submitted_at=now, updated_at=now)
    session.add(row)
    await session.commit()
    return row


async def update_order(
    session: AsyncSession,
    order_id: int,
    *,
    status: str,
    now: datetime,
    broker_order_id: str | None = None,
) -> None:
    values: dict[str, Any] = {"status": status, "updated_at": now}
    if broker_order_id is not None:
        values["broker_order_id"] = broker_order_id
    await session.execute(update(Order).where(Order.id == order_id).values(**values))
    await session.commit()


async def open_entry_orders(session: AsyncSession) -> list[Order]:
    rows = await session.scalars(
        select(Order)
        .where(Order.side == "buy", Order.status.in_(OPEN_ENTRY_STATUSES))
        .order_by(Order.id)
        .execution_options(populate_existing=True)
    )
    return list(rows)


async def count_entry_orders(session: AsyncSession, since: datetime, *, inclusive: bool) -> int:
    """Entry orders that were sent (or may have been) since `since`; `not_submitted`
    never reached the broker and does not count."""
    after = Order.submitted_at >= since if inclusive else Order.submitted_at > since
    count = await session.scalar(
        select(func.count())
        .select_from(Order)
        .where(Order.side == "buy", Order.status != "not_submitted", after)
    )
    return int(count or 0)


async def insert_equity_snapshot(
    session: AsyncSession,
    *,
    ts: datetime,
    equity: Decimal,
    cash: Decimal,
    exposure: Decimal,
    hwm: Decimal,
    daily_pnl: Decimal,
) -> None:
    session.add(EquitySnapshot(ts=ts, equity=equity, cash=cash, exposure=exposure, hwm=hwm,
                               daily_pnl=daily_pnl))
    await session.commit()


async def latest_snapshot(session: AsyncSession) -> EquitySnapshot | None:
    return await session.scalar(
        select(EquitySnapshot).order_by(EquitySnapshot.ts.desc()).limit(1)
    )


async def day_start_equity(session: AsyncSession, day_start: datetime) -> Decimal | None:
    """Equity of the last snapshot at or before `day_start`; if there is none (first day
    of operation), the first snapshot after it. None without any snapshot."""
    before = await session.scalar(
        select(EquitySnapshot.equity)
        .where(EquitySnapshot.ts <= day_start)
        .order_by(EquitySnapshot.ts.desc())
        .limit(1)
    )
    if before is not None:
        return before
    return await session.scalar(
        select(EquitySnapshot.equity)
        .where(EquitySnapshot.ts > day_start)
        .order_by(EquitySnapshot.ts)
        .limit(1)
    )


async def strategy_trade_stats(session: AsyncSession, strategy_id: int) -> tuple[int, int]:
    """(closed trades, wins) of a strategy; a win is realized P&L > 0 after fees."""
    closed = (
        select(Position.realized_pnl)
        .join(Cycle, Position.cycle_id == Cycle.id)
        .where(Cycle.strategy_id == strategy_id, Position.closed_at.is_not(None))
    )
    pnls = list(await session.scalars(closed))
    return len(pnls), sum(1 for p in pnls if p is not None and p > 0)
```

(Add `from decimal import Decimal` to repo imports.)

- [ ] **Step 7: Run** `uv run pytest tests/unit/core/test_tradingday.py tests/unit/journal -q` — Expected: PASS, including the existing migration-vs-model drift test in `test_db.py`.

- [ ] **Step 8: Full checks and commit** — `git add src/autotrader/core/tradingday.py src/autotrader/journal tests/unit/core/test_tradingday.py tests/unit/journal` — message: `Journal: client order ids, risk decision/order/equity queries; trading-day helpers`.

---

### Task 4: Risk types, entry pricing, checks 1–6 and the exit check

**Files:**
- Create: `src/autotrader/risk/types.py`, `src/autotrader/risk/checks.py`
- Create: `tests/unit/risk/__init__.py`, `tests/unit/risk/test_checks.py`

**Interfaces:**
- Consumes: `BrokerOrder`, `AssetRules`, `FeeSchedule`, `Dec` (Task 1); `StrategyProfile.risk.entry_slippage_bps` (Task 2); `ControlStatus` (`core/kill.py`).
- Produces (`risk/types.py`): `EntryIntent`, `ExitRequest`, `ExitReason`, `HeldPosition`, `OpenEntry`, `PortfolioState`, `MarketSnapshot`, `RiskDecision`, `Pause`.
- Produces (`risk/checks.py`): `entry_limit(ask, slippage_bps, price_increment) -> Decimal`; `check_entry(intent, portfolio, market, *, profile, control, mode_matches) -> RiskDecision`; `check_exit(request, positions, *, mode_matches) -> RiskDecision`.

- [ ] **Step 1: `risk/types.py`** (types only; exercised by the tests below)

```python
"""Risk Engine inputs and outputs (SPEC §4.5, spec 2026-10-11). Frozen; exact Decimals."""

from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from autotrader.broker.base import AssetRules, BrokerOrder, Dec, FeeSchedule


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class EntryIntent(_Frozen):
    """What the Decision Maker wants. No qty and no price: the engine sizes and prices."""

    cycle_id: int
    symbol: str
    stop: Dec = Field(gt=0)
    take_profit: Dec = Field(gt=0)
    confidence: Dec = Field(ge=0, le=1)
    size_fraction: Dec = Field(gt=0, le=1)


ExitReason = Literal["stop", "take_profit", "time", "trailing", "kill", "flatten", "stale"]


class ExitRequest(_Frozen):
    """Reduce-only, full exit (G1)."""

    symbol: str
    reason: ExitReason
    cycle_id: int | None = None


class HeldPosition(_Frozen):
    symbol: str
    qty: Dec
    market_value: Dec
    min_qty: Dec  # below it the position is dust: unsellable, ignored by the checks

    @property
    def is_dust(self) -> bool:
        return self.qty < self.min_qty


class OpenEntry(_Frozen):
    symbol: str
    notional: Dec


class PortfolioState(_Frozen):
    equity: Dec
    cash: Dec
    day_start_equity: Dec
    hwm: Dec
    positions: tuple[HeldPosition, ...] = ()
    open_entries: tuple[OpenEntry, ...] = ()
    entries_last_hour: int = 0
    entries_today: int = 0
    strategy_trades_n: int = 0
    strategy_win_rate: Dec | None = None


class MarketSnapshot(_Frozen):
    last_trade: Dec | None
    bid: Dec | None
    ask: Dec | None
    spread_bps: Dec | None
    atr: Dec | None
    stale: bool
    rules: AssetRules
    fees: FeeSchedule


Pause = Literal["until_next_day", "manual"]


class RiskDecision(_Frozen):
    approved: bool
    reasons: tuple[str, ...]
    adjusted: dict[str, str] = Field(default_factory=dict)
    qty: Dec | None = None
    order: BrokerOrder | None = None
    pause: Pause | None = None  # side effect the engine must apply on rejection


ZERO = Decimal(0)
```

- [ ] **Step 2: Failing tests** (`tests/unit/risk/test_checks.py`, first part; create empty `tests/unit/risk/__init__.py`)

```python
"""Pure risk checks (spec §4, §5). Base case: entry 100.1 (ask 100 + 10 bps), stop 98
(d = 2.1), ATR 2, equity 1000 -> qty 2.997 bound by the 30 % position cap."""

from decimal import Decimal as D
from pathlib import Path
from typing import Any

import pytest

from autotrader.broker.base import AssetRules, FeeSchedule
from autotrader.core.config import StrategyProfile, load_profile
from autotrader.core.kill import ControlStatus
from autotrader.risk.checks import check_entry, check_exit, entry_limit
from autotrader.risk.types import (
    EntryIntent, ExitRequest, HeldPosition, MarketSnapshot, OpenEntry, PortfolioState,
)

PROFILE: StrategyProfile = load_profile(
    Path(__file__).resolve().parents[3] / "strategies" / "fast_momentum_v1.yaml"
)
BTC, ETH, SOL = "BTC/USD", "ETH/USD", "SOL/USD"
RULES = AssetRules(min_qty=D("0.0001"), qty_increment=D("0.0001"), min_notional=D("1"),
                   price_increment=D("0.01"))
FEES = FeeSchedule(maker_bps=D("15"), taker_bps=D("25"))
ALLOWED = ControlStatus(killed=False, reasons=())

INTENT = EntryIntent(cycle_id=7, symbol=BTC, stop=D("98"), take_profit=D("106"),
                     confidence=D("0.6"), size_fraction=D("1"))
PORTFOLIO = PortfolioState(equity=D("1000"), cash=D("1000"), day_start_equity=D("1000"),
                           hwm=D("1000"))
MARKET = MarketSnapshot(last_trade=D("100"), bid=D("99.98"), ask=D("100"), spread_bps=D("2"),
                        atr=D("2"), stale=False, rules=RULES, fees=FEES)


def run(intent: dict[str, Any] | None = None, portfolio: dict[str, Any] | None = None,
        market: dict[str, Any] | None = None, *, control: ControlStatus = ALLOWED,
        mode_matches: bool = True):  # noqa: ANN201 - test helper
    return check_entry(
        INTENT.model_copy(update=intent or {}),
        PORTFOLIO.model_copy(update=portfolio or {}),
        MARKET.model_copy(update=market or {}),
        profile=PROFILE, control=control, mode_matches=mode_matches,
    )


def held(symbol: str, qty: str = "1", value: str = "100") -> HeldPosition:
    return HeldPosition(symbol=symbol, qty=D(qty), market_value=D(value), min_qty=D("0.001"))


def test_entry_limit_rounds_up_to_price_increment() -> None:
    assert entry_limit(D("100"), D("10"), D("0.01")) == D("100.10")
    assert entry_limit(D("100.003"), D("10"), D("0.01")) == D("100.11")  # 100.103003 up


def test_base_case_is_approved_as_marketable_ioc_limit() -> None:
    d = run()
    assert d.approved, d.reasons
    assert d.qty == D("2.997")
    assert d.adjusted["bound_by"] == "position"
    assert d.order is not None
    assert (d.order.type, d.order.time_in_force, d.order.limit_price) == ("limit", "ioc", D("100.10"))
    assert (d.order.side, d.order.qty, d.order.client_order_id) == ("buy", D("2.997"), "at-c7-e")


@pytest.mark.parametrize(
    ("kwargs", "reason"),
    [
        # 1 control and mode
        ({"control": ControlStatus(killed=True, reasons=("kill file present",))}, "control"),
        ({"mode_matches": False}, "mode"),
        # 2 market
        ({"intent": {"symbol": "DOGE/USD"}}, "universe"),
        ({"market": {"stale": True}}, "stale"),
        ({"market": {"spread_bps": None}}, "quote"),
        ({"market": {"ask": None}}, "quote"),
        ({"market": {"last_trade": None}}, "quote"),
        ({"market": {"spread_bps": D("15.01")}}, "spread"),
        # 5 positions
        ({"portfolio": {"positions": (held(BTC),)}}, "already"),
        ({"portfolio": {"open_entries": (OpenEntry(symbol=BTC, notional=D("1")),)}}, "already"),
        ({"portfolio": {"positions": (held(ETH),),
                        "open_entries": (OpenEntry(symbol=SOL, notional=D("1")),)}}, "max_positions"),
        # 6 stop
        ({"market": {"atr": None}}, "ATR"),
        ({"intent": {"stop": D("100.10")}}, "below entry"),
        ({"intent": {"stop": D("98.51")}}, "stop distance"),  # d = 1.59 < 0.8 x 2
        ({"intent": {"stop": D("94.09")}}, "stop distance"),  # d = 6.01 > 3.0 x 2
    ],
)
def test_entry_rejections_checks_1_to_6(kwargs: dict[str, Any], reason: str) -> None:
    d = run(**kwargs)
    assert not d.approved
    assert reason in d.reasons[0]
    assert d.order is None and d.pause is None


@pytest.mark.parametrize(
    "kwargs",
    [
        {"market": {"spread_bps": D("15")}},  # at the limit
        {"intent": {"stop": D("98.5")}},  # d = 1.6 = 0.8 x ATR
        # d = 6.0 = 3.0 x ATR; tp 120 keeps expectancy positive once Task 5 adds check 11
        {"intent": {"stop": D("94.1"), "take_profit": D("120")}},
        # dust ETH is ignored: 1 open entry (SOL) < 2
        {"portfolio": {"positions": (held(ETH, qty="0.0005"),),
                       "open_entries": (OpenEntry(symbol=SOL, notional=D("1")),)}},
        {"portfolio": {"positions": (held(BTC, qty="0.0005"),)}},  # dust in the same symbol
    ],
)
def test_entry_boundaries_pass_checks_1_to_6(kwargs: dict[str, Any]) -> None:
    d = run(**kwargs)
    assert d.approved, d.reasons


def test_daily_loss_rejects_and_pauses_until_next_day() -> None:
    assert run(portfolio={"equity": D("970.01")}).approved  # loss 29.99 < 30
    d = run(portfolio={"equity": D("970")})  # loss 30 = 3 % of 1000
    assert not d.approved and "daily loss" in d.reasons[0]
    assert d.pause == "until_next_day"


def test_drawdown_rejects_and_pauses_manually() -> None:
    ok = {"equity": D("990.01"), "day_start_equity": D("990.01"), "hwm": D("1100")}
    assert run(portfolio=ok).approved  # 109.99 < 110
    d = run(portfolio={**ok, "equity": D("990"), "day_start_equity": D("990")})
    assert not d.approved and "drawdown" in d.reasons[0]
    assert d.pause == "manual"


# --- exit ----------------------------------------------------------------------------

def test_exit_approved_for_held_position_with_broker_qty() -> None:
    d = check_exit(ExitRequest(symbol=BTC, reason="stop"), [held(BTC, qty="2.9895")],
                   mode_matches=True)
    assert d.approved and d.qty == D("2.9895") and d.reasons == ("exit: stop",)


@pytest.mark.parametrize(
    ("positions", "mode_matches", "reason"),
    [
        ([held(BTC)], False, "mode"),
        ([], True, "no position"),
        ([held(ETH)], True, "no position"),
        ([held(BTC, qty="0.0005")], True, "no position"),  # dust
    ],
)
def test_exit_rejections(positions: list[HeldPosition], mode_matches: bool, reason: str) -> None:
    d = check_exit(ExitRequest(symbol=BTC, reason="kill"), positions, mode_matches=mode_matches)
    assert not d.approved and reason in d.reasons[0]
```

- [ ] **Step 3: Run** `uv run pytest tests/unit/risk -q` — Expected: FAIL (`autotrader.risk.checks` missing).

- [ ] **Step 4: Implement `risk/checks.py`** (checks 1–6, pricing, exit; checks 7–11 come in Task 5 — until then the function approves after check 6 with the sizing below so the base-case test passes)

```python
"""Pure Risk Engine checks (SPEC §4.5, spec 2026-10-11 §4-5). No I/O, no clock.

`check_entry` evaluates the 11 checks in order and stops at the first failure; that
reason is what gets persisted. `check_exit` is reduce-only (G1): it never looks at
control state, pauses or rate limits, so a stop-out can't be blocked.
"""

from collections.abc import Sequence
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal

from autotrader.broker.base import BrokerOrder
from autotrader.core.config import StrategyProfile
from autotrader.core.kill import ControlStatus
from autotrader.risk.types import (
    EntryIntent, ExitRequest, HeldPosition, MarketSnapshot, Pause, PortfolioState, RiskDecision,
)

_BPS = Decimal(10_000)
_PCT = Decimal(100)
WIN_RATE_MARGIN = Decimal("0.10")  # SPEC §4.5 #11: confidence capped at win rate + 10 pp


def _reject(reason: str, pause: Pause | None = None) -> RiskDecision:
    return RiskDecision(approved=False, reasons=(reason,), pause=pause)


def _step(value: Decimal, step: Decimal, rounding: str) -> Decimal:
    return (value / step).to_integral_value(rounding=rounding) * step


def entry_limit(ask: Decimal, slippage_bps: Decimal, price_increment: Decimal) -> Decimal:
    """Marketable IOC limit (D14): ask plus the allowance, rounded up to the price step."""
    return _step(ask * (1 + slippage_bps / _BPS), price_increment, ROUND_CEILING)


def check_entry(
    intent: EntryIntent,
    portfolio: PortfolioState,
    market: MarketSnapshot,
    *,
    profile: StrategyProfile,
    control: ControlStatus,
    mode_matches: bool,
) -> RiskDecision:
    limits = profile.risk
    # 1. control and mode
    if not control.entries_allowed:
        return _reject("control: " + "; ".join(control.reasons))
    if not mode_matches:
        return _reject("mode mismatch between config and broker")
    # 2. market
    if intent.symbol not in profile.universe:
        return _reject(f"{intent.symbol} not in universe")
    if market.stale:
        return _reject("symbol stale")
    if market.spread_bps is None or market.ask is None or market.last_trade is None:
        return _reject("no quote or last trade")
    if market.spread_bps > profile.scanner.max_spread_bps:
        return _reject(f"spread {market.spread_bps} bps > {profile.scanner.max_spread_bps}")
    # 3. daily loss
    loss = portfolio.day_start_equity - portfolio.equity
    if loss >= limits.max_daily_loss_pct / _PCT * portfolio.day_start_equity:
        return _reject(f"daily loss {loss} >= {limits.max_daily_loss_pct} %", "until_next_day")
    # 4. drawdown
    drawdown = portfolio.hwm - portfolio.equity
    if drawdown >= limits.max_drawdown_pct / _PCT * portfolio.hwm:
        return _reject(f"drawdown {drawdown} >= {limits.max_drawdown_pct} %", "manual")
    # 5. positions (dust ignored)
    held = {p.symbol for p in portfolio.positions if not p.is_dust}
    busy = held | {o.symbol for o in portfolio.open_entries}
    if intent.symbol in busy:
        return _reject(f"{intent.symbol} already has a position or open entry order")
    if len(busy) >= limits.max_positions:
        return _reject(f"{len(busy)} positions/open entries >= max_positions")
    # 6. stop
    entry = entry_limit(market.ask, limits.entry_slippage_bps, market.rules.price_increment)
    if market.atr is None:
        return _reject("ATR unavailable")
    if intent.stop >= entry:
        return _reject(f"stop {intent.stop} not below entry {entry}")
    distance = entry - intent.stop
    if not limits.min_stop_atr * market.atr <= distance <= limits.max_stop_atr * market.atr:
        return _reject(f"stop distance {distance} outside {limits.min_stop_atr}-"
                       f"{limits.max_stop_atr} x ATR {market.atr}")
    return _size_and_finish(intent, portfolio, market, profile, entry, distance)


def _size_and_finish(
    intent: EntryIntent,
    portfolio: PortfolioState,
    market: MarketSnapshot,
    profile: StrategyProfile,
    entry: Decimal,
    distance: Decimal,
) -> RiskDecision:
    limits, rules = profile.risk, market.rules
    taker = market.fees.taker_bps / _BPS
    # 7. size (G4 + cash cap, D14)
    caps = {
        "risk": limits.risk_per_trade_pct / _PCT * portfolio.equity / distance,
        "position": limits.max_position_pct / _PCT * portfolio.equity / entry,
        "cash": portfolio.cash / (entry * (1 + taker)),
    }
    bound_by = min(caps, key=caps.__getitem__)
    qty = _step(intent.size_fraction * caps[bound_by], rules.qty_increment, ROUND_FLOOR)
    if qty < rules.min_qty or qty * entry < rules.min_notional:
        return _reject(f"size {qty} below broker minimum")
    order = BrokerOrder(
        symbol=intent.symbol, side="buy", qty=qty, type="limit", limit_price=entry,
        time_in_force="ioc", client_order_id=f"at-c{intent.cycle_id}-e",
    )
    return RiskDecision(
        approved=True,
        reasons=(f"sized by {bound_by} cap",),
        adjusted={"entry": str(entry), "bound_by": bound_by},
        qty=qty,
        order=order,
    )


def check_exit(
    request: ExitRequest, positions: Sequence[HeldPosition], *, mode_matches: bool
) -> RiskDecision:
    if not mode_matches:
        return _reject("mode mismatch between config and broker")
    position = next((p for p in positions if p.symbol == request.symbol), None)
    if position is None or position.is_dust:
        return _reject(f"no position to exit in {request.symbol}")
    return RiskDecision(approved=True, reasons=(f"exit: {request.reason}",), qty=position.qty)
```

- [ ] **Step 5: Run** `uv run pytest tests/unit/risk -q` — Expected: PASS.

- [ ] **Step 6: Full checks and commit** — `git add src/autotrader/risk tests/unit/risk` — message: `Risk: types, marketable entry pricing, checks 1-6 and reduce-only exit check`.

---

### Task 5: Checks 7–11 and sizing properties

**Files:**
- Modify: `src/autotrader/risk/checks.py` (`_size_and_finish`)
- Test: `tests/unit/risk/test_checks.py` (append)

**Interfaces:**
- Consumes: Task 4 helpers and fixtures. `PROFILE.learning.min_trades_for_evidence` (20), `PROFILE.risk.default_win_rate_prior` (0.40).
- Produces: approved decisions carry `adjusted["expected_value"]`.

- [ ] **Step 1: Failing tests** (append)

```python
@pytest.mark.parametrize(
    ("kwargs", "reason"),
    [
        # 7 size: 1e-5 x 2.997 rounds down to 0
        ({"intent": {"size_fraction": D("0.00001")}}, "below broker minimum"),
        # 8 exposure: 300.01 + 2.997 x 100.1 = 600.0097 > 600
        ({"portfolio": {"positions": (held(ETH, value="300.01"),)}}, "exposure"),
        # 9 rate
        ({"portfolio": {"entries_last_hour": 6}}, "per hour"),
        ({"portfolio": {"entries_today": 30}}, "per day"),
        # 10 price sanity: |100.1 - 99| = 1.1 > 0.99
        ({"market": {"last_trade": D("99")}}, "deviates"),
        ({"intent": {"take_profit": D("100.10")}}, "take profit"),
        # 11 expectancy: tp too close
        ({"intent": {"take_profit": D("101")}}, "expectancy"),
        # 11 with history: 20 trades at 20 % -> p = 0.3 -> negative
        ({"portfolio": {"strategy_trades_n": 20, "strategy_win_rate": D("0.2")}}, "expectancy"),
    ],
)
def test_entry_rejections_checks_7_to_11(kwargs: dict[str, Any], reason: str) -> None:
    d = run(**kwargs)
    assert not d.approved
    assert reason in d.reasons[0]


@pytest.mark.parametrize(
    "kwargs",
    [
        {"portfolio": {"positions": (held(ETH, value="300"),)}},  # 599.9997 <= 600
        {"portfolio": {"entries_last_hour": 5, "entries_today": 29}},
        # 19 trades: still the 0.40 prior, p = min(0.6, 0.5) = 0.5 -> positive
        {"portfolio": {"strategy_trades_n": 19, "strategy_win_rate": D("0.2")}},
    ],
)
def test_entry_boundaries_pass_checks_7_to_11(kwargs: dict[str, Any]) -> None:
    assert run(**kwargs).approved


def test_sizing_caps_choose_the_smallest() -> None:
    assert run(portfolio={"cash": D("100")}).adjusted["bound_by"] == "cash"
    d = run(intent={"stop": D("94.1"), "take_profit": D("120")})  # d = 6: 10/6 = 1.666 < 2.997
    assert d.adjusted["bound_by"] == "risk" and d.qty == D("1.6666")


def test_expected_value_is_reported() -> None:
    # p = 0.5; per unit: 5.9 x 0.5 - 2.1 x 0.5 - 100.1 x 0.0025 - 106 x 0.0025 = 1.38475
    d = run()
    assert D(d.adjusted["expected_value"]) == D("2.997") * D("1.38475")
```

Hypothesis properties (append; add `from hypothesis import given, settings` and `from hypothesis import strategies as st`):

```python
money = st.decimals(min_value=D("10"), max_value=D("100000"), places=2)


@settings(max_examples=300)
@given(
    equity=money,
    cash_share=st.decimals(min_value=D("0"), max_value=D("1"), places=3),
    ask=st.decimals(min_value=D("1"), max_value=D("100000"), places=2),
    stop_atr=st.decimals(min_value=D("0.8"), max_value=D("3"), places=2),
    reward=st.decimals(min_value=D("0.5"), max_value=D("10"), places=2),
    fraction=st.decimals(min_value=D("0.01"), max_value=D("1"), places=2),
    confidence=st.decimals(min_value=D("0"), max_value=D("1"), places=2),
)
def test_sizing_invariants(
    equity: D, cash_share: D, ask: D, stop_atr: D, reward: D, fraction: D, confidence: D
) -> None:
    entry = entry_limit(ask, PROFILE.risk.entry_slippage_bps, RULES.price_increment)
    atr = ask / 50
    stop = (entry - stop_atr * atr).quantize(D("0.0001"))
    if stop <= 0:
        return
    distance = entry - stop
    tp = entry + reward * distance
    cash = equity * cash_share
    d = run(
        intent={"stop": stop, "take_profit": tp, "confidence": confidence,
                "size_fraction": fraction},
        portfolio={"equity": equity, "cash": cash, "day_start_equity": equity, "hwm": equity},
        market={"ask": ask, "last_trade": ask, "atr": atr},
    )
    if not d.approved:
        return
    assert d.qty is not None and d.order is not None
    qty, taker = d.qty, FEES.taker_bps / D(10_000)
    tiny = D("1e-18") * equity  # Decimal division rounds to 28 digits
    assert qty > 0 and qty % RULES.qty_increment == 0
    assert qty * distance <= PROFILE.risk.risk_per_trade_pct / 100 * equity + tiny
    assert qty * entry <= PROFILE.risk.max_position_pct / 100 * equity + tiny
    assert qty * entry * (1 + taker) <= cash + tiny
    assert stop < entry < tp
    assert D(d.adjusted["expected_value"]) > 0
```

- [ ] **Step 2: Run** `uv run pytest tests/unit/risk -q` — Expected: FAIL (checks 8–11 missing, no `expected_value`).

- [ ] **Step 3: Implement** — give `_size_and_finish` a `last_trade: Decimal` parameter and pass `market.last_trade` from `check_entry` (mypy has narrowed it to `Decimal` by the check-2 test, so no assert is needed). Then, after the minimum-size rejection and before building the order:

```python
    notional = qty * entry
    # 8. exposure (reject, never shrink)
    exposure = (
        sum((p.market_value for p in portfolio.positions), Decimal(0))
        + sum((o.notional for o in portfolio.open_entries), Decimal(0))
        + notional
    )
    if exposure > limits.max_gross_exposure_pct / _PCT * portfolio.equity:
        return _reject(f"exposure {exposure} > {limits.max_gross_exposure_pct} % of equity")
    # 9. order rate (entries only; exits never count)
    if portfolio.entries_last_hour >= limits.max_orders_per_hour:
        return _reject(f"{portfolio.entries_last_hour} entry orders per hour >= limit")
    if portfolio.entries_today >= limits.max_orders_per_day:
        return _reject(f"{portfolio.entries_today} entry orders per day >= limit")
    # 10. price sanity
    if abs(entry - last_trade) > limits.max_price_deviation_pct / _PCT * last_trade:
        return _reject(
            f"entry {entry} deviates > {limits.max_price_deviation_pct} % from {last_trade}"
        )
    if intent.take_profit <= entry:
        return _reject(f"take profit {intent.take_profit} not above entry {entry}")
    # 11. fee-adjusted expectancy (G3, D4)
    rate = portfolio.strategy_win_rate
    if rate is None or portfolio.strategy_trades_n < profile.learning.min_trades_for_evidence:
        rate = limits.default_win_rate_prior
    p = min(intent.confidence, rate + WIN_RATE_MARGIN)
    ev = (
        qty * (intent.take_profit - entry) * p
        - qty * distance * (1 - p)
        - notional * taker
        - qty * intent.take_profit * taker
    )
    if ev <= 0:
        return _reject(f"expectancy {ev} <= 0 after fees (p = {p})")
```

and add `"expected_value": str(ev)` to `adjusted`.

- [ ] **Step 4: Run** `uv run pytest tests/unit/risk -q --cov=autotrader.risk.checks --cov=autotrader.risk.types --cov-branch --cov-report=term-missing` — Expected: PASS, 100 % on both files.

- [ ] **Step 5: Full checks and commit** — message: `Risk: checks 7-11 (size, exposure, rate, price sanity, expectancy) with sizing properties`.

---

### Task 6: `RiskEngine.check()` and feed accessors

**Files:**
- Modify: `src/autotrader/core/feed.py` (add `quote`, `last_price`)
- Create: `src/autotrader/risk/engine.py`
- Test: `tests/unit/core/test_feed.py` (append), `tests/unit/risk/test_engine.py`

**Interfaces:**
- Consumes: Tasks 1–5; `ControlGate`, `PAUSED_KEY`, `PAUSED_UNTIL_KEY` (`core/kill.py`); `compute_features` (`core/indicators.py`); `day_start`, `next_day_start` (Task 3).
- Produces:
  - `MarketFeed.quote(symbol) -> Quote | None`; `MarketFeed.last_price(symbol) -> float | None` (newest close among pending and verified 1m bars).
  - `RiskMarketView` protocol (`history`, `quote`, `last_price`, `is_stale`, `spread_bps`).
  - `@dataclass(frozen=True) OrderResult(decision, order_row_id: int | None = None, status: str | None = None, broker_order_id: str | None = None)`.
  - `RiskEngine(broker, *, profile, strategy_id, mode, gate, market, sessions, trading_day_tz="UTC", clock=...)` with `async check(intent) -> OrderResult` and `async snapshot_equity() -> None`.

- [ ] **Step 1: Feed accessors — failing test** (append to `tests/unit/core/test_feed.py`)

```python
async def test_quote_and_last_price_include_pending_minutes(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus
) -> None:
    clock = Clock(T0 + 30 * M + GRACE)
    feed = make_feed(FakeSource([bar(i, 100 + i) for i in range(30)]), sessions, bus, clock)
    await feed.backfill()
    assert feed.last_price(BTC) == 129
    assert feed.quote(BTC) is None
    await feed.on_bar(bar(30, 150))  # live: pending, not in history yet
    q = Quote(symbol=BTC, ts=clock.now, bid=149.9, ask=150.1)
    await feed.on_quote(q)
    assert feed.last_price(BTC) == 150
    assert feed.quote(BTC) == q
    assert feed.last_price("ETH/USD") is None
```

Implementation in `MarketFeed` (queries section):

```python
    def quote(self, symbol: str) -> Quote | None:
        return self._quotes.get(symbol)

    def last_price(self, symbol: str) -> float | None:
        """Close of the newest 1m bar, pending (unverified) or verified: the freshest
        trade price the feed has. Used for marking and price sanity, not for signals."""
        candidates = list(self._pending.get(symbol, {}).values())
        history = self._history.get((symbol, "1m"))
        if history:
            candidates.append(history[-1])
        return max(candidates, key=lambda b: b.ts).close if candidates else None
```

Run `uv run pytest tests/unit/core/test_feed.py -q` (FAIL before, PASS after).

- [ ] **Step 2: Engine — failing tests** (`tests/unit/risk/test_engine.py`)

```python
"""RiskEngine on SimBroker, a migrated SQLite journal and the real ControlGate."""

from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal as D
from pathlib import Path

import pytest
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.broker.base import AssetRules, BrokerOrder, FeeSchedule, OrderAck, OrderRejected
from autotrader.broker.sim import SimBroker
from autotrader.core.config import StrategyProfile, load_profile
from autotrader.core.kill import PAUSED_KEY, PAUSED_UNTIL_KEY, ControlGate, ControlStatus
from autotrader.core.types import Bar, Quote
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade
from autotrader.risk.engine import RiskEngine
from autotrader.risk.types import EntryIntent

REPO = Path(__file__).resolve().parents[3]
PROFILE: StrategyProfile = load_profile(REPO / "strategies" / "fast_momentum_v1.yaml")
BTC = "BTC/USD"
NOW = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
RULES = AssetRules(min_qty=D("0.0001"), qty_increment=D("0.0001"), min_notional=D("1"),
                   price_increment=D("0.01"))
FEES = FeeSchedule(maker_bps=D("15"), taker_bps=D("25"))


def five(i: int, close: float = 100.0) -> Bar:
    # range 2 per bar -> ATR 2 (no gaps)
    return Bar(symbol=BTC, timeframe="5m", ts=NOW - (30 - i) * timedelta(minutes=5),
               open=close, high=close + 1, low=close - 1, close=close, volume=1.0)


@dataclass
class FakeView:
    bars: list[Bar] = field(default_factory=lambda: [five(i) for i in range(30)])
    last: float | None = 100.0
    q: Quote | None = field(default_factory=lambda: Quote(symbol=BTC, ts=NOW, bid=99.98, ask=100.0))
    stale: bool = False

    def history(self, symbol: str, timeframe: str) -> list[Bar]:
        return list(self.bars) if symbol == BTC and timeframe == "5m" else []

    def quote(self, symbol: str) -> Quote | None:
        return self.q if symbol == BTC else None

    def last_price(self, symbol: str) -> float | None:
        return self.last if symbol == BTC else None

    def is_stale(self, symbol: str) -> bool:
        return self.stale

    def spread_bps(self, symbol: str) -> float | None:
        return None if self.q is None else (self.q.ask - self.q.bid) / self.q.ask * 10_000


class Clock:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


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
def sim() -> SimBroker:
    s = SimBroker(cash=D("1000"), fees=FEES, slippage_bps=D("0"), asset_rules={BTC: RULES},
                  timeframe="5m")
    s.process_bar(five(29))
    return s


@pytest.fixture
async def cycle_id(sessions: async_sessionmaker[AsyncSession]) -> int:
    async with sessions() as s:
        strategy = await repo.register_strategy(s, PROFILE, "yaml")
        row = m.Cycle(strategy_id=strategy.id, symbol=BTC, rule_id="r", signal_features={},
                      state="RISK_CHECK", created_at=NOW)
        s.add(row)
        await s.commit()
        return row.id


def make_engine_(broker: object, sessions: async_sessionmaker[AsyncSession], gate: object,
                 view: FakeView | None = None, clock: Clock | None = None,
                 tz: str = "UTC") -> RiskEngine:
    return RiskEngine(broker, profile=PROFILE, strategy_id=1, mode="paper",  # type: ignore[arg-type]
                      gate=gate, market=view or FakeView(), sessions=sessions,  # type: ignore[arg-type]
                      trading_day_tz=tz, clock=clock or Clock(NOW))


def intent(cycle_id: int, **kw: object) -> EntryIntent:
    base = dict(cycle_id=cycle_id, symbol=BTC, stop=D("98"), take_profit=D("106"),
                confidence=D("0.6"), size_fraction=D("1"))
    return EntryIntent(**{**base, **kw})  # type: ignore[arg-type]


async def orders(sessions: async_sessionmaker[AsyncSession]) -> list[m.Order]:
    async with sessions() as s:
        return list(await s.scalars(select(m.Order).order_by(m.Order.id)))


async def decisions(sessions: async_sessionmaker[AsyncSession]) -> list[m.RiskDecision]:
    async with sessions() as s:
        return list(await s.scalars(select(m.RiskDecision).order_by(m.RiskDecision.id)))


@pytest.fixture
def gate(tmp_path: Path, sessions: async_sessionmaker[AsyncSession]) -> ControlGate:
    return ControlGate(tmp_path / "KILL", sessions)


async def test_approved_entry_is_persisted_and_submitted(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    result = await make_engine_(sim, sessions, gate).check(intent(cycle_id))
    assert result.decision.approved and result.status == "new"
    (row,) = await orders(sessions)
    assert (row.client_order_id, row.status, row.broker_order_id) == (f"at-c{cycle_id}-e", "new", "sim-1")
    assert (row.side, row.type, row.qty, row.limit_price) == ("buy", "limit", D("2.997"), D("100.10"))
    assert (row.stop_price, row.tp_price, row.mode) == (D("98"), D("106"), "paper")
    (dec,) = await decisions(sessions)
    assert (dec.kind, dec.approved, dec.computed_size, dec.cycle_id) == ("entry", True, D("2.997"), cycle_id)


async def test_first_check_snapshots_equity_as_baseline(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    await make_engine_(sim, sessions, gate).check(intent(cycle_id))
    async with sessions() as s:
        snap = await repo.latest_snapshot(s)
    assert snap is not None and (snap.equity, snap.hwm, snap.daily_pnl) == (D("1000"), D("1000"), D(0))


async def test_daily_loss_sets_paused_until_next_day_start_in_trading_tz(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    async with sessions() as s:  # baseline 1040 before the Berlin day start -> loss 40 >= 31.2
        await repo.insert_equity_snapshot(s, ts=NOW - timedelta(hours=14), equity=D("1040"),
                                          cash=D("1040"), exposure=D(0), hwm=D("1040"),
                                          daily_pnl=D(0))
    result = await make_engine_(sim, sessions, gate, tz="Europe/Berlin").check(intent(cycle_id))
    assert not result.decision.approved and "daily loss" in result.decision.reasons[0]
    async with sessions() as s:
        assert await repo.get_control(s, PAUSED_UNTIL_KEY) == "2026-10-09T22:00:00+00:00"
    assert await orders(sessions) == []


async def test_drawdown_sets_manual_pause(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    async with sessions() as s:
        await repo.insert_equity_snapshot(s, ts=NOW - timedelta(minutes=1), equity=D("1000"),
                                          cash=D("1000"), exposure=D(0), hwm=D("1200"),
                                          daily_pnl=D(0))
    result = await make_engine_(sim, sessions, gate).check(intent(cycle_id))
    assert "drawdown" in result.decision.reasons[0]
    async with sessions() as s:
        assert await repo.get_control(s, PAUSED_KEY) is True


class BrokenBroker(SimBroker):
    async def get_account(self):  # type: ignore[no-untyped-def]
        raise ConnectionError("broker down")


async def test_state_failure_rejects_fail_closed(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    broker = BrokenBroker(cash=D("1000"), fees=FEES, slippage_bps=D(0), asset_rules={BTC: RULES},
                          timeframe="5m")
    result = await make_engine_(broker, sessions, gate).check(intent(cycle_id))
    assert result.decision.reasons == ("state unavailable: ConnectionError",)
    (dec,) = await decisions(sessions)
    assert not dec.approved


class FlippingGate:
    """Allows the first status() call, then reports a kill."""

    def __init__(self) -> None:
        self.calls = 0

    async def status(self, now: datetime | None = None) -> ControlStatus:
        self.calls += 1
        if self.calls == 1:
            return ControlStatus(killed=False, reasons=())
        return ControlStatus(killed=True, reasons=("kill file present",))


async def test_kill_between_gathering_and_submit_never_reaches_broker(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], cycle_id: int
) -> None:
    result = await make_engine_(sim, sessions, FlippingGate()).check(intent(cycle_id))
    assert result.status == "not_submitted"
    (row,) = await orders(sessions)
    assert row.status == "not_submitted" and row.broker_order_id is None


class RejectingBroker(SimBroker):
    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        raise OrderRejected("insufficient balance")


class TimeoutBroker(SimBroker):
    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        raise TimeoutError


@pytest.mark.parametrize(("cls", "status"), [(RejectingBroker, "rejected"), (TimeoutBroker, "unknown")])
async def test_submit_failures(
    cls: type[SimBroker], status: str, sessions: async_sessionmaker[AsyncSession],
    gate: ControlGate, cycle_id: int,
) -> None:
    broker = cls(cash=D("1000"), fees=FEES, slippage_bps=D(0), asset_rules={BTC: RULES}, timeframe="5m")
    broker.process_bar(five(29))
    result = await make_engine_(broker, sessions, gate).check(intent(cycle_id))
    assert result.status == status
    assert (await orders(sessions))[0].status == status


async def test_unknown_order_counts_as_open_and_blocks_the_symbol(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    broker = TimeoutBroker(cash=D("1000"), fees=FEES, slippage_bps=D(0), asset_rules={BTC: RULES},
                           timeframe="5m")
    broker.process_bar(five(29))
    engine = make_engine_(broker, sessions, gate)
    await engine.check(intent(cycle_id))
    async with sessions() as s:
        strategy = await repo.register_strategy(s, PROFILE, "yaml")
        row = m.Cycle(strategy_id=strategy.id, symbol=BTC, rule_id="r", signal_features={},
                      state="RISK_CHECK", created_at=NOW)
        s.add(row)
        await s.commit()
        second = row.id
    result = await engine.check(intent(second))
    assert "already has a position or open entry" in result.decision.reasons[0]


async def test_acked_entry_is_refreshed_from_broker(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    engine = make_engine_(sim, sessions, gate)
    await engine.check(intent(cycle_id))
    sim.process_bar(five(30, close=101.5).model_copy(update={"low": 101.0, "high": 102.0}))  # no fill: canceled
    async with sessions() as s:
        strategy = await repo.register_strategy(s, PROFILE, "yaml")
        row = m.Cycle(strategy_id=strategy.id, symbol=BTC, rule_id="r", signal_features={},
                      state="RISK_CHECK", created_at=NOW)
        s.add(row)
        await s.commit()
        second = row.id
    result = await engine.check(intent(second))
    statuses = [o.status for o in await orders(sessions)]
    assert statuses[0] == "canceled"  # refreshed from the broker
    assert result.decision.approved, result.decision.reasons  # no longer blocks BTC
```

- [ ] **Step 3: Run** `uv run pytest tests/unit/risk/test_engine.py -q` — Expected: FAIL (`autotrader.risk.engine` missing).

- [ ] **Step 4: Implement `risk/engine.py`**

```python
"""Risk Engine (SPEC §0.1, §4.5; spec 2026-10-11 §6). The only holder of the `Broker`.

`check()` gathers state under one lock, runs the pure `check_entry`, persists the
decision, applies a pause side effect on rejection, writes the order row, asks the
control gate again right before submitting, then submits. `exit()` (Task 7) is
reduce-only and ignores kill/pause. Any failure while gathering rejects (fail closed).
"""

import asyncio
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Protocol

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.broker.base import Broker, BrokerOrder, Mode, OrderAck, OrderRejected
from autotrader.core.config import StrategyProfile
from autotrader.core.indicators import compute_features
from autotrader.core.kill import PAUSED_KEY, PAUSED_UNTIL_KEY, ControlStatus
from autotrader.core.tradingday import day_start, next_day_start
from autotrader.core.types import Bar, Quote
from autotrader.journal import repo
from autotrader.risk.checks import check_entry
from autotrader.risk.types import (
    EntryIntent, HeldPosition, MarketSnapshot, OpenEntry, Pause, PortfolioState, RiskDecision,
)

log = structlog.get_logger(__name__)
HOUR = timedelta(hours=1)


class RiskMarketView(Protocol):
    def history(self, symbol: str, timeframe: str) -> list[Bar]: ...
    def quote(self, symbol: str) -> Quote | None: ...
    def last_price(self, symbol: str) -> float | None: ...
    def is_stale(self, symbol: str) -> bool: ...
    def spread_bps(self, symbol: str) -> float | None: ...


class Gate(Protocol):
    async def status(self, now: datetime | None = None) -> ControlStatus: ...


@dataclass(frozen=True)
class OrderResult:
    decision: RiskDecision
    order_row_id: int | None = None
    status: str | None = None
    broker_order_id: str | None = None


def _dec(value: float | None) -> Decimal | None:
    return None if value is None or not math.isfinite(value) else Decimal(str(value))


class RiskEngine:
    def __init__(
        self,
        broker: Broker,
        *,
        profile: StrategyProfile,
        strategy_id: int,
        mode: Mode,
        gate: Gate,
        market: RiskMarketView,
        sessions: async_sessionmaker[AsyncSession],
        trading_day_tz: str = "UTC",
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._broker = broker
        self._profile = profile
        self._strategy_id = strategy_id
        self._mode = mode
        self._gate = gate
        self._market = market
        self._sessions = sessions
        self._tz = trading_day_tz
        self._clock = clock
        self._lock = asyncio.Lock()

    # --- entries ---------------------------------------------------------------------

    async def check(self, intent: EntryIntent) -> OrderResult:
        async with self._lock:
            now = self._clock()
            try:
                control, portfolio, market = await self._gather_entry(intent.symbol, now)
            except Exception as e:  # fail closed: unknown state must not allow an entry
                log.exception("risk.state_unavailable", symbol=intent.symbol)
                decision = RiskDecision(
                    approved=False, reasons=(f"state unavailable: {type(e).__name__}",)
                )
                await self._persist(intent.cycle_id, "entry", decision, now)
                return OrderResult(decision)
            decision = check_entry(
                intent, portfolio, market, profile=self._profile, control=control,
                mode_matches=self._mode == self._broker.mode(),
            )
            await self._persist(intent.cycle_id, "entry", decision, now)
            if not decision.approved or decision.order is None:
                await self._apply_pause(decision.pause, now)
                log.info("risk.entry_rejected", symbol=intent.symbol, reason=decision.reasons[0])
                return OrderResult(decision)
            order = decision.order
            async with self._sessions() as session:
                row = await repo.insert_order(
                    session, cycle_id=intent.cycle_id, client_order_id=order.client_order_id,
                    symbol=intent.symbol, mode=self._mode, type_=order.type, side=order.side, qty=order.qty,
                    limit_price=order.limit_price, stop_price=intent.stop,
                    tp_price=intent.take_profit, status="pending", now=now,
                )
            final = await self._gate.status(self._clock())
            if not final.entries_allowed:
                await self._set_status(row.id, "not_submitted")
                log.warning("risk.entry_not_submitted", reasons=list(final.reasons))
                return OrderResult(decision, row.id, "not_submitted")
            return await self._submit(row.id, decision, lambda: self._broker.submit_order(order))

    async def _gather_entry(
        self, symbol: str, now: datetime
    ) -> tuple[ControlStatus, PortfolioState, MarketSnapshot]:
        control = await self._gate.status(now)
        account = await self._broker.get_account()
        held = await self._held_positions()
        rules = await self._broker.asset_rules(symbol)
        fees = self._broker.fee_schedule()
        async with self._sessions() as session:
            open_rows = await self._refresh_open_entries(session)
            entries_hour = await repo.count_entry_orders(session, now - HOUR, inclusive=False)
            entries_day = await repo.count_entry_orders(
                session, day_start(now, self._tz), inclusive=True
            )
            baseline = await repo.day_start_equity(session, day_start(now, self._tz))
            latest = await repo.latest_snapshot(session)
            trades_n, wins = await repo.strategy_trade_stats(session, self._strategy_id)
        if latest is None:  # first check ever: this snapshot is the baseline
            await self._write_snapshot(now, account.equity, account.cash, held)
            baseline = account.equity
        hwm = max(latest.hwm if latest else account.equity, account.equity)
        portfolio = PortfolioState(
            equity=account.equity,
            cash=account.cash,
            day_start_equity=baseline if baseline is not None else account.equity,
            hwm=hwm,
            positions=held,
            open_entries=tuple(
                # symbol is always set by insert_order; "" only guards legacy NULL rows and
                # still counts toward max_positions (conservative)
                OpenEntry(symbol=o.symbol or "", notional=o.qty * (o.limit_price or Decimal(0)))
                for o in open_rows
            ),
            entries_last_hour=entries_hour,
            entries_today=entries_day,
            strategy_trades_n=trades_n,
            strategy_win_rate=Decimal(wins) / Decimal(trades_n) if trades_n else None,
        )
        quote = self._market.quote(symbol)
        features = compute_features(
            self._market.history(symbol, self._profile.timeframes.signal),
            trading_day_tz=self._tz,
        )
        atr = features["atr14"][-1] if features["atr14"] else None
        market = MarketSnapshot(
            last_trade=_dec(self._market.last_price(symbol)),
            bid=_dec(quote.bid if quote else None),
            ask=_dec(quote.ask if quote else None),
            spread_bps=_dec(self._market.spread_bps(symbol)),
            atr=_dec(atr),
            stale=self._market.is_stale(symbol),
            rules=rules,
            fees=fees,
        )
        return control, portfolio, market
```

Remaining engine methods:

```python
    async def _refresh_open_entries(self, session: AsyncSession) -> list[Order]:
        """Order status sync for our own acked entries (WP 1.6 adds the fill stream):
        without it an IOC entry would stay `new` and block its symbol forever."""
        rows = await repo.open_entry_orders(session)
        still_open = []
        for row in rows:
            if row.status == "new" and row.broker_order_id is not None:
                status = await self._broker.get_order(row.broker_order_id)
                if status.status != "new":
                    await repo.update_order(session, row.id, status=status.status,
                                            now=self._clock())
                    continue
            still_open.append(row)
        return still_open

    async def _held_positions(self) -> tuple[HeldPosition, ...]:
        held = []
        for p in await self._broker.get_positions():
            price = _dec(self._market.last_price(p.symbol)) or p.avg_entry_price
            rules = await self._broker.asset_rules(p.symbol)
            held.append(HeldPosition(symbol=p.symbol, qty=p.qty, market_value=p.qty * price,
                                     min_qty=rules.min_qty))
        return tuple(held)

    async def _submit(
        self, row_id: int, decision: RiskDecision, call: Callable[[], Awaitable[OrderAck]]
    ) -> OrderResult:
        try:
            ack = await call()
        except OrderRejected as e:
            log.warning("risk.order_rejected", order_row=row_id, reason=str(e))
            await self._set_status(row_id, "rejected")
            return OrderResult(decision, row_id, "rejected")
        except Exception:  # timeout / network: may or may not exist at the broker
            log.exception("risk.order_unknown", order_row=row_id)
            await self._set_status(row_id, "unknown")
            return OrderResult(decision, row_id, "unknown")
        async with self._sessions() as session:
            await repo.update_order(session, row_id, status=ack.status, now=self._clock(),
                                    broker_order_id=ack.order_id)
        log.info("risk.order_submitted", order_row=row_id, broker_order_id=ack.order_id)
        return OrderResult(decision, row_id, ack.status, ack.order_id)

    async def _set_status(self, row_id: int, status: str) -> None:
        async with self._sessions() as session:
            await repo.update_order(session, row_id, status=status, now=self._clock())

    async def _persist(
        self, cycle_id: int | None, kind: str, decision: RiskDecision, now: datetime
    ) -> None:
        async with self._sessions() as session:
            await repo.record_risk_decision(
                session, cycle_id=cycle_id, kind=kind, approved=decision.approved,
                reasons=list(decision.reasons), adjusted=dict(decision.adjusted),
                computed_size=decision.qty, ts=now,
            )

    async def _apply_pause(self, pause: Pause | None, now: datetime) -> None:
        if pause is None:
            return
        async with self._sessions() as session:
            if pause == "until_next_day":
                until = next_day_start(now, self._tz)
                await repo.set_control(session, PAUSED_UNTIL_KEY, until.isoformat())
                log.warning("risk.paused_until", until=until.isoformat())
            else:
                await repo.set_control(session, PAUSED_KEY, True)
                log.warning("risk.paused_manual_resume")

    # --- equity snapshots ------------------------------------------------------------

    async def snapshot_equity(self) -> None:
        """Called every 60 s by `run core`. Lock-free on purpose: it must never delay
        an exit."""
        now = self._clock()
        account = await self._broker.get_account()
        await self._write_snapshot(now, account.equity, account.cash, await self._held_positions())

    async def _write_snapshot(
        self, now: datetime, equity: Decimal, cash: Decimal, held: tuple[HeldPosition, ...]
    ) -> None:
        async with self._sessions() as session:
            latest = await repo.latest_snapshot(session)
            baseline = await repo.day_start_equity(session, day_start(now, self._tz))
            hwm = max(latest.hwm if latest else equity, equity)
            await repo.insert_equity_snapshot(
                session, ts=now, equity=equity, cash=cash,
                exposure=sum((p.market_value for p in held), Decimal(0)), hwm=hwm,
                daily_pnl=equity - (baseline if baseline is not None else equity),
            )
```

(Add `from autotrader.journal.models import Order` to the engine imports.)

- [ ] **Step 5: Run** `uv run pytest tests/unit/risk tests/unit/core/test_feed.py tests/unit/journal -q` — Expected: PASS.

- [ ] **Step 6: Full checks and commit** — message: `Risk: RiskEngine.check() with fail-closed gathering, pre-submit gate check and order tracking`.

---

### Task 7: `RiskEngine.exit()` and equity snapshot tests

**Files:**
- Modify: `src/autotrader/risk/engine.py`
- Test: `tests/unit/risk/test_engine.py` (append)

**Interfaces:**
- Consumes: Task 6 internals (`_held_positions`, `_persist`, `_submit`).
- Produces: `async RiskEngine.exit(request: ExitRequest) -> OrderResult`.

- [ ] **Step 1: Failing tests** (append; add `ExitRequest` to the `autotrader.risk.types` import and `write_kill_file` to the `autotrader.core.kill` import)

```python
async def _held_btc(sim: SimBroker) -> None:
    await sim.submit_order(BrokerOrder(symbol=BTC, side="buy", qty=D("1"), type="market"))
    sim.process_bar(five(30))


async def test_exit_goes_through_while_killed(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate, tmp_path: Path
) -> None:
    await _held_btc(sim)
    write_kill_file(tmp_path / "KILL", source="cli", reason="test", now=NOW)
    result = await make_engine_(sim, sessions, gate).exit(ExitRequest(symbol=BTC, reason="kill"))
    assert result.decision.approved and result.status == "new"
    (row,) = await orders(sessions)
    assert (row.side, row.type, row.qty, row.client_order_id) == ("sell", "market", D("1"), None)
    (dec,) = await decisions(sessions)
    assert (dec.kind, dec.cycle_id) == ("exit", None)


async def test_exit_without_position_is_rejected_and_recorded(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    result = await make_engine_(sim, sessions, gate).exit(ExitRequest(symbol=BTC, reason="stop"))
    assert not result.decision.approved
    assert await orders(sessions) == []
    (dec,) = await decisions(sessions)
    assert not dec.approved


async def test_exit_state_failure_rejects(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    class NoPositions(SimBroker):
        async def get_positions(self):  # type: ignore[no-untyped-def]
            raise ConnectionError

    broker = NoPositions(cash=D("1000"), fees=FEES, slippage_bps=D(0), asset_rules={BTC: RULES},
                         timeframe="5m")
    result = await make_engine_(broker, sessions, gate).exit(ExitRequest(symbol=BTC, reason="stop"))
    assert result.decision.reasons == ("state unavailable: ConnectionError",)


async def test_snapshot_tracks_hwm_and_daily_pnl(
    sim: SimBroker, sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    clock = Clock(NOW)
    engine = make_engine_(sim, sessions, gate, clock=clock)
    await engine.snapshot_equity()  # baseline 1000
    await _held_btc(sim)  # 1 BTC @ 100 + fee
    sim.process_bar(five(31, close=120.0))  # marked up
    clock.now = NOW + timedelta(minutes=1)
    await engine.snapshot_equity()
    sim.process_bar(five(32, close=90.0))
    clock.now = NOW + timedelta(minutes=2)
    await engine.snapshot_equity()
    async with sessions() as s:
        rows = list(await s.scalars(select(m.EquitySnapshot).order_by(m.EquitySnapshot.ts)))
    assert [r.hwm for r in rows] == [D("1000"), rows[1].equity, rows[1].equity]
    assert rows[2].daily_pnl == rows[2].equity - D("1000")
    assert rows[2].exposure == D("100.0")  # marked at the feed view's last price (100)
```

- [ ] **Step 2: Run** — Expected: FAIL (`exit` missing).

- [ ] **Step 3: Implement** (in `RiskEngine`; import `check_exit`, `ExitRequest`)

```python
    # --- exits -----------------------------------------------------------------------

    async def exit(self, request: ExitRequest) -> OrderResult:
        """Reduce-only full exit (G1). No gate check: kill and pause never block it."""
        async with self._lock:
            now = self._clock()
            try:
                held = await self._held_positions()
            except Exception as e:
                log.exception("risk.exit_state_unavailable", symbol=request.symbol)
                decision = RiskDecision(
                    approved=False, reasons=(f"state unavailable: {type(e).__name__}",)
                )
                await self._persist(request.cycle_id, "exit", decision, now)
                return OrderResult(decision)
            decision = check_exit(request, held, mode_matches=self._mode == self._broker.mode())
            await self._persist(request.cycle_id, "exit", decision, now)
            if not decision.approved or decision.qty is None:
                log.warning("risk.exit_rejected", symbol=request.symbol, reason=decision.reasons[0])
                return OrderResult(decision)
            async with self._sessions() as session:
                row = await repo.insert_order(
                    session, cycle_id=request.cycle_id, client_order_id=None,
                    symbol=request.symbol, mode=self._mode, type_="market", side="sell", qty=decision.qty,
                    limit_price=None, stop_price=None, tp_price=None, status="pending", now=now,
                )
            return await self._submit(
                row.id, decision, lambda: self._broker.close_position(request.symbol)
            )
```

- [ ] **Step 4: Run with coverage** `uv run pytest tests/unit/risk -q --cov=autotrader.risk --cov-branch --cov-report=term-missing` — Expected: PASS, 100 %. If a branch is missing (e.g. `decision.order is None` with `approved=True`, which `check_entry` never produces), restructure instead of adding `# pragma`: check `decision.order is None` as the only condition (`approved` implies an order for entries) and `decision.qty is None` for exits.

- [ ] **Step 5: Full checks and commit** — message: `Risk: reduce-only RiskEngine.exit() that kill and pause never block`.

---

### Task 8: Wiring, CI gate, docs

**Files:**
- Modify: `src/autotrader/core/runtime.py`, `src/autotrader/cli.py` (help text)
- Modify: `tests/unit/core/test_runtime.py`
- Modify: `.github/workflows/ci.yml`
- Modify: `PLAN.md`, `docs/alpaca-notes.md`, `docs/superpowers/specs/2026-10-11-risk-engine-design.md`

**Interfaces:**
- Consumes: `RiskEngine` (Tasks 6–7). `run_core(..., snapshot_s: float = 60.0)`.

- [ ] **Step 1: Failing runtime test** — in `tests/unit/core/test_runtime.py`, give `FakeBroker` the methods the engine uses:

```python
    async def get_positions(self) -> list[Any]:
        return []
```

and in `test_run_core_persists_bars_and_responds_to_kill_file` pass `snapshot_s=0.01` to `run_core`, then assert after shutdown:

```python
        assert await s.scalar(select(func.count()).select_from(m.EquitySnapshot)) >= 1
```

Run: FAIL (`snapshot_s` unknown).

- [ ] **Step 2: Implement wiring** in `runtime.py`:
  - `_check_broker` returns the broker (`-> Any`); keep its log line.
  - After the scanner subscription: `risk = RiskEngine(broker, profile=profile, strategy_id=strategy.id, mode=cfg.mode, gate=gate, market=feed, sessions=sessions, trading_day_tz=cfg.trading_day_tz)` and `log.info("risk.engine_ready", mode=cfg.mode)`.
  - In the loop: `if loop.time() - last_snapshot >= snapshot_s:` call `await risk.snapshot_equity()` inside `try/except Exception: log.exception("risk.snapshot_failed")`, then update `last_snapshot = loop.time()` (initialise `last_snapshot = -math.inf` so the first tick snapshots).
  - Docstring: replace "nothing here keeps a reference (... WP 1.3 hands it over)" with: "The broker is connected here and handed to the `RiskEngine`, which is its only holder from then on (SPEC §0.1); `run core` also snapshots equity every 60 s for the daily-loss and drawdown checks. Nothing calls `check()`/`exit()` until the Cycle Runner (WP 1.4) and Position Manager (WP 1.5)."
  - `cli.py` `run_core` help: `"""Start the trading core: market feed, scanner, risk engine (equity snapshots)."""`

Run `uv run pytest tests/unit/core/test_runtime.py -q` — PASS.

- [ ] **Step 3: CI gate** — in `.github/workflows/ci.yml` after `uv run pytest --cov`:

```yaml
      # PLAN WP 1.3: the Risk Engine needs 100 % branch coverage.
      - run: uv run coverage report --include='src/autotrader/risk/*' --fail-under=100 --show-missing
```

Verify locally: `uv run pytest --cov -q && uv run coverage report --include='src/autotrader/risk/*' --fail-under=100 --show-missing; echo $?` → `0`.

- [ ] **Step 4: Docs**
  - `PLAN.md`: add **D14** after D13: "Risk Engine (WP 1.3) — **2026-10-11, owner:** equity snapshots every 60 s (start-of-day = last snapshot at/before the trading-day start, HWM = running max); entries are marketable IOC limits at ask + `risk.entry_slippage_bps` (10), rounded up to the price step, taker fee in expectancy; cash cap added to G4 sizing (crypto not marginable); exposure over the limit rejects, never shrinks; exits are full only via `close_position`; ambiguous submit failures are `unknown` and count as open until WP 1.5 reconciles them; the engine refreshes its own acked entry orders from the broker. Spec: `docs/superpowers/specs/2026-10-11-risk-engine-design.md`." Amend G4's resolution: append "**1.3:** plus a cash cap `cash / (entry × (1 + taker))` (D14)." Mark WP 1.3 **Done 2026-10-11** and update the repo-state line ("Risk Engine done (WP 1.3, `risk/`), sole broker holder in `run core`, equity snapshots every 60 s. Next: WP 1.4 (Cycle Runner)").
  - `docs/alpaca-notes.md`: replace "`price_increment` isn't in `AssetRules` yet. Add it when limit-price rounding is needed (Phase 1)." with "`price_increment` is mapped into `AssetRules.price_increment` (WP 1.3) and used to round the marketable entry limit up."
  - Spec: apply the two amendments listed at the top of this plan (§6 gather step and exit client id) and add the `orders.symbol` column to §7.
  - Search for now-false claims: `rg -n -i "nothing here keeps|hands it over|read-only AlpacaBroker|only the Risk Engine" --glob '!*.lock'` and fix every hit that the wiring falsified.

- [ ] **Step 5: Full checks and commit**

```bash
uv run pytest --cov -q && uv run coverage report --include='src/autotrader/risk/*' --fail-under=100 \
  && uv run ruff check && uv run ruff format --check && uv run mypy
git add src/autotrader/core/runtime.py src/autotrader/cli.py tests/unit/core/test_runtime.py \
  .github/workflows/ci.yml PLAN.md docs
git commit -m "WP 1.3: wire RiskEngine into run core, 60 s equity snapshots, risk coverage gate (D14)"
```

Then push and open the PR (`gh pr create`), body ending with the Claude Code line.
