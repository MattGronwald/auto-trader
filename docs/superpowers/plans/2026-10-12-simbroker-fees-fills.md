# SimBroker Fees and Fills Implementation Plan (WP 1.R2)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `SimBroker` model Alpaca's coin-denominated buy fee, deterministic scripted partial fills and stable fill identities, so WP 1.5 can be built and tested against it.

**Architecture:** `broker/base.py` gains `Fill.fill_id`, `Fill.fee_asset` and the non-final `OrderState` `"partially_filled"`. `SimBroker` is refactored so every fill is a *slice* of an order. An order fills completely in one slice by default, and a test-queued `FillPlan` can split, cancel or reject it bar by bar. Coin fees, dust handling and reservations all work per slice. The Risk Engine and journal learn that `partially_filled` is an open entry state.

**Tech Stack:** Python 3.12, pydantic v2, `Decimal`, pytest + hypothesis, SQLAlchemy 2 async (journal), uv.

**Spec:** `docs/superpowers/specs/2026-10-12-simbroker-fees-fills-design.md` (approved 2026-10-12 after two Codex review rounds on PR #21). Read it before starting; this plan implements it section by section.

## Global Constraints

- Branch: `wp-1.R2-simbroker` (already exists, contains the spec). Work on it; do not create another branch.
- Every commit message ends with exactly this trailer line (after a blank line):
  `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`
- Money and qty are `Decimal` everywhere outside indicator math; floats are refused by broker models.
- `coin_precision` default: `Decimal("1e-9")`.
- Coin fee: `ceil(q·r, coin_precision)`. Cash: exact, never rounded to cents.
- `avg_entry_price` of a position is the fill price `p`, weighted by net coins across buys.
- `close_position` sells the exact held qty minus pending sell remainders, even off-increment. A sellable remainder below `min_qty` raises `OrderRejected("dust below minimum …")`.
- Rejection reasons, verbatim: `"insufficient cash at fill"`, `"scripted rejection"`, `"fill too small for coin fee"`.
- `fill_id` format: `"{order_id}:{n}"`, with `n = 1, 2, …` per order.
- Unscripted orders must behave exactly as before. All existing tests in `tests/unit/broker/test_sim.py` stay green unchanged, except the one test Task 4 deletes on purpose.
- Commands, all exit 0 before every commit:
  - `uv run pytest -q`
  - `uv run ruff check`
  - `uv run ruff format --check`
  - `uv run mypy`
- `src/autotrader/risk/` keeps 100 % branch coverage:
  `uv run pytest -q --cov=autotrader.risk --cov-branch --cov-report=term-missing`.
- Never pipe a check into `tail`. Redirect to a file and read the real exit code.
- Line length 100 (ruff). mypy is strict.
- **ASCII only in Python code, comments and docstrings.** ruff's `RUF001`–`RUF003` reject `−`, `×` and similar characters (this plan's prose may use them; its code blocks don't). Docstrings are not wrapped by `ruff format`; wrap them by hand.
- Tests never hit Alpaca or Anthropic.

## Review Focus

Spec-implied conditions no task would otherwise exercise. Each has a test in the owning task:

1. **IOC + scripted `"cancel"` step on its only bar.** Expected: the order ends `canceled` with `filled_qty == 0` and is not counted twice. Owner: Task 3 (`test_ioc_scripted_cancel_on_its_bar`).
2. **A `FillPlan` with an invalid step** (0, > 1, a negative fraction, an unknown string). Expected: `ValueError` when the plan is built, not a silent no-op at fill time. Owner: Task 3 (`test_fill_plan_rejects_invalid_steps`).
3. **`cancel_order` on a `partially_filled` order.** Expected: it ends `canceled`, `filled_qty` is kept, and the remainder's reservation is released. Owner: Task 3 (`test_partial_buy_cancel_releases_cash`).
4. **A coin-fee buy into an existing position.** Expected: `avg_entry_price` is weighted by net coins at fill prices, and qty adds the net receipts. Owner: Task 4 (`test_base_buy_adds_to_existing_position`).
5. **`close_position` while a partial sell is still open.** Expected: it is never oversold; it raises "fully reserved" until the partial sell is canceled. Owner: Task 3 (`test_partial_sell_blocks_close_until_canceled`).

---

## File Structure

| File | Responsibility | Tasks |
|---|---|---|
| `src/autotrader/broker/base.py` | Shared domain types: `Fill` (+`fill_id`, `fee_asset`), `OrderState` (+`partially_filled`), comments | 1, 7 |
| `src/autotrader/broker/sim.py` | SimBroker: slices, scripts, coin fees, dust, reservations | 1, 3, 4, 5, 7 |
| `src/autotrader/journal/repo.py` | `OPEN_ENTRY_STATUSES` | 2 |
| `src/autotrader/risk/engine.py` | `_refresh_open_entries` keeps `partially_filled` open | 2 |
| `tests/unit/broker/test_sim.py` | All SimBroker tests | 1, 3, 4, 5, 6 |
| `tests/unit/journal/test_repo.py` | `partially_filled` is an open entry | 2 |
| `tests/unit/risk/test_engine.py` | Engine keeps and records `partially_filled` | 2 |
| `SPEC.md`, `PLAN.md`, `docs/alpaca-notes.md` | Docs alignment | 7 |

---

### Task 1: Fill identity and fee asset on `Fill`; `partially_filled` state

**Files:**
- Modify: `src/autotrader/broker/base.py` (lines 15-20 `OrderState`, lines 71-80 `Fill`)
- Modify: `src/autotrader/broker/sim.py` (`_Order`, `_try_fill`, `__init__`)
- Test: `tests/unit/broker/test_sim.py`

**Interfaces:**
- Produces: `OrderState = Literal["new", "partially_filled", "filled", "canceled", "rejected"]`; `Fill(fill_id: str, order_id: str, symbol: str, side: Side, qty: Dec, price: Dec, fee: Dec, fee_asset: Literal["base", "quote"], ts: AwareDatetime)`; `SimBroker.redeliver(fill_id: str) -> None` (raises `KeyError` for unknown ids).

- [ ] **Step 1: Write the failing tests** (append to `tests/unit/broker/test_sim.py`, after `test_fills_are_streamed_in_order`)

```python
async def test_fills_carry_stable_unique_ids_and_fee_asset() -> None:
    sim = make_sim(cash="10000")
    sim.process_bar(bar(0, 100.0))
    a = await sim.submit_order(market("buy", "1"))
    b = await sim.submit_order(market("buy", "2"))
    sim.process_bar(bar(1, 100.0))

    stream = sim.stream_fills()
    fills = [await asyncio.wait_for(anext(stream), timeout=1) for _ in range(2)]

    assert [f.fill_id for f in fills] == [f"{a.order_id}:1", f"{b.order_id}:1"]
    assert all(f.fee_asset == "quote" for f in fills)


async def test_redeliver_repeats_the_identical_fill() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    await sim.submit_order(market("buy", "1"))
    sim.process_bar(bar(1, 100.0))
    stream = sim.stream_fills()
    first = await asyncio.wait_for(anext(stream), timeout=1)

    sim.redeliver(first.fill_id)

    assert await asyncio.wait_for(anext(stream), timeout=1) == first
    with pytest.raises(KeyError):
        sim.redeliver("sim-999:1")
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/broker/test_sim.py -k "stable_unique_ids or redeliver" -q`
Expected: FAIL. `Fill` has no attribute `fill_id`; `SimBroker` has no attribute `redeliver`.

- [ ] **Step 3: Implement the types**

In `src/autotrader/broker/base.py` replace the `OrderState` line with:

```python
# "partially_filled": some qty filled, the rest still open (not final). A partly filled
# order can end canceled or rejected with filled_qty > 0 (PLAN §3.2 item 3).
OrderState = Literal["new", "partially_filled", "filled", "canceled", "rejected"]
```

Replace the whole `Fill` class with:

```python
class Fill(_Frozen):
    """One broker execution. `fill_id` is unique and stable per execution, so a consumer
    applies each fill once (WP 1.5). `qty` is the gross qty traded at `price`; `fee` is
    denominated in `fee_asset`: a buy with `FeeSchedule.buy_fee_asset == "base"` pays
    it in the coin (net coins received = qty - fee), every sell and every "quote" buy
    pays it in USD (G10, D10, docs/alpaca-notes.md)."""

    fill_id: str
    order_id: str
    symbol: str
    side: Side
    qty: Dec
    price: Dec
    fee: Dec
    fee_asset: Literal["base", "quote"]
    ts: AwareDatetime
```

- [ ] **Step 4: Emit the new fields from SimBroker**

In `src/autotrader/broker/sim.py`:

1. Add `fills: int = 0` to the `_Order` dataclass (after `avg_fill_price`).
2. In `__init__`, after `self._fills: asyncio.Queue[Fill] = asyncio.Queue()`, add:
   ```python
   self._fill_log: dict[str, Fill] = {}  # every emitted fill by id, for redeliver()
   ```
3. In `_try_fill`, replace the `self._fills.put_nowait(Fill(...))` block with:
   ```python
   o.fills += 1
   fill = Fill(
       fill_id=f"{o.id}:{o.fills}",
       order_id=o.id,
       symbol=order.symbol,
       side=order.side,
       qty=order.qty,
       price=price,
       fee=fee,
       fee_asset="quote",
       ts=bar.ts,
   )
   self._fill_log[fill.fill_id] = fill
   self._fills.put_nowait(fill)
   ```
4. After `stream_fills`, add:
   ```python
   def redeliver(self, fill_id: str) -> None:
       """Put an already emitted fill on the stream again (same `fill_id`), for duplicate
       and out-of-order delivery tests. It never creates a new execution. Unknown ids
       raise KeyError."""
       self._fills.put_nowait(self._fill_log[fill_id])
   ```

- [ ] **Step 5: Run the tests**

Run: `uv run pytest -q` (redirect to a file, check the exit code).
Expected: PASS, all tests. If a test elsewhere constructs `Fill(...)` directly, add `fill_id=` and `fee_asset="quote"` to it. Check first with `rg -n "Fill\(" src tests`.

- [ ] **Step 6: Lint, type-check, commit**

```bash
uv run ruff format && uv run ruff check && uv run mypy
git add src/autotrader/broker/base.py src/autotrader/broker/sim.py tests/unit/broker/test_sim.py
git commit -m "WP 1.R2: fill ids, fee asset and partially_filled state

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: `partially_filled` is an open entry state (journal + Risk Engine)

**Files:**
- Modify: `src/autotrader/journal/repo.py:299-301`
- Modify: `src/autotrader/risk/engine.py:241-260` (`_refresh_open_entries`)
- Test: `tests/unit/journal/test_repo.py:285-292`, `tests/unit/risk/test_engine.py` (after `test_own_orders_are_refreshed_before_positions_are_read`)

**Interfaces:**
- Consumes: `OrderState` `"partially_filled"` (Task 1).
- Produces: `repo.OPEN_ENTRY_STATUSES == ("pending", "new", "partially_filled", "unknown")`.

- [ ] **Step 1: Write the failing tests**

In `tests/unit/journal/test_repo.py`, change the first line of `test_open_entry_orders_and_update` to:

```python
    keep = [await _order(session, status=s) for s in ("pending", "new", "partially_filled", "unknown")]
```

and its last assertion to:

```python
    assert {o.id for o in await repo.open_entry_orders(session)} == {
        keep[0].id,
        keep[2].id,
        keep[3].id,
    }
```

In `tests/unit/risk/test_engine.py`, add after `test_own_orders_are_refreshed_before_positions_are_read`:

```python
class PartialFillBroker(SimBroker):
    """Reports every order as partially filled (an IOC never rests there in SimBroker)."""

    async def get_order(self, order_id: str) -> OrderStatus:
        status = await super().get_order(order_id)
        return status.model_copy(update={"status": "partially_filled"})


async def test_partially_filled_entry_stays_open_and_is_recorded(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate, cycle_id: int
) -> None:
    broker = PartialFillBroker(
        cash=D("1000"), fees=FEES, slippage_bps=D(0), asset_rules={BTC: RULES}, timeframe="5m"
    )
    broker.process_bar(five(29))
    engine = make_engine_(broker, sessions, gate)
    await engine.check(intent(cycle_id))
    async with sessions() as s:
        strategy = await repo.register_strategy(s, PROFILE, "yaml")
        row = m.Cycle(
            strategy_id=strategy.id,
            symbol=BTC,
            rule_id="r",
            signal_features={},
            state="RISK_CHECK",
            created_at=NOW,
        )
        s.add(row)
        await s.commit()
        second = row.id

    for _ in range(2):  # first poll records the change, the second sees it unchanged
        result = await engine.check(intent(second))
        assert "already has a position or open entry" in result.decision.reasons[0]
        assert [o.status for o in await orders(sessions)] == ["partially_filled"]
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/journal/test_repo.py tests/unit/risk/test_engine.py -k "open_entry_orders or partially_filled" -q`
Expected: FAIL. The repo test misses the `partially_filled` row. The engine test sees status `["partially_filled"]` written but the order dropped from the open set, so the second check is approved or the status assertion fails.

- [ ] **Step 3: Implement**

In `src/autotrader/journal/repo.py` replace the constant and its comment:

```python
# Entry orders that may still fill: not yet acked, acked and not final (including partly
# filled with the rest still open), or unknown after an ambiguous submit failure (WP 1.5
# reconciles those by client_order_id). "partially_filled" fits orders.status String(16).
OPEN_ENTRY_STATUSES = ("pending", "new", "partially_filled", "unknown")
```

In `src/autotrader/risk/engine.py` add, near the other module constants (below `T = TypeVar("T")`):

```python
# Broker order states that are not final: the order can still fill (some of) its qty.
_BROKER_OPEN = ("new", "partially_filled")
```

and replace the body of `_refresh_open_entries` after the docstring with:

```python
        async with self._sessions() as session:
            rows = await repo.open_entry_orders(session)
        still_open: list[Order] = []
        changed: list[tuple[int, str]] = []
        for row in rows:
            if row.status in _BROKER_OPEN and row.broker_order_id is not None:
                get_order = partial(self._broker.get_order, row.broker_order_id)
                status = await self._within(deadline, get_order)
                if status.status != row.status:
                    changed.append((row.id, status.status))
                if status.status not in _BROKER_OPEN:
                    continue
            still_open.append(row)
        if changed:
            async with self._sessions() as session:
                for row_id, new_status in changed:
                    await repo.update_order(session, row_id, status=new_status, now=self._clock())
        return still_open
```

Replace the docstring with (wrapped by hand: `ruff format` does not wrap docstrings):

```python
        """Order status sync for our own acked entries (WP 1.6 adds the fill stream): a
        final broker status is written and the row leaves the open set; "partially_filled"
        is written but stays open. Without it an IOC entry would stay `new` and block its
        symbol forever. No DB session is held open across the broker calls."""
```

- [ ] **Step 4: Run the tests and coverage**

Run: `uv run pytest -q --cov=autotrader.risk --cov-branch --cov-report=term-missing` (redirect, check exit code).
Expected: PASS; `risk/` at 100 % branch coverage.

- [ ] **Step 5: Lint, type-check, commit**

```bash
uv run ruff format && uv run ruff check && uv run mypy
git add src/autotrader/journal/repo.py src/autotrader/risk/engine.py tests/unit/journal/test_repo.py tests/unit/risk/test_engine.py
git commit -m "WP 1.R2: partially_filled entries stay open in journal and Risk Engine

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Fill slices, scripted `FillPlan`s and remainder reservations

**Files:**
- Modify: `src/autotrader/broker/sim.py` (module docstring, `_Order`, `submit_order`, `cancel_order`, `get_order`, `process_bar`, `_try_fill` → `_on_bar`/`_price`/`_slice`/`_fill`, `_estimated_cost`, `_reserved_cash`, `_pending_qty`)
- Test: `tests/unit/broker/test_sim.py`

**Interfaces:**
- Consumes: `Fill.fill_id`, `_Order.fills`, `self._fill_log` (Task 1).
- Produces:
  - `FillStep = Decimal | Literal["cancel", "reject"]`
  - `FillPlan(steps: tuple[FillStep, ...])`: a frozen dataclass that validates each step.
  - `SimBroker.script(symbol: str, side: Side, plan: FillPlan) -> None`
  - private `SimBroker._fill(o, qty, price, fee_bps, ts) -> None`, which Task 4 extends for coin fees.
  - private `SimBroker._submit(order, *, full_close: bool) -> OrderAck`, which Task 5 uses.

- [ ] **Step 1: Write the failing tests** (append to `tests/unit/broker/test_sim.py`; add `FillPlan` to the `autotrader.broker.sim` import)

```python
# --- scripted fills (WP 1.R2) ---------------------------------------------------------

D = Decimal


async def drain(sim: SimBroker) -> list[Fill]:
    stream = sim.stream_fills()
    out: list[Fill] = []
    while True:
        try:
            out.append(await asyncio.wait_for(anext(stream), timeout=0.01))
        except TimeoutError:
            return out


def test_fill_plan_rejects_invalid_steps() -> None:
    for bad in (D(0), D("1.01"), D("-0.5"), "later"):
        with pytest.raises(ValueError, match="step"):
            FillPlan((bad,))  # type: ignore[arg-type]
    assert FillPlan((D("0.5"), D(1), "cancel", "reject")).steps[-1] == "reject"


async def test_ioc_partial_fill_cancels_the_rest() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    sim.script(BTC, "buy", FillPlan((D("0.4"),)))
    ack = await sim.submit_order(ioc_buy("1", "101"))
    sim.process_bar(bar(1, 100.0))

    status = await sim.get_order(ack.order_id)
    assert (status.status, status.filled_qty, status.avg_fill_price) == (
        "canceled",
        D("0.4"),
        D("100"),
    )
    (pos,) = await sim.get_positions()
    assert pos.qty == D("0.4")
    (fill,) = await drain(sim)
    assert (fill.fill_id, fill.order_id, fill.qty) == (f"{ack.order_id}:1", ack.order_id, D("0.4"))


async def test_ioc_scripted_cancel_on_its_bar() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    sim.script(BTC, "buy", FillPlan(("cancel",)))
    ack = await sim.submit_order(ioc_buy("1", "101"))
    sim.process_bar(bar(1, 100.0))

    status = await sim.get_order(ack.order_id)
    assert (status.status, status.filled_qty, status.avg_fill_price) == ("canceled", D(0), None)
    assert await drain(sim) == []
    assert sim.cash == D("1000")


async def test_multi_bar_market_sell_goes_partial_then_filled() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    await sim.submit_order(market("buy", "1"))
    sim.process_bar(bar(1, 100.0))
    await drain(sim)
    sim.script(BTC, "sell", FillPlan((D("0.5"), D("0.3"))))
    ack = await sim.submit_order(market("sell", "1"))

    states = []
    for i, price in ((2, 110.0), (3, 120.0), (4, 130.0)):
        sim.process_bar(bar(i, price))
        states.append((await sim.get_order(ack.order_id)).status)

    assert states == ["partially_filled", "partially_filled", "filled"]
    status = await sim.get_order(ack.order_id)
    assert status.filled_qty == D(1)
    assert status.avg_fill_price == D("117")  # 0.5*110 + 0.3*120 + 0.2*130
    fills = await drain(sim)
    assert [f.fill_id for f in fills] == [f"{ack.order_id}:{n}" for n in (1, 2, 3)]
    assert [f.qty for f in fills] == [D("0.5"), D("0.3"), D("0.2")]
    assert await sim.get_positions() == []


@pytest.mark.parametrize(
    ("last", "status", "reason"),
    [("reject", "rejected", "scripted rejection"), ("cancel", "canceled", None)],
)
async def test_scripted_end_keeps_earlier_fills(last: str, status: str, reason: str | None) -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    sim.script(BTC, "buy", FillPlan((D("0.5"), last)))  # type: ignore[arg-type]
    ack = await sim.submit_order(market("buy", "1"))
    sim.process_bar(bar(1, 100.0))
    sim.process_bar(bar(2, 100.0))
    sim.process_bar(bar(3, 100.0))  # nothing more happens after a final state

    result = await sim.get_order(ack.order_id)
    assert (result.status, result.filled_qty, result.reason) == (status, D("0.5"), reason)
    (pos,) = await sim.get_positions()
    assert pos.qty == D("0.5")


async def test_non_crossing_limit_bar_consumes_no_step() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    sim.script(BTC, "buy", FillPlan((D("0.5"),)))
    ack = await sim.submit_order(limit("buy", "1", "90"))

    sim.process_bar(bar(1, 100.0, low=95.0))  # does not trade through 90
    assert (await sim.get_order(ack.order_id)).status == "new"
    sim.process_bar(bar(2, 100.0, low=89.0))
    assert (await sim.get_order(ack.order_id)).filled_qty == D("0.5")
    sim.process_bar(bar(3, 100.0, low=89.0))
    assert (await sim.get_order(ack.order_id)).status == "filled"


async def test_scripts_match_fifo_per_symbol_and_side() -> None:
    sim = make_sim(cash="10000")
    sim.process_bar(bar(0, 100.0))
    sim.script(BTC, "buy", FillPlan((D("0.4"),)))
    sim.script(BTC, "buy", FillPlan((D("0.6"),)))
    sim.script(BTC, "sell", FillPlan(("reject",)))  # never consumed by a buy
    a = await sim.submit_order(ioc_buy("1", "101"))
    b = await sim.submit_order(ioc_buy("1", "101"))
    c = await sim.submit_order(ioc_buy("1", "101"))  # unscripted: fills fully
    sim.process_bar(bar(1, 100.0))

    filled = [(await sim.get_order(x.order_id)).filled_qty for x in (a, b, c)]
    assert filled == [D("0.4"), D("0.6"), D("1")]


async def test_partial_sell_blocks_close_until_canceled() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    await sim.submit_order(market("buy", "1"))
    sim.process_bar(bar(1, 100.0))
    sim.script(BTC, "sell", FillPlan((D("0.5"),)))
    sell = await sim.submit_order(limit("sell", "1", "100"))  # gtc: rest stays open
    sim.process_bar(bar(2, 100.0, h=101.0))
    assert (await sim.get_order(sell.order_id)).status == "partially_filled"

    with pytest.raises(OrderRejected, match="fully reserved"):
        await sim.close_position(BTC)  # held 0.5, open sell remainder 0.5
    await sim.cancel_order(sell.order_id)
    ack = await sim.close_position(BTC)
    sim.process_bar(bar(3, 100.0))

    assert (await sim.get_order(ack.order_id)).filled_qty == D("0.5")
    assert (await sim.get_order(sell.order_id)).filled_qty == D("0.5")
    assert await sim.get_positions() == []


async def test_partial_buy_cancel_releases_cash() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    sim.script(BTC, "buy", FillPlan((D("0.5"),)))
    first = await sim.submit_order(limit("buy", "8", "100"))  # gtc
    sim.process_bar(bar(1, 100.0))  # 4 filled: cash 1000 - 400 - 0.6 maker fee
    assert (await sim.get_order(first.order_id)).status == "partially_filled"

    with pytest.raises(OrderRejected, match="insufficient cash"):
        await sim.submit_order(limit("buy", "4", "100"))  # remainder 4 is still reserved
    await sim.cancel_order(first.order_id)
    await sim.submit_order(limit("buy", "4", "100"))

    status = await sim.get_order(first.order_id)
    assert (status.status, status.filled_qty) == ("canceled", D("4"))


async def test_later_slice_failing_the_cash_check_rejects_and_keeps_earlier_fills() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    sim.script(BTC, "buy", FillPlan((D("0.5"),)))
    ack = await sim.submit_order(market("buy", "9"))
    sim.process_bar(bar(1, 100.0))  # 4.5 filled for 451.125
    sim.process_bar(bar(2, 200.0))  # remainder 4.5 at 200 needs 902.25 > 548.875

    status = await sim.get_order(ack.order_id)
    assert (status.status, status.filled_qty, status.reason) == (
        "rejected",
        D("4.5"),
        "insufficient cash at fill",
    )
    assert sim.cash == D("1000") - D("450") * D("1.0025")


async def test_pre_cancel_fill_is_delivered_after_cancel() -> None:
    sim = make_sim()
    sim.process_bar(bar(0, 100.0))
    sim.script(BTC, "buy", FillPlan((D("0.5"),)))
    ack = await sim.submit_order(market("buy", "1"))
    sim.process_bar(bar(1, 100.0))
    await sim.cancel_order(ack.order_id)  # nobody has read the stream yet

    (fill,) = await drain(sim)  # delayed delivery: executed before the cancel
    assert (fill.order_id, fill.qty) == (ack.order_id, D("0.5"))
    sim.process_bar(bar(2, 100.0))
    assert await drain(sim) == []  # the cancel is final: no later execution
```

Also add `Fill` to the `autotrader.broker.base` import list at the top of the test file.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/broker/test_sim.py -q`
Expected: collection error (`cannot import name 'FillPlan'`).

- [ ] **Step 3: Implement**

In `src/autotrader/broker/sim.py`:

Imports. Change to:

```python
import asyncio
from collections import deque
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import ROUND_FLOOR, Decimal
from typing import Literal, cast
```

and add `Side` to the `autotrader.broker.base` import list.

Below `_dec`, add:

```python
def _floor(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_FLOOR) * step


FillStep = Decimal | Literal["cancel", "reject"]


@dataclass(frozen=True)
class FillPlan:
    """Scripted outcome for one order (spec §5): one step per bar on which the order is
    eligible and its price condition holds. A fraction `0 < f <= 1` fills that share of
    the *original* qty (rounded down, capped at the remainder); "cancel" / "reject" end
    the order, keeping earlier fills. When steps run out the order fills its remainder."""

    steps: tuple[FillStep, ...]

    def __post_init__(self) -> None:
        for step in self.steps:
            if isinstance(step, Decimal):
                if not _D0 < step <= 1:
                    raise ValueError(f"fill step fraction must be in (0, 1], got {step}")
            elif step not in ("cancel", "reject"):
                raise ValueError(f"unknown fill step {step!r}")
```

with `_D0 = Decimal(0)` defined next to `_BPS`.

Replace the `_Order` dataclass with:

```python
@dataclass
class _Order:
    id: str
    order: BrokerOrder
    submitted_at: datetime
    status: OrderState = "new"
    filled_qty: Decimal = Decimal(0)
    filled_notional: Decimal = Decimal(0)
    fills: int = 0
    reason: str | None = None
    steps: deque[FillStep] = field(default_factory=deque)
    full_close: bool = False  # close_position: may be off-increment (spec §4)

    @property
    def remaining(self) -> Decimal:
        return self.order.qty - self.filled_qty

    @property
    def is_open(self) -> bool:
        return self.status in ("new", "partially_filled")

    @property
    def avg_fill_price(self) -> Decimal | None:
        return None if self.filled_qty == 0 else self.filled_notional / self.filled_qty
```

In `__init__`, add:

```python
        self._scripts: dict[tuple[str, str], deque[FillPlan]] = {}
```

Replace `submit_order` with:

```python
    async def submit_order(self, order: BrokerOrder) -> OrderAck:
        return await self._submit(order, full_close=False)

    async def _submit(self, order: BrokerOrder, *, full_close: bool) -> OrderAck:
        self._validate(order, full_close=full_close)
        self._seq += 1
        oid = f"sim-{self._seq}"
        submitted_at = self._now or datetime.now(UTC)
        queued = self._scripts.get((order.symbol, order.side))
        steps: deque[FillStep] = deque(queued.popleft().steps) if queued else deque()
        self._orders[oid] = _Order(oid, order, submitted_at, steps=steps, full_close=full_close)
        return OrderAck(
            order_id=oid,
            client_order_id=order.client_order_id,
            symbol=order.symbol,
            status="new",
            submitted_at=submitted_at,
        )

    def script(self, symbol: str, side: Side, plan: FillPlan) -> None:
        """Queue `plan` for the next order submitted for (symbol, side), FIFO (spec §5)."""
        self._scripts.setdefault((symbol, side), deque()).append(plan)
```

Replace `cancel_order` with:

```python
    async def cancel_order(self, order_id: str) -> None:
        o = self._get(order_id)
        if not o.is_open:
            raise OrderRejected(f"order {order_id} is not open ({o.status})")
        o.status = "canceled"  # final; filled_qty is kept, the remainder is released
```

In `get_order`, the `avg_fill_price=o.avg_fill_price` argument now reads the property (no change needed in text).

In `close_position`, replace the `return await self.submit_order(...)` with:

```python
        return await self._submit(
            BrokerOrder(symbol=symbol, side="sell", qty=qty, type="market"), full_close=True
        )
```

In `process_bar`, replace the order loop with:

```python
        for o in [o for o in self._orders.values() if o.is_open]:
            if o.order.symbol == bar.symbol and bar.ts > o.submitted_at:
                self._on_bar(o, bar)
                if o.is_open and o.order.time_in_force == "ioc":
                    o.status = "canceled"  # IOC: only the first bar after submit counts
```

Replace `_try_fill` with these four methods:

```python
    def _on_bar(self, o: _Order, bar: Bar) -> None:
        priced = self._price(o.order, bar)
        if priced is None:
            return  # limit not traded through: no step is consumed
        price, fee_bps = priced
        step: FillStep = o.steps.popleft() if o.steps else Decimal(1)
        if step == "cancel":
            o.status = "canceled"
        elif step == "reject":
            o.status, o.reason = "rejected", "scripted rejection"
        else:
            qty = self._slice(o, step)  # narrowed to Decimal by the branches above
            if qty > 0:
                self._fill(o, qty, price, fee_bps, bar.ts)

    def _price(self, order: BrokerOrder, bar: Bar) -> tuple[Decimal, Decimal] | None:
        """Fill price and fee rate in bps for `order` on `bar`, or None if it can't fill."""
        open_ = _dec(bar.open)
        if order.type == "market":
            slip = self.slippage_bps / _BPS
            price = open_ * (1 + slip) if order.side == "buy" else open_ * (1 - slip)
            return price, self.fees.taker_bps
        limit = cast(Decimal, order.limit_price)  # BrokerOrder guarantees it for limits
        if order.side == "buy" and _dec(bar.low) <= limit:
            price = min(open_, limit)
        elif order.side == "sell" and _dec(bar.high) >= limit:
            price = max(open_, limit)
        else:
            return None
        # An IOC limit crosses the book at submit (marketable limit): taker, not maker.
        ioc = order.time_in_force == "ioc"
        return price, self.fees.taker_bps if ioc else self.fees.maker_bps

    def _slice(self, o: _Order, fraction: Decimal) -> Decimal:
        """`fraction` of the original qty, rounded down and capped at the remainder. An
        off-increment full close rounds to `coin_precision` instead (spec §5)."""
        increment = self.asset_rules_by_symbol[o.order.symbol].qty_increment
        step = increment if o.order.qty % increment == 0 else self.coin_precision
        return min(_floor(fraction * o.order.qty, step), o.remaining)

    def _fill(self, o: _Order, qty: Decimal, price: Decimal, fee_bps: Decimal, ts: datetime) -> None:
        order = o.order
        notional = price * qty
        fee = notional * fee_bps / _BPS
        if order.side == "buy":
            if notional + fee > self.cash:
                o.status, o.reason = "rejected", "insufficient cash at fill"
                return
            self.cash -= notional + fee
            held = self._holdings.get(order.symbol)
            if held is None:
                self._holdings[order.symbol] = _Holding(qty, price)
            else:
                total = held.qty + qty
                held.avg_entry_price = (held.qty * held.avg_entry_price + qty * price) / total
                held.qty = total
        else:
            held = self._holdings[order.symbol]  # reserved at submit; cannot be missing
            self.cash += notional - fee
            held.qty -= qty
            if held.qty == 0:
                del self._holdings[order.symbol]
        o.filled_qty += qty
        o.filled_notional += notional
        o.status = "filled" if o.remaining == 0 else "partially_filled"
        o.fills += 1
        fill = Fill(
            fill_id=f"{o.id}:{o.fills}",
            order_id=o.id,
            symbol=order.symbol,
            side=order.side,
            qty=qty,
            price=price,
            fee=fee,
            fee_asset="quote",
            ts=ts,
        )
        self._fill_log[fill.fill_id] = fill
        self._fills.put_nowait(fill)
```

`self.coin_precision` is introduced in Task 4. To keep this task self-contained, add it now to `__init__` as the **last** keyword argument (after `timeframe`), `coin_precision: Decimal = Decimal("1e-9")`, stored as `self.coin_precision = coin_precision`. Task 4 adds its validation.

Change `_validate` to take `full_close` and skip only the increment check for a full close:

```python
    def _validate(self, order: BrokerOrder, *, full_close: bool = False) -> None:
        ...
        if not full_close and order.qty % rules.qty_increment != 0:
            raise OrderRejected(
                f"qty {order.qty} is not a multiple of increment {rules.qty_increment}"
            )
        ...
        if order.side == "buy":
            available = self.cash - self._reserved_cash()
            if self._estimated_cost(order, last, order.qty) > available:
                raise OrderRejected(f"insufficient cash: {available} available")
```

(keep every other line of `_validate` as it is).

Replace `_estimated_cost`, `_reserved_cash` and `_pending_qty` with:

```python
    def _estimated_cost(self, order: BrokerOrder, last: Decimal, qty: Decimal) -> Decimal:
        if order.limit_price is not None:
            fee_bps = self.fees.taker_bps if order.time_in_force == "ioc" else self.fees.maker_bps
            return order.limit_price * qty * (1 + fee_bps / _BPS)
        price = last * (1 + self.slippage_bps / _BPS)
        return price * qty * (1 + self.fees.taker_bps / _BPS)

    def _reserved_cash(self) -> Decimal:
        """Estimated cost of the unfilled remainder of every open buy."""
        return sum(
            (
                self._estimated_cost(o.order, self._last_price[o.order.symbol], o.remaining)
                for o in self._orders.values()
                if o.is_open and o.order.side == "buy"
            ),
            Decimal(0),
        )

    def _pending_qty(self, symbol: str, side: str) -> Decimal:
        """Unfilled remainder of open orders for (symbol, side)."""
        return sum(
            (
                o.remaining
                for o in self._orders.values()
                if o.is_open and o.order.symbol == symbol and o.order.side == side
            ),
            Decimal(0),
        )
```

Update the module docstring's fill-model bullets. Replace the two bullets "IOC limit: …" and "Fees are charged in quote currency (USD)." with:

```
- IOC limit: evaluated on the next processed bar only; fills like a limit but pays the
  taker fee (it crosses the book); any unfilled remainder is canceled.
- Every execution is a slice of its order and emits one `Fill` (`fill_id` =
  "{order_id}:{n}"). By default an order fills completely in one slice; a test can queue
  a `FillPlan` (`script()`) to split it over bars, cancel or reject it (spec 2026-10-12
  §5). Partly filled orders are `partially_filled` (not final); reservations cover only
  the unfilled remainder. `redeliver()` re-emits a fill for duplicate-delivery tests.
- Fees are charged in quote currency (USD).
```

- [ ] **Step 4: Run all tests**

Run: `uv run pytest -q` (redirect, check exit code).
Expected: PASS. All existing SimBroker tests are unchanged: an unscripted order is one slice of the full qty.

- [ ] **Step 5: Lint, type-check, commit**

```bash
uv run ruff format && uv run ruff check && uv run mypy
git add src/autotrader/broker/sim.py tests/unit/broker/test_sim.py
git commit -m "WP 1.R2: fill slices, scripted FillPlans and remainder reservations

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Coin-denominated buy fees (`buy_fee_asset="base"`)

**Files:**
- Modify: `src/autotrader/broker/sim.py` (`__init__`, `_fill`, `_estimated_cost`, docstring)
- Test: `tests/unit/broker/test_sim.py`

**Interfaces:**
- Consumes: `_fill`, `_estimated_cost(order, last, qty)`, `self.coin_precision` (Task 3).
- Produces: `SimBroker(..., coin_precision: Decimal = Decimal("1e-9"))`. It raises `ValueError` when an asset's `qty_increment` is not a multiple of `coin_precision`, and accepts both `buy_fee_asset` values.

- [ ] **Step 1: Write the failing tests**

Delete `test_sim_refuses_coin_denominated_buy_fees_until_it_models_them`. Change `make_sim` to accept rules:

```python
def make_sim(
    cash: str = "1000",
    slippage_bps: str = "0",
    fees: FeeSchedule = FEES,
    rules: dict[str, AssetRules] = RULES,
) -> SimBroker:
    return SimBroker(
        cash=Decimal(cash), fees=fees, slippage_bps=Decimal(slippage_bps), asset_rules=rules
    )
```

Append:

```python
# --- coin-denominated buy fees (WP 1.R2, D10) -----------------------------------------

BASE = FEES.model_copy(update={"buy_fee_asset": "base"})
FINE = {  # Alpaca's real BTC precision (docs/alpaca-notes.md, asset rules)
    BTC: AssetRules(
        min_qty=D("1e-9"), qty_increment=D("1e-9"), min_notional=D(0), price_increment=D("0.01")
    )
}


async def test_base_buy_takes_the_fee_from_the_coin() -> None:
    sim = make_sim(fees=BASE)
    sim.process_bar(bar(0, 100.0))
    ack = await sim.submit_order(market("buy", "1"))
    sim.process_bar(bar(1, 100.0))

    assert sim.cash == D("900")  # exactly the notional
    (pos,) = await sim.get_positions()
    assert (pos.qty, pos.avg_entry_price) == (D("0.9975"), D("100"))
    (fill,) = await drain(sim)
    assert (fill.qty, fill.fee, fill.fee_asset) == (D(1), D("0.0025"), "base")
    assert (await sim.get_order(ack.order_id)).filled_qty == D(1)  # gross


async def test_base_fee_rounds_up_like_the_paper_fill() -> None:
    # Fee check step 1 (2026-10-11): 0.000118113 BTC at 82994.2 left 0.000117817.
    sim = make_sim(fees=BASE, rules=FINE)
    sim.process_bar(bar(0, 82994.2))
    await sim.submit_order(ioc_buy("0.000118113", "82994.2"))
    sim.process_bar(bar(1, 82994.2))

    (pos,) = await sim.get_positions()
    assert pos.qty == D("0.000117817")  # fee 2.952825e-7 rounded up to 2.96e-7
    assert sim.cash == D("1000") - D("0.000118113") * D("82994.2")


async def test_base_sell_still_pays_usd() -> None:
    sim = make_sim(fees=BASE)
    sim.process_bar(bar(0, 100.0))
    await sim.submit_order(market("buy", "1"))
    sim.process_bar(bar(1, 100.0))
    await sim.submit_order(market("sell", "0.9975"))
    sim.process_bar(bar(2, 100.0))

    assert sim.cash == D("900") + D("99.75") * D("0.9975")
    fills = await drain(sim)
    assert (fills[-1].fee, fills[-1].fee_asset) == (D("99.75") * D("0.0025"), "quote")


async def test_base_buy_adds_to_existing_position() -> None:
    sim = make_sim(fees=BASE)
    sim.process_bar(bar(0, 100.0))
    await sim.submit_order(market("buy", "1"))
    sim.process_bar(bar(1, 100.0))
    await sim.submit_order(market("buy", "1"))
    sim.process_bar(bar(2, 200.0))

    (pos,) = await sim.get_positions()
    assert pos.qty == D("1.995")
    assert pos.avg_entry_price == D("150")  # (0.9975*100 + 0.9975*200) / 1.995


async def test_base_cash_checks_need_only_the_notional() -> None:
    sim = make_sim(cash="100", fees=BASE)
    sim.process_bar(bar(0, 100.0))
    ack = await sim.submit_order(limit("buy", "1", "100"))  # quote fees would need 100.15
    sim.process_bar(bar(1, 100.0))

    assert (await sim.get_order(ack.order_id)).status == "filled"
    assert sim.cash == D(0)
    (pos,) = await sim.get_positions()
    assert pos.qty == D("0.9985")  # 15 bps maker fee taken from the coin


async def test_base_slice_too_small_for_its_fee_is_rejected_before_any_change() -> None:
    sim = make_sim(fees=BASE, rules=FINE)
    sim.process_bar(bar(0, 100.0))
    sim.script(BTC, "buy", FillPlan((D("0.5"),)))
    ack = await sim.submit_order(market("buy", "0.000000003"))  # 3 quanta
    sim.process_bar(bar(1, 100.0))  # slice of 1 quantum: fee rounds up to 1 quantum
    status = await sim.get_order(ack.order_id)

    assert (status.status, status.filled_qty, status.reason) == (
        "rejected",
        D(0),
        "fill too small for coin fee",
    )
    assert sim.cash == D("1000")
    assert await sim.get_positions() == []
    assert await drain(sim) == []


def test_increment_must_be_a_multiple_of_coin_precision() -> None:
    finer = {BTC: FINE[BTC].model_copy(update={"qty_increment": D("1e-10")})}
    with pytest.raises(ValueError, match="coin_precision"):
        make_sim(fees=BASE, rules=finer)
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `uv run pytest tests/unit/broker/test_sim.py -k "base or coin_precision" -q`
Expected: FAIL (`SimBroker` raises "models only buy_fee_asset='quote'").

- [ ] **Step 3: Implement**

In `sim.py` add `ROUND_CEILING` to the decimal import and below `_floor`:

```python
def _ceil(value: Decimal, step: Decimal) -> Decimal:
    return (value / step).to_integral_value(rounding=ROUND_CEILING) * step
```

In `__init__`, replace the `if fees.buy_fee_asset != "quote": raise …` block with:

```python
        for symbol, symbol_rules in asset_rules.items():
            # Applies to both fee assets: quantities never go below one coin quantum, and a
            # coin fee rounded up to coin_precision can't exceed a minimal slice (spec §3).
            if symbol_rules.qty_increment % coin_precision != 0:
                raise ValueError(
                    f"{symbol}: qty_increment {symbol_rules.qty_increment} is not a multiple "
                    f"of coin_precision {coin_precision}"
                )
```

In `_fill`, replace the start of the method up to and including the buy branch's holding update with:

```python
    def _fill(self, o: _Order, qty: Decimal, price: Decimal, fee_bps: Decimal, ts: datetime) -> None:
        order = o.order
        notional = price * qty
        rate = fee_bps / _BPS
        if order.side == "buy" and self.fees.buy_fee_asset == "base":
            # Alpaca takes the buy fee from the coin at fill (G10, fee check step 1),
            # rounded up; cash pays exactly the notional.
            fee_asset: Literal["base", "quote"] = "base"
            fee = _ceil(qty * rate, self.coin_precision)
            cost, received = notional, qty - fee
            if received <= 0:
                o.status, o.reason = "rejected", "fill too small for coin fee"
                return
        else:
            fee_asset, fee = "quote", notional * rate
            cost, received = notional + fee, qty
        if order.side == "buy":
            if cost > self.cash:
                o.status, o.reason = "rejected", "insufficient cash at fill"
                return
            self.cash -= cost
            held = self._holdings.get(order.symbol)
            if held is None:
                self._holdings[order.symbol] = _Holding(received, price)
            else:
                # Weighted by coins held at fill prices: Alpaca keeps avg_entry_price = p.
                total = held.qty + received
                held.avg_entry_price = (held.qty * held.avg_entry_price + received * price) / total
                held.qty = total
```

Keep the sell branch and the bookkeeping that follows unchanged, except `fee_asset="quote"` in the `Fill(...)` call, which becomes `fee_asset=fee_asset`.

In `_estimated_cost`, apply the fee factor only for quote-fee buys:

```python
    def _estimated_cost(self, order: BrokerOrder, last: Decimal, qty: Decimal) -> Decimal:
        if order.limit_price is not None:
            price = order.limit_price
            fee_bps = self.fees.taker_bps if order.time_in_force == "ioc" else self.fees.maker_bps
        else:
            price = last * (1 + self.slippage_bps / _BPS)
            fee_bps = self.fees.taker_bps
        if self.fees.buy_fee_asset == "base":  # the fee comes out of the coin, not cash
            return price * qty
        return price * qty * (1 + fee_bps / _BPS)
```

In the module docstring replace `- Fees are charged in quote currency (USD).` with:

```
- Fees: sells pay in USD out of the proceeds. Buys follow `FeeSchedule.buy_fee_asset`:
  "quote" pays the fee in USD on top of the notional; "base" (Alpaca) pays exactly the
  notional and takes the fee from the coin, rounded up to `coin_precision` (1e-9). A
  slice too small for its rounded coin fee is rejected. `avg_entry_price` is the fill
  price (Alpaca's convention); cash is exact, never rounded to cents (spec §3).
```

- [ ] **Step 4: Run all tests**

Run: `uv run pytest -q` (redirect, check exit code). Expected: PASS.

- [ ] **Step 5: Lint, type-check, commit**

```bash
uv run ruff format && uv run ruff check && uv run mypy
git add src/autotrader/broker/sim.py tests/unit/broker/test_sim.py
git commit -m "WP 1.R2: coin-denominated buy fees in SimBroker (D10)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Exact-qty full close and the dust rule

**Files:**
- Modify: `src/autotrader/broker/sim.py` (`close_position`)
- Test: `tests/unit/broker/test_sim.py`

**Interfaces:**
- Consumes: `_submit(order, full_close=True)`, `_validate(..., full_close=...)` (Task 3), base fees (Task 4).
- Produces: `close_position` raises `OrderRejected("dust below minimum: …")` for a sellable remainder below `min_qty`.

- [ ] **Step 1: Write the failing tests**

```python
async def test_close_sells_an_off_increment_holding_exactly() -> None:
    sim = make_sim(fees=BASE)
    sim.process_bar(bar(0, 100.0))
    await sim.submit_order(market("buy", "1.0003"))
    sim.process_bar(bar(1, 100.0))
    (pos,) = await sim.get_positions()
    assert pos.qty == D("0.99779925")  # not a multiple of the 0.0001 increment

    with pytest.raises(OrderRejected, match="not a multiple"):
        await sim.submit_order(market("sell", "0.99779925"))  # ordinary orders still checked
    ack = await sim.close_position(BTC)
    sim.process_bar(bar(2, 100.0))

    assert (await sim.get_order(ack.order_id)).filled_qty == D("0.99779925")
    assert await sim.get_positions() == []


async def test_close_of_dust_is_rejected_and_the_position_stays() -> None:
    sim = make_sim(fees=BASE)
    sim.process_bar(bar(0, 20000.0))  # notional 2 >= min_notional 1
    await sim.submit_order(market("buy", "0.0001"))
    sim.process_bar(bar(1, 20000.0))  # net 0.00009975 < min_qty 0.0001

    with pytest.raises(OrderRejected, match="dust below minimum"):
        await sim.close_position(BTC)
    (pos,) = await sim.get_positions()
    assert pos.qty == D("0.00009975")
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/broker/test_sim.py -k "off_increment or dust" -q`
Expected: the dust test FAILS with `qty 0.00009975 below minimum quantity 0.0001` from `_validate`, not "dust below minimum". The off-increment test already passes after Task 3; keep it as the regression for spec §4.

- [ ] **Step 3: Implement**

In `close_position`, after the "already fully reserved" check, add:

```python
        rules = self.asset_rules_by_symbol[symbol]
        if qty < rules.min_qty:  # unsellable remainder: stays as dust (HeldPosition.is_dust)
            raise OrderRejected(f"dust below minimum: {qty} < min_qty {rules.min_qty}")
```

Add to the module docstring after the "Spot only" bullet:

```
- `close_position` sells the exact held qty minus pending sell remainders, even when it
  is not a multiple of `qty_increment` (Alpaca accepted the exact net qty on paper); a
  remainder below `min_qty` is dust and is rejected (spec §4). Whether Alpaca rejects a
  sell below `min_order_size` is unverified (WP 1.6).
```

- [ ] **Step 4: Run all tests** — `uv run pytest -q` (redirect, check exit code). Expected: PASS.

- [ ] **Step 5: Lint, type-check, commit**

```bash
uv run ruff format && uv run ruff check && uv run mypy
git add src/autotrader/broker/sim.py tests/unit/broker/test_sim.py
git commit -m "WP 1.R2: exact-qty full close and dust rule

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Property tests (conservation and the D16 cross-check)

**Files:**
- Test: `tests/unit/broker/test_sim.py`

**Interfaces:**
- Consumes: everything above, plus `autotrader.risk.checks.net_pnl_per_unit(entry, exit_price, fees, exit_slippage_bps) -> Decimal`.

- [ ] **Step 1: Write the tests** (append; add `from autotrader.risk.checks import net_pnl_per_unit` after the `autotrader.core.types` import)

```python
# --- properties (WP 1.R2) ---------------------------------------------------------------

_scripted_actions = st.lists(
    st.tuples(
        st.sampled_from(["buy", "sell", "limit_buy", "script_buy", "script_sell", "close", "bar"]),
        st.integers(min_value=1, max_value=50),
        st.floats(min_value=50, max_value=150, allow_nan=False),
    ),
    max_size=40,
)


@given(_scripted_actions, st.sampled_from(["base", "quote"]))
@settings(max_examples=200, deadline=None)
def test_replaying_fills_reproduces_cash_and_positions(
    actions: list[tuple[str, int, float]], fee_asset: str
) -> None:
    async def run() -> None:
        fees = FEES.model_copy(update={"buy_fee_asset": fee_asset})
        sim = make_sim(cash="2000", slippage_bps="5", fees=fees)
        sim.process_bar(bar(0, 100.0))
        for i, (kind, n, price) in enumerate(actions, start=1):
            qty = str(D(n) / 100)
            try:
                if kind == "bar":
                    sim.process_bar(bar(i, price, h=price * 1.01, low=price * 0.99))
                elif kind in ("buy", "sell"):
                    await sim.submit_order(market(kind, qty))
                elif kind == "limit_buy":
                    await sim.submit_order(limit("buy", qty, str(round(price, 2))))
                elif kind == "close":
                    await sim.close_position(BTC)
                else:
                    side = kind.removeprefix("script_")
                    sim.script(BTC, side, FillPlan((D(n) / 50,)))  # type: ignore[arg-type]
            except OrderRejected:
                pass
        cash, coins = D("2000"), D(0)
        for f in await drain(sim):
            if f.side == "buy":
                cash -= f.qty * f.price + (f.fee if f.fee_asset == "quote" else 0)
                coins += f.qty - (f.fee if f.fee_asset == "base" else 0)
            else:
                cash += f.qty * f.price - f.fee
                coins -= f.qty
        # Cash: exact up to Decimal's 28-digit rounding (summation order differs).
        assert abs(sim.cash - cash) < D("1e-18")
        held = {p.symbol: p.qty for p in await sim.get_positions()}
        assert held.get(BTC, D(0)) == coins  # coin quantities are exact
        assert sim.cash >= 0 and all(q > 0 for q in held.values())

    asyncio.run(run())


@given(
    entry=st.decimals(min_value=D("10"), max_value=D("1000"), places=2),
    drop=st.decimals(min_value=D("0.01"), max_value=D("0.10"), places=3),
    qty_steps=st.integers(min_value=2_000, max_value=50_000),  # close notional >= 1 at entry 10
    fee_asset=st.sampled_from(["base", "quote"]),
)
@settings(max_examples=200, deadline=None)
def test_ioc_buy_then_stop_sale_matches_d16_net_loss(
    entry: Decimal, drop: Decimal, qty_steps: int, fee_asset: str
) -> None:
    async def run() -> None:
        exit_slippage = D(10)  # the profile's risk.exit_slippage_bps
        fees = FEES.model_copy(update={"buy_fee_asset": fee_asset})
        stop = (entry * (1 - drop)).quantize(D("0.01"))
        qty = D(qty_steps) * D("0.0001")
        sim = SimBroker(
            cash=D("1000000"),
            fees=fees,
            slippage_bps=exit_slippage,
            asset_rules=RULES,
        )
        sim.process_bar(bar(0, float(entry)))
        await sim.submit_order(ioc_buy(str(qty), str(entry)))
        sim.process_bar(bar(1, float(entry)))  # IOC limit fills at entry, taker, no slip
        await sim.close_position(BTC)
        sim.process_bar(bar(2, float(stop)))  # market sell at stop x (1 - 10 bps)

        loss = D("1000000") - sim.cash
        expected = -net_pnl_per_unit(entry, stop, fees, exit_slippage) * qty
        if fee_asset == "quote":
            assert abs(loss - expected) < D("1e-18")
        else:  # one execution: the rounded-up coin fee costs at most one quantum of coin
            assert D(0) <= loss - expected <= sim.coin_precision * stop

    asyncio.run(run())
```

- [ ] **Step 2: Run them**

Run: `uv run pytest tests/unit/broker/test_sim.py -k "replaying or d16" -q`
Expected: PASS. These are properties of Tasks 3–5. If either fails, the implementation is wrong, not the test. Read the hypothesis counterexample, fix the code in the owning task's file, and re-run the whole suite.

- [ ] **Step 3: Commit**

```bash
uv run ruff format && uv run ruff check && uv run mypy
git add tests/unit/broker/test_sim.py
git commit -m "WP 1.R2: property tests for fill replay and the D16 cross-check

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Documentation alignment

**Files:**
- Modify: `src/autotrader/broker/base.py` (`FeeSchedule` comment)
- Modify: `SPEC.md` (§4.6, the `Implementations:` line and the paragraph after it)
- Modify: `PLAN.md` (1.R2 row, D10 row, "Current repo state" line)
- Modify: `docs/alpaca-notes.md` (G10 consequence 1)

- [ ] **Step 1: Search for claims this WP falsified**

Run:
```bash
rg -n "SimBroker until|charges all fees in USD|fees in USD|refuses .base.|until 1\.R2|until WP 1\.R2|durable fill identities|stream_fills" src SPEC.md PLAN.md docs/alpaca-notes.md README.md
```
Fix every hit that states the old behaviour as current. Dated history entries (PLAN decision rows, dated notes, `docs/superpowers/plans/*`, review reports) get an appended dated note, never a rewrite.

- [ ] **Step 2: Apply the known edits**

- `base.py`, `FeeSchedule.buy_fee_asset` comment: replace `"quote": charged in USD on top of the fill value (SimBroker until WP 1.R2).` with `"quote": charged in USD on top of the fill value. SimBroker models both (WP 1.R2).`
- `SPEC.md` §4.6, after the `Implementations:` sentence, append: ` Since WP 1.R2 (2026-10-12), SimBroker models Alpaca's coin-denominated buy fee (rounded up to 1e-9), exact-qty full closes with a dust rule, scripted partial fills (FillPlan) and stable fill ids (Fill.fill_id, fee_asset); see docs/superpowers/specs/2026-10-12-simbroker-fees-fills-design.md.` In the next paragraph, change `durable fill identities` to `durable fill persistence (fill ids exist since 1.R2)`.
- `PLAN.md` 1.R2 row: append ` **2026-10-12, 1.R2 done:** implemented per the spec (plan docs/superpowers/plans/2026-10-12-simbroker-fees-fills.md).` before the closing ` |`.
- `PLAN.md` D10 row: append ` **2026-10-12 (WP 1.R2):** implemented in SimBroker (coin fee rounded up to 1e-9, exact cash).` before the closing ` |`.
- `PLAN.md` "Current repo state": change `Next: 1.R2 SimBroker` to `1.R2 (SimBroker fees and fills) done. Next: WP 1.5`.
- `docs/alpaca-notes.md` G10 consequence 1: append ` **2026-10-12 (WP 1.R2):** SimBroker now models the coin fee (rounded up to 1e-9) and accepts buy_fee_asset="base".`

- [ ] **Step 3: Verify PLAN tables still render**

Run:
```bash
python3 -c "import sys; [print(n, l.count('|')) for n, l in enumerate(open('PLAN.md'), 1) if l.startswith(('| 1.R2', '| D10'))]"
```
Expected: the pipe count is unchanged from `git show origin/main:PLAN.md` for those rows (append inside the last cell, before the final ` |`). Then run `git diff origin/main -- PLAN.md | rg '^-[^-]'` and confirm every removed line has a replacement that only appends text.

- [ ] **Step 4: Final checks and commit**

```bash
uv run pytest -q > /tmp/wp-1r2-pytest.txt 2>&1; echo $?
uv run pytest -q --cov=autotrader.risk --cov-branch --cov-report=term-missing > /tmp/wp-1r2-cov.txt 2>&1; echo $?
uv run ruff check && uv run ruff format --check && uv run mypy
git add src/autotrader/broker/base.py SPEC.md PLAN.md docs/alpaca-notes.md
git commit -m "docs: WP 1.R2 SimBroker fees and fills

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
Expected: both pytest runs print `0`, and the risk coverage report shows 100 %.

---

## Plan verification (2026-10-12)

Tasks 1-6 were dry-run verbatim in a throwaway worktree before review. Six snippet bugs were found and are fixed above:
- non-ASCII `−`/`×` in code (RUF002/RUF003);
- an over-long one-line docstring (E501);
- a redundant `cast` (mypy `redundant-cast`);
- the dust test and the D16 property test failing on `min_notional` before reaching their real assertion.

With those fixes, every task's fail step failed for the stated reason. `uv run pytest -q` passed (737), ruff and mypy were clean, and `risk/` had 100 % branch coverage.
