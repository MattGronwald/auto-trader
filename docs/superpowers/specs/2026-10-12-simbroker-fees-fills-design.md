# SimBroker fees and fills design (WP 1.R2)

Date: 2026-10-12. Status: approved in conversation (sections 1 and 2), pending written-spec review.
Sources: SPEC §4.6; PLAN 1.R2, §3.2 items 3 and 5, G10, D10, D16; `docs/alpaca-notes.md`
(G10, asset rules, fee check steps 1 and 2).

## 1. Purpose and success criteria

WP 1.5 (Position Manager) is built and tested against `SimBroker`. Today the simulator
fills every order completely, charges all fees in USD and gives fills no identity, so 1.5
could not test partial entries, partial exits, idempotent fill handling or Alpaca's
coin-denominated buy fee. This WP makes the simulator realistic in exactly those respects.

Done means:
- `SimBroker` accepts `buy_fee_asset="base"` and reproduces Alpaca's observed fee
  mechanics: buy fee taken from the coin at fill, sell fee from the USD proceeds.
- Fills carry a stable, unique `fill_id`, the gross qty and the fee's asset.
- Tests can script partial fills, IOC partial cancels, multi-bar fills, fill-time
  rejections and redelivered (duplicate) fills deterministically.
- Reservations (cash, sell qty) account for partially filled orders.
- With either fee asset, the simulator's cash flows for a buy followed by a stop sale match
  `risk.checks.net_pnl_per_unit`, exactly apart from the coin-fee round-up (bounded by
  `coin_precision × price`), so D16 sizing is verified against the simulator.
- All existing behaviour of unscripted orders is unchanged; `risk/` keeps 100 % branch
  coverage.

Owner decisions taken while designing (2026-10-12, recorded in PLAN 1.R2):
- **Scripted fill outcomes**, not a volume-based participation cap. A cap can come in
  WP 1.7 if backtests need it.
- **Rounding: coin fee rounded up, cash exact.** The coin fee is rounded up to
  `coin_precision` (observed on paper, conservative). Cash is not rounded to cents; that
  rule rests on one sample (fee check step 2) and is worth less than a cent per fill.

## 2. Domain types (`broker/base.py`)

These are shared types; WP 1.5 builds on them.

**`Fill`**

| Field | Meaning |
|---|---|
| `fill_id: str` (new) | Unique and stable per broker execution. 1.5 applies each fill once, keyed on it (`fills.broker_fill_id`, unique). SimBroker: `"{order_id}:{n}"`, `n = 1, 2, …` per order. Alpaca's FILL activity id (`20261012140303109::<uuid>`) is mapped in WP 1.6. |
| `order_id`, `symbol`, `side`, `price`, `ts` | Unchanged. |
| `qty` | Gross qty traded at `price`. |
| `fee` | Fee amount, denominated in `fee_asset`. |
| `fee_asset: "base" \| "quote"` (new) | Asset the fee was charged in. A buy with `buy_fee_asset="base"` has `fee_asset="base"`; every sell and every `"quote"` buy has `"quote"`. |

Derived, not stored: coins received by a buy are `qty − fee` for `"base"`, `qty` for
`"quote"`. The stale comment on `Fill` (D10 "open decision") is replaced.

**`OrderState`** gains `"partially_filled"`: some qty filled, the rest still open. It is
not final. Final states stay `filled`, `canceled`, `rejected`. A partly filled order that
ends without filling completely is `canceled` (IOC remainder, scripted cancel,
`cancel_order`) or `rejected` (fill-time cash failure) with `filled_qty > 0`, matching
PLAN §3.2 item 3: `canceled` alone proves neither a fill nor its absence.

**`OrderStatus`**: `filled_qty` is cumulative; `avg_fill_price` is the qty-weighted
average price of all fills of the order.

## 3. Fee model (`broker/sim.py`)

`SimBroker.__init__` gains `coin_precision: Decimal = Decimal("1e-9")` (Alpaca's crypto
qty precision, `docs/alpaca-notes.md` asset rules). It is separate from
`AssetRules.qty_increment`: rounding a fee up to a test increment such as `0.001` would
inflate it by orders of magnitude.

Let `r` be the fee rate (taker or maker, as today: market and IOC limit pay taker, GTC
limit pays maker), `q` the gross fill qty and `p` the fill price.

| | `buy_fee_asset="base"` | `buy_fee_asset="quote"` (unchanged) |
|---|---|---|
| Buy: cash | `− p·q` exactly | `− p·q·(1 + r)` |
| Buy: coin fee | `ceil(q·r, coin_precision)` | none |
| Buy: position | `+ (q − fee)` | `+ q` |
| Sell: cash | `+ p·q·(1 − r)` exactly | same |
| Sell: position | `− q` | same |

- **Average entry price is the fill price `p`**, weighted by net coins across buys
  (`(held·avg + net·p) / (held + net)`). This is Alpaca's convention: on paper it kept
  `avg_entry_price = p` and reported `cost_basis = net_qty × p`, which hides the buy fee
  (fee check step 2). The alternative, `p·q / (q − fee)`, would fold the fee in, but then
  `Position` would differ between the two brokers. Real P&L is derived from cash flows,
  never from `avg_entry_price` (`docs/alpaca-notes.md`, G10 consequence 3).
- **Cash checks**: at submit and at fill, a `"base"` buy needs `p·q` cash, a `"quote"` buy
  `p·q·(1 + r)`. The submit-time estimate and pending-buy reservation use the same rule.
- **Conservation** (asserted by property tests, both fee assets): replaying the emitted
  `Fill`s through the table above reproduces the simulator's cash and every position
  exactly. Cash: `initial − Σ buy debits + Σ sell credits`. Coins per symbol:
  `Σ buys (q − base fee) − Σ sells q`. Nothing is created or lost outside a fee.
- **D16 cross-check**: an IOC limit buy filling at `entry`, followed by a market sell on a
  bar opening at `stop` with `slippage_bps = exit_slippage_bps` and the full held qty,
  yields a net cash loss per gross unit equal to
  `−net_pnl_per_unit(entry, stop, fees, exit_slippage_bps)` for `"quote"`. For `"base"` it
  is larger by at most `coin_precision × stop / q`, because the rounded-up fee leaves
  slightly fewer coins to sell. (The simulator's `slippage_bps` also applies to the buy
  of a *market* order; the IOC limit buy avoids that.)

## 4. Dust and full exits

- `close_position(symbol)` sells the **exact held qty minus pending sell remainders**,
  even when it is not a multiple of `qty_increment`. Alpaca accepted the exact 9-decimal
  net qty on paper (fee check step 2) and its increment equals its precision (1e-9).
- All other orders keep the `qty_increment` check.
- A sellable remainder below `AssetRules.min_qty` is **dust**: `close_position` raises
  `OrderRejected("dust below minimum …")` and the position stays. This matches the Risk
  Engine, which already treats such a remainder as dust (`HeldPosition.is_dust`).
- **Unverified:** whether Alpaca rejects a sell below `min_order_size`. WP 1.6 checks it on
  paper; the simulator's rule is the conservative assumption until then.

## 5. Scripted fill outcomes

```python
@dataclass(frozen=True)
class FillPlan:
    steps: tuple[Decimal | Literal["cancel", "reject"], ...]

sim.script("BTC/USD", "buy", FillPlan((D("0.4"),)))                    # IOC: 40 %, rest canceled
sim.script("BTC/USD", "sell", FillPlan((D("0.5"), D("0.3"), "cancel")))  # 3 bars, then canceled
sim.script("BTC/USD", "buy", FillPlan(("reject",)))                    # rejected at fill
sim.redeliver(fill_id)                                                 # same Fill again
```

- `script(symbol, side, plan)` queues a plan. At `submit_order`, the next order matching
  `(symbol, side)` takes the oldest queued plan (FIFO). `close_position` goes through
  `submit_order`, so exits are scriptable too.
- **One step per processed bar** on which the order is eligible (after its submission).
  A numeric step is a fraction `0 < f ≤ 1` of the *original* order qty. The slice is
  `min(floor(f·qty, step), remaining)`, where `step` is `qty_increment` when the order qty
  is a multiple of it, else `coin_precision` (an off-increment full close). A slice of 0
  fills nothing on that bar.
- `"cancel"` ends the order `canceled`; `"reject"` ends it `rejected` with reason
  `"scripted rejection"`. Both keep earlier fills.
- When the steps run out, a GTC or market order fills its remainder in full on the next
  eligible bar (the default). An IOC is canceled after its single bar regardless.
- **Prices are unchanged.** A plan only reduces or ends filling: a limit the bar does not
  trade through fills nothing, and that bar consumes no step.
- Each slice is cash-checked at fill like a whole order today. A slice that no longer fits
  ends the order `rejected` ("insufficient cash at fill") with earlier fills kept.
- Each slice emits its own `Fill` on `stream_fills`, with `fill_id = "{order_id}:{n}"`, the
  bar's `ts`, and its own fee.
- `redeliver(fill_id)` puts the identical `Fill` back on the stream. Unknown ids raise
  `KeyError`. Duplicate and out-of-order delivery (redeliver an older fill after a newer
  one) are built from it.
- Unscripted orders behave exactly as today.

## 6. Reservations

- Pending sells reserve `order.qty − filled_qty` while the order is `new` or
  `partially_filled`; `close_position` and sell validation subtract that remainder.
- Pending buys reserve the estimated cost of the unfilled remainder.
- `cancel_order` accepts `new` and `partially_filled` orders; the remainder is released.

## 7. Risk Engine and journal touch points

- `journal/repo.OPEN_ENTRY_STATUSES` gains `"partially_filled"`. The value is exactly 16
  characters and fits `orders.status String(16)`; no migration.
- `RiskEngine._refresh_open_entries` polls rows in `new` or `partially_filled`. A broker
  status of `new` or `partially_filled` keeps the row open (writing `partially_filled`
  when it changed); any final status is written and the row leaves the open set, as today.
  Entries are IOC, so with the simulator they never rest in `partially_filled`; the test
  scripts the state directly through a fake broker.
- `core/cycle.py` is unchanged: a `canceled` IOC entry with a partial fill is already left
  for WP 1.5 to reconcile from the broker's fill qty.
- `AlpacaBroker` is unchanged apart from the new types (its order side still raises until
  WP 1.6).

## 8. Out of scope

- Volume-based partial fills (WP 1.7, if needed).
- A fill arriving after a cancel request has been sent (1.5/1.8 can script it with a plan
  and a `cancel_order` between bars).
- Cash rounding to cents (owner decision above).
- Alpaca behaviour for sells below `min_order_size` (WP 1.6, paper check).
- Persisting fills (`fills` table, `broker_fill_id`): WP 1.5.

## 9. Testing

TDD in `tests/unit/broker/test_sim.py` unless noted.

- `"base"` buy: position `q − ceil(q·r)`, cash debit exactly `p·q`, fee rounded up at
  `coin_precision`, `Fill.fee_asset == "base"`; `avg_entry_price == p`.
- Sell: USD fee from proceeds, `fee_asset == "quote"`.
- `close_position` after a `"base"` buy sells the exact off-increment holding and leaves no
  position; a remainder below `min_qty` raises `dust below minimum`.
- Scripts: IOC 40 % → `canceled`, `filled_qty = 0.4·q`; 3-bar market sell →
  `partially_filled`, `partially_filled`, `filled`; `"reject"` and `"cancel"` keep earlier
  fills; an unscripted order after a consumed plan fills fully; a non-crossing limit bar
  consumes no step; FIFO matching per `(symbol, side)`.
- Fill identity: ids unique and stable across `get_order`/stream; `redeliver` yields an
  identical `Fill`; unknown id raises.
- Reservations: partial sell + `close_position` never oversells; partial buy releases cash
  on cancel; fill-time cash failure of a later slice.
- Property tests (hypothesis): conservation for both fee assets with random fill plans;
  D16 cross-check against `net_pnl_per_unit`.
- `tests/unit/risk/test_engine.py`: `_refresh_open_entries` keeps `partially_filled` open
  and records it; existing tests green; `risk/` 100 % branch coverage.
- `tests/unit/journal`: `partially_filled` round-trips through `orders.status`.

## 10. Documentation

- SPEC §4.6 SimBroker description; `sim.py` module docstring; `Fill`/`FeeSchedule`
  comments in `base.py` (`FeeSchedule` still says "SimBroker until WP 1.R2").
- PLAN: 1.R2 row (decisions, done note), D10 (implemented), repo-state line.
- `docs/alpaca-notes.md` G10 consequence 1 (simulator now models coin fees).
