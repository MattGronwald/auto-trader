# Risk Engine design (WP 1.3)

Date: 2026-10-11. Status: approved in conversation, pending written-spec review.
Sources: SPEC §0.1, §4.5, §4.6; PLAN G1, G3, G4, G6, G10, G14, D3, D4, D5, D13.
Decisions taken while designing are recorded as PLAN **D14**.

## 1. Purpose and success criteria

The Risk Engine is the only component that holds a `Broker` (SPEC §0.1). Every entry order
passes the 11 deterministic checks of SPEC §4.5; every exit is reduce-only (G1). No LLM is
in its call path.

Done means:
- No code path other than `RiskEngine` can submit, cancel or close at the broker.
- Every entry and exit decision is persisted in `risk_decisions` with its reasons and size.
- Stop-outs, time exits, kill and flatten can never be blocked by a pause, kill or rate limit.
- `src/autotrader/risk/` has 100 % branch coverage, enforced in CI.

## 2. Structure

| File | Content |
|---|---|
| `risk/types.py` | Frozen pydantic inputs/outputs (below). All money and qty are `Decimal`. |
| `risk/checks.py` | `check_entry(...)` and `check_exit(...)`: pure functions. No clock (the caller passes `now`), no broker, no DB, no logging side effects. |
| `risk/engine.py` | `RiskEngine`: the only `Broker` holder. Gathers state, calls the pure checks, persists, applies side effects, re-checks the gate, submits. |

Rejected alternatives: an engine whose checks read broker/DB inline (every branch needs
mocks; state can be read twice with different answers), and a pluggable chain of check
objects (indirection with no user).

### Types (`risk/types.py`)

- `EntryIntent`: `cycle_id`, `symbol`, `stop`, `take_profit`, `confidence` (0–1),
  `size_fraction` (0 < f ≤ 1). No qty and no price: the engine sizes and prices.
- `ExitRequest`: `symbol`, `reason` (`stop`, `take_profit`, `time`, `trailing`, `kill`,
  `flatten`, `stale`), optional `cycle_id`. Full exits only.
- `PortfolioState`: `equity`, `cash`, `day_start_equity`, `hwm`, broker positions with
  market value (`qty × last trade` from the feed view; the broker's `Position` type
  carries no market value), open entry orders (symbol, notional), entry-order timestamps for the rate
  check, `strategy_trades_n`, `strategy_win_rate`.
- `MarketSnapshot`: `last_trade`, `bid`, `ask`, `spread_bps`, `atr` (`Decimal | None`),
  `stale`, `asset_rules`, `fees`.
- `RiskDecision`: `approved`, `reasons: list[str]`, `adjusted: dict`, `qty`, `order:
  BrokerOrder | None`, plus `pause: Literal["until_next_day", "manual"] | None` naming
  the side effect the engine must apply.

ATR comes from the indicators as a float; it is converted once at the boundary with
`Decimal(str(x))`, NaN becoming `None`.

## 3. Entry pricing (D14)

Entries are **marketable limit orders, time in force `ioc`**:
`limit = ask × (1 + entry_slippage_bps / 10 000)`, rounded **up** to the asset's
`price_increment`. Whatever does not fill immediately is cancelled by the broker, so no
resting entry orders exist. The limit is the entry price for every check below.

New human-only setting `risk.entry_slippage_bps` (default `10`). Profile validation
rejects it unless `entry_slippage_bps / 100 < max_price_deviation_pct`, so check 10 can
only reject because the book moved away from the last trade, never because of our own
allowance.

## 4. Entry checks (`check_entry`)

Evaluated in order; the first failure ends the evaluation and is the persisted reason.
An approved decision lists which cap bound the size (`adjusted["bound_by"]`).

1. **Control and mode.** `ControlStatus.entries_allowed`; `AppConfig.mode == broker.mode()`.
2. **Market.** Symbol in the profile universe; not stale; `spread_bps ≤
   scanner.max_spread_bps`. Unknown spread rejects.
3. **Daily loss.** Reject if `day_start_equity − equity ≥ max_daily_loss_pct ×
   day_start_equity`. Side effect `until_next_day`: the engine sets
   `control_state.paused_until` to the next trading-day start in `trading_day_tz` (D5, D13).
4. **Drawdown.** Reject if `hwm − equity ≥ max_drawdown_pct × hwm`. Side effect `manual`:
   the engine sets `control_state.paused` (manual resume only).
5. **Positions.** `len(positions ∪ open-entry symbols) < max_positions`; the symbol has
   neither a position nor an open entry order (no pyramiding). A position with `qty <
   asset_rules.min_qty` is **dust** and ignored, so a fee remainder cannot block a symbol.
6. **Stop.** `atr is None` rejects (checked first); then `stop < entry`; with `d = entry − stop`:
   `min_stop_atr × atr ≤ d ≤ max_stop_atr × atr`. Only the persisted reason differs from the
   reverse order.
7. **Size (G4 + cash cap, D14).**
   `risk_qty = risk_per_trade_pct × equity / d`,
   `cap_qty = max_position_pct × equity / entry`,
   `cash_qty = cash / (entry × (1 + taker_bps / 10 000))` (crypto is not marginable),
   `qty = size_fraction × min(risk_qty, cap_qty, cash_qty)`, rounded **down** to
   `qty_increment`. Reject if `qty < min_qty` or `qty × entry < min_notional`. The broker's
   per-order notional maximum ($200k) is omitted as irrelevant at PoC size.
8. **Exposure.** `Σ position market values + Σ open entry notionals + qty × entry ≤
   max_gross_exposure_pct × equity`. Over the limit rejects; the order is never shrunk.
9. **Order rate.** Entry orders (including failed and unknown submits) in the rolling last
   60 min `< max_orders_per_hour`; since the trading-day start `< max_orders_per_day`.
   Exits never count.
10. **Price sanity.** `|limit − last_trade| ≤ max_price_deviation_pct × last_trade`;
    `stop < limit < take_profit`. "Last trade" is the close of the feed's newest 1m bar
    (pending or verified), which can be minutes old on sparse symbols (G13); do not widen
    `max_price_deviation_pct` to silence those rejections.
11. **Expectancy (G3, D4).** `win_rate_est = default_win_rate_prior` while
    `strategy_trades_n < learning.min_trades_for_evidence`, else `strategy_win_rate`.
    `p = min(confidence, win_rate_est + 0.10)`.
    `ev = qty·(tp − entry)·p − qty·(entry − stop)·(1 − p) − qty·entry·taker − qty·tp·taker`.
    Reject if `ev ≤ 0`. Known optimism: stop exits are market orders and slippage past the
    stop is not modelled.

## 5. Exit check (`check_exit`)

Approve if the broker reports a non-dust position in the symbol and the mode matches.
Nothing else: no gate, no pause, no rate limit (G1). The resulting action is the broker's
`close_position(symbol)`, which sells exactly what the broker holds; this avoids the
fee-reduced quantity of G10 and precision mismatches. Partial exits are out of scope.

## 6. Engine flow (`risk/engine.py`)

One `asyncio.Lock` serialises every entry and exit, from gathering to submit. Two entries
can therefore never pass check 5 on the same state; together with the Cycle Runner's caps
(G14) the limits are exact.

### `check(intent) -> EntryResult`
1. Gather: `ControlGate.status(now)`; **first refresh the engine's own `new` entry orders
   from `broker.get_order()`, holding no DB session across broker calls**, then read broker
   account, positions, `asset_rules(symbol)`, `fee_schedule()` (refreshing first means a
   fill between the two reads cannot hide the order from both); feed view (last trade,
   quote, staleness; ATR computed by the engine from the signal-timeframe history, never
   taken from the intent); journal (open entry orders, entry-order timestamps,
   `day_start_equity`, the highest stored `hwm`, win rate). **Any failure
   rejects** with `state unavailable: <error type>` and is persisted (fail closed).
2. `check_entry(...)`; persist `risk_decisions` (kind `entry`). On rejection apply the
   `pause` side effect and return.
3. Approved: insert the `orders` row first, `status = pending`, with the deterministic
   `client_order_id = "at-c{cycle_id}-e"`. The unique column means a cycle can never
   submit a second entry, even if the Cycle Runner retries.
4. Ask the gate again immediately before submitting; a kill or pause that arrived during
   gathering marks the order `not_submitted` and returns. (Status values fit the
   16-character `orders.status` column, which Postgres enforces.)
5. Submit:
   - ack → store `broker_order_id` and the ack status;
   - `OrderRejected` → `status = rejected`, reason stored;
   - any other exception (timeout, network) → `status = unknown`. The order may exist at
     the broker; WP 1.5 reconciliation resolves it by `client_order_id`. Until then it
     counts as an open entry order for checks 5, 8 and 9.

### `exit(request) -> ExitResult`
No gate check. Gather positions and asset rules, `check_exit`, persist (kind `exit`),
insert the `orders` row (no `client_order_id`: `Broker.close_position(symbol)` takes none,
so exits are identified by `broker_order_id`), `close_position`, record the outcome as above. The caller retries
failed exits; a stop that failed must be attempted again.

### `snapshot_equity()`
Called by `run core` every 60 s. Writes `equity_snapshots`: equity, cash, exposure,
`hwm = max(highest stored hwm, equity)`, `daily_pnl = equity − day_start_equity`. The
previous HWM is the maximum over **all** snapshot rows (`repo.max_hwm`), never the latest
row's value, so writers that interleave (the snapshot task and a `check()`) can never lower
it. Failure is logged and retried on the next tick. `check()` folds live equity into the
`hwm` it uses and, when live equity exceeds the stored maximum, writes a snapshot at once,
also when the check then rejects: a spike between two periodic snapshots that falls back
before the next one still bounds drawdown, including after a restart.

`day_start_equity` = equity of the last snapshot at or before the current trading-day
start (D5). If none exists (first day of operation) the first snapshot of the day is used.
If no snapshot exists at all, the engine takes one before checking.

## 7. Supporting changes

- **Broker types:** `BrokerOrder.time_in_force: Literal["gtc", "ioc"]` (default `gtc`);
  `AssetRules.price_increment`. `AlpacaBroker.asset_rules` maps `price_increment`
  (order submission itself stays WP 1.6).
- **SimBroker:** an `ioc` limit order is evaluated on the next processed bar only: it fills
  if the bar trades through the limit, at the better of open and limit, with the **taker**
  fee (it crosses the book); otherwise it is cancelled. `gtc` behaviour is unchanged.
- **Profile:** `risk.entry_slippage_bps` with the validation in §3; added to
  `strategies/fast_momentum_v1.yaml`.
- **Journal:** one Alembic migration adding `orders.client_order_id` (unique, nullable; entries only) and
  `orders.symbol` (SPEC §7 has none; exits from a kill or flatten have no cycle to take it from,
  and open entry orders must block their symbol).
  Repo functions for risk decisions, orders, equity snapshots, open entry orders, entry
  order timestamps and the strategy's closed-trade win rate (closed `positions` with
  `realized_pnl > 0`, i.e. net of fees, counted as wins; joined to the active strategy via
  `cycles`).
- **`run core`:** keeps the broker it connects at startup, hands it to `RiskEngine`, calls
  `snapshot_equity()` every 60 s. Nothing calls `check()`/`exit()` until WP 1.4/1.5.
- **CI:** `coverage report --include='src/autotrader/risk/*' --fail-under=100` (branch).

## 8. Tests

- **Pure checks:** for each of the 11 entry checks and the exit check, at least one pass
  and one fail, boundaries pinned exactly (`≤` vs `<` as specified above).
- **Hypothesis properties (sizing, G4 + cash cap):** `qty ≥ 0` and a multiple of
  `qty_increment`; `qty × d ≤ risk_per_trade_pct × equity`; `qty × entry ≤
  max_position_pct × equity`; `qty × entry × (1 + taker) ≤ cash`; every approved order has
  `stop < entry < tp` and `ev > 0`.
- **Engine** (SimBroker, migrated SQLite, real `ControlGate`): approved entry persisted
  and submitted; both pause side effects, including the next-day boundary in a non-UTC
  `trading_day_tz`; gathering failure rejects; kill between gathering and submit; broker
  rejection vs `unknown` (and `unknown` counting as open afterwards); exit while killed;
  dust ignored; `day_start_equity` / `hwm` from snapshots.
- **SimBroker:** `ioc` fills through the limit with taker fee, otherwise cancels.

## 9. Out of scope

Partial exits; broker stop-limit backup orders (G2, WP 1.5); live order submission on
`AlpacaBroker` (WP 1.6); reconciliation of `unknown` orders (WP 1.5); extra bus events
(the `risk_decisions` and `orders` tables are the audit trail, the Cycle Runner persists
its transitions in WP 1.4); the LLM budget (WP 2.1).
