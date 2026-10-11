# Risk Engine design (WP 1.3)

Date: 2026-10-11. Status: approved in conversation, pending written-spec review.
Sources: SPEC §0.1, §4.5, §4.6; PLAN G1, G3, G4, G6, G10, G14, D3, D4, D5, D13.
Decisions taken while designing are recorded as PLAN **D14**. Updated 2026-10-11 after the
PR #13 review (persisted HWM peaks, time budget, unpriced holdings fail closed, background
snapshot task, exit retry safety; see the D14 update).

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
  carries no market value; `None` when the feed has no last price for the symbol or the
  symbol is stale — never the cost basis, which would undercount exposure) and
  `avg_entry_price` (informational exposure estimate in snapshots only), open entry orders (symbol, notional), entry-order timestamps for the rate
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
   A non-dust held position without a current price (no feed price, or stale) makes the
   sum unknowable and rejects first: `held position <SYM> has no current price` (fail
   closed). Unpriced dust is ignored; priced dust still counts. Exits never need a price.
   Intended consequence: a held asset outside the feed universe (e.g. bought manually)
   never has a price, so it blocks every entry until it is sold.
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

**Two locks.** An entry lock serialises every `check()`, from gathering to submit, so two
entries can never pass check 5 on the same state; together with the Cycle Runner's caps
(G14) the limits are exact. A separate exit lock serialises every `exit()`, so two exits
on one symbol cannot both sell. An exit never waits for an entry: a stalled journal call
inside `check()` (outside the io budget by design) cannot block a stop-out. This is safe
because an entry that runs beside an exit still sees the position being exited and
rejects it or over-counts it (conservative), and a late IOC fill after a kill-flatten can
happen with or without a shared lock (fills are broker-side), so the Position Manager
handles it (WP 1.5). (Until the PR #13 round-2 review a single lock covered both.)

**Time budget.** Because each lock is held across broker calls, each locked section has
**one** budget, `RiskEngine(io_timeout_s=10.0)` (`run core` passes the default), measured
from taking its lock. Only the network/gate awaits draw on it: `gate.status`,
`get_account`, `get_positions`, each `asset_rules` and `get_order`, and the submit or
`close_position`; each gets what is left (`wait_for(..., deadline − now)`). Journal reads
and writes (every DB session, the peak snapshot write) run outside any `wait_for`, so a
timeout never cancels a DB session mid-statement or mid-commit — on a pooled Postgres
connection that could break the next caller, such as an exit's `_persist`. A timeout keeps
the stage-specific outcome (any gather await → `state unavailable: TimeoutError`,
persisted; pre-submit gate → `not_submitted`; submit or `close_position` → `unknown`). A
step whose budget is already spent is not called: the gate counts as unavailable
(`not_submitted`), and a submit or close that was never attempted is `not_submitted`, not
`unknown`, because nothing reached the broker. So one hung broker call holds its lock
(and delays the next entry, or the next exit) by at most ~1 × `io_timeout_s`. A journal
call that stalls instead errors on SQLite after `busy_timeout` (5 s, pinned in
`make_engine`); on Postgres `statement_timeout` / `command_timeout` follow in WP 1.6.
`asyncio.CancelledError` is never caught. `snapshot_equity()` (lock-free) bounds
its own broker calls with a separate budget. Broker-side request timeouts in
`AlpacaBroker` remain WP 1.6.

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
   gathering marks the order `not_submitted` and returns. A gate that raises or times out
   counts as "not allowed" (`not_submitted`, reason `gate unavailable: <error type>`). (Status values fit the
   16-character `orders.status` column, which Postgres enforces.)
5. Submit:
   - ack → store `broker_order_id` and the ack status;
   - `OrderRejected` → `status = rejected`, reason stored;
   - any other exception (timeout, network) → `status = unknown`. The order may exist at
     the broker; WP 1.5 reconciliation resolves it by `client_order_id`. Until then it
     counts as an open entry order for checks 5, 8 and 9.

### `exit(request) -> ExitResult`
No gate check. Gather the symbol's position and asset rules (no price needed), `check_exit`, persist (kind `exit`),
insert the `orders` row (no `client_order_id`: `Broker.close_position(symbol)` takes none,
so an acked exit is identified by its `broker_order_id`), `close_position`, record the outcome as above. The caller retries
failed exits; a stop that failed must be attempted again.

Retries are safe: every `exit()` re-reads the live position from the broker, and spot
brokers refuse to sell more than the free quantity (`SimBroker` does; for Alpaca this is
verified in the WP 1.6 paper smoke test, not assumed). So if an
accepted close lost its ack (`unknown`), a retry either finds no position (the sell
filled → rejected by `check_exit`) or is refused by the broker (the sell is still open →
`rejected`); it never sells twice. An exit row left `unknown`, or `pending` (a crash or a
failed ack write after the broker accepted it), carries no ids (no `client_order_id`, no
`broker_order_id`). WP 1.5 reconciles both from the broker's order history for that symbol
since the row's `submitted_at` (minus a clock-skew allowance), counting only sell orders not
linked to another row: exactly one → link it and adopt its status; none after a grace
window → `not_submitted`; several → pause (manual resume) and alert, never guess (PLAN WP
1.5).

### `snapshot_equity()`
Called by `run core` every 60 s. Writes `equity_snapshots`: equity (from the broker
account), cash, exposure (informational only, never used for a limit; `NULL` when the
positions were not read — migration 0003 made the column nullable; an unpriced holding
is estimated at `qty × avg_entry_price`),
`hwm = max(highest stored hwm, equity)`, `daily_pnl = equity − day_start_equity`. The
previous HWM is the maximum over **all** snapshot rows (`repo.max_hwm`), never the latest
row's value, so writers that interleave (the snapshot task and a `check()`) can never lower
it. `run core` runs it in one supervised background task (`_snapshot_loop`): the first
snapshot immediately, then every `snapshot_s` (60). There is no outer `wait_for`: the
engine bounds the snapshot's broker calls with its own `io_timeout_s` budget, and an outer
timeout would only risk cancelling the journal write mid-commit. A failure is logged as
`risk.snapshot_failed` (`timeout=True` when the engine's budget expired) and the next
attempt follows on schedule; a success logs `risk.snapshot_written`. The core loop never
awaits a snapshot, so a hung REST call (or even a hung DB write, which only stalls the
task) cannot delay kill detection or feed ticks; if the task ever ends it is
logged as `risk.snapshot_task_died` and restarted on the next tick. `check()` folds live equity into the
`hwm` it uses and, when live equity exceeds the stored maximum (or no snapshot exists),
writes an "account observation" snapshot **immediately after the account read** — before
the positions, asset-rules and journal reads, so their failure cannot lose it — with
exposure `NULL` (unknown). It is written also when the check then rejects or fails: a
spike between two periodic snapshots that falls back before the next one still bounds
drawdown, including after a restart. The early `max_hwm` read is not trusted alone: the
check re-reads `max_hwm` in its later journal session (with the counts, baseline and
stats) and decides with `max(early stored HWM, late stored HWM, live equity)`, so a peak
the snapshot task commits while the check awaits positions or rules still bounds
drawdown. `snapshot_equity()` likewise records the account
even when the positions read after it fails (exposure `NULL`, the error is still raised
and logged). Snapshots are append-only; the HWM is the max over all rows.

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
  `snapshot_equity()` every 60 s from a supervised background task (§6). Nothing calls `check()`/`exit()` until WP 1.4/1.5.
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
  dust ignored; `day_start_equity` / `hwm` from snapshots. PR #13 review additions: a
  peak seen by a rejected check still bounds drawdown (also after a restart) and the HWM
  is the max over all snapshots; a hung entry gather cannot block an exit; hung submit /
  close → `unknown`, hung or raising pre-submit gate → `not_submitted`; gather, gate and
  submit share one budget (a check with a slow gather and a hung submit ends after ~1
  budget; a budget spent before the gate or submit skips them → `not_submitted`, no
  broker order; same for an exit's close; budget assertions use an injected budget clock,
  never wall time); slow journal work in the gather is never cancelled by the budget; round 2: a peak seen by a
  check whose positions read then fails still bounds drawdown (also after restart), a
  stalled journal read inside `check()` never blocks an `exit()`, concurrent entries stay
  serialised and concurrent exits on one symbol sell once; an unpriced or
  stale held position rejects entries but not exits; a retried `unknown` exit never sells
  twice. `run core`: a hung snapshot never delays kill detection at the default interval;
  a dead snapshot task is logged and restarted.
- **SimBroker:** `ioc` fills through the limit with taker fee, otherwise cancels.

## 9. Out of scope

Partial exits; broker stop-limit backup orders (G2, WP 1.5); live order submission on
`AlpacaBroker` (WP 1.6); reconciliation of `pending` and `unknown` entry and exit orders (WP 1.5); extra bus events
(the `risk_decisions` and `orders` tables are the audit trail, the Cycle Runner persists
its transitions in WP 1.4); the LLM budget (WP 2.1).
