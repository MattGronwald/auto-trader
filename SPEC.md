# Auto Trader — Specification

**Status:** Draft v1 · 2026-10-09
**Owner:** Matthias
**Purpose:** PoC for a fully automated, agent-based crypto trading system. Paper trading first, live trading with a ~1,000 EUR account later. This document is the single source of truth for implementation by Claude Code.

---

## 0. Non-negotiables (read first)

These constraints override anything else in this document. If an implementation detail conflicts with one of these, the constraint wins.

1. **No order reaches a broker without passing the deterministic Risk Engine.** The Risk Engine is plain Python, has no LLM in its call path, and cannot be bypassed by any agent, config flag, or dashboard action. It is the only component with a reference to the broker's `submit_order`.
2. **Every LLM call is metered and attributed.** Token count, model, USD cost, cycle id, agent name. Cost is a first-class P&L line item. A cycle that costs more than its expected edge is a bug.
3. **Agents are called on signal, not on schedule.** The deterministic Signal Scanner runs continuously and cheaply; LLM agents run only when a candidate signal exists. There is no "call all agents every 15 minutes" loop.
4. **Learning is hypothesis-driven, gated, and reversible.** The system never silently rewrites its own strategy. Proposed changes are versioned, evaluated against a baseline, and promoted only by explicit rule (or human approval in the first phases).
5. **Live mode requires a passed gate** (see §12). Paper → live is a deliberate, logged transition with a checklist, not a config change.
6. **Kill-switch is a file and an API call**, independent of the LLM, the scheduler, and the dashboard backend. `touch KILL` stops all trading within one scanner tick.

---

## 1. Goals, non-goals, success criteria

### Goals
- Run an end-to-end automated trading loop on crypto (via Alpaca, broker abstracted) with multiple specialized LLM agents.
- Maintain a trading journal that evaluates past decisions and feeds structured learnings back into agent context ("gut feeling" as an explicit, inspectable memory, not a black box).
- Allow the whole system to be configured to a named strategy ("strategy profile") that defines universe, timeframe, risk budget, and the heuristics agents start from.
- Initial strategy: short-horizon momentum/breakout on liquid crypto pairs, holding minutes to hours, tight risk management.
- Monitoring and limited control through a web dashboard.

### Non-goals (for the PoC)
- Beating the market in a statistically significant way. With 1,000 EUR and a few hundred trades, the system will not produce significant alpha evidence. The PoC's purpose is to validate the architecture, the cost model, and the learning loop mechanics.
- High-frequency / sub-second trading. Latency floor is ~1 s (REST + LLM).
- Multi-broker live trading. One broker adapter is live at a time.
- Tax reporting. Out of scope, but the journal must export every fill with timestamp, qty, price, fee (needed for §23 EStG anyway).

### Success criteria for the PoC
- 4 weeks continuous paper trading with zero unhandled crashes and zero Risk Engine bypasses.
- LLM cost per day < 1 % of account equity (i.e. < ~10 USD/day at 1,000 EUR — target is < 3 USD/day).
- Journal produces at least one promoted and one rejected hypothesis with documented evidence.
- Dashboard shows live state, equity curve vs. benchmark, cost, and allows pause/resume/flatten.

---

## 2. Key decisions and rationale

| Decision | Choice | Why | Rejected alternatives |
|---|---|---|---|
| Asset class | Crypto spot | 24/7 market, no US PDT rule (3 day-trades / 5 days under 25k USD kills a "fast" equities strategy), high volatility suits short horizon | US equities swing (slower), equities intraday (illegal at this account size) |
| Broker | Alpaca (paper + live), behind a `Broker` interface | One API for paper and live, Python SDK (`alpaca-py`), crypto + equities for later | IBKR (best EU API but Gateway setup overhead), Kraken (fallback — see risk below) |
| Language | Python 3.12 | Ecosystem: alpaca-py, pandas, pydantic, backtesting libs | TypeScript (thin quant ecosystem) |
| LLM | Anthropic API directly, thin custom agent loop | Full cost control, no framework abstraction in the debugging path | Agent SDK, LangGraph, CrewAI |
| Orchestration | Deterministic Python state machine | An LLM orchestrator is cost and nondeterminism with no upside | "Orchestrator Agent" from prior design |
| Persistence | SQLite (dev) → Postgres (prod), via SQLAlchemy | Local-first, no infra for PoC; same schema in prod | Flat files (no querying), Postgres-only (infra too early) |
| Runtime | Docker Compose; dev on MacBook, **must** move to an always-on host before live | Crypto is 24/7; a sleeping laptop with open positions is an unmanaged risk | Running live from the laptop |
| Dashboard | FastAPI backend + single-page frontend (React + Vite + Tailwind), WebSocket for live updates | Clean separation, easy to make "pretty", reusable | Streamlit (fast but ugly and hard to control), Grafana (monitoring, not control) |

### Open risk: Alpaca crypto availability for German residents
Paper trading works regardless. **Live crypto trading for non-US residents must be verified against Alpaca's current terms before Phase 5.** The `Broker` interface exists so that a Kraken adapter (REST + WS, EU-friendly, crypto only) can replace Alpaca with no changes above the adapter layer. Implement Alpaca first; do not implement Kraken unless the check fails.

### Cost model (why event-driven matters)
A naive design calling 5–7 agents every 15 minutes during a 24 h market ≈ 100 cycles/day × 6 calls ≈ 600 LLM calls/day. At Sonnet-class prices with ~4k input / 500 output tokens that is roughly 10–15 USD/day → 300–450 USD/month → 30–45 % of the account per month just in tokens. **Not viable.**

Target design: scanner finds ~5–20 candidate signals/day; each triggers 2–3 cheap calls (Haiku-class) and at most 1 decision call (Sonnet-class); journal review runs once a day (Sonnet/Opus). Target: < 100 LLM calls/day, < 3 USD/day.

---

## 3. Architecture overview

```
┌──────────────────────────────────────────────────────────────────────┐
│  Dashboard (React SPA)  ◄──WS/REST──►  API (FastAPI)                 │
└──────────────────────────────────────────────────────────────────────┘
                                             │ reads/writes
┌──────────────────────────────────────────────────────────────────────┐
│  Core (single Python process, asyncio)                               │
│                                                                      │
│  ┌──────────────┐   ┌──────────────┐   ┌───────────────────────┐    │
│  │ Market Data  │──►│ Signal       │──►│ Cycle Runner          │    │
│  │ Feed (WS)    │   │ Scanner      │   │ (deterministic FSM)   │    │
│  └──────────────┘   │ (no LLM)     │   └───────────┬───────────┘    │
│                     └──────────────┘               │ on signal       │
│                                          ┌─────────▼───────────┐    │
│                                          │ Agent Pool          │    │
│                                          │  Technical Analyst  │    │
│                                          │  Context Analyst    │    │
│                                          │  Decision Maker     │    │
│                                          │  (each: prompt +    │    │
│                                          │   structured output)│    │
│                                          └─────────┬───────────┘    │
│                                                    │ OrderIntent     │
│                                          ┌─────────▼───────────┐    │
│                                          │ RISK ENGINE         │    │
│                                          │ (deterministic,     │    │
│                                          │  sole broker access)│    │
│                                          └─────────┬───────────┘    │
│                                                    │ approved order  │
│                                          ┌─────────▼───────────┐    │
│                                          │ Broker Adapter      │    │
│                                          │  AlpacaPaper/Live   │    │
│                                          └─────────┬───────────┘    │
│                                                    │ fills           │
│  ┌──────────────────────────────────────────────────▼─────────────┐  │
│  │ Position Manager (stops, trailing, time exits — no LLM)        │  │
│  └──────────────────────────────────────────────────┬─────────────┘  │
│                                                     │                │
│  ┌──────────────────────────────────────────────────▼─────────────┐  │
│  │ Journal + Learning Loop (nightly review, hypothesis registry)  │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                                                                      │
│  Cross-cutting: Event Bus · Cost Meter · Kill-Switch · Config/Strategy│
└──────────────────────────────────────────────────────────────────────┘
                              │
                     ┌────────▼────────┐
                     │ SQLite/Postgres │
                     └─────────────────┘
```

**Differences to the prior designs (screenshots):**
- No LLM orchestrator. The Cycle Runner is a finite state machine.
- No separate "Risk Agent" LLM. Risk is deterministic. Risk *context* (volatility regime, correlation) is computed and injected into prompts as data.
- No Bull/Bear debate. One Decision Maker with explicit "strongest counter-argument" field in its output schema gets most of the benefit at a third of the cost. Debate can be enabled per strategy profile later (`decision.mode = "debate"`), but is off by default.
- Position Manager is new and deterministic: stops and exits are not LLM decisions. The LLM decides entries and may *propose* exits; the Position Manager enforces them.

---

## 4. Components

### 4.1 Market Data Feed
- Alpaca crypto WebSocket for trades/quotes/bars on the configured universe; REST backfill of historical bars on startup.
- Maintains rolling in-memory OHLCV frames per symbol per timeframe (1m, 5m, 15m, 1h) with computed indicators (EMA 9/21/50, RSI 14, ATR 14, VWAP, volume z-score, Bollinger, Donchian 20).
- Persists 1m bars to DB for backtesting and journal replay.
- Emits `BarClosed(symbol, timeframe)` events.
- Health: if no data for a symbol for > N seconds, mark symbol stale → scanner ignores it, position manager still runs on last known price with stale flag, dashboard shows warning.

### 4.2 Signal Scanner (deterministic, no LLM)
- Subscribes to `BarClosed`. Runs the strategy profile's **signal rules** (see §6) on every closed bar.
- Emits `CandidateSignal(symbol, direction, rule_id, features: dict, score: float)`.
- Rate limit: max N candidates per symbol per hour, max M concurrent open cycles (both from strategy profile). This is the primary LLM cost control.
- Pre-filters: no signal if symbol stale, if spread > max_spread_bps, if volume z-score < threshold, if symbol already has an open position or open cycle, if daily loss limit hit, if kill-switch set.

### 4.3 Cycle Runner (deterministic FSM)
States: `NEW → ENRICHING → ANALYZING → DECIDING → RISK_CHECK → SUBMITTED → (FILLED | REJECTED | EXPIRED | ERROR)`.

- One cycle per `CandidateSignal`. Every state transition is persisted with timestamp and payload (full audit trail).
- Timeouts per state (e.g. LLM call > 30 s → `ERROR`, logged, cycle discarded; no retry storms).
- Budget check before each LLM call: if daily LLM budget exceeded → cycle `EXPIRED` with reason `budget`.

### 4.4 Agents
All agents share a common runner: build prompt from template + context → call Anthropic API with tool-use/structured output → validate with pydantic → persist request/response/tokens/cost → return typed object. No agent has side effects on the broker or DB beyond its own log.

Every agent receives the **Strategy Profile summary** and the **Active Learnings** block (§8.4) in its system prompt.

| Agent | Model (default) | Input | Output (pydantic) | Cost target |
|---|---|---|---|---|
| **Technical Analyst** | Haiku-class | Signal features, last 50 bars (compact table), indicators, regime stats | `TechnicalAssessment{setup_quality: 0–1, trend_alignment: enum, volatility_regime: enum, key_levels: [..], invalidation_price, notes}` | < 0.005 USD |
| **Context Analyst** | Haiku-class | Headlines last 24 h for the asset (if a news source is configured), funding rate, BTC correlation, time-of-day/weekend flags, upcoming known events | `ContextAssessment{sentiment: -1..1, event_risk: enum, notable_items: [..], confidence}` | < 0.005 USD |
| **Decision Maker** | Sonnet-class | Both assessments + portfolio state + risk context + active learnings | `OrderIntent{action: BUY/SELL/SKIP, symbol, side, size_fraction: 0–1 of max allowed, entry: MARKET/LIMIT@price, stop_price, take_profit_price, time_limit_min, confidence: 0–1, thesis: str, strongest_counter: str, learnings_applied: [ids]}` | < 0.03 USD |
| **Journal Reviewer** | Sonnet/Opus-class | Closed trades since last review, outcomes vs. thesis, equity vs. benchmark, cost, current hypotheses | `ReviewReport{trade_postmortems: [..], new_hypotheses: [..], hypothesis_evidence_updates: [..], promote: [ids], retire: [ids], summary}` | < 0.50 USD/day |

Notes:
- The Context Analyst is skipped (cycle goes straight to DECIDING) if no news source is configured. Do not hallucinate news. In Phase 1 there is no news source.
- `size_fraction` is a *request*; the Risk Engine computes the actual size. The LLM never sees or sets absolute quantities.
- Decision Maker prompt must require `strongest_counter` to be non-empty and `confidence` to be calibrated against the learnings block ("if learnings say setups like this won 35 % of the time, your confidence must reflect that").
- All prompts live in `prompts/<agent>/<version>.md`, versioned; the version used is stored per call.

### 4.5 Risk Engine (deterministic)
Pure function `check(intent: OrderIntent, portfolio: PortfolioState, market: MarketSnapshot, limits: RiskLimits) -> RiskDecision{approved: bool, order: BrokerOrder | None, reasons: [str], adjusted_fields: dict}`.

Checks, in order (all must pass):
1. Kill-switch not set; trading not paused; mode (paper/live) matches broker adapter.
2. Symbol in allowed universe; symbol not stale; spread ≤ `max_spread_bps`.
3. Daily realized+unrealized loss < `max_daily_loss_pct` of start-of-day equity. If hit: reject and set `trading_paused_until = next_day`.
4. Drawdown from high-water mark < `max_drawdown_pct`. If hit: reject and set `trading_paused = true` (manual resume only).
5. Open positions < `max_positions`; no existing position in symbol (no pyramiding in PoC).
6. Stop price present and on the correct side; distance to stop between `min_stop_atr` and `max_stop_atr` × ATR.
7. **Position size = min(** `risk_per_trade_pct × equity / stop_distance`, `max_position_pct × equity / price`, `size_fraction × that`, broker min/max notional **)**. Round to broker lot size. Reject if below broker minimum.
8. Total exposure after order ≤ `max_gross_exposure_pct`.
9. Order rate: ≤ `max_orders_per_hour`, ≤ `max_orders_per_day`.
10. Price sanity: limit price within ±`max_price_deviation_pct` of last trade; stop/TP form a valid bracket.
11. Fee-adjusted expectancy: `(tp_distance × p_win_est − stop_distance × (1−p_win_est)) − 2×fee > 0` with `p_win_est` = decision confidence capped at the strategy's historical win rate + 10 pp. Reject if ≤ 0 (prevents trades that can't beat fees).

Output is a bracket order (entry + stop + TP) when the broker supports it; otherwise entry order + Position Manager-enforced stops.

`RiskLimits` is loaded from the strategy profile and **cannot be changed by the Learning Loop**. Only a human edits it (dashboard with confirmation, or config file + restart).

### 4.6 Broker Adapter
Interface:
```python
class Broker(Protocol):
    async def get_account() -> Account
    async def get_positions() -> list[Position]
    async def submit_order(order: BrokerOrder) -> OrderAck
    async def cancel_order(order_id) -> None
    async def get_order(order_id) -> OrderStatus
    async def close_position(symbol) -> OrderAck
    async def close_all() -> list[OrderAck]
    async def stream_fills() -> AsyncIterator[Fill]
    def mode() -> Literal["paper", "live"]
```
Implementations: `AlpacaBroker(paper=True|False)`, `SimBroker` (in-process, for backtests and tests; fills at next bar open + slippage model + fee model).

Fees and slippage: adapter exposes `fee_schedule()` so the Risk Engine's expectancy check uses real numbers. **Verify Alpaca's current crypto maker/taker tiers before implementing; do not hard-code from memory.**

### 4.7 Position Manager (deterministic)
- Tracks open positions with their stop, TP, time limit, and entry thesis.
- Enforces: hard stop (if bracket not supported or broker stop fails → market close), time-based exit (`time_limit_min` from intent, capped by strategy profile), trailing stop once position is +`trail_trigger_r` R in profit (if enabled in profile).
- On fill events: updates position state, writes `TradeEvent`s.
- Optional LLM exit review (`exit_review.enabled`, default **off** in PoC): at +1R or −0.5R, asks the Decision Maker whether the thesis still holds. Costs money; measure before enabling.
- Reconciliation every 60 s against broker positions; any mismatch → alert + pause new entries.

### 4.8 Journal
Tables (see §7): every cycle, every agent call, every order, every fill, every position, every review, every hypothesis. Nothing is deleted.

Derived views: closed trades with R-multiple, MAE/MFE, holding time, fee, LLM cost attributed to the cycle, benchmark return over the same holding window, "thesis vs. outcome" tags from the reviewer.

### 4.9 Learning Loop
See §8.

### 4.10 Dashboard & API
See §9.

### 4.11 Cross-cutting
- **Event bus:** in-process asyncio pub/sub; every event also persisted to `events` table (append-only). Dashboard tails it via WebSocket.
- **Cost meter:** wraps the Anthropic client; per-call tokens and USD (price table in config, versioned); daily and per-cycle aggregates; hard daily budget → scanner stops emitting candidates when exceeded.
- **Kill-switch:** `KILL` file in working dir checked each scanner tick and before every broker call; `POST /control/kill` writes it; dashboard button. On kill: cancel open orders, optionally flatten (config `kill.flatten = true|false`, default true in live, false in paper).
- **Config:** `config.yaml` (infra, API keys via env) + `strategies/<name>.yaml` (strategy profile, §6). Secrets only via environment / `.env` (git-ignored).
- **Logging:** structured JSON (structlog), one line per event, cycle_id in every line.

---

## 5. Cycle walkthrough (happy path)

1. `BarClosed(BTC/USD, 5m)` → Scanner evaluates rules → rule `breakout_donchian20_volspike` fires, score 0.72 → checks rate limits, no open position → emits `CandidateSignal`.
2. Cycle Runner creates cycle `c_0142`, state `ENRICHING`: builds `MarketSnapshot` (last 50 bars compact, indicators, ATR, spread, regime), `PortfolioState`, `RiskContext` (remaining daily budget, remaining risk budget, correlation of open positions to BTC).
3. `ANALYZING`: Technical Analyst (Haiku) → `TechnicalAssessment(setup_quality=0.65, invalidation=…)`. Context Analyst skipped (no news source in Phase 1).
4. `DECIDING`: Decision Maker (Sonnet) with assessments + active learnings → `OrderIntent(BUY, size_fraction=0.6, stop=…, tp=…, time_limit=240, confidence=0.58, thesis=…, strongest_counter=…, learnings_applied=[H-007])`.
5. `RISK_CHECK`: Risk Engine computes size = 0.6 × min(risk-based, cap-based) → 0.0041 BTC; expectancy after fees positive → approved bracket order.
6. `SUBMITTED` → Alpaca paper → fill stream → `FILLED`; Position Manager takes over.
7. Position closes 3 h later at TP (+1.8R). Journal records trade with fee, LLM cost (0.021 USD), benchmark (BTC +0.4 % over window), thesis, learnings applied.
8. Nightly review (03:00 Europe/Berlin, low-volume hour): Journal Reviewer reads the day's trades, updates evidence on H-007 (now 9 wins / 5 losses, avg +0.6R), proposes H-012 ("breakouts during 14:00–16:00 UTC have lower follow-through"), flags nothing for promotion yet (sample too small).

---

## 6. Strategy Profile (`strategies/<name>.yaml`)

A strategy profile is the "clear strategy" the whole system is set to. Switching profiles resets the active learnings context (learnings are scoped to a profile) but keeps the journal.

```yaml
name: fast_momentum_v1
description: >
  Short-horizon momentum/breakout on liquid crypto. Hold minutes to hours.
  Prefer many small, well-defined trades over conviction bets. Cut losers fast.
universe: [BTC/USD, ETH/USD, SOL/USD]        # start small; liquidity + spread matter
timeframes: { signal: 5m, context: [15m, 1h], trend: 4h }

signal_rules:                                 # deterministic, run by Scanner
  - id: breakout_donchian20_volspike
    expr: "close > donchian_high_20[-1] and vol_z > 1.5 and ema9 > ema21"
    direction: long
    score: "min(1, vol_z / 3)"
  - id: pullback_ema21_uptrend
    expr: "ema21 > ema50 and low <= ema21 and close > ema21 and rsi14 > 45"
    direction: long
    score: "0.5"
  # shorts: only if broker supports crypto shorting (Alpaca spot does not) → long-only in PoC

scanner:
  max_candidates_per_symbol_per_hour: 2
  max_concurrent_cycles: 2
  min_vol_z: 0.5
  max_spread_bps: 15

agents:
  technical: { model: claude-haiku-latest, prompt_version: v1 }
  context:   { enabled: false }
  decision:  { model: claude-sonnet-latest, prompt_version: v1, mode: single }   # single | debate
  reviewer:  { model: claude-sonnet-latest, prompt_version: v1, schedule: "0 3 * * *", tz: Europe/Berlin }
  exit_review: { enabled: false }

risk:                                         # HUMAN-EDITABLE ONLY. Learning loop cannot touch this block.
  risk_per_trade_pct: 1.0                     # of equity, at stop
  max_position_pct: 30
  max_positions: 2
  max_gross_exposure_pct: 60
  max_daily_loss_pct: 3
  max_drawdown_pct: 10                        # pauses trading, manual resume
  min_stop_atr: 0.8
  max_stop_atr: 3.0
  max_hold_min: 480
  max_orders_per_hour: 6
  max_orders_per_day: 30
  max_price_deviation_pct: 1.0
  trailing: { enabled: true, trigger_r: 1.0, trail_atr: 1.5 }

budget:
  llm_usd_per_day: 3.00
  llm_usd_per_cycle_max: 0.10

learning:
  mode: propose_only            # propose_only | auto_promote (Phase 4+)
  min_trades_for_evidence: 20
  promote_if: "p_value < 0.1 and delta_expectancy_r > 0.15"   # vs. baseline window
  max_active_learnings: 12

benchmark: { symbol: BTC/USD, type: buy_and_hold }
```

**Why "fast" is constrained here:** 5m signals, ≤ 8 h holds, 2 concurrent positions, 1 % risk per trade. That is as fast as is sane at 1,000 EUR with ~1 s latency and LLM-in-the-loop. Anything faster is competing with market makers on their terms.

---

## 7. Data model (core tables)

```
strategies        (id, name, yaml, hash, active_from, active_to)
cycles            (id, strategy_id, symbol, rule_id, signal_features json, state, created_at, finished_at, outcome, reject_reason)
agent_calls       (id, cycle_id, agent, model, prompt_version, prompt_hash, input json, output json, tokens_in, tokens_out, cost_usd, latency_ms, ts)
order_intents     (id, cycle_id, json, confidence, thesis, strongest_counter, learnings_applied json)
risk_decisions    (id, cycle_id, approved, reasons json, adjusted json, computed_size, ts)
orders            (id, cycle_id, broker_order_id, mode, type, side, qty, limit_price, stop_price, tp_price, status, submitted_at, updated_at)
fills             (id, order_id, qty, price, fee, ts)
positions         (id, cycle_id, symbol, side, qty, entry_price, stop, tp, time_limit_at, opened_at, closed_at, exit_reason, realized_pnl, fees, mae, mfe, r_multiple, llm_cost_usd, benchmark_return)
bars              (symbol, timeframe, ts, o, h, l, c, v)   -- 1m persisted; others derived
equity_snapshots  (ts, equity, cash, exposure, hwm, daily_pnl, benchmark_equity)
events            (id, ts, type, cycle_id, payload json)    -- append-only
hypotheses        (id, strategy_id, text, kind, status: proposed|active|retired|rejected, created_by_review_id, evidence json, trades_n, win_rate, avg_r, p_value, promoted_at, retired_at)
reviews           (id, strategy_id, period_start, period_end, report json, cost_usd, ts)
control_state     (key, value)   -- paused, paused_until, kill, mode
```

Indexes on `cycles(created_at)`, `positions(closed_at)`, `events(ts)`, `agent_calls(cycle_id)`.

---

## 8. Learning Loop ("gut feeling", done explicitly)

### 8.1 Principle
The system does not fine-tune anything and does not rewrite its own prompts. "Learning" means: maintaining a registry of **hypotheses** with **evidence**, and injecting the **active** ones into agent prompts as a compact, ranked block. Agents are instructed to apply them and to cite which ones influenced a decision (`learnings_applied`), which makes each learning's effect measurable.

### 8.2 Hypothesis lifecycle
```
proposed ──(evidence ≥ min_trades, passes promote_if)──► active ──(evidence degrades)──► retired
    └──(evidence contradicts)──► rejected
```
- **Proposed:** created by the Journal Reviewer (or manually in the dashboard). Has `text`, `kind` (`filter` | `sizing_bias` | `timing` | `exit` | `regime`), and a machine-checkable `applies_to` predicate over signal features where possible (e.g. `hour_utc in 14..16 and rule_id == breakout_*`).
- **Evidence accrual:** nightly, for each hypothesis, the Journal computes outcomes of trades matching `applies_to` vs. non-matching (or vs. the same trades in a baseline window). Stores n, win rate, avg R, a simple significance test (bootstrap on R-multiples is fine; no need for rigor beyond "not obviously noise").
- **Promotion:** in `propose_only` mode (Phases 1–3) a human promotes via dashboard. In `auto_promote` the rule in the profile decides. Either way, promotion is an event with evidence snapshot attached.
- **Retirement:** active hypotheses are re-evaluated nightly; if evidence drops below threshold for 2 consecutive reviews → retired (not deleted; can be re-proposed).
- **Cap:** `max_active_learnings` to keep prompts short and prevent contradiction pile-up. The reviewer is asked to merge/condense when near the cap.

### 8.3 What the reviewer is and is not allowed to do
- May: write postmortems, propose hypotheses, update free-text evidence notes, propose merging/retiring.
- May not: change risk limits, change signal rules, change models, change prompts, place orders. Its output is data, consumed by code that enforces these boundaries.

### 8.4 Active Learnings block (injected into prompts)
```
ACTIVE LEARNINGS (strategy fast_momentum_v1, as of 2026-10-09):
H-007 [filter, n=14, win 64%, avg +0.6R] Breakouts with vol_z > 2.5 on BTC follow through better than ETH/SOL; prefer BTC when multiple fire.
H-009 [timing, n=22, win 31%, avg -0.2R] Signals in 00:00–04:00 UTC underperform; reduce size_fraction to ≤ 0.4.
H-011 [exit, n=18] Trades reaching +1R but closed by time limit average +0.3R; thesis-based early exits not yet validated.
Baseline (last 30d): 41 trades, win 46%, avg +0.21R, expectancy after fees +0.12R.
```

### 8.5 Guardrails against overfitting
- Minimum sample sizes; evidence windows of fixed length; no hypothesis may reference a single trade.
- Every nightly review also reports the **counterfactual**: equity curve if no learnings had been applied (approximated by re-scoring the Decision Maker's confidence without the learnings block on a sample — Phase 4 feature, optional).
- Benchmark comparison is mandatory in every review: strategy vs. buy-and-hold BTC over the same period, net of fees and LLM cost.

---

## 9. Dashboard

### 9.1 Views
1. **Overview:** equity curve vs. benchmark (net of fees and LLM cost as a separate line), today's P&L, drawdown from HWM, open positions with live R, mode badge (PAPER / LIVE, unmistakable colour), system health (feed, broker, last cycle), LLM spend today vs. budget.
2. **Cycles:** live feed of cycles with state; click → full timeline (signal → assessments → intent → risk decision → order → fills), raw agent inputs/outputs, cost, latency.
3. **Positions & Trades:** open positions (with stop/TP/time-left, manual close button), closed trade table with R, MAE/MFE, fees, cost, thesis, outcome tags, filters.
4. **Learning:** hypothesis registry (status, evidence, n, win rate, avg R, p), promote/retire buttons (confirmation), review reports with postmortems, counterfactual equity (if enabled).
5. **Strategy & Risk:** active profile (read-only YAML view), risk limits with edit form (confirmation + reason + logged), signal rule hit stats.
6. **Control:** pause/resume new entries, flatten all, kill-switch (two-step confirm), switch profile, trigger review now, mode switch (paper/live — gated, see §12).

### 9.2 Tech
- FastAPI, REST for queries/actions, WebSocket `/ws/events` tailing the events table. Pydantic schemas shared with core.
- React + TypeScript + Vite + Tailwind; charts with a lightweight library (e.g. Recharts or lightweight-charts for candles). Dark theme default. Mobile-usable overview (you will check this from your phone).
- Auth: single bearer token from env for the PoC (dashboard is bound to localhost / behind Tailscale). Not internet-exposed.

---

## 10. Backtesting & simulation

Not a full backtesting framework. Needed for three things:
1. **Signal rule validation:** run Scanner rules over persisted 1m bars (resampled) and report hit counts, forward returns at 1h/4h/8h, so rules are not pure guesswork before they cost LLM money.
2. **Risk/Position Manager tests:** `SimBroker` replays bars; verifies stops, time exits, sizing, fee accounting.
3. **Cheap end-to-end dry run:** full pipeline with `SimBroker` and agent calls stubbed by recorded responses (`agent_calls` table as fixture). Used in CI.

Replaying *with* live LLM calls over history is possible but costs money and leaks look-ahead via model knowledge; do not treat it as a backtest.

---

## 11. Repository layout

```
auto-trader/
  spec.md                      # this file
  pyproject.toml               # uv / hatch; python 3.12; ruff, mypy, pytest
  docker-compose.yml           # core, api, db (postgres), dashboard (nginx static)
  .env.example
  config.yaml
  strategies/fast_momentum_v1.yaml
  prompts/{technical,context,decision,reviewer}/v1.md
  src/autotrader/
    core/        bus.py, cycle.py (FSM), scanner.py, indicators.py, feed.py, position_manager.py, kill.py, cost.py, config.py
    agents/      runner.py, schemas.py, technical.py, context.py, decision.py, reviewer.py
    risk/        engine.py, limits.py, sizing.py
    broker/      base.py, alpaca.py, sim.py
    journal/     models.py (SQLAlchemy), repo.py, metrics.py, learning.py
    api/         app.py, routes/, ws.py, auth.py
    cli.py       # run core | run api | review-now | backtest-signals | flatten | kill
  dashboard/     # React app
  tests/         unit/, integration/ (SimBroker end-to-end with recorded agent fixtures)
```

---

## 12. Phases

Each phase has a definition of done. Do not start the next phase before the current one's DoD is met.

### Phase 0 — Skeleton (1–2 days)
- Repo, tooling, config loading, DB schema + migrations (alembic), event bus, structured logging, kill-switch, `SimBroker`, Alpaca paper adapter with account/positions read.
- DoD: `cli run core` starts, connects to Alpaca paper, streams bars for the universe, persists 1m bars, responds to `KILL` file. Tests green.

### Phase 1 — Deterministic loop without LLM (2–3 days)
- Indicators, Scanner with rule engine, Risk Engine, Position Manager, Cycle Runner with a **stub Decision Maker** (rule-based: BUY with fixed stop/TP if score > threshold).
- DoD: paper trades flow end to end; every cycle fully audited in DB; signal backtest CLI reports forward returns per rule; Risk Engine has 100 % branch coverage in unit tests; reconciliation works.

### Phase 2 — Agents (2–3 days)
- Agent runner with structured outputs, cost meter, Technical Analyst + Decision Maker, prompts v1, budget enforcement, recorded-fixture tests.
- DoD: real LLM decisions in paper mode; per-cycle cost visible; daily cost < budget over 3 consecutive days; no cycle exceeds `llm_usd_per_cycle_max`.

### Phase 3 — Journal & Dashboard (3–5 days)
- Metrics (R, MAE/MFE, benchmark, cost attribution), equity snapshots, API, dashboard views 1–3 and 6, WebSocket live feed.
- DoD: you can watch a cycle happen live, see cost and P&L, pause/flatten/kill from the UI, from your phone.

### Phase 4 — Learning Loop (3–5 days)
- Journal Reviewer, hypothesis registry, evidence computation, active-learnings injection, dashboard views 4–5, `propose_only` mode.
- DoD: 2 weeks of nightly reviews; at least one hypothesis with n ≥ 20; promote/retire works and is reflected in the next day's prompts; `learnings_applied` is populated and attributable.

### Phase 5 — Live gate
Preconditions, all required:
- ≥ 4 weeks paper trading on the final profile with zero Risk Engine bypasses and zero reconciliation mismatches lasting > 5 min.
- Paper expectancy after fees **and LLM cost** > 0 over the window (not required to beat benchmark — but if it loses to buy-and-hold by a wide margin, reconsider the whole thing honestly).
- LLM cost < 1 % of equity per day.
- Alpaca live crypto verified available for German residents and account funded; otherwise Kraken adapter implemented and paper-tested for ≥ 2 weeks.
- Runtime moved to an always-on host (Hetzner VPS or similar) with Docker Compose, Postgres, backups of the DB, alerting (at least: feed down, broker error, daily loss limit, drawdown pause, kill triggered) via a push channel (ntfy/Telegram/Slack).
- Live risk limits set *tighter* than paper for the first 2 weeks (e.g. `risk_per_trade_pct: 0.5`, `max_positions: 1`), with `kill.flatten = true`.
- Mode switch to live requires: env var `ALLOW_LIVE=1`, dashboard two-step confirm, and a logged `ModeChanged` event with operator note.

---

## 13. Observability & alerting

- Metrics (Prometheus endpoint, optional for PoC): cycles by state, LLM cost, latency per agent, feed lag, open positions, equity, drawdown.
- Alerts (required before live): feed stale > 2 min, broker API errors > 3/min, daily loss limit hit, drawdown pause, kill-switch triggered, reconciliation mismatch, LLM budget exceeded, process restart.
- All alerts also appear in the dashboard event feed.

---

## 14. Security

- API keys only via env; `.env` git-ignored; separate paper and live keys; live keys never on the laptop once live runs on the VPS.
- Dashboard not internet-exposed (localhost / Tailscale). Bearer token.
- Prompt injection: the Context Analyst (when enabled) consumes third-party text. Its output schema is narrow (numbers + enums + short strings) and it has no tool access; the Decision Maker treats it as data. No agent output is ever executed or used to construct prompts for other agents without schema validation.
- Dependency pinning, `pip-audit` in CI.

---

## 15. Testing

- Unit: indicators (against known values), rule engine, Risk Engine (every branch, property-based tests for sizing invariants: size never exceeds caps, never negative, stop always on correct side), Position Manager (stop/time/trail), cost meter, hypothesis evidence math.
- Integration: full cycle with `SimBroker` and recorded agent fixtures; kill-switch mid-cycle; broker error mid-submit; reconciliation mismatch.
- Contract: pydantic schemas for every agent output; a malformed LLM response must fail the cycle cleanly, never crash the process.
- Smoke (manual, per phase): Alpaca paper connectivity.

---

## 16. Open questions / to decide later

- News source for the Context Analyst (none in Phase 1; candidates: CryptoPanic API, Alpaca News API). Measure whether it moves expectancy before paying for it.
- Whether `debate` mode (Bull/Bear) ever earns its cost. Add as an experiment behind a flag in Phase 4 if curiosity wins.
- Exit review by LLM (default off). Same: measure.
- Shorts: not possible on Alpaca crypto spot. If the strategy needs them, that is a broker change (Kraken margin) and a Risk Engine extension — not a PoC concern.
- Tax: German private crypto sales are taxable if held < 1 year (§23 EStG, 1,000 EUR exemption limit on total private sale gains per year). The journal's fill export must be sufficient for a tax tool. Not legal advice — verify.

---

## 17. Glossary

- **R / R-multiple:** P&L expressed in units of initial risk (distance entry→stop × size). +2R = won twice the amount risked.
- **MAE / MFE:** maximum adverse / favourable excursion during a trade.
- **HWM:** high-water mark of equity.
- **Cycle:** one signal → decision → (order) pass through the pipeline.
- **Hypothesis / Learning:** a proposed or validated rule of thumb with attached evidence, scoped to a strategy profile.
- **Strategy Profile:** YAML defining universe, rules, agents, risk limits, budget, learning policy.
