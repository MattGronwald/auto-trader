# Auto Trader — Specification

**Status:** Draft v2 · reviewed against WP 0.1–0.8 and 1.1–1.4 · 2026-10-11
**Owner:** Matthias
**Purpose:** PoC for a fully automated, agent-based crypto trading system. Paper trading first, live trading with a ~1,000 EUR account later. This document defines the product requirements. `PLAN.md` records implementation status, work packages, and owner decisions. Accepted decisions must be reflected here; historical design notes are supporting context, not a separate source of conflicting requirements.

**Implementation boundary:** WP 0.1–0.8 and 1.1–1.4 are implemented. `run core` currently feeds the Scanner and snapshots equity; it does **not** subscribe the Cycle Runner or trade. Position management, recovery of broker orders, and Alpaca order execution remain WP 1.5–1.6. Later-phase requirements below are targets, not claims of existing functionality. See [review findings](docs/spec-plan-review-2026-10-11.md).

---

## 0. Non-negotiables (read first)

These constraints override anything else in this document. If an implementation detail conflicts with one of these, the constraint wins.

1. **No order reaches a broker without passing the deterministic Risk Engine.** The Risk Engine is plain Python, has no LLM in its call path, and cannot be bypassed by any agent, config flag, or dashboard action. It is the only component with a reference to the broker's `submit_order`.
2. **Every LLM call is metered and attributed.** Token count, model, USD cost, cycle id, agent name. Cost is a first-class P&L line item. A cycle that costs more than its expected edge is a bug.
3. **Entry-decision agents are called on signal, not on schedule.** The deterministic Signal Scanner runs continuously and cheaply; Technical, Context, and Decision agents run only for admitted candidate signals. The Journal Reviewer is the explicit scheduled/on-demand exception (§8); it uses the same daily cost budget. Optional exit reviews are position-event driven and disabled by default. There is no "call all agents every 15 minutes" loop.
4. **Learning is hypothesis-driven, gated, and reversible.** The system never silently rewrites its own strategy. Proposed changes are versioned, evaluated against a baseline, and promoted only by explicit rule (or human approval in the first phases).
5. **Live mode requires a passed gate** (see §12). Paper → live is a deliberate, logged transition with a checklist, not a config change.
6. **Kill-switch has an independent file trigger and a control API.** The configured `control.kill_file` (locally `data/control/KILL`) or DB kill state blocks **new entries** within one healthy core tick and is rechecked before submission. Reduce-only exits remain available while killed or paused. Detection cannot recall an in-flight broker request or prevent a late fill: WP 1.5 must cancel outstanding entries and manage/flatten late fills according to `kill.flatten`. The CLI/file path works without the dashboard; the API arrives in Phase 3.

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
| Asset class | Crypto spot | 24/7 spot market, small-account experimentation, volatility suits the proposed short horizon; broker/account/jurisdiction eligibility is checked separately before live | US equities swing (slower), equities intraday (different broker/account restrictions) |
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
- Maintains rolling OHLCV frames from 1m bars for **every timeframe in the profile**, including the 4h trend frame. Computes EMA 9/21/50, RSI 14, ATR 14, VWAP, volume z-score, Bollinger and Donchian 20 using PLAN D12 definitions. Backfill covers the largest indicator lookback/timeframe and a complete trading day for anchored VWAP (PLAN G5/D12).
- Persists 1m bars to DB for backtesting and journal replay.
- Emits `BarClosed(symbol, timeframe)` events.
- Validates bars and quotes before use (added 2026-10-12, WP 1.R1a and its bar follow-up). A bar with a non-finite OHLCV value, a non-positive price, negative volume or high < low is dropped from every source (stream, REST verification, backfill including stored rows, REST latest bar). It is not persisted, resampled or announced, and it does not refresh price age. A bad REST bar never replaces a valid stream bar. Open/close outside [low, high] is not checked.
- Health separates stream connectivity from per-symbol price age (PLAN D11). Refresh latest bar/quote via REST before declaring a symbol stale; Scanner suppresses stale symbols. The Position Manager alerts for stale held symbols and applies `feed.stale_exit_s` / `feed.stale_exit_enabled` (default off in paper, on in live); a stale price is not proof that a stop is safe.
- Higher-timeframe windows normally use REST-verified bars; empty buckets are flat-filled. The current feed can close a window unverified after 60 s of REST failures. WP 1.7 must report missing/synthetic/unverified data limitations rather than interpret every bucket as an observed trade. The current Scanner rejects a 1m signal timeframe (PLAN D13).

### 4.2 Signal Scanner (deterministic, no LLM)
- Subscribes to `BarClosed`. Runs the strategy profile's **signal rules** (see §6) on every closed bar.
- Emits `CandidateSignal(symbol, direction, rule_id, features: dict, score: float)`.
- Candidate rate limit: max N per symbol in a rolling 60 minutes, counted from persisted candidate events. Scanner cycle/position checks are soft pre-filters; the Cycle Runner authoritatively enforces max M open cycles and one open cycle per symbol at admission (PLAN G14). Daily/per-cycle LLM budgets are enforced separately in Phase 2.
- Pre-filters: no signal if symbol stale, if spread > max_spread_bps, if volume z-score < threshold, if symbol already has an open position or open cycle, if daily loss limit hit, if kill-switch set.

### 4.3 Cycle Runner (deterministic FSM)
States: `NEW → ENRICHING → ANALYZING → DECIDING → RISK_CHECK → SUBMITTED → (FILLED | REJECTED | EXPIRED | ERROR)`.

- At most one cycle per delivered, admitted `CandidateSignal`; stale, gated, or excess candidates are dropped with a reason. This depends on the current in-process, non-replaying event bus. Durable replay would require a unique candidate identity before it can be enabled. Every state transition and its `CycleTransitioned` event are committed atomically; event-specific data lives in `details` inside the persisted event payload.
- Pre-order enrichment/agent steps have bounded timeouts (default 30 s → `ERROR`). `RISK_CHECK` has no outer per-step timeout or automatic retry: the engine bounds broker/gate I/O, and an interrupted submission may already exist at the broker. Shutdown and reconciliation follow PLAN D15.
- Phase 2 reserves a conservative maximum call cost **before** each request, atomically against both remaining daily and per-cycle budgets, including concurrent calls. Insufficient budget → cycle `EXPIRED` with reason `budget`. Set a bounded output-token limit, account for all billable token classes/retries, settle actual usage afterward, and retain uncertain charges until resolved. Merely checking spend so far cannot enforce a hard cap.
- *Note 2026-10-11 (WP 1.4, PLAN D15):* a stub/agent skip ends the cycle `REJECTED` (no SKIPPED state). `RISK_CHECK` and `SUBMITTED` have no timeout; only a bounded forced shutdown cancels `check()`, preserving recovery evidence because the order row is written before submit; this does not prove broker execution stopped. After submission only proven outcomes are mapped (`filled` → `FILLED`; `rejected`/`not_submitted` → `REJECTED`); `new`/`unknown`/`canceled` stay `SUBMITTED` until reconciliation (WP 1.5), which will set `EXPIRED` only on a filled qty of 0 and `FILLED` on any fill. Orders are reconciled by order row, not by cycle state.

### 4.4 Agents
All agents share a common runner: build prompt from template + context → call Anthropic API with tool-use/structured output → validate with pydantic → persist request/response/tokens/cost → return typed object. No agent has side effects on the broker or DB beyond its own log.

Every agent receives the **Strategy Profile summary** and the **Active Learnings** block (§8.4) in its system prompt.

| Agent | Model (default) | Input | Output (pydantic) | Cost target |
|---|---|---|---|---|
| **Technical Analyst** | Haiku-class | Signal features, last 50 bars (compact table), indicators, regime stats | `TechnicalAssessment{setup_quality: 0–1, trend_alignment: enum, volatility_regime: enum, key_levels: [..], invalidation_price, notes}` | < 0.005 USD |
| **Context Analyst** | Haiku-class | Headlines last 24 h for the asset (if a news source is configured), funding rate, BTC correlation, time-of-day/weekend flags, upcoming known events | `ContextAssessment{sentiment: -1..1, event_risk: enum, notable_items: [..], confidence}` | < 0.005 USD |
| **Decision Maker** | Sonnet-class | Both assessments + portfolio state + risk context + active learnings | `OrderIntent{action: BUY/SKIP, symbol, size_fraction: (0,1] for BUY, stop_price, take_profit_price, time_limit_min, confidence: 0–1, thesis: str, strongest_counter: str, learnings_applied: [ids]}` | < 0.03 USD |
| **Journal Reviewer** | Sonnet/Opus-class | Closed trades since last review, outcomes vs. thesis, equity vs. benchmark, cost, current hypotheses | `ReviewReport{trade_postmortems: [..], new_hypotheses: [..], hypothesis_evidence_updates: [..], promote: [ids], retire: [ids], summary}` | < 0.50 USD/day |

Notes:
- Phase 1 uses the deterministic stub only; `ANALYZING` is recorded as skipped. Phase 2 runs the Technical Analyst and Decision Maker. The Context Analyst remains disabled without a configured news source; skipping it does not skip the Technical Analyst. Funding/news inputs require a real configured source; do not invent them.
- `size_fraction` is a *request*; the Risk Engine computes the actual size. The LLM never sees or sets absolute quantities.
- The Phase 2 agent schema is mapped to the implemented `EntryIntent(cycle_id, symbol, stop, take_profit, confidence, size_fraction)`; SKIP never calls the Risk Engine. The engine owns quantity and entry price (marketable IOC limit, PLAN D14). Hold time/thesis/learnings are durable position-management and journal metadata, not broker instructions. Long-only entries and full reduce-only exits are the PoC scope; optional exit proposals use the Position Manager → `exit()` path.
- Decision Maker prompt must require `strongest_counter` to be non-empty and `confidence` to be calibrated against the learnings block ("if learnings say setups like this won 35 % of the time, your confidence must reflect that").
- All prompts live in `prompts/<agent>/<version>.md`, versioned; the version used is stored per call.

### 4.5 Risk Engine (deterministic)
The implemented `RiskEngine` in `risk/engine.py` owns the broker, state reads, persistence and submission. Its `check(EntryIntent)` and `exit(ExitRequest)` return an `OrderResult` containing the decision and any order status/ids. The deterministic functions `check_entry` / `check_exit` in `risk/checks.py` are pure; the engine is not. No LLM appears in either path.

Entry checks run in order and persist the first rejection reason. Percentage settings are percentage points (`1.0` means 1%, divided by 100 in formulas); fees in bps are divided by 10,000. Let `e = ceil_to_price_step(ask × (1 + entry_slippage_bps / 10_000))`, `d = e − stop`, `f = taker_bps / 10_000`. Net cash result per unit bought (D16, WP 1.R1b), exiting at price `x` less `s = exit_slippage_bps / 10_000`: `pnl(x) = (1 − f) × x(1 − s)(1 − f) − e` when the buy fee is taken from the coin (`FeeSchedule.buy_fee_asset = base`, Alpaca), or `pnl(x) = x(1 − s)(1 − f) − e(1 + f)` when it is paid in USD (`quote`; SimBroker models both since 1.R2). Let `L = −pnl(stop)`, the estimated net loss per unit at the stop.

1. Kill not set, entries not paused, mode matches the broker. Recheck the gate immediately before submitting.
2. Symbol allowed, price fresh, quote/last trade available, bid/ask/last trade > 0 and bid ≤ ask (crossed rejects; locked is valid), spread computed from that bid/ask ≤ `scanner.max_spread_bps`.
3. `day_start_equity − equity < max_daily_loss_pct / 100 × day_start_equity`; otherwise pause until the next trading day. Day boundary is 00:00 in `trading_day_tz` (default UTC). Baseline is the last snapshot at/before that boundary, or the first afterward on the first day; without history the current account equity seeds the baseline.
4. `hwm − equity < max_drawdown_pct / 100 × hwm`; otherwise manual-resume pause. HWM is the maximum persisted peak, not merely the latest snapshot. Observed new peaks are persisted even when the entry subsequently fails.
5. Count non-dust held symbols **and unresolved entry-order symbols** against `max_positions`; no pyramiding. A terminal/error cycle does not release an unresolved order's reservation. Dust means qty below the broker's current minimum; it remains recorded.
6. `stop < e` and `min_stop_atr × ATR ≤ d ≤ max_stop_atr × ATR`; missing ATR rejects.
7. `qty = floor_to_qty_step(size_fraction × min((risk_per_trade_pct / 100) × equity / L, (max_position_pct / 100) × equity / e, cash / (e × (1 + f))))`. Reject below broker min qty/notional. The decision also records `initial_risk = qty × d` (R stays the price distance) and `est_net_stop_loss = qty × L`. Before WP 1.R1b (2026-10-12) the risk cap divided by `d`; WP 1.6 must use crypto-available `non_marginable_buying_power` rather than raw cash. The broker maximum notional is not currently modelled in `AssetRules` (PLAN G4). With `buy_fee_asset = base` the estimated net qty after the coin fee (fee rounded up to the qty increment) must also be >= min qty (WP 1.R2 final review, 2026-10-13). This covers a full fill only: an IOC that fills a small part of the order can still leave a position below min qty, which WP 1.5 must reconcile and report (PR #21 review, 2026-10-14).
8. Held market value + unresolved entry notional + proposed notional ≤ `max_gross_exposure_pct / 100 × equity`. Excess exposure rejects, never shrinks. Unpriced/stale non-dust holdings reject; cost basis is not a substitute for current value.
9. Entry-order rows in the rolling last 60 min / current trading day stay within `max_orders_per_hour` / `max_orders_per_day`. Failed and uncertain attempts count conservatively; proven `not_submitted` rows and exits do not count.
10. `abs(e − last_trade) ≤ max_price_deviation_pct / 100 × last_trade` and `stop < e < take_profit`.
11. `p = min(confidence, win_rate_est + 0.10)`, using `risk.default_win_rate_prior` until `learning.min_trades_for_evidence` closed trades exist for the strategy. Require `qty × (p × pnl(tp) − (1 − p) × L) > 0` (quote currency), the same cash-flow model as check 7, with taker fees on both legs and the exit slippage allowance applied to both TP and stop (WP 1.R1b, 2026-10-12). It does not guarantee positive realized P&L or include stop gaps/LLM cost; WP 1.7 must evaluate those separately.

Entries are **IOC marketable limits without brackets**. Alpaca crypto has no bracket/OCO support in the recorded WP 0.7 verification. The Position Manager owns stop/TP/time/trailing enforcement (WP 1.5); it must be active before entry admission is enabled.

`exit(ExitRequest(symbol, reason, cycle_id?))` is a full, reduce-only close of the broker-reported non-dust position. It checks position/mode and persists the decision, but never applies entry gates, loss pauses or order-rate limits. Separate entry and exit locks keep an entry from holding up an exit. Cancellation/reconciliation broker operations introduced in WP 1.5 must also remain behind the Risk Engine; neither dashboard nor Position Manager gets a broker reference.

The engine journals `pending` **before** submit/close, records ambiguous outcomes as `unknown`, and records `not_submitted` only when the request is known not to have been sent. A timeout or lost acknowledgement is not proof of failure. WP 1.5–1.6 must reconcile orders independently of cycle state before releasing reservations or retrying exits; see PLAN §3.2.

**Review follow-up (not yet implemented; scope confirmed by owner 2026-10-11, appended 2026-10-12):** PLAN 1.R1 rejects crossed/non-positive quotes, stores REST quote values, ignores out-of-order WS quotes and removes feed access from exits. Today `exit()` shares `_held_positions()` with entries, so a raised feed exception can reject a protective exit. WP 1.6 adds source-specific freshness/snapshot-age validation at the Risk Engine boundary, including prices for held exposure, and blocks re-entry into a symbol with an unresolved close. WP 1.5 owns durable order/fill handoff and local runtime liveness. The current `risk_per_trade_pct` formula limits nominal price-distance loss only; fees and gaps can exceed it. Owner-approved D16 / 1.R1 changes it to estimated net loss at stop including entry/exit fees and **`risk.exit_slippage_bps`, a human-only adverse exit allowance with initial/default value 10 bps** (finite, `0 ≤ value < 10_000`). It is separate from entry slippage. Implement the schema, profile comment/value and resulting profile hash together in 1.R1. PLAN §3.1 specifies fee-denomination-aware cash flows, rounding down and preservation of cash/position/exposure caps; actual gaps can still exceed the estimate. These are required follow-ups, not WP 1.3 behavior today. **Update 2026-10-12 (WP 1.R1a):** quote validation (feed and check 2), REST quote values, out-of-order WS quotes and the feed-free `exit()` are implemented. D16 net-loss sizing remains open for 1.R1b. **Update 2026-10-12 (WP 1.R1b):** D16 is implemented (checks 7 and 11 above; `risk.exit_slippage_bps` is required, with no schema default, and 10 is the profile's value; new profile hash); actual gaps can still exceed the estimate.

`RiskLimits` is loaded from the strategy profile and **cannot be changed by the Learning Loop**. Only a human edits it (dashboard with confirmation, or config file + restart).

### 4.6 Broker Adapter
Interface:
```python
class Broker(Protocol):
    async def get_account(self) -> Account: ...
    async def get_positions(self) -> list[Position]: ...
    async def submit_order(self, order: BrokerOrder) -> OrderAck: ...
    async def cancel_order(self, order_id: str) -> None: ...
    async def get_order(self, order_id: str) -> OrderStatus: ...
    async def close_position(self, symbol: str) -> OrderAck: ...
    async def close_all(self) -> list[OrderAck]: ...
    def stream_fills(self) -> AsyncIterator[Fill]: ...
    def mode(self) -> Literal["paper", "live"]: ...
    def fee_schedule(self) -> FeeSchedule: ...
    async def asset_rules(self, symbol: str) -> AssetRules: ...
```
Implementations: `AlpacaBroker(paper=True|False)`, `SimBroker` (in-process, for backtests and tests; fills at next bar open + slippage model + fee model). Since WP 1.R2 (2026-10-12), SimBroker models Alpaca's coin-denominated buy fee (rounded up to 1e-9), exact-qty full closes with a dust rule, scripted partial fills (FillPlan) and stable fill ids (Fill.fill_id, fee_asset); see docs/superpowers/specs/2026-10-12-simbroker-fees-fills-design.md.

The existing protocol is `broker/base.py`; client-order-id and order-history lookups, durable fill persistence (fill ids exist since 1.R2) and broker I/O deadlines are WP 1.5–1.6 additions. `close_all()` is not an alternate control path: flatten must go through audited Risk Engine exits. `AlpacaBroker` is currently read-only; its order methods are stubs.

Fees and slippage: adapter exposes `fee_schedule()` so the Risk Engine's expectancy check uses real numbers. **Verify Alpaca's current crypto maker/taker tiers before implementing; do not hard-code from memory.**

### 4.7 Position Manager (deterministic)
- Tracks open positions with their stop, TP, time limit, and entry thesis.
- Enforces hard stop, **take profit**, time-based exit (`time_limit_min` from the eventual agent intent, capped by `risk.max_hold_min`), and trailing stop once profit reaches `risk.trailing.trigger_r`, using `risk.trailing.trail_atr`. Full closes go through `RiskEngine.exit()`. Client-side stops are the required path; a broker stop-limit backup is optional and cannot guarantee a fill. WP 1.5 must specify trigger price, same-bar stop/TP ambiguity, and durable hold-time/initial-risk metadata before implementation.
- On fill events: updates position state, writes `TradeEvent`s.
- Optional LLM exit review (`exit_review.enabled`, default **off** in PoC): at +1R or −0.5R, asks the Decision Maker whether the thesis still holds. Costs money; measure before enabling.
- Reconcile on startup **before admission** and at least every 60 s thereafter. Recover every potentially submitted order, including those attached to terminal/error cycles, and adopt positive partial fills immediately into position management. Duplicate/out-of-order fill delivery must not double-count qty, fees or P&L.
- Compare broker positions against fills with an explicit, bounded allowance for documented fee deductions and rounding. Unexplained discrepancies → alert + pause new entries; do not hide missing trades by blindly overwriting local qty. Preserve unresolved order reservations. Apply overdue exits and handle late fills after kill/flatten. PLAN §3.2 defines the remaining acceptance cases.

### 4.8 Journal
Tables (see §7): every admitted cycle, every agent call, every order, every fill, every position, every review, every hypothesis. Nothing is deleted.

Derived views: closed trades with R-multiple, MAE/MFE, holding time, fee, LLM cost attributed to the cycle, benchmark return over the same holding window, "thesis vs. outcome" tags from the reviewer.

### 4.9 Learning Loop
See §8.

### 4.10 Dashboard & API
See §9.

### 4.11 Cross-cutting
- **Event bus:** in-process asyncio pub/sub with append-only persistence before delivery. It is not a durable replay queue. WP 1.4 `publish_tx` commits cycle state and event together; uncertain commits suspend admission until restart/recovery. Database evidence can exist without in-process delivery. The future dashboard tails persisted events by durable id, independently of core subscriptions.
- **Cost meter:** wraps the Anthropic client; per-call tokens and USD (price table in config, versioned); daily and per-cycle aggregates; hard daily budget → scanner stops emitting candidates when exceeded.
- **Kill-switch:** configured `control.kill_file`, resolved relative to `config.yaml` (default `data/control/KILL`), plus DB kill state. Core and API share this path/volume in Compose. Check before **entry** submission; allow cancellations and protective exits while killed. On kill, WP 1.5 cancels outstanding entry orders and optionally flattens (`kill.flatten`, default true in live, false in paper); API/dashboard controls arrive in Phase 3.
- **Single writer and bounded shutdown (WP 1.5a):** `run core` holds an exclusive lock on `<canonical journal path>.lock` for its lifetime, taken before the migration; a second core waits up to `shutdown.timeout_s` + 5 s and fails. Feed ticks, kill polling, a DB-free kill-file check and equity snapshots are independent supervised loops (stalls and `risk.snapshot_overdue` are logged). Every exit cause, startup included, runs one bounded teardown (`shutdown.timeout_s`, exit 1 if forced or failed); a second signal exits 130 immediately; a watchdog thread, never cancelled once armed, exits 70 at timeout + 5 s.
- **Config:** `config.yaml` (infra, API keys via env) + `strategies/<name>.yaml` (strategy profile, §6). Secrets only via environment / `.env` (git-ignored).
- **Logging:** structured JSON (structlog), one line per event, cycle_id in every line.

---

## 5. Cycle walkthrough (happy path)

This is the target Phase 2+ flow. Phase 1 substitutes the deterministic stub and has no LLM cost. WP 1.4 tests reach `SUBMITTED` with SimBroker; fill completion and position management arrive in WP 1.5–1.6.

1. `BarClosed(BTC/USD, 5m)` → Scanner evaluates rules → rule `breakout_donchian20_volspike` fires, score 0.72 → checks rate limits, no open position → emits `CandidateSignal`.
2. Cycle Runner creates cycle `c_0142`, state `ENRICHING`: builds `MarketSnapshot` (last 50 bars compact, indicators, ATR, spread, regime), `PortfolioState`, `RiskContext` (remaining daily budget, remaining risk budget, correlation of open positions to BTC).
3. `ANALYZING`: Technical Analyst (Haiku) → `TechnicalAssessment(setup_quality=0.65, invalidation=…)`. Context Analyst skipped (no news source configured).
4. `DECIDING`: Decision Maker (Sonnet) with assessments + active learnings → `OrderIntent(BUY, size_fraction=0.6, stop=…, tp=…, time_limit=240, confidence=0.58, thesis=…, strongest_counter=…, learnings_applied=[H-007])`.
5. `RISK_CHECK`: Risk Engine computes size = 0.6 × min(risk-based, position-cap-based, cash-based), rounded down; expectancy after fees positive → approved IOC limit entry with client-side stop/TP metadata.
6. `SUBMITTED` → Alpaca paper → fill stream → `FILLED`; Position Manager takes over.
7. Position closes 3 h later at TP (+1.8R). Journal records trade with fee, LLM cost (0.021 USD), benchmark (BTC +0.4 % over window), thesis, learnings applied.
8. Nightly review (03:00 Europe/Berlin; schedule is independent of the UTC accounting day): Journal Reviewer reads the day's trades, updates evidence on active H-007 (now 16 wins / 9 losses, avg +0.6R), proposes H-012 ("breakouts during 14:00–16:00 UTC have lower follow-through"), flags nothing for promotion yet (sample too small).

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
  technical: { model: claude-haiku-5-5, prompt_version: v1 }
  context:   { enabled: false }
  decision:  { model: claude-sonnet-5-5, prompt_version: v1, mode: single }   # single | debate
  reviewer:  { model: claude-sonnet-5-5, prompt_version: v1, schedule: "0 3 * * *", tz: Europe/Berlin }
  exit_review: { enabled: false }

risk:                                         # HUMAN-EDITABLE ONLY. Learning loop cannot touch this block.
  risk_per_trade_pct: 1.0                     # of equity, estimated net loss at stop including fees and exit slippage
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
  entry_slippage_bps: 10                      # D14: marketable IOC limit = ask + this
  exit_slippage_bps: 10                       # D16: adverse stop-exit allowance in net-loss sizing
  default_win_rate_prior: 0.40                # D4: used until n >= learning.min_trades_for_evidence
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

The example matches the current profile schema; its pinned model IDs are repository configuration, not a fresh verification of provider availability. Reverify exact IDs, token accounting and prices before Phase 2. `latest` aliases are rejected.

**Current baseline:** 5m signals, ≤ 8 h holds, 2 concurrent positions and a 1% per-trade risk setting (estimated net loss at stop including fees and exit slippage since WP 1.R1b, 2026-10-12). These are initial parameters, not demonstrated viable economics. PLAN §3.3 evaluates fee friction and SPEC §16.1 proposes alternatives; observed latency and executable prices must be measured rather than assumed.

---

## 7. Data model (core tables)

```
strategies        (id, name, yaml, hash, active_from, active_to)
cycles            (id, strategy_id, symbol, rule_id, signal_features json, state, created_at, finished_at, outcome, reject_reason)
agent_calls       (id, cycle_id, agent, model, prompt_version, prompt_hash, input json, output json, tokens_in, tokens_out, cost_usd, latency_ms, ts)
order_intents     (id, cycle_id, json, confidence, thesis, strongest_counter, learnings_applied json)
risk_decisions    (id, cycle_id, kind, approved, reasons json, adjusted json, computed_size, ts)
orders            (id, cycle_id, broker_order_id, client_order_id, symbol, mode, type, side, qty, limit_price, stop_price, tp_price, status, submitted_at, updated_at, filled_qty, settled_at, position_id, exit_reason)
fills             (id, order_id, broker_fill_id, side, fee_asset, qty, price, fee, ts)
positions         (id, cycle_id, symbol, side, qty, entry_price, stop, tp, time_limit_at, opened_at, closed_at, exit_reason, realized_pnl, fees, mae, mfe, r_multiple, llm_cost_usd, benchmark_return, initial_risk, dust_qty, dust_consumed)
symbol_dust       (symbol, qty, updated_at)   -- dust the broker still holds per symbol (WP 1.5b)
bars              (symbol, timeframe, ts, o, h, l, c, v)   -- 1m persisted; others derived
equity_snapshots  (ts, equity, cash, exposure, hwm, daily_pnl, benchmark_equity)
events            (id, ts, type, cycle_id, payload json)    -- append-only
hypotheses        (id, strategy_id, text, kind, status: proposed|active|retired|rejected, created_by_review_id, evidence json, trades_n, win_rate, avg_r, p_value, promoted_at, retired_at)
reviews           (id, strategy_id, period_start, period_end, report json, cost_usd, ts)
control_state     (key, value)   -- paused, paused_until, kill, mode
```

This is a logical schema summary; `journal/models.py` and migrations define the installed schema (head `0003` at this review). `cycle_id` is nullable for reviewer calls and control-driven exits; broker/client order ids are unique when present. `equity_snapshots.exposure = NULL` means unknown, not zero. WP 1.4 stores stub intent fields in transition details; it does not yet populate the `order_intents` table. WP 1.5 must preserve position exit metadata. **2026-10-15 (WP 1.5b):** migration `0004` adds fill identity (`broker_fill_id`, unique) and fee asset, order settlement (`filled_qty`, `settled_at`, `position_id`, `exit_reason`), position `initial_risk`/`dust_qty`/`dust_consumed` and `symbol_dust`; 1.5b books fills from SimBroker, WP 1.6 adds the Alpaca fill path; WP 3.1 adds fee-activity provenance/reconciliation. Schema presence alone does not mean a component writes it.

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
- **Evidence accrual (Phase 4):** pin the strategy hash, predicate version and evaluation window before measuring; separate the trades that suggested a hypothesis from later evaluation trades. Store the baseline definition and eligible/missing counts. Descriptive matching-vs-nonmatching differences are not proof of causal improvement. Nightly, for each hypothesis, the Journal computes outcomes of trades matching `applies_to` vs. non-matching (or vs. the same trades in a baseline window). Stores n, win rate, avg R, a simple significance test (bootstrap on R-multiples is fine; no need for rigor beyond "not obviously noise").
- **Promotion:** in `propose_only` mode (the initial Phase 4 rollout) a human promotes via dashboard. In `auto_promote` the rule in the profile decides. Either way, promotion is an event with evidence snapshot attached.
- **Retirement:** active hypotheses are re-evaluated nightly; if evidence drops below threshold for 2 consecutive reviews → retired (not deleted; can be re-proposed).
- **Cap:** `max_active_learnings` to keep prompts short and prevent contradiction pile-up. The reviewer is asked to merge/condense when near the cap.

### 8.3 What the reviewer is and is not allowed to do
- May: write postmortems, propose hypotheses, update free-text evidence notes, propose merging/retiring.
- May not: change risk limits, change signal rules, change models, change prompts, place orders. Its output is data, consumed by code that enforces these boundaries.

### 8.4 Active Learnings block (injected into prompts)

Illustrative formatting only; real active entries require a promotion evidence snapshot meeting the sample-size and promotion rules, not merely the summary numbers shown here.

```
ACTIVE LEARNINGS (strategy fast_momentum_v1, as of 2026-10-09):
H-007 [filter, n=25, win 64%, avg +0.6R] Breakouts with vol_z > 2.5 on BTC follow through better than ETH/SOL; prefer BTC when multiple fire.
H-009 [timing, n=22, win 31%, avg -0.2R] Signals in 00:00–04:00 UTC underperform; reduce size_fraction to ≤ 0.4.
H-011 [exit, n=24] Trades reaching +1R but closed by time limit average +0.3R; thesis-based early exits not yet validated.
Baseline (last 30d): 41 trades, win 46%, avg +0.21R, expectancy after fees +0.12R.
```

### 8.5 Guardrails against overfitting
- Minimum sample sizes; evidence windows of fixed length; no hypothesis may reference a single trade.
- An optional Phase 4 diagnostic may re-score recorded contexts without the learnings block using the metered client. Label it a **decision sensitivity analysis**, not a counterfactual equity curve: changed entries, sizes and exits would create a different portfolio path that re-scoring confidence cannot reconstruct. A true portfolio counterfactual requires a separately specified replay experiment and is outside the required PoC.
- Benchmark comparison is mandatory in every review: strategy vs. buy-and-hold BTC over the same period, net of fees and LLM cost.

---

## 9. Dashboard

### 9.1 Views
1. **Overview:** equity curve vs. benchmark (net of fees and LLM cost as a separate line), today's P&L, drawdown from HWM, open positions with live R, mode badge (PAPER / LIVE, unmistakable colour), system health (feed, broker, last cycle), LLM spend today vs. budget.
2. **Cycles:** live feed of cycles with state; click → full timeline (signal → assessments → intent → risk decision → order → fills), raw agent inputs/outputs, cost, latency.
3. **Positions & Trades:** open positions (with stop/TP/time-left, manual close button), closed trade table with R, MAE/MFE, fees, cost, thesis, outcome tags, filters.
4. **Learning:** hypothesis registry (status, evidence, n, win rate, avg R, p), promote/retire buttons (confirmation), review reports with postmortems, decision sensitivity analysis (if enabled).
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
  SPEC.md                      # this file; PLAN.md tracks delivery
  pyproject.toml               # uv / uv_build; python 3.12; ruff, mypy, pytest
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

Each phase has a definition of done. Do not start the next phase before the current one's DoD is met. A merged WP is not evidence of its phase's paper observation gate: record the commit/profile hash, configuration, dates and validation artifacts. Current phase status is in PLAN §3.

### Phase 0 — Skeleton

**Historical v1 estimate:** 1–2 days. Preserved as planning history, not a revised delivery commitment.

- Repo, tooling, config loading, DB schema + migrations (alembic), event bus, structured logging, kill-switch, `SimBroker`, Alpaca paper adapter with account/positions read.
- DoD: `cli run core` starts, connects to Alpaca paper, streams bars for the universe, persists 1m bars, responds to `KILL` file. Tests green.

### Phase 1 — Deterministic loop without LLM

**Historical v1 estimate:** 2–3 days. Preserved as planning history, not a revised delivery commitment.

- Indicators, Scanner with rule engine, Risk Engine, Position Manager, Cycle Runner with a **stub Decision Maker** (rule-based: BUY with fixed stop/TP if score > threshold).

**Original v1 DoD (preserved history):** paper trades flow end to end; every cycle fully audited in DB; signal backtest CLI reports forward returns per rule; Risk Engine has 100 % branch coverage in unit tests; reconciliation works.

**Revised DoD approved by owner 2026-10-11; appended 2026-10-12:**

- DoD: paper entries, partial fills and full exits flow end to end with durable stop/TP/time metadata; every cycle/order is audited; restart, kill/late-fill and duplicate-fill recovery pass; signal backtest CLI reports returns and fee/data-quality diagnostics per rule; Risk Engine retains 100 % branch coverage; unresolved reconciliation discrepancies block new entries. Broker-writing smoke tests require an explicitly selected paper account. WP 1.4 alone does not satisfy this gate.

### Phase 2 — Agents

**Historical v1 estimate:** 2–3 days. Preserved as planning history, not a revised delivery commitment.

- Agent runner with structured outputs, cost meter, Technical Analyst + Decision Maker, prompts v1, budget enforcement, recorded-fixture tests.
- DoD: real LLM decisions in paper mode; per-call/per-cycle costs visible, including rejected/no-trade cycles; daily cost stays within budget over 3 consecutive days; no cycle exceeds `llm_usd_per_cycle_max`. Concurrent-call, retry, timeout/unknown-usage and restart tests prove reservations cannot overspend the cap.

### Phase 3 — Journal & Dashboard

**Historical v1 estimate:** 3–5 days. Preserved as planning history, not a revised delivery commitment.

- Metrics (R, MAE/MFE, benchmark, cost attribution), equity snapshots, API, dashboard views 1–3 and 6, WebSocket live feed.
- DoD: you can watch a cycle happen live, see cost and P&L, pause/flatten/kill from the UI, from your phone.

### Phase 4 — Learning Loop

**Historical v1 estimate:** 3–5 days. Preserved as planning history, not a revised delivery commitment.

- Journal Reviewer, hypothesis registry, evidence computation, active-learnings injection, dashboard views 4–5, `propose_only` mode.
- DoD: 2 weeks of nightly reviews; at least one hypothesis with n ≥ 20; promote/retire works and is reflected in the next day's prompts; `learnings_applied` is populated and attributable.

### Phase 5 — Live gate
Preconditions, all required:
- ≥ 4 weeks paper trading on the final profile with zero Risk Engine bypasses and zero reconciliation mismatches lasting > 5 min. Record profile hash, code/prompt/model versions, active-learning versions, account scale and observation dates. Material changes to strategy parameters, prompts/models, execution or cost policy require a fresh qualifying window; ordinary learning updates under an unchanged approved policy are versioned observations within it. Test economics at the intended ~1,000 EUR scale in the account's quote currency: fixed LLM spend against a $100,000 paper balance does not establish viability for that account.
- Paper expectancy after fees **and LLM cost** > 0 over the window (not required to beat benchmark — but if it loses to buy-and-hold by a wide margin, reconsider the whole thing honestly).
- LLM cost < 1 % of equity per day.
- Alpaca live crypto verified available for German residents and account funded; otherwise Kraken adapter implemented and paper-tested for ≥ 2 weeks.
- WP 3.5 persistent journal account/mode binding, cash-flow discontinuity detection and Postgres ownership/recovery pass their acceptance tests before live.
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
- One core process per journal and one dedicated paper account/journal in Phase 1. WP 1.5a enforces a process-held local single-writer lock; WP 1.5d resolves the previous writer's SQLite work before admitting entries. WP 3.5 / the Phase 5 gate adds persistent account/mode binding, cash-flow discontinuity detection and Postgres ownership/recovery. Postgres locking and query deadlines require real Postgres tests, not only SQLite tests or DDL compilation.

---

## 15. Testing

- Unit: indicators (against known values), rule engine, Risk Engine (every branch, property-based tests for sizing invariants: size never exceeds caps, never negative, stop always on correct side), Position Manager (stop/time/trail), cost meter, hypothesis evidence math.
- Integration: full cycle with `SimBroker` (stub in Phase 1, recorded agent fixtures from Phase 2); crash before/after broker acceptance, partial IOC cancellation, duplicate/out-of-order fills, late fill after kill, uncertain DB commit and restart recovery. Verify broker holdings, durable order/position rows and exposure reservations, not only cycle terminal states.
- Contract: pydantic schemas for every agent output; a malformed LLM response must fail the cycle cleanly, never crash the process.
- Smoke (manual, per phase): Alpaca paper connectivity.

---

## 16. Open questions / to decide later

- News source for the Context Analyst (none in Phase 1; candidates: CryptoPanic API, Alpaca News API). Measure whether it moves expectancy before paying for it.
- Whether `debate` mode (Bull/Bear) ever earns its cost. Add as an experiment behind a flag in Phase 4 if curiosity wins.
- Exit review by LLM (default off). Same: measure.
- Shorts: not possible on Alpaca crypto spot. If the strategy needs them, that is a broker change (Kraken margin) and a Risk Engine extension — not a PoC concern.
- Tax: German private crypto sales are taxable if held < 1 year (§23 EStG, 1,000 EUR exemption limit on total private sale gains per year). The journal's fill export must be sufficient for a tax tool. Not legal advice — verify.

### 16.1 Proposed strategy revision for owner review

Requested by the owner during the 2026-10-11 review. **These are candidate requirements for evaluation, not changes to the active YAML profile.** Current evidence establishes high fee friction and uneven venue coverage, not which strategy will earn a profit. Keep `fast_momentum_v1` as the comparison baseline; create a separately versioned candidate only after approval.

| Setting | Current baseline | Proposed candidate | Rationale / evaluation needed |
|---|---|---|---|
| Universe | BTC/USD, ETH/USD, SOL/USD | BTC/USD initially | Recorded venue coverage is strongest for BTC. Reintroduce ETH/SOL only with adequate quote/bar coverage and executable net results, assessed separately. Concentrating in BTC is not diversification. |
| Signal timeframe | 5m | Compare 15m against 5m; retain 1h context / 4h trend | Larger price moves may better absorb roughly 50 bps round-trip fees. It is a hypothesis, not a reason to assume 15m ATR or trade frequency is sufficient. |
| Nominal per-trade risk setting | 1.0% | 0.5%, with the fee/slippage sizing decision in PLAN D16 / 1.R1 | Lower paper/live pilot exposure while execution and recovery mature. Report both nominal and estimated net loss; market gaps cannot be capped by a client-side stop. |
| Position / gross exposure | 2 positions; 30% each; 60% gross | 1 position; 25% each/gross | Simpler initial operating envelope; assess return and fixed-cost burden at the smaller notional. |
| LLM spend | $3/day; $0.10/cycle | Evaluate $0.50/day; $0.05/cycle | At roughly $1,000, $3/day consumes about 9% of equity per 30 days before trading fees; $0.50/day still consumes about 1.5%. Provider-price verification must establish whether the intended calls fit; if not, reduce call scope/frequency and report the trade-off rather than silently exceed limits. |
| Volume filter | Global `min_vol_z: 0.5` plus rule conditions | Compare the existing filter with a rule-specific filter for pullbacks | The current global filter also suppresses the pullback rule even though that rule has no volume clause. Any variant requires a schema/Scanner change in its own WP, and a recorded ablation; do not assume removing the filter improves results. |

Keep spread limits, loss/drawdown protections and the expectancy rejection intact during comparisons. Compare universe/timeframe/filter variants separately before combining them, using PLAN §3.3's held-out data and explicit execution assumptions. Current 1m OHLCV alone cannot verify historical spreads or IOC fills. Stop/TP/hold-time changes require an additional named experiment; do not widen stops merely to pass the fee check. A result with too few eligible trades remains inconclusive. No positive-expectancy or live-readiness claim follows from these proposals.

---

## 17. Glossary

- **R / R-multiple:** P&L expressed in units of initial risk (distance entry→stop × size). +2R = won twice the amount risked.
- **MAE / MFE:** maximum adverse / favourable excursion during a trade.
- **HWM:** high-water mark of equity.
- **Cycle:** one signal → decision → (order) pass through the pipeline.
- **Hypothesis / Learning:** a proposed or validated rule of thumb with attached evidence, scoped to a strategy profile.
- **Strategy Profile:** YAML defining universe, rules, agents, risk limits, budget, learning policy.
