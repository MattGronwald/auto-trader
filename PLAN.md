# Implementation Plan

Derived from `SPEC.md` (Draft v1, 2026-10-09). The spec stays the source of truth; this file covers sequencing, work packages, and spec gaps that must be closed before or during implementation.

**Current repo state:** WP 0.1–0.5 done — uv project, package skeleton, typer CLI (`check-config`, `db upgrade`, `kill` work, rest are stubs), ruff/mypy/pytest, GitHub Actions CI; config/profile loading with the G7 expression evaluator (`core/config.py`, `core/expr.py`); journal schema for all §7 tables + initial Alembic migration + repo for strategies/bars/events/control_state (`journal/`); event bus with persist-then-deliver (`core/bus.py`) and JSON logging with `cycle_id` (`core/logging.py`); kill-switch + `ControlGate` (`core/kill.py`). Next: WP 0.6.

---

## 1. Spec gaps and contradictions (resolve before/while coding)

Ordered by how badly they bite if ignored.

| # | Issue | Where | Proposed resolution |
|---|---|---|---|
| G1 | **Exit orders vs. "Risk Engine is the only broker caller".** Position Manager (stops, time exits), kill (`close_all`) and dashboard flatten all need to submit orders. If they go through the full `check()`, a stop-out can be blocked by `max_orders_per_hour` or the daily loss pause. | §0.1, §4.5, §4.7 | Risk Engine gets two entry points: `check(OrderIntent)` for entries (all 11 checks) and `exit(ExitRequest)` for reduce-only orders (only checks: symbol has a position, qty ≤ position qty, mode matches). Exits are never rate-limited or paused, but still logged as `risk_decisions`. Risk Engine remains the sole holder of the `Broker` reference. |
| G2 | **Alpaca crypto likely has no bracket/OCO orders.** Then every stop is client-side, enforced by the Position Manager. A sleeping laptop = no stop, even in paper. | §4.5 last para, §4.6 | Verify in Phase 0. Assume client-side stops as the default path. Treat "core process down" as a first-class risk: alert + reconcile on restart, and run paper on an always-on host earlier than Phase 5 if overnight runs matter. |
| G3 | **Expectancy check has no prior in Phase 1/2.** `p_win_est` is capped at "historical win rate + 10 pp" — no history exists at start. Units are also mixed (price distance vs. fee). | §4.5 #11 | Add `risk.default_win_rate_prior` (e.g. 0.45) used until `n ≥ min_trades_for_evidence`. Compute everything in quote currency for the computed qty: `qty·(tp−entry)·p − qty·(entry−stop)·(1−p) − fee(entry) − fee(exit) > 0`. Note: this lives in the human-only `risk` block. |
| G4 | **Sizing formula is ambiguous** (`size_fraction × that` inside a `min`). | §4.5 #7 | `qty = size_fraction × min(risk_qty, cap_qty, broker_max_qty)` → round down to lot size → reject if `< broker_min`. Property tests encode exactly this. |
| G5 | **Timeframe mismatch.** Profile uses `trend: 4h`; feed only maintains 1m/5m/15m/1h. Alpaca crypto stream delivers 1m bars only. | §4.1, §6 | Feed resamples from 1m to all timeframes listed in the profile (derived set, not hard-coded). Backfill depth = max indicator lookback × largest TF (≈ 50 × 4h ≈ 9 days of 1m bars). |
| G6 | **"Day" undefined for a 24/7 market.** Daily loss limit, daily budget, orders/day, reviewer schedule. | §4.5 #3, §6 | One config key `trading_day_tz` (default `UTC`), boundary at 00:00. Reviewer runs at 03:00 Europe/Berlin as specified, independently. |
| G7 | **Unsafe expression evaluation.** `signal_rules[].expr`, `score`, `promote_if`, `applies_to` are strings. `eval()` is not acceptable. | §6, §8.2 | One shared, whitelisted AST evaluator (names = feature dict, ops = arithmetic/compare/bool/`min`/`max`/`abs`, `[-n]` = lag access, `in a..b` for ranges). Validate all expressions at profile load, fail fast. **Done in 0.2** (`core/expr.py`): names checked against vocabularies in `core/features.py`; `**`/`%` excluded (DoS); numbers must be finite; statically wrong result types (predicate vs. score) rejected at load. Glob matching (`rule_id == breakout_*`) is not supported yet — add in 4.1 if `applies_to` needs it. |
| G8 | **Kill file across containers.** Core and API are separate compose services; `touch KILL` must be visible to both. | §4.11, §11 | `KILL` lives in a shared volume (`/data/control/KILL`); path from config. Core additionally polls `control_state` in DB. |
| G9 | **Model IDs.** `claude-haiku-latest` / `claude-sonnet-latest` are not real aliases. The price table must match exact IDs. | §6 | Pin exact model IDs in the profile; price table keyed by model ID in `config.yaml`, versioned. Verify current IDs/prices at Phase 2 start. **0.2:** profile pins `claude-haiku-5-5` / `claude-sonnet-5-5`; IDs containing `latest` are rejected at load. Price table deferred to 2.1. |
| G10 | **Alpaca crypto fee mechanics.** Fees on buys may be deducted from the received asset qty → position qty ≠ ordered qty → false reconciliation mismatches. | §4.6, §4.7 | Verify in Phase 0; reconciliation compares against broker-reported qty, Position Manager adopts broker qty after fill. |
| G11 | **Stale data + client-side stops.** "Position Manager still runs on last known price" means the stop is effectively off while stale. | §4.1 | Stale > N s with an open position → alert; stale > M s → market-exit via `exit()` (configurable, default on in live). |
| G12 | File name: layout says `spec.md`, repo has `SPEC.md`. | §11 | Keep `SPEC.md`. Trivial. |

---

## 2. Tech baseline (Phase 0 decisions)

- Python 3.12, **uv** for env/lock, `src/` layout, ruff + mypy (strict on `risk/`, `broker/`, `core/`), pytest + hypothesis + pytest-asyncio, coverage gate on `risk/` = 100 % branches.
- SQLAlchemy 2.x async (`aiosqlite` dev, `asyncpg` prod), Alembic with `render_as_batch=True` (SQLite ALTER limitations). JSON columns via SQLAlchemy `JSON` type (portable).
- Pydantic v2 for all domain types; shared between core, API, and agent schemas.
- structlog JSON, `cycle_id` bound via contextvars.
- `alpaca-py` for REST + WS; `anthropic` SDK directly, no agent framework.
- Money/qty: `Decimal` in Risk Engine, broker adapter and journal; floats only inside indicator math.
- Single asyncio process for core; API as separate process reading the same DB and writing `control_state` + kill file.
- CI: GitHub Actions — ruff, mypy, pytest (unit + SimBroker integration with recorded fixtures), `pip-audit`.

---

## 3. Work packages per phase

Each WP ≈ one PR. DoD per phase is from SPEC §12 and is not repeated in full.

### Phase 0 — Skeleton

| WP | Content |
|---|---|
| 0.1 | `pyproject.toml`, uv lock, ruff/mypy/pytest config, CI workflow, `.env.example`, `src/autotrader/` package skeleton, `cli.py` (typer) with stub commands. |
| 0.2 | Config: `config.yaml` + `strategies/fast_momentum_v1.yaml` → pydantic models; profile hash; expression evaluator (G7) with validation at load. |
| 0.3 | DB: SQLAlchemy models for all §7 tables, Alembic initial migration, repo layer. **Done:** money/qty columns are `Money` (exact Decimal; text on SQLite, NUMERIC(28,12) on Postgres), timestamps `UTCDateTime` (naive rejected); SQLite runs with `foreign_keys=ON` + WAL. Repo covers Phase 0 tables only; other tables get repo functions in their WP. Additions vs. §7: `risk_decisions.kind` (entry/exit, G1); `cycle_id` nullable on agent_calls/risk_decisions/orders/positions (reviewer calls, kill/flatten exits). |
| 0.4 | Event bus (asyncio pub/sub + append-only `events` persistence), structlog setup. **Done:** events persist before delivery (persist failure → `publish` raises, nothing delivered); one queue + worker per subscriber (slow handler can't block others, failing handler logged + skipped); class-based subscriptions; handler logs carry the event's `cycle_id`. Concrete event types are added by the WP that emits them. |
| 0.5 | Kill-switch (file + `control_state`), checked by a `ControlGate` used by scanner and Risk Engine. **Done:** either trigger kills; gate blocks entries on kill / `paused` / future `paused_until` and fails closed (unreadable state, malformed `paused_until`); exits stay allowed (G1). `autotrader kill` writes the file first (works without DB), `kill --clear` lifts it; `KillTriggered`/`KillCleared` events. Acting on a kill (cancel orders, `kill_flatten`) is 1.5. |
| 0.6 | `Broker` protocol, domain types (`BrokerOrder`, `Fill`, `Position`, `Account`, `FeeSchedule`), `SimBroker` (next-bar-open fill, slippage + fee model). |
| 0.7 | `AlpacaBroker` paper: account/positions read; **verification spike** for G2 (order types for crypto), G10 (fee mechanics), current fee tiers, min notional / lot sizes. Results documented in `docs/alpaca-notes.md`. |
| 0.8 | Market data feed: REST backfill, WS 1m bars, persist to `bars`, resample (G5), stale detection, `BarClosed` events. |

**Blocker for 0.7/0.8:** Alpaca paper API keys in env, and outbound network access to `*.alpaca.markets` from wherever this runs.

### Phase 1 — Deterministic loop

| WP | Content |
|---|---|
| 1.1 | Indicators (EMA, RSI, ATR, VWAP, vol z, Bollinger, Donchian) — vectorised over pandas/numpy, tested against reference values. |
| 1.2 | Signal Scanner: rule evaluation, pre-filters, rate limits, `CandidateSignal`. |
| 1.3 | Risk Engine: `check()` with all 11 checks + `exit()` (G1, G3, G4). 100 % branch coverage, hypothesis property tests for sizing invariants. Add the CI gate here (`coverage report --include='src/autotrader/risk/*' --fail-under=100`); it cannot run in 0.1 because an empty package yields no coverage data. |
| 1.4 | Cycle Runner FSM with persisted transitions, per-state timeouts; stub Decision Maker (rule-based). |
| 1.5 | Position Manager: client-side stop, time exit, trailing, fill handling, reconciliation every 60 s → pause on mismatch. React to a kill: cancel open orders, flatten via `exit()` if `AppConfig.kill_flatten`. |
| 1.6 | Alpaca order submission + fill stream (`stream_fills`), order status sync. Add `fills.broker_fill_id` (unique) so a stream reconnect cannot double-count fills. |
| 1.7 | `backtest-signals` CLI: rule hits + forward returns at 1h/4h/8h on persisted bars. |
| 1.8 | Integration tests: full cycle on SimBroker, kill mid-cycle, broker error mid-submit, reconciliation mismatch. |

### Phase 2 — Agents

| WP | Content |
|---|---|
| 2.1 | Cost meter wrapping the Anthropic client; price table (add to `config.yaml` + `AppConfig`, keyed by exact model ID, G9); per-call/per-cycle/daily aggregates; hard budget → scanner stops emitting. |
| 2.2 | Agent runner: template rendering, tool-use structured output, pydantic validation, persistence to `agent_calls`, timeouts; malformed output fails the cycle, never the process. |
| 2.3 | Technical Analyst + Decision Maker, `prompts/*/v1.md`, prompt hash/version stored per call. |
| 2.4 | Recorded-fixture replay (agent calls served from `agent_calls`) for CI and dry runs. |
| 2.5 | Per-cycle cost cap enforced before each call (`llm_usd_per_cycle_max`). |

### Phase 3 — Journal & Dashboard

| WP | Content |
|---|---|
| 3.1 | Metrics: R-multiple, MAE/MFE, holding time, fee + LLM cost attribution, benchmark return per window; equity snapshots. |
| 3.2 | FastAPI: bearer auth, REST read endpoints, control actions (pause/resume/flatten/kill), `/ws/events`. |
| 3.3 | Dashboard scaffold: Vite + React + TS + Tailwind, dark theme, mobile-first Overview. |
| 3.4 | Views: Overview, Cycles (timeline drill-down), Positions & Trades, Control. |
| 3.5 | Docker Compose: core, api, postgres, dashboard (nginx); shared control volume (G8). Add `asyncpg` and a CI job with a Postgres service running migrations + `tests/unit/journal` (so far only SQLite is exercised; Postgres DDL is checked by compile only). |
| 3.6 | Fill export (CSV) for tax tooling. |

### Phase 4 — Learning Loop

| WP | Content |
|---|---|
| 4.1 | Hypothesis registry + lifecycle, `applies_to` predicates via the shared evaluator (needs its own name vocabulary in `core/features.py`; glob matching if wanted). |
| 4.2 | Evidence computation (matching vs. non-matching, bootstrap p-value on R). |
| 4.3 | Journal Reviewer agent + scheduler (03:00 Europe/Berlin); output consumed by code that enforces §8.3 boundaries. Replace the hand-rolled `_check_cron` in `core/config.py` with the chosen scheduler's parser so load-time validation matches runtime semantics. |
| 4.4 | Active Learnings block injection, `learnings_applied` attribution, cap handling. |
| 4.5 | Dashboard views Learning + Strategy & Risk (risk edit with confirmation + reason + event). |

### Phase 5 — Live gate

Not code-first. Checklist from SPEC §12 plus: VPS + Compose + Postgres backups, alerting channel (ntfy is the cheapest option), `ALLOW_LIVE` + two-step confirm + `ModeChanged` event, tighter live risk limits. Kraken adapter only if Alpaca live crypto is unavailable for German residents.

---

## 4. Critical path and estimates

```
0.1 → 0.2 → 0.3 → 0.4/0.5 → 0.6 → 0.7 (spike!) → 0.8
                                   │
1.1 → 1.2 ─┐                       ▼
1.3 ───────┼→ 1.4 → 1.5 → 1.6 → 1.8      (1.7 parallel after 1.1)
           │
2.1 → 2.2 → 2.3 → 2.4/2.5
3.x (3.1 first, dashboard parallel to 3.2)
4.x
```

- Dev effort per spec: ~11–18 days for Phases 0–4. Realistic with interruptions: 4–6 weeks.
- Calendar is dominated by **observation windows**, not coding: 3 days (Phase 2 DoD) + 2 weeks nightly reviews (Phase 4) + 4 weeks paper on final profile (Phase 5). Earliest live: ~2.5–3 months from start.
- The 0.7 spike is the highest-information task — it decides whether G2/G10 change the Position Manager design. Do it early.

---

## 5. Decisions (2026-10-09)

| # | Topic | Decision |
|---|---|---|
| D1 | Runtime | Laptop (macOS) for Phases 0–3; VPS before Phase 5 (and earlier if overnight paper runs need to be trustworthy, see G2). |
| D2 | Secrets | Laptop: `.env` in repo root (git-ignored), template in `.env.example`. Variables: `ALPACA_API_KEY_ID`, `ALPACA_API_SECRET_KEY`, `ALPACA_PAPER=true`, `ANTHROPIC_API_KEY` (from Phase 2), `DASHBOARD_TOKEN` (from Phase 3). Loaded via pydantic-settings; real env vars override `.env`. Separate key pairs for paper and live; live keys never on the laptop. |
| D3 | G1 | Accepted: Risk Engine has `check()` for entries and `exit()` for reduce-only orders. |
| D4 | G3 | `risk.default_win_rate_prior: 0.40`, used until `n ≥ learning.min_trades_for_evidence`. With the +10 pp cap, `p_win_est ≤ 0.50` initially → effectively requires reward:risk ≳ 1.6 after fees. Conservative on purpose. |
| D5 | G6 | `trading_day_tz: UTC`, boundary 00:00 UTC (= 01:00/02:00 Berlin). Matches Alpaca bar timestamps, no DST jumps. |
| D6 | G11 | `feed.stale_alert_s: 60`; `feed.stale_exit_s: 300` with `feed.stale_exit_enabled: false` in paper, `true` in live. |
| D7 | Laptop mitigations | On core startup: reconcile first, then enforce overdue stops/time exits immediately. Optional `shutdown.flatten: false` (paper) for graceful stops. Run under `caffeinate -i` while plugged in. |
| D8 | Tooling | uv + typer (confirmed). |
| D9 | Dashboard auth | Bearer token + localhost/Tailscale-only binding (both, per SPEC §9.2/§14). |

**Cloud dev sessions:** outbound access to `*.alpaca.markets` is currently blocked by the environment network policy. Unit/integration tests use `SimBroker` and recorded fixtures; Alpaca smoke tests run on the laptop (or after allowlisting the domain and adding paper keys as environment variables).
