# CLAUDE.md

Automated, agent-based crypto trading PoC (Alpaca paper first, live later with ~1,000 EUR).

## Source of truth
- `SPEC.md` — what to build. Section 0 ("Non-negotiables") overrides everything else.
- `PLAN.md` — sequencing, work packages (WP x.y ≈ one PR), resolved spec gaps (G1–G12), owner decisions (D1–D9). Check it before starting a WP; update it when a decision changes.
- Do not start a phase before the previous phase's DoD (SPEC §12) is met.

## Hard rules (from SPEC §0, condensed)
- Only `risk/engine.py` holds a `Broker` reference. Entries via `check()`, reduce-only exits via `exit()` (G1/D3). No LLM in that call path.
- Every LLM call goes through the cost meter (tokens, model, USD, cycle id, agent).
- LLM agents run on scanner signals only — never on a timer.
- The learning loop never touches the `risk` block, signal rules, models or prompts.
- No `eval()`/`exec()` on config strings; use the shared expression evaluator (G7).
- Secrets only via env / `.env` (git-ignored). Never commit keys, never log them.

## Stack & conventions
- Python 3.12, uv (`uv sync`, `uv run ...`), typer CLI (`autotrader ...`), src layout `src/autotrader/`.
- pydantic v2 domain types; `Decimal` for money/qty outside indicator math.
- SQLAlchemy 2 async + Alembic (`render_as_batch=True`); SQLite dev, Postgres prod.
- structlog JSON logs with `cycle_id` bound via contextvars.
- ruff + mypy (strict on `core/`, `risk/`, `broker/`) + pytest/hypothesis. `risk/` requires 100 % branch coverage.
- Tests never hit Alpaca or Anthropic; use `SimBroker` and recorded agent fixtures. Real-API smoke tests are opt-in (marker `smoke`).
- Verify broker/LLM facts (order types, fees, model IDs, prices) against current docs — don't hard-code from memory.

## Commands
To be filled in by WP 0.1 (expected: `uv sync`, `uv run pytest`, `uv run ruff check`, `uv run mypy src`, `uv run autotrader --help`).
