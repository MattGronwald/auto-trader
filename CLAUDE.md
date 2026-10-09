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
- No `eval()`/`exec()` on config strings; use the shared expression evaluator (`core/expr.py`, G7). New expression names go into `core/features.py`.
- Secrets only via env / `.env` (git-ignored). Never commit keys, never log them.
- Before any new entry, ask `ControlGate.status()` (`core/kill.py`); it fails closed. Kill/pause block entries only — reduce-only exits must stay possible.

## Stack & conventions
- Python 3.12, uv (`uv sync`, `uv run ...`), typer CLI (`autotrader ...`), src layout `src/autotrader/`.
- pydantic v2 domain types; `Decimal` for money/qty outside indicator math. DB columns: `Money` / `UTCDateTime` from `journal/types.py` (floats and naive datetimes are rejected on write).
- SQLAlchemy 2 async + Alembic (`render_as_batch=True`); SQLite dev, Postgres prod.
- structlog JSON logs (`core/logging.py`); wrap cycle work in `bind_cycle(cycle_id)` so every line carries it. Never log secrets.
- Events subclass `DomainEvent` (`core/bus.py`) and go through `EventBus.publish()`, which persists before delivering. Don't write the `events` table directly.
- ruff + mypy (strict on `core/`, `risk/`, `broker/`) + pytest/hypothesis. `risk/` requires 100 % branch coverage.
- Tests never hit Alpaca or Anthropic; use `SimBroker` and recorded agent fixtures. Real-API smoke tests are opt-in (marker `smoke`).
- Verify broker/LLM facts (order types, fees, model IDs, prices) against current docs — don't hard-code from memory.

## Commands
```
uv sync                      # create .venv, install deps from uv.lock
uv run pytest                # unit + integration; `smoke` excluded by default
uv run pytest -m smoke       # opt-in real-API smoke tests (needs keys in .env)
uv run ruff check            # lint (`--fix` to autofix)
uv run ruff format           # format
uv run mypy                  # strict, src + tests (paths from pyproject)
uv run autotrader --help     # CLI; unimplemented commands exit 2
uv run autotrader check-config  # validate config.yaml + env + profile, print profile hash
uv run autotrader db upgrade    # apply migrations to config.yaml's database.url
uv run autotrader kill [--reason ...] / kill --clear  # kill-switch (fallback: touch data/control/KILL)
uv run alembic revision --autogenerate -m "..."  # new migration after editing journal/models.py
```
CI (`.github/workflows/ci.yml`) runs the same checks plus `pip-audit` on the exported lock file.
Add dependencies with `uv add <pkg>` / `uv add --dev <pkg>` — only in the WP that needs them.
