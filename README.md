# auto-trader

Automated, agent-based crypto trading PoC — Alpaca paper trading first. See `SPEC.md` (what) and `PLAN.md` (how / in which order).

## Quickstart

Requires [uv](https://docs.astral.sh/uv/) (installs Python 3.12 itself).

```bash
uv sync
cp .env.example .env   # fill in paper keys when needed
uv run pytest
uv run autotrader --help
```

## License

GPLv3, see `LICENSE`.
