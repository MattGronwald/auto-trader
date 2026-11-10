"""Shared test helpers that put a real `RiskEngine` into a state where entries may pass the
pre-admission gates (WP 1.5d-1)."""

from datetime import datetime, timedelta
from decimal import Decimal

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.core.tradingday import day_start
from autotrader.journal import repo


async def seed_baseline(
    sessions: async_sessionmaker[AsyncSession],
    now: datetime,
    tz: str = "UTC",
    equity: Decimal = Decimal("1000"),
) -> None:
    """One equity snapshot 60 s before the trading day of `now` starts: a trusted daily-loss
    baseline (inside the 120 s window). `equity_snapshots.ts` is unique: seed once per DB."""
    async with sessions() as s:
        await repo.insert_equity_snapshot(
            s,
            ts=day_start(now, tz) - timedelta(seconds=60),
            equity=equity,
            cash=equity,
            exposure=Decimal(0),
            hwm=equity,
            daily_pnl=Decimal(0),
        )
