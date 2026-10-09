import asyncio
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

import pytest
from alembic.autogenerate import compare_metadata
from alembic.migration import MigrationContext
from sqlalchemy import Connection, inspect, select, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import IntegrityError, StatementError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.schema import CreateTable

from autotrader.journal import models as m
from autotrader.journal.db import downgrade, make_engine, make_sessionmaker, upgrade

T0 = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)

SPEC_TABLES = {
    "strategies",
    "cycles",
    "agent_calls",
    "order_intents",
    "risk_decisions",
    "orders",
    "fills",
    "positions",
    "bars",
    "equity_snapshots",
    "events",
    "hypotheses",
    "reviews",
    "control_state",
}


@pytest.fixture
def db_url(tmp_path: Path) -> str:
    url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    upgrade(url)
    return url


@pytest.fixture
async def session(db_url: str) -> AsyncIterator[AsyncSession]:
    engine = make_engine(db_url)
    async with make_sessionmaker(engine)() as s:
        yield s
    await engine.dispose()


# --- migrations ----------------------------------------------------------------------


async def _table_names(url: str) -> set[str]:
    engine = make_engine(url)
    async with engine.connect() as conn:
        tables = await conn.run_sync(lambda c: set(inspect(c).get_table_names()))
    await engine.dispose()
    return tables


async def test_migration_creates_all_spec_tables(db_url: str) -> None:
    assert await _table_names(db_url) >= SPEC_TABLES


async def test_migration_matches_models(db_url: str) -> None:
    # Guards against editing models.py without a migration (or vice versa).
    def diff(conn: Connection) -> list[object]:
        return list(compare_metadata(MigrationContext.configure(conn), m.Base.metadata))

    engine = make_engine(db_url)
    async with engine.connect() as conn:
        changes = await conn.run_sync(diff)
    await engine.dispose()

    assert changes == []


def test_downgrade_removes_tables(db_url: str) -> None:
    # Sync test: Alembic runs its own event loop.
    downgrade(db_url, "base")

    assert not SPEC_TABLES & asyncio.run(_table_names(db_url))


def test_spec_indexes_exist() -> None:
    indexed = {
        (idx.table.name, tuple(c.name for c in idx.columns))
        for table in m.Base.metadata.tables.values()
        for idx in table.indexes
        if idx.table is not None
    }

    assert {
        ("cycles", ("created_at",)),
        ("positions", ("closed_at",)),
        ("events", ("ts",)),
        ("agent_calls", ("cycle_id",)),
    } <= indexed


# --- engine settings -----------------------------------------------------------------


async def test_sqlite_enforces_foreign_keys(session: AsyncSession) -> None:
    session.add(m.Fill(order_id=999, qty=Decimal(1), price=Decimal(1), fee=Decimal(0), ts=T0))

    with pytest.raises(IntegrityError):
        await session.flush()


async def test_sqlite_uses_wal(session: AsyncSession) -> None:
    mode = (await session.execute(text("PRAGMA journal_mode"))).scalar_one()

    assert mode == "wal"


def test_make_engine_creates_sqlite_parent_dir(tmp_path: Path) -> None:
    target = tmp_path / "nested" / "dir" / "x.db"

    make_engine(f"sqlite+aiosqlite:///{target}")

    assert target.parent.is_dir()


# --- column types --------------------------------------------------------------------


async def test_decimal_round_trip_is_exact(session: AsyncSession) -> None:
    qty = Decimal("0.000123456789012345")
    price = Decimal("61234.123456789")
    session.add(
        m.EquitySnapshot(
            ts=T0,
            equity=price,
            cash=qty,
            exposure=Decimal("0"),
            hwm=price,
            daily_pnl=Decimal("-0.01"),
            benchmark_equity=None,
        )
    )
    await session.commit()
    session.expire_all()

    row = (await session.execute(select(m.EquitySnapshot))).scalar_one()

    assert row.equity == price
    assert row.cash == qty
    assert isinstance(row.cash, Decimal)


async def test_floats_rejected_for_money(session: AsyncSession) -> None:
    session.add(
        m.EquitySnapshot(
            ts=T0,
            equity=0.1,
            cash=Decimal(0),
            exposure=Decimal(0),
            hwm=Decimal(0),
            daily_pnl=Decimal(0),
        )
    )

    with pytest.raises(StatementError, match="Decimal"):
        await session.flush()


async def test_datetime_round_trip_is_utc(session: AsyncSession) -> None:
    berlin = timezone(timedelta(hours=2))
    session.add(m.Event(ts=T0.astimezone(berlin), type="x", payload={}))
    await session.commit()
    session.expire_all()

    row = (await session.execute(select(m.Event))).scalar_one()

    assert row.ts == T0
    assert row.ts.tzinfo is UTC


async def test_naive_datetime_rejected(session: AsyncSession) -> None:
    session.add(m.Event(ts=datetime(2026, 1, 1), type="x", payload={}))

    with pytest.raises(StatementError, match="timezone"):
        await session.flush()


def test_decimal_is_numeric_on_postgres() -> None:
    pg = postgresql.dialect()  # type: ignore[no-untyped-call]  # untyped in SQLAlchemy stubs
    ddl = str(CreateTable(m.Base.metadata.tables["fills"]).compile(dialect=pg))

    assert "price NUMERIC(28, 12)" in ddl
    assert "ts TIMESTAMP WITH TIME ZONE" in ddl
