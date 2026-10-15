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
from autotrader.journal.db import (
    current_revision,
    downgrade,
    make_engine,
    make_sessionmaker,
    upgrade,
)

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


def test_unknown_exposure_survives_downgrade_to_0002_as_zero(db_url: str) -> None:
    async def insert_unknown() -> None:
        engine = make_engine(db_url)
        try:
            async with make_sessionmaker(engine)() as s:
                s.add(
                    m.EquitySnapshot(
                        ts=T0,
                        equity=Decimal("1200"),
                        cash=Decimal("1000"),
                        exposure=None,
                        hwm=Decimal("1200"),
                        daily_pnl=Decimal(0),
                    )
                )
                await s.commit()
        finally:
            await engine.dispose()

    async def exposure_after() -> object:
        engine = make_engine(db_url)
        try:
            async with engine.connect() as conn:
                return (await conn.execute(text("SELECT exposure FROM equity_snapshots"))).scalar()
        finally:
            await engine.dispose()

    asyncio.run(insert_unknown())
    downgrade(db_url, "0002")
    assert Decimal(str(asyncio.run(exposure_after()))) == 0
    upgrade(db_url)


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
    session.add(
        m.Fill(
            order_id=999,
            broker_fill_id="f-1",
            side="buy",
            fee_asset="quote",
            qty=Decimal(1),
            price=Decimal(1),
            fee=Decimal(0),
            ts=T0,
        )
    )

    with pytest.raises(IntegrityError):
        await session.flush()


async def test_sqlite_uses_wal(session: AsyncSession) -> None:
    mode = (await session.execute(text("PRAGMA journal_mode"))).scalar_one()

    assert mode == "wal"


async def test_sqlite_busy_timeout_is_explicit(db_url: str) -> None:
    """A locked database errors after 5 s instead of hanging a journal call forever."""
    engine = make_engine(db_url)
    try:
        async with engine.connect() as conn:
            assert (await conn.execute(text("PRAGMA busy_timeout"))).scalar() == 5000
    finally:
        await engine.dispose()


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


def _run_sql(url: str, *statements: str) -> None:
    async def run() -> None:
        engine = make_engine(url)
        try:
            async with engine.begin() as conn:
                for statement in statements:
                    await conn.execute(text(statement))
        finally:
            await engine.dispose()

    asyncio.run(run())


_ORDER_0003 = (
    "INSERT INTO orders (id, broker_order_id, mode, type, side, qty, status, submitted_at,"
    " updated_at) VALUES ({id}, {broker}, 'paper', 'limit', 'buy', '1', '{status}',"
    " '2026-10-09 12:00:00', '2026-10-09 12:00:00')"
)


def test_0004_refuses_to_invent_broker_fill_ids(db_url: str) -> None:
    downgrade(db_url, "0003")
    _run_sql(
        db_url,
        _ORDER_0003.format(id=1, broker="'b-1'", status="filled"),
        "INSERT INTO fills (order_id, qty, price, fee, ts) VALUES (1, '1', '100', '0',"
        " '2026-10-09 12:00:00')",
    )
    with pytest.raises(RuntimeError, match="fills has rows"):
        upgrade(db_url)
    assert current_revision(db_url) == "0003"


def test_0004_settles_final_rows_that_never_reached_the_broker(db_url: str) -> None:
    downgrade(db_url, "0003")
    _run_sql(
        db_url,
        _ORDER_0003.format(id=1, broker="NULL", status="not_submitted"),
        _ORDER_0003.format(id=2, broker="NULL", status="rejected"),
        _ORDER_0003.format(id=3, broker="'b-3'", status="filled"),
        _ORDER_0003.format(id=4, broker="NULL", status="unknown"),
    )
    upgrade(db_url)

    async def rows() -> list[tuple[int, bool, Decimal]]:
        engine = make_engine(db_url)
        try:
            async with make_sessionmaker(engine)() as s:
                found = await s.scalars(select(m.Order).order_by(m.Order.id))
                return [(o.id, o.settled_at is not None, o.filled_qty) for o in found]
        finally:
            await engine.dispose()

    assert asyncio.run(rows()) == [
        (1, True, Decimal(0)),
        (2, True, Decimal(0)),
        (3, False, Decimal(0)),
        (4, False, Decimal(0)),
    ]


async def test_broker_fill_id_is_unique(session: AsyncSession) -> None:
    order = m.Order(
        mode="paper",
        type="market",
        side="buy",
        qty=Decimal("1"),
        status="new",
        submitted_at=T0,
        updated_at=T0,
    )
    session.add(order)
    await session.flush()
    for _ in range(2):
        session.add(
            m.Fill(
                order_id=order.id,
                broker_fill_id="f-1",
                side="buy",
                fee_asset="quote",
                qty=Decimal("1"),
                price=Decimal("100"),
                fee=Decimal("0.25"),
                ts=T0,
            )
        )
    with pytest.raises(IntegrityError):
        await session.commit()
