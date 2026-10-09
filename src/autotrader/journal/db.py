"""Engine/session factories and programmatic Alembic upgrades."""

import asyncio
from pathlib import Path
from typing import Any

from alembic import command
from alembic.config import Config
from alembic.migration import MigrationContext
from sqlalchemy import event
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

MIGRATIONS = "autotrader.journal:migrations"


def _sqlite_pragmas(dbapi_conn: Any, _record: Any) -> None:
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")  # off by default in SQLite
    cursor.execute("PRAGMA journal_mode=WAL")  # API reads while core writes
    cursor.close()


def _ensure_sqlite_dir(url: str) -> bool:
    """Create the parent dir of a file-backed SQLite DB. Returns whether `url` is SQLite."""
    parsed = make_url(url)
    if parsed.get_backend_name() != "sqlite":
        return False
    if parsed.database not in (None, "", ":memory:"):
        Path(parsed.database).parent.mkdir(parents=True, exist_ok=True)
    return True


def make_engine(url: str) -> AsyncEngine:
    is_sqlite = _ensure_sqlite_dir(url)
    engine = create_async_engine(url)
    if is_sqlite:
        event.listen(engine.sync_engine, "connect", _sqlite_pragmas)
    return engine


def make_sessionmaker(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


def _alembic_config(url: str) -> Config:
    cfg = Config()
    cfg.set_main_option("script_location", MIGRATIONS)
    cfg.attributes["url"] = url
    return cfg


def upgrade(url: str, revision: str = "head") -> None:
    """Migrate the DB at `url`. Sync: runs its own event loop, call outside async code."""
    _ensure_sqlite_dir(url)
    command.upgrade(_alembic_config(url), revision)


def current_revision(url: str) -> str | None:
    """Revision the DB at `url` is at (None if never migrated)."""
    return asyncio.run(_current_revision(url))


async def _current_revision(url: str) -> str | None:
    engine = create_async_engine(url)
    async with engine.connect() as conn:
        rev = await conn.run_sync(lambda c: MigrationContext.configure(c).get_current_revision())
    await engine.dispose()
    return rev


def downgrade(url: str, revision: str) -> None:
    command.downgrade(_alembic_config(url), revision)
