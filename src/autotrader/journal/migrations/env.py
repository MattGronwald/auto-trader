"""Alembic environment.

URL source, in order: `autotrader.journal.db.upgrade()` passes it via `config.attributes`;
otherwise (plain `alembic` CLI) it comes from `config.yaml` + env, like the app.
"""

import asyncio
from pathlib import Path

from alembic import context
from sqlalchemy import pool
from sqlalchemy.engine import Connection
from sqlalchemy.ext.asyncio import create_async_engine

from autotrader.journal.models import Base

config = context.config
target_metadata = Base.metadata


def _url() -> str:
    url = config.attributes.get("url")
    if url:
        return str(url)
    from autotrader.core.config import Settings, load_config

    return load_config(Path("config.yaml"), Settings()).database.url


def _configure(**kwargs: object) -> None:
    context.configure(
        target_metadata=target_metadata,
        render_as_batch=True,  # SQLite cannot ALTER most things; batch recreates tables
        user_module_prefix="autotrader.journal.types.",
        **kwargs,  # type: ignore[arg-type]
    )


def run_migrations_offline() -> None:
    _configure(url=_url(), literal_binds=True, dialect_opts={"paramstyle": "named"})
    with context.begin_transaction():
        context.run_migrations()


def _do_run_migrations(connection: Connection) -> None:
    _configure(connection=connection)
    with context.begin_transaction():
        context.run_migrations()


async def _run_async_migrations() -> None:
    # Plain engine, no app pragmas: batch mode drops/recreates tables, which SQLite
    # refuses with foreign_keys=ON.
    engine = create_async_engine(_url(), poolclass=pool.NullPool)
    async with engine.connect() as connection:
        await connection.run_sync(_do_run_migrations)
    await engine.dispose()


if context.is_offline_mode():
    run_migrations_offline()
else:
    asyncio.run(_run_async_migrations())
