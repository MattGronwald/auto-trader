"""Journal access. Functions take an `AsyncSession` and commit their own unit of work.

Only Phase 0 needs live here (strategies, bars, events, control state); the other tables
get their functions in the work package that writes them. There is deliberately no
update/delete for events and no delete anywhere (SPEC §4.8: nothing is deleted).
"""

from collections.abc import Sequence
from datetime import UTC, datetime
from typing import Any

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.ext.asyncio import AsyncSession

from autotrader.core.config import StrategyProfile
from autotrader.core.types import Bar
from autotrader.journal.models import BarRow, ControlState, Event, Strategy


def _now() -> datetime:
    return datetime.now(UTC)


# --- strategies ----------------------------------------------------------------------


async def register_strategy(
    session: AsyncSession, profile: StrategyProfile, yaml_text: str, now: datetime | None = None
) -> Strategy:
    """Make `profile` the active strategy; one row per profile hash.

    Already active → no-op. Otherwise the previously active row is closed and this
    profile's row is (re)opened, so cycles keep pointing at the exact profile they ran.
    """
    now = now or _now()
    # populate_existing: sessions use expire_on_commit=False, so a row cached earlier in
    # this session could still look active after another session switched profiles.
    row = await session.scalar(
        select(Strategy)
        .where(Strategy.hash == profile.hash)
        .execution_options(populate_existing=True)
    )
    if row is not None and row.active_to is None:
        return row
    await session.execute(
        update(Strategy).where(Strategy.active_to.is_(None)).values(active_to=now)
    )
    if row is None:
        row = Strategy(name=profile.name, yaml=yaml_text, hash=profile.hash, active_from=now)
        session.add(row)
    else:
        row.active_from, row.active_to = now, None
    await session.commit()
    return row


# --- bars ----------------------------------------------------------------------------

# Rows per INSERT: 8 bind params each -> 8,000 params, well under SQLite's 32,766 and
# asyncpg's 32,767 per statement. A 9-day x 3-symbol backfill is ~39k rows.
_BAR_BATCH = 1000


async def upsert_bars(session: AsyncSession, bars: Sequence[Bar]) -> None:
    """Insert bars; an existing (symbol, timeframe, ts) is overwritten (backfill fixes)."""
    if not bars:
        return
    rows = [
        {
            "symbol": b.symbol,
            "timeframe": b.timeframe,
            "ts": b.ts,
            "o": b.open,
            "h": b.high,
            "l": b.low,
            "c": b.close,
            "v": b.volume,
        }
        for b in bars
    ]
    dialect = (await session.connection()).dialect.name
    insert = pg_insert if dialect == "postgresql" else sqlite_insert
    for i in range(0, len(rows), _BAR_BATCH):
        stmt = insert(BarRow).values(rows[i : i + _BAR_BATCH])
        stmt = stmt.on_conflict_do_update(
            index_elements=["symbol", "timeframe", "ts"],
            set_={c: stmt.excluded[c] for c in ("o", "h", "l", "c", "v")},
        )
        await session.execute(stmt)
    await session.commit()  # one transaction for the whole batch


async def get_bars(
    session: AsyncSession,
    symbol: str,
    timeframe: str,
    since: datetime,
    until: datetime | None = None,
) -> list[Bar]:
    """Bars with `since <= ts < until`, oldest first."""
    query = select(BarRow).where(
        BarRow.symbol == symbol, BarRow.timeframe == timeframe, BarRow.ts >= since
    )
    if until is not None:
        query = query.where(BarRow.ts < until)
    rows = await session.scalars(query.order_by(BarRow.ts))
    return [
        Bar(
            symbol=r.symbol,
            timeframe=r.timeframe,
            ts=r.ts,
            open=r.o,
            high=r.h,
            low=r.l,
            close=r.c,
            volume=r.v,
        )
        for r in rows
    ]


# --- events --------------------------------------------------------------------------


async def append_event(
    session: AsyncSession,
    type_: str,
    payload: dict[str, Any],
    *,
    cycle_id: int | None = None,
    ts: datetime | None = None,
) -> Event:
    row = Event(ts=ts or _now(), type=type_, cycle_id=cycle_id, payload=payload)
    session.add(row)
    await session.commit()
    return row


async def events_after(session: AsyncSession, after_id: int, limit: int = 500) -> list[Event]:
    """Events with id > `after_id`, oldest first (dashboard tail, WS replay)."""
    rows = await session.scalars(
        select(Event).where(Event.id > after_id).order_by(Event.id).limit(limit)
    )
    return list(rows)


# --- control state -------------------------------------------------------------------


async def get_control(session: AsyncSession, key: str) -> Any:
    row = await session.get(ControlState, key)
    return None if row is None else row.value


async def set_control(session: AsyncSession, key: str, value: Any) -> None:
    await session.merge(ControlState(key=key, value=value))
    await session.commit()
