from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from autotrader.core.config import load_profile, parse_profile
from autotrader.core.types import Bar
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade

REPO = Path(__file__).resolve().parents[3]
PROFILE_PATH = REPO / "strategies" / "fast_momentum_v1.yaml"
T0 = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)


@pytest.fixture
def db_url(tmp_path: Path) -> str:
    # Sync fixture: Alembic runs its own event loop.
    url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    upgrade(url)
    return url


@pytest.fixture
async def session(db_url: str) -> AsyncIterator[AsyncSession]:
    engine = make_engine(db_url)
    async with make_sessionmaker(engine)() as s:
        yield s
    await engine.dispose()


def bar(minute: int, close: float = 100.0, symbol: str = "BTC/USD") -> Bar:
    return Bar(
        symbol=symbol,
        timeframe="1m",
        ts=T0 + timedelta(minutes=minute),
        open=close,
        high=close + 1,
        low=close - 1,
        close=close,
        volume=10.0,
    )


# --- strategies ----------------------------------------------------------------------


async def test_register_strategy_is_idempotent(session: AsyncSession) -> None:
    profile = load_profile(PROFILE_PATH)
    yaml_text = PROFILE_PATH.read_text()

    first = await repo.register_strategy(session, profile, yaml_text, now=T0)
    again = await repo.register_strategy(session, profile, yaml_text, now=T0 + timedelta(hours=1))

    assert again.id == first.id
    assert first.hash == profile.hash
    assert first.yaml == yaml_text
    assert first.active_to is None


async def test_new_profile_version_closes_previous(session: AsyncSession) -> None:
    profile = load_profile(PROFILE_PATH)
    first = await repo.register_strategy(session, profile, "v1", now=T0)

    data = profile.model_dump(mode="json")
    data["risk"]["max_positions"] = 1
    changed = parse_profile(data)
    later = T0 + timedelta(days=1)
    second = await repo.register_strategy(session, changed, "v2", now=later)

    await session.refresh(first)
    assert second.id != first.id
    assert first.active_to == later
    assert second.active_from == later
    assert second.active_to is None


async def test_reactivating_old_version_reuses_its_row(session: AsyncSession) -> None:
    profile = load_profile(PROFILE_PATH)
    data = profile.model_dump(mode="json")
    data["risk"]["max_positions"] = 1
    other = parse_profile(data)

    first = await repo.register_strategy(session, profile, "a", now=T0)
    await repo.register_strategy(session, other, "b", now=T0 + timedelta(hours=1))
    back = await repo.register_strategy(session, profile, "a", now=T0 + timedelta(hours=2))

    # One row per (hash) keeps cycle -> strategy history stable; the window moves.
    assert back.id == first.id
    assert back.active_to is None
    active = await session.scalar(
        select(func.count()).select_from(m.Strategy).where(m.Strategy.active_to.is_(None))
    )
    assert active == 1


# --- bars ----------------------------------------------------------------------------


async def test_upsert_bars_inserts_and_overwrites(session: AsyncSession) -> None:
    await repo.upsert_bars(session, [bar(0), bar(1)])
    # Backfill after a reconnect may resend a bar with corrected values.
    await repo.upsert_bars(session, [bar(1, close=105.0), bar(2)])

    rows = await repo.get_bars(session, "BTC/USD", "1m", since=T0)

    assert [b.ts for b in rows] == [T0 + timedelta(minutes=i) for i in range(3)]
    assert rows[1].close == 105.0


async def test_get_bars_filters_symbol_and_window(session: AsyncSession) -> None:
    await repo.upsert_bars(session, [bar(i) for i in range(5)] + [bar(0, symbol="ETH/USD")])

    rows = await repo.get_bars(
        session, "BTC/USD", "1m", since=T0 + timedelta(minutes=1), until=T0 + timedelta(minutes=3)
    )

    assert [b.ts.minute for b in rows] == [1, 2]
    assert all(b.symbol == "BTC/USD" for b in rows)


async def test_upsert_no_bars_is_noop(session: AsyncSession) -> None:
    await repo.upsert_bars(session, [])


# --- events --------------------------------------------------------------------------


async def test_events_append_and_tail(session: AsyncSession) -> None:
    a = await repo.append_event(session, "BarClosed", {"symbol": "BTC/USD"}, ts=T0)
    b = await repo.append_event(session, "KillTriggered", {"source": "file"}, ts=T0)

    tail = await repo.events_after(session, after_id=a.id)

    assert [e.id for e in tail] == [b.id]
    assert tail[0].payload == {"source": "file"}


async def test_events_after_respects_limit(session: AsyncSession) -> None:
    for i in range(5):
        await repo.append_event(session, "X", {"i": i}, ts=T0)

    tail = await repo.events_after(session, after_id=0, limit=2)

    assert [e.payload["i"] for e in tail] == [0, 1]


# --- control state -------------------------------------------------------------------


async def test_control_state_get_set(session: AsyncSession) -> None:
    assert await repo.get_control(session, "paused") is None

    await repo.set_control(session, "paused", True)
    await repo.set_control(session, "paused", False)
    await repo.set_control(session, "paused_until", "2026-10-10T00:00:00+00:00")

    assert await repo.get_control(session, "paused") is False
    assert await repo.get_control(session, "paused_until") == "2026-10-10T00:00:00+00:00"
