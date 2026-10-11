from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
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


async def test_register_sees_changes_from_other_sessions(db_url: str) -> None:
    # PR #4 review: with expire_on_commit=False a cached Strategy row looked still active
    # after another session had switched profiles, so registration became a wrong no-op.
    profile = load_profile(PROFILE_PATH)
    data = profile.model_dump(mode="json")
    data["risk"]["max_positions"] = 1
    other = parse_profile(data)

    engine = make_engine(db_url)
    sessions = make_sessionmaker(engine)
    async with sessions() as a, sessions() as b:
        first = await repo.register_strategy(a, profile, "a", now=T0)
        await a.refresh(first)
        await a.commit()
        await repo.register_strategy(b, other, "b", now=T0 + timedelta(hours=1))

        again = await repo.register_strategy(a, profile, "a", now=T0 + timedelta(hours=2))

    async with sessions() as fresh:
        active = (
            await fresh.scalars(select(m.Strategy.id).where(m.Strategy.active_to.is_(None)))
        ).all()
    await engine.dispose()

    assert again.id == first.id
    assert active == [first.id]


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


async def test_upsert_backfill_sized_batch(session: AsyncSession) -> None:
    # PR #4 review: 9 days of 1m bars x 3 symbols (PLAN G5 backfill) exceeded SQLite's
    # bind-parameter limit in a single INSERT.
    symbols = ("BTC/USD", "ETH/USD", "SOL/USD")
    bars = [bar(i, symbol=s) for s in symbols for i in range(9 * 1440)]

    await repo.upsert_bars(session, bars)

    count = await session.scalar(select(func.count()).select_from(m.BarRow))
    assert count == 38_880


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


async def _cycle(session: AsyncSession) -> int:
    strategy = await repo.register_strategy(session, load_profile(PROFILE_PATH), "yaml")
    row = m.Cycle(
        strategy_id=strategy.id,
        symbol="BTC/USD",
        rule_id="r",
        signal_features={},
        state="RISK_CHECK",
        created_at=T0,
    )
    session.add(row)
    await session.commit()
    return row.id


async def _order(
    session: AsyncSession,
    *,
    status: str,
    side: str = "buy",
    at: datetime = T0,
    cycle_id: int | None = None,
) -> m.Order:
    return await repo.insert_order(
        session,
        cycle_id=cycle_id,
        client_order_id=None,
        symbol="BTC/USD",
        mode="paper",
        type_="limit",
        side=side,
        qty=Decimal("1"),
        limit_price=Decimal("100"),
        stop_price=None,
        tp_price=None,
        status=status,
        now=at,
    )


async def test_risk_decision_roundtrip(session: AsyncSession) -> None:
    cycle_id = await _cycle(session)
    row = await repo.record_risk_decision(
        session,
        cycle_id=cycle_id,
        kind="entry",
        approved=False,
        reasons=["x"],
        adjusted={"entry": "100.1"},
        computed_size=None,
        ts=T0,
    )
    assert (row.kind, row.reasons, row.adjusted) == ("entry", ["x"], {"entry": "100.1"})


async def test_client_order_id_is_unique(session: AsyncSession) -> None:
    cycle_id = await _cycle(session)
    kw: dict[str, Any] = dict(
        cycle_id=cycle_id,
        client_order_id="at-c1-e",
        symbol="BTC/USD",
        mode="paper",
        type_="limit",
        side="buy",
        qty=Decimal("1"),
        limit_price=Decimal("1"),
        stop_price=None,
        tp_price=None,
        status="pending",
        now=T0,
    )
    await repo.insert_order(session, **kw)
    with pytest.raises(IntegrityError):
        await repo.insert_order(session, **kw)


async def test_open_entry_orders_and_update(session: AsyncSession) -> None:
    keep = [await _order(session, status=s) for s in ("pending", "new", "unknown")]
    for s in ("filled", "canceled", "rejected", "not_submitted"):
        await _order(session, status=s)
    await _order(session, status="new", side="sell")
    assert {o.id for o in await repo.open_entry_orders(session)} == {o.id for o in keep}
    await repo.update_order(session, keep[1].id, status="filled", now=T0, broker_order_id="b1")
    assert {o.id for o in await repo.open_entry_orders(session)} == {keep[0].id, keep[2].id}


async def test_count_entry_orders_window(session: AsyncSession) -> None:
    await _order(session, status="filled", at=T0)
    await _order(session, status="rejected", at=T0 + timedelta(minutes=30))
    await _order(session, status="not_submitted", at=T0 + timedelta(minutes=40))  # never sent
    await _order(session, status="filled", side="sell", at=T0 + timedelta(minutes=40))  # exit
    assert await repo.count_entry_orders(session, T0, inclusive=True) == 2
    assert await repo.count_entry_orders(session, T0, inclusive=False) == 1


async def test_snapshots_and_day_start_equity(session: AsyncSession) -> None:
    assert await repo.latest_snapshot(session) is None
    assert await repo.day_start_equity(session, T0) is None
    for minutes, equity in ((-10, "990"), (5, "995")):
        await repo.insert_equity_snapshot(
            session,
            ts=T0 + timedelta(minutes=minutes),
            equity=Decimal(equity),
            cash=Decimal(equity),
            exposure=Decimal(0),
            hwm=Decimal("1000"),
            daily_pnl=Decimal(0),
        )
    assert await repo.day_start_equity(session, T0) == Decimal("990")  # last at/before
    before_all = T0 - timedelta(hours=1)
    assert await repo.day_start_equity(session, before_all) == Decimal("990")  # first after
    latest = await repo.latest_snapshot(session)
    assert latest is not None and latest.equity == Decimal("995")


async def test_snapshot_exposure_may_be_unknown(session: AsyncSession) -> None:
    await repo.insert_equity_snapshot(
        session,
        ts=T0,
        equity=Decimal("1200"),
        cash=Decimal("1000"),
        exposure=None,  # an account observation without a positions read
        hwm=Decimal("1200"),
        daily_pnl=Decimal(0),
    )
    latest = await repo.latest_snapshot(session)
    assert latest is not None and latest.exposure is None


async def test_max_hwm_is_the_numeric_maximum_over_all_snapshots(session: AsyncSession) -> None:
    assert await repo.max_hwm(session) is None
    # Out of ts order, and "999.5" > "1200.000000000001" as strings (SQLite stores text).
    for minutes, hwm in ((0, "1200.000000000001"), (1, "999.5"), (2, "1200")):
        await repo.insert_equity_snapshot(
            session,
            ts=T0 + timedelta(minutes=minutes),
            equity=Decimal("990"),
            cash=Decimal("990"),
            exposure=Decimal(0),
            hwm=Decimal(hwm),
            daily_pnl=Decimal(0),
        )
    assert await repo.max_hwm(session) == Decimal("1200.000000000001")


async def test_strategy_trade_stats_counts_closed_trades_of_that_strategy(
    session: AsyncSession,
) -> None:
    cycle_id = await _cycle(session)
    cycle = await session.get(m.Cycle, cycle_id)
    assert cycle is not None
    for pnl, closed in (("5", True), ("-3", True), ("1", True), ("9", False)):
        session.add(
            m.Position(
                cycle_id=cycle_id,
                symbol="BTC/USD",
                side="long",
                qty=Decimal(1),
                entry_price=Decimal(1),
                opened_at=T0,
                closed_at=T0 if closed else None,
                realized_pnl=Decimal(pnl),
            )
        )
    await session.commit()
    assert await repo.strategy_trade_stats(session, cycle.strategy_id) == (3, 2)
    assert await repo.strategy_trade_stats(session, cycle.strategy_id + 1) == (0, 0)


# --- cycles (WP 1.4) -----------------------------------------------------------------


async def _strategy_id(session: AsyncSession) -> int:
    return (await repo.register_strategy(session, load_profile(PROFILE_PATH), "y", now=T0)).id


async def test_append_event_without_commit_is_rolled_back(session: AsyncSession) -> None:
    await repo.append_event(session, "Ping", {"n": 1}, commit=False)
    await session.rollback()
    assert await repo.events_after(session, 0) == []


async def test_insert_cycle_flushes_an_id_but_does_not_commit(session: AsyncSession) -> None:
    sid = await _strategy_id(session)
    row = await repo.insert_cycle(
        session,
        strategy_id=sid,
        symbol="BTC/USD",
        rule_id="r1",
        signal_features={"close": 1.0, "vol_z": None, "_timeframe": "5m"},
        state="NEW",
        now=T0,
    )
    assert row.id is not None
    await session.rollback()
    assert await session.scalar(select(func.count()).select_from(m.Cycle)) == 0


async def test_transition_cycle_is_guarded_and_sets_terminal_fields(
    session: AsyncSession,
) -> None:
    sid = await _strategy_id(session)
    row = await repo.insert_cycle(
        session,
        strategy_id=sid,
        symbol="BTC/USD",
        rule_id="r1",
        signal_features={},
        state="NEW",
        now=T0,
    )
    await session.commit()

    await repo.transition_cycle(session, row.id, from_state="NEW", to_state="ENRICHING", now=T0)
    with pytest.raises(repo.StaleTransition):
        await repo.transition_cycle(session, row.id, from_state="NEW", to_state="ENRICHING", now=T0)
    done = T0 + timedelta(seconds=1)
    await repo.transition_cycle(
        session,
        row.id,
        from_state="ENRICHING",
        to_state="EXPIRED",
        now=done,
        reason="symbol stale",
    )
    await session.commit()

    got = await session.get(m.Cycle, row.id, populate_existing=True)
    assert got is not None
    assert (got.state, got.outcome, got.reject_reason, got.finished_at) == (
        "EXPIRED",
        "EXPIRED",
        "symbol stale",
        done,
    )
    assert await repo.cycle_state(session, row.id) == "EXPIRED"
    assert await repo.cycle_state(session, row.id + 99) is None


async def test_non_terminal_transition_leaves_outcome_empty(session: AsyncSession) -> None:
    sid = await _strategy_id(session)
    row = await repo.insert_cycle(
        session,
        strategy_id=sid,
        symbol="BTC/USD",
        rule_id="r1",
        signal_features={},
        state="NEW",
        now=T0,
    )
    await repo.transition_cycle(
        session, row.id, from_state="NEW", to_state="ENRICHING", now=T0, reason="ignored"
    )
    await session.commit()
    got = await session.get(m.Cycle, row.id, populate_existing=True)
    assert got is not None
    assert (got.outcome, got.finished_at, got.reject_reason) == (None, None, None)


async def test_open_cycles_and_order_lookup(session: AsyncSession) -> None:
    sid = await _strategy_id(session)
    kw: dict[str, Any] = dict(strategy_id=sid, rule_id="r1", signal_features={}, now=T0)
    open_ = await repo.insert_cycle(session, symbol="BTC/USD", state="RISK_CHECK", **kw)
    await repo.insert_cycle(session, symbol="ETH/USD", state="ERROR", **kw)
    await session.commit()

    assert [c.id for c in await repo.open_cycles(session)] == [open_.id]
    assert not await repo.has_order_for_cycle(session, open_.id)
    await repo.insert_order(
        session,
        cycle_id=open_.id,
        client_order_id=f"at-c{open_.id}-e",
        symbol="BTC/USD",
        mode="paper",
        type_="limit",
        side="buy",
        qty=Decimal("1"),
        limit_price=Decimal("100"),
        stop_price=None,
        tp_price=None,
        status="pending",
        now=T0,
    )
    assert await repo.has_order_for_cycle(session, open_.id)
