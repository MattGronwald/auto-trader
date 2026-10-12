import asyncio
import io
import json
from collections.abc import Awaitable, Callable, Sequence
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import func, select

from autotrader.broker.base import Account, BrokerError, FeeSchedule
from autotrader.core.config import AppConfig, Settings, load_config, load_profile
from autotrader.core.feed import Latest
from autotrader.core.logging import configure_logging
from autotrader.core.runtime import _snapshot_loop, run_core
from autotrader.core.types import Bar, Quote
from autotrader.journal import models as m
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade

REPO = Path(__file__).resolve().parents[3]


class StreamingSource:
    """Delivers two live bars, then idles until stopped."""

    def __init__(self, fail_stream: bool = False) -> None:
        self.fail_stream = fail_stream
        self.stopped = asyncio.Event()

    async def history(self, symbols: Sequence[str], start: datetime, end: datetime) -> list[Bar]:
        return []

    async def latest(self, symbol: str) -> Latest:
        return Latest(bar=None, quote=None)

    async def stream(
        self, on_bar: Callable[[Bar], Awaitable[None]], on_quote: Callable[[Quote], Awaitable[None]]
    ) -> None:
        if self.fail_stream:
            raise ConnectionError("stream died")
        # The current minute, computed now: never inside an already finalised window
        # (windows before `now - grace` are final at startup; late bars aren't announced).
        ts = datetime.now(UTC).replace(second=0, microsecond=0)
        for symbol in ("BTC/USD", "ETH/USD"):
            await on_bar(
                Bar(
                    symbol=symbol,
                    timeframe="1m",
                    ts=ts,
                    open=1,
                    high=1,
                    low=1,
                    close=1,
                    volume=1,
                )
            )
        await self.stopped.wait()

    def connected(self) -> bool:
        return True

    async def stop(self) -> None:
        self.stopped.set()


class FakeBroker:
    async def get_account(self) -> Account:
        return Account(equity=Decimal("100000"), cash=Decimal("100000"))

    async def get_positions(self) -> list[Any]:
        return []

    def mode(self) -> str:
        return "paper"

    def fee_schedule(self) -> FeeSchedule:
        return FeeSchedule(maker_bps=Decimal(15), taker_bps=Decimal(25))


class HangingSnapshotBroker(FakeBroker):
    """Answers the startup check, then every later `get_account` hangs (a stuck REST call)."""

    def __init__(self) -> None:
        self.calls = 0

    async def get_account(self) -> Account:
        self.calls += 1
        if self.calls > 1:
            await asyncio.Event().wait()
        return await super().get_account()


async def fake_connect(_: Settings) -> Any:
    return FakeBroker()


@pytest.fixture
def cfg(tmp_path: Path) -> AppConfig:
    text = (
        (REPO / "config.yaml")
        .read_text()
        .replace("strategy: strategies/", f"strategy: {REPO}/strategies/")
    )
    path = tmp_path / "config.yaml"
    path.write_text(text)
    config = load_config(path, Settings(_env_file=None))
    upgrade(config.database.url)
    return config


def _log_lines(buf: io.StringIO) -> list[dict[str, Any]]:
    return [json.loads(line) for line in buf.getvalue().splitlines() if line.startswith("{")]


async def _wait_for(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.01)


async def test_run_core_persists_bars_and_responds_to_kill_file(cfg: AppConfig) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    stop = asyncio.Event()
    source = StreamingSource()
    profile = load_profile(cfg.strategy)

    task = asyncio.create_task(
        run_core(
            cfg,
            Settings(_env_file=None),
            profile,
            "yaml",
            source=source,
            connect_broker=fake_connect,
            stop=stop,
            tick_s=0.01,
            snapshot_s=0.01,
        )
    )
    try:
        await _wait_for(lambda: any(ln["event"] == "core.started" for ln in _log_lines(buf)))
        cfg.control.kill_file.parent.mkdir(parents=True, exist_ok=True)
        cfg.control.kill_file.touch()
        await _wait_for(lambda: any(ln["event"] == "core.kill_detected" for ln in _log_lines(buf)))
        cfg.control.kill_file.unlink()
        await _wait_for(lambda: any(ln["event"] == "core.kill_cleared" for ln in _log_lines(buf)))
        await _wait_for(
            lambda: any(ln["event"] == "risk.snapshot_written" for ln in _log_lines(buf))
        )
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=5)

    started = next(ln for ln in _log_lines(buf) if ln["event"] == "core.started")
    assert started["mode"] == "paper"
    scanner = next(ln for ln in _log_lines(buf) if ln["event"] == "scanner.subscribed")
    assert scanner["timeframe"] == profile.timeframes.signal
    assert scanner["rules"] == [r.id for r in profile.signal_rules]
    assert started["profile"] == "fast_momentum_v1"
    assert source.stopped.is_set()  # clean shutdown
    engine = make_engine(cfg.database.url)
    try:  # dispose even when an assertion fails: a leaked connection breaks a later test
        async with make_sessionmaker(engine)() as s:
            assert await s.scalar(select(func.count()).select_from(m.BarRow)) == 2
            assert await s.scalar(select(func.count()).select_from(m.Strategy)) == 1
            snapshots = await s.scalar(select(func.count()).select_from(m.EquitySnapshot))
            assert snapshots is not None and snapshots >= 1
            closed = [e.payload for e in await s.scalars(select(m.Event)) if e.type == "BarClosed"]
    finally:
        await engine.dispose()
    # Only the 1m announcements are deterministic: depending on the wall-clock minute, the
    # bars' 5m/15m buckets may already have ended and are then (correctly) closed too.
    assert [p["timeframe"] for p in closed].count("1m") == 2


async def test_dead_stream_stops_the_core_with_an_error(cfg: AppConfig) -> None:
    source = StreamingSource(fail_stream=True)

    with pytest.raises(ConnectionError, match="stream died"):
        await asyncio.wait_for(
            run_core(
                cfg,
                Settings(_env_file=None),
                load_profile(cfg.strategy),
                "yaml",
                source=source,
                connect_broker=fake_connect,
                stop=asyncio.Event(),
                tick_s=0.01,
            ),
            timeout=5,
        )


async def test_broker_connect_failure_aborts_startup(cfg: AppConfig) -> None:
    async def failing(_: Settings) -> Any:
        raise BrokerError("auth failed")

    with pytest.raises(BrokerError, match="auth failed"):
        await run_core(
            cfg,
            Settings(_env_file=None),
            load_profile(cfg.strategy),
            "yaml",
            source=StreamingSource(),
            connect_broker=failing,
            stop=asyncio.Event(),
        )


class FlakySnapshots:
    """Times out (the engine's own budget), then raises, then succeeds."""

    def __init__(self) -> None:
        self.calls = 0

    async def snapshot_equity(self) -> None:
        self.calls += 1
        if self.calls == 1:
            raise TimeoutError
        if self.calls == 2:
            raise ConnectionError("broker down")


async def test_snapshot_loop_logs_failures_and_keeps_going() -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    loop_task = asyncio.create_task(_snapshot_loop(FlakySnapshots(), 0.01))
    try:
        await _wait_for(
            lambda: any(ln["event"] == "risk.snapshot_written" for ln in _log_lines(buf))
        )
    finally:
        loop_task.cancel()
        await asyncio.gather(loop_task, return_exceptions=True)
    events = [ln for ln in _log_lines(buf) if ln["event"].startswith("risk.snapshot")]
    assert [(ln["event"], ln.get("timeout")) for ln in events[:3]] == [
        ("risk.snapshot_failed", True),
        ("risk.snapshot_failed", None),
        ("risk.snapshot_written", None),
    ]


async def test_hung_snapshot_at_the_default_interval_never_delays_kill_detection(
    cfg: AppConfig,
) -> None:
    """Default snapshot_s (60 s); the snapshot hangs inside the engine's own 10 s budget.
    The core loop never waits for a snapshot, only the background snapshot task does."""
    buf = io.StringIO()
    configure_logging(stream=buf)
    stop = asyncio.Event()
    broker = HangingSnapshotBroker()

    async def connect(_: Settings) -> Any:
        return broker

    task = asyncio.create_task(
        run_core(
            cfg,
            Settings(_env_file=None),
            load_profile(cfg.strategy),
            "yaml",
            source=StreamingSource(),
            connect_broker=connect,
            stop=stop,
            tick_s=0.01,
        )
    )
    try:
        await _wait_for(lambda: any(ln["event"] == "core.started" for ln in _log_lines(buf)))
        await _wait_for(lambda: broker.calls > 1)  # the first snapshot is hanging now
        cfg.control.kill_file.parent.mkdir(parents=True, exist_ok=True)
        cfg.control.kill_file.touch()
        await _wait_for(
            lambda: any(ln["event"] == "core.kill_detected" for ln in _log_lines(buf)), timeout=1
        )
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=5)


class SnapshotKiller(BaseException):
    """Escapes the snapshot loop's `except Exception` (stands in for an unexpected end)."""


class DyingSnapshotBroker(FakeBroker):
    """Startup check answers; the first snapshot kills the task; later ones succeed."""

    def __init__(self) -> None:
        self.calls = 0

    async def get_account(self) -> Account:
        self.calls += 1
        if self.calls == 2:
            raise SnapshotKiller
        return await super().get_account()


async def test_dead_snapshot_task_is_logged_and_restarted(cfg: AppConfig) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    stop = asyncio.Event()
    broker = DyingSnapshotBroker()

    async def connect(_: Settings) -> Any:
        return broker

    task = asyncio.create_task(
        run_core(
            cfg,
            Settings(_env_file=None),
            load_profile(cfg.strategy),
            "yaml",
            source=StreamingSource(),
            connect_broker=connect,
            stop=stop,
            tick_s=0.01,
        )
    )
    try:
        await _wait_for(
            lambda: any(ln["event"] == "risk.snapshot_written" for ln in _log_lines(buf))
        )
    finally:
        stop.set()
        await asyncio.wait_for(task, timeout=5)
    died = [ln for ln in _log_lines(buf) if ln["event"] == "risk.snapshot_task_died"]
    assert len(died) == 1 and "SnapshotKiller" in died[0]["error"]
