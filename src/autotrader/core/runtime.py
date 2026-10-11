"""`autotrader run core` (SPEC §12 Phase 0 DoD): connect to the broker, backfill and stream
bars for the universe, persist 1m bars, respond to the kill-switch. Since WP 1.2 the Signal
Scanner runs on closed bars and persists `CandidateSignal`s; nothing consumes them until
the Cycle Runner (WP 1.4).

The broker is connected here and handed to the `RiskEngine`, which is its only holder from
then on (SPEC §0.1); `run core` also snapshots equity every 60 s for the daily-loss and
drawdown checks, in one supervised background task so a hung snapshot can never delay
kill detection or feed ticks. Nothing calls `check()`/`exit()` until the Cycle Runner (WP 1.4) and
Position Manager (WP 1.5).
"""

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from autotrader.core.bus import EventBus
from autotrader.core.config import AppConfig, Settings, StrategyProfile
from autotrader.core.feed import BarClosed, BarSource, MarketFeed
from autotrader.core.kill import ControlGate
from autotrader.core.scanner import Scanner
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker
from autotrader.risk.engine import RiskEngine

log = structlog.get_logger(__name__)

BrokerConnect = Callable[[Settings], Awaitable[Any]]


async def run_core(
    cfg: AppConfig,
    settings: Settings,
    profile: StrategyProfile,
    profile_yaml: str,
    *,
    source: BarSource,
    connect_broker: BrokerConnect,
    stop: asyncio.Event,
    tick_s: float = 1.0,
    snapshot_s: float = 60.0,
    snapshot_timeout_s: float | None = None,
) -> None:
    """Run until `stop` is set. Raises if the stream dies or startup checks fail."""
    engine = make_engine(cfg.database.url)
    sessions = make_sessionmaker(engine)
    bus = EventBus(sessions)
    stream_task: asyncio.Task[None] | None = None
    snapshot_task: asyncio.Task[None] | None = None
    try:
        async with sessions() as session:
            strategy = await repo.register_strategy(session, profile, profile_yaml)
        broker = await _check_broker(connect_broker, settings)

        timeframes = [
            profile.timeframes.signal,
            *profile.timeframes.context,
            profile.timeframes.trend,
        ]
        feed = MarketFeed(
            source,
            sessions,
            bus,
            symbols=list(profile.universe),
            timeframes=timeframes,
            stale_alert_s=cfg.feed.stale_alert_s,
        )
        await feed.backfill()  # publishes no events, so the scanner can subscribe after it
        gate = ControlGate(cfg.control.kill_file, sessions)
        scanner = Scanner(profile, feed, gate, sessions, bus, trading_day_tz=cfg.trading_day_tz)
        bus.subscribe(BarClosed, scanner.on_bar_closed)
        log.info(
            "scanner.subscribed",
            timeframe=profile.timeframes.signal,
            rules=[r.id for r in profile.signal_rules],
        )
        risk = RiskEngine(
            broker,
            profile=profile,
            strategy_id=strategy.id,
            mode=cfg.mode,
            gate=gate,
            market=feed,
            sessions=sessions,
            trading_day_tz=cfg.trading_day_tz,
        )
        log.info("risk.engine_ready", mode=cfg.mode)
        stream_task = asyncio.create_task(source.stream(feed.on_bar, feed.on_quote), name="feed")
        log.info(
            "core.started",
            mode=cfg.mode,
            profile=profile.name,
            profile_hash=profile.hash,
            strategy_id=strategy.id,
            symbols=list(profile.universe),
            timeframes=timeframes,
        )

        killed: bool | None = None
        snapshot_timeout = (
            min(snapshot_s / 2, 30.0) if snapshot_timeout_s is None else snapshot_timeout_s
        )

        def start_snapshots() -> asyncio.Task[None]:
            return asyncio.create_task(
                _snapshot_loop(risk, snapshot_s, snapshot_timeout), name="equity-snapshots"
            )

        snapshot_task = start_snapshots()
        while not stop.is_set():
            if stream_task.done():
                stream_task.result()  # re-raise the stream's failure
                raise RuntimeError("market data stream ended unexpectedly")
            if snapshot_task.done():  # it never returns; only an escaped BaseException ends it
                error = None if snapshot_task.cancelled() else snapshot_task.exception()
                log.error("risk.snapshot_task_died", error=repr(error))
                snapshot_task = start_snapshots()
            await feed.tick()
            status = await gate.status()
            if status.killed != killed:
                if status.killed:
                    log.warning("core.kill_detected", reasons=list(status.reasons))
                elif killed is not None:
                    log.warning("core.kill_cleared")
                killed = status.killed
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(stop.wait(), timeout=tick_s)
        log.info("core.stopping")
    finally:
        if snapshot_task is not None:
            snapshot_task.cancel()
            await asyncio.gather(snapshot_task, return_exceptions=True)
        await source.stop()
        if stream_task is not None and not stream_task.done():
            stream_task.cancel()
            await asyncio.gather(stream_task, return_exceptions=True)
        await bus.close()
        await engine.dispose()


async def _snapshot_loop(risk: RiskEngine, interval_s: float, timeout_s: float) -> None:
    """Snapshot equity now, then every `interval_s`. Each attempt is bounded by
    `timeout_s`; a failure or timeout is logged and the loop carries on. Runs beside the
    core loop, so a hung REST call only delays the next snapshot, never kill detection."""
    loop = asyncio.get_running_loop()
    next_at = loop.time()
    while True:
        try:
            await asyncio.wait_for(risk.snapshot_equity(), timeout=timeout_s)
        except TimeoutError:
            log.error("risk.snapshot_failed", timeout=True)
        except Exception:
            log.exception("risk.snapshot_failed")
        else:
            log.info("risk.snapshot_written")
        next_at = max(next_at + interval_s, loop.time())
        await asyncio.sleep(next_at - loop.time())


async def _check_broker(connect_broker: BrokerConnect, settings: Settings) -> Any:
    broker = await connect_broker(settings)
    account = await broker.get_account()
    fees = broker.fee_schedule()
    log.info(
        "core.broker_connected",
        mode=broker.mode(),
        equity=str(account.equity),
        cash=str(account.cash),
        maker_bps=str(fees.maker_bps),
        taker_bps=str(fees.taker_bps),
    )
    return broker
