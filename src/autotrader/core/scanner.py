"""Signal Scanner (SPEC §4.2, PLAN D13/G14). Deterministic, no LLM.

On every `BarClosed` of the profile's signal timeframe: compute features for the symbol
(`core/indicators.py`), evaluate each signal rule, pick the best hit (highest score,
profile order breaks ties) and, if no pre-filter objects, publish one `CandidateSignal`.

Rules run before the pre-filters so every suppressed hit is logged with its reason
(`scanner.suppressed`); WP 1.7 uses that to judge rules on this sparse venue (G13).

A rule whose evaluation touches a NaN feature (warm-up, zero-variance vol_z, VWAP on a
window's first day) raises `EvaluationError` in the evaluator. That rule is "not
evaluable" for this bar: logged at debug, never a hit.

Pre-filters (D13), in order: obsolete bar (more than one signal period has passed since
the bar ended; reconnect catch-up announces old bars while the symbol already reads
fresh, and those must neither become candidates nor use up the rate limit), kill/pause
(`ControlGate`, fails closed), stale symbol, spread above `max_spread_bps` or unknown,
vol_z below `min_vol_z` or undefined, open position on the symbol, open cycle on the
symbol, open cycles at `max_concurrent_cycles`,
`max_candidates_per_symbol_per_hour` reached in the last 60 min (counted from persisted
candidates, so a restart doesn't reset it).

The concurrency checks are soft: bars of all symbols close together, so several
candidates can go out before any cycle row exists. The Cycle Runner (`core/cycle.py`)
enforces both caps authoritatively when it admits a cycle (G14, D15). The daily loss limit
needs no check here: the Risk Engine sets `paused_until` (WP 1.3), which the gate reports.
"""

import math
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from typing import Annotated, Literal, Protocol

import structlog
from pydantic import AwareDatetime, Field
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.core.bus import DomainEvent, EventBus
from autotrader.core.config import SignalRule, StrategyProfile
from autotrader.core.expr import EvaluationError
from autotrader.core.feed import BarClosed
from autotrader.core.indicators import compute_features
from autotrader.core.kill import ControlStatus
from autotrader.core.resample import parse_timeframe
from autotrader.core.types import Bar
from autotrader.journal import repo

log = structlog.get_logger(__name__)

RATE_WINDOW = timedelta(hours=1)

Finite = Annotated[float, Field(allow_inf_nan=False)]


class CandidateSignal(DomainEvent):
    symbol: str
    direction: Literal["long"]
    rule_id: str
    score: Finite
    timeframe: str
    bar_ts: AwareDatetime  # open time of the bar the rule fired on
    features: dict[str, Finite | None]  # latest value per feature; NaN -> None


class MarketView(Protocol):
    def history(self, symbol: str, timeframe: str) -> list[Bar]: ...

    def spread_bps(self, symbol: str) -> float | None: ...

    def is_stale(self, symbol: str) -> bool: ...


class Gate(Protocol):
    async def status(self, now: datetime | None = None) -> ControlStatus: ...


class Scanner:
    def __init__(
        self,
        profile: StrategyProfile,
        market: MarketView,
        gate: Gate,
        sessions: async_sessionmaker[AsyncSession],
        bus: EventBus,
        *,
        trading_day_tz: str = "UTC",
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._profile = profile
        self._limits = profile.scanner
        self._timeframe = profile.timeframes.signal
        self._period = parse_timeframe(self._timeframe)
        self._market = market
        self._gate = gate
        self._sessions = sessions
        self._bus = bus
        self._tz = trading_day_tz
        self._clock = clock

    async def on_bar_closed(self, event: BarClosed) -> None:
        if event.timeframe != self._timeframe:
            return
        symbol = event.symbol
        # The feed may have appended newer bars before this handler ran.
        bars = [b for b in self._market.history(symbol, self._timeframe) if b.ts <= event.bar_ts]
        if not bars or bars[-1].ts != event.bar_ts:
            log.warning("scanner.bar_missing", symbol=symbol, bar_ts=event.bar_ts.isoformat())
            return
        env = compute_features(bars, trading_day_tz=self._tz)
        hit = self._best_hit(symbol, env)
        if hit is None:
            return
        rule, score = hit
        latest = {k: (v[-1] if math.isfinite(v[-1]) else None) for k, v in env.items()}
        now = self._clock()
        reason = await self._suppression(symbol, event.bar_ts, latest, now)
        if reason is not None:
            log.info(
                "scanner.suppressed", symbol=symbol, rule_id=rule.id, score=score, reason=reason
            )
            return
        await self._bus.publish(
            CandidateSignal(
                ts=now,
                symbol=symbol,
                direction=rule.direction,
                rule_id=rule.id,
                score=score,
                timeframe=self._timeframe,
                bar_ts=event.bar_ts,
                features=latest,
            )
        )
        log.info("scanner.candidate", symbol=symbol, rule_id=rule.id, score=score)

    def _best_hit(
        self, symbol: str, env: dict[str, list[float]]
    ) -> tuple[SignalRule, float] | None:
        best: tuple[SignalRule, float] | None = None
        for rule in self._profile.signal_rules:
            try:
                if not rule.expr.evaluate_bool(env):
                    continue
                score = rule.score.evaluate_number(env)
            except EvaluationError as e:
                log.debug(
                    "scanner.rule_not_evaluable", symbol=symbol, rule_id=rule.id, error=str(e)
                )
                continue
            if best is None or score > best[1]:
                best = (rule, score)
        return best

    async def _suppression(
        self, symbol: str, bar_ts: datetime, latest: dict[str, float | None], now: datetime
    ) -> str | None:
        """First pre-filter that blocks a candidate, or None."""
        age = now - (bar_ts + self._period)
        if age > self._period:
            return f"obsolete: bar ended {age.total_seconds():.0f} s ago"
        status = await self._gate.status(now)
        if not status.entries_allowed:
            return "control: " + "; ".join(status.reasons)
        if self._market.is_stale(symbol):
            return "stale"
        spread = self._market.spread_bps(symbol)
        if spread is None:
            return "spread unknown"
        if spread > self._limits.max_spread_bps:
            return f"spread {spread:.1f} bps > {self._limits.max_spread_bps}"
        vol_z = latest["vol_z"]
        if vol_z is None or vol_z < self._limits.min_vol_z:
            return f"vol_z {vol_z} < {self._limits.min_vol_z}"
        async with self._sessions() as session:
            if symbol in await repo.open_position_symbols(session):
                return "open position"
            cycles = await repo.open_cycle_symbols(session)
            if symbol in cycles:
                return "open cycle"
            if len(cycles) >= self._limits.max_concurrent_cycles:
                return f"{len(cycles)} open cycles >= {self._limits.max_concurrent_cycles}"
            recent = await repo.events_since(session, CandidateSignal.__name__, now - RATE_WINDOW)
        sent = sum(1 for e in recent if e.payload.get("symbol") == symbol)
        if sent >= self._limits.max_candidates_per_symbol_per_hour:
            return f"rate limit: {sent} candidates in the last hour"
        return None
