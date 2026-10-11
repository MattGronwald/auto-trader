"""Rule-based stand-in for the Decision Maker in Phase 1 (SPEC §12, PLAN D15).

Module constants, not profile keys (owner decision, D15): this file is deleted when the
LLM Decision Maker lands in Phase 2, and the profile hash stays unchanged.

The stop is measured from `ask x (1 + entry_slippage_bps)`, the engine's entry before
tick rounding. The engine rounds that entry *up* by less than one tick, so for the same
ask its stop distance lies in [1.5 x ATR, 1.5 x ATR + tick) (spec §7).
"""

from dataclasses import dataclass
from decimal import Decimal

from autotrader.core.config import RiskLimits

MIN_SCORE = Decimal("0.5")
STOP_ATR = Decimal("1.5")
TP_R = Decimal("2")
SIZE_FRACTION = Decimal("1")
CONFIDENCE_MARGIN = Decimal("0.10")
_BPS = Decimal(10_000)


@dataclass(frozen=True)
class StubBuy:
    stop: Decimal
    take_profit: Decimal
    confidence: Decimal
    size_fraction: Decimal


@dataclass(frozen=True)
class StubSkip:
    reason: str


def decide(score: float, ask: Decimal, atr: Decimal, limits: RiskLimits) -> StubBuy | StubSkip:
    """BUY with a fixed ATR stop and a 2R target if the score is high enough.

    Requires `ask > 0` and `atr > 0` (the Cycle Runner's ENRICHING step ensures both).
    """
    if Decimal(str(score)) < MIN_SCORE:
        return StubSkip(f"score {score} < {MIN_SCORE}")
    est_entry = ask * (1 + limits.entry_slippage_bps / _BPS)
    stop = est_entry - STOP_ATR * atr
    if stop <= 0:
        return StubSkip(f"stop {stop} <= 0")
    take_profit = est_entry + TP_R * (est_entry - stop)
    confidence = min(Decimal(1), limits.default_win_rate_prior + CONFIDENCE_MARGIN)
    return StubBuy(stop, take_profit, confidence, SIZE_FRACTION)
