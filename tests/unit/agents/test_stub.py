"""Phase-1 stub Decision Maker (spec §7)."""

from decimal import Decimal as D
from pathlib import Path

from hypothesis import assume, given
from hypothesis import strategies as st

from autotrader.agents.stub import STOP_ATR, StubBuy, StubSkip, decide
from autotrader.core.config import load_profile
from autotrader.risk.checks import entry_limit

LIMITS = load_profile(
    Path(__file__).resolve().parents[3] / "strategies" / "fast_momentum_v1.yaml"
).risk


def test_skips_below_min_score() -> None:
    got = decide(0.49, D("100"), D("2"), LIMITS)
    assert isinstance(got, StubSkip) and got.reason.startswith("score 0.49")


def test_buy_fields() -> None:
    got = decide(0.8, D("100.0"), D("2.0"), LIMITS)
    # est entry = 100 x 1.001 = 100.1; stop = 100.1 - 1.5 x 2; tp = 100.1 + 2 x 3
    assert got == StubBuy(
        stop=D("97.1"), take_profit=D("106.1"), confidence=D("0.50"), size_fraction=D("1")
    )


def test_skips_when_the_stop_would_not_be_positive() -> None:
    got = decide(0.8, D("1"), D("5"), LIMITS)
    assert isinstance(got, StubSkip) and "<= 0" in got.reason


def test_regression_tick_rounding_counterexample() -> None:
    # Review counterexample (2026-10-11): with the stop measured from the *rounded*
    # entry the distance exceeded 3 x ATR. Measured from ask x (1 + slip) it stays put.
    atr, tick = D("0.067"), D("0.01")
    got = decide(0.8, D("100.003"), atr, LIMITS)
    assert isinstance(got, StubBuy)
    distance = entry_limit(D("100.003"), LIMITS.entry_slippage_bps, tick) - got.stop
    assert STOP_ATR * atr <= distance < STOP_ATR * atr + tick
    assert LIMITS.min_stop_atr * atr <= distance <= LIMITS.max_stop_atr * atr


@given(
    ask=st.decimals(min_value=D("0.01"), max_value=D("200000"), places=4),
    atr=st.decimals(min_value=D("0.0001"), max_value=D("5000"), places=6),
    tick=st.sampled_from([D("0.0001"), D("0.001"), D("0.01"), D("0.1"), D("1")]),
)
def test_stop_distance_from_the_engines_rounded_entry_stays_in_band(
    ask: D, atr: D, tick: D
) -> None:
    assume(tick < (LIMITS.max_stop_atr - STOP_ATR) * atr)
    got = decide(0.8, ask, atr, LIMITS)
    assume(isinstance(got, StubBuy))
    assert isinstance(got, StubBuy)
    distance = entry_limit(ask, LIMITS.entry_slippage_bps, tick) - got.stop
    assert STOP_ATR * atr <= distance < STOP_ATR * atr + tick
    assert LIMITS.min_stop_atr * atr <= distance <= LIMITS.max_stop_atr * atr
