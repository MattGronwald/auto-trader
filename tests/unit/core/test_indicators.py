"""WP 1.1 indicators (PLAN D12).

Two kinds of checks: hand-computed reference values on tiny windows (n = 2/3, so every
number below can be re-derived on paper), and hypothesis cross-checks of the vectorised
pandas code against naive loop implementations of the same D12 definitions.
"""

import math
from datetime import UTC, datetime, timedelta
from itertools import pairwise

import pandas as pd
import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from autotrader.core.features import SIGNAL_FEATURES
from autotrader.core.indicators import (
    atr,
    bollinger,
    compute_features,
    donchian,
    ema,
    rsi,
    vol_z,
    vwap,
)
from autotrader.core.types import Bar

T0 = datetime(2026, 10, 9, 0, 0, tzinfo=UTC)
M = timedelta(minutes=1)


def s(*values: float) -> pd.Series:
    return pd.Series(values, dtype=float)


def nan_count(series: pd.Series) -> int:
    return int(series.isna().sum())


def bar(
    i: int,
    c: float,
    *,
    h: float | None = None,
    low: float | None = None,
    v: float = 1.0,
    ts: datetime | None = None,
    symbol: str = "BTC/USD",
    tf: str = "1m",
) -> Bar:
    return Bar(
        symbol=symbol,
        timeframe=tf,
        ts=ts if ts is not None else T0 + i * M,
        open=c,
        high=h if h is not None else c,
        low=low if low is not None else c,
        close=c,
        volume=v,
    )


# --- reference values ---------------------------------------------------------------


def test_ema_is_seeded_with_first_value_and_nan_during_warmup() -> None:
    # alpha = 2/(3+1) = 0.5: 1 -> 0.5*2+0.5*1 = 1.5 -> 0.5*3+0.5*1.5 = 2.25 -> 3.125
    out = ema(s(1, 2, 3, 4), 3)
    assert nan_count(out) == 2
    assert out.iloc[2:].tolist() == pytest.approx([2.25, 3.125])


def test_rsi_wilder_seeded_with_sma_of_first_n_changes() -> None:
    # changes +1, -1, +2. n=2 seed at idx 2: gain 0.5, loss 0.5 -> 50.
    # idx 3: gain 0.5*0.5+0.5*2 = 1.25, loss 0.25 -> RS 5 -> 100 - 100/6.
    out = rsi(s(1, 2, 1, 3), 2)
    assert nan_count(out) == 2
    assert out.iloc[2:].tolist() == pytest.approx([50.0, 100 - 100 / 6])


def test_rsi_is_100_on_pure_gains_0_on_pure_losses_50_when_flat() -> None:
    assert rsi(s(1, 2, 3, 4), 2).iloc[-1] == 100.0
    assert rsi(s(4, 3, 2, 1), 2).iloc[-1] == 0.0
    assert rsi(s(5, 5, 5, 5), 2).iloc[-1] == 50.0


def test_atr_wilder_seeded_with_sma_of_first_n_true_ranges() -> None:
    # TR0 = h-l = 2; TR1 = max(3, |12-9|, |9-9|) = 3; TR2 = max(1, |11-11|, |10-11|) = 1.
    # ATR1 = (2+3)/2 = 2.5; ATR2 = 0.5*2.5 + 0.5*1 = 1.75.
    out = atr(s(10, 12, 11), s(8, 9, 10), s(9, 11, 10.5), 2)
    assert nan_count(out) == 1
    assert out.iloc[1:].tolist() == pytest.approx([2.5, 1.75])


def test_bollinger_uses_population_std() -> None:
    upper, middle, lower = bollinger(s(1, 2, 3), 3, 2.0)
    sd = math.sqrt(2 / 3)
    assert middle.iloc[-1] == pytest.approx(2.0)
    assert upper.iloc[-1] == pytest.approx(2 + 2 * sd)
    assert lower.iloc[-1] == pytest.approx(2 - 2 * sd)
    assert nan_count(middle) == 2


def test_donchian_includes_current_bar() -> None:
    high, low = donchian(s(5, 7, 6, 9), s(4, 3, 5, 8), 3)
    assert high.iloc[2:].tolist() == [7, 9]
    assert low.iloc[2:].tolist() == [3, 3]
    assert nan_count(high) == 2


def test_vol_z_population_std_and_nan_on_zero_spread() -> None:
    assert vol_z(s(1, 2, 3), 3).iloc[-1] == pytest.approx(1 / math.sqrt(2 / 3))
    assert math.isnan(vol_z(s(2, 2, 2), 3).iloc[-1])  # zero std -> NaN, never inf


@pytest.mark.parametrize("volume", [0.1, 0.3, 1.1, 7.0])
def test_vol_z_is_nan_for_constant_fractional_window(volume: float) -> None:
    # PR #11 review: the mean of 20 x 0.1 rounds away from 0.1, leaving a tiny nonzero
    # std and a fake finite z of +-1. D12 requires NaN for zero variance.
    assert math.isnan(vol_z(pd.Series([volume] * 20), 20).iloc[-1])


def test_vol_z_keeps_genuinely_small_variance() -> None:
    out = vol_z(pd.Series([0.1] * 19 + [0.1 + 1e-9]), 20).iloc[-1]
    assert out == pytest.approx(math.sqrt(19))


def test_bollinger_collapses_on_constant_fractional_window() -> None:
    upper, middle, lower = bollinger(pd.Series([0.1] * 20), 20, 2.0)
    assert upper.iloc[-1] == middle.iloc[-1] == lower.iloc[-1] == 0.1


def test_vwap_typical_price_weighted_and_resets_at_day_boundary() -> None:
    day1 = T0
    day2 = T0 + timedelta(days=1)
    ts = pd.DatetimeIndex([day1 + 10 * M, day2, day2 + M])
    high, low, close = s(3, 12, 30), s(1, 8, 24), s(2, 10, 27)
    volume = s(5, 1, 3)
    out = vwap(high, low, close, volume, ts, "UTC")
    # Day 1 is the first day in the window and may be partial -> NaN.
    assert math.isnan(out.iloc[0])
    # Day 2: typical 10 (v=1), then 27 (v=3) -> (10 + 81) / 4.
    assert out.iloc[1:].tolist() == pytest.approx([10.0, 91 / 4])


def test_vwap_day_boundary_follows_trading_day_tz() -> None:
    # 22:30 and 23:30 UTC on 2026-10-09 are 00:30 / 01:30 on 2026-10-10 in Berlin (CEST),
    # so in Berlin both belong to the same, second day of the window.
    ts = pd.DatetimeIndex([T0 + timedelta(hours=h) for h in (12, 22.5, 23.5)])
    ones = s(1, 1, 1)
    prices = s(1, 2, 4)
    utc = vwap(prices, prices, prices, ones, ts, "UTC")
    berlin = vwap(prices, prices, prices, ones, ts, "Europe/Berlin")
    assert utc.isna().all()  # one UTC day, partial
    assert math.isnan(berlin.iloc[0])
    assert berlin.iloc[1:].tolist() == pytest.approx([2.0, 3.0])


def test_vwap_is_nan_until_the_day_has_volume() -> None:
    ts = pd.DatetimeIndex([T0, T0 + timedelta(days=1), T0 + timedelta(days=1, minutes=1)])
    out = vwap(s(1, 2, 4), s(1, 2, 4), s(1, 2, 4), s(1, 0, 2), ts, "UTC")
    assert math.isnan(out.iloc[1])
    assert out.iloc[2] == pytest.approx(4.0)


# --- compute_features ---------------------------------------------------------------


def test_compute_features_returns_exactly_the_signal_vocabulary_aligned_to_bars() -> None:
    bars = [bar(i, 100 + i, v=1 + i % 3) for i in range(60)]
    features = compute_features(bars)
    assert set(features) == SIGNAL_FEATURES
    assert all(len(values) == 60 for values in features.values())
    assert features["close"][-1] == 159.0
    assert all(isinstance(x, float) for values in features.values() for x in values)
    # After the longest warm-up (EMA 50) everything but VWAP (first, partial day) is finite.
    finite = {k: v[-1] for k, v in features.items() if k != "vwap"}
    assert all(math.isfinite(x) for x in finite.values()), finite


def test_compute_features_empty_input() -> None:
    features = compute_features([])
    assert set(features) == SIGNAL_FEATURES
    assert all(values == [] for values in features.values())


def test_compute_features_rejects_mixed_series() -> None:
    with pytest.raises(ValueError, match="one symbol"):
        compute_features([bar(0, 1), bar(1, 1, symbol="ETH/USD")])
    with pytest.raises(ValueError, match="one symbol"):
        compute_features([bar(0, 1), bar(1, 1, tf="5m")])


def test_compute_features_rejects_unsorted_or_duplicate_bars() -> None:
    with pytest.raises(ValueError, match="ascending"):
        compute_features([bar(1, 1), bar(0, 1)])
    with pytest.raises(ValueError, match="ascending"):
        compute_features([bar(0, 1), bar(0, 1)])


def test_compute_features_vwap_uses_trading_day_tz() -> None:
    bars = [bar(0, 1, ts=T0 + timedelta(hours=12)), bar(1, 2, ts=T0 + timedelta(hours=22.5))]
    assert math.isnan(compute_features(bars)["vwap"][-1])
    assert compute_features(bars, trading_day_tz="Europe/Berlin")["vwap"][-1] == 2.0


# --- cross-checks against naive loops -----------------------------------------------

prices = st.lists(
    st.floats(min_value=1.0, max_value=1e6, allow_nan=False, allow_infinity=False),
    min_size=1,
    max_size=80,
)
windows = st.integers(min_value=2, max_value=20)


def naive_ema(xs: list[float], n: int) -> list[float]:
    a = 2 / (n + 1)
    out, prev = [], xs[0]
    for i, x in enumerate(xs):
        prev = x if i == 0 else a * x + (1 - a) * prev
        out.append(prev if i >= n - 1 else math.nan)
    return out


def naive_wilder(xs: list[float], n: int, first: int) -> list[float]:
    """SMA of xs[first:first+n] at index first+n-1, then avg = avg + (x - avg)/n."""
    out = [math.nan] * len(xs)
    seed_at = first + n - 1
    if seed_at >= len(xs):
        return out
    avg = sum(xs[first : first + n]) / n
    out[seed_at] = avg
    for i in range(seed_at + 1, len(xs)):
        avg = avg + (xs[i] - avg) / n
        out[i] = avg
    return out


def naive_rsi(xs: list[float], n: int) -> list[float]:
    diffs = [0.0] + [b - a for a, b in pairwise(xs)]
    gains = naive_wilder([max(d, 0.0) for d in diffs], n, 1)
    losses = naive_wilder([max(-d, 0.0) for d in diffs], n, 1)
    out = []
    for g, lo in zip(gains, losses, strict=True):
        if math.isnan(g):
            out.append(math.nan)
        elif g == 0 and lo == 0:
            out.append(50.0)
        elif lo == 0:
            out.append(100.0)
        else:
            out.append(100 - 100 / (1 + g / lo))
    return out


def assert_series_close(actual: pd.Series, expected: list[float]) -> None:
    assert len(actual) == len(expected)
    for a, e in zip(actual.tolist(), expected, strict=True):
        if math.isnan(e):
            assert math.isnan(a)
        else:
            assert a == pytest.approx(e, rel=1e-9, abs=1e-6)


@settings(max_examples=200)
@given(prices, windows)
def test_ema_matches_naive_loop(xs: list[float], n: int) -> None:
    assert_series_close(ema(pd.Series(xs), n), naive_ema(xs, n))


@settings(max_examples=200)
@given(prices, windows)
def test_rsi_matches_naive_loop_and_stays_in_bounds(xs: list[float], n: int) -> None:
    out = rsi(pd.Series(xs), n)
    assert_series_close(out, naive_rsi(xs, n))
    assert out.dropna().between(0, 100).all()


@settings(max_examples=200)
@given(
    st.lists(
        st.tuples(
            st.floats(min_value=1.0, max_value=1e5),  # low
            st.floats(min_value=0.0, max_value=1e3),  # high - low
            st.floats(min_value=0.0, max_value=1.0),  # close position in [low, high]
        ),
        min_size=1,
        max_size=80,
    ),
    windows,
)
def test_atr_and_channels_match_naive_definitions(
    rows: list[tuple[float, float, float]], n: int
) -> None:
    low = [lo for lo, _, _ in rows]
    high = [lo + span for lo, span, _ in rows]
    close = [lo + span * f for lo, span, f in rows]
    tr = [high[0] - low[0]] + [
        max(h - lo, abs(h - pc), abs(lo - pc))
        for h, lo, pc in zip(high[1:], low[1:], close, strict=False)
    ]
    out = atr(pd.Series(high), pd.Series(low), pd.Series(close), n)
    assert_series_close(out, naive_wilder(tr, n, 0))
    assert (out.dropna() >= 0).all()

    d_high, d_low = donchian(pd.Series(high), pd.Series(low), n)
    for i in range(n - 1, len(rows)):
        assert d_high.iloc[i] == max(high[i - n + 1 : i + 1])
        assert d_low.iloc[i] == min(low[i - n + 1 : i + 1])
        assert d_low.iloc[i] <= close[i] <= d_high.iloc[i]


@settings(max_examples=200)
@given(prices, windows)
def test_bollinger_bands_are_ordered_and_constant_series_collapses(xs: list[float], n: int) -> None:
    upper, middle, lower = bollinger(pd.Series(xs), n, 2.0)
    valid = middle.notna()
    assert (lower[valid] <= middle[valid] + 1e-9).all()
    assert (middle[valid] <= upper[valid] + 1e-9).all()
    flat_upper, flat_mid, flat_lower = bollinger(pd.Series([xs[0]] * len(xs)), n, 2.0)
    assert_series_close(
        flat_mid,
        [math.nan] * (n - 1) + [xs[0]] * (len(xs) - n + 1)
        if len(xs) >= n
        else [math.nan] * len(xs),
    )
    assert flat_upper.dropna().tolist() == pytest.approx(flat_lower.dropna().tolist())


@settings(max_examples=100)
@given(prices)
def test_ema_of_constant_is_constant(xs: list[float]) -> None:
    out = ema(pd.Series([xs[0]] * 60), 50).dropna()
    assert out.tolist() == pytest.approx([xs[0]] * len(out))


@settings(max_examples=100)
@given(st.lists(st.floats(min_value=0.0, max_value=1e6), min_size=1, max_size=60), windows)
def test_vol_z_never_infinite(volumes: list[float], n: int) -> None:
    out = vol_z(pd.Series(volumes), n)
    assert not out.isin([math.inf, -math.inf]).any()
