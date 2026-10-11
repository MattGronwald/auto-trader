"""Technical indicators for the signal scanner (WP 1.1, definitions in PLAN.md D12).

`compute_features(bars)` turns one symbol's bars of one timeframe into the feature dict that
`core/expr.py` evaluates: keys are exactly `SIGNAL_FEATURES`, each value a list aligned with
the input bars (oldest first, latest last), so `close` is `env["close"][-1]` and
`donchian_high_20[-1]` in a rule is the previous bar's channel.

Contract for callers (Scanner, WP 1.2): positions without enough history are NaN. The
expression evaluator rejects non-finite inputs, so a symbol must be skipped until the
features a rule reads are finite. Never replace NaN with a number: a fake 0 would make
rules fire on warm-up data.

Definitions (D12):
- EMA n: alpha = 2/(n+1), seeded with the first value, NaN for the first n-1 bars.
- RSI n, ATR n: Wilder smoothing (alpha = 1/n) seeded with the SMA of the first n inputs
  (RSI: first n price changes, so the first value is at index n; ATR: first n true ranges,
  the first true range being high - low, so the first value is at index n-1).
  RSI is 100 with no losses in the average, 50 with neither gains nor losses.
- Bollinger n, k: SMA n ± k x population std (ddof 0).
- Donchian n: rolling max high / min low including the current bar.
- vol_z n: (volume - SMA n) / population std n; a constant window (zero std, detected
  exactly) gives NaN, never inf or a rounding artefact.
- VWAP: typical price (h+l+c)/3 weighted by volume, cumulative from the start of the
  trading day in `trading_day_tz` (D5). The first day in the window may be partial (the
  window can start mid-day), so its VWAP is NaN; it is also NaN until the day has volume.
"""

from collections.abc import Sequence
from itertools import pairwise

import numpy as np
import pandas as pd
from numpy.lib.stride_tricks import sliding_window_view

from autotrader.core.features import SIGNAL_FEATURES
from autotrader.core.types import Bar

EMA_PERIODS = (9, 21, 50)
RSI_PERIOD = 14
ATR_PERIOD = 14
BOLLINGER_PERIOD = 20
BOLLINGER_K = 2.0
DONCHIAN_PERIOD = 20
VOL_Z_PERIOD = 20


def ema(values: pd.Series, n: int) -> pd.Series:
    return values.ewm(span=n, adjust=False, min_periods=n).mean()


def _wilder(values: pd.Series, n: int, first: int) -> pd.Series:
    """Wilder average of `values[first:]`, seeded with their first-n SMA at first+n-1."""
    seed_at = first + n - 1
    if len(values) <= seed_at:
        return pd.Series(np.nan, index=values.index, dtype=float)
    seeded = values.astype(float).copy()
    seeded.iloc[:seed_at] = np.nan
    seeded.iloc[seed_at] = values.iloc[first : seed_at + 1].mean()
    return seeded.ewm(alpha=1 / n, adjust=False).mean()


def rsi(close: pd.Series, n: int) -> pd.Series:
    change = close.diff()
    gain = _wilder(change.clip(lower=0), n, first=1)
    loss = _wilder((-change).clip(lower=0), n, first=1)
    with np.errstate(divide="ignore", invalid="ignore"):
        raw = 100 - 100 / (1 + gain / loss)
    out = np.where(loss == 0, np.where(gain == 0, 50.0, 100.0), raw)
    return pd.Series(out, index=close.index).where(gain.notna())


def atr(high: pd.Series, low: pd.Series, close: pd.Series, n: int) -> pd.Series:
    prev = close.shift(1)
    true_range = pd.concat([high - low, (high - prev).abs(), (low - prev).abs()], axis=1).max(
        axis=1
    )
    return _wilder(true_range, n, first=0)


def _rolling_mean_std(values: pd.Series, n: int) -> tuple[pd.Series, pd.Series]:
    """Two-pass mean/population std per window. A constant window is detected exactly
    (max == min) and gets its value as mean and std 0: summing e.g. 20 x 0.1 rounds away
    from 0.1, which would otherwise leave a tiny std and a fake finite z of +-1."""
    mean = np.full(len(values), np.nan)
    std = mean.copy()
    if len(values) >= n:
        windows = sliding_window_view(values.to_numpy(dtype=float), n)
        constant = windows.max(axis=1) == windows.min(axis=1)
        mean[n - 1 :] = np.where(constant, windows[:, 0], windows.mean(axis=1))
        std[n - 1 :] = np.where(constant, 0.0, windows.std(axis=1))
    return pd.Series(mean, index=values.index), pd.Series(std, index=values.index)


def bollinger(close: pd.Series, n: int, k: float) -> tuple[pd.Series, pd.Series, pd.Series]:
    middle, std = _rolling_mean_std(close, n)
    return middle + k * std, middle, middle - k * std


def donchian(high: pd.Series, low: pd.Series, n: int) -> tuple[pd.Series, pd.Series]:
    return high.rolling(n).max(), low.rolling(n).min()


def vol_z(volume: pd.Series, n: int) -> pd.Series:
    mean, std = _rolling_mean_std(volume, n)
    return (volume - mean) / std.where(std > 0)


def vwap(
    high: pd.Series,
    low: pd.Series,
    close: pd.Series,
    volume: pd.Series,
    ts: pd.DatetimeIndex,
    trading_day_tz: str,
) -> pd.Series:
    day = ts.tz_convert(trading_day_tz).normalize().to_numpy()
    weighted = (high + low + close) / 3 * volume
    cum_volume = volume.groupby(day).cumsum()
    out = weighted.groupby(day).cumsum() / cum_volume.where(cum_volume > 0)
    if len(day):
        out[day == day[0]] = np.nan
    return out


def compute_features(bars: Sequence[Bar], *, trading_day_tz: str = "UTC") -> dict[str, list[float]]:
    """Feature dict for one symbol/timeframe; see the module docstring for the contract."""
    if len({(b.symbol, b.timeframe) for b in bars}) > 1:
        raise ValueError("compute_features needs bars of one symbol and one timeframe")
    if any(a.ts >= b.ts for a, b in pairwise(bars)):
        raise ValueError("bars must be strictly ascending by ts")

    def column(name: str) -> pd.Series:
        return pd.Series([getattr(b, name) for b in bars], dtype=float)

    o, h, lo, c, v = (column(n) for n in ("open", "high", "low", "close", "volume"))
    ts = pd.DatetimeIndex([b.ts for b in bars]) if bars else pd.DatetimeIndex([], tz="UTC")
    bb_upper, bb_middle, bb_lower = bollinger(c, BOLLINGER_PERIOD, BOLLINGER_K)
    dc_high, dc_low = donchian(h, lo, DONCHIAN_PERIOD)
    series: dict[str, pd.Series] = {
        "open": o,
        "high": h,
        "low": lo,
        "close": c,
        "volume": v,
        **{f"ema{n}": ema(c, n) for n in EMA_PERIODS},
        "rsi14": rsi(c, RSI_PERIOD),
        "atr14": atr(h, lo, c, ATR_PERIOD),
        "vwap": vwap(h, lo, c, v, ts, trading_day_tz),
        "vol_z": vol_z(v, VOL_Z_PERIOD),
        "bb_upper": bb_upper,
        "bb_middle": bb_middle,
        "bb_lower": bb_lower,
        "donchian_high_20": dc_high,
        "donchian_low_20": dc_low,
    }
    if series.keys() != SIGNAL_FEATURES:
        raise RuntimeError("indicator set out of sync with core/features.py SIGNAL_FEATURES")
    return {name: [float(x) for x in s.to_numpy(dtype=float)] for name, s in series.items()}
