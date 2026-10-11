"""1m -> higher timeframes (PLAN G5).

Buckets are aligned to the UTC epoch (5m at :00/:05/..., 4h at 00/04/08/... UTC). A bucket
closes when time passes its end plus `grace` (`close_until`) or when a 1m bar of a later
bucket arrives (`add`), whichever comes first. Closing by time matters on Alpaca's
sparse crypto feed, where many minutes have no bar at all.

A bucket with no 1m bars becomes a flat bar at the previous close with zero volume, so
higher-timeframe series stay evenly spaced for indicators. Nothing is emitted for a
symbol before its first bar. A 1m bar arriving for an already closed bucket is counted
in `late_bars` and ignored for that timeframe, including as the flat-fill reference
(it is still persisted by the feed).
"""

import re
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from autotrader.core.types import Bar

_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)
_UNITS = {"m": timedelta(minutes=1), "h": timedelta(hours=1), "d": timedelta(days=1)}


def parse_timeframe(text: str) -> timedelta:
    match = re.fullmatch(r"([1-9]\d*)([mhd])", text)
    if match is None:
        raise ValueError(f"invalid timeframe {text!r}")
    tf = int(match.group(1)) * _UNITS[match.group(2)]
    if timedelta(days=1) % tf:
        raise ValueError(f"timeframe {text!r} must divide a day evenly")
    return tf


def bucket_start(ts: datetime, tf: timedelta) -> datetime:
    return ts - (ts - _EPOCH) % tf


@dataclass
class _Bucket:
    start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float


class Resampler:
    def __init__(self, timeframes: list[str], grace: timedelta) -> None:
        self._tfs = {name: parse_timeframe(name) for name in timeframes}
        self._grace = grace
        self._open: dict[tuple[str, str], _Bucket] = {}  # (symbol, tf) -> current bucket
        self._next: dict[tuple[str, str], datetime] = {}  # first bucket start not yet closed
        self._last_close: dict[tuple[str, str], float] = {}  # accepted bars only, per tf
        self.late_bars = 0

    def add(self, bar: Bar) -> list[Bar]:
        """Feed one 1m bar; returns higher-timeframe bars this closed."""
        if bar.timeframe != "1m":
            raise ValueError(f"resampler takes 1m bars, got {bar.timeframe}")
        closed: list[Bar] = []
        for name, tf in self._tfs.items():
            key = (bar.symbol, name)
            start = bucket_start(bar.ts, tf)
            if key in self._next and start < self._next[key]:
                self.late_bars += 1
                continue
            closed += self._close_before(bar.symbol, name, start)
            b = self._open.get(key)
            if b is None:
                self._open[key] = _Bucket(start, bar.open, bar.high, bar.low, bar.close, bar.volume)
                self._next.setdefault(key, start)
            else:
                b.high, b.low = max(b.high, bar.high), min(b.low, bar.low)
                b.close, b.volume = bar.close, b.volume + bar.volume
            self._last_close[key] = bar.close
        closed.sort(key=_end)
        return closed

    def close_until(self, now: datetime) -> list[Bar]:
        """Close every bucket whose end + grace has passed; returns bars in time order."""
        closed: list[Bar] = []
        for symbol, name in list(self._next):
            tf = self._tfs[name]
            cutoff = bucket_start(now - self._grace, tf)  # buckets starting before this are done
            closed += self._close_before(symbol, name, cutoff)
        closed.sort(key=_end)
        return closed

    def _close_before(self, symbol: str, name: str, start: datetime) -> list[Bar]:
        """Emit all buckets of (symbol, tf) that start before `start`, filling gaps flat."""
        key, tf = (symbol, name), self._tfs[name]
        cursor = self._next.get(key)
        if cursor is None:
            return []
        out: list[Bar] = []
        b = self._open.get(key)
        # In add() this runs before the new bar updates _last_close, so it is the close of
        # the last accepted bar *before* the gap; in close_until() the open bucket is
        # emitted first. Rejected late bars never touch it.
        prev_close = self._last_close.get(key)
        while cursor < start:
            if b is not None and b.start == cursor:
                out.append(_to_bar(symbol, name, b))
                prev_close = b.close
                del self._open[key]
                b = None
            elif prev_close is not None:
                p = prev_close
                out.append(
                    Bar(
                        symbol=symbol,
                        timeframe=name,
                        ts=cursor,
                        open=p,
                        high=p,
                        low=p,
                        close=p,
                        volume=0.0,
                    )
                )
            cursor += tf
        self._next[key] = cursor
        return out


def _to_bar(symbol: str, name: str, b: _Bucket) -> Bar:
    return Bar(
        symbol=symbol,
        timeframe=name,
        ts=b.start,
        open=b.open,
        high=b.high,
        low=b.low,
        close=b.close,
        volume=b.volume,
    )


def _end(bar: Bar) -> datetime:
    return bar.ts + parse_timeframe(bar.timeframe)
