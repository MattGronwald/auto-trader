from datetime import UTC, datetime, timedelta

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from autotrader.core.resample import Resampler, bucket_start, parse_timeframe
from autotrader.core.types import Bar

T0 = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
M = timedelta(minutes=1)
GRACE = timedelta(seconds=10)


def m1(
    minute: int, o: float, h: float, low: float, c: float, v: float = 1.0, symbol: str = "BTC/USD"
) -> Bar:
    return Bar(
        symbol=symbol,
        timeframe="1m",
        ts=T0 + minute * M,
        open=o,
        high=h,
        low=low,
        close=c,
        volume=v,
    )


def flat(minute: int, price: float = 100.0, symbol: str = "BTC/USD") -> Bar:
    return m1(minute, price, price, price, price, symbol=symbol)


@pytest.mark.parametrize(
    ("text", "expected"),
    [
        ("1m", M),
        ("5m", 5 * M),
        ("15m", 15 * M),
        ("1h", timedelta(hours=1)),
        ("4h", timedelta(hours=4)),
        ("1d", timedelta(days=1)),
    ],
)
def test_parse_timeframe(text: str, expected: timedelta) -> None:
    assert parse_timeframe(text) == expected


@pytest.mark.parametrize("text", ["5x", "0m", "m", "7m"])  # 7m does not divide an hour/day
def test_parse_timeframe_rejects(text: str) -> None:
    with pytest.raises(ValueError, match="timeframe"):
        parse_timeframe(text)


def test_buckets_align_to_utc_epoch() -> None:
    ts = datetime(2026, 10, 9, 13, 47, 30, tzinfo=UTC)

    assert bucket_start(ts, parse_timeframe("5m")) == datetime(2026, 10, 9, 13, 45, tzinfo=UTC)
    assert bucket_start(ts, parse_timeframe("4h")) == datetime(2026, 10, 9, 12, 0, tzinfo=UTC)


def test_five_minute_bar_aggregates_ohlcv_and_closes_by_time() -> None:
    r = Resampler(["5m"], grace=GRACE)
    for i, (o, h, low, c) in enumerate(
        [
            (100, 101, 99, 100.5),
            (100.5, 103, 100, 102),
            (102, 102.5, 98, 99),
            (99, 100, 97, 99.5),
            (99.5, 101, 99, 100),
        ]
    ):
        assert r.add(m1(i, o, h, low, c, v=2.0)) == []

    assert r.close_until(T0 + 5 * M) == []  # still inside the grace period
    (bar,) = r.close_until(T0 + 5 * M + GRACE)

    assert (bar.timeframe, bar.ts) == ("5m", T0)
    assert (bar.open, bar.high, bar.low, bar.close, bar.volume) == (100, 103, 97, 100, 10.0)


def test_missing_minutes_still_close_the_bucket() -> None:
    # Alpaca crypto: many minutes have no bar. The bucket closes on time anyway.
    r = Resampler(["5m"], grace=GRACE)
    r.add(m1(1, 100, 105, 95, 101))

    (bar,) = r.close_until(T0 + 5 * M + GRACE)

    assert (bar.open, bar.high, bar.low, bar.close) == (100, 105, 95, 101)


def test_bar_of_next_bucket_closes_the_previous_one() -> None:
    r = Resampler(["5m"], grace=GRACE)
    r.add(m1(0, 100, 101, 99, 100))

    (closed,) = r.add(m1(5, 100, 100, 100, 100))

    assert closed.ts == T0


def test_empty_buckets_become_flat_bars_at_previous_close() -> None:
    r = Resampler(["5m"], grace=GRACE)
    r.add(m1(0, 100, 101, 99, 100.5))

    bars = r.close_until(T0 + 15 * M + GRACE)

    assert [b.ts for b in bars] == [T0, T0 + 5 * M, T0 + 10 * M]
    assert [(b.open, b.high, b.low, b.close, b.volume) for b in bars[1:]] == [
        (100.5, 100.5, 100.5, 100.5, 0.0)
    ] * 2


def test_no_bars_before_first_observation() -> None:
    # A symbol never seen has no previous close to fill from.
    r = Resampler(["5m"], grace=GRACE)

    assert r.close_until(T0 + 60 * M) == []


def test_late_bar_for_closed_bucket_is_ignored() -> None:
    r = Resampler(["5m"], grace=GRACE)
    r.add(m1(0, 100, 101, 99, 100))
    r.close_until(T0 + 5 * M + GRACE)

    assert r.add(m1(4, 1, 1, 1, 1)) == []
    assert r.late_bars == 1


def test_multiple_timeframes_and_symbols_independent() -> None:
    r = Resampler(["5m", "15m"], grace=GRACE)
    bars: list[Bar] = []
    for i in range(15):
        bars += r.add(flat(i, 100 + i))
        bars += r.add(flat(i, 10, symbol="ETH/USD"))

    bars += r.close_until(T0 + 15 * M + GRACE)

    by = {(b.symbol, b.timeframe, b.ts) for b in bars}
    assert ("BTC/USD", "15m", T0) in by
    assert ("ETH/USD", "5m", T0 + 10 * M) in by
    assert len(bars) == 2 * (3 + 1)
    btc15 = next(b for b in bars if b.symbol == "BTC/USD" and b.timeframe == "15m")
    assert (btc15.open, btc15.close) == (100, 114)


def test_closed_bars_come_out_in_time_order() -> None:
    r = Resampler(["5m", "15m"], grace=GRACE)
    r.add(flat(0))

    bars = r.close_until(T0 + 30 * M + GRACE)

    ends = [b.ts + parse_timeframe(b.timeframe) for b in bars]
    assert ends == sorted(ends)


def test_only_one_minute_input_accepted() -> None:
    r = Resampler(["5m"], grace=GRACE)
    five = Bar(symbol="BTC/USD", timeframe="5m", ts=T0, open=1, high=1, low=1, close=1, volume=1)

    with pytest.raises(ValueError, match="1m"):
        r.add(five)


@given(
    st.lists(
        st.tuples(st.integers(0, 179), st.floats(50, 150), st.floats(0, 5)),
        min_size=1,
        max_size=120,
        unique_by=lambda t: t[0],
    ),
    st.sampled_from(["5m", "15m", "1h"]),
)
@settings(max_examples=300, deadline=None)
def test_matches_brute_force_on_sparse_minutes(
    raw: list[tuple[int, float, float]], tf_name: str
) -> None:
    tf = parse_timeframe(tf_name)
    minutes = sorted(raw)
    r = Resampler([tf_name], grace=GRACE)
    out: list[Bar] = []
    for minute, price, vol in minutes:
        out += r.add(m1(minute, price, price + 1, price - 1, price, v=vol))
    out += r.close_until(T0 + 200 * M + GRACE)

    # Brute force: contiguous buckets from the first bar to the horizon; empty ones flat.
    first = bucket_start(T0 + minutes[0][0] * M, tf)
    expected: list[tuple[datetime, float, float, float, float, float]] = []
    cursor, prev = first, None
    while cursor + tf <= T0 + 200 * M:
        inside = [t for t in minutes if cursor <= T0 + t[0] * M < cursor + tf]
        if inside:
            prices = [p for _, p, _ in inside]
            expected.append(
                (
                    cursor,
                    inside[0][1],
                    max(prices) + 1,
                    min(prices) - 1,
                    inside[-1][1],
                    sum(v for _, _, v in inside),
                )
            )
            prev = inside[-1][1]
        elif prev is not None:
            expected.append((cursor, prev, prev, prev, prev, 0.0))
        cursor += tf

    got = [(b.ts, b.open, b.high, b.low, b.close, b.volume) for b in out]
    assert [g[:5] for g in got] == [e[:5] for e in expected]
    assert [g[5] for g in got] == pytest.approx([e[5] for e in expected])
