from datetime import UTC, datetime

from autotrader.core.tradingday import day_start, next_day_start


def test_utc_day_boundaries() -> None:
    now = datetime(2026, 10, 9, 15, 30, tzinfo=UTC)
    assert day_start(now, "UTC") == datetime(2026, 10, 9, tzinfo=UTC)
    assert next_day_start(now, "UTC") == datetime(2026, 10, 10, tzinfo=UTC)


def test_berlin_day_on_the_25_hour_dst_day() -> None:
    now = datetime(2026, 10, 25, 22, 30, tzinfo=UTC)  # 23:30 CET on the 25th
    assert day_start(now, "Europe/Berlin") == datetime(2026, 10, 24, 22, tzinfo=UTC)  # CEST
    assert next_day_start(now, "Europe/Berlin") == datetime(2026, 10, 25, 23, tzinfo=UTC)
