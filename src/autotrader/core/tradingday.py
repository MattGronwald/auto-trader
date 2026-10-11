"""Trading-day boundaries (G6, D5): 00:00 in `trading_day_tz`, returned as UTC."""

from datetime import UTC, datetime, time, timedelta
from zoneinfo import ZoneInfo


def day_start(now: datetime, tz: str) -> datetime:
    zone = ZoneInfo(tz)
    return datetime.combine(now.astimezone(zone).date(), time(), tzinfo=zone).astimezone(UTC)


def next_day_start(now: datetime, tz: str) -> datetime:
    zone = ZoneInfo(tz)
    tomorrow = now.astimezone(zone).date() + timedelta(days=1)
    return datetime.combine(tomorrow, time(), tzinfo=zone).astimezone(UTC)
