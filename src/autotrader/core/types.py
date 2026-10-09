"""Domain types shared across core, journal and API."""

from pydantic import AwareDatetime, BaseModel, ConfigDict


class Bar(BaseModel):
    """One OHLCV bar. Prices are float: bars feed indicator math, not money accounting."""

    model_config = ConfigDict(frozen=True)

    symbol: str
    timeframe: str
    ts: AwareDatetime  # bar open time, UTC
    open: float
    high: float
    low: float
    close: float
    volume: float
