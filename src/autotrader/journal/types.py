"""Portable column types: exact decimals and UTC timestamps on SQLite and Postgres."""

from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import DateTime, Dialect, Numeric, String
from sqlalchemy.types import TypeDecorator, TypeEngine


class Money(TypeDecorator[Decimal]):
    """Decimal for money and quantities.

    Postgres stores NUMERIC(28, 12). SQLite has no exact decimal type and would round
    through float, so there the value is stored as its exact string. Floats are rejected
    on write: a float has already lost precision before it reaches the DB.
    """

    impl = Numeric
    cache_ok = True

    def load_dialect_impl(self, dialect: Dialect) -> TypeEngine[Any]:
        if dialect.name == "sqlite":
            return dialect.type_descriptor(String(64))
        return dialect.type_descriptor(Numeric(28, 12, asdecimal=True))

    def process_bind_param(self, value: Decimal | None, dialect: Dialect) -> Any:
        if value is None:
            return None
        if not isinstance(value, Decimal):
            raise TypeError(f"Money columns take Decimal, got {type(value).__name__}")
        return str(value) if dialect.name == "sqlite" else value

    def process_result_value(self, value: Any, dialect: Dialect) -> Decimal | None:
        return None if value is None else Decimal(value)


class UTCDateTime(TypeDecorator[datetime]):
    """Timezone-aware datetime, normalised to UTC. Naive datetimes are rejected."""

    impl = DateTime(timezone=True)
    cache_ok = True

    def process_bind_param(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            raise ValueError("naive datetime: pass a timezone-aware value")
        utc = value.astimezone(UTC)
        # SQLite drops the offset on storage; store naive UTC and re-attach on read.
        return utc.replace(tzinfo=None) if dialect.name == "sqlite" else utc

    def process_result_value(self, value: datetime | None, dialect: Dialect) -> datetime | None:
        if value is None:
            return None
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
