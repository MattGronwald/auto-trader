"""Configuration: `config.yaml` (infra), `strategies/<name>.yaml` (profile), env (secrets).

Everything is validated at load and fails fast with a `ConfigError` naming the file and
field. Models are frozen: nothing at runtime mutates config, and the risk block in
particular is human-editable only (SPEC §4.5, §8.3).
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Callable, Mapping
from decimal import Decimal
from functools import cached_property
from pathlib import Path
from typing import Annotated, Any, Literal, Self
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import yaml
from pydantic import (
    AfterValidator,
    BaseModel,
    ConfigDict,
    Field,
    PlainSerializer,
    PlainValidator,
    SecretStr,
    StringConstraints,
    ValidationError,
    ValidationInfo,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, SettingsConfigDict
from sqlalchemy.engine import make_url
from sqlalchemy.exc import ArgumentError

from autotrader.core.expr import Expression, ExpressionError
from autotrader.core.features import EVIDENCE_FIELDS, SIGNAL_FEATURES


class ConfigError(ValueError):
    """Config or profile is missing, malformed or invalid."""


# --- secrets (D2) --------------------------------------------------------------------


class Settings(BaseSettings):
    """Secrets and switches from env / `.env`. Real env vars override `.env`."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore", frozen=True)

    alpaca_api_key_id: SecretStr | None = None
    alpaca_api_secret_key: SecretStr | None = None
    alpaca_paper: bool = True
    anthropic_api_key: SecretStr | None = None
    dashboard_token: SecretStr | None = None
    allow_live: bool = False


# --- shared field types --------------------------------------------------------------


class _Frozen(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


def _compiler(names: frozenset[str]) -> Callable[[object], Expression]:
    def compile_(value: object) -> Expression:
        # Unquoted YAML numbers (`score: 0.5`) arrive as int/float; treat them as literals.
        if isinstance(value, int | float) and not isinstance(value, bool):
            value = str(value)
        if not isinstance(value, str):
            raise ValueError("expected an expression string")
        try:
            return Expression.compile(value, allowed_names=names)
        except ExpressionError as e:
            raise ValueError(str(e)) from None

    return compile_


_AS_SOURCE = PlainSerializer(lambda e: e.source, return_type=str)
SignalExpr = Annotated[Expression, PlainValidator(_compiler(SIGNAL_FEATURES)), _AS_SOURCE]
EvidenceExpr = Annotated[Expression, PlainValidator(_compiler(EVIDENCE_FIELDS)), _AS_SOURCE]


def _check_tz(value: str) -> str:
    try:
        ZoneInfo(value)
    except (ZoneInfoNotFoundError, ValueError):
        raise ValueError(f"unknown time zone {value!r}") from None
    return value


def _check_model_id(value: str) -> str:
    # G9: `-latest` names are not real aliases, and costs are priced per exact model ID.
    if "latest" in value:
        raise ValueError(f"pin an exact model ID, not {value!r}")
    return value


_CRON_FIELD = r"[\d*/,-]+"


def _check_cron(value: str) -> str:
    if not re.fullmatch(rf"{_CRON_FIELD}( {_CRON_FIELD}){{4}}", value):
        raise ValueError(f"expected a 5-field cron expression, got {value!r}")
    return value


Symbol = Annotated[str, StringConstraints(pattern=r"^[A-Z0-9]+/[A-Z0-9]+$")]
Timeframe = Annotated[str, StringConstraints(pattern=r"^[1-9]\d*[mhd]$")]
Identifier = Annotated[str, StringConstraints(pattern=r"^[a-z][a-z0-9_]*$")]
TimeZone = Annotated[str, AfterValidator(_check_tz)]
ModelId = Annotated[
    str, StringConstraints(pattern=r"^claude-[a-z0-9-]+$"), AfterValidator(_check_model_id)
]
Cron = Annotated[str, AfterValidator(_check_cron)]
PromptVersion = Annotated[str, StringConstraints(pattern=r"^v\d+$")]
Pct = Annotated[Decimal, Field(gt=0, le=100)]
Positive = Annotated[Decimal, Field(gt=0)]


def _unique[T](items: tuple[T, ...]) -> tuple[T, ...]:
    if len(set(items)) != len(items):
        raise ValueError("entries must be unique")
    return items


# --- strategy profile (SPEC §6) ------------------------------------------------------


class Timeframes(_Frozen):
    signal: Timeframe
    context: tuple[Timeframe, ...]
    trend: Timeframe


class SignalRule(_Frozen):
    id: Identifier
    expr: SignalExpr
    # Alpaca spot cannot short crypto; a `short` rule would never be executable.
    direction: Literal["long"]
    score: SignalExpr


class Scanner(_Frozen):
    max_candidates_per_symbol_per_hour: Annotated[int, Field(ge=1)]
    max_concurrent_cycles: Annotated[int, Field(ge=1)]
    min_vol_z: float
    max_spread_bps: Positive


class AgentSpec(_Frozen):
    model: ModelId
    prompt_version: PromptVersion


class ContextAgent(_Frozen):
    enabled: bool
    model: ModelId | None = None
    prompt_version: PromptVersion | None = None

    @model_validator(mode="after")
    def _model_when_enabled(self) -> Self:
        if self.enabled and (self.model is None or self.prompt_version is None):
            raise ValueError("context agent enabled without model and prompt_version")
        return self


class DecisionAgent(AgentSpec):
    mode: Literal["single", "debate"]


class ReviewerAgent(AgentSpec):
    schedule: Cron
    tz: TimeZone


class Toggle(_Frozen):
    enabled: bool


class Agents(_Frozen):
    technical: AgentSpec
    context: ContextAgent
    decision: DecisionAgent
    reviewer: ReviewerAgent
    exit_review: Toggle


class Trailing(_Frozen):
    enabled: bool
    trigger_r: Positive
    trail_atr: Positive


class RiskLimits(_Frozen):
    """Human-editable only. The learning loop has no write path to this block."""

    risk_per_trade_pct: Pct
    max_position_pct: Pct
    max_positions: Annotated[int, Field(ge=1)]
    max_gross_exposure_pct: Pct
    max_daily_loss_pct: Pct
    max_drawdown_pct: Pct
    min_stop_atr: Positive
    max_stop_atr: Positive
    max_hold_min: Annotated[int, Field(ge=1)]
    max_orders_per_hour: Annotated[int, Field(ge=1)]
    max_orders_per_day: Annotated[int, Field(ge=1)]
    max_price_deviation_pct: Pct
    default_win_rate_prior: Annotated[Decimal, Field(gt=0, lt=1)]
    trailing: Trailing

    @model_validator(mode="after")
    def _stop_band(self) -> Self:
        if self.min_stop_atr >= self.max_stop_atr:
            raise ValueError("min_stop_atr must be below max_stop_atr")
        return self


class Budget(_Frozen):
    llm_usd_per_day: Positive
    llm_usd_per_cycle_max: Positive

    @model_validator(mode="after")
    def _cycle_within_day(self) -> Self:
        if self.llm_usd_per_cycle_max > self.llm_usd_per_day:
            raise ValueError("llm_usd_per_cycle_max exceeds llm_usd_per_day")
        return self


class Learning(_Frozen):
    mode: Literal["propose_only", "auto_promote"]
    min_trades_for_evidence: Annotated[int, Field(ge=1)]
    promote_if: EvidenceExpr
    max_active_learnings: Annotated[int, Field(ge=1)]


class Benchmark(_Frozen):
    symbol: Symbol
    type: Literal["buy_and_hold"]


class StrategyProfile(_Frozen):
    name: Identifier
    description: str
    universe: Annotated[tuple[Symbol, ...], Field(min_length=1), AfterValidator(_unique)]
    timeframes: Timeframes
    signal_rules: Annotated[tuple[SignalRule, ...], Field(min_length=1)]
    scanner: Scanner
    agents: Agents
    risk: RiskLimits
    budget: Budget
    learning: Learning
    benchmark: Benchmark

    @field_validator("signal_rules")
    @classmethod
    def _unique_rule_ids(cls, rules: tuple[SignalRule, ...]) -> tuple[SignalRule, ...]:
        ids = [r.id for r in rules]
        if dupes := sorted({i for i in ids if ids.count(i) > 1}):
            raise ValueError(f"duplicate rule id {', '.join(dupes)}")
        return rules

    @cached_property
    def hash(self) -> str:
        """sha256 of the validated content: YAML formatting and comments don't count."""
        canonical = json.dumps(self.model_dump(mode="json"), sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()


# --- app config (config.yaml) --------------------------------------------------------


def _resolve_path(value: Path, info: ValidationInfo) -> Path:
    base = (info.context or {}).get("base_dir")
    return value if value.is_absolute() or base is None else (base / value).resolve()


ConfigPath = Annotated[Path, AfterValidator(_resolve_path)]


def _resolve_sqlite_url(value: str, info: ValidationInfo) -> str:
    """Relative SQLite paths resolve against the config dir, like every other path."""
    try:
        url = make_url(value)
    except (ArgumentError, ValueError):
        # Never echo the value: database URLs can carry credentials.
        raise ValueError("not a valid SQLAlchemy database URL") from None
    db = url.database
    if url.get_backend_name() != "sqlite" or not db or db == ":memory:" or Path(db).is_absolute():
        return value
    return url.set(database=str(_resolve_path(Path(db), info))).render_as_string(
        hide_password=False
    )


class Database(_Frozen):
    url: Annotated[str, AfterValidator(_resolve_sqlite_url)]


class Control(_Frozen):
    kill_file: ConfigPath


class Kill(_Frozen):
    flatten: bool | None = None


class Feed(_Frozen):
    stale_alert_s: Annotated[int, Field(ge=1)]
    stale_exit_s: Annotated[int, Field(ge=1)]
    stale_exit_enabled: bool | None = None

    @model_validator(mode="after")
    def _exit_after_alert(self) -> Self:
        if self.stale_exit_s <= self.stale_alert_s:
            raise ValueError("stale_exit_s must be greater than stale_alert_s")
        return self


class Shutdown(_Frozen):
    flatten: bool


class AppConfig(_Frozen):
    mode: Literal["paper", "live"]
    strategy: ConfigPath
    trading_day_tz: TimeZone
    database: Database
    control: Control
    kill: Kill
    feed: Feed
    shutdown: Shutdown

    @property
    def kill_flatten(self) -> bool:
        """SPEC §4.11: flatten on kill defaults to true in live, false in paper."""
        return self.kill.flatten if self.kill.flatten is not None else self.mode == "live"

    @property
    def feed_stale_exit_enabled(self) -> bool:
        """D6: stale-data exit defaults to on in live, off in paper."""
        enabled = self.feed.stale_exit_enabled
        return enabled if enabled is not None else self.mode == "live"


# --- loading -------------------------------------------------------------------------


class _UniqueKeyLoader(yaml.SafeLoader):
    """SafeLoader that rejects duplicate keys instead of silently keeping the last."""


def _construct_unique_mapping(loader: yaml.SafeLoader, node: yaml.MappingNode) -> Any:
    seen: set[Any] = set()
    for key_node, _ in node.value:
        key = loader.construct_object(key_node)
        if key in seen:
            raise yaml.constructor.ConstructorError(
                None, None, f"duplicate key {key!r}", key_node.start_mark
            )
        seen.add(key)
    return loader.construct_mapping(node)


_UniqueKeyLoader.add_constructor(
    yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _construct_unique_mapping
)


def _read_yaml(path: Path) -> Any:
    try:
        return yaml.load(path.read_text(), Loader=_UniqueKeyLoader)  # noqa: S506 - safe subclass
    except OSError as e:
        raise ConfigError(f"{path}: cannot read: {e.strerror}") from None
    except yaml.YAMLError as e:
        raise ConfigError(f"{path}: invalid YAML: {e}") from None


def _validate[M: BaseModel](
    model: type[M], data: Any, source: str, context: Mapping[str, Any] | None = None
) -> M:
    try:
        return model.model_validate(data, context=dict(context or {}))
    except ValidationError as e:
        raise ConfigError(f"{source}: {_format_errors(e)}") from None


def _format_errors(error: ValidationError) -> str:
    """`field.path: message` per error, without input values.

    Pydantic's default text echoes the offending input, which for `database.url` can be
    a credential-bearing URL.
    """
    return "; ".join(
        f"{'.'.join(str(part) for part in err['loc']) or '<root>'}: {err['msg']}"
        for err in error.errors(include_url=False, include_input=False)
    )


def parse_profile(data: Any, source: str = "<profile>") -> StrategyProfile:
    return _validate(StrategyProfile, data, source)


def load_profile(path: Path) -> StrategyProfile:
    return parse_profile(_read_yaml(path), str(path))


def load_config(path: Path, settings: Settings) -> AppConfig:
    path = path.resolve()
    config = _validate(AppConfig, _read_yaml(path), str(path), {"base_dir": path.parent})
    if config.mode == "live" and not settings.allow_live:
        raise ConfigError(f"{path}: mode is live but ALLOW_LIVE=1 is not set")
    if settings.alpaca_paper != (config.mode == "paper"):
        raise ConfigError(
            f"{path}: mode is {config.mode} but ALPACA_PAPER={str(settings.alpaca_paper).lower()}"
        )
    return config
