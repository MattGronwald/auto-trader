import copy
from decimal import Decimal
from pathlib import Path
from typing import Any

import pytest
import yaml

from autotrader.core.config import (
    ConfigError,
    Settings,
    load_config,
    load_profile,
    parse_profile,
)

REPO = Path(__file__).resolve().parents[3]
CONFIG = REPO / "config.yaml"
PROFILE = REPO / "strategies" / "fast_momentum_v1.yaml"


def paper_settings(**overrides: Any) -> Settings:
    return Settings(_env_file=None, **overrides)


@pytest.fixture
def profile_data() -> dict[str, Any]:
    data: dict[str, Any] = yaml.safe_load(PROFILE.read_text())
    return copy.deepcopy(data)


# --- the shipped files load ----------------------------------------------------------


def test_repo_profile_loads() -> None:
    profile = load_profile(PROFILE)

    assert profile.name == "fast_momentum_v1"
    assert profile.universe == ("BTC/USD", "ETH/USD", "SOL/USD")
    assert [r.id for r in profile.signal_rules] == [
        "breakout_donchian20_volspike",
        "pullback_ema21_uptrend",
    ]
    assert profile.risk.risk_per_trade_pct == Decimal("1.0")
    assert profile.risk.default_win_rate_prior == Decimal("0.40")
    assert profile.agents.technical.model == "claude-haiku-5-5"


def test_repo_config_loads_with_resolved_paths() -> None:
    config = load_config(CONFIG, paper_settings())

    assert config.mode == "paper"
    assert config.strategy == REPO / "strategies" / "fast_momentum_v1.yaml"
    assert config.control.kill_file == REPO / "data" / "control" / "KILL"
    assert config.trading_day_tz == "UTC"


def test_risk_values_are_exact_decimals(profile_data: dict[str, Any]) -> None:
    profile = parse_profile(profile_data)

    assert isinstance(profile.risk.max_position_pct, Decimal)
    assert profile.budget.llm_usd_per_cycle_max == Decimal("0.10")


def test_compiled_rules_evaluate() -> None:
    rule = load_profile(PROFILE).signal_rules[1]

    env = {"ema21": 100.0, "ema50": 99.0, "low": 99.5, "close": 101.0, "rsi14": 50.0}
    assert rule.expr.evaluate_bool(env) is True
    assert rule.score.evaluate_number(env) == 0.5


# --- profile hash --------------------------------------------------------------------


def test_hash_ignores_yaml_formatting(tmp_path: Path) -> None:
    reformatted = tmp_path / "p.yaml"
    reformatted.write_text(yaml.safe_dump(yaml.safe_load(PROFILE.read_text())))

    assert load_profile(reformatted).hash == load_profile(PROFILE).hash


def test_hash_changes_with_risk(profile_data: dict[str, Any]) -> None:
    base = parse_profile(profile_data).hash
    profile_data["risk"]["max_positions"] = 3

    assert parse_profile(profile_data).hash != base


def test_hash_is_sha256_hex() -> None:
    h = load_profile(PROFILE).hash

    assert len(h) == 64
    int(h, 16)


# --- profile validation fails fast ---------------------------------------------------


def _set(data: dict[str, Any], path: str, value: Any) -> None:
    *parents, leaf = path.split(".")
    node: Any = data
    for key in parents:
        node = node[int(key)] if key.isdigit() else node[key]
    if leaf.isdigit():
        node[int(leaf)] = value
    else:
        node[leaf] = value


@pytest.mark.parametrize(
    ("path", "value", "match"),
    [
        ("risk.risk_per_trade_pc", 1.0, "risk_per_trade_pc"),  # typo → extra key
        ("signal_rules.0.expr", "close > foo", "unknown name 'foo'"),
        ("signal_rules.0.expr", "__import__('os')", "breakout_donchian20_volspike|expr"),
        ("signal_rules.0.score", "vol_z ** 2", "score"),
        ("signal_rules.0.direction", "short", "direction"),
        ("signal_rules.1.id", "breakout_donchian20_volspike", "duplicate rule id"),
        ("agents.technical.model", "claude-haiku-latest", "latest"),
        ("agents.context.enabled", True, "context"),
        ("agents.reviewer.tz", "Mars/Olympus", "tz"),
        ("agents.reviewer.schedule", "every night", "schedule"),
        # regressions from the PR #3 review
        ("agents.reviewer.schedule", "99 99 99 99 99", "schedule"),
        ("agents.reviewer.schedule", "*/0 * * * *", "schedule"),
        ("agents.reviewer.schedule", "0 3 31 2-1 *", "schedule"),  # reversed range
        ("agents.reviewer.schedule", "0 24 * * *", "schedule"),
        ("agents.reviewer.schedule", "60 3 * * *", "schedule"),
        ("agents.reviewer.schedule", "0 3 0 * *", "schedule"),  # day-of-month starts at 1
        ("agents.reviewer.schedule", "0 3 * 13 *", "schedule"),
        ("agents.reviewer.schedule", "0 3 * * 8", "schedule"),
        ("signal_rules.0.expr", "1 + 2", "expected a bool"),
        ("signal_rules.0.score", "close > 1", "expected a number"),
        ("signal_rules.0.score", "1e309", "score"),
        ("learning.promote_if", "p_value * 2", "expected a bool"),
        ("risk.min_stop_atr", 3.0, "min_stop_atr"),
        ("risk.max_daily_loss_pct", 0, "max_daily_loss_pct"),
        ("risk.max_position_pct", 101, "max_position_pct"),
        ("risk.default_win_rate_prior", 1.0, "default_win_rate_prior"),
        ("budget.llm_usd_per_cycle_max", 5.0, "llm_usd_per_cycle_max"),
        ("universe", ["BTC/USD", "BTC/USD"], "universe"),
        ("universe", ["btcusd"], "universe"),
        ("timeframes.signal", "5x", "signal"),
        ("learning.promote_if", "win_rat > 0.5", "unknown name 'win_rat'"),
        ("benchmark.type", "index", "type"),
    ],
)
def test_invalid_profile_rejected(
    profile_data: dict[str, Any], path: str, value: Any, match: str
) -> None:
    _set(profile_data, path, value)

    with pytest.raises(ConfigError, match=match):
        parse_profile(profile_data)


def test_unquoted_numeric_expression_accepted(profile_data: dict[str, Any]) -> None:
    profile_data["signal_rules"][1]["score"] = 0.5  # YAML `score: 0.5` without quotes

    assert parse_profile(profile_data).signal_rules[1].score.evaluate_number({}) == 0.5


def test_non_string_expression_rejected(profile_data: dict[str, Any]) -> None:
    profile_data["signal_rules"][0]["expr"] = True

    with pytest.raises(ConfigError, match="expected an expression string"):
        parse_profile(profile_data)


@pytest.mark.parametrize(
    "schedule",
    ["0 3 * * *", "*/15 * * * *", "0 0-6/2 1,15 1-12 0-7", "30 23 31 12 7"],
)
def test_valid_cron_schedules_accepted(profile_data: dict[str, Any], schedule: str) -> None:
    profile_data["agents"]["reviewer"]["schedule"] = schedule

    assert parse_profile(profile_data).agents.reviewer.schedule == schedule


def test_overflowing_literal_is_config_error_not_crash(profile_data: dict[str, Any]) -> None:
    profile_data["signal_rules"][0]["score"] = "9" * 400

    with pytest.raises(ConfigError, match="score"):
        parse_profile(profile_data)


def test_profile_is_immutable() -> None:
    profile = load_profile(PROFILE)

    with pytest.raises(ValueError, match="frozen"):
        profile.risk.max_positions = 5  # type: ignore[misc]


def test_duplicate_yaml_keys_rejected(tmp_path: Path) -> None:
    # PyYAML silently keeps the last value; for the risk block that would hide an edit.
    text = PROFILE.read_text().replace(
        "  max_positions: 2\n", "  max_positions: 2\n  max_positions: 5\n"
    )
    path = tmp_path / "dup.yaml"
    path.write_text(text)

    with pytest.raises(ConfigError, match="duplicate key 'max_positions'"):
        load_profile(path)


def test_missing_file_is_config_error(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match=r"nope\.yaml"):
        load_profile(tmp_path / "nope.yaml")


def test_malformed_yaml_is_config_error(tmp_path: Path) -> None:
    path = tmp_path / "bad.yaml"
    path.write_text("name: [unclosed\n")

    with pytest.raises(ConfigError, match=r"bad\.yaml"):
        load_profile(path)


# --- app config and mode guards ------------------------------------------------------


def _write_config(tmp_path: Path, **changes: Any) -> Path:
    data: dict[str, Any] = yaml.safe_load(CONFIG.read_text())
    data.update(changes)
    path = tmp_path / "config.yaml"
    path.write_text(yaml.safe_dump(data))
    return path


def test_mode_defaults_in_paper() -> None:
    config = load_config(CONFIG, paper_settings())

    assert config.kill_flatten is False
    assert config.feed_stale_exit_enabled is False


def test_mode_defaults_in_live(tmp_path: Path) -> None:
    path = _write_config(tmp_path, mode="live")

    config = load_config(path, paper_settings(allow_live=True, alpaca_paper=False))

    assert config.kill_flatten is True
    assert config.feed_stale_exit_enabled is True


def test_explicit_flags_override_mode_defaults(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path,
        kill={"flatten": True},
        feed={"stale_alert_s": 60, "stale_exit_s": 300, "stale_exit_enabled": True},
    )

    config = load_config(path, paper_settings())

    assert config.kill_flatten is True
    assert config.feed_stale_exit_enabled is True


def test_live_requires_allow_live(tmp_path: Path) -> None:
    path = _write_config(tmp_path, mode="live")

    with pytest.raises(ConfigError, match="ALLOW_LIVE"):
        load_config(path, paper_settings(alpaca_paper=False))


def test_mode_must_match_alpaca_paper_flag(tmp_path: Path) -> None:
    with pytest.raises(ConfigError, match="ALPACA_PAPER"):
        load_config(CONFIG, paper_settings(alpaca_paper=False))


def test_stale_exit_must_follow_alert(tmp_path: Path) -> None:
    path = _write_config(
        tmp_path, feed={"stale_alert_s": 300, "stale_exit_s": 60, "stale_exit_enabled": None}
    )

    with pytest.raises(ConfigError, match="stale_exit_s"):
        load_config(path, paper_settings())


def test_invalid_trading_day_tz(tmp_path: Path) -> None:
    path = _write_config(tmp_path, trading_day_tz="Nowhere/Land")

    with pytest.raises(ConfigError, match="trading_day_tz"):
        load_config(path, paper_settings())


# --- secrets ------------------------------------------------------------------------


def test_settings_read_env_and_hide_secrets(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("ALPACA_API_KEY_ID", "key-id")
    monkeypatch.setenv("ALPACA_API_SECRET_KEY", "super-secret")
    monkeypatch.setenv("ALPACA_PAPER", "true")

    settings = Settings(_env_file=None)

    assert settings.alpaca_api_secret_key is not None
    assert settings.alpaca_api_secret_key.get_secret_value() == "super-secret"
    assert "super-secret" not in repr(settings)
    assert settings.allow_live is False


def test_settings_read_dotenv(tmp_path: Path) -> None:
    env_file = tmp_path / ".env"
    env_file.write_text("ANTHROPIC_API_KEY=from-dotenv\n")

    settings = Settings(_env_file=env_file)

    assert settings.anthropic_api_key is not None
    assert settings.anthropic_api_key.get_secret_value() == "from-dotenv"
