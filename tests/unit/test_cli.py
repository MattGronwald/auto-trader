from pathlib import Path

import pytest
from typer.testing import CliRunner

from autotrader.cli import app

runner = CliRunner()
REPO = Path(__file__).resolve().parents[2]


def test_help_lists_all_spec_commands() -> None:
    result = runner.invoke(app, ["--help"])

    assert result.exit_code == 0
    for command in ("run", "review-now", "backtest-signals", "flatten", "kill"):
        assert command in result.output


def test_run_help_lists_core_and_api() -> None:
    result = runner.invoke(app, ["run", "--help"])

    assert result.exit_code == 0
    assert "core" in result.output
    assert "api" in result.output


@pytest.mark.parametrize(
    "args",
    [
        ["run", "core"],
        ["run", "api"],
        ["review-now"],
        ["backtest-signals"],
        ["flatten"],
    ],
)
def test_stub_commands_fail_loudly(args: list[str]) -> None:
    # A stub must never look like success: an operator running `kill` or `flatten`
    # before it is implemented has to see that nothing happened.
    result = runner.invoke(app, args)

    assert result.exit_code == 2
    assert "not implemented" in result.output


def test_check_config_reports_profile(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.chdir(REPO)
    monkeypatch.delenv("ALPACA_PAPER", raising=False)

    result = runner.invoke(app, ["check-config"])

    assert result.exit_code == 0, result.output
    assert "mode: paper" in result.output
    assert "profile: fast_momentum_v1" in result.output
    assert "hash: " in result.output


def test_check_config_fails_on_invalid_file(tmp_path: Path) -> None:
    bad = tmp_path / "config.yaml"
    bad.write_text("mode: sideways\n")

    result = runner.invoke(app, ["check-config", "--config", str(bad)])

    assert result.exit_code == 1
    assert "config.yaml" in result.output


def test_check_config_reports_bad_database_url(tmp_path: Path) -> None:
    config = tmp_path / "config.yaml"
    config.write_text(
        (REPO / "config.yaml")
        .read_text()
        .replace("url: sqlite+aiosqlite:///data/autotrader.db", "url: not-a-db-url")
    )

    result = runner.invoke(app, ["check-config", "--config", str(config)])

    assert result.exit_code == 1
    assert "invalid config" in result.output
    assert "database.url" in result.output


def test_db_upgrade_creates_schema(tmp_path: Path) -> None:
    config = tmp_path / "config.yaml"
    text = (
        (REPO / "config.yaml")
        .read_text()
        .replace("strategy: strategies/", f"strategy: {REPO}/strategies/")
    )
    config.write_text(text)

    result = runner.invoke(app, ["db", "upgrade", "--config", str(config)])

    assert result.exit_code == 0, result.output
    assert (tmp_path / "data" / "autotrader.db").is_file()  # resolved against config dir
    assert "at head" in result.output


def test_log_level_option_configures_json_logging(monkeypatch: pytest.MonkeyPatch) -> None:
    calls: list[str] = []
    monkeypatch.setattr("autotrader.cli.configure_logging", lambda level: calls.append(level))

    result = runner.invoke(app, ["--log-level", "debug", "flatten"])

    assert result.exit_code == 2  # still a stub
    assert calls == ["debug"]


def test_invalid_log_level_rejected() -> None:
    result = runner.invoke(app, ["--log-level", "loud", "flatten"])

    assert result.exit_code == 2
    assert "loud" in result.output


def _tmp_config(tmp_path: Path) -> Path:
    config = tmp_path / "config.yaml"
    config.write_text(
        (REPO / "config.yaml")
        .read_text()
        .replace("strategy: strategies/", f"strategy: {REPO}/strategies/")
    )
    return config


def test_kill_and_clear(tmp_path: Path) -> None:
    config = _tmp_config(tmp_path)
    assert runner.invoke(app, ["db", "upgrade", "-c", str(config)]).exit_code == 0
    kill_file = tmp_path / "data" / "control" / "KILL"

    killed = runner.invoke(app, ["kill", "-c", str(config), "--reason", "manual test"])

    assert killed.exit_code == 0, killed.output
    assert kill_file.exists()
    assert "KILL" in killed.output

    cleared = runner.invoke(app, ["kill", "--clear", "-c", str(config)])

    assert cleared.exit_code == 0, cleared.output
    assert not kill_file.exists()


def test_kill_works_without_database(tmp_path: Path) -> None:
    # No `db upgrade`: the DB write fails, but the kill file still stops trading.
    config = _tmp_config(tmp_path)

    result = runner.invoke(app, ["kill", "-c", str(config)])

    assert result.exit_code == 0
    assert (tmp_path / "data" / "control" / "KILL").exists()
    assert "database" in result.output.lower()


def test_kill_with_invalid_config_points_to_touch_fallback(tmp_path: Path) -> None:
    bad = tmp_path / "config.yaml"
    bad.write_text("mode: sideways\n")

    result = runner.invoke(app, ["kill", "-c", str(bad)])

    assert result.exit_code == 1
    assert "touch" in result.output


def test_kill_survives_engine_init_failure(tmp_path: Path) -> None:
    # P1: engine creation ran before the file write, so a broken DB path stopped the kill.
    config = _tmp_config(tmp_path)
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "blocker").write_text("a file where a directory should be")
    text = config.read_text().replace(
        "url: sqlite+aiosqlite:///data/autotrader.db",
        "url: sqlite+aiosqlite:///data/blocker/autotrader.db",
    )
    config.write_text(text)

    result = runner.invoke(app, ["kill", "-c", str(config)])

    assert result.exit_code == 0, result.output
    assert (tmp_path / "data" / "control" / "KILL").exists()
    assert "database" in result.output.lower()


def test_kill_survives_missing_db_driver(tmp_path: Path) -> None:
    config = _tmp_config(tmp_path)
    text = config.read_text().replace(
        "url: sqlite+aiosqlite:///data/autotrader.db",
        "url: postgresql+nosuchdriver://u@localhost/x",
    )
    config.write_text(text)

    result = runner.invoke(app, ["kill", "-c", str(config)])

    assert result.exit_code == 0, result.output
    assert (tmp_path / "data" / "control" / "KILL").exists()


def test_failed_clear_exits_nonzero_and_keeps_kill(tmp_path: Path) -> None:
    config = _tmp_config(tmp_path)
    kill_file = tmp_path / "data" / "control" / "KILL"
    kill_file.parent.mkdir(parents=True)
    kill_file.touch()  # file-only kill; DB never migrated -> clear's DB write fails

    result = runner.invoke(app, ["kill", "--clear", "-c", str(config)])

    assert result.exit_code == 1
    assert kill_file.exists()
    assert "still active" in result.output


def test_kill_fails_loudly_if_the_file_cannot_be_written(tmp_path: Path) -> None:
    config = _tmp_config(tmp_path)
    (tmp_path / "data").mkdir()
    (tmp_path / "data" / "control").write_text("not a directory")

    result = runner.invoke(app, ["kill", "-c", str(config)])

    assert result.exit_code == 1
    assert "NOT" in result.output
