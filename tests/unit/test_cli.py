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
        ["kill"],
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
