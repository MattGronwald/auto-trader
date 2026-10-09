import pytest
from typer.testing import CliRunner

from autotrader.cli import app

runner = CliRunner()


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
