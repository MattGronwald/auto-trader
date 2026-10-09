"""`autotrader` command line (SPEC §11).

All commands are stubs until their work package lands. Stubs exit with code 2 so an
operator never mistakes an unimplemented `kill` or `flatten` for a successful one.
"""

from typing import NoReturn

import typer

app = typer.Typer(help="Automated, agent-based crypto trading PoC.", no_args_is_help=True)
run_app = typer.Typer(help="Run a long-lived process.", no_args_is_help=True)
app.add_typer(run_app, name="run")


def _not_implemented(command: str, wp: str) -> NoReturn:
    typer.echo(f"`{command}` is not implemented yet (PLAN.md WP {wp}).", err=True)
    raise typer.Exit(code=2)


@run_app.command("core")
def run_core() -> None:
    """Start the trading core: feed, scanner, cycle runner, position manager."""
    _not_implemented("run core", "0.8")


@run_app.command("api")
def run_api() -> None:
    """Start the dashboard API."""
    _not_implemented("run api", "3.2")


@app.command("review-now")
def review_now() -> None:
    """Run the Journal Reviewer immediately instead of at its scheduled time."""
    _not_implemented("review-now", "4.3")


@app.command("backtest-signals")
def backtest_signals() -> None:
    """Report signal rule hits and forward returns on persisted bars."""
    _not_implemented("backtest-signals", "1.7")


@app.command("flatten")
def flatten() -> None:
    """Close all open positions via reduce-only exits."""
    _not_implemented("flatten", "1.5")


@app.command("kill")
def kill() -> None:
    """Trigger the kill-switch."""
    _not_implemented("kill", "0.5")
