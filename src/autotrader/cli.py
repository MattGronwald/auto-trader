"""`autotrader` command line (SPEC §11).

Commands that are stubs until their work package lands exit with code 2, so an operator
never mistakes an unimplemented `kill` or `flatten` for a successful one.
"""

import asyncio
from enum import StrEnum
from pathlib import Path
from typing import Annotated, NoReturn

import typer

from autotrader.core.bus import EventBus
from autotrader.core.config import AppConfig, ConfigError, Settings, load_config, load_profile
from autotrader.core.kill import clear_kill, trigger_kill
from autotrader.core.logging import configure_logging
from autotrader.journal import db

app = typer.Typer(help="Automated, agent-based crypto trading PoC.", no_args_is_help=True)
run_app = typer.Typer(help="Run a long-lived process.", no_args_is_help=True)
app.add_typer(run_app, name="run")
db_app = typer.Typer(help="Database migrations.", no_args_is_help=True)
app.add_typer(db_app, name="db")

ConfigOption = Annotated[Path, typer.Option("--config", "-c", help="Path to config.yaml.")]


class LogLevel(StrEnum):
    debug = "debug"
    info = "info"
    warning = "warning"
    error = "error"


@app.callback()
def main(
    log_level: Annotated[LogLevel, typer.Option(help="Minimum level of JSON log lines.")] = (
        LogLevel.info
    ),
) -> None:
    configure_logging(log_level.value)


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


_KILL_FALLBACK = (
    "To stop trading without a valid config, create the kill file directly: "
    "touch data/control/KILL (or the path set in control.kill_file)."
)


@app.command("kill")
def kill(
    config: ConfigOption = Path("config.yaml"),
    reason: Annotated[str | None, typer.Option(help="Recorded with the kill.")] = None,
    clear: Annotated[bool, typer.Option("--clear", help="Lift an active kill.")] = False,
) -> None:
    """Trigger the kill-switch: stop all new entries (exits stay possible)."""
    try:
        cfg = load_config(config, Settings())
    except ConfigError as e:
        typer.echo(f"invalid config: {e}\n{_KILL_FALLBACK}", err=True)
        raise typer.Exit(code=1) from None
    asyncio.run(_kill(cfg, reason, clear))


async def _kill(cfg: AppConfig, reason: str | None, clear: bool) -> None:
    kill_file = cfg.control.kill_file
    engine = db.make_engine(cfg.database.url)
    sessions = db.make_sessionmaker(engine)
    bus = EventBus(sessions)
    try:
        if clear:
            await clear_kill(kill_file, sessions, bus, source="cli")
            typer.echo(f"kill cleared: removed {kill_file} and control_state.kill")
            return
        result = await trigger_kill(kill_file, sessions, bus, source="cli", reason=reason)
        typer.echo(f"KILL active: {kill_file}")
        if result.db_error is not None:
            typer.echo(
                f"warning: database not updated ({result.db_error}); "
                "the kill file alone stops trading.",
                err=True,
            )
    finally:
        await bus.close()
        await engine.dispose()


def _load_config(path: Path) -> AppConfig:
    try:
        return load_config(path, Settings())
    except ConfigError as e:
        typer.echo(f"invalid config: {e}", err=True)
        raise typer.Exit(code=1) from None


@app.command("check-config")
def check_config(config: ConfigOption = Path("config.yaml")) -> None:
    """Validate config.yaml, env and the strategy profile; print the profile hash."""
    cfg = _load_config(config)
    try:
        profile = load_profile(cfg.strategy)
    except ConfigError as e:
        typer.echo(f"invalid config: {e}", err=True)
        raise typer.Exit(code=1) from None
    typer.echo(f"mode: {cfg.mode}")
    typer.echo(f"profile: {profile.name} ({len(profile.signal_rules)} signal rules)")
    typer.echo(f"hash: {profile.hash}")


@db_app.command("upgrade")
def db_upgrade(config: ConfigOption = Path("config.yaml")) -> None:
    """Apply all pending migrations to the configured database."""
    url = _load_config(config).database.url
    db.upgrade(url)
    typer.echo(f"database at head ({db.current_revision(url)})")
