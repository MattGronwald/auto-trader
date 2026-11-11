"""`autotrader repair link | not-submitted | oversell` (WP 1.5d-2, spec 2026-11-10 5.1).

A thin wrapper over `core/repair.py`: the commands only parse; `_run_repair` refuses a
journal that does not exist, takes the journal lock (so no core runs), refuses a journal whose
schema is not at head (it never migrates: run `autotrader db upgrade` first), prepares the
repair, shows the row, the change and (for `link`) the broker order, asks for confirmation,
commits and prints the exit hint. For `link` the CLI connects the broker
and hands it straight to `risk.engine.read_order_by_client_id` (SPEC 0.1); `core/repair.py`
only receives the lookup callable. The lock is held for the whole run: process exit releases
it (tests call `release()`).
"""

import asyncio
import getpass
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import Annotated

import typer
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.broker.alpaca import AlpacaBroker
from autotrader.broker.base import Broker, BrokerOrderView
from autotrader.core import lock as journal_lock
from autotrader.core import repair
from autotrader.core.bus import EventBus
from autotrader.core.config import AppConfig, ConfigError, Settings, load_config
from autotrader.core.repair import Action, RepairPlan, RepairRefused
from autotrader.journal import db
from autotrader.risk.engine import read_order_by_client_id

repair_app = typer.Typer(help="Operator repairs of the order journal.", no_args_is_help=True)

ConfigOption = Annotated[Path, typer.Option("--config", "-c", help="Path to config.yaml.")]
EvidenceOption = Annotated[
    str, typer.Option("--evidence", help="What you checked at the broker (recorded).")
]
YesOption = Annotated[bool, typer.Option("--yes", help="Apply without asking.")]

LOOKUP_TIMEOUT_S = 10.0
_LOCK_PREFIX = "journal locked by "
NOT_SUBMITTED_CONDITION = (
    "Use this only with authoritative broker evidence that the order was never and will never "
    "be accepted. A search that finds nothing, or a timeout, does not qualify. The mark is "
    "final: the next sync pass settles the row with zero fill."
)
DONE = "done; resume entries per docs/runbook.md once the next run core shows the row settled"

ConnectBroker = Callable[[Settings], Awaitable[Broker]]


def _show(plan: RepairPlan) -> None:
    typer.echo(f"order row {plan.order_row_id}, repair {plan.action}")
    typer.echo("row now:")
    for column, value in plan.before.items():
        typer.echo(f"  {column} = {value}")
    typer.echo("change to:")
    for column, value in plan.after.items():
        typer.echo(f"  {column} = {value}")
    if plan.broker_view is not None:
        typer.echo("broker order found by client id:")
        for field, value in plan.broker_view.items():
            typer.echo(f"  {field} = {value}")


async def _prepare(
    sessions: async_sessionmaker[AsyncSession],
    action: Action,
    row_id: int,
    broker_order_id: str | None,
    settings: Settings,
    connect_broker: ConnectBroker,
) -> RepairPlan:
    if action == "link":
        if broker_order_id is None:
            raise RepairRefused("link needs a broker order id")
        try:
            broker = await connect_broker(settings)
        except Exception as e:  # nothing was read or written
            raise RepairRefused(f"broker lookup failed: {type(e).__name__}: {e}") from e
        lookup: Callable[[str], Awaitable[BrokerOrderView | None]] = partial(
            read_order_by_client_id, broker, timeout_s=LOOKUP_TIMEOUT_S
        )
        return await repair.prepare_link(sessions, row_id, broker_order_id, lookup)
    if action == "not_submitted":
        return await repair.prepare_not_submitted(sessions, row_id)
    return await repair.prepare_oversell(sessions, row_id)


def _run_repair(
    cfg: AppConfig,
    settings: Settings,
    action: Action,
    row_id: int,
    *,
    broker_order_id: str | None,
    evidence: str,
    yes: bool,
    acquire: Callable[..., journal_lock.JournalLock] = journal_lock.acquire,
    revision: Callable[[str], str | None] = db.current_revision,
    head: Callable[[], str | None] = db.head_revision,
    connect_broker: ConnectBroker = AlpacaBroker.connect,
    confirm: Callable[[str], bool] = typer.confirm,
    operator: Callable[[], str] = getpass.getuser,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> int:
    if not evidence.strip():
        typer.echo("--evidence must not be blank", err=True)
        return 2
    try:
        _, db_path = journal_lock.canonical_journal(cfg.database.url)
        if not db_path.is_file():  # a wrong --config or path: never create a journal
            typer.echo(f"no journal at {db_path}", err=True)
            return 1
        lock = acquire(cfg.database.url, wait_s=0)
    except journal_lock.JournalLockError as e:
        text = str(e)
        if text.startswith(_LOCK_PREFIX):
            typer.echo(f"core is running ({text.removeprefix(_LOCK_PREFIX)})", err=True)
        else:
            typer.echo(text, err=True)
        return 1
    at, want = revision(lock.url), head()
    if at != want:  # repair reads and writes the current schema only; it never migrates
        typer.echo(
            f"journal schema at {at}, code expects {want}: run `autotrader db upgrade` first",
            err=True,
        )
        return 1

    async def go() -> int:
        engine = db.make_engine(lock.url)
        sessions = db.make_sessionmaker(engine)
        bus = EventBus(sessions)
        try:
            try:
                plan = await _prepare(
                    sessions, action, row_id, broker_order_id, settings, connect_broker
                )
                _show(plan)
                if action == "not_submitted":
                    typer.echo(NOT_SUBMITTED_CONDITION)
                if not yes and not confirm("Apply?"):
                    typer.echo("not applied", err=True)
                    return 1
                await repair.commit(bus, plan, evidence=evidence, operator=operator(), now=clock())
            except RepairRefused as e:
                typer.echo(f"refused: {e}", err=True)
                return 1
            typer.echo(DONE)
            return 0
        finally:
            await bus.close()
            await engine.dispose()

    return asyncio.run(go())


def _invoke(
    config: Path,
    action: Action,
    row: int,
    broker_order_id: str | None,
    evidence: str,
    yes: bool,
) -> None:
    settings = Settings()
    try:
        cfg = load_config(config, settings)
    except ConfigError as e:
        typer.echo(f"invalid config: {e}", err=True)
        raise typer.Exit(code=1) from None
    raise typer.Exit(
        code=_run_repair(
            cfg, settings, action, row, broker_order_id=broker_order_id, evidence=evidence, yes=yes
        )
    )


@repair_app.command("link")
def link(
    order_row: int,
    broker_order_id: str,
    evidence: EvidenceOption,
    yes: YesOption = False,
    config: ConfigOption = Path("config.yaml"),
) -> None:
    """Record the broker id of an unlinked or incident order row (broker read proves it)."""
    _invoke(config, "link", order_row, broker_order_id, evidence, yes)


@repair_app.command("not-submitted")
def not_submitted(
    order_row: int,
    evidence: EvidenceOption,
    yes: YesOption = False,
    config: ConfigOption = Path("config.yaml"),
) -> None:
    """Mark an unlinked pending/unknown order row not_submitted (final)."""
    _invoke(config, "not_submitted", order_row, None, evidence, yes)


@repair_app.command("oversell")
def oversell(
    order_row: int,
    evidence: EvidenceOption,
    yes: YesOption = False,
    config: ConfigOption = Path("config.yaml"),
) -> None:
    """Mark a sell row flagged oversell as excess_external."""
    _invoke(config, "oversell", order_row, None, evidence, yes)
