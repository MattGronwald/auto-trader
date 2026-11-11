"""`autotrader repair` (WP 1.5d-2 Task 10, spec 2026-11-10 5.1 and 8 test 11).

`_run_repair` is driven with fakes (lock, broker, confirm, operator); `CliRunner` covers only
parsing. Rows are inserted through the resolver test rig's helpers into a migrated journal
at the tmp config's database url.
"""

import asyncio
from collections.abc import Callable, Iterator
from decimal import Decimal as D
from pathlib import Path
from typing import Any, cast

import pytest
from sqlalchemy import update
from typer.testing import CliRunner

import tests.unit.risk.test_resolver as tr
from autotrader.broker.base import (
    Broker,
    BrokerError,
    BrokerOrder,
    BrokerOrderView,
    OrderMethodsUnavailable,
)
from autotrader.broker.sim import SimBroker
from autotrader.cli import app
from autotrader.cli_repair import NOT_SUBMITTED_CONDITION, _run_repair
from autotrader.core import lock as journal_lock
from autotrader.core.config import AppConfig, Settings, load_config
from autotrader.journal import db
from autotrader.journal import models as m
from autotrader.risk.resolve import entry_client_id
from tests.unit.test_cli import _tmp_config

runner = CliRunner()
EVIDENCE = "checked the broker UI at 10:00"
SETTINGS = Settings(_env_file=None)


class Rig:
    def __init__(self, tmp_path: Path) -> None:
        self.cfg: AppConfig = load_config(_tmp_config(tmp_path), SETTINGS)
        db.upgrade(self.cfg.database.url)
        self.confirms: list[str] = []
        self.answer = True
        self.locks: list[journal_lock.JournalLock] = []

    def run(
        self,
        action: Any,
        row: int,
        *,
        broker: Callable[..., Any] | None = None,
        broker_order_id: str | None = None,
        evidence: str = EVIDENCE,
        yes: bool = False,
        operator: str = "alice",
    ) -> int:
        def confirm(prompt: str) -> bool:
            self.confirms.append(prompt)
            return self.answer

        def acquire(url: str, **kw: Any) -> journal_lock.JournalLock:
            self.locks.append(journal_lock.acquire(url, **kw))
            return self.locks[-1]

        mine: list[journal_lock.JournalLock] = []

        def acquire_mine(url: str, **kw: Any) -> journal_lock.JournalLock:
            mine.append(acquire(url, **kw))
            return mine[-1]

        try:
            return self._call(
                acquire_mine, action, row, broker, broker_order_id, evidence, yes, confirm, operator
            )
        finally:
            for lock in mine:  # production keeps the lock until exit; a test run may repeat
                lock.release()

    def _call(
        self,
        acquire: Any,
        action: Any,
        row: int,
        broker: Any,
        broker_order_id: Any,
        evidence: str,
        yes: bool,
        confirm: Any,
        operator: str,
    ) -> int:
        async def no_broker(settings: Settings) -> Broker:
            raise AssertionError("broker must not be connected")

        return _run_repair(
            self.cfg,
            SETTINGS,
            action,
            row,
            broker_order_id=broker_order_id,
            evidence=evidence,
            yes=yes,
            acquire=acquire,
            connect_broker=broker or no_broker,
            confirm=confirm,
            operator=lambda: operator,
        )

    def db(self, coro: Any) -> Any:
        async def go() -> Any:
            engine = db.make_engine(self.cfg.database.url)
            try:
                return await coro(db.make_sessionmaker(engine))
            finally:
                await engine.dispose()

        return asyncio.run(go())

    def insert(self, **kw: Any) -> int:
        return int(self.db(lambda s: tr.insert_row(s, **kw)))

    def cid(self, n: int = 1) -> str:
        return entry_client_id(self.db(tr.jid_of), n)

    def rows(self, type_: str = "OperatorRepair") -> list[m.Event]:
        return list(self.db(lambda s: tr.events(s, type_)))

    def order(self, row: int) -> m.Order:
        return cast(m.Order, self.db(lambda s: tr.order(s, row)))


@pytest.fixture
def rig(tmp_path: Path) -> Iterator[Rig]:
    r = Rig(tmp_path)
    yield r
    for lock in r.locks:
        lock.release()


def sim_with(cid: str, qty: D = D("0.01")) -> tuple[SimBroker, str]:
    sim = tr.make_sim()
    ack = asyncio.run(
        sim.submit_order(
            BrokerOrder(symbol=tr.BTC, side="buy", qty=qty, type="market", client_order_id=cid)
        )
    )
    return sim, ack.order_id


def connect_to(broker: Broker) -> Callable[[Settings], Any]:
    async def connect(settings: Settings) -> Broker:
        return broker

    return connect


def test_refuses_while_core_holds_the_lock(rig: Rig, capsys: pytest.CaptureFixture[str]) -> None:
    row = rig.insert(cid=None)
    holder = journal_lock.acquire(rig.cfg.database.url, wait_s=0)
    rig.locks.append(holder)

    code = rig.run("not_submitted", row, yes=True)

    assert code == 1
    assert "core is running" in capsys.readouterr().err
    assert rig.rows() == []


def test_blank_evidence_exits_2(rig: Rig) -> None:
    row = rig.insert(cid=None)

    assert rig.run("not_submitted", row, evidence="  ", yes=True) == 2
    assert rig.rows() == [] and rig.locks == []


def test_settled_row_is_refused_with_the_reason(
    rig: Rig, capsys: pytest.CaptureFixture[str]
) -> None:
    row = rig.insert(cid=None)

    async def settle(s: Any) -> None:
        async with s() as session:
            await session.execute(
                update(m.Order).where(m.Order.id == row).values(settled_at=tr.NOW)
            )
            await session.commit()

    rig.db(settle)

    assert rig.run("not_submitted", row, yes=True) == 1
    assert "already settled" in capsys.readouterr().err


def test_link_mismatch_lists_the_difference(rig: Rig, capsys: pytest.CaptureFixture[str]) -> None:
    cid = rig.cid()
    sim, oid = sim_with(cid, qty=D("0.02"))
    row = rig.insert(cid=cid, qty=D("0.01"))

    code = rig.run("link", row, broker=connect_to(sim), broker_order_id=oid, yes=True)

    err = capsys.readouterr().err
    assert code == 1 and "does not match" in err and "qty" in err
    assert rig.rows() == [] and rig.order(row).broker_order_id is None


def test_link_applies_and_records_the_operator(
    rig: Rig, capsys: pytest.CaptureFixture[str]
) -> None:
    cid = rig.cid()
    sim, oid = sim_with(cid)
    row = rig.insert(cid=cid)

    code = rig.run(
        "link", row, broker=connect_to(sim), broker_order_id=oid, yes=True, operator="bob"
    )

    out = capsys.readouterr().out
    assert code == 0 and "per docs/runbook.md" in out and "broker_order_id" in out
    assert rig.order(row).broker_order_id == oid
    (event,) = rig.rows()
    assert event.payload["operator"] == "bob" and event.payload["evidence"] == EVIDENCE


def test_yes_skips_confirm_and_declined_confirm_writes_nothing(rig: Rig) -> None:
    row = rig.insert(cid=None)

    rig.answer = False
    assert rig.run("not_submitted", row) == 1
    assert rig.confirms == ["Apply?"] and rig.rows() == []
    assert rig.order(row).status == "pending"

    assert rig.run("not_submitted", row, yes=True) == 0
    assert rig.confirms == ["Apply?"]  # not asked again
    assert rig.order(row).status == "not_submitted" and len(rig.rows()) == 1


def test_not_submitted_prints_the_evidence_condition(
    rig: Rig, capsys: pytest.CaptureFixture[str]
) -> None:
    row = rig.insert(cid=None)
    rig.answer = False

    rig.run("not_submitted", row)

    out = capsys.readouterr().out
    assert NOT_SUBMITTED_CONDITION in out
    assert "never and will never" in out and "does not qualify" in out and "final" in out


def test_connect_failure_refuses_without_traceback(
    rig: Rig, capsys: pytest.CaptureFixture[str]
) -> None:
    row = rig.insert(cid=rig.cid())

    async def connect(settings: Settings) -> Broker:
        raise OrderMethodsUnavailable("alpaca orders not until 1.6")

    code = rig.run("link", row, broker=connect, broker_order_id="x", yes=True)

    err = capsys.readouterr().err
    assert code == 1 and "broker lookup failed: OrderMethodsUnavailable" in err
    assert "Traceback" not in err and rig.rows() == []


def test_lookup_failure_refuses_without_traceback(
    rig: Rig, capsys: pytest.CaptureFixture[str]
) -> None:
    row = rig.insert(cid=rig.cid())
    sim = tr.make_sim()

    async def boom(client_order_id: str) -> BrokerOrderView | None:
        raise BrokerError("503")

    sim.get_order_by_client_id = boom  # type: ignore[method-assign]
    code = rig.run("link", row, broker=connect_to(sim), broker_order_id="x", yes=True)

    err = capsys.readouterr().err
    assert code == 1 and "broker lookup failed: BrokerError: 503" in err
    assert "Traceback" not in err and rig.rows() == []


def test_oversell_without_anomaly_is_refused(rig: Rig, capsys: pytest.CaptureFixture[str]) -> None:
    row = rig.insert(cid=None)

    assert rig.run("oversell", row, yes=True) == 1
    assert "not a sell" in capsys.readouterr().err


def test_help_lists_the_three_commands() -> None:
    result = runner.invoke(app, ["repair", "--help"])

    assert result.exit_code == 0
    for command in ("link", "not-submitted", "oversell"):
        assert command in result.output


def test_parsing_requires_evidence(tmp_path: Path) -> None:
    result = runner.invoke(app, ["repair", "oversell", "1", "-c", str(_tmp_config(tmp_path))])

    assert result.exit_code == 2


def test_parsing_blank_evidence_exits_2(tmp_path: Path) -> None:
    config = _tmp_config(tmp_path)

    result = runner.invoke(
        app, ["repair", "not-submitted", "1", "--evidence", " ", "-c", str(config)]
    )

    assert result.exit_code == 2
