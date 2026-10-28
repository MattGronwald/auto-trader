import asyncio
import io
import json
import subprocess
import sys
from collections.abc import Callable, Coroutine
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from autotrader.cli import _run_core_process, app
from autotrader.core import lock as journal_lock
from autotrader.core.config import Settings, load_config, load_profile
from autotrader.core.logging import configure_logging
from autotrader.core.shutdown import ShutdownControl, Watchdog
from autotrader.journal import db, repo

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
        ["run", "api"],
        ["review-now"],
        ["backtest-signals"],
    ],
)
def test_stub_commands_fail_loudly(args: list[str]) -> None:
    # A stub must never look like success: an operator running a command
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

    result = runner.invoke(app, ["--log-level", "debug", "review-now"])

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


def test_run_core_with_invalid_config_exits_1(tmp_path: Path) -> None:
    bad = tmp_path / "config.yaml"
    bad.write_text("mode: sideways\n")

    result = runner.invoke(app, ["run", "core", "-c", str(bad)])

    assert result.exit_code == 1
    assert "invalid config" in result.output


HOLD = """
import sys
from autotrader.core.lock import acquire
lock = acquire(sys.argv[1], wait_s=0)
print("held", flush=True)
sys.stdin.readline()
"""


class Spy:
    def __init__(self, result: Any = None) -> None:
        self.calls: list[tuple[Any, ...]] = []
        self.result = result

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        self.calls.append(args)
        return self.result


class FakeTimer:
    def __init__(self, seconds: float, fn: Callable[[], None]) -> None:
        self.seconds = seconds
        self.daemon = False
        self.started = False

    def start(self) -> None:
        self.started = True


class Locks:
    """Real acquire, released when the test ends (production never releases early)."""

    def __init__(self) -> None:
        self.held: list[journal_lock.JournalLock] = []

    def __call__(self, url: str, **kwargs: Any) -> journal_lock.JournalLock:
        self.held.append(journal_lock.acquire(url, **kwargs))
        return self.held[-1]

    def release(self) -> None:
        for held in self.held:
            held.release()


def _fast_config(tmp_path: Path) -> Path:
    path = _tmp_config(tmp_path)
    path.write_text(path.read_text().replace("timeout_s: 30", "timeout_s: 0.1"))
    return path


def test_losing_core_never_migrates_or_opens_the_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("autotrader.core.shutdown.WATCHDOG_GRACE_S", 0.0)
    cfg = load_config(_fast_config(tmp_path), Settings(_env_file=None))
    proc = subprocess.Popen(  # noqa: S603  (fixed argv: this interpreter + a constant script)
        [sys.executable, "-c", HOLD, cfg.database.url],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None and proc.stdin is not None
    assert proc.stdout.readline().strip() == "held"
    upgrade, make_source, run = Spy(), Spy(), Spy(0)
    try:
        code = _run_core_process(
            cfg,
            Settings(_env_file=None),
            load_profile(cfg.strategy),
            install=Spy(),
            upgrade=upgrade,
            make_source=make_source,
            run=run,
        )
    finally:
        proc.stdin.close()
        proc.wait(timeout=10)
        proc.stdout.close()
    assert code == 1
    assert upgrade.calls == [] and make_source.calls == [] and run.calls == []


def test_signal_during_migration_arms_the_watchdog_and_skips_startup(tmp_path: Path) -> None:
    configure_logging(stream=io.StringIO())  # an earlier CliRunner test left a closed stdout
    cfg = load_config(_tmp_config(tmp_path), Settings(_env_file=None))
    locks = Locks()
    timers: list[FakeTimer] = []
    handlers: list[Any] = []

    def factory(seconds: float, fn: Callable[[], None]) -> FakeTimer:
        timers.append(FakeTimer(seconds, fn))
        return timers[-1]

    def upgrade(url: str) -> None:
        from autotrader.journal import db

        db.upgrade(url)
        handlers[0](2, None)  # SIGINT arrives during the migration

    class Source:
        stopped = False

        async def stop(self) -> None:
            Source.stopped = True

    code = _run_core_process(
        cfg,
        Settings(_env_file=None),
        load_profile(cfg.strategy),
        watchdog=Watchdog(timer_factory=factory, exit=Spy()),
        install=handlers.append,
        upgrade=upgrade,
        make_source=lambda settings, symbols: Source(),
        acquire=locks,
    )
    locks.release()

    assert code == 0
    assert timers and timers[0].started and timers[0].seconds == 34.5  # exit by T + 5 = 35
    assert Source.stopped


def test_failure_inside_run_exits_1_and_leaves_the_watchdog_armed(tmp_path: Path) -> None:
    cfg = load_config(_tmp_config(tmp_path), Settings(_env_file=None))
    locks = Locks()
    timers: list[FakeTimer] = []
    order: list[str] = []

    def factory(seconds: float, fn: Callable[[], None]) -> FakeTimer:
        timers.append(FakeTimer(seconds, fn))
        return timers[-1]

    def run(coro: Coroutine[Any, Any, int]) -> int:
        coro.close()
        order.append("run")
        raise RuntimeError("stream died")

    code = _run_core_process(
        cfg,
        Settings(_env_file=None),
        load_profile(cfg.strategy),
        watchdog=Watchdog(timer_factory=factory, exit=Spy()),
        install=Spy(),
        upgrade=Spy(),
        make_source=Spy(),
        run=run,
        acquire=locks,
    )
    locks.release()

    assert code == 1
    assert order == ["run"]
    assert len(timers) == 1 and timers[0].started  # armed for the exit; never cancelled


def test_ctrl_c_while_waiting_for_the_lock_exits_130(tmp_path: Path) -> None:
    cfg = load_config(_tmp_config(tmp_path), Settings(_env_file=None))
    upgrade = Spy()

    def interrupted(url: str, **kwargs: Any) -> Any:
        raise KeyboardInterrupt

    code = _run_core_process(
        cfg,
        Settings(_env_file=None),
        load_profile(cfg.strategy),
        install=Spy(),
        upgrade=upgrade,
        acquire=interrupted,
    )

    assert code == 130 and upgrade.calls == []


def _core_failed(buf: io.StringIO) -> list[dict[str, Any]]:
    lines = [json.loads(ln) for ln in buf.getvalue().splitlines() if ln.startswith("{")]
    return [ln for ln in lines if ln["event"] == "core.failed"]


def test_startup_failure_before_run_core_is_logged_as_core_failed(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    cfg = load_config(_tmp_config(tmp_path), Settings(_env_file=None))
    locks = Locks()

    def upgrade(url: str) -> None:
        raise RuntimeError("migration broke")

    code = _run_core_process(
        cfg,
        Settings(_env_file=None),
        load_profile(cfg.strategy),
        watchdog=Watchdog(timer_factory=FakeTimer, exit=Spy()),
        install=Spy(),
        upgrade=upgrade,
        make_source=Spy(),
        run=Spy(0),
        acquire=locks,
    )
    locks.release()

    assert code == 1
    (failed,) = _core_failed(buf)
    assert failed["cause"] == "startup_failure"
    assert "migration broke" in failed["exception"] and "Traceback" in failed["exception"]
    assert capsys.readouterr().err == "run core failed: RuntimeError: migration broke\n"


def test_failure_run_core_already_logged_is_not_logged_twice(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    cfg = load_config(_tmp_config(tmp_path), Settings(_env_file=None))
    locks = Locks()

    async def run_core(*args: Any, control: ShutdownControl, **kwargs: Any) -> int:
        control.begin("stream_death")  # run_core logs core.failed itself, then re-raises
        raise ConnectionError("stream died")

    monkeypatch.setattr("autotrader.core.runtime.run_core", run_core)
    code = _run_core_process(
        cfg,
        Settings(_env_file=None),
        load_profile(cfg.strategy),
        watchdog=Watchdog(timer_factory=FakeTimer, exit=Spy()),
        install=Spy(),
        upgrade=Spy(),
        make_source=Spy(),
        acquire=locks,
    )
    locks.release()

    assert code == 1
    assert _core_failed(buf) == []


def test_engine_failure_inside_run_core_exits_1_with_a_one_line_summary(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    cfg = load_config(_tmp_config(tmp_path), Settings(_env_file=None))
    locks = Locks()

    def make_engine(url: str) -> Any:
        raise RuntimeError("no engine")

    monkeypatch.setattr("autotrader.core.runtime.make_engine", make_engine)
    code = _run_core_process(
        cfg,
        Settings(_env_file=None),
        load_profile(cfg.strategy),
        watchdog=Watchdog(timer_factory=FakeTimer, exit=Spy()),
        install=Spy(),
        upgrade=Spy(),
        make_source=Spy(),
        acquire=locks,
    )
    locks.release()

    assert code == 1
    assert capsys.readouterr().err == "run core failed: RuntimeError: no engine\n"
    (failed,) = _core_failed(buf)
    assert failed["cause"] == "startup_failure" and "no engine" in failed["exception"]


def _read_flatten_state(config: Path) -> tuple[dict[str, Any] | None, Any, dict[str, Any] | None]:
    cfg = load_config(config, Settings(_env_file=None))

    async def read() -> tuple[dict[str, Any] | None, Any, dict[str, Any] | None]:
        engine = db.make_engine(cfg.database.url)
        sessions = db.make_sessionmaker(engine)
        try:
            async with sessions() as session:
                request = await repo.get_control(session, "flatten_requested")
                paused = await repo.get_control(session, "paused")
                result = await repo.get_control(session, "flatten_result")
            return request, paused, result
        finally:
            await engine.dispose()

    return asyncio.run(read())


def test_flatten_without_a_core_queues_the_request_and_pauses(tmp_path: Path) -> None:
    config = _tmp_config(tmp_path)
    assert runner.invoke(app, ["db", "upgrade", "-c", str(config)]).exit_code == 0

    result = runner.invoke(app, ["flatten", "-c", str(config), "--reason", "test"])

    assert result.exit_code == 0, result.output
    assert "queued" in result.output
    request, paused, _ = _read_flatten_state(config)
    assert request is not None and request["reason"] == "test" and paused is True


def test_flatten_with_a_running_core_waits_and_reports_pending(tmp_path: Path) -> None:
    config = _tmp_config(tmp_path)
    runner.invoke(app, ["db", "upgrade", "-c", str(config)])
    cfg = load_config(config, Settings(_env_file=None))
    held = journal_lock.acquire(cfg.database.url, wait_s=0)  # stands in for a core
    try:
        result = runner.invoke(app, ["flatten", "-c", str(config), "--wait-s", "0"])
    finally:
        held.release()
    assert result.exit_code == 1
    assert "pending" in result.output


def test_flatten_cancel_clears_the_request_and_keeps_the_pause(tmp_path: Path) -> None:
    config = _tmp_config(tmp_path)
    runner.invoke(app, ["db", "upgrade", "-c", str(config)])
    assert runner.invoke(app, ["flatten", "-c", str(config)]).exit_code == 0

    cancelled = runner.invoke(app, ["flatten", "-c", str(config), "--cancel"])
    again = runner.invoke(app, ["flatten", "-c", str(config), "--cancel"])

    assert cancelled.exit_code == 0 and "cancelled" in cancelled.output
    assert again.exit_code == 0 and "no flatten request" in again.output
    request, paused, result = _read_flatten_state(config)
    assert request is None and paused is True
    assert result is not None and result["cancelled"] is True


def test_flatten_twice_keeps_the_first_request(tmp_path: Path) -> None:
    config = _tmp_config(tmp_path)
    runner.invoke(app, ["db", "upgrade", "-c", str(config)])
    runner.invoke(app, ["flatten", "-c", str(config), "--reason", "first"])
    first, _, _ = _read_flatten_state(config)

    second = runner.invoke(app, ["flatten", "-c", str(config), "--reason", "second"])

    assert second.exit_code == 0 and "already pending" in second.output
    kept, _, _ = _read_flatten_state(config)
    assert kept == first
