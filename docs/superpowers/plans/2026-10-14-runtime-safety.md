# Runtime Safety Implementation Plan (WP 1.5a)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `run core` safe to leave running. This adds:
- a single-writer journal lock taken before the migration;
- independent supervised loops (feed tick, kill polling, equity snapshots) with stall and snapshot-freshness logging;
- one bounded shutdown coordinator for every exit cause, with a synchronous trigger, a second-signal hard exit and a watchdog thread.

**Architecture:** The plan adds three small modules and then rewires two existing ones:
- `core/lock.py`: the `flock` lock on the canonical journal path.
- `core/shutdown.py`: `ShutdownControl`, the synchronous trigger; `Watchdog`; the signal handler.
- `core/supervise.py`: `Supervisor`, `Loop`, `Freshness`.
- `core/runtime.run_core` is rewired onto them.
- `cli.run core` takes the lock before `db.upgrade`, installs the handlers, and maps outcomes to exit codes.

`RiskEngine` records when it last committed a snapshot.

**Tech Stack:** Python 3.12, asyncio, `fcntl`, `threading`, `signal`, structlog, SQLAlchemy 2 async + aiosqlite, pytest + pytest-asyncio, typer, uv.

**Spec:** `docs/superpowers/specs/2026-10-14-runtime-safety-design.md` (approved 2026-10-14 after three Codex rounds on PR #23). Read it before starting; this plan implements it section by section.

## Global Constraints

- **Branch:** `wp-1.5a-runtime-safety`, which already contains the spec. Work on it; don't create another branch.
- **Commit trailer:** every commit message ends with exactly this line, after a blank line: `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`
- **ASCII only** in Python code, comments and docstrings. ruff `RUF001`-`RUF003` reject characters like `−` or `×`. `ruff format` doesn't wrap docstrings, so wrap them by hand at 100 characters.
- **Config:** `shutdown.timeout_s` is required, finite, `0 < v <= 300`, and `config.yaml` sets 30.
- **Constants:** `WATCHDOG_GRACE_S = 5.0` (last resort at T + 5 s), and lock poll 1.0 s.
- **Teardown outcome (Codex plan review):** a teardown step that times out is *forced*; one that raises or is cancelled is *failed* (`core.shutdown_failed`). Either makes the exit 1, the remaining steps still run, and `core.stopped` is logged only when nothing was forced or failed.
- **Loop cadence:** start to start. Pause `max(0, interval - step duration)`; an overrun starts the next step immediately, with no catch-up.
- **Watchdog line:** best-effort, written by a daemon thread with a bounded wait (`log_wait_s=0.5`); `exit(70)` runs in a `finally`, whatever happens to stderr. The timer fires `log_wait_s` before the bound, so the process ends by T + 5 s including that wait.
- **Teardown floor (dry-run finding, spec revision 2026-10-14):** `engine.dispose` gets at least 1.0 s even after the deadline, still inside the 5 s watchdog grace, so a forced teardown still closes the journal's connections. No other step gets a floor.
- **Exit codes:** 0 clean; 1 for startup failure (including lock timeout), stream death or a forced graceful deadline; 130 for the second signal; 70 for the watchdog.
- **Lock error text** contains `journal locked by PID <pid> since <ts>`. The non-file DB error is `single-writer lock needs a file journal; cross-host ownership is WP 3.5`.
- **Log event names** (verbatim):
  - `core.started`, `core.stopping`, `core.stopped`, `core.shutdown_forced`, `core.shutdown_abandoned`, `core.kill_detected`, `core.kill_cleared`;
  - `<loop>.stalled`, `<loop>.recovered`, `<loop>.step_failed`, `<loop>.task_died`;
  - `risk.snapshot_overdue`, `risk.snapshot_fresh`, `risk.snapshot_written`, `risk.snapshot_failed`;
  - `core.watchdog_exit` (written to stderr).
- **The lock is never released on a production path;** process death releases it. `JournalLock.release()` exists for tests only.
- **Tests:**
  - Never assert wall-clock timing; use injected clocks/events (lesson from PR #13).
  - Never call the real `os._exit`.
  - Never install real process signal handlers in tests; inject `install`.
- **Before every commit** these must exit 0: `uv run pytest -q`, `uv run ruff check`, `uv run ruff format --check`, `uv run mypy`. Run `uv run ruff check --fix` (import sorting, `I001`) and `uv run ruff format` before checking.
- **No `assert` in `src/`:** ruff `S101` applies outside `tests/`; raise `RuntimeError` for internal invariants.
- **Subprocesses in tests:** close `proc.stdout` after `proc.wait()` (pytest runs with `filterwarnings = error`, so an unclosed pipe is a `ResourceWarning` failure), and mark the `Popen` line `# noqa: S603  (fixed argv: this interpreter + a constant script)`. `src/autotrader/risk/` keeps 100 % branch coverage: `uv run pytest -q --cov=autotrader.risk --cov-branch --cov-report=term-missing`.
- **Check output:** never pipe a check into `tail`; redirect to a file and read the real exit code. Chain `git commit` with `&&` after the checks.
- **Never use `git stash`.** The stash stack is shared with other sessions.
- **Known flake:** `tests/unit/risk/test_engine.py::test_hung_entry_submit_is_unknown` is a known pre-existing timing flake. If it fails, re-run it in isolation and note it.

## Review Focus

Conditions the spec implies that no task would otherwise exercise. Each one has a test in the owning task:

1. **A loop's step raises on every call.** Expected: the loop keeps running, and it logs `<loop>.step_failed` each time, without being marked dead. Owner: Task 4 (`test_raising_step_keeps_its_loop_running`).
2. **`supervisor.check()` after `stop()` has begun.** Expected: no restart of loops that are stopping; a done task during shutdown is not "died". Owner: Task 4 (`test_check_never_restarts_loops_after_stop`).
3. **A shutdown requested before `run_core` starts**, e.g. a signal during the migration. Expected: no broker connect, no stream, no loops; teardown still disposes the engine; exit 0. Owner: Task 6 (`test_shutdown_requested_before_start_skips_startup`).
4. **`run_core` given a lock for a different journal.** Expected: `ValueError` before anything opens the journal. Owner: Task 6 (`test_run_core_rejects_a_lock_for_another_journal`).
5. **A watchdog armed twice** (`begin` after a startup failure following a signal). Expected: one timer, the original deadline. Owner: Task 3 (`test_begin_is_idempotent_and_never_rearms`).

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `src/autotrader/core/config.py`, `config.yaml` | `shutdown.timeout_s` | 1 |
| `src/autotrader/core/lock.py` (new) | Canonical journal path, `flock` single-writer lock | 2 |
| `src/autotrader/core/shutdown.py` (new) | `ShutdownControl`, `Watchdog`, signal handler, exit-code constants | 3 |
| `src/autotrader/core/supervise.py` (new) | `Loop`, `Freshness`, `Supervisor` | 4 |
| `src/autotrader/risk/engine.py` | `last_snapshot_at` | 5 |
| `src/autotrader/core/runtime.py` | `run_core` on lock/control/supervisor; bounded teardown | 6 |
| `src/autotrader/cli.py` | Lock before `db.upgrade`, handlers, watchdog, exit codes | 7 |
| `SPEC.md`, `PLAN.md`, `CLAUDE.md` | Docs alignment | 8 |
| Tests | `tests/unit/core/test_config.py`, `test_lock.py` (new), `test_shutdown.py` (new), `test_supervise.py` (new), `test_runtime.py`, `tests/unit/risk/test_engine.py`, `tests/unit/test_cli.py` | 1-7 |

---

### Task 1: `shutdown.timeout_s`

**Files:**
- Modify: `src/autotrader/core/config.py` (class `Shutdown`, around line 369)
- Modify: `config.yaml` (the `shutdown:` block)
- Test: `tests/unit/core/test_config.py`

**Interfaces:**
- Produces: `AppConfig.shutdown.timeout_s: float`, finite, `0 < v <= 300`.

- [ ] **Step 1: Write the failing tests** (append to `tests/unit/core/test_config.py`)

```python
def test_shutdown_timeout_loads_from_the_repo_config() -> None:
    assert load_config(CONFIG, paper_settings()).shutdown.timeout_s == 30


@pytest.mark.parametrize("value", [0, -1, 301, "NaN", "Infinity", None])
def test_shutdown_timeout_is_required_finite_and_bounded(tmp_path: Path, value: Any) -> None:
    shutdown: dict[str, Any] = {"flatten": False}
    if value is not None:
        shutdown["timeout_s"] = value
    path = _write_config(tmp_path, shutdown=shutdown)

    with pytest.raises(ConfigError, match="timeout_s"):
        load_config(path, paper_settings())
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/core/test_config.py -k shutdown_timeout -q`
Expected: FAIL for `test_shutdown_timeout_loads_from_the_repo_config` and the `None` case. The other cases already raise, because the extra-key error also names `timeout_s`; that's expected, and they keep passing for the right reason after Step 3.

- [ ] **Step 3: Implement**

In `src/autotrader/core/config.py` replace the `Shutdown` class:

```python
class Shutdown(_Frozen):
    flatten: bool
    # WP 1.5a: graceful teardown budget T; the watchdog ends the process at T + 5 s.
    # Above the Cycle Runner's 20 s close budget (wired in WP 1.5d). allow_inf_nan=False
    # rejects NaN/inf, which the bounds alone would not.
    timeout_s: Annotated[float, Field(gt=0, le=300, allow_inf_nan=False)]
```

In `config.yaml` replace the `shutdown:` block:

```yaml
shutdown:
  flatten: false                # D7: flatten on graceful stop
  timeout_s: 30                 # WP 1.5a: graceful teardown budget; watchdog at +5 s
```

- [ ] **Step 4: Run all tests** with `uv run pytest -q` (redirect the output, check the exit code). Expected: PASS.

- [ ] **Step 5: Lint, type-check, commit**

```bash
uv run ruff format && uv run ruff check && uv run mypy && git add src/autotrader/core/config.py config.yaml tests/unit/core/test_config.py && git commit -m "WP 1.5a: shutdown.timeout_s config

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Single-writer journal lock (`core/lock.py`)

**Files:**
- Create: `src/autotrader/core/lock.py`
- Test: `tests/unit/core/test_lock.py` (new)

**Interfaces:**
- Produces:
  - `JournalLockError(RuntimeError)`
  - `canonical_journal(url: str) -> tuple[str, Path]`: the canonical URL and the canonical DB file path
  - `JournalLock` dataclass with `url: str`, `db_path: Path`, `lock_path: Path`, `fd: int | None`, the `held` property, and `release()` (tests only)
  - `acquire(url: str, *, wait_s: float, poll_s: float = POLL_S, monotonic: Callable[[], float] = time.monotonic, sleep: Callable[[float], None] = time.sleep) -> JournalLock`
  - `POLL_S = 1.0`

- [ ] **Step 1: Write the failing tests** (`tests/unit/core/test_lock.py`)

```python
import os
import subprocess
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path

import pytest

from autotrader.core.lock import JournalLockError, acquire, canonical_journal

HOLD = """
import sys
from autotrader.core.lock import acquire
lock = acquire(sys.argv[1], wait_s=0)
print("held", flush=True)
sys.stdin.readline()
"""


@contextmanager
def held_elsewhere(url: str) -> Iterator[subprocess.Popen[str]]:
    """Hold the journal lock in another process until the block ends."""
    proc = subprocess.Popen(  # noqa: S603  (fixed argv: this interpreter + a constant script)
        [sys.executable, "-c", HOLD, url],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        text=True,
    )
    assert proc.stdout is not None and proc.stdin is not None
    assert proc.stdout.readline().strip() == "held"
    try:
        yield proc
    finally:
        proc.stdin.close()
        proc.wait(timeout=10)
        proc.stdout.close()


def url_for(path: Path) -> str:
    return f"sqlite+aiosqlite:///{path}"


def test_lock_file_sits_beside_the_canonical_journal(tmp_path: Path) -> None:
    real = Path(os.path.realpath(tmp_path)) / "j.db"
    lock = acquire(url_for(tmp_path / "j.db"), wait_s=0)
    try:
        assert lock.held
        assert lock.db_path == real
        assert lock.lock_path == real.with_name("j.db.lock")
        assert lock.url == url_for(real)
        assert lock.lock_path.read_text().startswith(f"PID {os.getpid()} since ")
    finally:
        lock.release()
    assert not lock.held


def test_second_holder_is_refused_with_the_holders_pid(tmp_path: Path) -> None:
    url = url_for(tmp_path / "j.db")
    with held_elsewhere(url) as proc, pytest.raises(JournalLockError) as exc:
        acquire(url, wait_s=0)
    assert f"journal locked by PID {proc.pid} since " in str(exc.value)


def test_lock_is_free_again_once_the_holder_exits(tmp_path: Path) -> None:
    url = url_for(tmp_path / "j.db")
    with held_elsewhere(url):
        pass
    lock = acquire(url, wait_s=0)
    lock.release()


def test_contention_waits_with_bounded_polls(tmp_path: Path) -> None:
    url = url_for(tmp_path / "j.db")
    now = [0.0]
    sleeps: list[float] = []

    def sleep(s: float) -> None:
        sleeps.append(s)
        now[0] += s

    with held_elsewhere(url), pytest.raises(JournalLockError):
        acquire(url, wait_s=2.5, poll_s=1.0, monotonic=lambda: now[0], sleep=sleep)
    assert sleeps == [1.0, 1.0, 1.0]  # polls at 0, 1, 2, 3 s; gives up past 2.5 s


@pytest.mark.parametrize("kind", ["file", "directory"])
def test_symlinked_journal_contends_with_the_real_path(tmp_path: Path, kind: str) -> None:
    real_dir = tmp_path / "real"
    real_dir.mkdir()
    (real_dir / "j.db").touch()
    if kind == "file":
        alias = tmp_path / "alias.db"
        alias.symlink_to(real_dir / "j.db")
    else:
        (tmp_path / "linkdir").symlink_to(real_dir)
        alias = tmp_path / "linkdir" / "j.db"

    with held_elsewhere(url_for(real_dir / "j.db")), pytest.raises(JournalLockError):
        acquire(url_for(alias), wait_s=0)


@pytest.mark.parametrize(
    "url",
    [
        "postgresql+asyncpg://u@db/autotrader",
        "sqlite+aiosqlite:///:memory:",
        "sqlite+aiosqlite://",
    ],
)
def test_non_file_journals_are_refused(url: str) -> None:
    with pytest.raises(JournalLockError, match="needs a file journal"):
        canonical_journal(url)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/core/test_lock.py -q`
Expected: collection error, `No module named 'autotrader.core.lock'`.

- [ ] **Step 3: Implement** (`src/autotrader/core/lock.py`)

```python
"""Single-writer journal lock (WP 1.5a, spec 2026-10-14 section 2).

`run core` holds an exclusive `flock` on `<canonical journal path>.lock` for its whole
lifetime, so a second core can never write the same journal. The path is canonical
(`os.path.realpath`), so symlinked aliases of one journal contend for one lock; hard links
are unsupported. The lock lives in a sidecar file, not on the database, because SQLite's own
POSIX locks may interact with `flock` on macOS. Only process death releases it on a
production path: a cancelled aiosqlite caller can leave a queued write running, so closing
the descriptor early could let another core in while this one still writes.
"""

import fcntl
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from sqlalchemy.engine import make_url

POLL_S = 1.0
_NOT_A_FILE = "single-writer lock needs a file journal; cross-host ownership is WP 3.5"


class JournalLockError(RuntimeError):
    pass


def canonical_journal(url: str) -> tuple[str, Path]:
    """The URL that opens the canonical journal file, and that file's path."""
    parsed = make_url(url)
    db = parsed.database
    if parsed.get_backend_name() != "sqlite" or not db or db == ":memory:":
        raise JournalLockError(_NOT_A_FILE)
    path = Path(os.path.realpath(db))
    return parsed.set(database=str(path)).render_as_string(hide_password=False), path


@dataclass
class JournalLock:
    url: str  # opens the canonical journal; use it for the migration and the engine
    db_path: Path
    lock_path: Path
    fd: int | None

    @property
    def held(self) -> bool:
        return self.fd is not None

    def release(self) -> None:
        """Tests only: production code keeps the lock until the process exits."""
        if self.fd is not None:
            os.close(self.fd)
            self.fd = None


def acquire(
    url: str,
    *,
    wait_s: float,
    poll_s: float = POLL_S,
    monotonic: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
) -> JournalLock:
    """Take the journal's lock, retrying every `poll_s` for up to `wait_s` seconds."""
    canonical, db_path = canonical_journal(url)
    lock_path = db_path.with_name(db_path.name + ".lock")
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(lock_path, os.O_RDWR | os.O_CREAT, 0o644)
    deadline = monotonic() + wait_s
    try:
        while True:
            try:
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
                break
            except BlockingIOError:
                if monotonic() >= deadline:
                    raise JournalLockError(f"journal locked by {_holder(lock_path)}") from None
                sleep(poll_s)
    except BaseException:
        os.close(fd)
        raise
    os.ftruncate(fd, 0)
    os.pwrite(fd, f"PID {os.getpid()} since {datetime.now(UTC).isoformat()}\n".encode(), 0)
    return JournalLock(url=canonical, db_path=db_path, lock_path=lock_path, fd=fd)


def _holder(lock_path: Path) -> str:
    """Diagnostic text the holder wrote ("PID <pid> since <ts>"); never the lock itself."""
    try:
        text = lock_path.read_text().strip()
    except OSError:
        text = ""
    return text or "an unknown process"
```

- [ ] **Step 4: Run all tests** with `uv run pytest -q` (redirect the output, check the exit code). Expected: PASS.

- [ ] **Step 5: Lint, type-check, commit**

```bash
uv run ruff format && uv run ruff check && uv run mypy && git add src/autotrader/core/lock.py tests/unit/core/test_lock.py && git commit -m "WP 1.5a: single-writer journal lock

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Shutdown trigger, watchdog, signal handler (`core/shutdown.py`)

**Files:**
- Create: `src/autotrader/core/shutdown.py`
- Test: `tests/unit/core/test_shutdown.py` (new)

**Interfaces:**
- Produces:
  - Constants: `WATCHDOG_GRACE_S = 5.0`, `EXIT_CLEAN = 0`, `EXIT_FAILED = 1`, `EXIT_HARD = 130`, `EXIT_WATCHDOG = 70`.
  - `Watchdog(*, timer_factory=threading.Timer, exit=os._exit, stderr=None, log_wait_s=0.5)`. Firing always exits 70; the stderr line is best-effort and bounded. with `arm(seconds: float) -> None` (once only), `cancel() -> None` and the property `armed: bool`.
  - `ShutdownControl(timeout_s: float, *, arm_watchdog: Callable[[float], None], monotonic=time.monotonic)`, with:
    - attributes `cause: str | None`, `deadline: float | None` and `timeout_s`;
    - property `requested: bool`;
    - `begin(cause: str) -> None`, which is synchronous and idempotent;
    - `attach() -> asyncio.Event`, which must be called inside the running loop;
    - `remaining() -> float`.
  - `make_signal_handler(control, *, exit=os._exit) -> Callable[[int, FrameType | None], None]`
  - `install_signal_handlers(handler) -> None`

- [ ] **Step 1: Write the failing tests** (`tests/unit/core/test_shutdown.py`)

```python
import asyncio
import io
import json
import signal
import threading
from collections.abc import Callable

import pytest

from autotrader.core.shutdown import (
    EXIT_HARD,
    EXIT_WATCHDOG,
    WATCHDOG_GRACE_S,
    ShutdownControl,
    Watchdog,
    make_signal_handler,
)


class FakeTimer:
    def __init__(self, seconds: float, fn: Callable[[], None]) -> None:
        self.seconds, self.fn = seconds, fn
        self.daemon = False
        self.started = self.cancelled = False

    def start(self) -> None:
        self.started = True

    def cancel(self) -> None:
        self.cancelled = True


class Timers:
    def __init__(self) -> None:
        self.made: list[FakeTimer] = []

    def __call__(self, seconds: float, fn: Callable[[], None]) -> FakeTimer:
        timer = FakeTimer(seconds, fn)
        self.made.append(timer)
        return timer


class Clock:
    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def test_watchdog_arms_once_as_a_daemon_and_exits_70_with_a_stderr_line() -> None:
    timers, err = Timers(), io.StringIO()
    exits: list[int] = []
    dog = Watchdog(timer_factory=timers, exit=exits.append, stderr=err)

    dog.arm(35)
    dog.arm(99)  # a second arm is a no-op

    (timer,) = timers.made
    assert (timer.seconds, timer.daemon, timer.started) == (34.5, True, True)  # 35 - log wait
    timer.fn()
    assert exits == [EXIT_WATCHDOG]
    assert json.loads(err.getvalue())["event"] == "core.watchdog_exit"
    dog.cancel()
    assert timer.cancelled


class RaisingStream(io.StringIO):
    def write(self, s: str) -> int:
        raise OSError("broken pipe")


class BlockingStream(io.StringIO):
    def __init__(self) -> None:
        super().__init__()
        self.release = threading.Event()

    def write(self, s: str) -> int:
        self.release.wait(5)  # stands in for a full pipe
        return len(s)


def test_watchdog_exit_including_the_log_wait_stays_within_the_armed_bound() -> None:
    timers = Timers()
    Watchdog(timer_factory=timers, exit=lambda code: None, log_wait_s=2.0).arm(35)

    (timer,) = timers.made
    assert timer.seconds + 2.0 == 35  # fires at 33, the log wait ends by 35


def test_watchdog_exits_70_even_if_stderr_raises() -> None:
    timers = Timers()
    exits: list[int] = []
    Watchdog(timer_factory=timers, exit=exits.append, stderr=RaisingStream()).arm(35)

    timers.made[0].fn()

    assert exits == [EXIT_WATCHDOG]


def test_watchdog_exits_70_even_if_stderr_blocks() -> None:
    timers, stream = Timers(), BlockingStream()
    exits: list[int] = []
    dog = Watchdog(timer_factory=timers, exit=exits.append, stderr=stream, log_wait_s=0.01)
    dog.arm(35)

    try:
        timers.made[0].fn()  # returns after the bounded wait, not when the write ends
        assert exits == [EXIT_WATCHDOG]
    finally:
        stream.release.set()


def test_begin_records_deadline_and_arms_before_anything_else() -> None:
    clock = Clock()
    armed: list[float] = []
    control = ShutdownControl(30, arm_watchdog=armed.append, monotonic=clock)

    control.begin("signal")

    assert (control.cause, control.deadline, control.requested) == ("signal", 130.0, True)
    assert armed == [30 + WATCHDOG_GRACE_S]
    clock.now = 110.0
    assert control.remaining() == 20.0


def test_begin_is_idempotent_and_never_rearms() -> None:
    clock = Clock()
    armed: list[float] = []
    control = ShutdownControl(30, arm_watchdog=armed.append, monotonic=clock)
    control.begin("signal")
    clock.now = 125.0

    control.begin("startup_failure")

    assert (control.cause, control.deadline) == ("signal", 130.0)
    assert armed == [35.0]


async def test_first_signal_arms_the_watchdog_while_the_loop_is_blocked() -> None:
    clock, timers = Clock(), Timers()
    exits: list[int] = []
    dog = Watchdog(timer_factory=timers, exit=exits.append)
    control = ShutdownControl(30, arm_watchdog=dog.arm, monotonic=clock)
    wakeup = control.attach()
    handler = make_signal_handler(control, exit=exits.append)

    # This coroutine doesn't yield between here and the asserts: the loop is "blocked".
    handler(signal.SIGINT, None)
    assert timers.made and timers.made[0].started  # armed in the handler itself
    assert control.deadline == 130.0  # measured from the signal
    assert not wakeup.is_set()  # the queued wake-up hasn't run: no loop callback yet

    await asyncio.sleep(0)
    assert wakeup.is_set()
    assert exits == []


def test_second_signal_exits_130_without_touching_the_loop() -> None:
    exits: list[int] = []
    control = ShutdownControl(30, arm_watchdog=lambda s: None)
    handler = make_signal_handler(control, exit=exits.append)

    handler(signal.SIGTERM, None)
    handler(signal.SIGINT, None)

    assert exits == [EXIT_HARD]
    assert control.cause == "signal"


async def test_attach_after_begin_starts_set() -> None:
    control = ShutdownControl(30, arm_watchdog=lambda s: None)
    control.begin("signal")  # e.g. a signal during the migration, before any loop

    assert control.attach().is_set()


def test_remaining_before_begin_is_the_whole_budget() -> None:
    assert ShutdownControl(30, arm_watchdog=lambda s: None).remaining() == 30


def test_install_signal_handlers_targets_sigint_and_sigterm(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    installed: list[int] = []
    monkeypatch.setattr("signal.signal", lambda sig, h: installed.append(sig))
    from autotrader.core.shutdown import install_signal_handlers

    install_signal_handlers(lambda signum, frame: None)

    assert installed == [signal.SIGINT, signal.SIGTERM]
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/core/test_shutdown.py -q`
Expected: collection error, `No module named 'autotrader.core.shutdown'`.

- [ ] **Step 3: Implement** (`src/autotrader/core/shutdown.py`)

```python
"""Shutdown trigger, watchdog and signal handling (WP 1.5a, spec 2026-10-14 section 4).

`ShutdownControl.begin()` is plain synchronous code: the first SIGINT/SIGTERM calls it from a
Python-level `signal.signal` handler, which runs between bytecodes even while the event loop
is stuck in synchronous Python code. It records the monotonic deadline (T from the trigger)
and starts the watchdog thread *before* asking the loop for the async teardown. Every exit
cause uses it; later calls are no-ops. A second signal calls `os._exit(130)` at once: no
async cleanup and no lock release (process death releases the journal lock). The watchdog
thread ends the process with `os._exit(70)` at T + 5 s if teardown still hangs.
"""

import asyncio
import contextlib
import json
import os
import signal
import sys
import threading
import time
from collections.abc import Callable
from types import FrameType
from typing import Protocol, TextIO

WATCHDOG_GRACE_S = 5.0
EXIT_CLEAN = 0
EXIT_FAILED = 1
EXIT_HARD = 130
EXIT_WATCHDOG = 70


class _Timer(Protocol):
    daemon: bool

    def start(self) -> None: ...
    def cancel(self) -> None: ...


TimerFactory = Callable[[float, Callable[[], None]], _Timer]
SignalHandler = Callable[[int, FrameType | None], None]


class Watchdog:
    """Last resort: a daemon thread that ends the process if teardown hangs."""

    def __init__(
        self,
        *,
        timer_factory: TimerFactory = threading.Timer,
        exit: Callable[[int], object] = os._exit,
        stderr: TextIO | None = None,
        log_wait_s: float = 0.5,
    ) -> None:
        self._factory = timer_factory
        self._exit = exit
        self._stderr = stderr
        self._log_wait_s = log_wait_s
        self._timer: _Timer | None = None

    @property
    def armed(self) -> bool:
        return self._timer is not None

    def arm(self, seconds: float) -> None:
        """Guarantee exit within `seconds`: fire `log_wait_s` early, so the bounded
        diagnostic wait in `_fire` still ends by the deadline."""
        if self._timer is not None:
            return
        timer = self._factory(max(0.0, seconds - self._log_wait_s), self._fire)
        timer.daemon = True
        timer.start()
        self._timer = timer

    def cancel(self) -> None:
        if self._timer is not None:
            self._timer.cancel()

    def _fire(self) -> None:
        """Exit 70, whatever happens to the diagnostic line. The line is best-effort: it is
        written by a throwaway daemon thread that gets at most `log_wait_s`, so a closed,
        broken or blocking stderr can never keep the process alive."""
        try:
            writer = threading.Thread(target=self._write_line, name="watchdog-log", daemon=True)
            writer.start()
            writer.join(self._log_wait_s)
        finally:
            self._exit(EXIT_WATCHDOG)

    def _write_line(self) -> None:
        # Not through structlog: the event loop (and anything it owns) may be stuck.
        stream = self._stderr if self._stderr is not None else sys.stderr
        with contextlib.suppress(Exception):
            stream.write(json.dumps({"event": "core.watchdog_exit", "level": "critical"}) + "\n")
            stream.flush()


class ShutdownControl:
    """The synchronous, idempotent shutdown trigger shared by every exit cause."""

    def __init__(
        self,
        timeout_s: float,
        *,
        arm_watchdog: Callable[[float], None],
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.timeout_s = timeout_s
        self._arm_watchdog = arm_watchdog
        self._monotonic = monotonic
        self.cause: str | None = None
        self.deadline: float | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._wakeup: asyncio.Event | None = None

    @property
    def requested(self) -> bool:
        return self.cause is not None

    def begin(self, cause: str) -> None:
        """Deadline, then watchdog, then wake the loop. Safe in a signal handler."""
        if self.cause is not None:
            return
        self.cause = cause
        self.deadline = self._monotonic() + self.timeout_s
        self._arm_watchdog(self.timeout_s + WATCHDOG_GRACE_S)
        if self._loop is not None and self._wakeup is not None:
            self._loop.call_soon_threadsafe(self._wakeup.set)

    def attach(self) -> asyncio.Event:
        """Bind to the running loop; the returned event is set once shutdown begins."""
        self._loop = asyncio.get_running_loop()
        self._wakeup = asyncio.Event()
        if self.cause is not None:
            self._wakeup.set()
        return self._wakeup

    def remaining(self) -> float:
        if self.deadline is None:
            return self.timeout_s
        return max(0.0, self.deadline - self._monotonic())


def make_signal_handler(
    control: ShutdownControl, *, exit: Callable[[int], object] = os._exit
) -> SignalHandler:
    """First signal: `control.begin("signal")`. Second: exit 130 immediately."""
    received = 0

    def handler(signum: int, frame: FrameType | None) -> None:
        nonlocal received
        received += 1
        if received == 1:
            control.begin("signal")
        else:
            exit(EXIT_HARD)

    return handler


def install_signal_handlers(handler: SignalHandler) -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, handler)
```

- [ ] **Step 4: Run all tests** with `uv run pytest -q` (redirect the output, check the exit code). Expected: PASS. If mypy rejects `threading.Timer` as a `TimerFactory`, change the default to `lambda seconds, fn: threading.Timer(seconds, fn)` and record that in the report.

- [ ] **Step 5: Lint, type-check, commit**

```bash
uv run ruff format && uv run ruff check && uv run mypy && git add src/autotrader/core/shutdown.py tests/unit/core/test_shutdown.py && git commit -m "WP 1.5a: synchronous shutdown trigger, watchdog and signal handler

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Supervised loops (`core/supervise.py`)

**Files:**
- Create: `src/autotrader/core/supervise.py`
- Test: `tests/unit/core/test_supervise.py` (new)

**Interfaces:**
- Produces:
  - `Loop(name: str, step: Callable[[], Awaitable[None]], interval_s: float, stall_after_s: float, stall_level: Literal["warning", "error"] = "warning")`
  - `Freshness(name: str, last: Callable[[], float | None], max_age_s: float)`. It logs `<name>_overdue` / `<name>_fresh`.
  - `Supervisor(loops, freshness=(), *, clock=time.monotonic)`, with:
    - `start() -> None`, called inside the running loop;
    - `check() -> None`;
    - `async stop(deadline: float) -> list[str]`, where `deadline` is on `clock`;
    - `heartbeat(name: str) -> float`.

- [ ] **Step 1: Write the failing tests** (`tests/unit/core/test_supervise.py`)

```python
import asyncio
import io
import json
from typing import Any

from autotrader.core.logging import configure_logging
from autotrader.core.supervise import Freshness, Loop, Supervisor


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def lines(buf: io.StringIO) -> list[dict[str, Any]]:
    return [json.loads(x) for x in buf.getvalue().splitlines() if x.startswith("{")]


def events(buf: io.StringIO) -> list[str]:
    return [ln["event"] for ln in lines(buf)]


async def until(pred: Any, timeout: float = 5.0) -> None:
    async with asyncio.timeout(timeout):
        while not pred():
            await asyncio.sleep(0.001)


async def test_stall_and_recovery_are_logged_once_per_transition() -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    clock, release = Clock(), asyncio.Event()

    async def step() -> None:
        await release.wait()

    sup = Supervisor([Loop("feed.tick", step, 0.001, 30, stall_level="warning")], clock=clock)
    sup.start()
    clock.now += 31
    sup.check()
    sup.check()
    assert events(buf).count("feed.tick.stalled") == 1
    stalled = next(ln for ln in lines(buf) if ln["event"] == "feed.tick.stalled")
    assert (stalled["level"], stalled["age_s"]) == ("warning", 31.0)

    release.set()
    await until(lambda: sup.heartbeat("feed.tick") == clock.now)
    sup.check()
    assert events(buf).count("feed.tick.recovered") == 1
    await sup.stop(clock.now + 1)


async def test_error_level_stall_for_control_poll() -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    clock = Clock()

    async def step() -> None:
        await asyncio.Event().wait()

    sup = Supervisor([Loop("control.poll", step, 1, 10, stall_level="error")], clock=clock)
    sup.start()
    clock.now += 11
    sup.check()
    stalled = next(ln for ln in lines(buf) if ln["event"] == "control.poll.stalled")
    assert stalled["level"] == "error"
    await sup.stop(clock.now)  # deadline already reached: the hung step is cancelled


async def test_one_hung_loop_never_delays_another() -> None:
    clock, ticks = Clock(), []

    async def hung() -> None:
        await asyncio.Event().wait()

    async def ticking() -> None:
        ticks.append(1)

    sup = Supervisor(
        [Loop("feed.tick", hung, 0.001, 30), Loop("control.poll", ticking, 0.001, 10)],
        clock=clock,
    )
    sup.start()
    await until(lambda: len(ticks) >= 3)
    assert await sup.stop(clock.now) == ["feed.tick"]


async def test_raising_step_keeps_its_loop_running() -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    calls = []

    async def step() -> None:
        calls.append(1)
        raise ConnectionError("down")

    sup = Supervisor([Loop("feed.tick", step, 0.001, 30)])
    sup.start()
    await until(lambda: len(calls) >= 3)
    assert events(buf).count("feed.tick.step_failed") >= 3
    sup.check()
    assert "feed.tick.task_died" not in events(buf)
    await sup.stop(asyncio.get_running_loop().time() + 1)


class Killer(BaseException):
    pass


async def test_dead_task_is_logged_and_restarted() -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    calls = []

    async def step() -> None:
        calls.append(1)
        if len(calls) == 1:
            raise Killer

    sup = Supervisor([Loop("risk.snapshot", step, 0.001, 120)])
    sup.start()
    await until(lambda: len(calls) == 1)
    await asyncio.sleep(0)
    sup.check()
    await until(lambda: len(calls) >= 2)
    died = [ln for ln in lines(buf) if ln["event"] == "risk.snapshot.task_died"]
    assert len(died) == 1 and "Killer" in died[0]["error"]
    await sup.stop(asyncio.get_running_loop().time() + 1)


async def test_check_never_restarts_loops_after_stop() -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)

    async def step() -> None:
        return None

    sup = Supervisor([Loop("feed.tick", step, 0.001, 30)])
    sup.start()
    assert await sup.stop(asyncio.get_running_loop().time() + 1) == []
    sup.check()
    assert "feed.tick.task_died" not in events(buf)


async def test_stop_cancels_a_cancellation_resistant_step_once_and_leaves_it() -> None:
    clock, release = Clock(), asyncio.Event()
    cancels = []

    async def stubborn() -> None:
        while not release.is_set():
            try:
                await release.wait()
            except asyncio.CancelledError:
                cancels.append(1)

    sup = Supervisor([Loop("feed.tick", stubborn, 0.001, 30)], clock=clock)
    sup.start()
    await asyncio.sleep(0)
    assert await sup.stop(clock.now + 0.01) == ["feed.tick"]
    await asyncio.sleep(0)
    assert cancels == [1]  # cancelled once, then left behind (not awaited again)
    release.set()
    await asyncio.sleep(0.01)


async def test_cadence_subtracts_step_duration_without_catch_up() -> None:
    clock = Clock()
    durations = iter([3.0, 12.0])
    waits: list[float] = []

    class Recording(Supervisor):
        async def _pause(self, stopping: asyncio.Event, seconds: float) -> None:
            waits.append(seconds)
            if len(waits) == 2:
                stopping.set()
            await asyncio.sleep(0)

    async def step() -> None:
        clock.now += next(durations)

    sup = Recording([Loop("risk.snapshot", step, 10, 120)], clock=clock)
    sup.start()
    await until(lambda: len(waits) == 2)

    assert waits == [7.0, 0.0]  # 10 - 3; then a 12 s overrun: next step at once
    await sup.stop(clock.now + 1)


async def test_freshness_is_tracked_separately_from_liveness() -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    clock = Clock()
    last: list[float | None] = [None]

    async def step() -> None:
        return None  # the loop is alive and keeps completing

    sup = Supervisor(
        [Loop("risk.snapshot", step, 0.001, 120)],
        [Freshness("risk.snapshot", lambda: last[0], 120)],
        clock=clock,
    )
    sup.start()  # no write yet: age measured from start
    clock.now += 121
    await until(lambda: sup.heartbeat("risk.snapshot") == clock.now)  # the loop is alive
    sup.check()
    assert events(buf).count("risk.snapshot_overdue") == 1
    assert "risk.snapshot.stalled" not in events(buf)  # the loop itself is fine

    last[0] = clock.now  # a committed write
    sup.check()
    assert events(buf).count("risk.snapshot_fresh") == 1
    await sup.stop(clock.now + 1)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/core/test_supervise.py -q`
Expected: collection error, `No module named 'autotrader.core.supervise'`.

- [ ] **Step 3: Implement** (`src/autotrader/core/supervise.py`)

```python
"""Supervised background loops (WP 1.5a, spec 2026-10-14 section 3).

Each `Loop` runs `step()` every `interval_s` (start to start: the pause is the rest of the
interval; an overrun starts the next step at once, no catch-up) in its own task, so a hung
step in one loop never delays another. The heartbeat is when `step()` last completed;
`check()` logs a stall once per transition and never cancels a slow step (cancelling could
cut a journal write mid-commit). A raising step is logged and the loop carries on; a task
killed by an escaped `BaseException` is logged and restarted. `Freshness` probes track data
age (e.g. the last committed equity snapshot) separately from loop liveness.
`stop(deadline)` lets steps finish until the deadline, then cancels the rest once and leaves
them behind.
"""

import asyncio
import contextlib
import time
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Literal

import structlog

log = structlog.get_logger(__name__)


@dataclass(frozen=True)
class Loop:
    name: str
    step: Callable[[], Awaitable[None]]
    interval_s: float
    stall_after_s: float
    stall_level: Literal["warning", "error"] = "warning"


@dataclass(frozen=True)
class Freshness:
    name: str  # logs "<name>_overdue" / "<name>_fresh"
    last: Callable[[], float | None]  # on the supervisor's clock; None = never
    max_age_s: float


class Supervisor:
    def __init__(
        self,
        loops: Sequence[Loop],
        freshness: Sequence[Freshness] = (),
        *,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._loops = {loop.name: loop for loop in loops}
        self._freshness = list(freshness)
        self._clock = clock
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._beats: dict[str, float] = {}
        self._stalled: set[str] = set()
        self._overdue: set[str] = set()
        self._started_at = 0.0
        self._stopping: asyncio.Event | None = None

    def start(self) -> None:
        self._started_at = self._clock()
        self._stopping = asyncio.Event()
        for loop in self._loops.values():
            self._beats[loop.name] = self._started_at
            self._spawn(loop)

    def heartbeat(self, name: str) -> float:
        return self._beats[name]

    def check(self) -> None:
        """Restart dead loop tasks; log stall and freshness transitions."""
        if self._stopping is None or self._stopping.is_set():
            return
        now = self._clock()
        for name, task in list(self._tasks.items()):
            if task.done():
                error = None if task.cancelled() else task.exception()
                log.error(f"{name}.task_died", error=repr(error))
                self._spawn(self._loops[name])
        for name, loop in self._loops.items():
            age = now - self._beats[name]
            if age > loop.stall_after_s:
                if name not in self._stalled:
                    self._stalled.add(name)
                    log_at = log.error if loop.stall_level == "error" else log.warning
                    log_at(f"{name}.stalled", age_s=round(age, 3))
            elif name in self._stalled:
                self._stalled.discard(name)
                log.info(f"{name}.recovered")
        for probe in self._freshness:
            last = probe.last()
            age = now - (last if last is not None else self._started_at)
            if age > probe.max_age_s:
                if probe.name not in self._overdue:
                    self._overdue.add(probe.name)
                    log.warning(f"{probe.name}_overdue", age_s=round(age, 3))
            elif probe.name in self._overdue:
                self._overdue.discard(probe.name)
                log.info(f"{probe.name}_fresh")

    async def stop(self, deadline: float) -> list[str]:
        """Let running steps finish until `deadline` (on `clock`), then cancel the rest
        once and leave them behind. Returns the names that had to be cancelled."""
        if self._stopping is not None:
            self._stopping.set()
        running = {task: name for name, task in self._tasks.items() if not task.done()}
        if not running:
            return []
        _, pending = await asyncio.wait(running, timeout=max(0.0, deadline - self._clock()))
        for task in pending:
            task.cancel()
        return sorted(running[task] for task in pending)

    def _spawn(self, loop: Loop) -> None:
        self._tasks[loop.name] = asyncio.create_task(self._run(loop), name=loop.name)

    async def _run(self, loop: Loop) -> None:
        stopping = self._stopping
        if stopping is None:
            raise RuntimeError("Supervisor.start() was not called")
        while not stopping.is_set():
            started = self._clock()
            try:
                await loop.step()
            except Exception:
                log.exception(f"{loop.name}.step_failed")
            self._beats[loop.name] = self._clock()
            # Start-to-start cadence: sleep only for the rest of the interval. An overrun
            # starts the next step at once, with no catch-up burst.
            await self._pause(stopping, max(0.0, loop.interval_s - (self._clock() - started)))

    async def _pause(self, stopping: asyncio.Event, seconds: float) -> None:
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(stopping.wait(), timeout=seconds)
```

- [ ] **Step 4: Run all tests** with `uv run pytest -q` (redirect the output, check the exit code). Expected: PASS.

- [ ] **Step 5: Lint, type-check, commit**

```bash
uv run ruff format && uv run ruff check && uv run mypy && git add src/autotrader/core/supervise.py tests/unit/core/test_supervise.py && git commit -m "WP 1.5a: supervised loops with stall and freshness logging

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: `RiskEngine.last_snapshot_at`

**Files:**
- Modify: `src/autotrader/risk/engine.py`: `__init__` (around line 90-118) and `_write_snapshot` (around line 428-452)
- Test: `tests/unit/risk/test_engine.py`

**Interfaces:**
- Produces: `RiskEngine.last_snapshot_at: float | None`, on the engine's `monotonic` clock. It is set right after every committed `_write_snapshot`, including the partial "positions failed" path and the peak written by `check()`. It is `None` before the first write.

- [ ] **Step 1: Write the failing tests** (append to `tests/unit/risk/test_engine.py`)

```python
class AccountDown(SimBroker):
    async def get_account(self) -> Account:
        raise ConnectionError("account down")


class PositionsDown(SimBroker):
    async def get_positions(self) -> list[Position]:
        raise ConnectionError("positions down")


def _broker(cls: type[SimBroker]) -> SimBroker:
    b = cls(cash=D("1000"), fees=FEES, slippage_bps=D(0), asset_rules={BTC: RULES}, timeframe="5m")
    b.process_bar(five(29))
    return b


async def test_last_snapshot_at_tracks_committed_writes(
    sessions: async_sessionmaker[AsyncSession], gate: ControlGate
) -> None:
    mono = Mono()
    mono.t = 2.0
    ok = make_engine_(_broker(SimBroker), sessions, gate, monotonic=mono)
    assert ok.last_snapshot_at is None
    await ok.snapshot_equity()
    assert ok.last_snapshot_at == 2.0

    no_account = make_engine_(_broker(AccountDown), sessions, gate, monotonic=mono)
    with pytest.raises(ConnectionError):
        await no_account.snapshot_equity()
    assert no_account.last_snapshot_at is None  # nothing was written

    mono.t = 5.0  # advance, so the assertion shows the value was set by this write
    later = Clock(NOW + timedelta(minutes=1))  # equity_snapshots.ts is unique
    partial = make_engine_(_broker(PositionsDown), sessions, gate, clock=later, monotonic=mono)
    with pytest.raises(ConnectionError):
        await partial.snapshot_equity()
    assert partial.last_snapshot_at == 5.0  # equity/HWM still committed
```

Check that `Mono`, `Clock`, `NOW`, `timedelta`, `Account`, `Position`, `SimBroker`, `five`, `FEES`, `RULES` and `D` already exist or are imported in `test_engine.py` (`rg -n "class Mono|^from|^import" tests/unit/risk/test_engine.py`). Add any missing import from the module it's used from elsewhere in the file. If `Mono()` needs arguments or isn't callable, look at its definition and use it the way the existing tests do.

- [ ] **Step 2: Run it to verify it fails**

Run: `uv run pytest tests/unit/risk/test_engine.py -k last_snapshot_at -q`
Expected: FAIL with `AttributeError: 'RiskEngine' object has no attribute 'last_snapshot_at'`.

- [ ] **Step 3: Implement**

In `RiskEngine.__init__`, after `self._monotonic = monotonic` add:

```python
        # Monotonic time of the last committed equity snapshot (None = none yet): drives
        # run core's `risk.snapshot_overdue`, separately from the snapshot loop's liveness.
        self.last_snapshot_at: float | None = None
```

At the end of `_write_snapshot`, after the `await repo.insert_equity_snapshot(...)` call, inside the `async with` block (the insert commits), add:

```python
        self.last_snapshot_at = self._monotonic()
```

- [ ] **Step 4: Run all tests and risk coverage**

Run `uv run pytest -q` and `uv run pytest -q --cov=autotrader.risk --cov-branch --cov-report=term-missing`, redirecting the output and checking the exit codes. Expected: PASS, with `risk/` at 100 %.

- [ ] **Step 5: Lint, type-check, commit**

```bash
uv run ruff format && uv run ruff check && uv run mypy && git add src/autotrader/risk/engine.py tests/unit/risk/test_engine.py && git commit -m "WP 1.5a: RiskEngine.last_snapshot_at after committed writes

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: `run_core` on lock, control and supervisor with bounded teardown

**Files:**
- Modify: `src/autotrader/core/runtime.py` (whole file)
- Test: `tests/unit/core/test_runtime.py` (update the existing tests, add new ones)

**Interfaces:**
- Consumes:
  - `JournalLock`, `canonical_journal` (Task 2);
  - `ShutdownControl`, `EXIT_CLEAN`, `EXIT_FAILED` (Task 3);
  - `Loop`, `Freshness`, `Supervisor` (Task 4);
  - `RiskEngine.last_snapshot_at` (Task 5).
- Produces: `async run_core(cfg, settings, profile, profile_yaml, *, source, connect_broker, lock: JournalLock, control: ShutdownControl, tick_s: float = TICK_S, snapshot_s: float = SNAPSHOT_S, supervise_s: float = SUPERVISE_S) -> int`.
  - It returns `EXIT_CLEAN`, or `EXIT_FAILED` if any teardown step was forced (timed out) or failed (raised, or was cancelled). The remaining steps always run.
  - It raises the original error after teardown on startup failure or stream death.
  - The `stop` parameter is removed: programmatic stop is `control.begin("stop")`.
  - Module constants: `TICK_S = 1.0`, `SNAPSHOT_S = 60.0`, `SUPERVISE_S = 1.0`, `FEED_STALL_S = 30.0`, `CONTROL_STALL_S = 10.0`.

- [ ] **Step 1: Rewrite the runtime tests** (`tests/unit/core/test_runtime.py`)

Change the imports at the top:

```python
from sqlalchemy.ext.asyncio import AsyncEngine

from autotrader.core.bus import EventBus
from autotrader.core.lock import JournalLock, acquire
from autotrader.core.runtime import _snapshot_step, run_core
from autotrader.core.shutdown import EXIT_CLEAN, EXIT_FAILED, ShutdownControl
```

Remove `_snapshot_loop` from the old import. Add `Iterator` to the `collections.abc` import.

After the `cfg` fixture, add these fixtures and helpers:

```python
@pytest.fixture
def lock(cfg: AppConfig) -> Iterator[JournalLock]:
    held = acquire(cfg.database.url, wait_s=0)
    yield held
    held.release()  # tests only: production never releases early


class FakeWatchdog:
    def __init__(self) -> None:
        self.armed: list[float] = []

    def arm(self, seconds: float) -> None:
        self.armed.append(seconds)


def make_control(timeout_s: float = 5.0) -> tuple[ShutdownControl, FakeWatchdog]:
    dog = FakeWatchdog()
    return ShutdownControl(timeout_s, arm_watchdog=dog.arm), dog


def start_core(
    cfg: AppConfig, lock: JournalLock, control: ShutdownControl, **kw: Any
) -> "asyncio.Task[int]":
    kw.setdefault("source", StreamingSource())
    kw.setdefault("connect_broker", fake_connect)
    kw.setdefault("tick_s", 0.01)
    kw.setdefault("supervise_s", 0.01)
    return asyncio.create_task(
        run_core(
            cfg,
            Settings(_env_file=None),
            load_profile(cfg.strategy),
            "yaml",
            lock=lock,
            control=control,
            **kw,
        )
    )


class Stubborn:
    """An awaitable that ignores cancellation until released."""

    def __init__(self) -> None:
        self.release = asyncio.Event()
        self.cancels = 0

    async def wait(self) -> None:
        while not self.release.is_set():
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancels += 1
```

Then update the existing tests as follows:

- **`test_run_core_persists_bars_and_responds_to_kill_file`:**
  - Add the `lock: JournalLock` parameter.
  - Create `control, _ = make_control()`.
  - Replace the `asyncio.create_task(run_core(...))` call with `task = start_core(cfg, lock, control, source=source, snapshot_s=0.01)`.
  - In `finally`, replace `stop.set()` with `control.begin("stop")`, and assert `await asyncio.wait_for(task, timeout=5) == EXIT_CLEAN`.
  - Remove the now-unused `stop` variable. Keep `profile = load_profile(cfg.strategy)`: the `scanner` asserts use it.
- **`test_dead_stream_stops_the_core_with_an_error`:**

```python
async def test_dead_stream_stops_the_core_with_an_error(cfg: AppConfig, lock: JournalLock) -> None:
    control, dog = make_control()

    with pytest.raises(ConnectionError, match="stream died"):
        await asyncio.wait_for(
            start_core(cfg, lock, control, source=StreamingSource(fail_stream=True)), timeout=5
        )
    assert control.cause == "stream_death"
    assert dog.armed  # the coordinator armed the watchdog for this exit cause too
```

- **`test_broker_connect_failure_aborts_startup`:** add the `lock` parameter, then:

```python
    control, dog = make_control()
    with pytest.raises(BrokerError, match="auth failed"):
        await start_core(cfg, lock, control, connect_broker=failing)
    assert control.cause == "startup_failure" and dog.armed
```

- **`test_snapshot_loop_logs_failures_and_keeps_going`:** replace it with a test of the step function:

```python
async def test_snapshot_step_logs_failures_and_successes() -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    snapshots = FlakySnapshots()
    for _ in range(3):
        await _snapshot_step(snapshots)
    events = [ln for ln in _log_lines(buf) if ln["event"].startswith("risk.snapshot")]
    assert [(ln["event"], ln.get("timeout")) for ln in events] == [
        ("risk.snapshot_failed", True),
        ("risk.snapshot_failed", None),
        ("risk.snapshot_written", None),
    ]
```

- **`test_hung_snapshot_at_the_default_interval_never_delays_kill_detection`:** add the `lock` parameter and `control, _ = make_control(timeout_s=0.5)` (a short budget: the hung snapshot is cut off at the deadline instead of making the test wait 5 s). Use `task = start_core(cfg, lock, control, connect_broker=connect)`, which keeps the default `snapshot_s`. In `finally` use `control.begin("stop")` and `await asyncio.wait_for(task, timeout=15)`. The hung snapshot is cancelled at the deadline, so also accept `EXIT_FAILED`: `assert await asyncio.wait_for(task, timeout=15) in (EXIT_CLEAN, EXIT_FAILED)`.
- **`test_dead_snapshot_task_is_logged_and_restarted`:** add the `lock` parameter and `control, _ = make_control()`. Use `task = start_core(cfg, lock, control, connect_broker=connect)`, and `control.begin("stop")` in `finally`. Change the asserted event to `risk.snapshot.task_died`.

Append the new tests:

```python
async def test_shutdown_requested_before_start_skips_startup(
    cfg: AppConfig, lock: JournalLock
) -> None:
    connected = []

    async def connect(s: Settings) -> Any:
        connected.append(1)
        return FakeBroker()

    control, _ = make_control()
    control.begin("signal")  # e.g. a signal during the CLI's migration
    source = StreamingSource()

    assert await start_core(cfg, lock, control, source=source, connect_broker=connect) == 0
    assert connected == [] and source.stopped.is_set()


async def test_run_core_rejects_a_lock_for_another_journal(
    cfg: AppConfig, tmp_path: Path
) -> None:
    other = acquire(f"sqlite+aiosqlite:///{tmp_path / 'other.db'}", wait_s=0)
    try:
        control, _ = make_control()
        with pytest.raises(ValueError, match="different journal"):
            await start_core(cfg, other, control)
    finally:
        other.release()


async def test_cancellation_resistant_cleanup_is_left_behind_at_the_deadline(
    cfg: AppConfig, lock: JournalLock
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    stubborn = Stubborn()
    source = StreamingSource()
    source.stop = stubborn.wait  # type: ignore[method-assign]
    control, _ = make_control(timeout_s=0.2)

    task = start_core(cfg, lock, control, source=source)
    await _wait_for(lambda: any(ln["event"] == "core.started" for ln in _log_lines(buf)))
    control.begin("stop")
    try:
        assert await asyncio.wait_for(task, timeout=5) == EXIT_FAILED
    finally:
        stubborn.release.set()
        await asyncio.sleep(0.01)
    forced = next(ln for ln in _log_lines(buf) if ln["event"] == "core.shutdown_forced")
    assert "source.stop" in forced["steps"]
    assert "engine.dispose" not in forced["steps"]  # the dispose floor still closes the journal
    assert stubborn.cancels == 1
    assert lock.held  # run_core never releases the journal lock


@pytest.mark.parametrize("target", ["bus.close", "engine.dispose", "source.stop"])
async def test_unsuccessful_cleanup_step_exits_1_and_cleanup_continues(
    cfg: AppConfig, lock: JournalLock, monkeypatch: pytest.MonkeyPatch, target: str
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)
    source = StreamingSource()
    if target == "bus.close":
        real_close = EventBus.close

        async def close(self: EventBus, budget_s: float = 20.0) -> None:
            await real_close(self, budget_s=budget_s)  # drain for real, then report failure
            raise RuntimeError("bus close failed")

        monkeypatch.setattr("autotrader.core.runtime.EventBus.close", close)
    elif target == "engine.dispose":
        real_dispose = AsyncEngine.dispose

        async def dispose(self: AsyncEngine, close: bool = True) -> None:
            await real_dispose(self, close)  # close the connections for real, then fail
            raise RuntimeError("dispose failed")

        monkeypatch.setattr(AsyncEngine, "dispose", dispose)
    else:

        async def cancelled_stop() -> None:
            source.stopped.set()
            raise asyncio.CancelledError

        source.stop = cancelled_stop  # type: ignore[method-assign]
    control, _ = make_control()

    task = start_core(cfg, lock, control, source=source)
    await _wait_for(lambda: any(ln["event"] == "core.started" for ln in _log_lines(buf)))
    control.begin("stop")

    assert await asyncio.wait_for(task, timeout=5) == EXIT_FAILED
    failed = next(ln for ln in _log_lines(buf) if ln["event"] == "core.shutdown_failed")
    assert failed["steps"] == [target]  # nothing else failed: the later steps still ran
    events = [ln["event"] for ln in _log_lines(buf)]
    assert "core.stopped" not in events and "core.shutdown_forced" not in events
    assert source.stopped.is_set()


async def test_stream_death_with_stuck_cleanup_still_ends_within_the_deadline(
    cfg: AppConfig, lock: JournalLock
) -> None:
    stubborn = Stubborn()
    source = StreamingSource(fail_stream=True)
    source.stop = stubborn.wait  # type: ignore[method-assign]
    control, dog = make_control(timeout_s=0.2)

    try:
        with pytest.raises(ConnectionError):
            await asyncio.wait_for(start_core(cfg, lock, control, source=source), timeout=5)
    finally:
        stubborn.release.set()
        await asyncio.sleep(0.01)
    assert dog.armed and control.cause == "stream_death"


async def test_hung_feed_tick_never_delays_kill_detection(
    cfg: AppConfig, lock: JournalLock, monkeypatch: pytest.MonkeyPatch
) -> None:
    buf = io.StringIO()
    configure_logging(stream=buf)

    async def hung_tick(self: Any) -> None:
        await asyncio.Event().wait()

    monkeypatch.setattr("autotrader.core.runtime.MarketFeed.tick", hung_tick)
    monkeypatch.setattr("autotrader.core.runtime.FEED_STALL_S", 0.05)
    control, _ = make_control(timeout_s=0.5)
    task = start_core(cfg, lock, control)
    try:
        await _wait_for(lambda: any(ln["event"] == "core.started" for ln in _log_lines(buf)))
        cfg.control.kill_file.parent.mkdir(parents=True, exist_ok=True)
        cfg.control.kill_file.touch()
        await _wait_for(lambda: any(ln["event"] == "core.kill_detected" for ln in _log_lines(buf)))
        await _wait_for(lambda: any(ln["event"] == "feed.tick.stalled" for ln in _log_lines(buf)))
    finally:
        control.begin("stop")
        await asyncio.wait_for(task, timeout=5)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/core/test_runtime.py -q`
Expected: collection error (`cannot import name '_snapshot_step'`) or `TypeError: run_core() got an unexpected keyword argument 'lock'`.

- [ ] **Step 3: Implement**: replace `src/autotrader/core/runtime.py` with:

```python
"""`autotrader run core` (SPEC §12 Phase 0 DoD; WP 1.5a runtime safety).

Connects the broker, backfills and streams bars for the universe, persists 1m bars, runs the
Signal Scanner on closed bars (WP 1.2) and snapshots equity for the daily-loss/drawdown
checks. The broker is handed to the `RiskEngine`, its only holder from then on (SPEC §0.1).
Nothing calls `check()`/`exit()` until WP 1.5d wires the Cycle Runner and the Position
Manager.

WP 1.5a (spec 2026-10-14):
- The caller (the CLI) holds the single-writer journal lock (`core/lock.py`) before the
  migration and passes it in; `run_core` opens the journal through `lock.url` and never
  releases the lock (process death does).
- Feed ticks, kill/control polling and equity snapshots run as independent supervised loops
  (`core/supervise.py`): a hung step in one never delays the others. Stalls are logged;
  `risk.snapshot_overdue` follows the last *committed* snapshot.
- Every exit cause (signal, programmatic stop, startup failure, stream death) goes through
  `ShutdownControl` (`core/shutdown.py`), whose deadline and watchdog are set before any
  async work. Teardown is ordered and bounded: a step still running at the deadline is
  cancelled once and left behind (`core.shutdown_forced`), and abandoned work stays in the
  journal for startup recovery (WP 1.5d).
"""

import asyncio
import contextlib
from collections.abc import Awaitable, Callable
from functools import partial
from typing import Any, Protocol

import structlog

from autotrader.core.bus import EventBus
from autotrader.core.config import AppConfig, Settings, StrategyProfile
from autotrader.core.feed import BarClosed, BarSource, MarketFeed
from autotrader.core.kill import ControlGate, ControlStatus
from autotrader.core.lock import JournalLock, canonical_journal
from autotrader.core.scanner import Scanner
from autotrader.core.shutdown import EXIT_CLEAN, EXIT_FAILED, ShutdownControl
from autotrader.core.supervise import Freshness, Loop, Supervisor
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker
from autotrader.risk.engine import RiskEngine

log = structlog.get_logger(__name__)

BrokerConnect = Callable[[Settings], Awaitable[Any]]

TICK_S = 1.0  # feed tick and control poll interval
SNAPSHOT_S = 60.0
SUPERVISE_S = 1.0  # stall/freshness checks and stream supervision
FEED_STALL_S = 30.0
CONTROL_STALL_S = 10.0


class _Snapshotter(Protocol):
    async def snapshot_equity(self) -> None: ...


class ControlPoller:
    """Polls the kill/pause gate and logs changes; keeps the latest status for WP 1.5c."""

    def __init__(self, gate: ControlGate) -> None:
        self._gate = gate
        self.latest: ControlStatus | None = None

    async def step(self) -> None:
        status = await self._gate.status()
        previous, self.latest = self.latest, status
        if previous is not None and status.killed == previous.killed:
            return
        if status.killed:
            log.warning("core.kill_detected", reasons=list(status.reasons))
        elif previous is not None:
            log.warning("core.kill_cleared")


async def _snapshot_step(risk: _Snapshotter) -> None:
    """One equity snapshot; failures are logged, never raised (the engine bounds its own
    broker calls, `io_timeout_s`, and its journal writes are never cancelled)."""
    try:
        await risk.snapshot_equity()
    except TimeoutError:
        log.error("risk.snapshot_failed", timeout=True)
    except Exception:
        log.exception("risk.snapshot_failed")
    else:
        log.info("risk.snapshot_written")


async def run_core(
    cfg: AppConfig,
    settings: Settings,
    profile: StrategyProfile,
    profile_yaml: str,
    *,
    source: BarSource,
    connect_broker: BrokerConnect,
    lock: JournalLock,
    control: ShutdownControl,
    tick_s: float = TICK_S,
    snapshot_s: float = SNAPSHOT_S,
    supervise_s: float = SUPERVISE_S,
) -> int:
    """Run until shutdown is requested. Returns EXIT_CLEAN, or EXIT_FAILED when teardown
    was forced at the deadline. Startup failures and stream death are re-raised after
    teardown."""
    if not lock.held:
        raise ValueError("run_core needs the held single-writer journal lock")
    if lock.url != canonical_journal(cfg.database.url)[0]:
        raise ValueError("the journal lock belongs to a different journal")
    wakeup = control.attach()
    engine = make_engine(lock.url)
    sessions = make_sessionmaker(engine)
    bus = EventBus(sessions)
    supervisor: Supervisor | None = None
    stream_task: asyncio.Task[None] | None = None
    failure: BaseException | None = None
    try:
        if not control.requested:
            async with sessions() as session:
                strategy = await repo.register_strategy(session, profile, profile_yaml)
            broker = await _check_broker(connect_broker, settings)
            timeframes = [
                profile.timeframes.signal,
                *profile.timeframes.context,
                profile.timeframes.trend,
            ]
            feed = MarketFeed(
                source,
                sessions,
                bus,
                symbols=list(profile.universe),
                timeframes=timeframes,
                stale_alert_s=cfg.feed.stale_alert_s,
            )
            await feed.backfill()  # publishes no events, so the scanner can subscribe after
            gate = ControlGate(cfg.control.kill_file, sessions)
            scanner = Scanner(
                profile, feed, gate, sessions, bus, trading_day_tz=cfg.trading_day_tz
            )
            bus.subscribe(BarClosed, scanner.on_bar_closed)
            log.info(
                "scanner.subscribed",
                timeframe=profile.timeframes.signal,
                rules=[r.id for r in profile.signal_rules],
            )
            risk = RiskEngine(
                broker,
                profile=profile,
                strategy_id=strategy.id,
                mode=cfg.mode,
                gate=gate,
                market=feed,
                sessions=sessions,
                trading_day_tz=cfg.trading_day_tz,
            )
            log.info("risk.engine_ready", mode=cfg.mode)
            poller = ControlPoller(gate)
            supervisor = Supervisor(
                [
                    Loop("feed.tick", feed.tick, tick_s, FEED_STALL_S),
                    Loop("control.poll", poller.step, tick_s, CONTROL_STALL_S, "error"),
                    Loop("risk.snapshot", partial(_snapshot_step, risk), snapshot_s, 2 * snapshot_s),
                ],
                [Freshness("risk.snapshot", lambda: risk.last_snapshot_at, 2 * snapshot_s)],
            )
            stream_task = asyncio.create_task(
                source.stream(feed.on_bar, feed.on_quote), name="feed"
            )
            supervisor.start()
            log.info(
                "core.started",
                mode=cfg.mode,
                profile=profile.name,
                profile_hash=profile.hash,
                strategy_id=strategy.id,
                symbols=list(profile.universe),
                timeframes=timeframes,
            )
            while not control.requested:
                if stream_task.done():
                    control.begin("stream_death")
                    error = None if stream_task.cancelled() else stream_task.exception()
                    failure = error or RuntimeError("market data stream ended unexpectedly")
                    break
                supervisor.check()
                with contextlib.suppress(TimeoutError):
                    await asyncio.wait_for(wakeup.wait(), timeout=supervise_s)
    except Exception as e:
        control.begin("startup_failure")  # a no-op if another cause came first
        failure = e
    forced = await _teardown(control, source, supervisor, stream_task, bus, engine)
    if failure is not None:
        raise failure
    return EXIT_FAILED if forced else EXIT_CLEAN


async def _teardown(
    control: ShutdownControl,
    source: BarSource,
    supervisor: Supervisor | None,
    stream_task: "asyncio.Task[None] | None",
    bus: EventBus,
    engine: Any,
) -> list[str]:
    """Ordered, bounded teardown (spec section 4). Each step gets what is left of the
    deadline; a step still running then is cancelled once and left behind (forced). A step
    that raises or is cancelled is failed. Either way the remaining steps still run, and
    the returned list (forced + failed) makes the exit unclean."""
    deadline = control.deadline
    if deadline is None:
        raise RuntimeError("teardown before ShutdownControl.begin()")
    log.info("core.stopping", cause=control.cause)
    forced: list[str] = []  # timed out: cancelled once and left behind
    failed: list[str] = []  # completed, but raised or was cancelled

    async def bounded(
        name: str, work: Awaitable[object], floor_s: float = 0.0, cancel_ok: bool = False
    ) -> None:
        task = asyncio.ensure_future(work)
        done, _ = await asyncio.wait({task}, timeout=max(control.remaining(), floor_s))
        if not done:
            task.cancel()
            forced.append(name)
        elif task.cancelled():
            if not cancel_ok:
                failed.append(name)
                log.error("core.shutdown_step_failed", step=name, error="cancelled")
        elif (error := task.exception()) is not None:
            failed.append(name)
            log.error("core.shutdown_step_failed", step=name, error=repr(error))

    # 1. Admission: nothing to close in 1.5a (WP 1.5d: close admission, runner.close()).
    await bounded("source.stop", source.stop())
    if supervisor is not None:
        forced += [f"loop:{name}" for name in await supervisor.stop(deadline)]
    if stream_task is not None and not stream_task.done():
        stream_task.cancel()
        await bounded("stream", stream_task, cancel_ok=True)  # we cancelled it ourselves
    await bounded("bus.close", bus.close(budget_s=control.remaining()))
    # A floor even after the deadline (inside the watchdog's 5 s grace): a forced teardown
    # must still close the journal's connections (dry run, spec revision 2026-10-14).
    await bounded("engine.dispose", engine.dispose(), floor_s=1.0)
    if forced:
        log.error("core.shutdown_forced", steps=forced, cause=control.cause)
        log.warning(
            "core.shutdown_abandoned",
            steps=forced,
            note="left for startup recovery (WP 1.5d); not proof that broker work stopped",
        )
    if failed:
        log.error("core.shutdown_failed", steps=failed, cause=control.cause)
    if not forced and not failed:
        log.info("core.stopped", cause=control.cause)
    return forced + failed  # any entry makes the exit unclean


async def _check_broker(connect_broker: BrokerConnect, settings: Settings) -> Any:
    broker = await connect_broker(settings)
    account = await broker.get_account()
    fees = broker.fee_schedule()
    log.info(
        "core.broker_connected",
        mode=broker.mode(),
        equity=str(account.equity),
        cash=str(account.cash),
        maker_bps=str(fees.maker_bps),
        taker_bps=str(fees.taker_bps),
    )
    return broker
```

Note: the `stream_task` annotation in `_teardown` is quoted. If `ruff` prefers it unquoted, unquote it.

- [ ] **Step 4: Run all tests** with `uv run pytest -q` (redirect the output, check the exit code). Expected: PASS. `tests/integration/test_scanner_feed.py` and other callers of `run_core` must also pass. Check with `rg -n "run_core\(" tests src`, and update any other caller the same way: a `lock` fixture, `make_control()`, `control.begin("stop")`.

- [ ] **Step 5: Don't commit yet**

`cli.py` still calls `run_core(..., stop=...)`, so mypy is red until Task 7 rewires the CLI. **Tasks 6 and 7 are one review unit with one commit**, made at the end of Task 7. Run `uv run pytest -q tests/unit/core/test_runtime.py` here to confirm the runtime tests pass, then continue with Task 7.

---

### Task 7: CLI wiring: lock before migration, handlers, watchdog, exit codes (commits together with Task 6)

**Files:**
- Modify: `src/autotrader/cli.py`: the `run_core` command (around line 60-75) and `_run_core_until_signal` (around line 77-97)
- Test: `tests/unit/test_cli.py`

**Interfaces:**
- Consumes: `journal_lock.acquire`, `JournalLockError` (Task 2); `ShutdownControl`, `Watchdog`, `make_signal_handler`, `install_signal_handlers`, `WATCHDOG_GRACE_S`, `EXIT_FAILED` (Task 3); `run_core(..., lock=, control=) -> int` (Task 6).
- Produces: `_run_core_process(cfg, settings, profile, *, watchdog: Watchdog | None = None, install=install_signal_handlers, upgrade=db.upgrade, make_source=AlpacaBarSource.from_settings, run=asyncio.run, acquire=journal_lock.acquire) -> int`. `_run_core_until_signal` is removed. Ctrl-C while waiting for the lock exits 130 without a traceback.

- [ ] **Step 1: Write the failing tests** (append to `tests/unit/test_cli.py`)

```python
import io
import subprocess
import sys
from collections.abc import Callable, Coroutine
from typing import Any

from autotrader.cli import _run_core_process
from autotrader.core import lock as journal_lock
from autotrader.core.config import Settings, load_config, load_profile
from autotrader.core.logging import configure_logging
from autotrader.core.shutdown import Watchdog

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
        self.started = self.cancelled = False

    def start(self) -> None:
        self.started = True

    def cancel(self) -> None:
        self.cancelled = True


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
    assert timers[0].cancelled  # only after asyncio.run returned


def test_failure_inside_run_exits_1_and_cancels_the_watchdog_after_run(tmp_path: Path) -> None:
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
    assert order == ["run"] and timers[0].cancelled


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
```

Check the imports at the top of `test_cli.py`. `Path`, `pytest` and `REPO` already exist; add the imports above that are missing, at the top of the file, not mid-file.

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/test_cli.py -k "losing_core or migration or failure_inside" -q`
Expected: collection error, `cannot import name '_run_core_process'`.

- [ ] **Step 3: Implement**

In `src/autotrader/cli.py`, add these imports:

```python
from collections.abc import Callable, Coroutine
from typing import Any

from autotrader.core import lock as journal_lock
from autotrader.core import shutdown
from autotrader.core.shutdown import (
    EXIT_FAILED,
    EXIT_HARD,
    ShutdownControl,
    SignalHandler,
    Watchdog,
    install_signal_handlers,
    make_signal_handler,
)
```

Merge `Any` and `Callable` into the existing `typing`/`collections.abc` imports. Remove `import signal` if nothing else in the file uses it (`rg -n "signal\." src/autotrader/cli.py`).

Replace the `run core` command and `_run_core_until_signal` with:

```python
@run_app.command("core")
def run_core(config: ConfigOption = Path("config.yaml")) -> None:
    """Start the trading core: market feed, scanner, risk engine (equity snapshots)."""
    cfg = _load_config(config)
    settings = Settings()
    try:
        profile = load_profile(cfg.strategy)
    except ConfigError as e:
        typer.echo(f"invalid config: {e}", err=True)
        raise typer.Exit(code=1) from None
    raise typer.Exit(code=_run_core_process(cfg, settings, profile))


def _run_core_process(
    cfg: AppConfig,
    settings: Settings,
    profile: StrategyProfile,
    *,
    watchdog: Watchdog | None = None,
    install: Callable[[SignalHandler], None] = install_signal_handlers,
    upgrade: Callable[[str], None] = db.upgrade,
    make_source: Callable[[Settings, list[str]], Any] = AlpacaBarSource.from_settings,
    run: Callable[[Coroutine[Any, Any, int]], int] = asyncio.run,
    acquire: Callable[..., journal_lock.JournalLock] = journal_lock.acquire,
) -> int:
    """WP 1.5a ownership and shutdown boundary (spec 2026-10-14 sections 2 and 4).

    The journal lock comes first, before the migration; the signal handlers right after it.
    The lock is never released here: process exit releases it. The watchdog is cancelled
    only after `run` (asyncio.run) has fully returned."""
    timeout_s = cfg.shutdown.timeout_s
    try:
        lock = acquire(cfg.database.url, wait_s=timeout_s + shutdown.WATCHDOG_GRACE_S)
    except journal_lock.JournalLockError as e:
        typer.echo(f"cannot start: {e}", err=True)
        return EXIT_FAILED
    except KeyboardInterrupt:  # Ctrl-C while waiting: nothing is owned yet, exit quietly
        typer.echo("interrupted while waiting for the journal lock", err=True)
        return EXIT_HARD
    dog = watchdog if watchdog is not None else Watchdog()
    control = ShutdownControl(timeout_s, arm_watchdog=dog.arm)
    install(make_signal_handler(control))
    try:
        upgrade(lock.url)
        source = make_source(settings, list(profile.universe))
        code = run(
            core_runtime.run_core(
                cfg,
                settings,
                profile,
                cfg.strategy.read_text(),
                source=source,
                connect_broker=AlpacaBroker.connect,
                lock=lock,
                control=control,
            )
        )
    except Exception as e:
        control.begin("startup_failure")  # no-op if teardown already began
        typer.echo(f"run core failed: {type(e).__name__}: {e}", err=True)
        code = EXIT_FAILED
    dog.cancel()
    return code
```

`shutdown.WATCHDOG_GRACE_S` is read through the module so that tests can monkeypatch it.

- [ ] **Step 4: Run all tests** with `uv run pytest -q` (redirect the output, check the exit code). Expected: PASS, including the existing `test_run_core_with_invalid_config_exits_1`.

- [ ] **Step 5: Lint, type-check, commit (Tasks 6 and 7 together)**

```bash
uv run ruff check --fix && uv run ruff format && uv run ruff check && uv run mypy && git add src/autotrader/core/runtime.py tests/unit/core/test_runtime.py src/autotrader/cli.py tests/unit/test_cli.py && git commit -m "WP 1.5a: run_core on lock/supervisor/shutdown coordinator; CLI lock before migration

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Documentation alignment

**Files:**
- Modify: `SPEC.md` §4.11 (the "Cross-cutting" bullet list)
- Modify: `PLAN.md` (the 1.5 row and the "Current repo state" line)
- Modify: `CLAUDE.md` (the commands list)

- [ ] **Step 1: Search for claims this WP falsified**

```bash
rg -n "Ctrl-C|run core|snapshot task|_snapshot_loop|kill detection|single-writer|shutdown" SPEC.md PLAN.md CLAUDE.md README.md src
```

Fix every hit that describes the old behaviour as current: kill detection inline in the feed loop, an unbounded stop, `_snapshot_loop`. Dated history entries (PLAN decision rows, dated notes, `docs/superpowers/plans/*`, review reports) get an appended dated note and are never rewritten.

- [ ] **Step 2: Apply the known edits**

- **`SPEC.md` §4.11:** add a bullet after the **Kill-switch** bullet:
  `- **Single writer and bounded shutdown (WP 1.5a):** \`run core\` holds an exclusive lock on \`<canonical journal path>.lock\` for its lifetime, taken before the migration; a second core waits up to \`shutdown.timeout_s\` + 5 s and fails. Feed ticks, kill polling and equity snapshots are independent supervised loops (stalls and \`risk.snapshot_overdue\` are logged). Every exit cause runs one bounded teardown (\`shutdown.timeout_s\`, exit 1 if forced); a second signal exits 130 immediately; a watchdog thread exits 70 at timeout + 5 s.`
- **`PLAN.md` 1.5 row:** append inside the last cell, before the final ` |`:
  ` **2026-10-14, 1.5a done:** single-writer lock, supervised loops, bounded shutdown coordinator, second-signal exit and watchdog (spec/plan 2026-10-14-runtime-safety). Next: 1.5b.`
- **`PLAN.md` "Current repo state":** change `Next: WP 1.5` to `WP 1.5a (runtime safety) done. Next: WP 1.5b`. Read the line first; if the wording differs, change the "Next" sentence equivalently.
- **`CLAUDE.md`:** change the `run core` line comment to `# market feed + scanner + kill detection against Alpaca paper; single-writer journal lock; Ctrl-C stops (twice = hard stop, exit 130)`.

- [ ] **Step 3: Verify that the PLAN tables still render**

Run:
```bash
python3 -c "import subprocess; old=subprocess.run(['git','show','origin/main:PLAN.md'],capture_output=True,text=True).stdout.splitlines(); new=open('PLAN.md').read().splitlines(); print([ (o[:12], o.count('|'), n.count('|')) for o,n in zip(old,new) if o!=n])"
```
Expected: every changed row keeps its pipe count, and each changed line apart from "Current repo state" only appends text.

- [ ] **Step 4: Final checks and commit**

```bash
uv run pytest -q > /tmp/wp-1-5a-pytest.txt 2>&1; echo $?
uv run pytest -q --cov=autotrader.risk --cov-branch --cov-report=term-missing > /tmp/wp-1-5a-cov.txt 2>&1; echo $?
uv run ruff check && uv run ruff format --check && uv run mypy && git add SPEC.md PLAN.md CLAUDE.md && git commit -m "docs: WP 1.5a runtime safety

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```
Expected: both pytest runs print `0`, and the risk coverage report shows 100 %.

---

## Plan verification (2026-10-14)

Tasks 1-7 were dry-run verbatim in a throwaway worktree before review. Nine snippet problems were found and are fixed above:
- unclosed subprocess pipes and the `S603` noqa;
- mypy `var-annotated` in test lists;
- a freshness test that ignored loop liveness;
- `assert` in `src/` (`S101`);
- a duplicate `equity_snapshots.ts` and an un-advanced `Mono`;
- a forced teardown that never disposed the engine, now `engine.dispose` with a 1 s floor (a design fix, also recorded in the spec);
- the Task 6/7 commit order, now one review unit;
- import sorting via `ruff check --fix`;
- a structlog stream left closed by an earlier CLI test, and leaked locks in the CLI tests.

Design notes from the dry run:
- The `signal.signal` handlers coexist with `asyncio.run`, which installs its own SIGINT handler only when the default is still in place.
- Ctrl-C while waiting for the lock now exits 130 without a traceback.

With all fixes, `uv run pytest -q` passed (791), ruff and mypy were clean, `risk/` had 100 % branch coverage, and five repeated runs of the new test files showed no flakiness.

A Codex plan review (2026-10-14) then added three fixes:
- the watchdog's stderr line is best-effort and bounded;
- a failed or cancelled teardown step makes the exit unclean;
- loop cadence is start-to-start with no catch-up.

The revised modules and tests were re-run in the dry-run worktree: 798 passed, ruff and mypy clean, `risk/` at 100 %, three repeated runs of the shutdown/supervise/runtime tests green.
