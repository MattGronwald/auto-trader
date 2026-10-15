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
