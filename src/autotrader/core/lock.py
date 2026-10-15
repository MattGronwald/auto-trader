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
