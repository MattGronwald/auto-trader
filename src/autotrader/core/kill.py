"""Kill-switch and pause state (SPEC §0.6, §4.11; PLAN G8).

Two independent kill triggers, either one is enough:
- the kill file (`control.kill_file`; a bare `touch` works, no DB needed),
- `control_state.kill` (set by `autotrader kill` / the API).

`ControlGate` answers "may we open new positions?" for the scanner and the Risk Engine.
It blocks on a kill, on `paused`, and on `paused_until` in the future, and fails closed:
unreadable control state or a malformed `paused_until` also block.

Kill and pause block **entries only**. Reduce-only exits (Risk Engine `exit()`, G1) stay
allowed so a kill can still flatten. Acting on a kill (cancel orders, optional flatten)
is the core's job once the broker exists (WP 0.6/1.5).
"""

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

import structlog
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.core.bus import DomainEvent, EventBus
from autotrader.journal import repo

log = structlog.get_logger(__name__)

KILL_KEY = "kill"
PAUSED_KEY = "paused"
PAUSED_UNTIL_KEY = "paused_until"


class KillTriggered(DomainEvent):
    source: str  # cli | api | dashboard | file
    reason: str | None


class KillCleared(DomainEvent):
    source: str


@dataclass(frozen=True)
class ControlStatus:
    killed: bool
    reasons: tuple[str, ...]  # why entries are blocked; empty = allowed

    @property
    def entries_allowed(self) -> bool:
        return not self.reasons


class ControlGate:
    def __init__(self, kill_file: Path, sessions: async_sessionmaker[AsyncSession]):
        self._kill_file = kill_file
        self._sessions = sessions

    @staticmethod
    def kill_file_present(kill_file: Path) -> bool:
        """Sync, DB-free check; cheap enough to run before every broker call."""
        return kill_file.exists()

    async def status(self, now: datetime | None = None) -> ControlStatus:
        now = now or datetime.now(UTC)
        reasons: list[str] = []
        killed = False
        if self.kill_file_present(self._kill_file):
            killed = True
            reasons.append(f"kill file present: {self._kill_file}")
        try:
            async with self._sessions() as session:
                kill = await repo.get_control(session, KILL_KEY)
                paused = await repo.get_control(session, PAUSED_KEY)
                paused_until = await repo.get_control(session, PAUSED_UNTIL_KEY)
        except Exception as e:  # fail closed: unknown state must not allow trading
            log.exception("control.state_unavailable")
            reasons.append(f"control state unavailable: {type(e).__name__}")
            return ControlStatus(killed, tuple(reasons))

        if kill:
            killed = True
            reasons.append(f"kill set in control_state: {kill}")
        if paused:
            reasons.append("trading paused (manual resume required)")
        if paused_until is not None:
            until = _parse_aware(paused_until)
            if until is None:
                reasons.append(f"paused_until is malformed: {paused_until!r}")
            elif now < until:
                reasons.append(f"trading paused until {until.isoformat()}")
        return ControlStatus(killed, tuple(reasons))


def _parse_aware(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None


def write_kill_file(
    kill_file: Path, *, source: str, reason: str | None, now: datetime
) -> dict[str, str | None]:
    """Latch the kill via the file. Sync and DB-free: this step must not depend on
    anything that can fail for database reasons. Raises if the file can't be written."""
    record = {"source": source, "reason": reason, "ts": now.isoformat()}
    kill_file.parent.mkdir(parents=True, exist_ok=True)
    kill_file.write_text(json.dumps(record) + "\n")
    log.warning("kill.triggered", source=source, reason=reason, kill_file=str(kill_file))
    return record


async def record_kill(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus | None,
    record: dict[str, str | None],
    now: datetime,
) -> None:
    """Second step of a kill: DB flag + `KillTriggered` event. Raises on failure."""
    async with sessions() as session:
        await repo.set_control(session, KILL_KEY, record)
    if bus is not None:
        await bus.publish(
            KillTriggered(source=str(record["source"]), reason=record["reason"], ts=now)
        )


@dataclass(frozen=True)
class KillResult:
    db_error: str | None  # set if the DB flag/event could not be written; file still is


async def trigger_kill(
    kill_file: Path,
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus | None,
    *,
    source: str,
    reason: str | None,
    now: datetime | None = None,
) -> KillResult:
    """Kill: latch the file first, then the DB flag and the `KillTriggered` event.

    The file alone stops trading, so a DB failure is reported, not raised. Callers that
    must create DB resources first (CLI) call `write_kill_file` before doing so.
    """
    now = now or datetime.now(UTC)
    record = write_kill_file(kill_file, source=source, reason=reason, now=now)
    try:
        await record_kill(sessions, bus, record, now)
    except Exception as e:
        log.exception("kill.db_update_failed")
        return KillResult(db_error=f"{type(e).__name__}: {e}")
    return KillResult(db_error=None)


async def clear_kill(
    kill_file: Path,
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus | None,
    *,
    source: str,
    now: datetime | None = None,
) -> None:
    """Lift a kill: clear the DB flag, record `KillCleared`, and only then remove the file.

    The file goes last: if any DB/audit step raises, the file (possibly the only latch,
    e.g. after a bare `touch`) stays and the kill holds. With a concurrent trigger, the
    result is "killed" unless the whole clear ran after the trigger's DB write.
    """
    now = now or datetime.now(UTC)
    async with sessions() as session:
        await repo.set_control(session, KILL_KEY, None)
    if bus is not None:
        await bus.publish(KillCleared(source=source, ts=now))
    kill_file.unlink(missing_ok=True)
    log.warning("kill.cleared", source=source)
