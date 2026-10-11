# Cycle Runner (WP 1.4) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn each delivered `CandidateSignal` into at most one audited cycle and drive it through a deterministic FSM (stub Decision Maker, one `RiskEngine.check()` call) with the G14 caps enforced exactly, without wiring it into `run core`.

**Architecture:** `EventBus.publish_tx(build, verify, on_uncertain=)` commits a cycle-row write and its `CycleTransitioned` event in one bus-owned transaction that survives caller cancellation. `CycleRunner` (in `core/cycle.py`) admits candidates under a lock and runs one asyncio task per cycle. `recover()` resolves interrupted cycles at startup, and `close(budget_s)` is a bounded shutdown. `agents/stub.py` is a pure rule-based decision function.

**Tech Stack:** Python 3.12, asyncio, SQLAlchemy 2 async (aiosqlite in tests), pydantic v2, structlog, pytest (asyncio_mode=auto) + hypothesis, uv.

**Spec:** `docs/superpowers/specs/2026-10-11-cycle-runner-design.md` (PLAN D15). Read it before starting any task. Section numbers below (§n) refer to it.

## Global Constraints

- Python 3.12, run everything through uv: `uv run pytest`, `uv run ruff check`, `uv run ruff format --check`, `uv run mypy`. All four must pass before every commit (ruff line length 100; mypy strict on `src` and `tests`).
- `Decimal` for money/qty/prices outside indicator math; convert floats with `Decimal(str(x))`, never `Decimal(x)`.
- Events subclass `DomainEvent` and go through `EventBus.publish()` / `publish_tx()`. Never write the `events` table directly outside `repo.append_event`.
- Only `risk/engine.py` holds a `Broker`. The runner talks to the engine through `check()` only.
- `check()` is called at most once per cycle and never retried. There is no runner-side timeout around it except the forced shutdown in `close()`.
- Tests never hit Alpaca or Anthropic. Never assert wall-clock durations; block on `asyncio.Event`s, and use tiny timeouts only where the code under test *is* a timeout.
- Warnings are errors in pytest (`filterwarnings = error`). Clean up every task you start in a test.
- Not wired into `run core` (WP 1.5 does that). Do not touch `core/runtime.py`.
- Commit messages describe the "why" and end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. Stage specific files, never `git add -A`.

## Review Focus

1. A feed quote with `ask` 0, negative or NaN (a feed glitch) must end the cycle `EXPIRED` ("no ask"), never crash on `EntryIntent` validation. The test is in Task 5 (enrich parametrization).
2. A gate read that hangs at admission (locked DB) must not stall the bus worker forever. Admission bounds `gate.status()` with `step_timeout_s` and drops with `gate unavailable: TimeoutError`. The test is in Task 6.
3. A candidate whose `features` contain `None` (NaN in the scanner) must still be stored in `cycles.signal_features` as valid JSON. The test is in Task 3.
4. Transition details holding Decimals must reach the `events` table as JSON strings that parse back to the same Decimal. The test is in Task 5 (happy path asserts `Decimal(details["stop"])`).
5. Suspension set while a second admission is already waiting for the lock must still drop that admission (the flag is re-checked inside the lock). The test is in Task 6.

---

## File Structure

| File | Responsibility |
|---|---|
| `src/autotrader/journal/repo.py` (modify) | `append_event(..., commit=)`; no-commit cycle writes `insert_cycle`, `transition_cycle` (guarded); reads `cycle_state`, `open_cycles`, `has_order_for_cycle`; `StaleTransition` |
| `src/autotrader/core/bus.py` (modify) | `publish_tx`, `CommitUncertain`, bus-owned units, bounded `close(budget_s)` |
| `src/autotrader/core/cycle.py` (create) | FSM vocabulary (`CycleState`, edges, `check_edge`), `CycleTransitioned`, `signal_is_stale`, `signal_features`, `CycleSnapshot`, `CloseReport`, `CycleRunner` |
| `src/autotrader/agents/stub.py` (create) | Phase-1 stub Decision Maker (`decide`, `StubBuy`, `StubSkip`, constants) |
| `tests/dbhooks.py` (create) | `CommitHook` + `hooked_sessionmaker`: commit-boundary fault injection shared by bus and runner tests |
| `tests/unit/journal/test_repo.py` (modify) | repo tests |
| `tests/unit/core/test_bus.py` (modify) | `publish_tx` / close tests |
| `tests/unit/core/test_cycle_fsm.py` (create) | pure FSM vocabulary tests |
| `tests/unit/agents/__init__.py`, `tests/unit/agents/test_stub.py` (create) | stub tests incl. hypothesis property |
| `tests/unit/core/test_cycle.py` (create) | runner FSM, admission, recovery, shutdown |
| `tests/integration/test_cycle_runner.py` (create) | real `RiskEngine` + `SimBroker` |
| `PLAN.md`, `SPEC.md`, `CLAUDE.md`, `core/scanner.py` docstring (modify) | docs alignment (Task 8) |

---

### Task 1: Journal functions for cycles and no-commit events

**Files:**
- Modify: `src/autotrader/journal/repo.py` (module docstring; `append_event` at ~line 149; new section after `open_position_symbols` at ~line 195)
- Test: `tests/unit/journal/test_repo.py` (append at the end)

**Interfaces:**
- Consumes: existing `Cycle`, `Event`, `Order` models; `TERMINAL_CYCLE_STATES`.
- Produces:
  - `append_event(session, type_: str, payload: dict[str, Any], *, cycle_id: int | None = None, ts: datetime | None = None, commit: bool = True) -> Event`
  - `class StaleTransition(RuntimeError)`
  - `insert_cycle(session, *, strategy_id: int, symbol: str, rule_id: str, signal_features: dict[str, Any], state: str, now: datetime) -> Cycle` (flushes, never commits)
  - `transition_cycle(session, cycle_id: int, *, from_state: str, to_state: str, now: datetime, reason: str | None = None) -> None` (never commits; raises `StaleTransition`)
  - `cycle_state(session, cycle_id: int) -> str | None`
  - `open_cycles(session) -> list[Cycle]`
  - `has_order_for_cycle(session, cycle_id: int) -> bool`

- [ ] **Step 1: Write the failing tests** (append to `tests/unit/journal/test_repo.py`)

```python
# --- cycles (WP 1.4) -----------------------------------------------------------------


async def _strategy_id(session: AsyncSession) -> int:
    return (await repo.register_strategy(session, load_profile(PROFILE_PATH), "y", now=T0)).id


async def test_append_event_without_commit_is_rolled_back(session: AsyncSession) -> None:
    await repo.append_event(session, "Ping", {"n": 1}, commit=False)
    await session.rollback()
    assert await repo.events_after(session, 0) == []


async def test_insert_cycle_flushes_an_id_but_does_not_commit(session: AsyncSession) -> None:
    sid = await _strategy_id(session)
    row = await repo.insert_cycle(
        session,
        strategy_id=sid,
        symbol="BTC/USD",
        rule_id="r1",
        signal_features={"close": 1.0, "vol_z": None, "_timeframe": "5m"},
        state="NEW",
        now=T0,
    )
    assert row.id is not None
    await session.rollback()
    assert await session.scalar(select(func.count()).select_from(m.Cycle)) == 0


async def test_transition_cycle_is_guarded_and_sets_terminal_fields(
    session: AsyncSession,
) -> None:
    sid = await _strategy_id(session)
    row = await repo.insert_cycle(
        session, strategy_id=sid, symbol="BTC/USD", rule_id="r1",
        signal_features={}, state="NEW", now=T0,
    )
    await session.commit()

    await repo.transition_cycle(
        session, row.id, from_state="NEW", to_state="ENRICHING", now=T0
    )
    with pytest.raises(repo.StaleTransition):
        await repo.transition_cycle(
            session, row.id, from_state="NEW", to_state="ENRICHING", now=T0
        )
    done = T0 + timedelta(seconds=1)
    await repo.transition_cycle(
        session, row.id, from_state="ENRICHING", to_state="EXPIRED", now=done,
        reason="symbol stale",
    )
    await session.commit()

    got = await session.get(m.Cycle, row.id, populate_existing=True)
    assert got is not None
    assert (got.state, got.outcome, got.reject_reason, got.finished_at) == (
        "EXPIRED", "EXPIRED", "symbol stale", done,
    )
    assert await repo.cycle_state(session, row.id) == "EXPIRED"
    assert await repo.cycle_state(session, row.id + 99) is None


async def test_non_terminal_transition_leaves_outcome_empty(session: AsyncSession) -> None:
    sid = await _strategy_id(session)
    row = await repo.insert_cycle(
        session, strategy_id=sid, symbol="BTC/USD", rule_id="r1",
        signal_features={}, state="NEW", now=T0,
    )
    await repo.transition_cycle(
        session, row.id, from_state="NEW", to_state="ENRICHING", now=T0, reason="ignored"
    )
    await session.commit()
    got = await session.get(m.Cycle, row.id, populate_existing=True)
    assert got is not None
    assert (got.outcome, got.finished_at, got.reject_reason) == (None, None, None)


async def test_open_cycles_and_order_lookup(session: AsyncSession) -> None:
    sid = await _strategy_id(session)
    kw: dict[str, Any] = dict(strategy_id=sid, rule_id="r1", signal_features={}, now=T0)
    open_ = await repo.insert_cycle(session, symbol="BTC/USD", state="RISK_CHECK", **kw)
    await repo.insert_cycle(session, symbol="ETH/USD", state="ERROR", **kw)
    await session.commit()

    assert [c.id for c in await repo.open_cycles(session)] == [open_.id]
    assert not await repo.has_order_for_cycle(session, open_.id)
    await repo.insert_order(
        session, cycle_id=open_.id, client_order_id=f"at-c{open_.id}-e", symbol="BTC/USD",
        mode="paper", type_="limit", side="buy", qty=Decimal("1"),
        limit_price=Decimal("100"), stop_price=None, tp_price=None, status="pending", now=T0,
    )
    assert await repo.has_order_for_cycle(session, open_.id)
```

Run `uv run ruff format tests/unit/journal/test_repo.py` after pasting, so the compact argument lists get reformatted.

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/journal/test_repo.py -k "append_event_without or insert_cycle or transition_cycle or non_terminal or open_cycles_and" -v`
Expected: FAIL (`append_event() got an unexpected keyword argument 'commit'`, `module 'autotrader.journal.repo' has no attribute 'insert_cycle'`).

- [ ] **Step 3: Implement**

In `repo.py`:

1. Module docstring, first sentence. Replace it with:

```python
"""Journal access. Functions take an `AsyncSession` and commit their own unit of work,
except the cycle writes (`insert_cycle`, `transition_cycle`) and `append_event(...,
commit=False)`: those run inside `EventBus.publish_tx`, which commits the row together with
its event (WP 1.4, D15).
```

Keep the rest of the docstring. Add `from sqlalchemy.engine import CursorResult` and `from typing import Any, cast` to the imports (`Any` is already imported; add `cast`).

2. Replace `append_event`:

```python
async def append_event(
    session: AsyncSession,
    type_: str,
    payload: dict[str, Any],
    *,
    cycle_id: int | None = None,
    ts: datetime | None = None,
    commit: bool = True,
) -> Event:
    """Append one event. `commit=False` only flushes: the caller's transaction decides."""
    row = Event(ts=ts or _now(), type=type_, cycle_id=cycle_id, payload=payload)
    session.add(row)
    if commit:
        await session.commit()
    else:
        await session.flush()
    return row
```

3. After `open_position_symbols`, add:

```python
# --- cycles (WP 1.4) -----------------------------------------------------------------
# The two writes below never commit: they run inside `EventBus.publish_tx`, which
# commits the cycle row together with its `CycleTransitioned` event (D15).


class StaleTransition(RuntimeError):
    """The guarded update matched no row: the cycle is not in the expected state."""


async def insert_cycle(
    session: AsyncSession,
    *,
    strategy_id: int,
    symbol: str,
    rule_id: str,
    signal_features: dict[str, Any],
    state: str,
    now: datetime,
) -> Cycle:
    row = Cycle(
        strategy_id=strategy_id,
        symbol=symbol,
        rule_id=rule_id,
        signal_features=signal_features,
        state=state,
        created_at=now,
    )
    session.add(row)
    await session.flush()  # assigns row.id
    return row


async def transition_cycle(
    session: AsyncSession,
    cycle_id: int,
    *,
    from_state: str,
    to_state: str,
    now: datetime,
    reason: str | None = None,
) -> None:
    """`from_state -> to_state` only if the row is still in `from_state`.

    Terminal states also set `outcome`, `finished_at` and `reject_reason`.
    """
    values: dict[str, Any] = {"state": to_state}
    if to_state in TERMINAL_CYCLE_STATES:
        values.update(outcome=to_state, finished_at=now, reject_reason=reason)
    result = cast(
        CursorResult[Any],
        await session.execute(
            update(Cycle)
            .where(Cycle.id == cycle_id, Cycle.state == from_state)
            .values(**values)
        ),
    )
    if result.rowcount != 1:
        raise StaleTransition(f"cycle {cycle_id} is not in state {from_state}")


async def cycle_state(session: AsyncSession, cycle_id: int) -> str | None:
    return await session.scalar(select(Cycle.state).where(Cycle.id == cycle_id))


async def open_cycles(session: AsyncSession) -> list[Cycle]:
    rows = await session.scalars(
        select(Cycle)
        .where(Cycle.state.not_in(TERMINAL_CYCLE_STATES))
        .order_by(Cycle.id)
        .execution_options(populate_existing=True)
    )
    return list(rows)


async def has_order_for_cycle(session: AsyncSession, cycle_id: int) -> bool:
    n = await session.scalar(
        select(func.count()).select_from(Order).where(Order.cycle_id == cycle_id)
    )
    return bool(n)
```

- [ ] **Step 4: Run the tests to verify they pass, then the full suite**

Run: `uv run pytest tests/unit/journal/test_repo.py -v` → all PASS.
Run: `uv run pytest && uv run ruff check && uv run ruff format --check && uv run mypy` → all pass. Existing bus tests patch `repo.append_event` with a fake; if one of those fakes rejects the new `commit` keyword, it only matters for `publish_tx` (Task 2), and plain `publish` doesn't pass it.

- [ ] **Step 5: Commit**

```bash
git add src/autotrader/journal/repo.py tests/unit/journal/test_repo.py
git commit -m "Journal: no-commit cycle writes for transactional publishing

The Cycle Runner must commit a cycle row and its transition event in one
transaction (D15), so these writes flush instead of committing; the guard
on the expected state turns a lost race into an error instead of an
overwrite.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: `EventBus.publish_tx`, uncertain commits and a bounded `close()`

**Files:**
- Create: `tests/dbhooks.py`
- Modify: `src/autotrader/core/bus.py`
- Test: `tests/unit/core/test_bus.py` (append)

**Interfaces:**
- Consumes: `repo.append_event(..., commit=False)` (Task 1).
- Produces (in `autotrader.core.bus`):
  - `class CommitUncertain(RuntimeError)`
  - `Build = Callable[[AsyncSession], Awaitable[DomainEvent | None]]`
  - `Verify = Callable[[AsyncSession, DomainEvent], Awaitable[bool]]`
  - `OnUncertain = Callable[[CommitUncertain], None]`
  - `EventBus.publish_tx(build: Build, verify: Verify, *, on_uncertain: OnUncertain | None = None) -> DomainEvent | None`
  - `EventBus.close(budget_s: float = CLOSE_BUDGET_S) -> None`, with `CLOSE_BUDGET_S = 10.0`
- Produces (in `tests.dbhooks`): `CommitHook` (attributes `fail_before: int`, `fail_after: int`, `block_after: asyncio.Event | None`, `reached: asyncio.Event`) and `hooked_sessionmaker(engine: AsyncEngine, hook: CommitHook) -> async_sessionmaker[AsyncSession]`.

- [ ] **Step 1: Create the fault-injection helper `tests/dbhooks.py`**

```python
"""Commit-boundary fault injection for tests (WP 1.4, D15).

`hooked_sessionmaker` returns sessions whose `commit()` consults a `CommitHook`:
- `fail_before = n`: the next n commits raise *without* committing (a commit that may
  still land later, from the caller's point of view);
- `fail_after = n`: the next n commits *do* commit, then raise (a lost acknowledgement);
- `block_after = event`: the next commit commits, sets `reached`, then waits for `event`
  (the caller can be cancelled between the DB commit and its confirmation).
"""

import asyncio

from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker


def lost_ack() -> OperationalError:
    return OperationalError("COMMIT", None, Exception("connection lost during COMMIT"))


class CommitHook:
    def __init__(self) -> None:
        self.fail_before = 0
        self.fail_after = 0
        self.block_after: asyncio.Event | None = None
        self.reached = asyncio.Event()

    async def before(self) -> None:
        if self.fail_before:
            self.fail_before -= 1
            raise lost_ack()

    async def after(self) -> None:
        if self.block_after is not None:
            release, self.block_after = self.block_after, None
            self.reached.set()
            await release.wait()
        if self.fail_after:
            self.fail_after -= 1
            raise lost_ack()


def hooked_sessionmaker(engine: AsyncEngine, hook: CommitHook) -> async_sessionmaker[AsyncSession]:
    class HookedSession(AsyncSession):
        async def commit(self) -> None:
            await hook.before()
            await super().commit()
            await hook.after()

    return async_sessionmaker(engine, class_=HookedSession, expire_on_commit=False)
```

- [ ] **Step 2: Write the failing tests** (append to `tests/unit/core/test_bus.py`; add the imports at the top)

Imports to add:

```python
from structlog.testing import capture_logs

from autotrader.core.bus import CommitUncertain
from tests.dbhooks import CommitHook, hooked_sessionmaker
```

Tests:

```python
# --- publish_tx (WP 1.4, D15) --------------------------------------------------------


class Note(DomainEvent):
    key: str


def note_build(
    key: str, *, skip: bool = False, fail: bool = False, hold: asyncio.Event | None = None,
    entered: asyncio.Event | None = None,
) -> Callable[[AsyncSession], Awaitable[DomainEvent | None]]:
    """Writes a control_state row (stand-in for the cycle row) and returns its event."""

    async def build(session: AsyncSession) -> DomainEvent | None:
        session.add(m.ControlState(key=key, value=True))
        await session.flush()
        if entered is not None:
            entered.set()
        if hold is not None:
            await hold.wait()
        if fail:
            raise RuntimeError("build failed")
        return None if skip else Note(key=key)

    return build


async def note_visible(session: AsyncSession, event: DomainEvent) -> bool:
    return isinstance(event, Note) and await session.get(m.ControlState, event.key) is not None


async def note_invisible(session: AsyncSession, event: DomainEvent) -> bool:
    return False


async def has_control(sessions: async_sessionmaker[AsyncSession], key: str) -> bool:
    async with sessions() as s:
        return await s.get(m.ControlState, key) is not None


@pytest.fixture
def hook() -> CommitHook:
    return CommitHook()


@pytest.fixture
async def hbus(db_url: str, hook: CommitHook) -> AsyncIterator[EventBus]:
    engine = make_engine(db_url)
    b = EventBus(hooked_sessionmaker(engine, hook))
    yield b
    await b.close()
    await engine.dispose()


def collect(bus: EventBus) -> list[str]:
    seen: list[str] = []

    async def handler(e: DomainEvent) -> None:
        seen.append(e.key if isinstance(e, Note) else f"{type(e).__name__}")

    bus.subscribe(DomainEvent, handler)
    return seen


async def test_publish_tx_commits_write_and_event_together_and_delivers(
    bus: EventBus, sessions: async_sessionmaker[AsyncSession]
) -> None:
    seen = collect(bus)
    out = await bus.publish_tx(note_build("a"), note_visible)
    await bus.drain()

    assert isinstance(out, Note) and out.key == "a"
    assert seen == ["a"]
    assert await has_control(sessions, "a")
    assert [r.type for r in await _events(sessions)] == ["Note"]


async def test_publish_tx_none_writes_and_delivers_nothing(
    bus: EventBus, sessions: async_sessionmaker[AsyncSession]
) -> None:
    seen = collect(bus)
    assert await bus.publish_tx(note_build("a", skip=True), note_visible) is None
    await bus.drain()
    assert seen == []
    assert not await has_control(sessions, "a")
    assert await _events(sessions) == []


async def test_publish_tx_build_error_rolls_back_and_propagates(
    bus: EventBus, sessions: async_sessionmaker[AsyncSession]
) -> None:
    with pytest.raises(RuntimeError, match="build failed"):
        await bus.publish_tx(note_build("a", fail=True), note_visible)
    assert not await has_control(sessions, "a")
    assert await _events(sessions) == []


async def test_plain_publish_waits_for_a_transactional_unit_and_order_is_kept(
    bus: EventBus, sessions: async_sessionmaker[AsyncSession]
) -> None:
    seen = collect(bus)
    hold, entered = asyncio.Event(), asyncio.Event()
    unit = asyncio.create_task(bus.publish_tx(note_build("a", hold=hold, entered=entered),
                                              note_visible))
    await entered.wait()
    plain = asyncio.create_task(bus.publish(Ping(n=1)))
    for _ in range(20):
        await asyncio.sleep(0)
    assert not plain.done()  # the unit holds the bus lock

    hold.set()
    await asyncio.gather(unit, plain)
    await bus.drain()
    assert [r.type for r in await _events(sessions)] == ["Note", "Ping"]
    assert seen == ["a", "Ping"]


async def test_cancelled_caller_does_not_stop_the_unit(
    bus: EventBus, sessions: async_sessionmaker[AsyncSession]
) -> None:
    seen = collect(bus)
    hold, entered = asyncio.Event(), asyncio.Event()
    caller = asyncio.create_task(
        bus.publish_tx(note_build("a", hold=hold, entered=entered), note_visible)
    )
    await entered.wait()
    caller.cancel()
    hold.set()
    with pytest.raises(asyncio.CancelledError):
        await caller
    await bus.close()  # waits for the unit, then drains
    assert seen == ["a"]
    assert await has_control(sessions, "a")


async def test_caller_cancelled_after_db_commit_before_confirmation(
    hbus: EventBus, hook: CommitHook, sessions: async_sessionmaker[AsyncSession]
) -> None:
    seen = collect(hbus)
    release = asyncio.Event()
    hook.block_after = release
    caller = asyncio.create_task(hbus.publish_tx(note_build("a"), note_visible))
    await hook.reached.wait()  # the DB has committed; the unit hasn't confirmed yet
    caller.cancel()
    with pytest.raises(asyncio.CancelledError):
        await caller
    release.set()
    await hbus.close()
    assert seen == ["a"]  # exactly once
    assert await has_control(sessions, "a")
    assert [r.type for r in await _events(sessions)] == ["Note"]


async def test_commit_error_with_positive_read_back_is_recovered(
    hbus: EventBus, hook: CommitHook, sessions: async_sessionmaker[AsyncSession]
) -> None:
    seen = collect(hbus)
    hook.fail_after = 1
    with capture_logs() as logs:
        out = await hbus.publish_tx(note_build("a"), note_visible)
    await hbus.drain()
    assert isinstance(out, Note)
    assert seen == ["a"]
    assert any(e["event"] == "bus.commit_recovered" for e in logs)


@pytest.mark.parametrize("verify_raises", [False, True])
async def test_commit_error_without_confirmation_is_uncertain(
    verify_raises: bool,
    hbus: EventBus,
    hook: CommitHook,
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    seen = collect(hbus)
    flagged: list[CommitUncertain] = []

    async def broken(session: AsyncSession, event: DomainEvent) -> bool:
        raise RuntimeError("read-back failed")

    hook.fail_before = 1  # nothing committed, but the caller can't know that
    with pytest.raises(CommitUncertain):
        await hbus.publish_tx(
            note_build("a"), broken if verify_raises else note_invisible,
            on_uncertain=flagged.append,
        )
    await hbus.drain()
    assert seen == []
    assert len(flagged) == 1


async def test_on_uncertain_fires_even_when_the_caller_is_gone(
    hbus: EventBus, hook: CommitHook
) -> None:
    flagged: list[CommitUncertain] = []
    hold, entered = asyncio.Event(), asyncio.Event()
    hook.fail_before = 1
    caller = asyncio.create_task(
        hbus.publish_tx(
            note_build("a", hold=hold, entered=entered), note_invisible,
            on_uncertain=flagged.append,
        )
    )
    await entered.wait()
    caller.cancel()
    hold.set()
    with pytest.raises(asyncio.CancelledError):
        await caller
    await hbus.close()
    assert len(flagged) == 1


async def test_close_finishes_an_in_flight_unit_then_refuses(
    bus: EventBus, sessions: async_sessionmaker[AsyncSession]
) -> None:
    seen = collect(bus)
    hold, entered = asyncio.Event(), asyncio.Event()
    unit = asyncio.create_task(
        bus.publish_tx(note_build("a", hold=hold, entered=entered), note_visible)
    )
    await entered.wait()
    closing = asyncio.create_task(bus.close(budget_s=5))
    for _ in range(20):
        await asyncio.sleep(0)
    hold.set()
    await asyncio.gather(unit, closing)
    assert seen == ["a"]
    with pytest.raises(BusClosedError):
        await bus.publish_tx(note_build("b"), note_visible)
    with pytest.raises(BusClosedError):
        await bus.publish(Ping(n=2))


async def test_close_is_bounded_and_reports_what_it_abandons(
    sessions: async_sessionmaker[AsyncSession],
) -> None:
    bus = EventBus(sessions)
    hold, entered = asyncio.Event(), asyncio.Event()
    unit = asyncio.create_task(
        bus.publish_tx(note_build("a", hold=hold, entered=entered), note_visible)
    )
    await entered.wait()
    with capture_logs() as logs:
        await bus.close(budget_s=0.01)  # the unit never finishes within the budget
    assert any(e["event"] == "bus.task_abandoned" for e in logs)
    hold.set()  # clean up: let the abandoned unit finish
    await unit
```

`Callable` and `Awaitable` need `from collections.abc import AsyncIterator, Awaitable, Callable` (`AsyncIterator` is already imported).

- [ ] **Step 3: Run them to verify they fail**

Run: `uv run pytest tests/unit/core/test_bus.py -v`
Expected: the new tests FAIL (`ImportError: cannot import name 'CommitUncertain'`); the existing ones would pass once the import resolves.

- [ ] **Step 4: Implement in `src/autotrader/core/bus.py`**

1. Extend the module docstring. Append this paragraph before "Subscriptions match by class":

```
`publish_tx(build, verify, on_uncertain=)` (WP 1.4, D15) commits a caller's writes and
their event in one transaction: under the same lock, `build(session)` writes without
committing and returns the event (or `None`: roll back, nothing to do), the bus appends
the event, commits once and enqueues. The unit runs in a bus-owned task the caller
awaits through `asyncio.shield`, so cancelling the caller never interrupts a commit or
loses a committed event's delivery. A commit that raises is not proof of rollback: the
bus asks `verify` once in a fresh session; only a positive answer confirms (deliver),
anything else raises `CommitUncertain` after calling `on_uncertain` from the unit
itself. Delivery is exactly once for confirmed commits; an uncertain or abandoned unit
may be persisted without delivery (the `events` table stays the record).

`close(budget_s)` is bounded: it marks the bus closed, waits for in-flight publishes
and units and for the subscriber backlog until the deadline, then stops the workers.
Anything still running at the deadline is logged `bus.task_abandoned` and left to
process exit (the process-level hard deadline, WP 1.5).
```

Also correct the stale sentence `close()` takes the same lock before marking the bus closed, ... to: "`close()` marks the bus closed and then waits for the lock (within its budget), so a publish already in progress completes (persisted *and* enqueued) before shutdown drains the queues, and a later publish is rejected before it persists."

2. Below `class BusClosedError`, add:

```python
class CommitUncertain(RuntimeError):
    """A transactional publish may or may not have committed (D15)."""


Build = Callable[[AsyncSession], Awaitable["DomainEvent | None"]]
Verify = Callable[[AsyncSession, "DomainEvent"], Awaitable[bool]]
OnUncertain = Callable[[CommitUncertain], None]

CLOSE_BUDGET_S = 10.0
```

(Place these after `DomainEvent` is defined so the quotes are unnecessary; then drop the quotes.)

3. In `EventBus.__init__` add `self._units: set[asyncio.Task[DomainEvent | None]] = set()`.

4. Factor the enqueue loop out of `publish` and add the new methods:

```python
    async def publish(self, event: DomainEvent) -> None:
        event_type = type(event).__name__
        async with self._lock:
            if self._closed:
                raise BusClosedError("event bus is closed")
            async with self._sessions() as session:
                row = await repo.append_event(
                    session, event_type, event.payload(), cycle_id=event.cycle_id, ts=event.ts
                )
            self._enqueue(event)
        log.debug("bus.published", event_type=event_type, event_id=row.id)

    async def publish_tx(
        self, build: Build, verify: Verify, *, on_uncertain: OnUncertain | None = None
    ) -> DomainEvent | None:
        """Commit `build`'s writes and the event it returns together, then deliver."""
        if self._closed:
            raise BusClosedError("event bus is closed")
        unit = asyncio.create_task(self._unit(build, verify, on_uncertain), name="bus:unit")
        self._units.add(unit)
        unit.add_done_callback(self._unit_done)
        return await asyncio.shield(unit)

    def _unit_done(self, unit: asyncio.Task[DomainEvent | None]) -> None:
        self._units.discard(unit)
        if not unit.cancelled():
            unit.exception()  # retrieved here; a caller that is still waiting re-raises it

    async def _unit(
        self, build: Build, verify: Verify, on_uncertain: OnUncertain | None
    ) -> DomainEvent | None:
        async with self._lock:
            async with self._sessions() as session:
                event = await build(session)
                if event is None:
                    await session.rollback()
                    return None
                event_type = type(event).__name__
                await repo.append_event(
                    session,
                    event_type,
                    event.payload(),
                    cycle_id=event.cycle_id,
                    ts=event.ts,
                    commit=False,
                )
                try:
                    await session.commit()
                except Exception as e:
                    if not await self._confirmed(verify, event):
                        error = CommitUncertain(f"{event_type} may or may not be committed")
                        log.error(
                            "bus.commit_uncertain",
                            event_type=event_type,
                            cycle_id=event.cycle_id,
                            error=repr(e),
                        )
                        if on_uncertain is not None:
                            try:
                                on_uncertain(error)
                            except Exception:
                                log.exception("bus.on_uncertain_failed")
                        raise error from e
                    log.warning(
                        "bus.commit_recovered", event_type=event_type, cycle_id=event.cycle_id
                    )
            self._enqueue(event)
        return event

    async def _confirmed(self, verify: Verify, event: DomainEvent) -> bool:
        try:
            async with self._sessions() as session:
                return await verify(session, event)
        except Exception:
            log.exception("bus.verify_failed", event_type=type(event).__name__)
            return False

    def _enqueue(self, event: DomainEvent) -> None:
        for sub in self._subs:
            if isinstance(event, sub.event_type):
                sub.queue.put_nowait(event)
                if sub.queue.qsize() == HIGH_WATER:
                    log.warning(
                        "bus.queue_high_water", handler=_name(sub.handler), queued=HIGH_WATER
                    )
```

5. Replace `close`:

```python
    async def close(self, budget_s: float = CLOSE_BUDGET_S) -> None:
        """Stop accepting events, finish in-flight work and the backlog within `budget_s`."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + budget_s

        def remaining() -> float:
            return max(0.0, deadline - loop.time())

        self._closed = True  # new publishes raise from here on
        try:
            async with asyncio.timeout(remaining()):
                async with self._lock:  # in-flight publishes and units hold it
                    pass
                if self._units:
                    await asyncio.wait(set(self._units))
                await self.drain()
        except TimeoutError:
            log.warning("bus.close_timeout", budget_s=budget_s)
        workers = [sub.task for sub in self._subs if sub.task is not None]
        for task in workers:
            task.cancel()
        if workers:
            await asyncio.wait(workers, timeout=remaining())
        for task in [*workers, *self._units]:
            if not task.done():
                log.error("bus.task_abandoned", task=task.get_name())
```

- [ ] **Step 5: Run the bus tests, then the whole suite**

Run: `uv run pytest tests/unit/core/test_bus.py -v` → all PASS (old and new).
Run: `uv run pytest && uv run ruff check && uv run ruff format --check && uv run mypy` → pass.

- [ ] **Step 6: Commit**

```bash
git add src/autotrader/core/bus.py tests/dbhooks.py tests/unit/core/test_bus.py
git commit -m "Bus: transactional publish owned by the bus, bounded close

A cycle row and its transition event must commit together and be
delivered in id order even if the publishing task is cancelled after the
database committed. A failed commit is only trusted as committed after a
positive read-back; otherwise the caller is told the outcome is uncertain
through a callback the bus fires itself. close() no longer waits without
bound.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: FSM vocabulary (`core/cycle.py`, part 1)

**Files:**
- Create: `src/autotrader/core/cycle.py`
- Test: `tests/unit/core/test_cycle_fsm.py`

**Interfaces:**
- Consumes: `DomainEvent` (`core/bus.py`), `parse_timeframe` (`core/resample.py`), `CandidateSignal` (`core/scanner.py`), `repo.TERMINAL_CYCLE_STATES`.
- Produces:
  - `class CycleState(StrEnum)`: `NEW, ENRICHING, ANALYZING, DECIDING, RISK_CHECK, SUBMITTED, FILLED, REJECTED, EXPIRED, ERROR`
  - `TERMINAL: frozenset[CycleState]`, `BEFORE_CHECK: frozenset[CycleState]` (= NEW, ENRICHING, ANALYZING, DECIDING)
  - `RUNNER_EDGES`, `RECONCILIATION_ONLY_EDGES: frozenset[tuple[CycleState, CycleState]]`
  - `check_edge(from_: CycleState | None, to: CycleState, *, reconciliation: bool = False) -> None` (raises `ValueError`)
  - `class CycleTransitioned(DomainEvent)`: `from_state: CycleState | None`, `to_state: CycleState`, `details: dict[str, Any]`
  - `signal_is_stale(bar_ts: datetime, timeframe: str, now: datetime) -> bool`
  - `signal_features(candidate: CandidateSignal) -> dict[str, Any]`

- [ ] **Step 1: Write the failing tests** (`tests/unit/core/test_cycle_fsm.py`)

```python
"""FSM vocabulary of the Cycle Runner (spec 2026-10-11-cycle-runner-design.md §4)."""

import json
from datetime import UTC, datetime, timedelta

import pytest

from autotrader.core.cycle import (
    RECONCILIATION_ONLY_EDGES,
    TERMINAL,
    CycleState,
    CycleTransitioned,
    check_edge,
    signal_features,
    signal_is_stale,
)
from autotrader.core.scanner import CandidateSignal
from autotrader.journal import repo

S = CycleState
NOW = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)


def test_terminal_states_match_the_journal() -> None:
    assert {str(s) for s in TERMINAL} == repo.TERMINAL_CYCLE_STATES


@pytest.mark.parametrize(
    ("from_", "to"),
    [
        (None, S.NEW),
        (S.NEW, S.ENRICHING),
        (S.ENRICHING, S.ANALYZING),
        (S.ENRICHING, S.EXPIRED),
        (S.ANALYZING, S.DECIDING),
        (S.DECIDING, S.RISK_CHECK),
        (S.DECIDING, S.REJECTED),
        (S.RISK_CHECK, S.SUBMITTED),
        (S.RISK_CHECK, S.REJECTED),
        (S.RISK_CHECK, S.EXPIRED),
        (S.SUBMITTED, S.FILLED),
        (S.SUBMITTED, S.REJECTED),
        *[(s, S.ERROR) for s in S if s not in TERMINAL],
    ],
)
def test_runner_edges_are_allowed(from_: CycleState | None, to: CycleState) -> None:
    check_edge(from_, to)


@pytest.mark.parametrize(
    ("from_", "to"),
    [
        (None, S.ENRICHING),
        (S.NEW, S.DECIDING),
        (S.ERROR, S.NEW),
        (S.FILLED, S.ERROR),
        (S.SUBMITTED, S.EXPIRED),  # reconciliation only
        (S.RISK_CHECK, S.FILLED),  # reconciliation only
    ],
)
def test_other_edges_raise(from_: CycleState | None, to: CycleState) -> None:
    with pytest.raises(ValueError, match="illegal cycle transition"):
        check_edge(from_, to)


def test_reconciliation_may_take_its_own_edges_and_the_shared_one() -> None:
    assert RECONCILIATION_ONLY_EDGES == {(S.SUBMITTED, S.EXPIRED), (S.RISK_CHECK, S.FILLED)}
    for from_, to in [*RECONCILIATION_ONLY_EDGES, (S.SUBMITTED, S.FILLED)]:
        check_edge(from_, to, reconciliation=True)


@pytest.mark.parametrize(
    ("age_after_bar_end", "stale"),
    [(timedelta(0), False), (timedelta(minutes=5), False), (timedelta(minutes=5, seconds=1), True)],
)
def test_signal_freshness_matches_the_scanner_rule(
    age_after_bar_end: timedelta, stale: bool
) -> None:
    bar_ts = NOW - timedelta(minutes=5)  # bar ends at NOW
    assert signal_is_stale(bar_ts, "5m", NOW + age_after_bar_end) is stale


def test_signal_features_keep_timeframe_bar_and_nulls_as_json() -> None:
    c = CandidateSignal(
        ts=NOW, symbol="BTC/USD", direction="long", rule_id="r1", score=0.7,
        timeframe="5m", bar_ts=NOW - timedelta(minutes=5),
        features={"close": 100.0, "vol_z": None},
    )
    got = signal_features(c)
    assert got == {
        "close": 100.0, "vol_z": None, "_timeframe": "5m", "_bar_ts": "2026-10-09T11:55:00+00:00",
    }
    json.dumps(got)  # valid JSON for the cycles.signal_features column


def test_cycle_transitioned_round_trips_through_its_payload() -> None:
    e = CycleTransitioned(
        ts=NOW, cycle_id=7, from_state=S.DECIDING, to_state=S.RISK_CHECK,
        details={"stop": "97.1000"},
    )
    payload = e.payload()  # must stay the method: the field is `details`, not `payload`
    assert payload == {"from_state": "DECIDING", "to_state": "RISK_CHECK",
                       "details": {"stop": "97.1000"}}
    back = CycleTransitioned.model_validate({**payload, "ts": NOW, "cycle_id": 7})
    assert back == e
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/core/test_cycle_fsm.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'autotrader.core.cycle'`.

- [ ] **Step 3: Implement `src/autotrader/core/cycle.py` (part 1)**

```python
"""Cycle Runner (SPEC §4.3; PLAN D15; spec 2026-10-11-cycle-runner-design.md).

One cycle per delivered `CandidateSignal` (the bus delivers at most once and never
replays). The `CandidateSignal` handler only admits: freshness, gate, then under the
admission lock the G14 caps and open entry orders, and the insert (`publish_tx`, so the
row and its `CycleTransitioned` event commit together). One asyncio task per cycle then
walks

    NEW -> ENRICHING -> ANALYZING -> DECIDING -> RISK_CHECK -> SUBMITTED -> FILLED

with REJECTED / EXPIRED / ERROR exits (§4). `check()` is called once, never retried,
and never timed out by the runner (only `close()` forces it). After submission only
proven outcomes are mapped; `new` / `unknown` / `canceled` stay SUBMITTED until WP 1.5
reconciles the order. Orders are reconciled by order row, not cycle state: an ERROR
cycle may own a real `pending` order.

Any `CommitUncertain` suspends admission for the rest of the process; `recover()` on
the next start resolves the uncertain rows (§9).
"""

from datetime import datetime
from enum import StrEnum
from typing import Any

from pydantic import Field

from autotrader.core.bus import DomainEvent
from autotrader.core.resample import parse_timeframe
from autotrader.core.scanner import CandidateSignal


class CycleState(StrEnum):
    NEW = "NEW"
    ENRICHING = "ENRICHING"
    ANALYZING = "ANALYZING"
    DECIDING = "DECIDING"
    RISK_CHECK = "RISK_CHECK"
    SUBMITTED = "SUBMITTED"
    FILLED = "FILLED"
    REJECTED = "REJECTED"
    EXPIRED = "EXPIRED"
    ERROR = "ERROR"


S = CycleState
TERMINAL = frozenset({S.FILLED, S.REJECTED, S.EXPIRED, S.ERROR})
# Nothing was sent to the broker in these states (recover() may end them).
BEFORE_CHECK = frozenset({S.NEW, S.ENRICHING, S.ANALYZING, S.DECIDING})

RUNNER_EDGES: frozenset[tuple[CycleState, CycleState]] = frozenset(
    {
        (S.NEW, S.ENRICHING),
        (S.ENRICHING, S.ANALYZING),
        (S.ENRICHING, S.EXPIRED),
        (S.ANALYZING, S.DECIDING),
        (S.DECIDING, S.RISK_CHECK),
        (S.DECIDING, S.REJECTED),
        (S.RISK_CHECK, S.SUBMITTED),
        (S.RISK_CHECK, S.REJECTED),
        (S.RISK_CHECK, S.EXPIRED),
        (S.SUBMITTED, S.FILLED),  # shared with reconciliation
        (S.SUBMITTED, S.REJECTED),
        *((s, S.ERROR) for s in S if s not in TERMINAL),
    }
)
# Taken only by WP 1.5 reconciliation, from the broker's filled qty (§4).
RECONCILIATION_ONLY_EDGES: frozenset[tuple[CycleState, CycleState]] = frozenset(
    {(S.SUBMITTED, S.EXPIRED), (S.RISK_CHECK, S.FILLED)}
)


def check_edge(from_: CycleState | None, to: CycleState, *, reconciliation: bool = False) -> None:
    if from_ is None:
        ok = to is S.NEW
    else:
        edges = RUNNER_EDGES | RECONCILIATION_ONLY_EDGES if reconciliation else RUNNER_EDGES
        ok = (from_, to) in edges
    if not ok:
        raise ValueError(f"illegal cycle transition {from_} -> {to}")


class CycleTransitioned(DomainEvent):
    """One persisted state change. `details` (not `payload`: that is the bus's method)."""

    from_state: CycleState | None
    to_state: CycleState
    details: dict[str, Any] = Field(default_factory=dict)


def signal_is_stale(bar_ts: datetime, timeframe: str, now: datetime) -> bool:
    """The Scanner's obsolete-bar rule (D13): more than one period since the bar ended."""
    period = parse_timeframe(timeframe)
    return now - (bar_ts + period) > period


def signal_features(candidate: CandidateSignal) -> dict[str, Any]:
    """`cycles.signal_features`: the features plus the candidate's identity in time."""
    return {
        **candidate.features,
        "_timeframe": candidate.timeframe,
        "_bar_ts": candidate.bar_ts.isoformat(),
    }
```

- [ ] **Step 4: Run the tests, then the whole suite**

Run: `uv run pytest tests/unit/core/test_cycle_fsm.py -v` → PASS.
Run: `uv run pytest && uv run ruff check && uv run ruff format --check && uv run mypy` → pass.

- [ ] **Step 5: Commit**

```bash
git add src/autotrader/core/cycle.py tests/unit/core/test_cycle_fsm.py
git commit -m "Cycle: FSM states, edges and the transition event

An explicit edge table turns an illegal transition into an error instead
of silent state drift, and keeps the edges only reconciliation may take
apart from the runner's.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Stub Decision Maker (`agents/stub.py`)

**Files:**
- Create: `src/autotrader/agents/stub.py`, `tests/unit/agents/__init__.py` (empty), `tests/unit/agents/test_stub.py`

**Interfaces:**
- Consumes: `RiskLimits` (`core/config.py`; fields `entry_slippage_bps: Decimal`, `default_win_rate_prior: Decimal`, `min_stop_atr`, `max_stop_atr: Decimal`); in tests `entry_limit` from `risk/checks.py`.
- Produces: `MIN_SCORE`, `STOP_ATR`, `TP_R`, `SIZE_FRACTION`, `CONFIDENCE_MARGIN` (Decimals); `@dataclass(frozen=True) StubBuy(stop, take_profit, confidence, size_fraction: Decimal)`; `@dataclass(frozen=True) StubSkip(reason: str)`; `decide(score: float, ask: Decimal, atr: Decimal, limits: RiskLimits) -> StubBuy | StubSkip`. Preconditions: `ask > 0`, `atr > 0` (ENRICHING guarantees them).

- [ ] **Step 1: Write the failing tests** (`tests/unit/agents/test_stub.py`)

```python
"""Phase-1 stub Decision Maker (spec §7)."""

from decimal import Decimal as D
from pathlib import Path

from hypothesis import assume, given
from hypothesis import strategies as st

from autotrader.agents.stub import STOP_ATR, StubBuy, StubSkip, decide
from autotrader.core.config import load_profile
from autotrader.risk.checks import entry_limit

LIMITS = load_profile(
    Path(__file__).resolve().parents[3] / "strategies" / "fast_momentum_v1.yaml"
).risk


def test_skips_below_min_score() -> None:
    got = decide(0.49, D("100"), D("2"), LIMITS)
    assert isinstance(got, StubSkip) and got.reason.startswith("score 0.49")


def test_buy_fields() -> None:
    got = decide(0.8, D("100.0"), D("2.0"), LIMITS)
    # est entry = 100 x 1.001 = 100.1; stop = 100.1 - 1.5 x 2; tp = 100.1 + 2 x 3
    assert got == StubBuy(
        stop=D("97.1"), take_profit=D("106.1"), confidence=D("0.50"), size_fraction=D("1")
    )


def test_skips_when_the_stop_would_not_be_positive() -> None:
    got = decide(0.8, D("1"), D("5"), LIMITS)
    assert isinstance(got, StubSkip) and "<= 0" in got.reason


def test_regression_tick_rounding_counterexample() -> None:
    # Review counterexample (2026-10-11): with the stop measured from the *rounded*
    # entry the distance exceeded 3 x ATR. Measured from ask x (1 + slip) it stays put.
    atr, tick = D("0.067"), D("0.01")
    got = decide(0.8, D("100.003"), atr, LIMITS)
    assert isinstance(got, StubBuy)
    distance = entry_limit(D("100.003"), LIMITS.entry_slippage_bps, tick) - got.stop
    assert STOP_ATR * atr <= distance < STOP_ATR * atr + tick
    assert LIMITS.min_stop_atr * atr <= distance <= LIMITS.max_stop_atr * atr


@given(
    ask=st.decimals(min_value=D("0.01"), max_value=D("200000"), places=4),
    atr=st.decimals(min_value=D("0.0001"), max_value=D("5000"), places=6),
    tick=st.sampled_from([D("0.0001"), D("0.001"), D("0.01"), D("0.1"), D("1")]),
)
def test_stop_distance_from_the_engines_rounded_entry_stays_in_band(
    ask: D, atr: D, tick: D
) -> None:
    assume(tick < (LIMITS.max_stop_atr - STOP_ATR) * atr)
    got = decide(0.8, ask, atr, LIMITS)
    assume(isinstance(got, StubBuy))
    assert isinstance(got, StubBuy)
    distance = entry_limit(ask, LIMITS.entry_slippage_bps, tick) - got.stop
    assert STOP_ATR * atr <= distance < STOP_ATR * atr + tick
    assert LIMITS.min_stop_atr * atr <= distance <= LIMITS.max_stop_atr * atr
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/agents/test_stub.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'autotrader.agents.stub'`.

- [ ] **Step 3: Implement `src/autotrader/agents/stub.py`**

```python
"""Rule-based stand-in for the Decision Maker in Phase 1 (SPEC §12, PLAN D15).

Module constants, not profile keys (owner decision, D15): this file is deleted when the
LLM Decision Maker lands in Phase 2, and the profile hash stays unchanged.

The stop is measured from `ask x (1 + entry_slippage_bps)`, the engine's entry before
tick rounding. The engine rounds that entry *up* by less than one tick, so for the same
ask its stop distance lies in [1.5 x ATR, 1.5 x ATR + tick) (spec §7).
"""

from dataclasses import dataclass
from decimal import Decimal

from autotrader.core.config import RiskLimits

MIN_SCORE = Decimal("0.5")
STOP_ATR = Decimal("1.5")
TP_R = Decimal("2")
SIZE_FRACTION = Decimal("1")
CONFIDENCE_MARGIN = Decimal("0.10")
_BPS = Decimal(10_000)


@dataclass(frozen=True)
class StubBuy:
    stop: Decimal
    take_profit: Decimal
    confidence: Decimal
    size_fraction: Decimal


@dataclass(frozen=True)
class StubSkip:
    reason: str


def decide(score: float, ask: Decimal, atr: Decimal, limits: RiskLimits) -> StubBuy | StubSkip:
    """BUY with a fixed ATR stop and a 2R target if the score is high enough.

    Requires `ask > 0` and `atr > 0` (the Cycle Runner's ENRICHING step ensures both).
    """
    if Decimal(str(score)) < MIN_SCORE:
        return StubSkip(f"score {score} < {MIN_SCORE}")
    est_entry = ask * (1 + limits.entry_slippage_bps / _BPS)
    stop = est_entry - STOP_ATR * atr
    if stop <= 0:
        return StubSkip(f"stop {stop} <= 0")
    take_profit = est_entry + TP_R * (est_entry - stop)
    confidence = min(Decimal(1), limits.default_win_rate_prior + CONFIDENCE_MARGIN)
    return StubBuy(stop, take_profit, confidence, SIZE_FRACTION)
```

- [ ] **Step 4: Run the tests, then the whole suite**

Run: `uv run pytest tests/unit/agents/test_stub.py -v` → PASS. If `test_buy_fields` fails on Decimal representation, note that `D("97.1000") == D("97.1")` holds, so only a value difference can fail it. Recheck the arithmetic rather than the literals.
Run: `uv run pytest && uv run ruff check && uv run ruff format --check && uv run mypy` → pass.

- [ ] **Step 5: Commit**

```bash
git add src/autotrader/agents/stub.py tests/unit/agents/__init__.py tests/unit/agents/test_stub.py
git commit -m "Agents: rule-based stub Decision Maker for Phase 1

Lets the cycle reach the Risk Engine without an LLM. The stop is taken
from the engine's unrounded entry so tick rounding can only widen it by
less than one tick, which keeps it inside the ATR band check 6 enforces.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: `CycleRunner` FSM execution (`core/cycle.py`, part 2)

**Files:**
- Modify: `src/autotrader/core/cycle.py` (append)
- Test: `tests/unit/core/test_cycle.py` (create; Tasks 6 and 7 append to it)

**Interfaces:**
- Consumes: Task 1 repo functions; Task 2 `EventBus.publish_tx`, `CommitUncertain`; Task 3 vocabulary; Task 4 `stub.decide`, `StubBuy`, `StubSkip`; `RiskEngine.check` result type `OrderResult` (`risk/engine.py`: `decision: RiskDecision`, `order_row_id`, `status: str | None`, `broker_order_id`); `EntryIntent` (`risk/types.py`); `compute_features` (`core/indicators.py`); `bind_cycle` (`core/logging.py`); `ControlStatus` (`core/kill.py`).
- Produces (later tasks use these exact names):
  - `class CycleMarketView(Protocol)`: `history(symbol, timeframe) -> list[Bar]`, `quote(symbol) -> Quote | None`, `last_price(symbol) -> float | None`, `is_stale(symbol) -> bool`
  - `class Gate(Protocol)`: `async status(now: datetime | None = None) -> ControlStatus`
  - `class EntryChecker(Protocol)`: `async check(intent: EntryIntent) -> OrderResult`
  - `@dataclass(frozen=True) CycleSnapshot(ask: Decimal, bid: Decimal | None, last_price: Decimal | None, atr: Decimal)` with `details() -> dict[str, Any]`
  - `Decision = StubBuy | StubSkip`; `DecisionMaker = Callable[[CandidateSignal, CycleSnapshot], Awaitable[Decision]]`
  - `@dataclass(frozen=True) CloseReport(abandoned: tuple[int, ...] = ())` (used in Task 7)
  - `CycleRunner(*, profile: StrategyProfile, strategy_id: int, market: CycleMarketView, gate: Gate, risk: EntryChecker, bus: EventBus, sessions: async_sessionmaker[AsyncSession], decide: DecisionMaker | None = None, trading_day_tz: str = "UTC", clock: Callable[[], datetime] = ..., step_timeout_s: float = 30.0)`
  - `CycleRunner.admission_suspended: bool` (property); `CycleRunner._run(cycle_id: int, candidate: CandidateSignal) -> None` (tests call it directly in this task); `CycleRunner.join() -> None`

- [ ] **Step 1: Write the failing tests** (`tests/unit/core/test_cycle.py`)

```python
"""Cycle Runner (WP 1.4; spec 2026-10-11-cycle-runner-design.md)."""

import asyncio
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from decimal import Decimal as D
from pathlib import Path
from typing import Any

import pytest
import yaml
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog.testing import capture_logs

from autotrader.agents.stub import StubBuy, StubSkip
from autotrader.core.bus import EventBus
from autotrader.core.config import StrategyProfile, parse_profile
from autotrader.core.cycle import CycleRunner, CycleSnapshot, Decision
from autotrader.core.kill import ControlGate
from autotrader.core.scanner import CandidateSignal
from autotrader.core.types import Bar, Quote
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade
from autotrader.risk.engine import OrderResult
from autotrader.risk.types import EntryIntent, RiskDecision
from tests.dbhooks import CommitHook, hooked_sessionmaker

PROFILE_PATH = Path(__file__).resolve().parents[3] / "strategies" / "fast_momentum_v1.yaml"
BTC, ETH, SOL = "BTC/USD", "ETH/USD", "SOL/USD"
NOW = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
FIVE = timedelta(minutes=5)


def profile(max_cycles: int = 2) -> StrategyProfile:
    data: dict[str, Any] = yaml.safe_load(PROFILE_PATH.read_text())
    data["scanner"] = {**data["scanner"], "max_concurrent_cycles": max_cycles}
    return parse_profile(data)


def bars(symbol: str, spread: float = 1.0) -> list[Bar]:
    """30 flat 5m bars ending at NOW; high - low = 2 x spread, so ATR14 = 2 x spread."""
    return [
        Bar(symbol=symbol, timeframe="5m", ts=NOW - (30 - i) * FIVE, open=100.0,
            high=100.0 + spread, low=100.0 - spread, close=100.0, volume=1.0)
        for i in range(30)
    ]


@dataclass
class Market:
    stale: set[str] = field(default_factory=set)
    quotes: dict[str, Quote | None] = field(
        default_factory=lambda: {s: Quote(symbol=s, ts=NOW, bid=99.98, ask=100.0)
                                 for s in (BTC, ETH, SOL)}
    )
    series: dict[str, list[Bar]] = field(
        default_factory=lambda: {s: bars(s) for s in (BTC, ETH, SOL)}
    )

    def history(self, symbol: str, timeframe: str) -> list[Bar]:
        return list(self.series.get(symbol, [])) if timeframe == "5m" else []

    def quote(self, symbol: str) -> Quote | None:
        return self.quotes.get(symbol)

    def last_price(self, symbol: str) -> float | None:
        q = self.quotes.get(symbol)
        return None if q is None else q.ask

    def is_stale(self, symbol: str) -> bool:
        return symbol in self.stale

    def spread_bps(self, symbol: str) -> float | None:  # for the real RiskEngine (Task 8)
        q = self.quotes.get(symbol)
        return None if q is None else (q.ask - q.bid) / q.ask * 10_000


class Clock:
    def __init__(self, now: datetime = NOW) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


def approved(status: str | None) -> OrderResult:
    return OrderResult(
        RiskDecision(approved=True, reasons=(), qty=D("1")), order_row_id=1, status=status,
        broker_order_id="b-1",
    )


class FakeRisk:
    """`EntryChecker` double: records calls, optionally blocks until `release` is set."""

    def __init__(self, result: OrderResult | Exception | None = None) -> None:
        self.result: OrderResult | Exception = result if result is not None else approved("new")
        self.calls: list[EntryIntent] = []
        self.release: asyncio.Event | None = None
        self.swallow_cancel = False
        self.started = asyncio.Event()
        self.finished = asyncio.Event()

    async def check(self, intent: EntryIntent) -> OrderResult:
        self.calls.append(intent)
        self.started.set()
        try:
            while self.release is not None:
                try:
                    await self.release.wait()
                    break
                except asyncio.CancelledError:
                    if not self.swallow_cancel:
                        raise
            if isinstance(self.result, Exception):
                raise self.result
            return self.result
        finally:
            self.finished.set()


def candidate(
    symbol: str = BTC, score: float = 0.8, bar_ts: datetime = NOW - FIVE
) -> CandidateSignal:
    return CandidateSignal(
        ts=NOW, symbol=symbol, direction="long", rule_id="r1", score=score, timeframe="5m",
        bar_ts=bar_ts, features={"close": 100.0, "vol_z": None},
    )


@pytest.fixture
def db_url(tmp_path: Path) -> str:
    url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    upgrade(url)
    return url


@pytest.fixture
async def sessions(db_url: str) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    engine = make_engine(db_url)
    yield make_sessionmaker(engine)
    await engine.dispose()


@pytest.fixture
async def bus(sessions: async_sessionmaker[AsyncSession]) -> AsyncIterator[EventBus]:
    b = EventBus(sessions)
    yield b
    await b.close()


@pytest.fixture
def hook() -> CommitHook:
    return CommitHook()


@pytest.fixture
async def hbus(db_url: str, hook: CommitHook) -> AsyncIterator[EventBus]:
    engine = make_engine(db_url)
    b = EventBus(hooked_sessionmaker(engine, hook))
    yield b
    await b.close()
    await engine.dispose()


@pytest.fixture
def gate(tmp_path: Path, sessions: async_sessionmaker[AsyncSession]) -> ControlGate:
    return ControlGate(tmp_path / "KILL", sessions)


@pytest.fixture
async def strategy_id(sessions: async_sessionmaker[AsyncSession]) -> int:
    async with sessions() as s:
        return (await repo.register_strategy(s, profile(), "yaml")).id


def make_runner(
    sessions: async_sessionmaker[AsyncSession],
    bus: EventBus,
    gate: Any,
    risk: Any,
    strategy_id: int,
    *,
    max_cycles: int = 2,
    market: Market | None = None,
    clock: Clock | None = None,
    decide: Callable[[CandidateSignal, CycleSnapshot], Awaitable[Decision]] | None = None,
    step_timeout_s: float = 30.0,
) -> CycleRunner:
    return CycleRunner(
        profile=profile(max_cycles), strategy_id=strategy_id, market=market or Market(),
        gate=gate, risk=risk, bus=bus, sessions=sessions, decide=decide,
        clock=clock or Clock(), step_timeout_s=step_timeout_s,
    )


async def new_cycle(
    sessions: async_sessionmaker[AsyncSession], strategy_id: int, symbol: str = BTC,
    state: str = "NEW",
) -> int:
    async with sessions() as s:
        row = await repo.insert_cycle(
            s, strategy_id=strategy_id, symbol=symbol, rule_id="r1", signal_features={},
            state=state, now=NOW,
        )
        await s.commit()
        return row.id


async def cycle_row(sessions: async_sessionmaker[AsyncSession], cycle_id: int) -> m.Cycle:
    async with sessions() as s:
        row = await s.get(m.Cycle, cycle_id, populate_existing=True)
        assert row is not None
        return row


async def transitions(
    sessions: async_sessionmaker[AsyncSession], cycle_id: int
) -> list[dict[str, Any]]:
    async with sessions() as s:
        rows = await s.scalars(
            select(m.Event)
            .where(m.Event.type == "CycleTransitioned", m.Event.cycle_id == cycle_id)
            .order_by(m.Event.id)
        )
        return [r.payload for r in rows]


def edges(payloads: list[dict[str, Any]]) -> list[tuple[str | None, str]]:
    return [(p["from_state"], p["to_state"]) for p in payloads]


# --- FSM (Task 5) --------------------------------------------------------------------

PATH_TO_SUBMITTED = [
    ("NEW", "ENRICHING"), ("ENRICHING", "ANALYZING"), ("ANALYZING", "DECIDING"),
    ("DECIDING", "RISK_CHECK"), ("RISK_CHECK", "SUBMITTED"),
]


async def test_approved_entry_ends_submitted_with_a_full_audit_trail(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, gate: ControlGate,
    strategy_id: int,
) -> None:
    risk = FakeRisk(approved("new"))
    runner = make_runner(sessions, bus, gate, risk, strategy_id)
    cid = await new_cycle(sessions, strategy_id)

    await runner._run(cid, candidate())

    assert (await cycle_row(sessions, cid)).state == "SUBMITTED"
    trail = await transitions(sessions, cid)
    assert edges(trail) == PATH_TO_SUBMITTED
    assert trail[2]["details"] == {"skipped": "no analyst in phase 1"}
    intent_details = trail[3]["details"]
    assert D(intent_details["stop"]) == D("97.1")
    assert D(intent_details["take_profit"]) == D("106.1")
    assert trail[4]["details"]["status"] == "new"
    (intent,) = risk.calls
    assert (intent.cycle_id, intent.symbol, intent.confidence) == (cid, BTC, D("0.50"))


@pytest.mark.parametrize(
    ("status", "final", "reason"),
    [
        ("filled", "FILLED", None),
        ("rejected", "REJECTED", "order rejected"),
        ("not_submitted", "REJECTED", "order not_submitted"),
        ("new", "SUBMITTED", None),
        ("unknown", "SUBMITTED", None),
        ("canceled", "SUBMITTED", None),  # an IOC cancel may follow a partial fill
    ],
)
async def test_order_status_mapping(
    status: str, final: str, reason: str | None,
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, gate: ControlGate,
    strategy_id: int,
) -> None:
    runner = make_runner(sessions, bus, gate, FakeRisk(approved(status)), strategy_id)
    cid = await new_cycle(sessions, strategy_id)
    await runner._run(cid, candidate())
    row = await cycle_row(sessions, cid)
    assert (row.state, row.reject_reason) == (final, reason)


async def test_risk_rejection_ends_rejected_with_the_first_reason(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, gate: ControlGate,
    strategy_id: int,
) -> None:
    rejected = OrderResult(RiskDecision(approved=False, reasons=("expectancy -1 <= 0", "x")))
    runner = make_runner(sessions, bus, gate, FakeRisk(rejected), strategy_id)
    cid = await new_cycle(sessions, strategy_id)
    await runner._run(cid, candidate())
    row = await cycle_row(sessions, cid)
    assert (row.state, row.reject_reason) == ("REJECTED", "expectancy -1 <= 0")


async def test_check_exception_is_error_and_never_retried(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, gate: ControlGate,
    strategy_id: int,
) -> None:
    risk = FakeRisk(RuntimeError("boom"))
    runner = make_runner(sessions, bus, gate, risk, strategy_id)
    cid = await new_cycle(sessions, strategy_id)
    await runner._run(cid, candidate())
    row = await cycle_row(sessions, cid)
    assert (row.state, row.reject_reason) == ("ERROR", "RISK_CHECK: RuntimeError")
    assert len(risk.calls) == 1


async def test_stub_skip_is_rejected(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, gate: ControlGate,
    strategy_id: int,
) -> None:
    risk = FakeRisk()
    runner = make_runner(sessions, bus, gate, risk, strategy_id)
    cid = await new_cycle(sessions, strategy_id)
    await runner._run(cid, candidate(score=0.3))
    row = await cycle_row(sessions, cid)
    assert row.state == "REJECTED"
    assert row.reject_reason is not None and row.reject_reason.startswith("decision: skip (")
    assert risk.calls == []


@pytest.mark.parametrize(
    ("tweak", "reason"),
    [
        ("stale", "symbol stale"),
        ("no_quote", "no ask"),
        ("zero_ask", "no ask"),
        ("nan_ask", "no ask"),
        ("no_bars", "ATR missing"),
        ("flat_bars", "ATR not positive"),
    ],
)
async def test_enrichment_failures_expire(
    tweak: str, reason: str, sessions: async_sessionmaker[AsyncSession], bus: EventBus,
    gate: ControlGate, strategy_id: int,
) -> None:
    market = Market()
    if tweak == "stale":
        market.stale.add(BTC)
    elif tweak == "no_quote":
        market.quotes[BTC] = None
    elif tweak == "zero_ask":
        market.quotes[BTC] = Quote(symbol=BTC, ts=NOW, bid=0.0, ask=0.0)
    elif tweak == "nan_ask":
        market.quotes[BTC] = Quote(symbol=BTC, ts=NOW, bid=99.0, ask=float("nan"))
    elif tweak == "no_bars":
        market.series[BTC] = []
    else:
        market.series[BTC] = bars(BTC, spread=0.0)
    risk = FakeRisk()
    runner = make_runner(sessions, bus, gate, risk, strategy_id, market=market)
    cid = await new_cycle(sessions, strategy_id)
    await runner._run(cid, candidate())
    row = await cycle_row(sessions, cid)
    assert (row.state, row.reject_reason) == ("EXPIRED", reason)
    assert risk.calls == []


async def test_signal_gone_stale_while_deciding_expires_without_check(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, gate: ControlGate,
    strategy_id: int,
) -> None:
    clock = Clock()

    async def slow_decide(c: CandidateSignal, snap: CycleSnapshot) -> Decision:
        clock.now = NOW + timedelta(minutes=6)  # > one period after the bar ended
        return StubBuy(stop=D("97"), take_profit=D("106"), confidence=D("0.5"),
                       size_fraction=D("1"))

    risk = FakeRisk()
    runner = make_runner(sessions, bus, gate, risk, strategy_id, clock=clock,
                         decide=slow_decide)
    cid = await new_cycle(sessions, strategy_id)
    await runner._run(cid, candidate())
    row = await cycle_row(sessions, cid)
    assert (row.state, row.reject_reason) == ("EXPIRED", "signal stale")
    assert edges(await transitions(sessions, cid))[-1] == ("RISK_CHECK", "EXPIRED")
    assert risk.calls == []


async def test_step_timeout_is_error_and_not_retried(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, gate: ControlGate,
    strategy_id: int,
) -> None:
    calls = 0

    async def hang(c: CandidateSignal, snap: CycleSnapshot) -> Decision:
        nonlocal calls
        calls += 1
        await asyncio.Event().wait()
        return StubSkip("unreachable")

    runner = make_runner(sessions, bus, gate, FakeRisk(), strategy_id, decide=hang,
                         step_timeout_s=0.01)
    cid = await new_cycle(sessions, strategy_id)
    await runner._run(cid, candidate())
    row = await cycle_row(sessions, cid)
    assert (row.state, row.reject_reason) == ("ERROR", "DECIDING: timeout")
    assert calls == 1


def fail_transition_to(
    monkeypatch: pytest.MonkeyPatch, to_state: str
) -> None:
    """The first write of a transition into `to_state` fails; later writes are real."""
    real = repo.transition_cycle
    failed = False

    async def flaky(session: AsyncSession, cycle_id: int, **kw: Any) -> None:
        nonlocal failed
        if kw["to_state"] == to_state and not failed:
            failed = True
            raise RuntimeError("disk I/O error")
        await real(session, cycle_id, **kw)

    monkeypatch.setattr(repo, "transition_cycle", flaky)


async def test_write_failure_before_check_falls_back_to_error(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, gate: ControlGate,
    strategy_id: int, monkeypatch: pytest.MonkeyPatch,
) -> None:
    fail_transition_to(monkeypatch, "ANALYZING")
    risk = FakeRisk()
    runner = make_runner(sessions, bus, gate, risk, strategy_id)
    cid = await new_cycle(sessions, strategy_id)
    await runner._run(cid, candidate())
    row = await cycle_row(sessions, cid)
    assert (row.state, row.reject_reason) == ("ERROR", "transition write failed")
    assert risk.calls == []


@pytest.mark.parametrize(
    ("status", "failing", "stays"),
    [("new", "SUBMITTED", "RISK_CHECK"), ("filled", "FILLED", "SUBMITTED")],
)
async def test_write_failure_after_check_keeps_the_last_committed_state(
    status: str, failing: str, stays: str,
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, gate: ControlGate,
    strategy_id: int, monkeypatch: pytest.MonkeyPatch,
) -> None:
    fail_transition_to(monkeypatch, failing)
    runner = make_runner(sessions, bus, gate, FakeRisk(approved(status)), strategy_id)
    cid = await new_cycle(sessions, strategy_id)
    with capture_logs() as logs:
        await runner._run(cid, candidate())
    assert (await cycle_row(sessions, cid)).state == stays
    assert all(p["to_state"] != "ERROR" for p in await transitions(sessions, cid))
    assert any(e["event"] == "cycle.stuck" for e in logs)


async def test_uncertain_commit_on_a_transition_suspends_admission_without_fallback(
    sessions: async_sessionmaker[AsyncSession], hbus: EventBus, hook: CommitHook,
    gate: ControlGate, strategy_id: int,
) -> None:
    runner = make_runner(sessions, hbus, gate, FakeRisk(), strategy_id)
    cid = await new_cycle(sessions, strategy_id)
    hook.fail_before = 1  # NEW -> ENRICHING does not commit; read-back sees NEW
    await runner._run(cid, candidate())
    assert runner.admission_suspended
    assert (await cycle_row(sessions, cid)).state == "NEW"
    assert await transitions(sessions, cid) == []  # no fallback ERROR write
```

Run `uv run ruff format tests/unit/core/test_cycle.py` after pasting.

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/core/test_cycle.py -v`
Expected: FAIL with `ImportError: cannot import name 'CycleRunner' from 'autotrader.core.cycle'`.

- [ ] **Step 3: Implement (append to `src/autotrader/core/cycle.py`)**

Extend the imports at the top of the file to:

```python
import asyncio
import functools
import math
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from enum import StrEnum
from typing import Any, Protocol, TypeVar

import structlog
from pydantic import Field
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from autotrader.agents import stub
from autotrader.agents.stub import StubBuy, StubSkip
from autotrader.core.bus import CommitUncertain, DomainEvent, EventBus
from autotrader.core.config import StrategyProfile
from autotrader.core.indicators import compute_features
from autotrader.core.kill import ControlStatus
from autotrader.core.logging import bind_cycle
from autotrader.core.resample import parse_timeframe
from autotrader.core.scanner import CandidateSignal
from autotrader.core.types import Bar, Quote
from autotrader.journal import repo
from autotrader.risk.engine import OrderResult
from autotrader.risk.types import EntryIntent

log = structlog.get_logger(__name__)
T = TypeVar("T")
```

Append:

```python
STEP_TIMEOUT_S = 30.0  # SPEC §4.3: e.g. an LLM call > 30 s -> ERROR
NO_ANALYST = {"skipped": "no analyst in phase 1"}


class CycleMarketView(Protocol):
    def history(self, symbol: str, timeframe: str) -> list[Bar]: ...
    def quote(self, symbol: str) -> Quote | None: ...
    def last_price(self, symbol: str) -> float | None: ...
    def is_stale(self, symbol: str) -> bool: ...


class Gate(Protocol):
    async def status(self, now: datetime | None = None) -> ControlStatus: ...


class EntryChecker(Protocol):
    async def check(self, intent: EntryIntent) -> OrderResult: ...


@dataclass(frozen=True)
class CycleSnapshot:
    ask: Decimal
    bid: Decimal | None
    last_price: Decimal | None
    atr: Decimal

    def details(self) -> dict[str, Any]:
        return {
            "ask": str(self.ask),
            "bid": None if self.bid is None else str(self.bid),
            "last_price": None if self.last_price is None else str(self.last_price),
            "atr": str(self.atr),
        }


Decision = StubBuy | StubSkip
DecisionMaker = Callable[[CandidateSignal, CycleSnapshot], Awaitable[Decision]]


@dataclass(frozen=True)
class CloseReport:
    abandoned: tuple[int, ...] = ()  # cycle ids whose task or check() was still running


@dataclass
class _Cursor:
    cycle_id: int
    state: CycleState


class _WriteFailed(Exception):
    """A transition write failed; the database error is the `__cause__`."""


def _dec(value: float | None) -> Decimal | None:
    return None if value is None or not math.isfinite(value) else Decimal(str(value))


async def _verify(session: AsyncSession, event: DomainEvent) -> bool:
    """Read-back after a commit error: is the cycle row in the event's target state?"""
    if not isinstance(event, CycleTransitioned) or event.cycle_id is None:
        raise TypeError(f"cannot verify {type(event).__name__}")
    return await repo.cycle_state(session, event.cycle_id) == event.to_state


def _consume(task: asyncio.Future[Any]) -> None:
    if not task.cancelled():
        task.exception()  # retrieved: a check() nobody awaits any more must not warn


class CycleRunner:
    def __init__(
        self,
        *,
        profile: StrategyProfile,
        strategy_id: int,
        market: CycleMarketView,
        gate: Gate,
        risk: EntryChecker,
        bus: EventBus,
        sessions: async_sessionmaker[AsyncSession],
        decide: DecisionMaker | None = None,
        trading_day_tz: str = "UTC",
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        step_timeout_s: float = STEP_TIMEOUT_S,
    ) -> None:
        self._profile = profile
        self._strategy_id = strategy_id
        self._market = market
        self._gate = gate
        self._risk = risk
        self._bus = bus
        self._sessions = sessions
        self._decide: DecisionMaker = decide or self._stub_decide
        self._tz = trading_day_tz
        self._clock = clock
        self._step_timeout = step_timeout_s
        self._admission_lock = asyncio.Lock()
        self._tasks: dict[int, asyncio.Task[None]] = {}
        self._checks: dict[int, asyncio.Future[OrderResult]] = {}  # cycles inside check()
        self._closing = False
        self._suspended = False

    @property
    def admission_suspended(self) -> bool:
        return self._suspended

    def _suspend(self, error: CommitUncertain) -> None:
        """`on_uncertain` callback: called by the bus-owned unit itself (D15)."""
        if not self._suspended:
            self._suspended = True
            log.error("cycle.admission_suspended", error=str(error))

    async def join(self) -> None:
        """Wait until every running cycle task has finished (tests; not a shutdown path)."""
        while self._tasks:
            await asyncio.gather(*list(self._tasks.values()), return_exceptions=True)
            await asyncio.sleep(0)  # let done-callbacks drop finished tasks

    # --- transitions -----------------------------------------------------------------

    async def _move(
        self,
        cycle_id: int,
        from_: CycleState,
        to: CycleState,
        details: dict[str, Any] | None = None,
        reason: str | None = None,
    ) -> None:
        check_edge(from_, to)  # ValueError: a bug, handled like any step error
        now = self._clock()
        event = CycleTransitioned(
            ts=now,
            cycle_id=cycle_id,
            from_state=from_,
            to_state=to,
            details={**(details or {}), **({"reason": reason} if reason else {})},
        )

        async def build(session: AsyncSession) -> DomainEvent:
            await repo.transition_cycle(
                session, cycle_id, from_state=from_, to_state=to, now=now, reason=reason
            )
            return event

        try:
            await self._bus.publish_tx(build, _verify, on_uncertain=self._suspend)
        except (CommitUncertain, asyncio.CancelledError):
            raise
        except Exception as e:
            raise _WriteFailed(f"{from_} -> {to}") from e
        log.info("cycle.transition", from_state=from_, to_state=to, reason=reason)

    async def _advance(
        self,
        cur: _Cursor,
        to: CycleState,
        details: dict[str, Any] | None = None,
        reason: str | None = None,
    ) -> None:
        await self._move(cur.cycle_id, cur.state, to, details, reason)
        cur.state = to

    # --- one cycle -------------------------------------------------------------------

    async def _run(self, cycle_id: int, candidate: CandidateSignal) -> None:
        with bind_cycle(cycle_id):
            cur = _Cursor(cycle_id, S.NEW)
            try:
                intent = await self._prepare(cur, candidate)
            except asyncio.CancelledError:
                raise
            except CommitUncertain:
                log.error("cycle.stuck", state=cur.state, reason="commit uncertain")
                return
            except Exception as e:
                if isinstance(e, _WriteFailed):
                    log.error("cycle.transition_failed", state=cur.state, error=repr(e.__cause__))
                    reason = "transition write failed"
                elif isinstance(e, TimeoutError):
                    log.error("cycle.step_timeout", state=cur.state)
                    reason = f"{cur.state}: timeout"
                else:
                    log.exception("cycle.step_failed", state=cur.state)
                    reason = f"{cur.state}: {type(e).__name__}"
                await self._fail(cur, reason)
                return
            if intent is not None:
                await self._risk_check(cur, intent)

    async def _fail(self, cur: _Cursor, reason: str) -> None:
        """General rule (§9): one attempt to write ERROR, else the row keeps its state."""
        try:
            await self._advance(cur, S.ERROR, reason=reason)
        except asyncio.CancelledError:
            raise
        except Exception:
            log.exception("cycle.stuck", state=cur.state, reason=reason)

    async def _timed(self, step: Awaitable[T]) -> T:
        async with asyncio.timeout(self._step_timeout):
            return await step

    async def _prepare(self, cur: _Cursor, candidate: CandidateSignal) -> EntryIntent | None:
        """NEW .. RISK_CHECK. Returns the intent, or None when the cycle already ended."""
        await self._advance(cur, S.ENRICHING)
        snap = await self._timed(self._enrich(candidate))
        if isinstance(snap, str):
            await self._advance(cur, S.EXPIRED, reason=snap)
            return None
        await self._advance(cur, S.ANALYZING, details=snap.details())
        await self._advance(cur, S.DECIDING, details=dict(NO_ANALYST))
        decision = await self._timed(self._decide(candidate, snap))
        if isinstance(decision, StubSkip):
            await self._advance(cur, S.REJECTED, reason=f"decision: skip ({decision.reason})")
            return None
        intent = EntryIntent(
            cycle_id=cur.cycle_id,
            symbol=candidate.symbol,
            stop=decision.stop,
            take_profit=decision.take_profit,
            confidence=decision.confidence,
            size_fraction=decision.size_fraction,
        )
        await self._advance(cur, S.RISK_CHECK, details=intent.model_dump(mode="json"))
        if signal_is_stale(candidate.bar_ts, candidate.timeframe, self._clock()):
            await self._advance(cur, S.EXPIRED, reason="signal stale")
            return None
        return intent

    async def _enrich(self, candidate: CandidateSignal) -> CycleSnapshot | str:
        """The snapshot, or the reason the cycle expires."""
        symbol = candidate.symbol
        if self._market.is_stale(symbol):
            return "symbol stale"
        quote = self._market.quote(symbol)
        ask = None if quote is None else _dec(quote.ask)
        if ask is None or ask <= 0:
            return "no ask"
        features = compute_features(
            self._market.history(symbol, self._profile.timeframes.signal),
            trading_day_tz=self._tz,
        )
        if not features["atr14"]:
            return "ATR missing"
        atr = _dec(features["atr14"][-1])
        if atr is None:
            return "ATR not finite"
        if atr <= 0:
            return "ATR not positive"
        return CycleSnapshot(
            ask=ask,
            bid=_dec(quote.bid) if quote is not None else None,
            last_price=_dec(self._market.last_price(symbol)),
            atr=atr,
        )

    async def _stub_decide(self, candidate: CandidateSignal, snap: CycleSnapshot) -> Decision:
        return stub.decide(candidate.score, snap.ask, snap.atr, self._profile.risk)

    async def _risk_check(self, cur: _Cursor, intent: EntryIntent) -> None:
        """RISK_CHECK onwards. `check()` once; after it, no fallback ERROR write (§9)."""
        check = asyncio.ensure_future(self._risk.check(intent))
        check.add_done_callback(_consume)
        self._checks[cur.cycle_id] = check
        try:
            try:
                result = await asyncio.shield(check)  # only close() may force it
            except asyncio.CancelledError:
                raise
            except Exception as e:
                log.exception("cycle.check_failed")
                await self._after_check(cur, S.ERROR, reason=f"RISK_CHECK: {type(e).__name__}")
                return
            decision = result.decision
            if not decision.approved:
                reason = decision.reasons[0] if decision.reasons else "rejected"
                await self._after_check(cur, S.REJECTED, reason=reason)
                return
            submitted = {
                "order_row_id": result.order_row_id,
                "status": result.status,
                "broker_order_id": result.broker_order_id,
                "qty": None if decision.qty is None else str(decision.qty),
            }
            if not await self._after_check(cur, S.SUBMITTED, details=submitted):
                return
            if result.status == "filled":
                await self._after_check(cur, S.FILLED)
            elif result.status in ("rejected", "not_submitted"):
                await self._after_check(cur, S.REJECTED, reason=f"order {result.status}")
            else:  # new / unknown / canceled: WP 1.5 reconciles from the broker's fill qty
                log.info("cycle.awaiting_order", status=result.status)
        finally:
            self._checks.pop(cur.cycle_id, None)

    async def _after_check(
        self,
        cur: _Cursor,
        to: CycleState,
        details: dict[str, Any] | None = None,
        reason: str | None = None,
    ) -> bool:
        try:
            await self._advance(cur, to, details, reason)
            return True
        except asyncio.CancelledError:
            raise
        except Exception:  # incl. CommitUncertain: the order row decides what happens
            log.exception("cycle.stuck", state=cur.state, to_state=to)
            return False
```

`_tasks`, `_closing` and the `parse_timeframe` import are used by Tasks 6 and 7. If ruff flags an unused import now, keep it out until the task that uses it (`parse_timeframe` is already used by `signal_is_stale`).

- [ ] **Step 4: Run the tests, then the whole suite**

Run: `uv run pytest tests/unit/core/test_cycle.py -v` → PASS.
Run: `uv run pytest && uv run ruff check && uv run ruff format --check && uv run mypy` → pass.

- [ ] **Step 5: Commit**

```bash
git add src/autotrader/core/cycle.py tests/unit/core/test_cycle.py
git commit -m "Cycle: run a cycle from NEW through the Risk Engine

Each step persists its transition with the event; check() runs once
under a shield and a failure after it never rewrites the cycle, because
the order row already records what reached the broker. Only outcomes that
prove the executed quantity end the cycle; the rest wait for
reconciliation.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Admission (G14) and startup recovery

**Files:**
- Modify: `src/autotrader/core/cycle.py` (add methods to `CycleRunner`)
- Test: `tests/unit/core/test_cycle.py` (append)

**Interfaces:**
- Consumes: Task 5 runner internals (`_move`, `_run`, `_suspend`, `_tasks`, `_closing`, `_suspended`, `_admission_lock`), `repo.open_cycle_symbols`, `repo.open_entry_orders`, `repo.insert_cycle`, `repo.open_cycles`, `repo.has_order_for_cycle`.
- Produces: `CycleRunner.on_candidate(event: CandidateSignal) -> None` (bus handler, never raises); `CycleRunner.recover() -> None`.

- [ ] **Step 1: Write the failing tests** (append to `tests/unit/core/test_cycle.py`)

Add `from sqlalchemy.exc import OperationalError`, `from autotrader.core.bus import CommitUncertain` and `from autotrader.core.kill import ControlStatus, write_kill_file` to the imports.

```python
# --- admission (Task 6) --------------------------------------------------------------


async def all_cycles(sessions: async_sessionmaker[AsyncSession]) -> list[m.Cycle]:
    async with sessions() as s:
        return list(await s.scalars(select(m.Cycle).order_by(m.Cycle.id)))


def dropped(logs: list[dict[str, Any]]) -> list[str]:
    return [e["reason"] for e in logs if e["event"] == "cycle.dropped"]


def hold_admission(monkeypatch: pytest.MonkeyPatch) -> tuple[asyncio.Event, asyncio.Event]:
    """The first admission blocks inside its transaction until `release` is set."""
    entered, release = asyncio.Event(), asyncio.Event()
    real = repo.open_cycle_symbols
    first = True

    async def held(session: AsyncSession) -> list[str]:
        nonlocal first
        if first:
            first = False
            entered.set()
            await release.wait()
        return await real(session)

    monkeypatch.setattr(repo, "open_cycle_symbols", held)
    return entered, release


async def test_admitted_candidate_gets_a_row_an_event_and_a_task(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, gate: ControlGate,
    strategy_id: int,
) -> None:
    runner = make_runner(sessions, bus, gate, FakeRisk(approved("new")), strategy_id)
    await runner.on_candidate(candidate())
    await runner.join()

    (row,) = await all_cycles(sessions)
    assert (row.symbol, row.rule_id, row.state) == (BTC, "r1", "SUBMITTED")
    assert row.signal_features["_timeframe"] == "5m"
    assert row.signal_features["_bar_ts"] == (NOW - FIVE).isoformat()
    first = (await transitions(sessions, row.id))[0]
    assert (first["from_state"], first["to_state"]) == (None, "NEW")


async def test_concurrency_cap_holds_for_simultaneous_candidates(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, gate: ControlGate,
    strategy_id: int, monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered, release = hold_admission(monkeypatch)
    runner = make_runner(sessions, bus, gate, FakeRisk(approved("new")), strategy_id,
                         max_cycles=1)
    with capture_logs() as logs:
        first = asyncio.create_task(runner.on_candidate(candidate(BTC)))
        await entered.wait()
        second = asyncio.create_task(runner.on_candidate(candidate(ETH)))
        for _ in range(20):
            await asyncio.sleep(0)
        release.set()
        await asyncio.gather(first, second)
        await runner.join()
    assert [c.symbol for c in await all_cycles(sessions)] == [BTC]
    assert dropped(logs) == ["max_concurrent_cycles"]


async def test_one_open_cycle_per_symbol(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, gate: ControlGate,
    strategy_id: int,
) -> None:
    runner = make_runner(sessions, bus, gate, FakeRisk(approved("new")), strategy_id)
    with capture_logs() as logs:
        await runner.on_candidate(candidate(BTC))
        await runner.on_candidate(candidate(BTC))
        await runner.join()
    assert len(await all_cycles(sessions)) == 1
    assert dropped(logs) == ["open cycle on symbol"]


async def test_open_entry_order_of_an_error_cycle_blocks_its_symbol(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, gate: ControlGate,
    strategy_id: int,
) -> None:
    cid = await new_cycle(sessions, strategy_id, state="ERROR")
    async with sessions() as s:
        await repo.insert_order(
            s, cycle_id=cid, client_order_id=f"at-c{cid}-e", symbol=BTC, mode="paper",
            type_="limit", side="buy", qty=D("1"), limit_price=D("100.1"), stop_price=None,
            tp_price=None, status="pending", now=NOW,
        )
    runner = make_runner(sessions, bus, gate, FakeRisk(), strategy_id)
    with capture_logs() as logs:
        await runner.on_candidate(candidate(BTC))
    assert dropped(logs) == ["open entry order on symbol"]
    assert len(await all_cycles(sessions)) == 1


async def test_stale_candidate_is_dropped(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, gate: ControlGate,
    strategy_id: int,
) -> None:
    runner = make_runner(sessions, bus, gate, FakeRisk(), strategy_id)
    with capture_logs() as logs:
        await runner.on_candidate(candidate(bar_ts=NOW - 3 * FIVE))
    assert dropped(logs) == ["signal stale"]
    assert await all_cycles(sessions) == []


class BrokenGate:
    def __init__(self, hang: bool) -> None:
        self.hang = hang

    async def status(self, now: datetime | None = None) -> ControlStatus:
        if self.hang:
            await asyncio.Event().wait()
        raise RuntimeError("control state unreadable")


@pytest.mark.parametrize(
    ("gate_kind", "reason_prefix"),
    [
        ("killed", "entries blocked"),
        ("raises", "gate unavailable: RuntimeError"),
        ("hangs", "gate unavailable: TimeoutError"),
    ],
)
async def test_gate_blocks_admission_fail_closed(
    gate_kind: str, reason_prefix: str, tmp_path: Path,
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, gate: ControlGate,
    strategy_id: int,
) -> None:
    use: Any = gate
    if gate_kind == "killed":
        write_kill_file(tmp_path / "KILL", source="cli", reason="test", now=NOW)
    else:
        use = BrokenGate(hang=gate_kind == "hangs")
    runner = make_runner(sessions, bus, use, FakeRisk(), strategy_id, step_timeout_s=0.01)
    with capture_logs() as logs:
        await runner.on_candidate(candidate())
    (reason,) = dropped(logs)
    assert reason.startswith(reason_prefix)
    assert await all_cycles(sessions) == []


async def test_db_error_at_admission_is_logged_not_raised(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, gate: ControlGate,
    strategy_id: int, monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def broken(*a: Any, **kw: Any) -> m.Cycle:
        raise OperationalError("INSERT", None, Exception("database is locked"))

    monkeypatch.setattr(repo, "insert_cycle", broken)
    runner = make_runner(sessions, bus, gate, FakeRisk(), strategy_id)
    with capture_logs() as logs:
        await runner.on_candidate(candidate())  # must not raise
    assert any(e["event"] == "cycle.admission_failed" for e in logs)
    assert await all_cycles(sessions) == []


async def test_uncertain_admission_suspends_and_a_late_landing_insert_never_breaks_the_cap(
    sessions: async_sessionmaker[AsyncSession], hbus: EventBus, hook: CommitHook,
    gate: ControlGate, strategy_id: int,
) -> None:
    risk = FakeRisk(approved("filled"))
    risk.release = asyncio.Event()
    runner = make_runner(sessions, hbus, gate, risk, strategy_id, max_cycles=2)

    await runner.on_candidate(candidate(SOL))  # a running cycle, held inside check()
    await risk.started.wait()

    hook.fail_before = 1  # BTC's insert does not commit; the read-back sees nothing
    with capture_logs() as logs:
        await runner.on_candidate(candidate(BTC))
        assert runner.admission_suspended
        await runner.on_candidate(candidate(ETH))  # would be admitted without suspension
    assert dropped(logs) == ["admission suspended: commit uncertain"]

    await new_cycle(sessions, strategy_id, symbol=BTC)  # the uncertain insert "lands"
    open_ = [c for c in await all_cycles(sessions) if c.state not in repo.TERMINAL_CYCLE_STATES]
    assert len(open_) <= 2

    risk.release.set()  # cycles already running are unaffected
    await runner.join()
    sol = next(c for c in await all_cycles(sessions) if c.symbol == SOL)
    assert sol.state == "FILLED"


async def test_suspension_is_checked_inside_the_admission_lock(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, gate: ControlGate,
    strategy_id: int, monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered, release = hold_admission(monkeypatch)
    runner = make_runner(sessions, bus, gate, FakeRisk(approved("new")), strategy_id)
    with capture_logs() as logs:
        first = asyncio.create_task(runner.on_candidate(candidate(BTC)))
        await entered.wait()
        second = asyncio.create_task(runner.on_candidate(candidate(ETH)))
        for _ in range(20):
            await asyncio.sleep(0)  # `second` passed the pre-lock checks and waits
        runner._suspend(CommitUncertain("elsewhere"))
        release.set()
        await asyncio.gather(first, second)
        await runner.join()
    assert [c.symbol for c in await all_cycles(sessions)] == [BTC]
    assert dropped(logs) == ["admission suspended: commit uncertain"]


# --- recovery (Task 6) ---------------------------------------------------------------


@pytest.mark.parametrize(
    ("state", "with_order", "after", "reason"),
    [
        ("NEW", False, "ERROR", "interrupted"),
        ("ENRICHING", False, "ERROR", "interrupted"),
        ("ANALYZING", False, "ERROR", "interrupted"),
        ("DECIDING", False, "ERROR", "interrupted"),
        ("RISK_CHECK", False, "ERROR", "interrupted"),
        ("RISK_CHECK", True, "RISK_CHECK", None),
        ("SUBMITTED", True, "SUBMITTED", None),
        ("FILLED", True, "FILLED", None),
    ],
)
async def test_recover(
    state: str, with_order: bool, after: str, reason: str | None,
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, gate: ControlGate,
    strategy_id: int,
) -> None:
    cid = await new_cycle(sessions, strategy_id, state=state)
    if with_order:
        async with sessions() as s:
            await repo.insert_order(
                s, cycle_id=cid, client_order_id=f"at-c{cid}-e", symbol=BTC, mode="paper",
                type_="limit", side="buy", qty=D("1"), limit_price=D("100.1"),
                stop_price=None, tp_price=None, status="pending", now=NOW,
            )
    await make_runner(sessions, bus, gate, FakeRisk(), strategy_id).recover()
    row = await cycle_row(sessions, cid)
    assert (row.state, row.reject_reason) == (after, reason)
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/core/test_cycle.py -k "admit or cap or symbol or stale_candidate or gate or db_error or uncertain_admission or inside_the_admission or recover" -v`
Expected: FAIL with `AttributeError: 'CycleRunner' object has no attribute 'on_candidate'` / `'recover'`.

- [ ] **Step 3: Implement (add to `CycleRunner`, after `join`)**

```python
    # --- admission (G14) -------------------------------------------------------------

    async def on_candidate(self, event: CandidateSignal) -> None:
        """Bus handler. Never raises: a failed admission drops the candidate."""
        try:
            await self._admit(event)
        except Exception:
            log.exception("cycle.admission_failed", symbol=event.symbol, rule_id=event.rule_id)

    def _blocked(self) -> str | None:
        if self._closing:
            return "shutting down"
        if self._suspended:
            return "admission suspended: commit uncertain"
        return None

    @staticmethod
    def _drop(event: CandidateSignal, reason: str) -> None:
        log.info("cycle.dropped", symbol=event.symbol, rule_id=event.rule_id, reason=reason)

    async def _admit(self, event: CandidateSignal) -> None:
        if (reason := self._blocked()) is not None:
            return self._drop(event, reason)
        now = self._clock()
        if signal_is_stale(event.bar_ts, event.timeframe, now):
            return self._drop(event, "signal stale")
        try:
            status = await self._timed(self._gate.status(now))
        except Exception as e:  # an unanswered gate is not an allowed gate
            return self._drop(event, f"gate unavailable: {type(e).__name__}")
        if not status.entries_allowed:
            return self._drop(event, f"entries blocked: {', '.join(status.reasons)}")
        async with self._admission_lock:
            if (reason := self._blocked()) is not None:  # re-checked inside the lock
                return self._drop(event, reason)
            why: list[str] = []

            async def build(session: AsyncSession) -> DomainEvent | None:
                open_symbols = await repo.open_cycle_symbols(session)
                if len(open_symbols) >= self._profile.scanner.max_concurrent_cycles:
                    why.append("max_concurrent_cycles")
                    return None
                if event.symbol in open_symbols:
                    why.append("open cycle on symbol")
                    return None
                if any(o.symbol == event.symbol for o in await repo.open_entry_orders(session)):
                    why.append("open entry order on symbol")
                    return None
                row = await repo.insert_cycle(
                    session,
                    strategy_id=self._strategy_id,
                    symbol=event.symbol,
                    rule_id=event.rule_id,
                    signal_features=signal_features(event),
                    state=S.NEW,
                    now=now,
                )
                return CycleTransitioned(
                    ts=now,
                    cycle_id=row.id,
                    from_state=None,
                    to_state=S.NEW,
                    details={"rule_id": event.rule_id, "score": event.score},
                )

            published = await self._bus.publish_tx(build, _verify, on_uncertain=self._suspend)
            if published is None or published.cycle_id is None:
                return self._drop(event, why[0] if why else "not admitted")
            cycle_id = published.cycle_id
            if self._closing:  # committed after close() began: recover() ends it
                log.warning("cycle.not_started", cycle_id=cycle_id, reason="shutting down")
                return None
            task = asyncio.create_task(self._run(cycle_id, event), name=f"cycle-{cycle_id}")
            self._tasks[cycle_id] = task
            task.add_done_callback(functools.partial(self._task_done, cycle_id))
        log.info("cycle.admitted", cycle_id=cycle_id, symbol=event.symbol, rule_id=event.rule_id)
        return None

    def _task_done(self, cycle_id: int, task: asyncio.Task[None]) -> None:
        self._tasks.pop(cycle_id, None)
        if not task.cancelled() and (exc := task.exception()) is not None:
            log.error("cycle.task_crashed", cycle_id=cycle_id, error=repr(exc))

    # --- recovery --------------------------------------------------------------------

    async def recover(self) -> None:
        """Startup: end cycles that provably sent nothing; leave the rest to WP 1.5."""
        async with self._sessions() as session:
            rows = await repo.open_cycles(session)
            with_order = {
                r.id for r in rows if await repo.has_order_for_cycle(session, r.id)
            }
        for row in rows:
            state = CycleState(row.state)
            with bind_cycle(row.id):
                if state in BEFORE_CHECK or (state is S.RISK_CHECK and row.id not in with_order):
                    await self._move(row.id, state, S.ERROR, reason="interrupted")
                else:
                    log.warning("cycle.awaiting_reconciliation", state=state)
```

`_admit` returns `None` on every path. mypy accepts `return self._drop(...)` because `_drop` returns `None`. ruff's `RET` rules are not enabled; if they flag anything, split each into `self._drop(...); return`.

- [ ] **Step 4: Run the tests, then the whole suite**

Run: `uv run pytest tests/unit/core/test_cycle.py -v` → PASS.
Run: `uv run pytest && uv run ruff check && uv run ruff format --check && uv run mypy` → pass.

- [ ] **Step 5: Commit**

```bash
git add src/autotrader/core/cycle.py tests/unit/core/test_cycle.py
git commit -m "Cycle: admission enforces the G14 caps; startup recovery

Candidates for several symbols arrive together, so the caps are checked
and the cycle inserted in one transaction under a lock. An open entry
order still blocks its symbol after its cycle ended in ERROR, and any
uncertain commit stops admission so a late-landing insert can't exceed
the cap. Recovery ends only cycles that provably sent nothing.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Bounded shutdown (`CycleRunner.close`)

**Files:**
- Modify: `src/autotrader/core/cycle.py` (add `close` to `CycleRunner`, constants near `STEP_TIMEOUT_S`)
- Test: `tests/unit/core/test_cycle.py` (append)

**Interfaces:**
- Consumes: `_tasks`, `_checks`, `_closing`, `CloseReport` (Task 5), admission (Task 6).
- Produces: `CLOSE_BUDGET_S = 20.0`, `CANCEL_WAIT_S = 3.0`, `CycleRunner.close(budget_s: float = CLOSE_BUDGET_S, cancel_wait_s: float = CANCEL_WAIT_S) -> CloseReport`.

- [ ] **Step 1: Write the failing tests** (append)

```python
# --- shutdown (Task 7) ---------------------------------------------------------------


async def test_close_cancels_a_cycle_before_check_and_recover_ends_it(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, gate: ControlGate,
    strategy_id: int,
) -> None:
    deciding = asyncio.Event()

    async def hang(c: CandidateSignal, snap: CycleSnapshot) -> Decision:
        deciding.set()
        await asyncio.Event().wait()
        return StubSkip("unreachable")

    runner = make_runner(sessions, bus, gate, FakeRisk(), strategy_id, decide=hang)
    await runner.on_candidate(candidate())
    await deciding.wait()
    report = await runner.close(budget_s=5)
    assert report.abandoned == ()
    (row,) = await all_cycles(sessions)
    assert row.state == "DECIDING"

    await make_runner(sessions, bus, gate, FakeRisk(), strategy_id).recover()
    assert (await cycle_row(sessions, row.id)).state == "ERROR"


async def test_close_drains_a_check_that_finishes_in_time(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, gate: ControlGate,
    strategy_id: int,
) -> None:
    risk = FakeRisk(approved("new"))
    risk.release = asyncio.Event()
    runner = make_runner(sessions, bus, gate, risk, strategy_id)
    await runner.on_candidate(candidate())
    await risk.started.wait()
    closing = asyncio.create_task(runner.close(budget_s=5, cancel_wait_s=1))
    for _ in range(20):
        await asyncio.sleep(0)
    assert not closing.done()  # draining the check
    risk.release.set()
    report = await closing
    assert report.abandoned == ()
    (row,) = await all_cycles(sessions)
    assert row.state == "SUBMITTED"


async def test_close_forces_a_check_past_the_drain_deadline(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, gate: ControlGate,
    strategy_id: int,
) -> None:
    risk = FakeRisk(approved("new"))
    risk.release = asyncio.Event()  # never set: check() only ends by cancellation
    runner = make_runner(sessions, bus, gate, risk, strategy_id)
    await runner.on_candidate(candidate())
    await risk.started.wait()
    with capture_logs() as logs:
        report = await runner.close(budget_s=0.05, cancel_wait_s=0.04)
    assert report.abandoned == ()
    assert any(e["event"] == "cycle.forced_cancel" for e in logs)
    (row,) = await all_cycles(sessions)
    assert row.state == "RISK_CHECK"  # recover() leaves it for reconciliation


async def test_close_reports_a_task_that_ignores_cancellation(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, gate: ControlGate,
    strategy_id: int,
) -> None:
    risk = FakeRisk(approved("new"))
    risk.release = asyncio.Event()
    risk.swallow_cancel = True
    runner = make_runner(sessions, bus, gate, risk, strategy_id)
    await runner.on_candidate(candidate())
    await risk.started.wait()
    with capture_logs() as logs:
        report = await runner.close(budget_s=0.05, cancel_wait_s=0.04)
    (cid,) = report.abandoned
    assert any(e["event"] == "cycle.task_abandoned" and e["cycle_id"] == cid for e in logs)
    risk.release.set()  # clean up the abandoned check
    await risk.finished.wait()


async def test_close_does_not_wait_for_an_admission_and_that_cycle_never_starts(
    sessions: async_sessionmaker[AsyncSession], bus: EventBus, gate: ControlGate,
    strategy_id: int, monkeypatch: pytest.MonkeyPatch,
) -> None:
    entered, release = hold_admission(monkeypatch)
    risk = FakeRisk()
    runner = make_runner(sessions, bus, gate, risk, strategy_id)
    admitting = asyncio.create_task(runner.on_candidate(candidate(BTC)))
    await entered.wait()
    report = await asyncio.wait_for(runner.close(budget_s=5), timeout=5)  # safety net only
    assert report.abandoned == ()
    with capture_logs() as logs:
        release.set()
        await admitting
        await runner.on_candidate(candidate(ETH))
    assert any(e["event"] == "cycle.not_started" for e in logs)
    assert dropped(logs) == ["shutting down"]
    (row,) = await all_cycles(sessions)
    assert row.state == "NEW" and risk.calls == []
    await make_runner(sessions, bus, gate, FakeRisk(), strategy_id).recover()
    assert (await cycle_row(sessions, row.id)).state == "ERROR"
```

- [ ] **Step 2: Run them to verify they fail**

Run: `uv run pytest tests/unit/core/test_cycle.py -k close -v`
Expected: FAIL with `AttributeError: 'CycleRunner' object has no attribute 'close'`.

- [ ] **Step 3: Implement**

Next to `STEP_TIMEOUT_S` add:

```python
CLOSE_BUDGET_S = 20.0  # whole runner.close(); the hard deadline is process-level (WP 1.5)
CANCEL_WAIT_S = 3.0  # part of the budget reserved for cancelled tasks to unwind
```

Add to `CycleRunner` (after `recover`):

```python
    # --- shutdown --------------------------------------------------------------------

    async def close(
        self, budget_s: float = CLOSE_BUDGET_S, cancel_wait_s: float = CANCEL_WAIT_S
    ) -> CloseReport:
        """Bounded, best-effort stop (§8). The whole call is bounded by `budget_s`.

        Never waits for the admission lock: admission re-checks `closing` after its
        commit and starts no task. Cycles before check() are cancelled; check() calls
        are drained until `deadline - cancel_wait_s`, then cancelled (safe: the order
        row is written before submit). Whatever still runs at the deadline is reported
        and left to process exit.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + budget_s
        self._closing = True
        for cycle_id, task in list(self._tasks.items()):
            if cycle_id not in self._checks:
                task.cancel()
        checks = dict(self._checks)
        if checks:
            drain = max(0.0, deadline - cancel_wait_s - loop.time())
            await asyncio.wait(checks.values(), timeout=drain)
        for cycle_id, check in checks.items():
            if not check.done():
                log.warning("cycle.forced_cancel", cycle_id=cycle_id)
                check.cancel()
                if (task := self._tasks.get(cycle_id)) is not None:
                    task.cancel()
        pending = [f for f in [*self._tasks.values(), *checks.values()] if not f.done()]
        if pending:
            await asyncio.wait(pending, timeout=max(0.0, deadline - loop.time()))
        abandoned = sorted(
            {cid for cid, t in self._tasks.items() if not t.done()}
            | {cid for cid, c in checks.items() if not c.done()}
        )
        for cycle_id in abandoned:
            log.error("cycle.task_abandoned", cycle_id=cycle_id)
        return CloseReport(tuple(abandoned))
```

- [ ] **Step 4: Run the tests, then the whole suite**

Run: `uv run pytest tests/unit/core/test_cycle.py -v` → PASS. Run it three times (`for i in 1 2 3; do uv run pytest tests/unit/core/test_cycle.py -q || break; done`) to catch interleaving flakiness. Every wait in these tests blocks on events; none depends on timing except the deliberate tiny close budgets.
Run: `uv run pytest && uv run ruff check && uv run ruff format --check && uv run mypy` → pass.

- [ ] **Step 5: Commit**

```bash
git add src/autotrader/core/cycle.py tests/unit/core/test_cycle.py
git commit -m "Cycle: bounded shutdown that never loses an order

close() drains in-flight risk checks for a grace period, then forces
them; forcing is safe because the order row is durable before submit.
Tasks that ignore cancellation are reported instead of awaited, so the
call has an upper bound; the hard stop belongs to the process (WP 1.5).

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Real-engine integration tests and docs alignment

**Files:**
- Create: `tests/integration/test_cycle_runner.py`
- Modify: `PLAN.md`, `SPEC.md` (§4.3), `CLAUDE.md`, `src/autotrader/core/scanner.py` (docstring only)

**Interfaces:**
- Consumes: everything above; `RiskEngine` (`risk/engine.py`, constructor as in `tests/unit/risk/test_engine.py::make_engine_`), `SimBroker` (`broker/sim.py`), `AssetRules`, `FeeSchedule` (`broker/base.py`).
- Produces: nothing new in code.

- [ ] **Step 1: Write the integration tests** (`tests/integration/test_cycle_runner.py`)

```python
"""Cycle Runner on a real RiskEngine + SimBroker (WP 1.4; spec §10).

SimBroker acks every accepted order `new` (fills happen on a later bar), so an approved
cycle ends in SUBMITTED here; completing it is WP 1.5's reconciliation.
"""

from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from decimal import Decimal as D
from pathlib import Path
from typing import Any

import pytest
import yaml
from sqlalchemy import select
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from structlog.testing import capture_logs

from autotrader.broker.base import AssetRules, FeeSchedule
from autotrader.broker.sim import SimBroker
from autotrader.core.bus import EventBus
from autotrader.core.config import StrategyProfile, parse_profile
from autotrader.core.cycle import CycleRunner
from autotrader.core.kill import ControlGate
from autotrader.core.scanner import CandidateSignal
from autotrader.core.types import Bar, Quote
from autotrader.journal import models as m
from autotrader.journal import repo
from autotrader.journal.db import make_engine, make_sessionmaker, upgrade
from autotrader.risk.engine import RiskEngine

PROFILE_PATH = Path(__file__).resolve().parents[2] / "strategies" / "fast_momentum_v1.yaml"
BTC = "BTC/USD"
NOW = datetime(2026, 10, 9, 12, 0, tzinfo=UTC)
FIVE = timedelta(minutes=5)
RULES = AssetRules(
    min_qty=D("0.0001"), qty_increment=D("0.0001"), min_notional=D("1"),
    price_increment=D("0.01"),
)
FEES = FeeSchedule(maker_bps=D("15"), taker_bps=D("25"))


def profile() -> StrategyProfile:
    data: dict[str, Any] = yaml.safe_load(PROFILE_PATH.read_text())
    return parse_profile(data)


def five(i: int) -> Bar:  # high - low = 2 -> ATR 2
    return Bar(symbol=BTC, timeframe="5m", ts=NOW - (30 - i) * FIVE, open=100.0,
               high=101.0, low=99.0, close=100.0, volume=1.0)


class View:
    """Feed stand-in for both the runner and the engine."""

    def history(self, symbol: str, timeframe: str) -> list[Bar]:
        return [five(i) for i in range(30)] if (symbol, timeframe) == (BTC, "5m") else []

    def quote(self, symbol: str) -> Quote | None:
        return Quote(symbol=BTC, ts=NOW, bid=99.98, ask=100.0) if symbol == BTC else None

    def last_price(self, symbol: str) -> float | None:
        return 100.0 if symbol == BTC else None

    def is_stale(self, symbol: str) -> bool:
        return False

    def spread_bps(self, symbol: str) -> float | None:
        return 2.0 if symbol == BTC else None


def candidate() -> CandidateSignal:
    return CandidateSignal(
        ts=NOW, symbol=BTC, direction="long", rule_id="breakout", score=0.8,
        timeframe="5m", bar_ts=NOW - FIVE, features={"close": 100.0},
    )


@pytest.fixture
async def sessions(tmp_path: Path) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    url = f"sqlite+aiosqlite:///{tmp_path / 'test.db'}"
    upgrade(url)
    engine = make_engine(url)
    yield make_sessionmaker(engine)
    await engine.dispose()


@pytest.fixture
async def wired(
    tmp_path: Path, sessions: async_sessionmaker[AsyncSession]
) -> AsyncIterator[tuple[CycleRunner, SimBroker]]:
    prof = profile()
    async with sessions() as s:
        strategy_id = (await repo.register_strategy(s, prof, "yaml")).id
    sim = SimBroker(cash=D("1000"), fees=FEES, slippage_bps=D("0"),
                    asset_rules={BTC: RULES}, timeframe="5m")
    sim.process_bar(five(29))
    gate = ControlGate(tmp_path / "KILL", sessions)
    view = View()
    engine = RiskEngine(sim, profile=prof, strategy_id=strategy_id, mode="paper", gate=gate,
                        market=view, sessions=sessions, clock=lambda: NOW)
    bus = EventBus(sessions)
    runner = CycleRunner(profile=prof, strategy_id=strategy_id, market=view, gate=gate,
                         risk=engine, bus=bus, sessions=sessions, clock=lambda: NOW)
    yield runner, sim
    await runner.close(budget_s=5)
    await bus.close()


async def test_candidate_reaches_the_broker_and_waits_in_submitted(
    wired: tuple[CycleRunner, SimBroker], sessions: async_sessionmaker[AsyncSession]
) -> None:
    runner, _ = wired
    await runner.on_candidate(candidate())
    await runner.join()
    async with sessions() as s:
        (cycle,) = list(await s.scalars(select(m.Cycle)))
        (order,) = list(await s.scalars(select(m.Order)))
        events = list(await s.scalars(
            select(m.Event).where(m.Event.cycle_id == cycle.id).order_by(m.Event.id)
        ))
    assert cycle.state == "SUBMITTED"
    assert (order.cycle_id, order.status, order.client_order_id) == (
        cycle.id, "new", f"at-c{cycle.id}-e",
    )
    assert [e.payload["to_state"] for e in events if e.type == "CycleTransitioned"] == [
        "NEW", "ENRICHING", "ANALYZING", "DECIDING", "RISK_CHECK", "SUBMITTED",
    ]


async def test_broker_accepts_then_the_ack_write_fails(
    wired: tuple[CycleRunner, SimBroker], sessions: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runner, sim = wired

    async def lost(*a: Any, **kw: Any) -> None:
        raise OperationalError("UPDATE orders", None, Exception("disk I/O error"))

    monkeypatch.setattr(repo, "update_order", lost)  # after submit_order() succeeded
    await runner.on_candidate(candidate())
    await runner.join()

    async with sessions() as s:
        (cycle,) = list(await s.scalars(select(m.Cycle)))
        (order,) = list(await s.scalars(select(m.Order)))
    assert (cycle.state, cycle.reject_reason) == ("ERROR", "RISK_CHECK: OperationalError")
    assert (order.status, order.client_order_id) == ("pending", f"at-c{cycle.id}-e")
    assert (await sim.get_order("sim-1")).status == "new"  # it really is at the broker

    with capture_logs() as logs:
        await runner.on_candidate(candidate())
    assert [e["reason"] for e in logs if e["event"] == "cycle.dropped"] == [
        "open entry order on symbol"
    ]
```

- [ ] **Step 2: Run them**

Run: `uv run pytest tests/integration/test_cycle_runner.py -v` → PASS. If the happy path is `REJECTED`, print `cycle.reject_reason`. The stub's numbers (stop 97.1, TP 106.1, confidence 0.5, ATR 2, ask 100) are meant to pass all 11 checks with these fixtures, and the reason names the check that disagrees. Fix the fixture, never the engine.

- [ ] **Step 3: Docs alignment** (this step only changes text; keep the edits exactly to these claims)

1. `PLAN.md`:
   - **"Current repo state" line:** replace "persists `CandidateSignal` events that nothing consumes until 1.4" with "persists `CandidateSignal` events". Replace "Next: WP 1.4 (Cycle Runner)." with "Cycle Runner done (WP 1.4, `core/cycle.py`, D15): admission with exact G14 caps, persisted FSM with a stub Decision Maker (`agents/stub.py`), recovery and bounded shutdown; not wired into `run core` until WP 1.5. Next: WP 1.5 (Position Manager)."
   - **1.4 row:** append "**Done 2026-10-11:** `core/cycle.py`, `agents/stub.py`, `EventBus.publish_tx` + bounded `close()`; design in `docs/superpowers/specs/2026-10-11-cycle-runner-design.md` (D15). Not subscribed in `run core` (WP 1.5)."
   - **1.5 row:** append "**From WP 1.4 (D15):** subscribe the Cycle Runner in `run core` after `recover()`; shutdown order: stop the feed, `runner.close(budget)`, then `bus.close(budget)`; process-level hard deadline (second SIGINT/SIGTERM or a `shutdown.timeout_s` watchdog → `os._exit(1)`). Complete `SUBMITTED` and `RISK_CHECK`-with-order cycles from the order's broker status via the reconciliation-only edges (`EXPIRED` only on a final status with filled qty 0; any fill, full or partial → `FILLED` and a managed position). Reconcile open entry orders whatever their cycle's state (an `ERROR` cycle can own a real `pending` order). SimBroker next-bar fill/cancel integration tests."
   - **1.6 row:** append "One core process per journal (the Cycle Runner's admission lock is per process, D15). Postgres recovery caveat (D15): a fresh pool doesn't prove that the old process's server-side `COMMIT` has finished. Before admission reopens, startup must establish that the previous process's transactions have resolved (e.g. none of its backends left in `pg_stat_activity`, or terminate them)."
   - **D10 row:** append "**2026-10-11 (WP 1.4 note):** with ~25 bps taker on both legs, check 11 needs roughly R ≳ 1 % of price (5m ATR ≳ 0.67 %), so most live-paper stub cycles will end `REJECTED: expectancy`; whether the strategy's timeframe/targets fit the fee level is a strategy question to settle with D10."
2. `SPEC.md` §4.3: append after the last bullet:
   `- *Note 2026-10-11 (WP 1.4, PLAN D15):* a stub/agent skip ends the cycle `REJECTED` (no SKIPPED state). `RISK_CHECK` and `SUBMITTED` have no timeout; only a bounded forced shutdown cancels `check()`, which is safe because the order row is written before submit. After submission only proven outcomes are mapped (`filled` → `FILLED`; `rejected`/`not_submitted` → `REJECTED`); `new`/`unknown`/`canceled` stay `SUBMITTED` until reconciliation, which sets `EXPIRED` only on a filled qty of 0 and `FILLED` on any fill. Orders are reconciled by order row, not by cycle state.`
3. `CLAUDE.md`, "Hard rules" / events bullet: change "Events subclass `DomainEvent` (`core/bus.py`) and go through `EventBus.publish()`, which persists before delivering." to "Events subclass `DomainEvent` (`core/bus.py`) and go through `EventBus.publish()`, which persists before delivering, or `EventBus.publish_tx(build, verify)` when a row write must commit atomically with its event (cycle transitions)."
4. `src/autotrader/core/scanner.py` module docstring: replace "The Cycle Runner (WP 1.4) enforces both caps when it creates a cycle (G14)." with "The Cycle Runner (`core/cycle.py`) enforces both caps authoritatively when it admits a cycle (G14, D15)."
5. Search for falsified claims and fix every hit:
   `rg -n "nothing consumes|until 1\.4|WP 1\.4|Cycle Runner" --glob '!docs/superpowers/**' -g '*.md' -g '*.py' .`
   Leave dated history entries (D-rows' dated text, the risk-engine spec) unchanged. Fix present-tense claims that are now false. Also run `rg -n "close\(\)" src/autotrader/core/bus.py` and confirm the docstring matches the bounded `close`.

- [ ] **Step 4: Full verification**

Run each separately and read its own exit status (no pipes):

```bash
uv run pytest
uv run ruff check
uv run ruff format --check
uv run mypy
```

Expected: all four pass. `uv run pytest -m smoke` is NOT required (no real-API change).

- [ ] **Step 5: Commit**

```bash
git add tests/integration/test_cycle_runner.py PLAN.md SPEC.md CLAUDE.md src/autotrader/core/scanner.py
git commit -m "Cycle: real-engine integration tests; align docs with WP 1.4

Proves a candidate reaches SimBroker through the real Risk Engine and
that an order accepted before a failed ack write keeps blocking its
symbol after its cycle errored. PLAN, SPEC and CLAUDE.md now describe the
runner as built and hand the reconciliation and wiring work to 1.5/1.6.

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Self-Review Notes (author)

- **Spec coverage:**
  - §4 FSM: Tasks 3 and 5.
  - §5 admission and freshness: Task 6 (freshness also in Task 5).
  - §6 bus: Task 2.
  - §7 stub: Task 4.
  - §8 recovery and shutdown: Tasks 6 and 7.
  - §9 error rules: Task 5 (general rule and exception), Task 6 (`CommitUncertain` at admission).
  - §10 tests: spread across Tasks 1–8.
  - §11 docs: Task 8.
- **Known deviation to confirm in review:** admission bounds `gate.status()` with `step_timeout_s` (Review Focus 2). The spec says a raising gate drops the candidate, and a hanging one is treated the same way.
- **Private access in tests:** Task 5 calls `runner._run()` and Task 6 calls `runner._suspend()` directly. Both are deliberate and documented. The FSM is tested before admission exists, and the suspension-inside-lock test needs the flag set at an exact point.
