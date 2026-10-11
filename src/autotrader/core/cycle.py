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
