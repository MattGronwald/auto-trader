"""FSM vocabulary of the Cycle Runner (spec 2026-10-11-cycle-runner-design.md §4)."""

import json
from datetime import UTC, datetime, timedelta

import pytest

from autotrader.core.cycle import (
    RECONCILIATION_ONLY_EDGES,
    RUNNER_EDGES,
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
    assert {(S.SUBMITTED, S.EXPIRED), (S.RISK_CHECK, S.FILLED)} == RECONCILIATION_ONLY_EDGES
    for from_, to in [*RECONCILIATION_ONLY_EDGES, (S.SUBMITTED, S.FILLED)]:
        check_edge(from_, to, reconciliation=True)


def _accepted(*, reconciliation: bool) -> set[tuple[CycleState | None, CycleState]]:
    accepted: set[tuple[CycleState | None, CycleState]] = set()
    for from_ in (None, *S):
        for to in S:
            try:
                check_edge(from_, to, reconciliation=reconciliation)
            except ValueError:
                continue
            accepted.add((from_, to))
    return accepted


def test_check_edge_accepts_exactly_the_declared_edges() -> None:
    assert not RUNNER_EDGES & RECONCILIATION_ONLY_EDGES
    runner: set[tuple[CycleState | None, CycleState]] = {*RUNNER_EDGES, (None, S.NEW)}
    assert _accepted(reconciliation=False) == runner
    assert _accepted(reconciliation=True) == runner | RECONCILIATION_ONLY_EDGES


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
        ts=NOW,
        symbol="BTC/USD",
        direction="long",
        rule_id="r1",
        score=0.7,
        timeframe="5m",
        bar_ts=NOW - timedelta(minutes=5),
        features={"close": 100.0, "vol_z": None},
    )
    got = signal_features(c)
    assert got == {
        "close": 100.0,
        "vol_z": None,
        "_timeframe": "5m",
        "_bar_ts": "2026-10-09T11:55:00+00:00",
    }
    json.dumps(got)  # valid JSON for the cycles.signal_features column


def test_cycle_transitioned_round_trips_through_its_payload() -> None:
    e = CycleTransitioned(
        ts=NOW,
        cycle_id=7,
        from_state=S.DECIDING,
        to_state=S.RISK_CHECK,
        details={"stop": "97.1000"},
    )
    payload = e.payload()  # must stay the method: the field is `details`, not `payload`
    assert payload == {
        "from_state": "DECIDING",
        "to_state": "RISK_CHECK",
        "details": {"stop": "97.1000"},
    }
    back = CycleTransitioned.model_validate({**payload, "ts": NOW, "cycle_id": 7})
    assert back == e
