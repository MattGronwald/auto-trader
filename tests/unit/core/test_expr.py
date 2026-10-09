import contextlib

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from autotrader.core.expr import Env, EvaluationError, Expression, ExpressionError

SERIES_NAMES = frozenset({"close", "low", "donchian_high_20", "vol_z", "ema9", "ema21"})


def compile_(source: str, names: frozenset[str] = SERIES_NAMES) -> Expression:
    return Expression.compile(source, allowed_names=names)


# --- the expressions from SPEC §6 -------------------------------------------------


def test_spec_breakout_rule_uses_previous_bar_for_lag() -> None:
    expr = compile_("close > donchian_high_20[-1] and vol_z > 1.5 and ema9 > ema21")
    env: Env = {
        "close": [100.0, 105.0],
        "donchian_high_20": [104.0, 105.0],  # current bar includes the breakout close
        "vol_z": 2.0,
        "ema9": 101.0,
        "ema21": 100.0,
    }

    assert expr.evaluate_bool(env) is True


def test_spec_score_expression() -> None:
    expr = compile_("min(1, vol_z / 3)")

    assert expr.evaluate_number({"vol_z": 1.5}) == pytest.approx(0.5)
    assert expr.evaluate_number({"vol_z": 6.0}) == 1


def test_spec_constant_score() -> None:
    assert compile_("0.5").evaluate_number({}) == 0.5


def test_spec_promote_if() -> None:
    expr = Expression.compile(
        "p_value < 0.1 and delta_expectancy_r > 0.15",
        allowed_names=frozenset({"p_value", "delta_expectancy_r"}),
    )

    assert expr.evaluate_bool({"p_value": 0.05, "delta_expectancy_r": 0.2}) is True
    assert expr.evaluate_bool({"p_value": 0.2, "delta_expectancy_r": 0.2}) is False


# --- language features ------------------------------------------------------------


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("hour_utc in 14..16", True),  # upper bound inclusive
        ("hour_utc in 16..20", True),  # lower bound inclusive
        ("hour_utc in 17..20", False),
        ("hour_utc not in 14..16", False),
        ("hour_utc in 0.5..16.5", True),
        ("hour_utc in -5..16", True),  # negative bound
        ("hour_utc in -20..-5", False),
    ],
)
def test_inclusive_range(source: str, expected: bool) -> None:
    expr = Expression.compile(source, allowed_names=frozenset({"hour_utc"}))

    assert expr.evaluate_bool({"hour_utc": 16}) is expected


@pytest.mark.parametrize(
    ("source", "expected"),
    [
        ("1 + 2 * 3 - 4 / 2", 5.0),
        ("-close + 10", 8.0),
        ("abs(-3)", 3.0),
        ("max(1, 2, 3)", 3.0),
        ("(1 + 2) * 3", 9.0),
    ],
)
def test_arithmetic(source: str, expected: float) -> None:
    assert compile_(source).evaluate_number({"close": 2.0}) == pytest.approx(expected)


def test_boolean_ops_short_circuit() -> None:
    # Right side would fail (insufficient history) but must not be evaluated.
    expr = compile_("close > 100 and close[-5] > 0")

    assert expr.evaluate_bool({"close": [1.0]}) is False


def test_not_and_or() -> None:
    assert compile_("not close > 1 or close == 1").evaluate_bool({"close": 1.0}) is True


def test_string_equality() -> None:
    expr = Expression.compile('rule_id == "pullback"', allowed_names=frozenset({"rule_id"}))

    assert expr.evaluate_bool({"rule_id": "pullback"}) is True
    assert expr.evaluate_bool({"rule_id": "breakout"}) is False


def test_bool_equality() -> None:
    expr = Expression.compile("flag == True", allowed_names=frozenset({"flag"}))

    assert expr.evaluate_bool({"flag": True}) is True


def test_bool_ordering_rejected() -> None:
    expr = Expression.compile("flag > False", allowed_names=frozenset({"flag"}))

    with pytest.raises(EvaluationError):
        expr.evaluate({"flag": True})


def test_chained_comparison() -> None:
    assert compile_("1 < close < 3").evaluate_bool({"close": 2.0}) is True


def test_source_is_kept() -> None:
    assert compile_("close > 1").source == "close > 1"


# --- rejected at compile time (fail fast at profile load) --------------------------


@pytest.mark.parametrize(
    "source",
    [
        "__import__('os').system('true')",
        "close.__class__",
        "open('x')",
        "[c for c in close]",
        "lambda: 1",
        "close ** 2",  # pow: 9**9**9 is a CPU/memory bomb
        "close % 2",
        "close if close else 1",
        "close[0]",  # only [-n] lag access
        "close[-0]",
        "close[1]",
        "close[-1.5]",
        "close[ema9]",
        "close[-1:]",
        "close in (1, 2)",  # `in` only with a..b ranges
        "close is 1",
        "close is not 1",
        "max(close, key=abs)",
        "max(*close)",
        "min()",
        "close = 1",
        "close; ema9",
        "x > 1",  # unknown name: typo protection
        "__range__(1, 2)",
        "",
        "close >",
        "close in 1..",
        "None",
        "1and close",  # parses with a SyntaxWarning
        "True and None",
        " + ".join(["close"] * 200),  # over the length limit
    ],
)
def test_rejected_at_compile(source: str) -> None:
    with pytest.raises(ExpressionError):
        compile_(source)


def test_compile_error_names_the_expression() -> None:
    with pytest.raises(ExpressionError, match="x > 1"):
        compile_("x > 1")


# --- runtime errors are typed, never raw Python exceptions -----------------------


@pytest.mark.parametrize(
    ("source", "env"),
    [
        ("close[-3] > 1", {"close": [1.0, 2.0]}),  # insufficient history
        ("close[-1] > 1", {"close": 1.0}),  # lag on a scalar
        ("close / ema9 > 1", {"close": 1.0, "ema9": 0.0}),
        ("close > 1", {}),  # missing from env
        ("close > 1", {"close": []}),  # empty series
        ('close > "a"', {"close": 1.0}),
        ("close + 1 > 1", {"close": "a"}),
        ("close and ema9", {"close": 1.0, "ema9": 1.0}),  # and/or require bools
        ("not close", {"close": 1.0}),
        ("close + ema9 > 1", {"close": True, "ema9": 1.0}),  # bools are not numbers
    ],
)
def test_runtime_errors(source: str, env: dict[str, object]) -> None:
    with pytest.raises(EvaluationError):
        compile_(source).evaluate(env)  # type: ignore[arg-type]


def test_result_type_is_enforced() -> None:
    with pytest.raises(EvaluationError):
        compile_("close + 1").evaluate_bool({"close": 1.0})
    with pytest.raises(EvaluationError):
        compile_("close > 1").evaluate_number({"close": 2.0})


# --- fuzz: compile never raises anything but ExpressionError ----------------------

_ALPHABET = "closema9[]-+*/<>=!().,_ andortinx0123456789\"'"


@given(st.text(alphabet=_ALPHABET, max_size=60))
@settings(max_examples=2000)
def test_compile_only_raises_expression_error(source: str) -> None:
    try:
        expr = compile_(source)
    except ExpressionError:
        return
    with contextlib.suppress(EvaluationError):
        expr.evaluate({"close": [1.0, 2.0], "ema9": 1.0})
