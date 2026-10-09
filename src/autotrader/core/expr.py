"""Safe expression evaluator for config strings (PLAN.md G7).

Profile strings such as `signal_rules[].expr`, `score`, `promote_if` and `applies_to` are
parsed with `ast` and checked against a whitelist when the profile loads. Nothing is ever
passed to `eval()`/`exec()`; evaluation walks the checked tree.

Language:
- names from an allowed vocabulary; `x` is the current value, `x[-n]` the value n bars ago
- number/string/bool literals
- `+ - * /`, comparisons (chainable), `and`, `or`, `not`
- `min(a, b, ...)`, `max(a, b, ...)`, `abs(a)`
- `x in a..b` / `x not in a..b`: inclusive numeric range with literal bounds

Deliberately absent: `**` and `%` (`9**9**9` is a CPU/memory bomb), attribute access,
calls other than the three above, comprehensions, conditionals, slices.
"""

from __future__ import annotations

import ast
import operator
import re
import warnings
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, cast

Value = float | int | bool | str
Env = Mapping[str, Value | Sequence[float]]

MAX_SOURCE_LEN = 1000

_RANGE_FN = "__range__"
_NUM = r"-?\d+(?:\.\d+)?"
_RANGE_RE = re.compile(rf"\bin\s+({_NUM})\s*\.\.\s*({_NUM})")

_BIN_OPS: dict[type[ast.operator], Callable[[float, float], float]] = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
}
_CMP_OPS: dict[type[ast.cmpop], Callable[[Any, Any], bool]] = {
    ast.Eq: operator.eq,
    ast.NotEq: operator.ne,
    ast.Lt: operator.lt,
    ast.LtE: operator.le,
    ast.Gt: operator.gt,
    ast.GtE: operator.ge,
}
_ORDERING_OPS = (ast.Lt, ast.LtE, ast.Gt, ast.GtE)
_FUNCS: dict[str, Callable[..., float]] = {"min": min, "max": max, "abs": abs}


class ExpressionError(ValueError):
    """Expression is not valid; raised at compile (profile load) time."""


class EvaluationError(ValueError):
    """Expression could not be evaluated against the given environment."""


def _is_number(v: object) -> bool:
    return isinstance(v, int | float) and not isinstance(v, bool)


@dataclass(frozen=True)
class Expression:
    source: str
    _tree: ast.Expression = field(repr=False, compare=False)

    @classmethod
    def compile(cls, source: str, *, allowed_names: frozenset[str]) -> Expression:
        def fail(reason: str) -> ExpressionError:
            return ExpressionError(f"invalid expression {source!r}: {reason}")

        if len(source) > MAX_SOURCE_LEN:
            raise fail(f"longer than {MAX_SOURCE_LEN} characters")
        if _RANGE_FN in source:
            raise fail(f"{_RANGE_FN} is reserved")
        rewritten = _RANGE_RE.sub(rf"in {_RANGE_FN}(\1, \2)", source)
        try:
            with warnings.catch_warnings():
                # e.g. `1and x` parses but warns; ambiguous input is an error here.
                warnings.simplefilter("error", SyntaxWarning)
                tree = ast.parse(rewritten, mode="eval")
        except (SyntaxError, SyntaxWarning) as e:
            raise fail(f"syntax error: {e}") from None
        try:
            _Checker(allowed_names).check(tree.body)
        except _Reject as e:
            raise fail(str(e)) from None
        return cls(source=source, _tree=tree)

    def evaluate(self, env: Env) -> Value:
        return _eval(self._tree.body, env)

    def evaluate_bool(self, env: Env) -> bool:
        result = self.evaluate(env)
        if not isinstance(result, bool):
            raise EvaluationError(f"{self.source!r}: expected bool, got {result!r}")
        return result

    def evaluate_number(self, env: Env) -> float:
        result = self.evaluate(env)
        if not _is_number(result):
            raise EvaluationError(f"{self.source!r}: expected number, got {result!r}")
        return float(result)


# --- compile-time whitelist --------------------------------------------------------


class _Reject(Exception):
    pass


def _numeric_literal(node: ast.expr) -> float | None:
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.USub):
        inner = _numeric_literal(node.operand)
        return None if inner is None else -inner
    if isinstance(node, ast.Constant) and _is_number(node.value):
        return float(cast(float, node.value))
    return None


def _range_bounds(node: ast.expr) -> tuple[float, float] | None:
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Name)
        and node.func.id == _RANGE_FN
        and len(node.args) == 2
    ):
        lo, hi = (_numeric_literal(a) for a in node.args)
        if lo is not None and hi is not None:
            return lo, hi
    return None


def _lag(node: ast.Subscript) -> int | None:
    s = node.slice
    if (
        isinstance(s, ast.UnaryOp)
        and isinstance(s.op, ast.USub)
        and isinstance(s.operand, ast.Constant)
        and type(s.operand.value) is int
        and s.operand.value >= 1
    ):
        return s.operand.value
    return None


@dataclass(frozen=True)
class _Checker:
    allowed_names: frozenset[str]

    def check(self, node: ast.expr) -> None:
        match node:
            case ast.BoolOp(values=values):
                for v in values:
                    self.check(v)
            case ast.UnaryOp(op=ast.Not() | ast.USub() | ast.UAdd(), operand=operand):
                self.check(operand)
            case ast.BinOp(left=left, op=bin_op, right=right) if type(bin_op) in _BIN_OPS:
                self.check(left)
                self.check(right)
            case ast.Compare(left=left, ops=ops, comparators=comparators):
                self.check(left)
                for cmp_op, comp in zip(ops, comparators, strict=True):
                    is_range = _range_bounds(comp) is not None
                    if isinstance(cmp_op, ast.In | ast.NotIn):
                        if not is_range:
                            raise _Reject("`in` requires a literal range `a..b`")
                    elif type(cmp_op) in _CMP_OPS and not is_range:
                        self.check(comp)
                    else:
                        raise _Reject(f"unsupported comparison {type(cmp_op).__name__}")
            case ast.Name(id=name):
                if name not in self.allowed_names:
                    raise _Reject(f"unknown name {name!r}")
            case ast.Constant(value=value):
                if type(value) not in (int, float, bool, str):
                    raise _Reject(f"unsupported literal {value!r}")
            case ast.Call(func=ast.Name(id=fn), args=args, keywords=[]) if fn in _FUNCS:
                arity_ok = len(args) == 1 if fn == "abs" else len(args) >= 2
                if not arity_ok or any(isinstance(a, ast.Starred) for a in args):
                    raise _Reject(f"bad arguments to {fn}()")
                for a in args:
                    self.check(a)
            case ast.Subscript(value=ast.Name() as value):
                if _lag(node) is None:
                    raise _Reject("only lag access `name[-n]` with literal n >= 1")
                self.check(value)
            case _:
                raise _Reject(f"unsupported syntax {type(node).__name__}")


# --- evaluation --------------------------------------------------------------------


def _resolve(name: str, env: Env, lag: int = 0) -> Value:
    try:
        raw = env[name]
    except KeyError:
        raise EvaluationError(f"{name!r} not provided") from None
    if isinstance(raw, str | int | float):
        if lag:
            raise EvaluationError(f"{name!r} has no history for lag [-{lag}]")
        return raw
    if len(raw) <= lag:
        raise EvaluationError(f"{name!r} has {len(raw)} values, need {lag + 1}")
    return raw[-1 - lag]


def _number(v: Value) -> float:
    if not _is_number(v):
        raise EvaluationError(f"expected number, got {v!r}")
    return float(v)


def _boolean(v: Value) -> bool:
    if not isinstance(v, bool):
        raise EvaluationError(f"expected bool, got {v!r}")
    return v


def _compare(op: ast.cmpop, a: Value, b: Value) -> bool:
    if (_is_number(a) and _is_number(b)) or (isinstance(a, str) and isinstance(b, str)):
        return _CMP_OPS[type(op)](a, b)
    if isinstance(a, bool) and isinstance(b, bool) and not isinstance(op, _ORDERING_OPS):
        return _CMP_OPS[type(op)](a, b)
    raise EvaluationError(f"cannot compare {a!r} with {b!r}")


def _eval(node: ast.expr, env: Env) -> Value:
    match node:
        case ast.Constant(value=value):
            return cast(Value, value)  # whitelisted to int/float/bool/str by _Checker
        case ast.Name(id=name):
            return _resolve(name, env)
        case ast.Subscript(value=ast.Name(id=name)):
            return _resolve(name, env, lag=_lag(node) or 0)
        case ast.BoolOp(op=ast.And(), values=values):
            return all(_boolean(_eval(v, env)) for v in values)
        case ast.BoolOp(op=ast.Or(), values=values):
            return any(_boolean(_eval(v, env)) for v in values)
        case ast.UnaryOp(op=ast.Not(), operand=operand):
            return not _boolean(_eval(operand, env))
        case ast.UnaryOp(op=ast.USub(), operand=operand):
            return -_number(_eval(operand, env))
        case ast.UnaryOp(op=ast.UAdd(), operand=operand):
            return _number(_eval(operand, env))
        case ast.BinOp(left=left, op=bin_op, right=right):
            a, b = _number(_eval(left, env)), _number(_eval(right, env))
            if isinstance(bin_op, ast.Div) and b == 0:
                raise EvaluationError("division by zero")
            return _BIN_OPS[type(bin_op)](a, b)
        case ast.Compare(left=left, ops=ops, comparators=comparators):
            current = _eval(left, env)
            for cmp_op, comp in zip(ops, comparators, strict=True):
                bounds = _range_bounds(comp)
                if bounds is not None:
                    inside = bounds[0] <= _number(current) <= bounds[1]
                    if inside is isinstance(cmp_op, ast.NotIn):
                        return False
                    continue
                rhs = _eval(comp, env)
                if not _compare(cmp_op, current, rhs):
                    return False
                current = rhs
            return True
        case ast.Call(func=ast.Name(id=fn), args=args):
            return _FUNCS[fn](*(_number(_eval(a, env)) for a in args))
    raise AssertionError(f"unchecked node {ast.dump(node)}")  # pragma: no cover
