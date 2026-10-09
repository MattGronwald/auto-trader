"""Name vocabularies for profile expressions.

Profile expressions are validated against these sets at load time, so a typo in a signal
rule fails at startup instead of silently never firing. The indicator pipeline (WP 1.1)
and the evidence computation (WP 4.2) must provide exactly these names.
"""

# Per-bar features available to `signal_rules[].expr` and `.score` (SPEC §4.1).
SIGNAL_FEATURES = frozenset(
    {
        "open",
        "high",
        "low",
        "close",
        "volume",
        "ema9",
        "ema21",
        "ema50",
        "rsi14",
        "atr14",
        "vwap",
        "vol_z",
        "bb_upper",
        "bb_middle",
        "bb_lower",
        "donchian_high_20",
        "donchian_low_20",
    }
)

# Hypothesis evidence available to `learning.promote_if` (SPEC §7 `hypotheses`, §8.2).
EVIDENCE_FIELDS = frozenset({"trades_n", "win_rate", "avg_r", "p_value", "delta_expectancy_r"})
