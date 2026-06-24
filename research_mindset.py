"""Explainability and uncertainty contracts for research signals."""

from typing import Dict, Iterable


DEFAULT_REGIME_WEAKNESSES = {
    "momentum": ["Sideways", "Crash"],
    "value": ["Bull Trend", "Recovery"],
    "quality": ["Speculative Bull Trend"],
    "growth": ["High Volatility", "Rising-rate regime"],
    "low_volatility": ["Sharp Recovery"],
    "pairs_trading": ["Crash", "Structural break", "Liquidity shock"],
    "breakout": ["Sideways", "High Volatility"],
}


def research_envelope(
    signal_name: str,
    thesis: str,
    evidence: Iterable[str],
    assumptions: Iterable[str],
    failure_modes: Iterable[str],
    confidence: float,
    robustness: Dict | None = None,
    regime_weaknesses: Iterable[str] | None = None,
) -> Dict:
    """Return the minimum evidence package required for any actionable signal."""
    confidence = max(0.0, min(100.0, float(confidence)))
    robustness = robustness or {}
    weaknesses = list(regime_weaknesses or DEFAULT_REGIME_WEAKNESSES.get(signal_name, []))
    return {
        "signal": signal_name,
        "thesis": thesis,
        "evidence": [str(item) for item in evidence if item],
        "assumptions": [str(item) for item in assumptions if item],
        "failure_modes": [str(item) for item in failure_modes if item],
        "regime_weaknesses": weaknesses,
        "confidence": round(confidence, 1),
        "uncertainty": uncertainty_label(confidence),
        "historical_robustness": robustness,
        "decision_rule": "Research candidate only; require risk limits and out-of-sample evidence before deployment.",
    }


def uncertainty_label(confidence: float) -> str:
    if confidence >= 80:
        return "Lower uncertainty"
    if confidence >= 60:
        return "Moderate uncertainty"
    if confidence >= 40:
        return "High uncertainty"
    return "Insufficient evidence"


def validate_signal(signal: Dict) -> Dict:
    required = {
        "signal",
        "thesis",
        "evidence",
        "assumptions",
        "failure_modes",
        "regime_weaknesses",
        "confidence",
        "uncertainty",
    }
    missing = sorted(required - set(signal))
    return {
        "valid": not missing and bool(signal.get("evidence")) and bool(signal.get("failure_modes")),
        "missing_fields": missing,
    }
