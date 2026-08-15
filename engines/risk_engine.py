"""Risk analytics engine contracts."""

from __future__ import annotations

from typing import Any, Dict, Iterable


def risk_brief(portfolio: Dict[str, Any]) -> Dict[str, Any]:
    """Summarize portfolio risk in human-readable terms."""
    top = sorted(portfolio.get("risk_contributions", {}).items(), key=lambda item: item[1], reverse=True)[:3]
    return {
        "var_95": portfolio.get("tail_risk", {}).get("value_at_risk"),
        "cvar_95": portfolio.get("tail_risk", {}).get("conditional_value_at_risk"),
        "max_drawdown": portfolio.get("maximum_drawdown"),
        "top_risk_contributors": top,
        "interpretation": "Risk is most useful when tied to position contribution, drawdown, and correlation.",
    }
