"""Portfolio state adapters."""

from __future__ import annotations

from typing import Any, Dict


def weight_map(portfolio_payload: Dict[str, Any]) -> Dict[str, float]:
    """Return ticker-to-weight percentage map from portfolio state."""
    return {item.get("ticker"): float(item.get("weight", 0.0)) for item in portfolio_payload.get("positions", [])}
