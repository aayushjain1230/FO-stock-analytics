"""Portfolio model contracts."""

from __future__ import annotations

from typing import Dict


def allocation_contract(weights: Dict[str, float], objective: str) -> Dict[str, object]:
    """Package an optimized allocation."""
    return {"weights": weights, "objective": objective, "interpretation": "Optimization output is a proposal, not an order."}
