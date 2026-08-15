"""Market data normalization adapters."""

from __future__ import annotations

from typing import Any, Dict


def normalize_market_snapshot(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize market state for frontend/services."""
    return {
        "regime": payload.get("regime") or payload.get("current_regime") or "Unknown",
        "confidence": payload.get("regime_confidence") or payload.get("confidence"),
        "transition_probabilities": payload.get("transition_probabilities", {}),
    }
