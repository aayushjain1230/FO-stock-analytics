"""Factor research engine adapters."""

from __future__ import annotations

from typing import Any, Dict


def factor_brief(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Summarize portfolio factor tilts."""
    exposures = payload.get("exposures", {})
    leader = max(exposures.items(), key=lambda item: item[1], default=("N/A", 0))
    return {
        "exposures": exposures,
        "main_driver": leader[0],
        "warnings": payload.get("warnings", []),
        "interpretation": "Strong factor tilts can drive both performance and correlated drawdowns.",
    }
