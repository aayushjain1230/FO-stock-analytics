"""Research notebook service."""

from __future__ import annotations

from datetime import date, timedelta
from typing import Any, Dict, Iterable


def create_notebook_entry(ticker: str, hypothesis: str, evidence: Iterable[str], counterevidence: Iterable[str]) -> Dict[str, Any]:
    """Create a structured research notebook entry."""
    return {
        "ticker": ticker,
        "hypothesis": hypothesis,
        "evidence": list(evidence),
        "counterevidence": list(counterevidence),
        "assumptions": [],
        "failure_modes": [],
        "confidence": None,
        "next_research_step": "Validate signal out of sample.",
        "follow_up_date": (date.today() + timedelta(days=30)).isoformat(),
    }
