"""Reusable page-specific financial dictionary."""

from __future__ import annotations

from typing import Dict, List


GLOSSARY: Dict[str, List[Dict[str, str]]] = {
    "ev_probability": [
        {
            "term": "Expected Value",
            "simple": "The average payoff expected across many repetitions.",
            "math": "EV = Σ p_i × payoff_i",
            "why_it_matters": "Positive EV is the foundation of repeatable edge.",
            "common_mistake": "Treating one positive-EV estimate as certainty.",
        }
    ],
    "risk": [
        {
            "term": "CVaR",
            "simple": "Average loss after VaR has already been breached.",
            "math": "CVaR = E(loss | loss exceeds VaR)",
            "why_it_matters": "It describes tail pain better than VaR alone.",
            "common_mistake": "Ignoring that CVaR is historical/model dependent.",
        }
    ],
}


def definitions_for_page(page: str) -> List[Dict[str, str]]:
    """Return only definitions relevant to a page."""
    return GLOSSARY.get(page, [])
