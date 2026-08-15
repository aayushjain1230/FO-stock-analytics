"""Report-building service layer."""

from __future__ import annotations

from typing import Any, Dict


def page_context(title: str, question: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Build page context around a research question."""
    return {"title": title, "question": question, "payload": payload}
