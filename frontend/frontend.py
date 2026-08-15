"""Frontend route/rendering boundary.

The current app generates a static dashboard from ``quant_dashboard.py``. This
module defines the future Flask/FastAPI boundary so routing does not leak into
model or engine code.
"""

from __future__ import annotations

from typing import Any, Dict


def render_context(page: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Return a template context for a frontend page."""
    return {"page": page, "payload": payload}
