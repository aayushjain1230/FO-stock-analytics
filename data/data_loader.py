"""General JSON/data loading helpers."""

from __future__ import annotations

import json
import os
from typing import Any


def load_json(path: str, fallback: Any | None = None) -> Any:
    """Load JSON safely with a fallback."""
    if fallback is None:
        fallback = {}
    if not os.path.exists(path):
        return fallback
    try:
        with open(path, "r", encoding="utf-8") as file:
            return json.load(file)
    except Exception:
        return fallback


def save_json(path: str, payload: Any) -> None:
    """Save JSON with deterministic formatting."""
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, "w", encoding="utf-8") as file:
        json.dump(payload, file, indent=2, default=str)
