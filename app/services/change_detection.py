"""Meaningful change detection for watchlist analysis."""

from __future__ import annotations

import json
from pathlib import Path

from app.models.stock import StockAnalysis

SNAPSHOT_PATH = Path("state/v1_last_analysis.json")


def load_previous_snapshot(path: Path = SNAPSHOT_PATH) -> dict:
    """Load previous CLI/scheduled analysis snapshot."""
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def save_snapshot(analyses: list[StockAnalysis], path: Path = SNAPSHOT_PATH) -> None:
    """Save current valid analysis snapshot for future change detection."""
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {item.ticker: item.to_dict() for item in analyses if item.confidence != "Insufficient Data"}
    path.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")


def detect_changes(analyses: list[StockAnalysis], previous: dict) -> list[dict]:
    """Return only meaningful watchlist changes."""
    changes: list[dict] = []
    for item in analyses:
        old = previous.get(item.ticker)
        if not old:
            continue
        for field in ["overall_view", "trend", "volume_status", "confidence"]:
            if old.get(field) != getattr(item, field):
                changes.append({"ticker": item.ticker, "field": field, "from": old.get(field), "to": getattr(item, field)})
                break
    return changes
