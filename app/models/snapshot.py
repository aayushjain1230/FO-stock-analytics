"""Market and run snapshot models."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime


@dataclass
class MarketSnapshot:
    """Normalized market overview."""

    sp500_change_pct: float | None = None
    nasdaq_change_pct: float | None = None
    condition: str = "Unavailable"
    explanation: str = "Market data is unavailable."
    updated_at: datetime = field(default_factory=datetime.utcnow)
    warnings: list[str] = field(default_factory=list)
