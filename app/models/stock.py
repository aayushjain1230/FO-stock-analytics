"""Normalized stock models for UI and Telegram."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from app.models.evidence import Evidence


@dataclass
class StockSnapshot:
    """Normalized market and fundamental snapshot for one ticker."""

    ticker: str
    company_name: str | None
    price: float | None
    daily_change_pct: float | None
    five_day_change_pct: float | None
    twenty_day_change_pct: float | None
    volume_ratio: float | None
    market_cap: float | None = None
    revenue_growth: float | None = None
    earnings_growth: float | None = None
    profit_margin: float | None = None
    debt_to_equity: float | None = None
    free_cash_flow: float | None = None
    forward_pe: float | None = None
    next_earnings_date: str | None = None
    source: str = "yfinance"
    updated_at: datetime = field(default_factory=datetime.utcnow)
    error: str | None = None


@dataclass
class StockAnalysis:
    """Plain-English stock analysis backed by structured evidence."""

    ticker: str
    company_name: str | None
    overall_view: str
    trend: str
    volume_status: str
    confidence: str
    what_changed: str
    why_it_matters: str
    main_risk: str
    what_to_watch: str
    positive_evidence: list[Evidence]
    negative_evidence: list[Evidence]
    neutral_evidence: list[Evidence]
    unknowns: list[str]
    analyzed_at: datetime
    snapshot: StockSnapshot

    def attention_score(self) -> int:
        """Rank stocks for attention without implying a trade recommendation."""
        importance = {"low": 1, "medium": 2, "high": 3}
        score = sum(importance[e.importance] for e in self.positive_evidence + self.negative_evidence)
        if self.volume_status == "Unusual volume":
            score += 3
        if self.trend in {"Improving", "Weakening"}:
            score += 2
        if self.confidence == "Low":
            score -= 1
        return score

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-friendly analysis record."""
        return {
            "ticker": self.ticker,
            "company_name": self.company_name,
            "overall_view": self.overall_view,
            "trend": self.trend,
            "volume_status": self.volume_status,
            "confidence": self.confidence,
            "what_changed": self.what_changed,
            "why_it_matters": self.why_it_matters,
            "main_risk": self.main_risk,
            "what_to_watch": self.what_to_watch,
            "positive_evidence": [e.to_dict() for e in self.positive_evidence],
            "negative_evidence": [e.to_dict() for e in self.negative_evidence],
            "neutral_evidence": [e.to_dict() for e in self.neutral_evidence],
            "unknowns": self.unknowns,
            "analyzed_at": self.analyzed_at.isoformat(),
        }
