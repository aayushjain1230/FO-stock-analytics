"""Normalized SEC filing models used by Version 2 intelligence services."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from typing import Any, Literal


FilingContext = Literal["confirmed_filing", "delayed_ownership", "unknown"]


@dataclass(frozen=True)
class FilingRecord:
    """A normalized, source-linked SEC filing or filing transaction."""

    accession_number: str
    ticker: str
    cik: str
    form_type: str
    filing_date: date
    event_date: date | None
    reporting_owner: str | None
    reporting_owner_type: str | None
    transaction_type: str | None
    shares: float | None
    price: float | None
    transaction_value: float | None
    ownership_percent: float | None
    position_change: float | None
    purpose: str | None
    source_url: str
    parsed_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    parser_status: str = "parsed"
    warnings: list[str] = field(default_factory=list)

    def identity_key(self) -> str:
        """Return a deterministic de-duplication key."""
        pieces = [
            self.accession_number,
            self.form_type,
            self.transaction_type or "",
            str(self.event_date or ""),
            str(self.shares or ""),
            str(self.price or ""),
            self.reporting_owner or "",
        ]
        return "|".join(pieces)

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe representation."""
        return {
            "accession_number": self.accession_number,
            "ticker": self.ticker,
            "cik": self.cik,
            "form_type": self.form_type,
            "filing_date": self.filing_date.isoformat(),
            "event_date": self.event_date.isoformat() if self.event_date else None,
            "reporting_owner": self.reporting_owner,
            "reporting_owner_type": self.reporting_owner_type,
            "transaction_type": self.transaction_type,
            "shares": self.shares,
            "price": self.price,
            "transaction_value": self.transaction_value,
            "ownership_percent": self.ownership_percent,
            "position_change": self.position_change,
            "purpose": self.purpose,
            "source_url": self.source_url,
            "parsed_at": self.parsed_at.isoformat(),
            "parser_status": self.parser_status,
            "warnings": self.warnings,
        }


@dataclass(frozen=True)
class FilingInsight:
    """Plain-English interpretation of a filing record."""

    ticker: str
    headline: str
    what_changed: str
    why_it_matters: str
    caution: str
    source_label: str
    source_url: str
    confidence: str
    context: FilingContext
    record: FilingRecord

    def to_dict(self) -> dict[str, Any]:
        """Return a JSON-safe insight."""
        return {
            "ticker": self.ticker,
            "headline": self.headline,
            "what_changed": self.what_changed,
            "why_it_matters": self.why_it_matters,
            "caution": self.caution,
            "source_label": self.source_label,
            "source_url": self.source_url,
            "confidence": self.confidence,
            "context": self.context,
            "record": self.record.to_dict(),
        }
