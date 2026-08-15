"""Calibrated evidence aggregation that keeps categories separate."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(frozen=True)
class EvidenceCategory:
    """One separated category in the final conclusion."""

    name: str
    conclusion: str
    supporting: list[str]
    contradicting: list[str]
    confidence: str
    freshness: str
    validation_status: str
    limitations: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class AggregatedConclusion:
    """Final conclusion without mystery scoring."""

    overall_view: str
    confidence: str
    why: list[str]
    main_risks: list[str]
    what_to_watch: list[str]
    categories: list[EvidenceCategory]

    def to_dict(self) -> dict[str, Any]:
        return {
            "overall_view": self.overall_view,
            "confidence": self.confidence,
            "why": self.why,
            "main_risks": self.main_risks,
            "what_to_watch": self.what_to_watch,
            "categories": [item.__dict__ for item in self.categories],
        }


def aggregate_evidence(categories: list[EvidenceCategory], event_risk: bool = False, structural_break: bool = False) -> AggregatedConclusion:
    """Aggregate evidence using transparent rules instead of arbitrary weighted scores."""
    usable = [item for item in categories if item.validation_status not in {"failed_validation", "error"}]
    positives = [item for item in usable if item.supporting]
    negatives = [item for item in usable if item.contradicting]
    why = [item.conclusion for item in positives[:5]]
    risks = [text for item in negatives for text in item.contradicting[:2]]
    if structural_break:
        risks.insert(0, "A structural break may invalidate relative-value evidence.")
    if event_risk:
        risks.insert(0, "Upcoming event risk caps confidence.")
    confidence = "Medium" if len(positives) >= 3 and not event_risk and not structural_break else "Low"
    view = "Worth Watching" if positives and len(positives) >= len(negatives) else "Mixed Evidence" if positives else "Not Enough Evidence"
    return AggregatedConclusion(view, confidence, why or ["No validated model evidence is strong enough yet."], risks or ["Evidence could weaken if price breaks support on heavy volume."], ["Refresh after new market data, SEC filings, or model validation updates."], usable)


def assert_supported_claim(claim: str, evidence: list[Any]) -> None:
    """Development-only guard for important displayed claims."""
    lowered = claim.lower()
    if "executive purchase" in lowered and not any(getattr(item, "transaction_type", None) == "Open-market purchase" for item in evidence):
        raise AssertionError("Unsupported executive purchase claim.")
    if "stable peer relationship" in lowered and not any(getattr(item, "relationship_status", "") in {"Moderate Divergence", "Unusual Divergence", "Normal Relationship"} for item in evidence):
        raise AssertionError("Unsupported stable peer relationship claim.")
