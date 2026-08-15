"""Version 2 confidence engine with abstention support."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class ConfidenceInputs:
    """Evidence-quality inputs for a single conclusion."""

    price_data_complete: bool = False
    intraday_data_complete: bool = False
    fundamental_data_fresh: bool = False
    filing_data_fresh: bool = False
    independent_evidence_categories: int = 0
    contradicting_evidence_count: int = 0
    model_agreement: str = "Unknown"
    calibration_sample_size: int = 0
    upcoming_earnings: bool = False
    confounder_count: int = 0
    structural_market_change: bool = False


def confidence_level(inputs: ConfidenceInputs) -> str:
    """Return High, Medium, Low, or Insufficient Data without double-counting related indicators."""
    if not inputs.price_data_complete and inputs.independent_evidence_categories == 0:
        return "Insufficient Data"
    score = 0
    score += 20 if inputs.price_data_complete else -20
    score += 10 if inputs.intraday_data_complete else 0
    score += 10 if inputs.fundamental_data_fresh else 0
    score += 12 if inputs.filing_data_fresh else 0
    score += min(inputs.independent_evidence_categories, 4) * 10
    score -= inputs.contradicting_evidence_count * 10
    score += 10 if inputs.model_agreement == "High" else 4 if inputs.model_agreement == "Medium" else 0
    score += 10 if inputs.calibration_sample_size >= 60 else 4 if inputs.calibration_sample_size >= 20 else -5
    score -= 15 if inputs.upcoming_earnings else 0
    score -= inputs.confounder_count * 12
    score -= 20 if inputs.structural_market_change else 0
    if score >= 70:
        return "High"
    if score >= 40:
        return "Medium"
    if score >= 15:
        return "Low"
    return "Insufficient Data"
