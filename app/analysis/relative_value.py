"""Relative-value analysis built on economic peer selection."""

from __future__ import annotations

from datetime import datetime, timezone

import pandas as pd

from app.analysis.cointegration import analyze_cointegration, benjamini_hochberg
from app.analysis.pair_validation import validate_pair_walk_forward
from app.models.evidence import Evidence
from app.models.pair_result import PeerCandidate, RelativeValueResult, default_valid_until


def analyze_relative_value_candidates(candidates: list[PeerCandidate], histories: dict[str, pd.DataFrame]) -> list[RelativeValueResult]:
    """Analyze economically selected pairs and apply multiple-testing correction."""
    raw = []
    for candidate in candidates:
        left = histories.get(candidate.ticker_a)
        right = histories.get(candidate.ticker_b)
        raw.append((candidate, _analyze_one(candidate, left, right, None)))
    adjusted = benjamini_hochberg([item.raw_pvalue for _, item in raw])
    output = []
    for (candidate, result), adj in zip(raw, adjusted):
        output.append(_with_adjusted(candidate, result, adj))
    return output


def _analyze_one(candidate: PeerCandidate, left: pd.DataFrame | None, right: pd.DataFrame | None, adjusted_pvalue: float | None) -> RelativeValueResult:
    now = datetime.now(timezone.utc)
    if left is None or right is None or left.empty or right.empty or "Close" not in left or "Close" not in right:
        return _empty(candidate, "Insufficient price history.")
    stats = analyze_cointegration(left["Close"], right["Close"])
    validation = validate_pair_walk_forward(left["Close"], right["Close"])
    evidence: list[Evidence] = []
    contra: list[Evidence] = []
    limitations = list(stats.warnings)
    if validation.warning:
        limitations.append(validation.warning)
    if stats.raw_pvalue is not None and stats.raw_pvalue < 0.1:
        evidence.append(_ev("Residual mean reversion", "relative_value", stats.raw_pvalue, "The pair passed a residual mean-reversion check.", "positive", "high", now, "v3_cointegration_1", "calculation"))
    else:
        contra.append(_ev("Weak stationarity", "relative_value", stats.raw_pvalue, "The relationship did not pass the residual stability threshold.", "negative", "high", now, "v3_cointegration_1", "calculation"))
    if stats.rolling_stability is not None and stats.rolling_stability >= 0.55:
        evidence.append(_ev("Rolling stability", "relative_value", stats.rolling_stability, "Recent windows support a persistent relationship.", "positive", "medium", now, "v3_cointegration_1", "calculation"))
    else:
        contra.append(_ev("Rolling instability", "relative_value", stats.rolling_stability, "The relationship was not stable enough across rolling windows.", "negative", "medium", now, "v3_cointegration_1", "calculation"))
    if stats.structural_break_status == "No obvious break":
        evidence.append(_ev("Structural-break check", "relative_value", stats.structural_break_status, "No obvious structural break was detected.", "positive", "medium", now, "v3_cointegration_1", "calculation"))
    else:
        contra.append(_ev("Structural-break check", "relative_value", stats.structural_break_status, "A possible structural break weakens the relationship.", "negative", "high", now, "v3_cointegration_1", "calculation"))
    relationship_status = _status(stats.spread_zscore, stats.raw_pvalue, adjusted_pvalue, stats.rolling_stability, stats.structural_break_status, validation.sample_size)
    direction = _direction(candidate, stats.spread_zscore)
    confidence = _confidence(relationship_status, adjusted_pvalue, stats.rolling_stability, validation.sample_size)
    cost_warning = None if validation.after_cost_status == "Passed conservative cost check" else validation.after_cost_status
    return RelativeValueResult(candidate.ticker_a, candidate.ticker_b, relationship_status, direction, stats.spread_zscore, stats.hedge_ratio, stats.half_life_days, stats.raw_pvalue, adjusted_pvalue, stats.rolling_stability, f"{validation.narrowing_rate:.0%} narrowed historically" if validation.narrowing_rate is not None else "Insufficient validation sample", stats.structural_break_status, cost_warning, confidence, evidence, contra, limitations, default_valid_until() if relationship_status not in {"Insufficient Evidence", "Relationship Broken"} else None)


def _with_adjusted(candidate: PeerCandidate, result: RelativeValueResult, adjusted_pvalue: float | None) -> RelativeValueResult:
    return _analyze_one(candidate, None, None, adjusted_pvalue) if result.raw_pvalue is None else RelativeValueResult(result.ticker_a, result.ticker_b, _status(result.spread_zscore, result.raw_pvalue, adjusted_pvalue, result.rolling_stability, result.structural_break_status, 1 if "Insufficient validation" not in result.out_of_sample_status else 0), result.divergence_direction, result.spread_zscore, result.hedge_ratio, result.half_life_days, result.raw_pvalue, adjusted_pvalue, result.rolling_stability, result.out_of_sample_status, result.structural_break_status, result.estimated_cost_warning, _confidence(result.relationship_status, adjusted_pvalue, result.rolling_stability, 1), result.evidence, result.contradicting_evidence, result.limitations, result.valid_until, result.analyzed_at)


def _status(z: float | None, raw_p: float | None, adj_p: float | None, stability: float | None, break_status: str, validation_sample: int) -> str:
    if z is None or raw_p is None:
        return "Insufficient Evidence"
    if break_status != "No obvious break":
        return "Relationship Broken"
    if adj_p is not None and adj_p > 0.15:
        return "Relationship Weakening"
    if stability is not None and stability < 0.55:
        return "Relationship Weakening"
    if abs(z) >= 2.0 and validation_sample > 0:
        return "Unusual Divergence"
    if abs(z) >= 1.25:
        return "Moderate Divergence"
    return "Normal Relationship"


def _direction(candidate: PeerCandidate, z: float | None) -> str:
    if z is None:
        return "No reliable divergence direction."
    if z < -1:
        return f"{candidate.ticker_a} is trading below its historical relationship with {candidate.ticker_b}."
    if z > 1:
        return f"{candidate.ticker_a} is trading above its historical relationship with {candidate.ticker_b}."
    return "The pair is trading near its historical relationship."


def _confidence(status: str, adj_p: float | None, stability: float | None, validation_sample: int) -> str:
    if status in {"Insufficient Evidence", "Relationship Broken"}:
        return "Low"
    if adj_p is not None and adj_p < 0.1 and (stability or 0) >= 0.7 and validation_sample > 0:
        return "Medium"
    return "Low"


def _empty(candidate: PeerCandidate, reason: str) -> RelativeValueResult:
    return RelativeValueResult(candidate.ticker_a, candidate.ticker_b, "Insufficient Evidence", "No reliable peer relationship qualified.", None, None, None, None, None, None, "Unavailable", "Insufficient data", None, "Low", [], [], [reason], None)


def _ev(signal: str, category: str, value: object, text: str, direction: str, importance: str, now: datetime, model_version: str, kind: str) -> Evidence:
    return Evidence(signal, category, value, text, direction, importance, "model", now, "fresh", kind, calculation=signal, model_version=model_version, confidence="Medium", validation_status="tested")  # type: ignore[arg-type]
