from pathlib import Path

import pytest

from app.services.evidence_aggregation import EvidenceCategory, aggregate_evidence, assert_supported_claim
from app.services.methodology_report import generate_model_report
from app.services.model_registry import registry_as_dict
from app.services.telegram import render_daily_brief
from app.models.snapshot import MarketSnapshot


def test_model_registry_inspectable():
    registry = registry_as_dict()
    assert "relative_value" in registry
    assert registry["garch_volatility"]["active"] is False


def test_aggregation_keeps_failed_model_out_and_event_caps_confidence():
    categories = [
        EvidenceCategory("Relative value", "Stable peer relationship", ["Passed tests"], [], "Medium", "fresh", "tested"),
        EvidenceCategory("Failed model", "Should not count", ["Bad"], [], "High", "fresh", "failed_validation"),
    ]
    result = aggregate_evidence(categories, event_risk=True)
    assert result.confidence == "Low"
    assert all(item.name != "Failed model" for item in result.categories)


def test_unsupported_claim_detection():
    with pytest.raises(AssertionError):
        assert_supported_claim("executive purchase confirmed", [])


def test_model_report_generated_to_ignored_reports(tmp_path):
    path = generate_model_report(tmp_path / "report.html")
    assert path.exists()
    assert "Known Weaknesses" in path.read_text(encoding="utf-8")


def test_telegram_no_pair_trade_instructions():
    message = render_daily_brief(MarketSnapshot(explanation="Mixed market."), [], {})
    lowered = message.lower()
    assert "short" not in lowered
    assert "guaranteed convergence" not in lowered
