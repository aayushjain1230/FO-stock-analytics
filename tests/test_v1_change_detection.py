from datetime import datetime

from app.models.stock import StockAnalysis, StockSnapshot
from app.services.change_detection import detect_changes


def _analysis(trend="Stable", overall="Mixed", volume="Normal volume", confidence="Medium"):
    snap = StockSnapshot("ABC", "ABC Co", 100, 0.0, 0.0, 0.0, 1.0)
    return StockAnalysis("ABC", "ABC Co", overall, trend, volume, confidence, "", "", "", "", [], [], [], [], datetime.utcnow(), snap)


def test_meaningful_status_change_detected():
    old = {"ABC": {"overall_view": "Mixed", "trend": "Stable", "volume_status": "Normal volume", "confidence": "Medium"}}
    changes = detect_changes([_analysis(trend="Weakening")], old)
    assert changes
    assert changes[0]["field"] == "trend"


def test_missing_previous_snapshot_no_change():
    assert detect_changes([_analysis()], {}) == []
