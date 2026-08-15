from datetime import datetime

import pandas as pd

from app.models.stock import StockSnapshot
from app.services.analysis import analyze_stock


def _history(values, volumes=None):
    volumes = volumes or [100] * len(values)
    return pd.DataFrame({"Close": values, "Volume": volumes})


def test_improving_stock_has_evidence():
    history = _history(list(range(100, 170)), [100] * 69 + [220])
    snapshot = StockSnapshot("ABC", "ABC Co", 169, 0.04, 0.08, 0.2, 2.0, updated_at=datetime.utcnow())
    result = analyze_stock(snapshot, history)
    assert result.trend == "Improving"
    assert result.volume_status == "Unusual volume"
    assert result.overall_view in {"Positive", "Worth Watching"}
    assert result.positive_evidence


def test_weakening_stock():
    history = _history(list(range(170, 100, -1)))
    snapshot = StockSnapshot("ABC", "ABC Co", 101, -0.04, -0.08, -0.2, 1.0, updated_at=datetime.utcnow())
    result = analyze_stock(snapshot, history)
    assert result.trend == "Weakening"
    assert result.negative_evidence


def test_missing_fundamentals_are_unknown_not_negative():
    history = _history(list(range(100, 170)))
    snapshot = StockSnapshot("ABC", "ABC Co", 169, 0.01, 0.02, 0.03, 1.0, updated_at=datetime.utcnow())
    result = analyze_stock(snapshot, history)
    assert any("Revenue growth is unavailable" in item for item in result.unknowns)
    assert not any(e.category == "fundamental" for e in result.negative_evidence)


def test_no_institutional_claim():
    history = _history(list(range(100, 170)), [100] * 69 + [300])
    snapshot = StockSnapshot("ABC", "ABC Co", 169, 0.05, 0.08, 0.2, 3.0, updated_at=datetime.utcnow())
    result = analyze_stock(snapshot, history)
    text = " ".join([result.what_changed, result.why_it_matters, result.main_risk, result.what_to_watch]).lower()
    assert "institution" not in text
    assert "whale" not in text
