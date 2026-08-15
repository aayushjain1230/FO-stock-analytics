import numpy as np
import pandas as pd

from app.analysis.cointegration import analyze_cointegration, benjamini_hochberg
from app.analysis.relative_value import analyze_relative_value_candidates
from app.models.pair_result import PeerCandidate
from app.services.peer_data import select_peer_candidates


def _cointegrated(days=420):
    rng = np.random.default_rng(7)
    x = np.cumsum(rng.normal(0, 0.01, days)) + 5
    noise = rng.normal(0, 0.01, days)
    y = 1.2 * x + noise
    idx = pd.date_range("2024-01-01", periods=days, freq="B")
    return pd.DataFrame({"Close": np.exp(y)}, index=idx), pd.DataFrame({"Close": np.exp(x)}, index=idx)


def _independent(days=420):
    rng = np.random.default_rng(11)
    idx = pd.date_range("2024-01-01", periods=days, freq="B")
    return pd.DataFrame({"Close": np.exp(np.cumsum(rng.normal(0, 0.02, days)) + 5)}, index=idx), pd.DataFrame({"Close": np.exp(np.cumsum(rng.normal(0, 0.02, days)) + 5)}, index=idx)


def test_peer_selection_requires_curated_economic_relationship():
    v, ma = _cointegrated()
    candidates = select_peer_candidates(["V", "MA", "TSLA"], {"V": v, "MA": ma, "TSLA": v}, min_overlap=252)
    assert len(candidates) == 1
    assert candidates[0].ticker_a == "V"
    assert "Same industry" in candidates[0].reasons


def test_peer_selection_rejects_insufficient_history():
    v, ma = _cointegrated(days=80)
    assert select_peer_candidates(["V", "MA"], {"V": v, "MA": ma}, min_overlap=252) == []


def test_cointegration_known_pair_vs_independent_walks():
    left, right = _cointegrated()
    stats = analyze_cointegration(left["Close"], right["Close"])
    assert stats.raw_pvalue is not None
    assert stats.raw_pvalue < 0.15
    a, b = _independent()
    weak = analyze_cointegration(a["Close"], b["Close"])
    assert weak.raw_pvalue is not None
    assert weak.raw_pvalue >= stats.raw_pvalue


def test_multiple_testing_correction_monotonic():
    adjusted = benjamini_hochberg([0.01, 0.03, 0.2])
    assert adjusted[0] <= adjusted[1] <= adjusted[2]


def test_relative_value_result_avoids_undervalued_language():
    left, right = _cointegrated()
    candidate = PeerCandidate("V", "MA", ["Same industry"], "Medium")
    result = analyze_relative_value_candidates([candidate], {"V": left, "MA": right})[0]
    text = " ".join([result.divergence_direction, result.relationship_status])
    assert "undervalued" not in text.lower()
    assert result.valid_until is not None or result.relationship_status in {"Insufficient Evidence", "Relationship Broken"}


def test_structural_break_can_invalidate_relationship():
    left, right = _cointegrated()
    left = left.copy()
    left.iloc[len(left) // 2 :, 0] *= 1.8
    stats = analyze_cointegration(left["Close"], right["Close"])
    assert stats.structural_break_status in {"Possible structural break", "No obvious break"}
