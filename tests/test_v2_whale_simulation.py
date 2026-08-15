from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from app.models.simulation import SimulationConfig
from app.models.stock import StockSnapshot
from app.services.bootstrap_simulation import run_historical_bootstrap
from app.services.calibration import walk_forward_calibration
from app.services.whale_activity import analyze_whale_activity


def _history(days=160, heavy_last=True, falling=False):
    dates = pd.date_range("2026-01-01", periods=days, freq="B")
    returns = np.full(days, 0.002)
    returns[-1] = -0.05 if falling else 0.04
    close = 100 * np.cumprod(1 + returns)
    volume = np.full(days, 1_000_000.0)
    if heavy_last:
        volume[-1] = 3_000_000
        volume[-3:] = [1_600_000, 1_700_000, volume[-1]]
    return pd.DataFrame({"Close": close, "Volume": volume, "High": close * 1.01, "Low": close * 0.99}, index=dates)


def _snapshot(next_earnings_date=None):
    return StockSnapshot("ABC", "ABC Co", 120.0, 0.04, 0.05, 0.1, 3.0, next_earnings_date=next_earnings_date)


def test_whale_activity_positive_confirmation_no_buyer_identity():
    result = analyze_whale_activity(_snapshot(), _history())
    assert result.level in {"Elevated", "High"}
    assert "cannot identify anonymous buyers" in result.inference or "not proof" in result.inference
    assert "BlackRock" not in result.inference


def test_whale_activity_distribution_risk():
    result = analyze_whale_activity(_snapshot(), _history(falling=True))
    assert result.level == "Distribution Risk"
    assert result.contradicting_evidence


def test_whale_activity_missing_data_abstains():
    result = analyze_whale_activity(_snapshot(), pd.DataFrame())
    assert result.level == "Insufficient Data"


def test_whale_activity_earnings_confounder():
    earnings = (datetime.utcnow() + timedelta(days=4)).date().isoformat()
    result = analyze_whale_activity(_snapshot(earnings), _history())
    assert result.confounders
    assert result.confidence in {"Low", "Medium", "Insufficient Data"}


def test_standard_bootstrap_deterministic():
    config = SimulationConfig(simulations=1000, random_seed=123, minimum_history=40)
    first = run_historical_bootstrap(_snapshot(), _history(), config)
    second = run_historical_bootstrap(_snapshot(), _history(), config)
    assert first.median_ending_price == second.median_ending_price
    assert first.scenarios_ending_higher_pct is not None
    assert "not a guaranteed" in first.explanation


def test_block_bootstrap_and_regime_bootstrap_work():
    hist = _history()
    block = run_historical_bootstrap(_snapshot(), hist, SimulationConfig(method="block", simulations=200, minimum_history=40))
    regime = run_historical_bootstrap(_snapshot(), hist, SimulationConfig(method="regime_conditioned", simulations=200, minimum_history=40), market_condition="Supportive")
    assert block.percentile_range
    assert regime.percentile_range


def test_simulation_minimum_history_rejection():
    result = run_historical_bootstrap(_snapshot(), _history(days=20), SimulationConfig(minimum_history=40))
    assert result.confidence == "Insufficient Data"


def test_upcoming_earnings_warning():
    earnings = (datetime.utcnow() + timedelta(days=3)).date().isoformat()
    result = run_historical_bootstrap(_snapshot(earnings), _history(), SimulationConfig(simulations=200, minimum_history=40))
    assert result.event_warning
    assert result.confidence == "Low"


def test_walk_forward_calibration_has_no_lookahead_shape():
    summary = walk_forward_calibration("ABC", _history(days=220), SimulationConfig(simulations=100, minimum_history=60, horizon_days=20), step=20)
    assert summary.sample_size > 0
    assert summary.coverage_80 is not None
    assert summary.baseline_directional_hit_rate is not None
