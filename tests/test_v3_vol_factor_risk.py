import numpy as np
import pandas as pd

from app.models.stock import StockSnapshot
from app.services.factor_model import analyze_single_stock_factors
from app.services.risk_metrics import calculate_risk_metrics
from app.services.volatility_forecast import ewma_volatility, forecast_volatility, safe_garch_forecast


def _history(days=260, drift=0.001, vol=0.01):
    rng = np.random.default_rng(5)
    idx = pd.date_range("2025-01-01", periods=days, freq="B")
    returns = rng.normal(drift, vol, days)
    close = 100 * np.cumprod(1 + returns)
    return pd.DataFrame({"Close": close, "Volume": 1_000_000}, index=idx)


def test_ewma_volatility_positive_and_missing_safe():
    returns = _history()["Close"].pct_change().dropna()
    assert ewma_volatility(returns) > 0


def test_garch_failure_or_unavailable_falls_back_safely():
    returns = _history()["Close"].pct_change().dropna()
    vol, status, warnings = safe_garch_forecast(returns)
    assert status in {"success", "unavailable", "failed_validation", "error"}
    if status != "success":
        assert warnings


def test_volatility_forecast_plain_english_no_garch_main_label():
    snap = StockSnapshot("ABC", "ABC Co", 100, 0.01, 0.02, 0.03, 1.0)
    result = forecast_volatility(snap, _history())
    assert result.expected_movement in {"Elevated", "Normal", "Lower", "Insufficient Data"}
    assert "GARCH(1,1)" not in result.why


def test_factor_model_known_market_exposure():
    market = _history()
    sector = _history(vol=0.012)
    stock = market.copy()
    stock["Close"] = market["Close"] * 1.1
    result = analyze_single_stock_factors(StockSnapshot("ABC", "ABC Co", 100, None, None, None, None), stock, market, sector)
    assert result.sample_size > 80
    assert "alpha" not in result.residual_interpretation.lower() or "without stronger validation" in result.residual_interpretation


def test_risk_metrics_multiple_windows_and_no_prediction_language():
    result = calculate_risk_metrics(StockSnapshot("ABC", "ABC Co", 100, None, None, None, None), _history(days=300))
    assert len(result.windows) >= 3
    assert "predict" not in result.conclusion.lower()
