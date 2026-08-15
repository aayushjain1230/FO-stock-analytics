"""EWMA volatility forecasting with restrained GARCH fallback behavior."""

from __future__ import annotations

import numpy as np
import pandas as pd

from app.models.stock import StockSnapshot
from app.models.volatility import VolatilityForecast


def forecast_volatility(snapshot: StockSnapshot, history: pd.DataFrame | None, horizon_days: int = 30, decay: float = 0.94) -> VolatilityForecast:
    """Forecast movement using realized volatility and EWMA; GARCH is optional and safely disabled."""
    returns = _returns(history)
    if len(returns) < 60:
        return VolatilityForecast(snapshot.ticker, "Unavailable", "Insufficient Data", None, None, horizon_days, "Insufficient Data", "Not enough reliable return history is available.", "The observation window is too short.", None, None, "unavailable", {}, ["At least 60 returns are required."])
    ewma_daily = ewma_volatility(returns, decay)
    realized_daily = float(returns.tail(20).std())
    annualized = float(ewma_daily * np.sqrt(252))
    horizon_vol = float(ewma_daily * np.sqrt(horizon_days))
    garch_vol, garch_status, garch_warnings = safe_garch_forecast(returns, horizon_days)
    active = "EWMA"
    active_vol = annualized
    validation = validate_volatility_forecast(returns)
    if garch_status == "success" and garch_vol is not None and validation.get("ewma_beats_realized_baseline") is False:
        active = "GARCH fallback candidate"
        active_vol = garch_vol
    expected = "Elevated" if ewma_daily > realized_daily * 1.15 else "Lower" if ewma_daily < realized_daily * 0.85 else "Normal"
    confidence = "Medium" if validation.get("sample_size", 0) >= 120 else "Low"
    return VolatilityForecast(
        snapshot.ticker,
        active,
        expected,
        active_vol,
        horizon_vol,
        horizon_days,
        confidence,
        "Recent price swings have increased and the volatility model expects them to remain elevated." if expected == "Elevated" else "Recent volatility is close to its own baseline.",
        "The estimate may not capture surprise earnings, regulatory, or macro announcements.",
        annualized,
        garch_vol,
        garch_status,
        validation,
        garch_warnings,
    )


def ewma_volatility(returns: pd.Series, decay: float = 0.94) -> float:
    """Calculate daily EWMA volatility."""
    clean = returns.dropna().to_numpy()
    if len(clean) == 0:
        return 0.0
    variance = clean[0] ** 2
    for value in clean[1:]:
        variance = decay * variance + (1 - decay) * value**2
    return float(np.sqrt(max(variance, 0)))


def safe_garch_forecast(returns: pd.Series, horizon_days: int = 30) -> tuple[float | None, str, list[str]]:
    """Try a minimal GARCH(1,1)-style forecast; fall back when unavailable or unstable."""
    warnings: list[str] = []
    try:
        from arch import arch_model  # type: ignore
    except Exception:
        return None, "unavailable", ["GARCH dependency is unavailable; EWMA is used."]
    try:
        scaled = returns.dropna() * 100
        model = arch_model(scaled, p=1, q=1, mean="zero", vol="Garch", dist="t")
        fit = model.fit(disp="off")
        params = fit.params
        persistence = float(params.get("alpha[1]", 0) + params.get("beta[1]", 0))
        if persistence >= 0.995:
            warnings.append("GARCH volatility is near-integrated; EWMA fallback is safer.")
            return None, "failed_validation", warnings
        forecast = fit.forecast(horizon=horizon_days)
        variance = float(forecast.variance.iloc[-1].mean()) / 10000
        return float(np.sqrt(variance * 252)), "success", warnings
    except Exception as exc:
        return None, "error", [f"GARCH failed safely: {exc}"]


def validate_volatility_forecast(returns: pd.Series, horizon: int = 20) -> dict:
    """Walk-forward compare EWMA against a simple realized-volatility baseline."""
    clean = returns.dropna()
    if len(clean) < 80:
        return {"sample_size": len(clean), "warning": "Small sample"}
    errors_ewma = []
    errors_base = []
    for end in range(60, len(clean) - horizon, horizon):
        train = clean.iloc[:end]
        actual = float(clean.iloc[end : end + horizon].std())
        ewma = ewma_volatility(train)
        baseline = float(train.tail(20).std())
        errors_ewma.append(abs(ewma - actual))
        errors_base.append(abs(baseline - actual))
    return {
        "sample_size": len(errors_ewma),
        "mae": float(np.mean(errors_ewma)) if errors_ewma else None,
        "baseline_mae": float(np.mean(errors_base)) if errors_base else None,
        "ewma_beats_realized_baseline": bool(np.mean(errors_ewma) <= np.mean(errors_base)) if errors_ewma else None,
    }


def _returns(history: pd.DataFrame | None) -> pd.Series:
    if history is None or history.empty or "Close" not in history:
        return pd.Series(dtype=float)
    return pd.to_numeric(history["Close"], errors="coerce").pct_change().dropna()
