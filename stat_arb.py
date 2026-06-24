"""Statistical arbitrage and pairs-trading research utilities."""

from typing import Dict, Iterable, Tuple

import numpy as np
import pandas as pd
import research_mindset

try:
    from scipy import stats
except Exception:
    stats = None

try:
    from statsmodels.tsa.stattools import adfuller
except Exception:
    adfuller = None


def hedge_ratio_ols(y: pd.Series, x: pd.Series) -> Dict:
    aligned = pd.concat([y, x], axis=1).dropna()
    aligned.columns = ["y", "x"]
    if len(aligned) < 30:
        return {"available": False, "message": "Need at least 30 aligned observations."}
    x_matrix = np.column_stack([np.ones(len(aligned)), aligned["x"].values])
    intercept, beta = np.linalg.lstsq(x_matrix, aligned["y"].values, rcond=None)[0]
    spread = aligned["y"] - (intercept + beta * aligned["x"])
    return {"available": True, "intercept": float(intercept), "hedge_ratio": float(beta), "spread": spread}


def spread_zscore(spread: pd.Series, window: int = 60) -> Dict:
    spread = pd.to_numeric(spread, errors="coerce").dropna()
    if len(spread) < max(10, window // 2):
        return {"available": False, "message": "Not enough spread history."}
    lookback = spread.tail(window)
    std = lookback.std(ddof=1)
    z = 0.0 if std == 0 else (lookback.iloc[-1] - lookback.mean()) / std
    return {"available": True, "z_score": float(z), "mean": float(lookback.mean()), "std": float(std), "latest_spread": float(lookback.iloc[-1])}


def half_life_mean_reversion(spread: pd.Series) -> float | None:
    s = pd.to_numeric(spread, errors="coerce").dropna()
    if len(s) < 30:
        return None
    lagged = s.shift(1).dropna()
    delta = s.diff().dropna()
    aligned = pd.concat([delta.rename("delta"), lagged.rename("lagged")], axis=1).dropna()
    x = np.column_stack([np.ones(len(aligned)), aligned["lagged"].values])
    _, beta = np.linalg.lstsq(x, aligned["delta"].values, rcond=None)[0]
    if beta >= 0:
        return None
    return float(-np.log(2) / beta)


def cointegration_score(y: pd.Series, x: pd.Series) -> Dict:
    model = hedge_ratio_ols(y, x)
    if not model.get("available"):
        return model
    spread = model["spread"]
    z_payload = spread_zscore(spread)
    corr = pd.concat([y, x], axis=1).dropna().pct_change().corr().iloc[0, 1]
    stationarity = _engle_granger_residual_test(spread)
    p_value = stationarity.get("p_value")
    hl = half_life_mean_reversion(spread)
    is_cointegrated = bool(stationarity.get("cointegrated"))
    strength = (
        "Strong"
        if is_cointegrated and corr > 0.5 and hl is not None and hl < 60
        else "Moderate"
        if is_cointegrated and corr > 0.35
        else "Weak"
    )
    signal = _pair_signal(z_payload.get("z_score"))
    performance = _spread_backtest(spread, z_threshold=2.0, exit_threshold=0.5)
    confidence = _pair_confidence(p_value, corr, hl, performance)
    return {
        "available": True,
        "hedge_ratio": model["hedge_ratio"],
        "intercept": model["intercept"],
        "correlation": float(corr) if pd.notna(corr) else None,
        "spread_zscore": z_payload.get("z_score"),
        "half_life_days": hl,
        "engle_granger_p_value": float(p_value) if p_value is not None else None,
        "cointegrated": is_cointegrated,
        "stationarity_test": stationarity,
        "cointegration_strength": strength,
        "signal": signal,
        "historical_performance": performance,
        "spread_history": [
            {"date": str(index), "spread": round(float(value), 6)}
            for index, value in spread.tail(120).items()
        ],
        "research_mindset": research_mindset.research_envelope(
            "pairs_trading",
            "The price relationship is stationary enough that an extreme spread may mean-revert.",
            [
                f"Engle-Granger p-value: {p_value:.4f}" if p_value is not None else "Stationarity p-value unavailable",
                f"Spread z-score: {z_payload.get('z_score', 0):.2f}",
                f"Estimated half-life: {hl:.1f} days" if hl is not None else "Half-life is not stable",
            ],
            [
                "The hedge ratio remains stable after entry.",
                "Borrow, liquidity, and execution costs are available near historical levels.",
                "No corporate event permanently changes the relationship.",
            ],
            [
                "Cointegration can break after acquisitions, index changes, or business-model divergence.",
                "A high z-score may reflect a structural break rather than temporary mispricing.",
                "Short borrow and gap risk can dominate modeled edge.",
            ],
            confidence=confidence,
            robustness=performance,
        ),
        "interpretation": "Require residual stationarity, practical half-life, and net-of-cost performance before deployment.",
    }


def pairs_scan(price_frame: pd.DataFrame, z_threshold: float = 2.0) -> Dict:
    prices = price_frame.apply(pd.to_numeric, errors="coerce").dropna(how="all")
    results = []
    columns = list(prices.columns)
    for i, left in enumerate(columns):
        for right in columns[i + 1:]:
            payload = cointegration_score(np.log(prices[left]).dropna(), np.log(prices[right]).dropna())
            if not payload.get("available"):
                continue
            if payload.get("cointegrated") and abs(payload.get("spread_zscore") or 0) >= z_threshold:
                results.append({"pair": f"{left}/{right}", **payload})
    return {"candidates": sorted(results, key=lambda row: abs(row.get("spread_zscore") or 0), reverse=True), "z_threshold": z_threshold}


def _engle_granger_residual_test(spread: pd.Series) -> Dict:
    spread = pd.to_numeric(spread, errors="coerce").dropna()
    if len(spread) < 60:
        return {"available": False, "cointegrated": False, "message": "Need at least 60 residual observations."}
    if adfuller is not None:
        statistic, p_value, used_lag, nobs, critical_values, _ = adfuller(spread, regression="c", autolag="AIC")
        return {
            "available": True,
            "method": "Engle-Granger residual ADF",
            "test_statistic": float(statistic),
            "p_value": float(p_value),
            "used_lag": int(used_lag),
            "observations": int(nobs),
            "critical_values": {name: float(value) for name, value in critical_values.items()},
            "cointegrated": bool(p_value < 0.05),
        }

    lagged = spread.shift(1)
    delta = spread.diff()
    frame = pd.concat([delta.rename("delta"), lagged.rename("lagged")], axis=1).dropna()
    x = np.column_stack([np.ones(len(frame)), frame["lagged"].values])
    coefficients = np.linalg.lstsq(x, frame["delta"].values, rcond=None)[0]
    residuals = frame["delta"].values - x @ coefficients
    mse = (residuals @ residuals) / max(len(frame) - 2, 1)
    covariance = mse * np.linalg.pinv(x.T @ x)
    stderr = np.sqrt(max(covariance[1, 1], 1e-12))
    t_stat = float(coefficients[1] / stderr)
    approximate_p = float(2 * stats.t.sf(abs(t_stat), df=max(len(frame) - 2, 1))) if stats is not None else None
    return {
        "available": True,
        "method": "Approximate residual ADF fallback",
        "test_statistic": t_stat,
        "p_value": approximate_p,
        "cointegrated": bool(t_stat < -3.34),
        "warning": "Fallback critical value is approximate; install statsmodels for full ADF inference.",
    }


def _pair_signal(z_score, entry=2.0, exit_level=0.5) -> Dict:
    z_score = float(z_score or 0)
    if z_score >= entry:
        return {"action": "short_y_long_x", "reason": "Spread is above its historical mean.", "entry": True}
    if z_score <= -entry:
        return {"action": "long_y_short_x", "reason": "Spread is below its historical mean.", "entry": True}
    if abs(z_score) <= exit_level:
        return {"action": "exit", "reason": "Spread is near its historical mean.", "entry": False}
    return {"action": "hold", "reason": "Spread is between entry and exit thresholds.", "entry": False}


def _spread_backtest(spread: pd.Series, z_threshold=2.0, exit_threshold=0.5, window=60, cost_bps=10) -> Dict:
    spread = pd.to_numeric(spread, errors="coerce").dropna()
    mean = spread.rolling(window).mean()
    std = spread.rolling(window).std().replace(0, np.nan)
    z = (spread - mean) / std
    position = pd.Series(0.0, index=spread.index)
    current = 0.0
    for index, value in z.items():
        if pd.isna(value):
            position.loc[index] = current
            continue
        if abs(value) <= exit_threshold:
            current = 0.0
        elif value >= z_threshold:
            current = -1.0
        elif value <= -z_threshold:
            current = 1.0
        position.loc[index] = current
    changes = position.diff().abs().fillna(0)
    returns = position.shift(1) * spread.diff() - changes * cost_bps / 10000
    trades = int((changes > 0).sum())
    wins = returns[returns > 0]
    losses = returns[returns < 0]
    equity = returns.fillna(0).cumsum()
    drawdown = equity - equity.cummax()
    return {
        "trades": trades,
        "win_rate": float((returns > 0).mean()) if len(returns.dropna()) else 0.0,
        "average_daily_pnl": float(returns.mean()) if len(returns.dropna()) else 0.0,
        "profit_factor": float(wins.sum() / abs(losses.sum())) if abs(losses.sum()) > 0 else None,
        "maximum_drawdown": float(drawdown.min()) if not drawdown.empty else 0.0,
        "cost_bps_per_turn": cost_bps,
    }


def _pair_confidence(p_value, correlation, half_life, performance) -> float:
    score = 0.0
    if p_value is not None:
        score += max(0, min(40, (0.10 - p_value) / 0.10 * 40))
    score += max(0, min(20, (float(correlation or 0) - 0.30) / 0.50 * 20))
    if half_life is not None:
        score += max(0, min(20, (90 - half_life) / 90 * 20))
    profit_factor = performance.get("profit_factor")
    if profit_factor is not None:
        score += max(0, min(20, (profit_factor - 1) * 20))
    return max(0, min(100, score))
