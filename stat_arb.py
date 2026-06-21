"""Statistical arbitrage and pairs-trading research utilities."""

from typing import Dict, Iterable, Tuple

import numpy as np
import pandas as pd

try:
    from scipy import stats
except Exception:
    stats = None


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
    p_value = None
    if stats is not None and len(spread) >= 30:
        slope, _, _, p_value, _ = stats.linregress(spread.shift(1).dropna(), spread.diff().dropna().loc[spread.shift(1).dropna().index])
    hl = half_life_mean_reversion(spread)
    strength = "Strong" if abs(z_payload.get("z_score", 0)) >= 2 and corr > 0.5 and (hl is not None and hl < 60) else "Moderate" if corr > 0.4 else "Weak"
    return {
        "available": True,
        "hedge_ratio": model["hedge_ratio"],
        "correlation": float(corr) if pd.notna(corr) else None,
        "spread_zscore": z_payload.get("z_score"),
        "half_life_days": hl,
        "mean_reversion_p_value_proxy": float(p_value) if p_value is not None else None,
        "cointegration_strength": strength,
        "interpretation": "Pairs are research candidates only when correlation, spread behavior, and mean reversion all support the setup.",
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
            if abs(payload.get("spread_zscore") or 0) >= z_threshold:
                results.append({"pair": f"{left}/{right}", **payload})
    return {"candidates": sorted(results, key=lambda row: abs(row.get("spread_zscore") or 0), reverse=True), "z_threshold": z_threshold}
