"""Single-stock transparent factor proxy model."""

from __future__ import annotations

import numpy as np
import pandas as pd

from app.models.factor import FactorModelResult
from app.models.stock import StockSnapshot


def analyze_single_stock_factors(snapshot: StockSnapshot, history: pd.DataFrame | None, market_history: pd.DataFrame | None = None, sector_history: pd.DataFrame | None = None) -> FactorModelResult:
    """Estimate market and sector sensitivity using available proxy returns."""
    stock = _returns(history)
    market = _returns(market_history)
    sector = _returns(sector_history)
    aligned = pd.concat([stock.rename("stock"), market.rename("market"), sector.rename("sector")], axis=1, sort=False).dropna()
    if len(aligned) < 80:
        return FactorModelResult(snapshot.ticker, "Not enough data for a reliable factor comparison.", None, None, None, None, "Company-specific movement could not be separated from market effects.", None, "Insufficient Data", len(aligned), ["Market or sector proxy history is missing."])
    y = aligned["stock"].to_numpy()
    x = aligned[["market", "sector"]].to_numpy()
    xmat = np.column_stack([np.ones(len(x)), x])
    coef = np.linalg.lstsq(xmat, y, rcond=None)[0]
    pred = xmat @ coef
    ss_res = float(((y - pred) ** 2).sum())
    ss_tot = float(((y - y.mean()) ** 2).sum()) or 1
    r2 = max(0.0, min(1.0, 1 - ss_res / ss_tot))
    momentum = float(stock.tail(63).sum()) if len(stock) >= 63 else None
    stability = _rolling_beta_stability(aligned)
    conclusion = "Performance Driver: Mostly Market and Sector Environment" if r2 >= 0.45 else "Performance Driver: Meaningful Company-Specific Movement"
    residual = "Residual movement is present and should not be called alpha without stronger validation."
    confidence = "Medium" if stability is not None and stability >= 0.5 else "Low"
    return FactorModelResult(snapshot.ticker, conclusion, float(coef[1]), float(coef[2]), momentum, r2, residual, stability, confidence, len(aligned))


def _returns(history: pd.DataFrame | None) -> pd.Series:
    if history is None or history.empty or "Close" not in history:
        return pd.Series(dtype=float)
    return pd.to_numeric(history["Close"], errors="coerce").pct_change().dropna()


def _rolling_beta_stability(aligned: pd.DataFrame, window: int = 63) -> float | None:
    if len(aligned) < window * 2:
        return None
    betas = []
    for start in range(0, len(aligned) - window + 1, window):
        frame = aligned.iloc[start : start + window]
        xmat = np.column_stack([np.ones(len(frame)), frame[["market", "sector"]].to_numpy()])
        coef = np.linalg.lstsq(xmat, frame["stock"].to_numpy(), rcond=None)[0]
        betas.append(coef[1:])
    if len(betas) < 2:
        return None
    arr = np.array(betas)
    dispersion = float(np.mean(np.std(arr, axis=0) / np.maximum(np.abs(np.mean(arr, axis=0)), 1e-6)))
    return float(np.clip(1 - dispersion, 0, 1))
