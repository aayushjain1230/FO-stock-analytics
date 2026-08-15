"""Sharpe, Sortino, drawdown, and downside-risk metrics."""

from __future__ import annotations

import numpy as np
import pandas as pd

from app.models.risk_metrics import RiskMetricWindow, RiskMetricsResult
from app.models.stock import StockSnapshot


WINDOWS = {"3M": 63, "6M": 126, "1Y": 252, "3Y": 756}


def calculate_risk_metrics(snapshot: StockSnapshot, history: pd.DataFrame | None) -> RiskMetricsResult:
    """Calculate descriptive risk-adjusted metrics across multiple windows."""
    returns = _returns(history)
    if len(returns) < 63:
        return RiskMetricsResult(snapshot.ticker, "Not enough history for a reliable risk-adjusted comparison.", "Insufficient Data", [], ["At least three months of returns are required."])
    windows = [_window_metrics(label, returns.tail(days)) for label, days in WINDOWS.items() if len(returns) >= min(days, 63)]
    latest = windows[0]
    if latest.sharpe is None:
        conclusion = "Risk-Adjusted Consistency: Unavailable"
        confidence = "Low"
    elif latest.sharpe > 1 and latest.sortino and latest.sortino > 1:
        conclusion = "Risk-Adjusted Consistency: Strong but descriptive only."
        confidence = "Medium"
    elif latest.max_drawdown is not None and latest.max_drawdown < -0.2:
        conclusion = "Risk-Adjusted Consistency: Weak due to meaningful drawdowns."
        confidence = "Medium"
    else:
        conclusion = "Risk-Adjusted Consistency: Mixed."
        confidence = "Medium" if len(returns) >= 126 else "Low"
    return RiskMetricsResult(snapshot.ticker, conclusion, confidence, windows)


def _returns(history: pd.DataFrame | None) -> pd.Series:
    if history is None or history.empty or "Close" not in history:
        return pd.Series(dtype=float)
    return pd.to_numeric(history["Close"], errors="coerce").pct_change().dropna()


def _window_metrics(label: str, returns: pd.Series) -> RiskMetricWindow:
    if returns.empty or float(returns.std()) == 0:
        return RiskMetricWindow(label, len(returns), None, None, None, None, None, None, None)
    annual_return = float(returns.mean() * 252)
    annual_vol = float(returns.std() * np.sqrt(252))
    downside = returns[returns < 0]
    downside_dev = float(downside.std() * np.sqrt(252)) if len(downside) > 1 else None
    sharpe = annual_return / annual_vol if annual_vol else None
    sortino = annual_return / downside_dev if downside_dev and downside_dev > 0 else None
    equity = (1 + returns).cumprod()
    dd = equity / equity.cummax() - 1
    max_dd = float(dd.min())
    recovery = _recovery_time(dd)
    tail = float(np.percentile(returns, 5))
    consistency = float((returns > 0).mean())
    return RiskMetricWindow(label, len(returns), sharpe, sortino, max_dd, downside_dev, consistency, recovery, tail)


def _recovery_time(drawdown: pd.Series) -> int | None:
    longest = current = 0
    for value in drawdown:
        if value < 0:
            current += 1
            longest = max(longest, current)
        else:
            current = 0
    return longest or 0
