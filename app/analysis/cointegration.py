"""Cointegration-style residual analysis for economically selected pairs."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd


@dataclass(frozen=True)
class CointegrationStats:
    """Statistical diagnostics for one pair."""

    hedge_ratio: float | None
    intercept: float | None
    spread_zscore: float | None
    half_life_days: float | None
    raw_pvalue: float | None
    rolling_stability: float | None
    hedge_stability: float | None
    structural_break_status: str
    sample_size: int
    data_start: str | None
    data_end: str | None
    warnings: list[str]


def analyze_cointegration(price_a: pd.Series, price_b: pd.Series, min_obs: int = 252) -> CointegrationStats:
    """Estimate a restrained Engle-Granger-style relationship using log prices."""
    aligned = pd.concat([price_a.rename("a"), price_b.rename("b")], axis=1, sort=False).dropna()
    aligned = aligned[(aligned["a"] > 0) & (aligned["b"] > 0)]
    if len(aligned) < min_obs:
        return CointegrationStats(None, None, None, None, None, None, None, "Insufficient data", len(aligned), None, None, ["Insufficient overlapping history."])
    logs = np.log(aligned)
    beta, intercept = _ols(logs["b"].to_numpy(), logs["a"].to_numpy())
    spread = logs["a"] - (intercept + beta * logs["b"])
    zscore = _zscore(spread)
    half_life = _half_life(spread)
    raw_p = _residual_stationarity_score(spread)
    rolling_stability = _rolling_stationarity(logs["a"], logs["b"])
    hedge_stability = _hedge_stability(logs["a"], logs["b"])
    structural_break = _structural_break(spread)
    warnings: list[str] = []
    if half_life is None or half_life <= 1 or half_life > 90:
        warnings.append("Mean-reversion half-life is not in a useful range.")
    if rolling_stability is not None and rolling_stability < 0.55:
        warnings.append("Rolling relationship stability is weak.")
    if structural_break != "No obvious break":
        warnings.append("Structural-break check weakened the relationship.")
    return CointegrationStats(float(beta), float(intercept), zscore, half_life, raw_p, rolling_stability, hedge_stability, structural_break, len(aligned), str(aligned.index.min().date()), str(aligned.index.max().date()), warnings)


def benjamini_hochberg(pvalues: list[float | None]) -> list[float | None]:
    """Apply Benjamini-Hochberg false-discovery control."""
    indexed = [(idx, p) for idx, p in enumerate(pvalues) if p is not None]
    if not indexed:
        return [None for _ in pvalues]
    m = len(indexed)
    ordered = sorted(indexed, key=lambda item: item[1])
    adjusted = [None for _ in pvalues]
    prev = 1.0
    for rank, (idx, p) in reversed(list(enumerate(ordered, start=1))):
        value = min(prev, p * m / rank)
        adjusted[idx] = float(min(value, 1.0))
        prev = value
    return adjusted


def _ols(x: np.ndarray, y: np.ndarray) -> tuple[float, float]:
    xmat = np.column_stack([np.ones(len(x)), x])
    coef = np.linalg.lstsq(xmat, y, rcond=None)[0]
    return float(coef[1]), float(coef[0])


def _zscore(series: pd.Series) -> float | None:
    std = float(series.std())
    if std <= 0 or np.isnan(std):
        return None
    return float((series.iloc[-1] - series.mean()) / std)


def _half_life(spread: pd.Series) -> float | None:
    delta = spread.diff().dropna()
    lag = spread.shift(1).dropna().loc[delta.index]
    if len(delta) < 30:
        return None
    beta, _ = _ols(lag.to_numpy(), delta.to_numpy())
    if beta >= 0:
        return None
    return float(np.clip(-np.log(2) / beta, 0, 10_000))


def _residual_stationarity_score(spread: pd.Series) -> float:
    delta = spread.diff().dropna()
    lag = spread.shift(1).dropna().loc[delta.index]
    beta, _ = _ols(lag.to_numpy(), delta.to_numpy())
    residual = delta - (beta * lag + float((delta - beta * lag).mean()))
    se = float(residual.std() / np.sqrt(max(((lag - lag.mean()) ** 2).sum(), 1e-12)))
    t_stat = beta / se if se else 0.0
    # Smooth approximation: lower p-like values for strongly negative reversion.
    return float(np.clip(np.exp(t_stat), 0.0001, 1.0))


def _rolling_stationarity(a: pd.Series, b: pd.Series, window: int = 126) -> float | None:
    if len(a) < window * 2:
        return None
    scores = []
    for start in range(0, len(a) - window + 1, max(window // 3, 1)):
        stats = analyze_cointegration(np.exp(a.iloc[start : start + window]), np.exp(b.iloc[start : start + window]), min_obs=max(60, window // 2))
        scores.append(1.0 if stats.raw_pvalue is not None and stats.raw_pvalue < 0.1 else 0.0)
    return float(np.mean(scores)) if scores else None


def _hedge_stability(a: pd.Series, b: pd.Series, window: int = 126) -> float | None:
    if len(a) < window * 2:
        return None
    betas = []
    for start in range(0, len(a) - window + 1, max(window // 3, 1)):
        beta, _ = _ols(b.iloc[start : start + window].to_numpy(), a.iloc[start : start + window].to_numpy())
        betas.append(beta)
    if not betas:
        return None
    dispersion = np.std(betas) / max(abs(np.mean(betas)), 1e-6)
    return float(np.clip(1 - dispersion, 0, 1))


def _structural_break(spread: pd.Series) -> str:
    if len(spread) < 160:
        return "Not enough data for break test"
    early = spread.iloc[: len(spread) // 2]
    late = spread.iloc[len(spread) // 2 :]
    pooled = spread.std() or 1
    shift = abs(float(late.mean() - early.mean())) / float(pooled)
    return "Possible structural break" if shift > 1.25 else "No obvious break"
