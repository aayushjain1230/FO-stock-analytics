"""Factor model research utilities."""

from typing import Dict, Iterable

import research_mindset

import numpy as np
import pandas as pd

TRADING_DAYS = 252

FACTOR_WEIGHTS = {
    "momentum": 0.25,
    "quality": 0.22,
    "growth": 0.20,
    "value": 0.18,
    "low_volatility": 0.15,
}


def _returns(prices: pd.DataFrame) -> pd.DataFrame:
    return prices.apply(pd.to_numeric, errors="coerce").pct_change().dropna(how="all")


def market_model(asset_returns: pd.Series, market_returns: pd.Series, risk_free_rate: float = 0.0) -> Dict:
    aligned = pd.concat([asset_returns, market_returns], axis=1).dropna()
    aligned.columns = ["asset", "market"]
    if len(aligned) < 20:
        return {"available": False, "message": "Need at least 20 aligned observations."}
    y = aligned["asset"] - risk_free_rate / TRADING_DAYS
    x = aligned["market"] - risk_free_rate / TRADING_DAYS
    x_matrix = np.column_stack([np.ones(len(x)), x.values])
    alpha_daily, beta = np.linalg.lstsq(x_matrix, y.values, rcond=None)[0]
    fitted = x_matrix @ np.array([alpha_daily, beta])
    residuals = y.values - fitted
    ss_total = float(((y.values - y.values.mean()) ** 2).sum())
    ss_resid = float((residuals**2).sum())
    return {
        "available": True,
        "alpha_annualized": float(alpha_daily * TRADING_DAYS),
        "beta": float(beta),
        "r_squared": 1 - ss_resid / ss_total if ss_total else 0.0,
        "residual_volatility": float(np.std(residuals, ddof=1) * np.sqrt(TRADING_DAYS)),
        "interpretation": "Market model separates broad-market exposure from residual alpha.",
    }


def multi_factor_regression(asset_returns: pd.Series, factor_returns: pd.DataFrame, risk_free_rate: float = 0.0) -> Dict:
    frame = pd.concat([asset_returns.rename("asset"), factor_returns], axis=1).dropna()
    if len(frame) < max(30, len(factor_returns.columns) + 5):
        return {"available": False, "message": "Not enough aligned data for multi-factor regression."}
    y = frame["asset"].values - risk_free_rate / TRADING_DAYS
    x_raw = frame.drop(columns=["asset"]).values - risk_free_rate / TRADING_DAYS
    x = np.column_stack([np.ones(len(x_raw)), x_raw])
    coefficients = np.linalg.lstsq(x, y, rcond=None)[0]
    fitted = x @ coefficients
    residuals = y - fitted
    dof = max(len(y) - x.shape[1], 1)
    mse = float((residuals @ residuals) / dof)
    cov = mse * np.linalg.pinv(x.T @ x)
    se = np.sqrt(np.diag(cov))
    names = ["alpha"] + list(frame.drop(columns=["asset"]).columns)
    exposures = {}
    for i, name in enumerate(names):
        coef = float(coefficients[i])
        stderr = float(se[i]) if se[i] else None
        exposures[name] = {
            "coefficient": coef * TRADING_DAYS if name == "alpha" else coef,
            "std_error": stderr,
            "t_stat": coef / stderr if stderr else None,
        }
    ss_total = float(((y - y.mean()) ** 2).sum())
    ss_resid = float((residuals**2).sum())
    return {
        "available": True,
        "sample_size": int(len(frame)),
        "r_squared": 1 - ss_resid / ss_total if ss_total else 0.0,
        "exposures": exposures,
        "residual_volatility": float(np.std(residuals, ddof=1) * np.sqrt(TRADING_DAYS)),
        "interpretation": "Multiple regression estimates which factors matter after accounting for the others.",
    }


def pca_factor_model(return_frame: pd.DataFrame, n_components: int | None = None) -> Dict:
    returns = return_frame.apply(pd.to_numeric, errors="coerce").dropna()
    if returns.shape[0] < 20 or returns.shape[1] < 2:
        return {"available": False, "message": "Need at least 20 rows and 2 assets/features for PCA."}
    standardized = (returns - returns.mean()) / returns.std(ddof=1).replace(0, np.nan)
    standardized = standardized.dropna(axis=1)
    if standardized.shape[1] < 2:
        return {"available": False, "message": "Not enough non-constant columns for PCA."}
    cov = np.cov(standardized.values, rowvar=False)
    eigenvalues, eigenvectors = np.linalg.eigh(cov)
    order = np.argsort(eigenvalues)[::-1]
    eigenvalues = eigenvalues[order]
    eigenvectors = eigenvectors[:, order]
    explained = eigenvalues / eigenvalues.sum()
    count = n_components or min(5, standardized.shape[1])
    components = []
    for idx in range(min(count, len(eigenvalues))):
        loadings = {col: float(eigenvectors[i, idx]) for i, col in enumerate(standardized.columns)}
        components.append({"component": idx + 1, "explained_variance_pct": float(explained[idx] * 100), "loadings": loadings})
    return {
        "available": True,
        "components": components,
        "effective_factor_count": int(np.sum(explained > 0.05)),
        "interpretation": "PCA compresses correlated signals into fewer independent factors.",
    }


def factor_research_report(price_data: Dict[str, pd.DataFrame], factor_prices: Dict[str, pd.DataFrame]) -> Dict:
    asset_prices = pd.DataFrame({ticker: df["Close"] for ticker, df in price_data.items() if df is not None and not df.empty and "Close" in df})
    factor_frame = pd.DataFrame({name: df["Close"] for name, df in factor_prices.items() if df is not None and not df.empty and "Close" in df})
    asset_returns = _returns(asset_prices)
    factor_returns = _returns(factor_frame)
    reports = {}
    for ticker in asset_returns.columns:
        reports[ticker] = multi_factor_regression(asset_returns[ticker], factor_returns)
    return {"assets": reports, "pca": pca_factor_model(asset_returns), "factor_names": list(factor_returns.columns)}


def _percentile(series: pd.Series, higher_is_better: bool = True) -> pd.Series:
    numeric = pd.to_numeric(series, errors="coerce")
    ranked = numeric.rank(pct=True, method="average") * 100
    return ranked if higher_is_better else 100 - ranked


def cross_sectional_factor_scores(features: pd.DataFrame) -> Dict:
    """Score economically distinct factors from 0-100 across a universe."""
    if features is None or features.empty:
        return {"available": False, "message": "No cross-sectional features supplied."}

    frame = features.copy()
    components = {
        "momentum": [
            ("return_12m_ex_1m", True),
            ("return_6m", True),
            ("relative_strength", True),
        ],
        "value": [
            ("forward_pe", False),
            ("price_to_book", False),
            ("ev_to_ebitda", False),
            ("fcf_yield", True),
        ],
        "quality": [
            ("return_on_equity", True),
            ("return_on_invested_capital", True),
            ("gross_margin", True),
            ("operating_margin", True),
            ("debt_to_equity", False),
        ],
        "growth": [
            ("revenue_growth", True),
            ("earnings_growth", True),
            ("free_cashflow_growth", True),
            ("analyst_revision", True),
        ],
        "low_volatility": [
            ("annualized_volatility", False),
            ("maximum_drawdown", True),
            ("downside_deviation", False),
        ],
    }

    factor_scores = pd.DataFrame(index=frame.index)
    coverage = pd.DataFrame(index=frame.index)
    for factor, definitions in components.items():
        ranked_inputs = []
        for column, higher_is_better in definitions:
            if column in frame:
                ranked_inputs.append(_percentile(frame[column], higher_is_better).rename(column))
        if ranked_inputs:
            ranked = pd.concat(ranked_inputs, axis=1)
            factor_scores[factor] = ranked.mean(axis=1, skipna=True)
            coverage[factor] = ranked.notna().mean(axis=1)
        else:
            factor_scores[factor] = np.nan
            coverage[factor] = 0.0

    factor_scores["composite"] = sum(
        factor_scores[name].fillna(50) * weight for name, weight in FACTOR_WEIGHTS.items()
    )
    factor_scores["coverage"] = coverage.mean(axis=1)

    rows = {}
    for ticker, row in factor_scores.iterrows():
        scores = {
            name: round(float(row[name]), 1) if pd.notna(row[name]) else None
            for name in FACTOR_WEIGHTS
        }
        contributions = {
            name: round((scores[name] if scores[name] is not None else 50) * weight, 2)
            for name, weight in FACTOR_WEIGHTS.items()
        }
        confidence = float(row["coverage"] * 100)
        rows[str(ticker)] = {
            "scores": scores,
            "composite_score": round(float(row["composite"]), 1),
            "contributions": contributions,
            "data_coverage_pct": round(confidence, 1),
            "explanation": _factor_explanation(scores),
            "research_mindset": research_mindset.research_envelope(
                "multi_factor",
                "The stock ranks well when several economically distinct factors agree.",
                _factor_evidence(scores),
                [
                    "Cross-sectional inputs are comparable across the universe.",
                    "Accounting fields are current and adjusted for one-off events.",
                    "Factor relationships remain stable over the evaluation horizon.",
                ],
                [
                    "Crowded factors can unwind together.",
                    "Cheap securities can remain value traps.",
                    "Growth and momentum can reverse after expectations peak.",
                ],
                confidence=min(confidence, 90),
                regime_weaknesses=["Factor crowding", "Liquidity shock", "Rapid regime transition"],
            ),
        }

    leaderboard = sorted(
        [{"ticker": ticker, **payload} for ticker, payload in rows.items()],
        key=lambda item: item["composite_score"],
        reverse=True,
    )
    return {
        "available": True,
        "factor_weights": FACTOR_WEIGHTS,
        "stocks": rows,
        "leaderboard": leaderboard,
    }


def portfolio_factor_exposure(factor_payload: Dict, weights: Dict[str, float] | pd.Series) -> Dict:
    stocks = factor_payload.get("stocks", {}) if factor_payload else {}
    weights = pd.Series(weights, dtype=float)
    weights = weights[weights.index.isin(stocks)]
    if weights.empty:
        return {"available": False, "message": "No weighted holdings matched factor scores."}
    weights = weights / weights.sum()

    exposures = {}
    contributions = {}
    for factor in FACTOR_WEIGHTS:
        values = pd.Series(
            {ticker: stocks[ticker]["scores"].get(factor) for ticker in weights.index},
            dtype=float,
        ).fillna(50)
        exposures[factor] = round(float((values * weights).sum()), 2)
        contributions[factor] = {
            ticker: round(float(weights[ticker] * values[ticker]), 2)
            for ticker in weights.index
        }

    return {
        "available": True,
        "exposures": exposures,
        "holding_contributions": contributions,
        "warnings": factor_concentration_warnings(exposures, contributions),
        "concentration_score": round(max(exposures.values()) - min(exposures.values()), 2),
    }


def factor_concentration_warnings(exposures: Dict[str, float], contributions: Dict[str, Dict[str, float]]) -> list:
    warnings = []
    for factor, score in exposures.items():
        if score >= 75:
            warnings.append(f"Portfolio has a strong {factor} tilt ({score:.1f}/100).")
        holdings = contributions.get(factor, {})
        if holdings:
            total = sum(abs(value) for value in holdings.values()) or 1
            ticker, value = max(holdings.items(), key=lambda item: abs(item[1]))
            if abs(value) / total >= 0.35:
                warnings.append(f"{ticker} supplies at least 35% of portfolio {factor} exposure.")
    return warnings


def historical_factor_performance(factor_scores: pd.Series, forward_returns: pd.Series, quantiles: int = 5) -> Dict:
    aligned = pd.concat(
        [factor_scores.rename("score"), forward_returns.rename("forward_return")],
        axis=1,
    ).dropna()
    if len(aligned) < quantiles * 10:
        return {"available": False, "message": "Insufficient factor history."}
    aligned["bucket"] = pd.qcut(aligned["score"], quantiles, labels=False, duplicates="drop")
    bucket_returns = aligned.groupby("bucket")["forward_return"].mean()
    spread = float(bucket_returns.iloc[-1] - bucket_returns.iloc[0]) if len(bucket_returns) >= 2 else 0.0
    information_coefficient = aligned["score"].corr(aligned["forward_return"], method="spearman")
    return {
        "available": True,
        "bucket_returns": {str(key): float(value) for key, value in bucket_returns.items()},
        "top_minus_bottom_return": spread,
        "information_coefficient": float(information_coefficient) if pd.notna(information_coefficient) else None,
        "positive_monotonicity": bool(bucket_returns.is_monotonic_increasing),
    }


def _factor_evidence(scores: Dict) -> list:
    ranked = sorted(
        [(name, value) for name, value in scores.items() if value is not None],
        key=lambda item: item[1],
        reverse=True,
    )
    return [f"{name.replace('_', ' ').title()}: {value:.1f}/100" for name, value in ranked[:3]]


def _factor_explanation(scores: Dict) -> str:
    evidence = _factor_evidence(scores)
    return "Primary score drivers: " + ", ".join(evidence) if evidence else "Factor coverage is insufficient."
