"""Factor model research utilities."""

from typing import Dict, Iterable

import numpy as np
import pandas as pd

TRADING_DAYS = 252


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
