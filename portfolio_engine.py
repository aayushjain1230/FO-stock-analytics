import json
import math
import os
from datetime import datetime
from itertools import combinations
from typing import Dict, Iterable, Optional

import numpy as np
import pandas as pd
from scipy.optimize import minimize

import quant_analytics


TRADING_DAYS = 252
PORTFOLIO_FILE = "portfolio.json"


def load_portfolio(path: str = PORTFOLIO_FILE, fallback_tickers: Optional[Iterable[str]] = None) -> Dict:
    if os.path.exists(path):
        with open(path, "r") as f:
            payload = json.load(f)
        positions = payload.get("positions", payload if isinstance(payload, list) else [])
    else:
        positions = [{"ticker": ticker, "weight": 1} for ticker in (fallback_tickers or [])]

    clean = []
    for item in positions:
        ticker = str(item.get("ticker", "")).upper().strip()
        if not ticker:
            continue
        clean.append(
            {
                "ticker": ticker,
                "shares": _safe_float(item.get("shares")),
                "cost_basis": _safe_float(item.get("cost_basis")),
                "sector": item.get("sector", "Unknown"),
                "weight": _safe_float(item.get("weight")),
            }
        )
    return {"positions": clean}


def price_frame_from_data(price_data: Dict[str, pd.DataFrame]) -> pd.DataFrame:
    prices = {}
    for ticker, df in price_data.items():
        if df is None or df.empty:
            continue
        prices[ticker] = quant_analytics._as_price_series(df)
    return pd.DataFrame(prices).dropna(how="all").ffill().dropna(axis=1, how="all")


def weights_from_positions(positions: Iterable[Dict], price_frame: pd.DataFrame) -> pd.Series:
    positions = list(positions)
    if price_frame.empty:
        return pd.Series(dtype=float)
    tickers = [ticker for ticker in price_frame.columns if any(pos["ticker"] == ticker for pos in positions)]
    if not tickers:
        tickers = list(price_frame.columns)

    values = {}
    explicit_weights = {}
    for pos in positions:
        ticker = pos["ticker"]
        if ticker not in price_frame.columns:
            continue
        if pos.get("shares") is not None:
            values[ticker] = pos["shares"] * price_frame[ticker].dropna().iloc[-1]
        elif pos.get("weight") is not None:
            explicit_weights[ticker] = pos["weight"]

    if values:
        weights = pd.Series(values, dtype=float)
    elif explicit_weights:
        weights = pd.Series(explicit_weights, dtype=float)
    else:
        weights = pd.Series(1.0, index=tickers, dtype=float)

    weights = weights[weights.index.isin(price_frame.columns)]
    total = weights.sum()
    if total <= 0:
        return pd.Series(1 / len(tickers), index=tickers, dtype=float)
    return weights / total



def portfolio_drift_monitor(positions: Iterable[Dict], weights: pd.Series, drift_threshold_pct: float = 2.0) -> Dict:
    target_weights = {}
    for position in positions:
        ticker = position.get("ticker")
        target = position.get("target_weight", position.get("weight"))
        if ticker and target is not None:
            target_weights[ticker] = float(target)
    if not target_weights and not weights.empty:
        target_weights = {ticker: 1 / len(weights) for ticker in weights.index}
    rows = []
    for ticker, current_weight in weights.items():
        target = target_weights.get(ticker, 1 / len(weights) if len(weights) else 0)
        drift_pct = (float(current_weight) - float(target)) * 100
        rows.append({
            "ticker": ticker,
            "current_weight_pct": round(float(current_weight) * 100, 2),
            "target_weight_pct": round(float(target) * 100, 2),
            "drift_pct": round(drift_pct, 2),
            "status": "rebalance_watch" if abs(drift_pct) >= drift_threshold_pct else "in_band",
        })
    alerts = [row for row in rows if row["status"] == "rebalance_watch"]
    return {
        "threshold_pct": drift_threshold_pct,
        "alerts": alerts,
        "positions": sorted(rows, key=lambda row: abs(row["drift_pct"]), reverse=True),
        "summary": f"{len(alerts)} positions drifted by at least {drift_threshold_pct:.1f}% from target.",
    }

def portfolio_returns(price_frame: pd.DataFrame, weights: pd.Series) -> pd.Series:
    returns = price_frame[weights.index].pct_change().dropna(how="all").fillna(0)
    return returns.dot(weights)


def portfolio_variance_metrics(price_frame: pd.DataFrame, weights: pd.Series) -> Dict:
    returns = price_frame[weights.index].pct_change().dropna()
    covariance = returns.cov()
    daily_variance = float(weights.T @ covariance.values @ weights)
    daily_vol = math.sqrt(max(daily_variance, 0))
    return {
        "daily_variance": daily_variance,
        "monthly_variance": daily_variance * 21,
        "annual_variance": daily_variance * TRADING_DAYS,
        "daily_volatility": daily_vol,
        "monthly_volatility": daily_vol * math.sqrt(21),
        "annual_volatility": daily_vol * math.sqrt(TRADING_DAYS),
        "risk_classification": classify_volatility(daily_vol * math.sqrt(TRADING_DAYS)),
        "covariance_matrix": covariance.round(8).to_dict(),
    }


def correlation_analysis(price_frame: pd.DataFrame, weights: pd.Series) -> Dict:
    returns = price_frame[weights.index].pct_change().dropna()
    corr = returns.corr()
    pairs = []
    for left, right in combinations(corr.columns, 2):
        value = corr.loc[left, right]
        if pd.notna(value):
            pairs.append({"pair": [left, right], "correlation": round(float(value), 4)})
    pairs = sorted(pairs, key=lambda item: item["correlation"], reverse=True)
    average_corr = float(np.mean([item["correlation"] for item in pairs])) if pairs else 0.0
    redundant = [item for item in pairs if item["correlation"] >= 0.80]
    opportunities = [item for item in sorted(pairs, key=lambda item: item["correlation"]) if item["correlation"] <= 0.30]
    return {
        "correlation_matrix": corr.round(4).to_dict(),
        "average_correlation": round(average_corr, 4),
        "highest_correlated_pairs": pairs[:5],
        "lowest_correlated_pairs": opportunities[:5],
        "redundant_holdings": redundant[:5],
        "diversification_opportunities": opportunities[:5],
    }


def sharpe_engine(portfolio_return_series: pd.Series, risk_free_rate: float = 0.045) -> Dict:
    annual_return = quant_analytics.cagr(portfolio_return_series)
    annual_vol = portfolio_return_series.std() * math.sqrt(TRADING_DAYS)
    sharpe = quant_analytics.sharpe_ratio(portfolio_return_series, risk_free_rate=risk_free_rate)
    return {
        "portfolio_return": annual_return,
        "risk_free_rate": risk_free_rate,
        "portfolio_volatility": _safe_float(annual_vol, 0.0),
        "sharpe_ratio": sharpe,
        "classification": classify_sharpe(sharpe),
    }


def risk_contributions(price_frame: pd.DataFrame, weights: pd.Series) -> Dict:
    returns = price_frame[weights.index].pct_change().dropna()
    covariance = returns.cov().values
    weight_values = weights.values
    portfolio_variance = float(weight_values.T @ covariance @ weight_values)
    if portfolio_variance <= 0:
        return {ticker: 0.0 for ticker in weights.index}
    marginal = covariance @ weight_values
    contribution = weight_values * marginal / portfolio_variance
    return {ticker: round(float(value * 100), 2) for ticker, value in zip(weights.index, contribution)}


def diversification_score(weights: pd.Series, average_correlation: float, sector_exposure: Dict[str, float]) -> Dict:
    holding_count_score = min(len(weights) / 20, 1) * 25
    concentration_score = (1 - float((weights**2).sum())) * 35
    correlation_score = max(0, 1 - average_correlation) * 25
    sector_max = max(sector_exposure.values()) if sector_exposure else 1
    sector_score = max(0, 1 - sector_max) * 15
    score = max(0, min(100, holding_count_score + concentration_score + correlation_score + sector_score))
    return {
        "score": round(float(score), 2),
        "classification": classify_diversification(score),
        "position_concentration": round(float((weights**2).sum() * 100), 2),
        "largest_position": {"ticker": weights.idxmax(), "weight": round(float(weights.max() * 100), 2)} if not weights.empty else {},
        "largest_sector_weight": round(float(sector_max * 100), 2) if sector_exposure else None,
    }


def portfolio_health_score(metrics: Dict) -> Dict:
    vol = metrics["variance"]["annual_volatility"]
    sharpe = metrics["sharpe"]["sharpe_ratio"]
    drawdown = abs(metrics["maximum_drawdown"])
    avg_corr = metrics["correlation"]["average_correlation"]
    div = metrics["diversification"]["score"]
    top_risk = max(metrics["risk_contributions"].values()) if metrics["risk_contributions"] else 0

    score = 100
    score -= min(vol / 0.50, 1) * 20
    score += min(max(sharpe, 0) / 2, 1) * 20
    score -= min(drawdown / 0.40, 1) * 20
    score -= min(avg_corr / 0.90, 1) * 15
    score += div * 0.25
    if top_risk > 40:
        score -= 15
    elif top_risk > 30:
        score -= 8
    score = round(float(max(0, min(100, score))), 2)

    strengths = []
    weaknesses = []
    if sharpe >= 1:
        strengths.append("Strong risk-adjusted return")
    else:
        weaknesses.append("Sharpe ratio is not yet strong")
    if div >= 70:
        strengths.append("Diversification profile is healthy")
    else:
        weaknesses.append("Diversification can improve")
    if drawdown <= 0.15:
        strengths.append("Drawdown has been contained")
    else:
        weaknesses.append("Drawdown risk is meaningful")
    if top_risk > 35:
        weaknesses.append("One position contributes too much portfolio risk")

    return {"score": score, "classification": classify_health(score), "strengths": strengths, "weaknesses": weaknesses}


def time_series_monitor(price_frame: pd.DataFrame, weights: pd.Series) -> Dict:
    returns = portfolio_returns(price_frame, weights)
    rolling_vol = returns.rolling(21).std() * math.sqrt(TRADING_DAYS)
    rolling_sharpe = returns.rolling(63).mean() / returns.rolling(63).std() * math.sqrt(TRADING_DAYS)
    rolling_corr = price_frame[weights.index].pct_change().rolling(63).corr()
    latest_corr = _rolling_average_correlation(rolling_corr, weights.index)
    drawdown = (1 + returns).cumprod() / (1 + returns).cumprod().cummax() - 1
    return {
        "volatility_trend": _trend_label(rolling_vol),
        "sharpe_trend": _trend_label(rolling_sharpe, higher_is_better=True),
        "drawdown_trend": _trend_label(drawdown.abs()),
        "correlation_trend": latest_corr.get("trend", "Unknown"),
        "latest_rolling_volatility": _safe_float(rolling_vol.dropna().iloc[-1] if not rolling_vol.dropna().empty else None),
        "latest_rolling_sharpe": _safe_float(rolling_sharpe.dropna().iloc[-1] if not rolling_sharpe.dropna().empty else None),
        "latest_rolling_correlation": latest_corr.get("latest"),
    }


def forecasting_engine(price_frame: pd.DataFrame, weights: pd.Series) -> Dict:
    returns = portfolio_returns(price_frame, weights)
    current_vol = returns.tail(21).std() * math.sqrt(TRADING_DAYS)
    expected_vol = returns.tail(63).std() * math.sqrt(TRADING_DAYS)
    autocorr = returns.autocorr(lag=1)
    if expected_vol > current_vol * 1.15:
        risk_direction = "Increasing"
    elif expected_vol < current_vol * 0.85:
        risk_direction = "Decreasing"
    else:
        risk_direction = "Stable"
    return {
        "autocorrelation_1d": _safe_float(autocorr, 0.0),
        "momentum_state": "Momentum continuing" if autocorr and autocorr > 0.05 else "Mean reversion likely" if autocorr and autocorr < -0.05 else "No clear short-term edge",
        "current_volatility": _safe_float(current_vol, 0.0),
        "expected_volatility": _safe_float(expected_vol, 0.0),
        "risk_direction": risk_direction,
        "interpretation": f"The portfolio risk direction is {risk_direction.lower()} based on recent realized volatility.",
    }


def monte_carlo_simulation(price_frame: pd.DataFrame, weights: pd.Series, horizon_days: int = 126, simulations: int = 10000, target_return: float = 0.10, drawdown_threshold: float = -0.15, seed: int = 42) -> Dict:
    returns = price_frame[weights.index].pct_change().dropna()
    mean = returns.mean().values
    covariance = returns.cov().values
    rng = np.random.default_rng(seed)
    paths = np.zeros((simulations, horizon_days + 1))
    paths[:, 0] = 1.0
    for day in range(1, horizon_days + 1):
        sampled = rng.multivariate_normal(mean, covariance, simulations)
        portfolio_daily = sampled @ weights.values
        paths[:, day] = paths[:, day - 1] * (1 + portfolio_daily)
    terminal_returns = paths[:, -1] - 1
    max_drawdowns = np.min(paths / np.maximum.accumulate(paths, axis=1) - 1, axis=1)
    return {
        "horizon_days": horizon_days,
        "simulations": simulations,
        "expected_return": float(np.mean(terminal_returns)),
        "best_case_p95": float(np.percentile(terminal_returns, 95)),
        "worst_case_p05": float(np.percentile(terminal_returns, 5)),
        "probability_of_loss": float((terminal_returns < 0).mean()),
        "probability_of_hitting_target": float((terminal_returns >= target_return).mean()),
        "probability_of_large_drawdown": float((max_drawdowns <= drawdown_threshold).mean()),
    }


def optimize_portfolio(price_frame: pd.DataFrame, weights: pd.Series, risk_free_rate: float = 0.045) -> Dict:
    returns = price_frame[weights.index].pct_change().dropna()
    expected = returns.mean().values * TRADING_DAYS
    covariance = returns.cov().values * TRADING_DAYS
    n = len(weights)
    if n == 0:
        return {}

    def negative_sharpe(candidate):
        ret = candidate @ expected
        vol = math.sqrt(max(candidate.T @ covariance @ candidate, 1e-12))
        return -((ret - risk_free_rate) / vol)

    result = minimize(negative_sharpe, np.repeat(1 / n, n), bounds=[(0, 1)] * n, constraints={"type": "eq", "fun": lambda x: x.sum() - 1})
    optimized = result.x if result.success else np.repeat(1 / n, n)
    current_returns = portfolio_returns(price_frame, weights)
    optimized_returns = returns.dot(optimized)
    current_vol = current_returns.std() * math.sqrt(TRADING_DAYS)
    optimized_vol = optimized_returns.std() * math.sqrt(TRADING_DAYS)
    return {
        "current_volatility": _safe_float(current_vol, 0.0),
        "optimized_volatility": _safe_float(optimized_vol, 0.0),
        "current_sharpe": quant_analytics.sharpe_ratio(current_returns, risk_free_rate),
        "optimized_sharpe": quant_analytics.sharpe_ratio(optimized_returns, risk_free_rate),
        "risk_reduction_opportunity": _safe_float((current_vol - optimized_vol) / current_vol if current_vol else 0.0, 0.0),
        "optimized_weights": {ticker: round(float(weight * 100), 2) for ticker, weight in zip(weights.index, optimized)},
        "note": "Educational Markowitz comparison only; not investment advice.",
    }


def factor_exposure(price_frame: pd.DataFrame, weights: pd.Series, factor_prices: Optional[Dict[str, pd.DataFrame]] = None) -> Dict:
    if not factor_prices:
        return {"message": "Factor data unavailable."}
    port = portfolio_returns(price_frame, weights).dropna()
    factors = {}
    for name, df in factor_prices.items():
        if df is not None and not df.empty:
            factors[name] = quant_analytics._as_price_series(df).pct_change()
    factor_frame = pd.DataFrame(factors).dropna()
    aligned = pd.concat([port.rename("portfolio"), factor_frame], axis=1).dropna()
    if len(aligned) < 30:
        return {"message": "Not enough aligned factor data."}
    y = aligned["portfolio"].values
    x = aligned.drop(columns=["portfolio"]).values
    x = np.column_stack([np.ones(len(x)), x])
    coefficients = np.linalg.lstsq(x, y, rcond=None)[0][1:]
    raw = {name: abs(float(value)) for name, value in zip(aligned.drop(columns=["portfolio"]).columns, coefficients)}
    total = sum(raw.values()) or 1
    return {
        "main_risk_drivers": {name: round(value / total * 100, 2) for name, value in raw.items()},
        "interpretation": "Factor exposures are estimated with linear regression on ETF proxies.",
    }


def generate_portfolio_report(positions: Iterable[Dict], price_data: Dict[str, pd.DataFrame], benchmark_df: Optional[pd.DataFrame] = None, factor_prices: Optional[Dict[str, pd.DataFrame]] = None, risk_free_rate: float = 0.045) -> Dict:
    price_frame = price_frame_from_data(price_data)
    weights = weights_from_positions(positions, price_frame)
    if price_frame.empty or weights.empty:
        return {"error": "No usable portfolio price data."}
    returns = portfolio_returns(price_frame, weights)
    sector_exposure = _sector_exposure(positions, weights)
    variance = portfolio_variance_metrics(price_frame, weights)
    corr = correlation_analysis(price_frame, weights)
    sharpe = sharpe_engine(returns, risk_free_rate=risk_free_rate)
    risk_contrib = risk_contributions(price_frame, weights)
    diversification = diversification_score(weights, corr["average_correlation"], sector_exposure)
    metrics = {
        "generated_at": datetime.now().isoformat(),
        "positions": [{"ticker": ticker, "weight": round(float(weight * 100), 2)} for ticker, weight in weights.items()],
        "portfolio_return": sharpe["portfolio_return"],
        "variance": variance,
        "correlation": corr,
        "sharpe": sharpe,
        "maximum_drawdown": quant_analytics.max_drawdown(returns),
        "risk_contributions": risk_contrib,
        "sector_exposure": {key: round(float(value * 100), 2) for key, value in sector_exposure.items()},
        "drift_monitor": portfolio_drift_monitor(positions, weights),
        "diversification": diversification,
        "time_series_monitor": time_series_monitor(price_frame, weights),
        "forecast": forecasting_engine(price_frame, weights),
        "monte_carlo": monte_carlo_simulation(price_frame, weights),
        "optimization": optimize_portfolio(price_frame, weights, risk_free_rate=risk_free_rate),
        "factor_exposure": factor_exposure(price_frame, weights, factor_prices=factor_prices),
    }
    metrics["portfolio_health"] = portfolio_health_score(metrics)
    metrics["why_now"] = portfolio_why_now(metrics)
    metrics["report"] = portfolio_intelligence_text(metrics)
    return metrics


def portfolio_why_now(metrics: Dict, previous: Optional[Dict] = None) -> Dict:
    triggers = []
    monitor = metrics.get("time_series_monitor", {})
    if monitor.get("volatility_trend") == "Rising":
        triggers.append(("Portfolio volatility spike", "Rolling volatility is rising.", 75))
    if monitor.get("correlation_trend") == "Rising":
        triggers.append(("Correlation spike", "Holdings are moving together more than before.", 75))
    if metrics["diversification"]["score"] < 50:
        triggers.append(("Diversification weakness", "Diversification score is below 50.", 65))
    top_risk = max(metrics.get("risk_contributions", {}).values(), default=0)
    if top_risk >= 40:
        triggers.append(("Risk concentration", "One position contributes at least 40% of portfolio risk.", 80))
    if metrics.get("forecast", {}).get("risk_direction") == "Increasing":
        triggers.append(("Forecasted volatility increase", "Expected volatility is above current volatility.", 70))

    best = max(triggers, key=lambda item: item[2], default=None)
    if not best:
        return {"send_alert": False, "reason": "No clear portfolio Why Now trigger", "evidence": "Portfolio risk did not materially change.", "strength": 0}
    return {"send_alert": True, "reason": best[0], "evidence": best[1], "strength": best[2], "what_to_watch": "Monitor sector weakness, correlation, drawdown, and top risk contributors."}


def portfolio_intelligence_text(metrics: Dict) -> str:
    health = metrics["portfolio_health"]
    top_risk = sorted(metrics["risk_contributions"].items(), key=lambda item: item[1], reverse=True)[:3]
    top_risk_text = ", ".join(f"{ticker}: {value:.1f}%" for ticker, value in top_risk)
    why = metrics["why_now"]
    return (
        f"Portfolio Health Score: {health['score']}/100 ({health['classification']}). "
        f"Annual volatility is {metrics['variance']['annual_volatility'] * 100:.2f}%, Sharpe is {metrics['sharpe']['sharpe_ratio']:.2f}, "
        f"max drawdown is {metrics['maximum_drawdown'] * 100:.2f}%, and average correlation is {metrics['correlation']['average_correlation']:.2f}. "
        f"Top risk contributors: {top_risk_text or 'N/A'}. "
        f"Why Now: {why['reason']}. What to watch next: {why.get('what_to_watch', 'Risk, correlation, and drawdown trends.')}"
    )


def classify_volatility(annual_volatility: float) -> str:
    if annual_volatility < 0.08:
        return "Very Low"
    if annual_volatility < 0.15:
        return "Low"
    if annual_volatility < 0.25:
        return "Moderate"
    if annual_volatility < 0.40:
        return "High"
    return "Very High"


def classify_sharpe(sharpe: float) -> str:
    if sharpe < 0.5:
        return "Weak"
    if sharpe < 1.0:
        return "Average"
    if sharpe < 2.0:
        return "Strong"
    return "Excellent"


def classify_diversification(score: float) -> str:
    if score >= 85:
        return "Excellent"
    if score >= 70:
        return "Good"
    if score >= 50:
        return "Moderate"
    if score >= 30:
        return "Weak"
    return "Poor"


def classify_health(score: float) -> str:
    if score >= 85:
        return "Excellent"
    if score >= 70:
        return "Good"
    if score >= 50:
        return "Moderate"
    if score >= 30:
        return "Weak"
    return "Poor"


def _sector_exposure(positions: Iterable[Dict], weights: pd.Series) -> Dict[str, float]:
    sector_map = {pos["ticker"]: pos.get("sector", "Unknown") for pos in positions}
    exposure = {}
    for ticker, weight in weights.items():
        sector = sector_map.get(ticker, "Unknown")
        exposure[sector] = exposure.get(sector, 0.0) + float(weight)
    return exposure


def _trend_label(series: pd.Series, higher_is_better: bool = False) -> str:
    clean = series.dropna()
    if len(clean) < 10:
        return "Unknown"
    recent = clean.tail(5).mean()
    prior = clean.tail(20).head(10).mean()
    if recent > prior * 1.10:
        return "Improving" if higher_is_better else "Rising"
    if recent < prior * 0.90:
        return "Deteriorating" if higher_is_better else "Falling"
    return "Stable"


def _rolling_average_correlation(rolling_corr, tickers) -> Dict:
    try:
        matrices = []
        for date in rolling_corr.index.get_level_values(0).unique()[-20:]:
            matrix = rolling_corr.loc[date]
            values = [matrix.loc[a, b] for a, b in combinations(tickers, 2) if a in matrix.index and b in matrix.columns]
            if values:
                matrices.append(float(np.nanmean(values)))
        if len(matrices) < 5:
            return {"latest": None, "trend": "Unknown"}
        latest = np.mean(matrices[-5:])
        prior = np.mean(matrices[:10])
        trend = "Rising" if latest > prior * 1.10 else "Falling" if latest < prior * 0.90 else "Stable"
        return {"latest": round(float(latest), 4), "trend": trend}
    except Exception:
        return {"latest": None, "trend": "Unknown"}


def _safe_float(value, default=None):
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default
