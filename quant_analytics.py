import math
from typing import Dict, Iterable, Optional

import numpy as np
import pandas as pd


TRADING_DAYS = 252


def _as_price_series(df: pd.DataFrame) -> pd.Series:
    if isinstance(df, pd.Series):
        return pd.to_numeric(df, errors="coerce").dropna()
    for col in ("Adj Close", "Close", "close"):
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").dropna()
    raise KeyError("No Close or Adj Close column found")


def safe_float(value, default=None):
    try:
        if value is None or pd.isna(value):
            return default
        return float(value)
    except Exception:
        return default


def daily_returns(df: pd.DataFrame) -> pd.Series:
    return _as_price_series(df).pct_change().dropna()


def period_returns(df: pd.DataFrame) -> Dict[str, float]:
    close = _as_price_series(df)
    if close.empty:
        return {"daily": 0.0, "weekly": 0.0, "monthly": 0.0, "annual": 0.0}

    returns = {
        "daily": close.pct_change().iloc[-1] if len(close) > 1 else 0.0,
        "weekly": close.pct_change(5).iloc[-1] if len(close) > 5 else np.nan,
        "monthly": close.pct_change(21).iloc[-1] if len(close) > 21 else np.nan,
        "annual": close.pct_change(TRADING_DAYS).iloc[-1] if len(close) > TRADING_DAYS else np.nan,
    }
    return {key: safe_float(value, 0.0) for key, value in returns.items()}


def max_drawdown(return_series: pd.Series) -> float:
    if return_series.empty:
        return 0.0
    equity = (1 + return_series.fillna(0)).cumprod()
    peak = equity.cummax()
    drawdown = equity / peak - 1
    return safe_float(drawdown.min(), 0.0)


def cagr(return_series: pd.Series, periods_per_year: int = TRADING_DAYS) -> float:
    returns = return_series.dropna()
    if returns.empty:
        return 0.0
    total_return = (1 + returns).prod()
    years = len(returns) / periods_per_year
    if years <= 0 or total_return <= 0:
        return 0.0
    return safe_float(total_return ** (1 / years) - 1, 0.0)


def sharpe_ratio(return_series: pd.Series, risk_free_rate: float = 0.0, periods_per_year: int = TRADING_DAYS) -> float:
    returns = return_series.dropna()
    if returns.empty:
        return 0.0
    excess = returns - risk_free_rate / periods_per_year
    volatility = excess.std()
    if volatility == 0 or pd.isna(volatility):
        return 0.0
    return safe_float(math.sqrt(periods_per_year) * excess.mean() / volatility, 0.0)


def sortino_ratio(return_series: pd.Series, risk_free_rate: float = 0.0, periods_per_year: int = TRADING_DAYS) -> float:
    returns = return_series.dropna()
    if returns.empty:
        return 0.0
    excess = returns - risk_free_rate / periods_per_year
    downside = excess[excess < 0].std()
    if downside == 0 or pd.isna(downside):
        return 0.0
    return safe_float(math.sqrt(periods_per_year) * excess.mean() / downside, 0.0)


def calmar_ratio(return_series: pd.Series) -> float:
    dd = abs(max_drawdown(return_series))
    if dd == 0:
        return 0.0
    return safe_float(cagr(return_series) / dd, 0.0)


def beta_alpha(stock_returns: pd.Series, benchmark_returns: pd.Series, risk_free_rate: float = 0.0) -> Dict[str, float]:
    aligned = pd.concat([stock_returns, benchmark_returns], axis=1).dropna()
    aligned.columns = ["stock", "benchmark"]
    if len(aligned) < 2:
        return {"beta": 0.0, "alpha": 0.0, "capm_expected_return": 0.0, "actual_return": 0.0, "market_return": 0.0}

    benchmark_var = aligned["benchmark"].var()
    beta = aligned["stock"].cov(aligned["benchmark"]) / benchmark_var if benchmark_var else 0.0
    market_return = aligned["benchmark"].mean() * TRADING_DAYS
    actual_return = aligned["stock"].mean() * TRADING_DAYS
    capm_expected = risk_free_rate + beta * (market_return - risk_free_rate)
    alpha = actual_return - capm_expected
    return {
        "beta": safe_float(beta, 0.0),
        "alpha": safe_float(alpha, 0.0),
        "capm_expected_return": safe_float(capm_expected, 0.0),
        "actual_return": safe_float(actual_return, 0.0),
        "market_return": safe_float(market_return, 0.0),
    }


def capm_interpretation(beta_payload: Dict[str, float]) -> str:
    beta = safe_float(beta_payload.get("beta"), 0.0)
    alpha = safe_float(beta_payload.get("alpha"), 0.0)
    capm_return = safe_float(beta_payload.get("capm_expected_return"), 0.0)
    if beta >= 1.25:
        beta_text = f"Moves roughly {(beta - 1) * 100:.0f}% more than the market."
    elif beta <= 0.75:
        beta_text = f"Moves roughly {(1 - beta) * 100:.0f}% less than the market."
    else:
        beta_text = "Moves roughly in line with the market."
    alpha_text = "positive alpha versus CAPM" if alpha > 0 else "negative alpha versus CAPM" if alpha < 0 else "no CAPM alpha detected"
    return f"Beta {beta:.2f}: {beta_text} CAPM expected return is {capm_return:.2%}; realized alpha is {alpha:.2%}, meaning {alpha_text}."


def factor_decomposition(beta_payload: Dict[str, float], momentum: Dict[str, float], risk: Dict[str, float]) -> Dict:
    actual = safe_float(beta_payload.get("actual_return"), 0.0)
    market_component = safe_float(beta_payload.get("capm_expected_return"), 0.0)
    alpha_component = safe_float(beta_payload.get("alpha"), 0.0)
    momentum_component = max(safe_float(momentum.get("12m"), 0.0), 0.0) * 0.25
    volatility_drag = -max(safe_float(risk.get("annualized_volatility"), 0.0) - 0.25, 0.0) * 0.20
    components = {
        "market_component": market_component,
        "momentum_component": momentum_component,
        "volatility_drag": volatility_drag,
        "alpha_component": alpha_component,
    }
    total_abs = sum(abs(value) for value in components.values()) or 1.0
    percent = {key: round(abs(value) / total_abs * 100, 2) for key, value in components.items()}
    return {
        "actual_return": actual,
        "components": components,
        "component_weights_pct": percent,
        "interpretation": "Decomposes realized return into market risk, momentum proxy, volatility drag, and residual alpha. This is a research approximation, not proof of causality.",
    }


def momentum_score(df: pd.DataFrame) -> Dict[str, float]:
    close = _as_price_series(df)
    horizons = {"3m": 63, "6m": 126, "12m": 252, "24m": 504}
    values = {}
    score = 0.0
    weight = 100 / len(horizons)
    for label, days in horizons.items():
        value = close.pct_change(days).iloc[-1] if len(close) > days else np.nan
        values[label] = safe_float(value, 0.0)
        if pd.notna(value) and value > 0:
            score += weight
    values["score"] = round(score, 2)
    return values


def realized_volatility(return_series: pd.Series, window: int = 21) -> pd.Series:
    return return_series.rolling(window).std() * math.sqrt(TRADING_DAYS)


def volatility_regime(return_series: pd.Series, window: int = 21) -> str:
    vol = realized_volatility(return_series, window=window).dropna()
    if len(vol) < 20:
        return "Unknown"
    latest = vol.iloc[-1]
    percentile = (vol <= latest).mean()
    if percentile >= 0.8:
        return "High volatility"
    if percentile <= 0.2:
        return "Low volatility"
    return "Normal volatility"


def comprehensive_stock_analysis(df: pd.DataFrame, benchmark_df: Optional[pd.DataFrame] = None, risk_free_rate: float = 0.0) -> Dict:
    returns = daily_returns(df)
    benchmark_returns = daily_returns(benchmark_df) if benchmark_df is not None else pd.Series(dtype=float)
    beta_payload = beta_alpha(returns, benchmark_returns, risk_free_rate) if not benchmark_returns.empty else {"beta": 0.0, "alpha": 0.0, "capm_expected_return": 0.0, "actual_return": 0.0, "market_return": 0.0}
    close = _as_price_series(df)

    annual_vol = safe_float(returns.std() * math.sqrt(TRADING_DAYS), 0.0)
    risk = {
        "volatility": safe_float(returns.std(), 0.0),
        "annualized_volatility": annual_vol,
        "maximum_drawdown": max_drawdown(returns),
        "sharpe_ratio": sharpe_ratio(returns, risk_free_rate),
        "sortino_ratio": sortino_ratio(returns, risk_free_rate),
        "calmar_ratio": calmar_ratio(returns),
        **beta_payload,
    }
    trend = {
        "sma20": safe_float(close.rolling(20).mean().iloc[-1]),
        "sma50": safe_float(close.rolling(50).mean().iloc[-1]),
        "sma200": safe_float(close.rolling(200).mean().iloc[-1]),
        "ema20": safe_float(close.ewm(span=20, adjust=False).mean().iloc[-1]),
        "ema50": safe_float(close.ewm(span=50, adjust=False).mean().iloc[-1]),
        "ema200": safe_float(close.ewm(span=200, adjust=False).mean().iloc[-1]),
    }
    volume = {}
    if "Volume" in df.columns:
        volume_series = pd.to_numeric(df["Volume"], errors="coerce")
        average_volume = volume_series.rolling(20).mean().iloc[-1]
        relative_volume = volume_series.iloc[-1] / average_volume if average_volume else np.nan
        volume = {
            "average_volume": safe_float(average_volume, 0.0),
            "relative_volume": safe_float(relative_volume, 0.0),
            "volume_spike": bool(pd.notna(relative_volume) and relative_volume >= 2.0),
        }

    momentum = momentum_score(df)
    score = quant_score(risk, trend, momentum, volume)
    return {
        "returns": period_returns(df),
        "risk": risk,
        "capm": {
            **beta_payload,
            "interpretation": capm_interpretation(beta_payload),
        },
        "factor_decomposition": factor_decomposition(beta_payload, momentum, risk),
        "trend": trend,
        "momentum": momentum,
        "volume": volume,
        "volatility_regime": volatility_regime(returns),
        "quant_score": score,
    }


def quant_score(risk: Dict, trend: Dict, momentum: Dict, volume: Dict) -> Dict:
    score = 0
    if safe_float(momentum.get("score"), 0) >= 75:
        score += 25
    elif safe_float(momentum.get("score"), 0) >= 50:
        score += 15

    close_above_trend = all(safe_float(trend.get(key), 0) > 0 for key in ("sma20", "sma50", "sma200"))
    if close_above_trend and safe_float(trend.get("sma20"), 0) > safe_float(trend.get("sma50"), 0) > safe_float(trend.get("sma200"), 0):
        score += 20

    if safe_float(risk.get("sharpe_ratio"), 0) > 1:
        score += 20
    elif safe_float(risk.get("sharpe_ratio"), 0) > 0:
        score += 10

    if safe_float(risk.get("maximum_drawdown"), 0) > -0.2:
        score += 15
    elif safe_float(risk.get("maximum_drawdown"), 0) > -0.35:
        score += 8

    if volume.get("volume_spike"):
        score += 10

    if safe_float(risk.get("annualized_volatility"), 1) <= 0.45:
        score += 10

    score = max(0, min(100, score))
    label = "Research priority" if score >= 75 else "Watch closely" if score >= 55 else "Neutral" if score >= 35 else "High risk / weak setup"
    return {"score": score, "label": label}


def portfolio_analytics(price_frame: pd.DataFrame, weights: Optional[Iterable[float]] = None, benchmark: Optional[pd.Series] = None) -> Dict:
    returns = price_frame.pct_change().dropna(how="all")
    if returns.empty:
        return {}
    if weights is None:
        weights = np.repeat(1 / len(returns.columns), len(returns.columns))
    weights = np.array(list(weights), dtype=float)
    weights = weights / weights.sum()
    portfolio_returns = returns.dot(weights)
    concentration = float(np.square(weights).sum())
    payload = {
        "portfolio_volatility": safe_float(portfolio_returns.std() * math.sqrt(TRADING_DAYS), 0.0),
        "portfolio_sharpe": sharpe_ratio(portfolio_returns),
        "maximum_drawdown": max_drawdown(portfolio_returns),
        "correlation_matrix": returns.corr().round(4).to_dict(),
        "diversification_score": round((1 - concentration) * 100, 2),
        "concentration_risk": round(concentration * 100, 2),
    }
    if benchmark is not None:
        payload.update(beta_alpha(portfolio_returns, benchmark.pct_change().dropna()))
    return payload


def value_at_risk(return_series: pd.Series, confidence: float = 0.95) -> float:
    returns = return_series.dropna()
    if returns.empty:
        return 0.0
    return safe_float(np.percentile(returns, (1 - confidence) * 100), 0.0)


def expected_shortfall(return_series: pd.Series, confidence: float = 0.95) -> float:
    returns = return_series.dropna()
    var = value_at_risk(returns, confidence)
    tail = returns[returns <= var]
    return safe_float(tail.mean(), var)
