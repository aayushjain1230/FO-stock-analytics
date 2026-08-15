from typing import Dict

import numpy as np
import pandas as pd

import quant_analytics
import research_mindset


INDEX_SYMBOLS = {
    "sp500": "SPY",
    "nasdaq": "QQQ",
    "russell2000": "IWM",
    "dow": "DIA",
    "vix": "^VIX",
}

SECTOR_ETFS = {
    "Technology": "XLK",
    "Healthcare": "XLV",
    "Financials": "XLF",
    "Industrials": "XLI",
    "Energy": "XLE",
    "Consumer Discretionary": "XLY",
    "Consumer Staples": "XLP",
    "Utilities": "XLU",
    "Materials": "XLB",
    "Real Estate": "XLRE",
    "Communication Services": "XLC",
}


def classify_market(index_data: Dict[str, pd.DataFrame], sector_data: Dict[str, pd.DataFrame] | None = None) -> Dict:
    sector_data = sector_data or {}
    index_metrics = {name: _index_trend(df) for name, df in index_data.items() if df is not None and not df.empty}
    sector_metrics = {name: _sector_trend(df) for name, df in sector_data.items() if df is not None and not df.empty}
    health_score = _health_score(index_metrics, sector_metrics)
    vix_status = _vix_status(index_data.get("vix"))
    risk_environment = _risk_environment(index_metrics, sector_metrics, vix_status)
    heuristic_regime = _regime_label(index_metrics, health_score, risk_environment)
    statistical_regime = detect_statistical_regime(index_data.get("sp500"))
    regime = statistical_regime.get("current_regime", heuristic_regime)
    return {
        "regime": regime,
        "heuristic_regime": heuristic_regime,
        "regime_confidence": statistical_regime.get("confidence", 0),
        "regime_model": statistical_regime.get("model", "heuristic"),
        "transition_probabilities": statistical_regime.get("transition_probabilities", {}),
        "strategy_recommendation": strategy_for_regime(regime),
        "regime_research_mindset": statistical_regime.get("research_mindset", {}),
        "health_score": health_score,
        "risk_environment": risk_environment,
        "buy_environment": "Favorable" if health_score >= 65 and risk_environment != "Risk-off" else "Selective" if health_score >= 45 else "Dangerous",
        "vix_status": vix_status,
        "index_trends": index_metrics,
        "sector_rankings": sorted(sector_metrics.values(), key=lambda item: item["momentum_score"], reverse=True),
        "sector_participation": _sector_participation(sector_metrics),
        "risk_on_risk_off": _risk_on_score(sector_metrics),
        "breadth_proxy": _breadth_proxy(index_metrics),
    }


def detect_statistical_regime(spy_df: pd.DataFrame, n_states: int = 6) -> Dict:
    """
    Fit a diagonal-Gaussian HMM to market return, volatility, trend, and drawdown.

    A local K-Means implementation is used if HMM estimation becomes unstable.
    """
    features = _regime_features(spy_df)
    if len(features) < 120:
        return {
            "available": False,
            "model": "heuristic",
            "current_regime": "Unknown",
            "confidence": 0,
            "transition_probabilities": {},
            "message": "Need at least 120 observations for statistical regime inference.",
        }
    try:
        model = _gaussian_hmm(features, n_states=min(n_states, max(2, len(features) // 60)))
        model["model"] = "Gaussian HMM"
    except Exception as exc:
        model = _kmeans_regime_fallback(features, n_states=min(n_states, max(2, len(features) // 60)))
        model["fallback_reason"] = str(exc)

    model["strategy_recommendation"] = strategy_for_regime(model["current_regime"])
    model["research_mindset"] = research_mindset.research_envelope(
        "market_regime",
        "Market behavior is better modeled as a changing latent state than as one permanent distribution.",
        [
            f"Model: {model.get('model')}",
            f"Current state confidence: {model.get('confidence', 0):.1f}%",
            f"Observations: {len(features)}",
        ],
        [
            "Historical return, volatility, trend, and drawdown contain information about the latent state.",
            "State behavior is persistent enough for transition probabilities to be useful.",
        ],
        [
            "Regime labels are assigned after estimation and may be economically ambiguous.",
            "Transition probabilities can change after structural breaks.",
            "The current state may be revised as new observations arrive.",
        ],
        confidence=model.get("confidence", 0),
        regime_weaknesses=["Structural break", "Policy shock", "Sparse crisis history"],
    )
    return model


def _regime_features(spy_df: pd.DataFrame) -> pd.DataFrame:
    if spy_df is None or spy_df.empty:
        return pd.DataFrame()
    close = quant_analytics._as_price_series(spy_df).dropna()
    returns = close.pct_change()
    rolling_high = close.rolling(252, min_periods=60).max()
    return pd.DataFrame(
        {
            "return_21d": close.pct_change(21),
            "volatility_21d": returns.rolling(21).std() * np.sqrt(252),
            "trend_50d": close / close.rolling(50).mean() - 1,
            "drawdown": close / rolling_high - 1,
        }
    ).replace([np.inf, -np.inf], np.nan).dropna()


def _kmeans(data: np.ndarray, k: int, iterations: int = 50, seed: int = 42):
    rng = np.random.default_rng(seed)
    centers = data[rng.choice(len(data), size=k, replace=False)].copy()
    labels = np.zeros(len(data), dtype=int)
    for _ in range(iterations):
        distances = ((data[:, None, :] - centers[None, :, :]) ** 2).sum(axis=2)
        new_labels = distances.argmin(axis=1)
        if np.array_equal(new_labels, labels):
            break
        labels = new_labels
        for idx in range(k):
            members = data[labels == idx]
            if len(members):
                centers[idx] = members.mean(axis=0)
    return labels, centers


def _gaussian_hmm(features: pd.DataFrame, n_states: int, iterations: int = 30) -> Dict:
    raw = features.values.astype(float)
    mean = raw.mean(axis=0)
    std = raw.std(axis=0)
    std[std == 0] = 1
    x = (raw - mean) / std
    labels, means = _kmeans(x, n_states)
    variances = np.vstack([
        x[labels == state].var(axis=0) + 0.10 if np.any(labels == state) else np.ones(x.shape[1])
        for state in range(n_states)
    ])
    transition = np.full((n_states, n_states), 0.15 / max(n_states - 1, 1))
    np.fill_diagonal(transition, 0.85)
    initial = np.full(n_states, 1 / n_states)

    for _ in range(iterations):
        emission = _emission_probabilities(x, means, variances)
        alpha, scales = _forward(emission, initial, transition)
        beta = _backward(emission, transition, scales)
        gamma = alpha * beta
        gamma /= gamma.sum(axis=1, keepdims=True)

        xi_sum = np.zeros_like(transition)
        for t in range(len(x) - 1):
            xi = alpha[t, :, None] * transition * emission[t + 1, None, :] * beta[t + 1, None, :]
            xi_sum += xi / max(xi.sum(), 1e-12)

        initial = gamma[0]
        transition = xi_sum / np.maximum(xi_sum.sum(axis=1, keepdims=True), 1e-12)
        weights = np.maximum(gamma.sum(axis=0), 1e-12)
        means = (gamma.T @ x) / weights[:, None]
        for state in range(n_states):
            residual = x - means[state]
            variances[state] = (gamma[:, state, None] * residual**2).sum(axis=0) / weights[state]
        variances = np.maximum(variances, 0.03)

    raw_centroids = means * std + mean
    state_names = _name_states(raw_centroids)
    latest_probabilities = gamma[-1]
    current_state = int(np.argmax(latest_probabilities))
    transition_by_name = _aggregate_transitions(transition, state_names, current_state)
    return {
        "available": True,
        "current_regime": state_names[current_state],
        "confidence": round(float(latest_probabilities[current_state] * 100), 1),
        "state_probabilities": {
            state_names[idx]: round(float(probability * 100), 2)
            for idx, probability in enumerate(latest_probabilities)
        },
        "transition_probabilities": transition_by_name,
        "state_centroids": {
            state_names[idx]: {
                column: round(float(raw_centroids[idx, col]), 5)
                for col, column in enumerate(features.columns)
            }
            for idx in range(n_states)
        },
    }


def _emission_probabilities(x, means, variances):
    diff = x[:, None, :] - means[None, :, :]
    log_prob = -0.5 * (
        np.log(2 * np.pi * variances)[None, :, :]
        + (diff**2) / variances[None, :, :]
    ).sum(axis=2)
    log_prob -= log_prob.max(axis=1, keepdims=True)
    return np.maximum(np.exp(log_prob), 1e-300)


def _forward(emission, initial, transition):
    alpha = np.zeros_like(emission)
    scales = np.ones(len(emission))
    alpha[0] = initial * emission[0]
    scales[0] = max(alpha[0].sum(), 1e-12)
    alpha[0] /= scales[0]
    for t in range(1, len(emission)):
        alpha[t] = (alpha[t - 1] @ transition) * emission[t]
        scales[t] = max(alpha[t].sum(), 1e-12)
        alpha[t] /= scales[t]
    return alpha, scales


def _backward(emission, transition, scales):
    beta = np.ones_like(emission)
    for t in range(len(emission) - 2, -1, -1):
        beta[t] = transition @ (emission[t + 1] * beta[t + 1])
        beta[t] /= max(scales[t + 1], 1e-12)
    return beta


def _kmeans_regime_fallback(features: pd.DataFrame, n_states: int) -> Dict:
    raw = features.values.astype(float)
    mean = raw.mean(axis=0)
    std = raw.std(axis=0)
    std[std == 0] = 1
    labels, centers = _kmeans((raw - mean) / std, n_states)
    raw_centers = centers * std + mean
    state_names = _name_states(raw_centers)
    latest_state = int(labels[-1])
    distances = np.sqrt((((raw[-1] - raw_centers) / std) ** 2).sum(axis=1))
    confidence = 100 / (1 + distances[latest_state])
    counts = np.ones((n_states, n_states)) * 0.5
    for left, right in zip(labels[:-1], labels[1:]):
        counts[left, right] += 1
    transition = counts / counts.sum(axis=1, keepdims=True)
    return {
        "available": True,
        "model": "K-Means fallback",
        "current_regime": state_names[latest_state],
        "confidence": round(float(confidence), 1),
        "transition_probabilities": _aggregate_transitions(transition, state_names, latest_state),
    }


def _name_states(centroids: np.ndarray) -> list:
    volatility_median = float(np.median(centroids[:, 1]))
    names = []
    for return_21d, volatility, trend, drawdown in centroids:
        if drawdown <= -0.20 and return_21d <= -0.08:
            name = "Crash"
        elif volatility >= volatility_median * 1.35:
            name = "High Volatility"
        elif trend < -0.03 and return_21d < 0:
            name = "Bear Trend"
        elif drawdown < -0.05 and return_21d > 0.03:
            name = "Recovery"
        elif trend > 0.02 and return_21d > 0:
            name = "Bull Trend"
        else:
            name = "Sideways"
        names.append(name)
    return names


def _aggregate_transitions(transition, state_names, current_state):
    output = {}
    for target_state, probability in enumerate(transition[current_state]):
        name = state_names[target_state]
        output[name] = output.get(name, 0) + float(probability)
    return {name: round(value * 100, 2) for name, value in sorted(output.items(), key=lambda item: item[1], reverse=True)}


def strategy_for_regime(regime: str) -> Dict:
    recommendations = {
        "Bull Trend": {"risk_posture": "Risk-on", "preferred": ["momentum", "growth", "breakouts"], "avoid": ["premature mean reversion"]},
        "Bear Trend": {"risk_posture": "Defensive", "preferred": ["quality", "low volatility", "cash"], "avoid": ["leveraged dip buying"]},
        "Sideways": {"risk_posture": "Selective", "preferred": ["pairs trading", "mean reversion", "income"], "avoid": ["late breakouts"]},
        "High Volatility": {"risk_posture": "Reduced size", "preferred": ["quality", "volatility targeting"], "avoid": ["tight stops", "high leverage"]},
        "Crash": {"risk_posture": "Capital preservation", "preferred": ["liquidity", "hedges"], "avoid": ["uncapped leverage", "illiquid trades"]},
        "Recovery": {"risk_posture": "Gradually add risk", "preferred": ["cyclicals", "small caps", "improving momentum"], "avoid": ["chasing the first rebound"]},
    }
    return recommendations.get(regime, {"risk_posture": "Neutral", "preferred": ["diversification"], "avoid": ["concentrated bets"]})


def _index_trend(df: pd.DataFrame) -> Dict:
    close = quant_analytics._as_price_series(df)
    latest = close.iloc[-1]
    sma20 = close.rolling(20).mean().iloc[-1]
    sma50 = close.rolling(50).mean().iloc[-1]
    sma200 = close.rolling(200).mean().iloc[-1]
    returns = close.pct_change().dropna()
    above_count = sum(latest > x for x in [sma20, sma50, sma200] if pd.notna(x))
    return {
        "latest": quant_analytics.safe_float(latest),
        "above_sma20": bool(pd.notna(sma20) and latest > sma20),
        "above_sma50": bool(pd.notna(sma50) and latest > sma50),
        "above_sma200": bool(pd.notna(sma200) and latest > sma200),
        "trend_score": round(above_count / 3 * 100, 2),
        "one_month_return": quant_analytics.safe_float(close.pct_change(21).iloc[-1], 0.0),
        "three_month_return": quant_analytics.safe_float(close.pct_change(63).iloc[-1], 0.0),
        "volatility": quant_analytics.safe_float(returns.tail(21).std() * np.sqrt(252), 0.0),
    }


def _sector_trend(df: pd.DataFrame) -> Dict:
    close = quant_analytics._as_price_series(df)
    latest = close.iloc[-1]
    sma50 = close.rolling(50).mean().iloc[-1]
    one_month = close.pct_change(21).iloc[-1] if len(close) > 21 else 0
    three_month = close.pct_change(63).iloc[-1] if len(close) > 63 else 0
    momentum = (max(one_month, -0.25) + max(three_month, -0.35)) * 100 + (25 if latest > sma50 else 0)
    return {
        "name": getattr(df, "name", "Sector"),
        "latest": quant_analytics.safe_float(latest),
        "above_sma50": bool(pd.notna(sma50) and latest > sma50),
        "one_month_return": quant_analytics.safe_float(one_month, 0.0),
        "three_month_return": quant_analytics.safe_float(three_month, 0.0),
        "momentum_score": round(float(max(0, min(100, momentum + 50))), 2),
    }


def _health_score(index_metrics: Dict, sector_metrics: Dict) -> float:
    if not index_metrics:
        return 0.0
    trend = np.mean([payload["trend_score"] for payload in index_metrics.values() if "trend_score" in payload])
    breadth = _breadth_proxy(index_metrics)
    sector_participation = _sector_participation(sector_metrics)
    return round(float(trend * 0.5 + breadth * 0.25 + sector_participation * 0.25), 2)


def _breadth_proxy(index_metrics: Dict) -> float:
    if not index_metrics:
        return 0.0
    positive = sum(1 for payload in index_metrics.values() if payload.get("one_month_return", 0) > 0)
    return round(positive / len(index_metrics) * 100, 2)


def _sector_participation(sector_metrics: Dict) -> float:
    if not sector_metrics:
        return 0.0
    positive = sum(1 for payload in sector_metrics.values() if payload.get("above_sma50"))
    return round(positive / len(sector_metrics) * 100, 2)


def _risk_on_score(sector_metrics: Dict) -> Dict:
    risk_on = ["Technology", "Consumer Discretionary", "Financials", "Industrials"]
    defensive = ["Utilities", "Consumer Staples", "Healthcare"]
    risk_on_avg = np.mean([sector_metrics[name]["momentum_score"] for name in risk_on if name in sector_metrics]) if sector_metrics else 0
    defensive_avg = np.mean([sector_metrics[name]["momentum_score"] for name in defensive if name in sector_metrics]) if sector_metrics else 0
    spread = float(risk_on_avg - defensive_avg)
    return {
        "score": round(spread, 2),
        "label": "Risk-on" if spread > 5 else "Risk-off" if spread < -5 else "Neutral",
    }


def _vix_status(vix_df) -> Dict:
    if vix_df is None or vix_df.empty:
        return {"level": None, "regime": "Unknown"}
    close = quant_analytics._as_price_series(vix_df)
    level = close.iloc[-1]
    if level >= 30:
        regime = "Stress"
    elif level >= 22:
        regime = "Elevated"
    elif level <= 15:
        regime = "Calm"
    else:
        regime = "Normal"
    return {"level": round(float(level), 2), "regime": regime}


def _risk_environment(index_metrics: Dict, sector_metrics: Dict, vix_status: Dict) -> str:
    risk_on = _risk_on_score(sector_metrics)
    sp500 = index_metrics.get("sp500", {})
    if vix_status.get("regime") in ("Stress", "Elevated") and not sp500.get("above_sma50"):
        return "Risk-off"
    if risk_on["label"] == "Risk-on" and sp500.get("above_sma50") and sp500.get("above_sma200"):
        return "Risk-on"
    if not sp500.get("above_sma200"):
        return "High-risk"
    return "Neutral"


def _regime_label(index_metrics: Dict, health_score: float, risk_environment: str) -> str:
    sp500 = index_metrics.get("sp500", {})
    if risk_environment == "Risk-off":
        return "Risk-off"
    if health_score >= 75 and sp500.get("above_sma50") and sp500.get("above_sma200"):
        return "Bull market"
    if health_score <= 30 and not sp500.get("above_sma200"):
        return "Bear market"
    if sp500.get("above_sma200") and not sp500.get("above_sma50"):
        return "Correction"
    if not sp500.get("above_sma200") and sp500.get("one_month_return", 0) > 0:
        return "Recovery"
    if 40 <= health_score <= 60:
        return "Sideways"
    if risk_environment == "High-risk":
        return "High-risk"
    return "Neutral"
