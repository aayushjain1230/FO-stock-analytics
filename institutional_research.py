"""Institutional research payload builder for the static quant dashboard.

This module does not fetch data. It converts the saved quant, portfolio, and
research state files into an explainable platform payload that the dashboard can
render as institutional-style pages: factor exposure, attribution, risk,
correlation networks, uncertainty, scenarios, optimization, notebook entries,
and alert discipline.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, Iterable, List, Tuple


ASSUMPTIONS = [
    "Historical relationships remain directionally informative.",
    "Volatility does not regime-shift sharply before the next review.",
    "Liquidity remains sufficient for position sizing and exits.",
    "Earnings expectations do not change suddenly.",
    "Correlations remain within their recent historical range.",
    "No major macro, policy, credit, or geopolitical shock occurs.",
]


def _num(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except Exception:
        return default


def _pct(value: Any, default: float = 0.0) -> float:
    """Return a percentage number from either decimal or percentage inputs."""
    value = _num(value, default)
    return value * 100 if abs(value) <= 1 else value


def _weight_map(portfolio: Dict[str, Any]) -> Dict[str, float]:
    weights: Dict[str, float] = {}
    for item in portfolio.get("positions", []):
        ticker = str(item.get("ticker", "")).upper()
        if ticker:
            weights[ticker] = _pct(item.get("weight"))
    total = sum(weights.values()) or 1.0
    return {ticker: value / total for ticker, value in weights.items()}


def _risk_label(value: float, high: float, medium: float) -> str:
    if value >= high:
        return "High"
    if value >= medium:
        return "Medium"
    return "Low"


def _drivers_for_stock(row: Dict[str, Any]) -> Tuple[List[str], List[str]]:
    positives: List[str] = []
    negatives: List[str] = []
    if row.get("above_sma200"):
        positives.append("Price is above the 200-day trend line.")
    else:
        negatives.append("Price is not above the 200-day trend line.")
    if str(row.get("rs_status", "")).lower() == "leading":
        positives.append("Relative strength is leading the benchmark/watchlist.")
    elif row.get("rs_status"):
        negatives.append("Relative strength is lagging.")
    if _num(row.get("sharpe_ratio")) >= 1:
        positives.append("Risk-adjusted return is constructive.")
    else:
        negatives.append("Sharpe ratio is below institutional quality.")
    if abs(_num(row.get("max_drawdown"))) >= 0.35:
        negatives.append("Historical drawdown has been severe.")
    if _num(row.get("annualized_volatility")) >= 0.45:
        negatives.append("Realized volatility is elevated.")
    if row.get("why_now") and row.get("why_now") != "No clear Why Now trigger":
        positives.append(f"Why-now trigger: {row.get('why_now')}.")
    return positives[:4], negatives[:4]


def factor_exposure_engine(quant: Dict[str, Any], portfolio: Dict[str, Any]) -> Dict[str, Any]:
    """Build stock and portfolio factor exposure views from saved factor scores."""
    stocks = quant.get("factor_model", {}).get("stocks", {})
    leaderboard = quant.get("factor_model", {}).get("leaderboard", [])
    weights = _weight_map(portfolio)
    portfolio_factors = quant.get("portfolio_factor_exposure", {})
    rows = []
    for item in leaderboard:
        ticker = str(item.get("ticker", "")).upper()
        scores = item.get("scores", {})
        rows.append(
            {
                "ticker": ticker,
                "weight_pct": round(weights.get(ticker, 0) * 100, 2),
                "momentum": _num(scores.get("momentum"), 50),
                "value": _num(scores.get("value"), 50),
                "quality": _num(scores.get("quality"), 50),
                "growth": _num(scores.get("growth"), 50),
                "volatility": _num(scores.get("low_volatility"), 50),
                "market_beta": _num(stocks.get(ticker, {}).get("scores", {}).get("market_beta"), 50),
                "style": _style_from_scores(scores),
                "composite": _num(item.get("composite_score"), 50),
                "coverage_pct": _num(item.get("data_coverage_pct"), 0),
            }
        )
    exposures = portfolio_factors.get("exposures", {})
    warnings = list(portfolio_factors.get("warnings", []))
    for name, score in exposures.items():
        if _num(score) >= 75 or _num(score) <= 25:
            warnings.append(f"Portfolio has an extreme {name.replace('_', ' ')} factor tilt ({_num(score):.1f}/100).")
    return {
        "stock_rows": rows,
        "portfolio_breakdown": exposures,
        "concentration_warnings": warnings,
        "factor_return_contribution": _factor_contribution_placeholder(exposures, "return"),
        "factor_risk_contribution": _factor_contribution_placeholder(exposures, "risk"),
    }


def _style_from_scores(scores: Dict[str, Any]) -> str:
    growth = _num(scores.get("growth"), 50)
    value = _num(scores.get("value"), 50)
    quality = _num(scores.get("quality"), 50)
    momentum = _num(scores.get("momentum"), 50)
    if growth >= value + 10:
        return "Growth"
    if value >= growth + 10:
        return "Value"
    if quality >= 70:
        return "Quality"
    if momentum >= 70:
        return "Momentum"
    return "Blend"


def _factor_contribution_placeholder(exposures: Dict[str, Any], kind: str) -> Dict[str, Any]:
    if not exposures:
        return {"available": False, "message": "Run the quant report to estimate portfolio factor scores."}
    total = sum(abs(_num(v) - 50) for v in exposures.values()) or 1
    return {
        "available": True,
        "method": f"Relative {kind} contribution proxy from factor tilt distance from neutral.",
        "items": {
            name: round(abs(_num(value) - 50) / total * 100, 2)
            for name, value in exposures.items()
        },
    }


def attribution_engine(portfolio: Dict[str, Any], quant: Dict[str, Any]) -> Dict[str, Any]:
    """Explain portfolio return sources with available state and mark gaps."""
    portfolio_return = _pct(portfolio.get("portfolio_return"))
    bench_periods = portfolio.get("benchmark_comparison", {}).get("periods", {})
    latest_benchmark = next(iter(bench_periods.values()), {})
    benchmark_return = _pct(latest_benchmark.get("benchmark_return"))
    relative = portfolio_return - benchmark_return if bench_periods else None
    factor_effect = _num(quant.get("portfolio_factor_exposure", {}).get("exposures", {}).get("momentum"), 50) - 50
    sector_effect = _sector_concentration(portfolio) - 25
    alpha = relative if relative is not None else None
    if alpha is not None:
        alpha -= factor_effect * 0.05
        alpha -= max(sector_effect, 0) * 0.03
    return {
        "market_return_contribution": benchmark_return if bench_periods else None,
        "sector_allocation_effect": round(sector_effect * 0.03, 2) if sector_effect > 0 else 0.0,
        "stock_selection_effect": relative,
        "factor_exposure_effect": round(factor_effect * 0.05, 2),
        "alpha_unexplained_return": round(alpha, 2) if alpha is not None else None,
        "cash_drag": 0.0,
        "explanation": "Attribution uses saved benchmark/factor data when present. Missing benchmark history is shown as unavailable rather than guessed.",
    }


def _sector_concentration(portfolio: Dict[str, Any]) -> float:
    exposure = portfolio.get("sector_exposure", {})
    return max((_num(v) for v in exposure.values()), default=0.0)


def risk_contribution_engine(portfolio: Dict[str, Any]) -> Dict[str, Any]:
    risk = portfolio.get("risk_contributions", {})
    positions = {item.get("ticker"): item for item in portfolio.get("positions", [])}
    rows = []
    for ticker, contribution in sorted(risk.items(), key=lambda item: _num(item[1]), reverse=True):
        rows.append(
            {
                "ticker": ticker,
                "position_weight_pct": _num(positions.get(ticker, {}).get("weight")),
                "position_volatility": None,
                "marginal_risk_contribution": _num(contribution),
                "percentage_risk_contribution": _num(contribution),
                "concentration_risk": _risk_label(_num(contribution), 35, 20),
            }
        )
    warnings = []
    if rows and rows[0]["percentage_risk_contribution"] >= 35:
        warnings.append(f"{rows[0]['ticker']} dominates portfolio risk at {rows[0]['percentage_risk_contribution']:.1f}%.")
    return {"rows": rows, "top_risk_contributors": rows[:5], "warnings": warnings}


def correlation_network_engine(portfolio: Dict[str, Any]) -> Dict[str, Any]:
    corr = portfolio.get("correlation", {})
    high_pairs = corr.get("highest_correlated_pairs", [])
    low_pairs = corr.get("lowest_correlated_pairs", [])
    clusters: List[Dict[str, Any]] = []
    used = set()
    for pair in high_pairs:
        tickers = pair.get("pair", [])
        if len(tickers) == 2 and _num(pair.get("correlation")) >= 0.70:
            cluster = sorted(set(tickers))
            used.update(cluster)
            clusters.append({"members": cluster, "reason": f"Correlation {pair.get('correlation')}"})
    warnings = list(corr.get("redundant_holdings", []))
    if _num(corr.get("average_correlation")) >= 0.65:
        warnings.append({"message": "Average portfolio correlation is elevated; diversification may be weaker than holdings count suggests."})
    return {
        "nodes": sorted(used),
        "edges": [item for item in high_pairs if _num(item.get("correlation")) >= 0.70],
        "clusters": clusters,
        "average_portfolio_correlation": corr.get("average_correlation"),
        "highest_correlated_pairs": high_pairs,
        "lowest_correlated_pairs": low_pairs,
        "diversification_warnings": warnings,
    }


def stock_research_score_engine(quant: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = []
    factor_stocks = quant.get("factor_model", {}).get("stocks", {})
    for row in quant.get("tickers", []):
        ticker = str(row.get("ticker", "")).upper()
        scores = factor_stocks.get(ticker, {}).get("scores", {})
        positives, negatives = _drivers_for_stock(row)
        missing = []
        for name in ["quant_score", "confidence", "annualized_volatility", "sharpe_ratio"]:
            if row.get(name) is None:
                missing.append(name)
        rows.append(
            {
                "ticker": ticker,
                "overall_score": _num(row.get("quant_score") or row.get("final_score") or row.get("score"), 50),
                "confidence_score": _num(row.get("confidence"), 0),
                "momentum_score": _num(scores.get("momentum"), _num(row.get("score"), 50)),
                "fundamental_score": _num(scores.get("value"), 50),
                "quality_score": _num(scores.get("quality"), 50),
                "valuation_score": _num(scores.get("value"), 50),
                "risk_score": max(0, 100 - _pct(row.get("annualized_volatility"))),
                "liquidity_score": _liquidity_score(row),
                "regime_compatibility_score": 75 if row.get("above_sma200") else 40,
                "top_positive_drivers": positives,
                "top_negative_drivers": negatives,
                "uncertainty_level": _risk_label(100 - _num(row.get("confidence"), 0), 75, 50),
                "missing_data_warnings": missing,
            }
        )
    return rows


def _liquidity_score(row: Dict[str, Any]) -> float:
    rv = str(row.get("relative_volume", "1")).replace("x", "")
    score = 55 + min(max((_num(rv, 1) - 1) * 20, -20), 25)
    return round(max(0, min(100, score)), 2)


def confidence_engine(quant: Dict[str, Any], signal_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    historical = signal_payload.get("signal_types", {}) if signal_payload else {}
    rows = []
    for row in quant.get("tickers", []):
        score = _num(row.get("quant_score") or row.get("score"), 50)
        confidence = _num(row.get("confidence"), 0)
        direction = "Constructive" if score >= 65 else "Defensive" if score <= 40 else "Neutral"
        rows.append(
            {
                "ticker": row.get("ticker"),
                "signal_direction": direction,
                "confidence_pct": confidence,
                "reason_for_confidence": row.get("research_note", "Multiple research inputs are aligned."),
                "reason_for_uncertainty": "Confidence is penalized when data coverage, signal validation, or risk-adjusted return is weak.",
                "historical_accuracy": historical.get("quant_score", {}).get("win_rate"),
                "data_quality_score": min(100, confidence + 35),
                "assumptions": ASSUMPTIONS,
                "risks": [row.get("what_invalidates") or "Signal can fail if trend, liquidity, or regime changes."],
            }
        )
    return rows


def assumption_checker(portfolio: Dict[str, Any], quant: Dict[str, Any]) -> List[Dict[str, Any]]:
    checks = []
    avg_corr = _num(portfolio.get("correlation", {}).get("average_correlation"))
    vol = _num(portfolio.get("variance", {}).get("annual_volatility"))
    checks.append({"assumption": ASSUMPTIONS[1], "status": "Breaking" if vol >= 0.35 else "Watch" if vol >= 0.25 else "OK", "evidence": f"Portfolio annual volatility is {vol*100:.1f}%."})
    checks.append({"assumption": ASSUMPTIONS[4], "status": "Breaking" if avg_corr >= 0.75 else "Watch" if avg_corr >= 0.60 else "OK", "evidence": f"Average correlation is {avg_corr:.2f}."})
    regime = quant.get("market_regime", {}).get("regime", "Unknown")
    checks.append({"assumption": ASSUMPTIONS[5], "status": "Watch" if regime in {"Crash", "High Volatility", "Bear Trend"} else "OK", "evidence": f"Current regime is {regime}."})
    for item in ASSUMPTIONS:
        if not any(c["assumption"] == item for c in checks):
            checks.append({"assumption": item, "status": "Model Assumption", "evidence": "Requires ongoing monitoring."})
    return checks


def market_breadth_dashboard(quant: Dict[str, Any]) -> Dict[str, Any]:
    rows = quant.get("tickers", [])
    if not rows:
        return {"available": False, "message": "Run quant report to compute watchlist breadth."}
    above50 = sum(1 for r in rows if r.get("above_sma50") or r.get("above_sma200")) / len(rows) * 100
    above200 = sum(1 for r in rows if r.get("above_sma200")) / len(rows) * 100
    advancing = sum(1 for r in rows if _num(r.get("quant_score")) >= 50)
    declining = max(1, len(rows) - advancing)
    score = round((above50 + above200 + min(advancing / declining, 2) / 2 * 100) / 3, 2)
    return {
        "available": True,
        "advance_decline_ratio": round(advancing / declining, 2),
        "new_highs_vs_new_lows": "Requires 52-week high/low state in report.",
        "percent_above_sma50": round(above50, 2),
        "percent_above_sma200": round(above200, 2),
        "market_breadth_score": score,
        "breadth_trend": "Healthy" if score >= 65 else "Fragile" if score >= 45 else "Weak",
        "breadth_divergence_warning": score < 45,
    }


def sector_rotation_engine(portfolio: Dict[str, Any], discovery: Dict[str, Any]) -> Dict[str, Any]:
    rankings = discovery.get("sector_rankings", []) if discovery else []
    exposure = portfolio.get("sector_exposure", {})
    rows = []
    for item in rankings:
        sector = item.get("sector")
        score = _num(item.get("average_score"))
        rows.append(
            {
                "sector": sector,
                "sector_momentum": score,
                "relative_strength_vs_sp500": score - 50,
                "sector_volatility": None,
                "leadership_rank": len(rows) + 1,
                "portfolio_weight_pct": exposure.get(sector),
                "rotation_signal": "Leading" if score >= 65 else "Deteriorating" if score <= 40 else "Neutral",
            }
        )
    return {
        "leading_sectors": [r for r in rows if r["rotation_signal"] == "Leading"][:5],
        "weak_sectors": [r for r in rows if r["sector_momentum"] <= 40][:5],
        "improving_sectors": [r for r in rows if r["relative_strength_vs_sp500"] > 5][:5],
        "deteriorating_sectors": [r for r in rows if r["relative_strength_vs_sp500"] < -10][:5],
        "rows": rows,
    }


def liquidity_dashboard(quant: Dict[str, Any]) -> List[Dict[str, Any]]:
    rows = []
    for row in quant.get("tickers", []):
        rv = _num(str(row.get("relative_volume", "1")).replace("x", ""), 1)
        liquidity_score = _liquidity_score(row)
        rows.append(
            {
                "ticker": row.get("ticker"),
                "volume_spike": rv >= 1.5,
                "relative_volume": rv,
                "average_dollar_volume": None,
                "liquidity_score": liquidity_score,
                "spread_estimate": "Unavailable without bid/ask feed",
                "slippage_risk_estimate": "High" if liquidity_score < 45 else "Medium" if liquidity_score < 65 else "Low",
            }
        )
    return rows


def probability_forecasting_lab(quant: Dict[str, Any]) -> List[Dict[str, Any]]:
    horizons = {"1 week": 5, "1 month": 21, "3 months": 63}
    rows = []
    for row in quant.get("tickers", []):
        vol = _num(row.get("annualized_volatility"), 0.25)
        score_edge = (_num(row.get("quant_score"), 50) - 50) / 100
        for horizon, days in horizons.items():
            horizon_vol = vol * (days / 252) ** 0.5
            expected = score_edge * days / 252
            rows.append(
                {
                    "ticker": row.get("ticker"),
                    "horizon": horizon,
                    "probability_plus_5": _normalish_probability(expected, horizon_vol, 0.05),
                    "probability_plus_10": _normalish_probability(expected, horizon_vol, 0.10),
                    "probability_minus_5": _normalish_probability(-expected, horizon_vol, 0.05),
                    "probability_minus_10": _normalish_probability(-expected, horizon_vol, 0.10),
                    "expected_return": round(expected * 100, 2),
                    "downside_probability": _normalish_probability(-expected, horizon_vol, 0.05),
                    "upside_probability": _normalish_probability(expected, horizon_vol, 0.05),
                    "confidence_level": row.get("confidence"),
                }
            )
    return rows


def _normalish_probability(edge: float, vol: float, threshold: float) -> float:
    if vol <= 0:
        return 0.0
    # Lightweight monotonic approximation; avoids scipy dependency.
    z = (edge - threshold) / vol
    probability = 1 / (1 + pow(2.71828, -1.7 * z))
    return round(max(0, min(100, probability * 100)), 2)


def scenario_engine(portfolio: Dict[str, Any]) -> List[Dict[str, Any]]:
    beta = 1.0
    vol = _num(portfolio.get("variance", {}).get("annual_volatility"), 0.2)
    top_risk = sorted(portfolio.get("risk_contributions", {}).items(), key=lambda i: _num(i[1]), reverse=True)[:3]
    scenarios = [
        ("Market falls 10%", -0.10, "Market beta and high-correlation holdings drive downside."),
        ("Market rises 10%", 0.10, "Beta-sensitive holdings benefit first."),
        ("Volatility doubles", -vol * 0.35, "Higher volatility compresses risk appetite and widens expected drawdown."),
        ("Interest rates rise", -0.04, "Long-duration growth and high-multiple stocks are most exposed."),
        ("Interest rates fall", 0.035, "Growth and duration-sensitive assets may get valuation support."),
        ("Oil rises 20%", -0.015, "Input-cost pressure and inflation expectations can hurt broad equity risk."),
        ("Tech sector sells off", -0.08, "Technology concentration creates sector-specific drawdown risk."),
        ("Recession scenario", -0.18, "Earnings revisions, spread widening, and risk-off positioning dominate."),
        ("2008-style crisis", -0.35, "Correlation approaches one and liquidity assumptions weaken."),
        ("COVID-style crash", -0.28, "Gap risk and volatility shock dominate near-term portfolio behavior."),
        ("2022 inflation shock", -0.20, "Rates and valuation multiple compression pressure long-duration assets."),
    ]
    return [
        {
            "scenario": name,
            "estimated_portfolio_return": round(shock * beta * 100, 2),
            "estimated_drawdown": round(min(shock * beta * 1.25, shock) * 100, 2),
            "estimated_volatility_change": round(abs(shock) * 100 + vol * 25, 2),
            "most_exposed_holdings": [ticker for ticker, _ in top_risk],
            "explanation": explanation,
        }
        for name, shock, explanation in scenarios
    ]


def optimizer_lab(portfolio: Dict[str, Any]) -> Dict[str, Any]:
    opt = portfolio.get("optimization", {})
    weights = opt.get("optimized_weights", {})
    return {
        "modes": [
            "Max Sharpe portfolio",
            "Minimum variance portfolio",
            "Risk parity portfolio",
            "Equal risk contribution portfolio",
            "Maximum diversification portfolio",
            "User-constrained portfolio",
        ],
        "constraints": ["Max/min position size", "Sector limits", "Long-only mode", "Cash allocation option"],
        "current_vs_optimized": opt,
        "suggested_weights": weights,
        "turnover_required": _turnover_required(portfolio, weights),
    }


def _turnover_required(portfolio: Dict[str, Any], optimized_weights: Dict[str, Any]) -> float | None:
    if not optimized_weights:
        return None
    current = {item.get("ticker"): _num(item.get("weight")) for item in portfolio.get("positions", [])}
    return round(sum(abs(_num(optimized_weights.get(t, 0)) - current.get(t, 0)) for t in set(current) | set(optimized_weights)) / 2, 2)


def efficient_frontier(portfolio: Dict[str, Any]) -> Dict[str, Any]:
    opt = portfolio.get("optimization", {})
    return {
        "available": bool(opt),
        "current_portfolio": {"return": _pct(portfolio.get("portfolio_return")), "volatility": _pct(portfolio.get("variance", {}).get("annual_volatility"))},
        "minimum_variance_portfolio": {"volatility": _pct(opt.get("optimized_volatility"))},
        "max_sharpe_portfolio": {"sharpe": opt.get("optimized_sharpe"), "weights": opt.get("optimized_weights", {})},
        "equal_weight_portfolio": "Use current universe with equal weights as benchmark.",
        "risk_return_curve": "Requires Monte Carlo/frontier sampling run; dashboard is wired for display once generated.",
    }


def historical_stress_testing(portfolio: Dict[str, Any]) -> List[Dict[str, Any]]:
    base = scenario_engine(portfolio)
    mapping = {
        "2008 financial crisis": "2008-style crisis",
        "COVID crash": "COVID-style crash",
        "Dot-com crash": "Tech sector sells off",
        "2022 inflation bear market": "2022 inflation shock",
        "Black Monday-style shock": "Market falls 10%",
    }
    rows = []
    for label, source in mapping.items():
        scenario = next((item for item in base if item["scenario"] == source), {})
        rows.append(
            {
                "event": label,
                "estimated_loss": scenario.get("estimated_portfolio_return"),
                "recovery_time": "Unknown without event-window backtest",
                "worst_drawdown": scenario.get("estimated_drawdown"),
                "holdings_most_responsible": scenario.get("most_exposed_holdings", []),
                "risk_warning": scenario.get("explanation"),
            }
        )
    return rows


def research_notebook(quant: Dict[str, Any]) -> List[Dict[str, Any]]:
    follow_up = (datetime.now() + timedelta(days=30)).date().isoformat()
    entries = []
    for row in quant.get("tickers", []):
        positives, negatives = _drivers_for_stock(row)
        entries.append(
            {
                "ticker": row.get("ticker"),
                "hypothesis": row.get("research_note") or "Research thesis requires a refreshed quant report.",
                "evidence": positives,
                "counterevidence": negatives,
                "assumptions": ASSUMPTIONS,
                "historical_tests": "Review walk-forward signal validation and factor stability.",
                "failure_modes": [row.get("what_invalidates") or "Trend failure, factor reversal, liquidity shock, or regime change."],
                "confidence_score": row.get("confidence"),
                "data_sources_used": ["price history", "technical indicators", "factor model", "portfolio state"],
                "what_would_invalidate": row.get("what_invalidates") or "Break of thesis drivers or material data-quality failure.",
                "final_decision": row.get("quant_label") or row.get("rating") or "Watch",
                "follow_up_date": follow_up,
            }
        )
    return entries


def intelligent_alerts(portfolio: Dict[str, Any], quant: Dict[str, Any]) -> List[Dict[str, Any]]:
    alerts = []
    for warning in portfolio.get("risk_warnings", []):
        alerts.append({"trigger": "Risk threshold exceeded", "what_changed": warning, "why_it_matters": "Portfolio loss may be dominated by one exposure.", "portfolio_impact": "Higher drawdown sensitivity.", "confidence_level": 75, "what_to_watch_next": "Risk contribution, correlation, and drawdown."})
    regime = quant.get("market_regime", {})
    if regime.get("regime") in {"Crash", "Bear Trend", "High Volatility"}:
        alerts.append({"trigger": "Regime change", "what_changed": f"Regime is {regime.get('regime')}.", "why_it_matters": "Strategy edge changes by regime.", "portfolio_impact": "Reduce reliance on momentum-only signals.", "confidence_level": regime.get("regime_confidence"), "what_to_watch_next": "Transition probabilities and volatility."})
    for pair in quant.get("pairs_trading", {}).get("candidates", [])[:3]:
        signal = pair.get("signal", {}).get("action")
        if signal and signal != "watch":
            alerts.append({"trigger": "Pair trading opportunity", "what_changed": str(pair.get("pair")), "why_it_matters": "Spread is statistically stretched.", "portfolio_impact": "Potential market-neutral research idea.", "confidence_level": pair.get("score", 60), "what_to_watch_next": "Z-score reversion and failure conditions."})
    return alerts


def build_platform_payload(
    quant: Dict[str, Any],
    portfolio: Dict[str, Any],
    discovery: Dict[str, Any] | None = None,
    watchlist: Dict[str, Any] | None = None,
    trade: Dict[str, Any] | None = None,
    signal: Dict[str, Any] | None = None,
) -> Dict[str, Any]:
    """Build all institutional dashboard modules from saved app state."""
    discovery = discovery or {}
    signal = signal or {}
    return {
        "factor_exposure": factor_exposure_engine(quant, portfolio),
        "attribution": attribution_engine(portfolio, quant),
        "risk_contribution": risk_contribution_engine(portfolio),
        "correlation_network": correlation_network_engine(portfolio),
        "stock_research_scores": stock_research_score_engine(quant),
        "confidence": confidence_engine(quant, signal),
        "assumptions": assumption_checker(portfolio, quant),
        "market_breadth": market_breadth_dashboard(quant),
        "sector_rotation": sector_rotation_engine(portfolio, discovery),
        "liquidity": liquidity_dashboard(quant),
        "probability_forecasts": probability_forecasting_lab(quant),
        "scenarios": scenario_engine(portfolio),
        "optimizer": optimizer_lab(portfolio),
        "efficient_frontier": efficient_frontier(portfolio),
        "historical_stress_tests": historical_stress_testing(portfolio),
        "research_notebook": research_notebook(quant),
        "alerts": intelligent_alerts(portfolio, quant),
    }
