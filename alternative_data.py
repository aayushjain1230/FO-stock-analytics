"""Provider-neutral alternative-data feature engineering and validation."""

from typing import Dict

import numpy as np
import pandas as pd

import research_mindset


SOURCE_DIRECTIONS = {
    "google_trends": 1,
    "reddit_sentiment": 1,
    "news_sentiment": 1,
    "transcript_sentiment": 1,
    "app_download_growth": 1,
    "website_traffic_growth": 1,
}


def engineer_features(raw_data: Dict[str, pd.DataFrame], max_staleness_days: int = 14) -> Dict:
    """
    Normalize configured sources without assuming any specific vendor.

    Each source DataFrame should be date-indexed with tickers as columns.
    """
    normalized = {}
    quality = {}
    for source, frame in (raw_data or {}).items():
        if frame is None or frame.empty:
            continue
        clean = frame.apply(pd.to_numeric, errors="coerce").sort_index()
        rolling_mean = clean.rolling(60, min_periods=20).mean()
        rolling_std = clean.rolling(60, min_periods=20).std().replace(0, np.nan)
        zscore = (clean - rolling_mean) / rolling_std
        change_7d = clean.pct_change(7).replace([np.inf, -np.inf], np.nan)
        normalized[source] = {
            "level": clean,
            "zscore_60d": zscore,
            "change_7d": change_7d,
        }
        last_valid = clean.apply(lambda series: series.last_valid_index())
        latest_date = clean.index.max()
        stale = {
            ticker: (
                True
                if date is None
                else (pd.Timestamp(latest_date) - pd.Timestamp(date)).days > max_staleness_days
            )
            for ticker, date in last_valid.items()
        }
        quality[source] = {
            "coverage_pct": round(float(clean.notna().mean().mean() * 100), 2),
            "stale_tickers": [ticker for ticker, is_stale in stale.items() if is_stale],
            "latest_date": str(latest_date),
        }
    return {"available": bool(normalized), "features": normalized, "data_quality": quality}


def latest_composite_signal(engineered: Dict, source_weights: Dict[str, float] | None = None) -> Dict:
    features = engineered.get("features", {}) if engineered else {}
    if not features:
        return {"available": False, "message": "No configured alternative-data sources."}

    weights = source_weights or {source: 1.0 for source in features}
    latest_rows = []
    for source, payload in features.items():
        zscore = payload["zscore_60d"]
        if zscore.empty:
            continue
        row = zscore.iloc[-1].rename(source) * SOURCE_DIRECTIONS.get(source, 1)
        latest_rows.append(row)
    if not latest_rows:
        return {"available": False, "message": "Alternative-data history is too short to normalize."}

    matrix = pd.concat(latest_rows, axis=1)
    active_weights = pd.Series({column: weights.get(column, 1.0) for column in matrix.columns})
    weighted = matrix.mul(active_weights, axis=1)
    denominator = matrix.notna().mul(active_weights, axis=1).sum(axis=1).replace(0, np.nan)
    composite = weighted.sum(axis=1, skipna=True) / denominator
    coverage = matrix.notna().mean(axis=1)

    rows = {}
    for ticker in matrix.index:
        value = composite.get(ticker)
        if pd.isna(value):
            continue
        confidence = min(90, float(coverage[ticker] * 100))
        rows[str(ticker)] = {
            "composite_zscore": round(float(value), 3),
            "coverage_pct": round(float(coverage[ticker] * 100), 1),
            "source_signals": {
                source: round(float(matrix.loc[ticker, source]), 3)
                for source in matrix.columns
                if pd.notna(matrix.loc[ticker, source])
            },
            "label": "Bullish" if value >= 1 else "Bearish" if value <= -1 else "Neutral",
            "research_mindset": research_mindset.research_envelope(
                "alternative_data",
                "Non-price behavior may reveal changing attention or demand before financial statements.",
                [f"Composite normalized signal: {value:.2f}", f"Source coverage: {coverage[ticker] * 100:.1f}%"],
                [
                    "Source definitions and collection methods are stable.",
                    "The signal is not dominated by bots, promotions, or one-off events.",
                    "Publication timestamps do not leak future information.",
                ],
                [
                    "Attention can rise because of negative events.",
                    "Vendor methodology changes can create false signals.",
                    "Alternative signals decay quickly and are vulnerable to crowding.",
                ],
                confidence=confidence,
                regime_weaknesses=["News shock", "Meme-driven market", "Sparse data"],
            ),
        }
    return {
        "available": True,
        "source_weights": active_weights.to_dict(),
        "stocks": rows,
        "leaderboard": sorted(
            [{"ticker": ticker, **payload} for ticker, payload in rows.items()],
            key=lambda item: item["composite_zscore"],
            reverse=True,
        ),
    }


def predictive_test(signal_history: pd.DataFrame, forward_returns: pd.DataFrame, quantiles: int = 5) -> Dict:
    signal = signal_history.stack().rename("signal")
    future = forward_returns.stack().rename("forward_return")
    aligned = pd.concat([signal, future], axis=1).dropna()
    if len(aligned) < quantiles * 20:
        return {"available": False, "message": "Need more timestamp-aligned history for predictive testing."}
    aligned["bucket"] = pd.qcut(aligned["signal"], quantiles, labels=False, duplicates="drop")
    bucket_returns = aligned.groupby("bucket")["forward_return"].mean()
    ic = aligned["signal"].corr(aligned["forward_return"], method="spearman")
    spread = bucket_returns.iloc[-1] - bucket_returns.iloc[0] if len(bucket_returns) >= 2 else 0
    return {
        "available": True,
        "observations": int(len(aligned)),
        "information_coefficient": float(ic) if pd.notna(ic) else None,
        "top_minus_bottom_return": float(spread),
        "bucket_returns": {str(key): float(value) for key, value in bucket_returns.items()},
        "passes_basic_validation": bool(pd.notna(ic) and ic > 0.03 and spread > 0),
        "warning": "Do not deploy unless results survive walk-forward tests, source lagging, and realistic acquisition costs.",
    }


def provider_requirements() -> Dict:
    return {
        "google_trends": "Configure a Trends provider or pytrends-compatible export.",
        "reddit_sentiment": "Configure an API-compliant Reddit data provider and bot/spam controls.",
        "news_sentiment": "Configure timestamped full-text news and a sentiment model.",
        "transcript_sentiment": "Configure licensed earnings transcripts with speaker-aware parsing.",
        "app_download_growth": "Configure a licensed mobile intelligence provider.",
        "website_traffic_growth": "Configure a licensed web traffic provider.",
        "policy": "No alternative source influences rankings until timestamp integrity and predictive validation pass.",
    }
