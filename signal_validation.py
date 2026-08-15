import json
import math
from datetime import datetime
from typing import Dict

import numpy as np
import pandas as pd

import database
import quant_analytics

try:
    from scipy import stats
except Exception:
    stats = None


HORIZONS = {
    "return_1w": 5,
    "return_1m": 21,
    "return_3m": 63,
    "return_6m": 126,
}


def calculate_signal_outcome(signal: Dict, price_df: pd.DataFrame, benchmark_df: pd.DataFrame | None = None) -> Dict:
    if price_df is None or price_df.empty:
        return {}
    close = quant_analytics._as_price_series(price_df)
    signal_date = pd.to_datetime(signal["date"])
    future = close[close.index >= signal_date]
    if future.empty:
        return {}
    entry = signal.get("entry_price") or future.iloc[0]
    payload = {}
    for key, days in HORIZONS.items():
        if len(future) > days:
            payload[key] = float(future.iloc[days] / entry - 1)
    if len(future) > 1:
        path_returns = future / entry - 1
        payload["max_drawdown"] = float(path_returns.min())
    if benchmark_df is not None and not benchmark_df.empty and "return_1m" in payload:
        benchmark_close = quant_analytics._as_price_series(benchmark_df)
        benchmark_future = benchmark_close[benchmark_close.index >= signal_date]
        if len(benchmark_future) > 21:
            payload["sp500_relative_return"] = payload["return_1m"] - float(benchmark_future.iloc[21] / benchmark_future.iloc[0] - 1)
    return payload


def update_signal_outcomes(price_lookup: Dict[str, pd.DataFrame], benchmark_df: pd.DataFrame | None = None) -> int:
    updated = 0
    for signal in database.iter_signals_without_outcomes():
        price_df = price_lookup.get(signal["ticker"])
        outcome = calculate_signal_outcome(signal, price_df, benchmark_df=benchmark_df)
        if outcome:
            database.upsert_signal_outcome(signal["id"], outcome)
            updated += 1
    return updated


def summarize_signal_performance():
    database.initialize_database()
    with database.connect() as conn:
        rows = conn.execute(
            """
            SELECT s.signal_type, s.score, s.confidence, s.market_regime, s.sector, s.payload_json,
                   o.return_1w, o.return_1m, o.return_3m, o.return_6m,
                   o.max_drawdown, o.sp500_relative_return
            FROM signals s
            JOIN signal_outcomes o ON o.signal_id = s.id
            """
        ).fetchall()
    if not rows:
        return {"generated_at": datetime.now().isoformat(), "message": "No completed signal outcomes yet.", "signal_types": {}}
    frame = pd.DataFrame([dict(row) for row in rows])
    summary = {}
    for signal_type, group in frame.groupby("signal_type"):
        summary[signal_type] = _signal_group_summary(group, horizon="return_1m")
    return {
        "generated_at": datetime.now().isoformat(),
        "primary_horizon": "return_1m",
        "signal_types": summary,
        "regression": _regression_summary(frame),
        "interpretation": "High expected value with low variance is more attractive than the same EV with high uncertainty.",
    }


def _signal_group_summary(group: pd.DataFrame, horizon: str = "return_1m") -> Dict:
    returns = pd.to_numeric(group[horizon], errors="coerce").dropna()
    wins = returns[returns > 0]
    losses = returns[returns <= 0]
    sample_size = int(len(returns))
    win_rate = float((returns > 0).mean()) if sample_size else None
    average_win = float(wins.mean()) if len(wins) else 0.0
    average_loss = float(losses.mean()) if len(losses) else 0.0
    expected_value = (win_rate * average_win + (1 - win_rate) * average_loss) if win_rate is not None else None
    variance = float(returns.var(ddof=1)) if sample_size > 1 else 0.0 if sample_size == 1 else None
    standard_deviation = float(returns.std(ddof=1)) if sample_size > 1 else 0.0 if sample_size == 1 else None
    sharpe_like = float(expected_value / standard_deviation) if expected_value is not None and standard_deviation not in (None, 0) else None
    p_value = _p_value_vs_zero(returns)
    ci_low, ci_high = _confidence_interval(returns, confidence_level=0.95)
    train_test = _train_test_split_summary(returns)
    confidence = _confidence_label(sample_size, p_value)
    attractiveness = _attractiveness_label(expected_value, variance, sample_size, p_value, train_test)
    return {
        "sample_size": sample_size,
        "win_rate": win_rate,
        "average_win": average_win,
        "average_loss": average_loss,
        "expected_value": expected_value,
        "average_return": _mean(returns),
        "median_return": _median(returns),
        "variance": variance,
        "standard_deviation": standard_deviation,
        "sharpe_like": sharpe_like,
        "p_value": p_value,
        "confidence_interval_95": {"low": ci_low, "high": ci_high},
        "train_test": train_test,
        "confidence": confidence,
        "attractiveness": attractiveness,
        "average_sp500_relative_return": _mean(group["sp500_relative_return"]),
        "average_max_drawdown": _mean(group["max_drawdown"]),
        "false_positive_rate": float((returns <= 0).mean()) if sample_size else None,
        "readout": _readout(expected_value, variance, attractiveness),
    }



def _confidence_interval(returns: pd.Series, confidence_level: float = 0.95):
    values = returns.dropna()
    n = len(values)
    if n < 2:
        mean = float(values.mean()) if n else None
        return mean, mean
    mean = float(values.mean())
    stderr = float(values.std(ddof=1) / np.sqrt(n))
    if stats is not None:
        critical = float(stats.t.ppf((1 + confidence_level) / 2, df=n - 1))
    else:
        critical = 1.96
    margin = critical * stderr
    return mean - margin, mean + margin


def _train_test_split_summary(returns: pd.Series, train_fraction: float = 0.70) -> Dict:
    values = returns.dropna().reset_index(drop=True)
    n = len(values)
    if n < 10:
        return {"available": False, "message": "Need at least 10 outcomes for train/test split."}
    split = max(1, min(n - 1, int(n * train_fraction)))
    train = values.iloc[:split]
    test = values.iloc[split:]
    train_mean = float(train.mean()) if len(train) else None
    test_mean = float(test.mean()) if len(test) else None
    degradation = (test_mean - train_mean) if train_mean is not None and test_mean is not None else None
    return {
        "available": True,
        "train_size": int(len(train)),
        "test_size": int(len(test)),
        "train_average_return": train_mean,
        "test_average_return": test_mean,
        "generalization_gap": degradation,
        "passes_out_of_sample": bool(test_mean is not None and test_mean > 0),
    }


def _p_value_vs_zero(returns: pd.Series):
    values = returns.dropna()
    if len(values) < 3:
        return None
    if stats is not None:
        try:
            return float(stats.ttest_1samp(values, 0.0, nan_policy="omit").pvalue)
        except Exception:
            pass
    std = values.std(ddof=1)
    if std == 0:
        return 0.0 if values.mean() != 0 else 1.0
    t_stat = values.mean() / (std / np.sqrt(len(values)))
    # Normal approximation fallback.
    return float(2 * (1 - 0.5 * (1 + math.erf(abs(t_stat) / np.sqrt(2)))))


def _confidence_label(sample_size: int, p_value):
    if sample_size < 30:
        return "Low sample size"
    if p_value is None:
        return "Unknown"
    if p_value <= 0.01:
        return "High"
    if p_value <= 0.05:
        return "Moderate"
    return "Low / likely noisy"


def _attractiveness_label(expected_value, variance, sample_size: int, p_value, train_test: Dict = None):
    if expected_value is None or variance is None:
        return "Insufficient data"
    if sample_size < 10:
        return "Too early"
    if expected_value <= 0:
        return "Unattractive"
    if train_test and train_test.get("test_average_return") is not None and train_test.get("test_average_return") <= 0:
        return "Fails out-of-sample check"
    if p_value is not None and p_value > 0.10 and sample_size >= 30:
        return "Positive but not statistically convincing"
    if expected_value >= 0.04 and variance <= 0.01:
        return "Very attractive signal"
    if expected_value >= 0.04 and variance > 0.04:
        return "High EV but dangerous variance"
    if expected_value > 0 and variance <= 0.02:
        return "Constructive"
    return "Positive EV but monitor risk"


def _readout(expected_value, variance, attractiveness):
    if expected_value is None:
        return "No expected value estimate yet."
    if variance is None:
        return f"Expected value is {expected_value:.2%}; variance unavailable."
    return f"Expected value is {expected_value:.2%} with variance {variance:.4f}. {attractiveness}."


def _regression_summary(frame: pd.DataFrame) -> Dict:
    usable = frame[["return_1m", "score", "confidence"]].apply(pd.to_numeric, errors="coerce").dropna()
    if len(usable) < 20:
        return {"available": False, "message": "Need at least 20 completed signals for regression evidence."}
    y = usable["return_1m"].values
    x_raw = usable[["score", "confidence"]].values
    x = np.column_stack([np.ones(len(x_raw)), x_raw])
    coefficients = np.linalg.lstsq(x, y, rcond=None)[0]
    predictions = x @ coefficients
    residuals = y - predictions
    dof = max(len(y) - x.shape[1], 1)
    mse = float((residuals @ residuals) / dof)
    cov = mse * np.linalg.pinv(x.T @ x)
    se = np.sqrt(np.diag(cov))
    result = {}
    for idx, name in enumerate(["intercept", "score", "confidence"]):
        coef = float(coefficients[idx])
        stderr = float(se[idx]) if se[idx] else None
        t_stat = coef / stderr if stderr else None
        p_value = None
        if t_stat is not None and stats is not None:
            p_value = float(2 * stats.t.sf(abs(t_stat), dof))
        result[name] = {"coefficient": coef, "std_error": stderr, "t_stat": t_stat, "p_value": p_value}
    ss_total = float(((y - y.mean()) @ (y - y.mean())))
    ss_resid = float(residuals @ residuals)
    return {"available": True, "sample_size": int(len(y)), "r_squared": 1 - ss_resid / ss_total if ss_total else 0.0, "features": result}


def _mean(series):
    values = pd.to_numeric(series, errors="coerce").dropna()
    return float(values.mean()) if len(values) else None


def _median(series):
    values = pd.to_numeric(series, errors="coerce").dropna()
    return float(values.median()) if len(values) else None
