from datetime import datetime
from typing import Dict

import pandas as pd

import database
import quant_analytics


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
            SELECT s.signal_type, o.return_1w, o.return_1m, o.return_3m, o.return_6m, o.max_drawdown, o.sp500_relative_return
            FROM signals s
            JOIN signal_outcomes o ON o.signal_id = s.id
            """
        ).fetchall()
    if not rows:
        return {"message": "No completed signal outcomes yet."}
    frame = pd.DataFrame([dict(row) for row in rows])
    summary = {}
    for signal_type, group in frame.groupby("signal_type"):
        summary[signal_type] = {
            "count": int(len(group)),
            "win_rate_1m": float((group["return_1m"].dropna() > 0).mean()) if group["return_1m"].notna().any() else None,
            "average_return_1m": _mean(group["return_1m"]),
            "median_return_1m": _median(group["return_1m"]),
            "average_sp500_relative_return": _mean(group["sp500_relative_return"]),
            "average_max_drawdown": _mean(group["max_drawdown"]),
            "false_positive_rate_1m": float((group["return_1m"].dropna() <= 0).mean()) if group["return_1m"].notna().any() else None,
        }
    return {"generated_at": datetime.now().isoformat(), "signal_types": summary}


def _mean(series):
    values = series.dropna()
    return float(values.mean()) if len(values) else None


def _median(series):
    values = series.dropna()
    return float(values.median()) if len(values) else None
