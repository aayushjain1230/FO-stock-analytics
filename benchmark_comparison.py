from typing import Dict
import pandas as pd

import quant_analytics


def comparison(price_frame: pd.DataFrame, benchmark_df: pd.DataFrame) -> Dict:
    if price_frame is None or price_frame.empty or benchmark_df is None or benchmark_df.empty:
        return {"available": False, "message": "Portfolio or benchmark price data unavailable."}
    weights = pd.Series(1 / len(price_frame.columns), index=price_frame.columns)
    portfolio_price = (price_frame / price_frame.iloc[0]).dot(weights)
    benchmark_price = quant_analytics._as_price_series(benchmark_df)
    aligned = pd.concat([portfolio_price.rename("portfolio"), benchmark_price.rename("benchmark")], axis=1).dropna()
    if aligned.empty:
        return {"available": False, "message": "No aligned benchmark data."}
    out = {"available": True, "benchmark": "SPY", "periods": {}}
    for label, days in [("1M", 21), ("3M", 63), ("6M", 126), ("1Y", 252)]:
        if len(aligned) <= days:
            continue
        p = aligned["portfolio"].iloc[-1] / aligned["portfolio"].iloc[-days] - 1
        b = aligned["benchmark"].iloc[-1] / aligned["benchmark"].iloc[-days] - 1
        out["periods"][label] = {"portfolio_return": float(p), "benchmark_return": float(b), "relative_return": float(p - b)}
    return out
