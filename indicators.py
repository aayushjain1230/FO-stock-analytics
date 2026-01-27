import pandas as pd
import pandas_ta as ta
import numpy as np


def calculate_metrics(df, benchmark_df):
    """
    Advanced Technical Engine for the JFO Analytics Project.
    Integrates Institutional Volume, Mansfield RS, ATR Volatility,
    and Multi-Timeframe RSI.
    """

    # ------------------------------------------------------------
    # Safety Check: ensure datetime index
    # ------------------------------------------------------------
    if not isinstance(df.index, pd.DatetimeIndex):
        df = df.copy()
        df.index = pd.to_datetime(df.index)

    if not isinstance(benchmark_df.index, pd.DatetimeIndex):
        benchmark_df = benchmark_df.copy()
        benchmark_df.index = pd.to_datetime(benchmark_df.index)

    # ------------------------------------------------------------
    # Align CLOSE series only (critical fix for multi-column)
    # ------------------------------------------------------------
    bench_close = None
    if isinstance(benchmark_df, pd.DataFrame):
        if "Close" in benchmark_df.columns:
            bench_close = benchmark_df["Close"]
        else:
            bench_close = benchmark_df.iloc[:, 0]  # pick first column if multi-column
    else:
        bench_close = benchmark_df

    df_close, benchmark_close = df["Close"].align(bench_close, join="inner")
    df = df.loc[df_close.index]  # align df to match

    # ------------------------------------------------------------
    # 1. Trend Foundation & Stage Analysis
    # ------------------------------------------------------------
    df["SMA20"] = pd.to_numeric(ta.sma(df_close, length=20), errors="coerce")
    df["SMA50"] = pd.to_numeric(ta.sma(df_close, length=50), errors="coerce")
    df["SMA200"] = pd.to_numeric(ta.sma(df_close, length=200), errors="coerce")

    # ------------------------------------------------------------
    # 2. Institutional Volume Intelligence
    # ------------------------------------------------------------
    df["Vol_20_Avg"] = pd.to_numeric(ta.sma(df["Volume"], length=20), errors="coerce")
    df["RV"] = df["Volume"] / df["Vol_20_Avg"]
    df["Volume_Spike"] = (df["RV"] >= 2.0).fillna(False)

    # ------------------------------------------------------------
    # 3. Mansfield Relative Strength (MRS)
    # ------------------------------------------------------------
    df["RS_Line"] = df_close / benchmark_close
    df["RS_SMA50"] = pd.to_numeric(ta.sma(df["RS_Line"], length=50), errors="coerce")
    df["RS_SMA20"] = pd.to_numeric(ta.sma(df["RS_Line"], length=20), errors="coerce")
    df["MRS"] = ((df["RS_Line"] / df["RS_SMA50"]) - 1) * 100

    # ------------------------------------------------------------
    # 4. Volatility Guard (ATR)
    # ------------------------------------------------------------
    df["ATR"] = pd.to_numeric(ta.atr(df["High"], df["Low"], df_close, length=14), errors="coerce")
    atr_safe = df["ATR"].replace(0, np.nan)
    df["Dist_SMA20"] = (df_close - df["SMA20"]) / atr_safe

    # ------------------------------------------------------------
    # 5. Momentum (Daily RSI)
    # ------------------------------------------------------------
    df["RSI"] = pd.to_numeric(ta.rsi(df_close, length=14), errors="coerce")

    # ------------------------------------------------------------
    # 6. Multi-Timeframe RSI (Weekly / Monthly)
    # ------------------------------------------------------------
    ohlc_dict = {
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last",
        "Volume": "sum",
    }

    # Weekly
    df_weekly = df.resample("W").apply(ohlc_dict)
    df_weekly["RSI_W"] = pd.to_numeric(ta.rsi(df_weekly["Close"], length=14), errors="coerce")
    df["RSI_Weekly"] = df_weekly["RSI_W"].reindex(df.index, method="ffill")

    # Monthly
    df_monthly = df.resample("M").apply(ohlc_dict)
    df_monthly["RSI_M"] = pd.to_numeric(ta.rsi(df_monthly["Close"], length=14), errors="coerce")
    df["RSI_Monthly"] = df_monthly["RSI_M"].reindex(df.index, method="ffill")

    # ------------------------------------------------------------
    # 7. Critical Signal Triggers
    # ------------------------------------------------------------
    df["Golden_Cross"] = ((df["SMA50"] > df["SMA200"]) & (df["SMA50"].shift(1) <= df["SMA200"].shift(1))).fillna(False)
    df["RS_Breakout"] = ((df["MRS"] > 0) & (df["MRS"].shift(1) <= 0)).fillna(False)

    # ------------------------------------------------------------
    # 8. Market Regime Label (metadata only)
    # ------------------------------------------------------------
    df.attrs["market_regime"] = get_market_regime_label(benchmark_df.loc[df.index])

    return df


def get_market_regime_label(spy_df):
    """
    Determines the market regime based on SPY SMA200.
    Returns a string label.
    """
    try:
        if spy_df is None or len(spy_df) < 200:
            return "Unknown (Incomplete Data)"

        close = spy_df["Close"]
        sma_raw = ta.sma(close, length=200)

        # Normalize to Series
        if isinstance(sma_raw, pd.Series):
            sma_series = pd.to_numeric(sma_raw, errors="coerce")
        elif isinstance(sma_raw, (float, np.floating)):
            sma_series = pd.Series([sma_raw] * len(close), index=close.index)
        else:
            sma_series = pd.Series(sma_raw, index=close.index).astype(float)

        latest_sma = sma_series.iloc[-1]
        latest_close = close.iloc[-1]

        if pd.isna(latest_sma):
            return "Neutral (Calculating...)"
        return "🟢 Bullish (Above SMA200)" if latest_close > latest_sma else "🔴 Bearish (Below SMA200)"

    except Exception as e:
        print(f"Regime Check Error: {e}")
        return "Neutral"


def calculate_market_leader_score(row):
    """
    JFO Market Leader Score (0–100)
    """
    score = 0

    # Safety check
    for key in ("SMA200", "RSI_Weekly", "MRS"):
        if row.get(key) is None or pd.isna(row[key]):
            return 0

    # Stage & Trend Health
    if pd.notna(row.get("SMA50")) and pd.notna(row.get("SMA200")) and row["Close"] > row["SMA50"] > row["SMA200"]:
        score += 40
    elif row["Close"] > row["SMA200"]:
        score += 20

    # Relative Strength
    if row["MRS"] > 0:
        score += 20
    if pd.notna(row.get("RS_Line")) and pd.notna(row.get("RS_SMA20")) and row["RS_Line"] > row["RS_SMA20"]:
        score += 10

    # Momentum & Volume
    if row["RSI_Weekly"] > 50:
        score += 10
    if row.get("RSI_Monthly") is not None and row["RSI_Monthly"] > 50:
        score += 10
    if row.get("RV") is not None and row["RV"] > 1.5:
        score += 10

    # Extension Penalty
    if row.get("Dist_SMA20") is not None and row["Dist_SMA20"] > 3.0:
        score -= 20

    return max(0, min(score, 100))
