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
    # Safety Check: Align dataframes so RS calculations are valid
    # ------------------------------------------------------------
    df, benchmark_df = df.align(benchmark_df, join="inner", axis=0)

    # ------------------------------------------------------------
    # 1. Trend Foundation & Stage Analysis
    # ------------------------------------------------------------
    df["SMA20"] = pd.to_numeric(ta.sma(df["Close"], length=20), errors="coerce")
    df["SMA50"] = pd.to_numeric(ta.sma(df["Close"], length=50), errors="coerce")
    df["SMA200"] = pd.to_numeric(ta.sma(df["Close"], length=200), errors="coerce")

    # ------------------------------------------------------------
    # 2. Institutional Volume Intelligence
    # ------------------------------------------------------------
    df["Vol_20_Avg"] = pd.to_numeric(ta.sma(df["Volume"], length=20), errors="coerce")
    df["RV"] = df["Volume"] / df["Vol_20_Avg"]
    df["Volume_Spike"] = (df["RV"] >= 2.0).fillna(False)  # True if 2x average volume

    # ------------------------------------------------------------
    # 3. Mansfield Relative Strength (MRS)
    # ------------------------------------------------------------
    benchmark_close = benchmark_df["Close"]
    if isinstance(benchmark_close, pd.DataFrame):
        benchmark_close = benchmark_close.iloc[:, 0]

    df["RS_Line"] = df["Close"].values / benchmark_close.values
    df["RS_SMA50"] = pd.to_numeric(ta.sma(df["RS_Line"], length=50), errors="coerce")
    df["RS_SMA20"] = pd.to_numeric(ta.sma(df["RS_Line"], length=20), errors="coerce")

    df["MRS"] = ((df["RS_Line"] / df["RS_SMA50"]) - 1) * 100

    # ------------------------------------------------------------
    # 4. Volatility Guard (ATR)
    # ------------------------------------------------------------
    df["ATR"] = pd.to_numeric(ta.atr(df["High"], df["Low"], df["Close"], length=14), errors="coerce")
    df["Dist_SMA20"] = (df["Close"] - df["SMA20"]) / df["ATR"]

    # ------------------------------------------------------------
    # 5. Momentum (Daily RSI)
    # ------------------------------------------------------------
    df["RSI"] = pd.to_numeric(ta.rsi(df["Close"], length=14), errors="coerce")

    # ------------------------------------------------------------
    # 6. Multi-Timeframe RSI (Weekly / Monthly)
    # ------------------------------------------------------------
    ohlc_dict = {"Open": "first", "High": "max", "Low": "min", "Close": "last", "Volume": "sum"}

    # Weekly Analysis
    df_weekly = df.resample("W").apply(ohlc_dict)
    df_weekly["RSI_W"] = pd.to_numeric(ta.rsi(df_weekly["Close"], length=14), errors="coerce")
    df["RSI_Weekly"] = df_weekly["RSI_W"].reindex(df.index, method="ffill")

    # Monthly Analysis
    df_monthly = df.resample("ME").apply(ohlc_dict)
    df_monthly["RSI_M"] = pd.to_numeric(ta.rsi(df_monthly["Close"], length=14), errors="coerce")
    df["RSI_Monthly"] = df_monthly["RSI_M"].reindex(df.index, method="ffill")

    # ------------------------------------------------------------
    # 7. Critical Signal Triggers
    # ------------------------------------------------------------
    df["Golden_Cross"] = (
        (df["SMA50"] > df["SMA200"]) & (df["SMA50"].shift(1) <= df["SMA200"].shift(1))
    ).fillna(False)

    df["RS_Breakout"] = ((df["MRS"] > 0) & (df["MRS"].shift(1) <= 0)).fillna(False)

    # ------------------------------------------------------------
    # 8. Market Regime Label (stored in metadata)
    # ------------------------------------------------------------
    df.attrs["market_regime"] = get_market_regime_label(benchmark_df)

    return df


def get_market_regime_label(spy_df):
    """
    Determines the market regime based on SPY SMA200.

    Returns:
        str: Market regime label
    """
    try:
        if spy_df is None or len(spy_df) < 200:
            return "Unknown (Incomplete Data)"

        sma_raw = ta.sma(spy_df["Close"], length=200)

        # Ensure sma_series is always a pd.Series
        if isinstance(sma_raw, (float, np.float64)):
            sma_series = pd.Series([sma_raw] * len(spy_df), index=spy_df.index)
        else:
            sma_series = pd.to_numeric(sma_raw, errors="coerce")

        # If somehow all values are NaN
        if sma_series.isna().all():
            return "Neutral (Calculating...)"

        latest_close = spy_df["Close"].iloc[-1]
        latest_sma = sma_series.iloc[-1]

        if latest_close > latest_sma:
            return "🟢 Bullish (Above SMA200)"
        else:
            return "🔴 Bearish (Below SMA200)"

    except Exception as e:
        print(f"Regime Check Error: {e}")
        return "Neutral"


def calculate_market_leader_score(row):
    """
    JFO Market Leader Score (0–100)

    Breakdown:
        - Stage & Trend Health: 40
        - Relative Strength: 30
        - Momentum & Volume: 30
        - Volatility penalty for climax extensions
    """
    score = 0

    # Safety Check
    if (
        pd.isna(row.get("SMA200"))
        or pd.isna(row.get("RSI_Weekly"))
        or pd.isna(row.get("MRS"))
    ):
        return 0

    # 1. Stage & Trend Health (40)
    if pd.notna(row.get("SMA50")) and pd.notna(row.get("SMA200")) and row["Close"] > row["SMA50"] > row["SMA200"]:
        score += 40
    elif pd.notna(row.get("SMA200")) and row["Close"] > row["SMA200"]:
        score += 20

    # 2. Relative Strength (30)
    if pd.notna(row.get("MRS")) and row["MRS"] > 0:
        score += 20
    if pd.notna(row.get("RS_Line")) and pd.notna(row.get("RS_SMA20")) and row["RS_Line"] > row["RS_SMA20"]:
        score += 10

    # 3. Momentum & Volume (30)
    if pd.notna(row.get("RSI_Weekly")) and row["RSI_Weekly"] > 50:
        score += 10
    if pd.notna(row.get("RSI_Monthly")) and row["RSI_Monthly"] > 50:
        score += 10
    if row.get("RV", 0) > 1.5:
        score += 10

    # 4. Extension Penalty
    if row.get("Dist_SMA20", 0) > 3.0:
        score -= 20

    return max(0, min(score, 100))

