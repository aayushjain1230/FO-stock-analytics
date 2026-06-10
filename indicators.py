import numpy as np
import pandas as pd
try:
    import pandas_ta as ta
except ModuleNotFoundError:
    class _TechnicalAnalysisFallback:
        @staticmethod
        def sma(series, length=20):
            return pd.Series(series).rolling(length).mean()

        @staticmethod
        def ema(series, length=20):
            return pd.Series(series).ewm(span=length, adjust=False).mean()

        @staticmethod
        def rsi(series, length=14):
            series = pd.Series(series).astype(float)
            delta = series.diff()
            gain = delta.clip(lower=0).rolling(length).mean()
            loss = (-delta.clip(upper=0)).rolling(length).mean()
            rs = gain / loss.replace(0, np.nan)
            return 100 - (100 / (1 + rs))

        @staticmethod
        def atr(high, low, close, length=14):
            high = pd.Series(high).astype(float)
            low = pd.Series(low).astype(float)
            close = pd.Series(close).astype(float)
            true_range = pd.concat([high - low, (high - close.shift()).abs(), (low - close.shift()).abs()], axis=1).max(axis=1)
            return true_range.rolling(length).mean()

        @staticmethod
        def macd(series, fast=12, slow=26, signal=9):
            series = pd.Series(series).astype(float)
            macd_line = series.ewm(span=fast, adjust=False).mean() - series.ewm(span=slow, adjust=False).mean()
            signal_line = macd_line.ewm(span=signal, adjust=False).mean()
            histogram = macd_line - signal_line
            return pd.DataFrame({
                f"MACD_{fast}_{slow}_{signal}": macd_line,
                f"MACDh_{fast}_{slow}_{signal}": histogram,
                f"MACDs_{fast}_{slow}_{signal}": signal_line,
            })

        @staticmethod
        def roc(series, length=20):
            return pd.Series(series).pct_change(length) * 100

    ta = _TechnicalAnalysisFallback()


DEFAULT_SETTINGS = {
    "rsi_period": 14,
    "atr_period": 14,
    "sma_trend": 20,
    "sma_fast": 50,
    "sma_slow": 200,
    "relative_strength_period": 50,
    "relative_strength_signal_period": 20,
    "volume_average_period": 20,
    "relative_volume_alert_threshold": 2.0,
    "fifty_two_week_window": 252,
}


def _get_settings(config):
    settings = dict(DEFAULT_SETTINGS)
    if isinstance(config, dict):
        settings.update(config.get("settings", {}))
    return settings


def calculate_metrics(df, benchmark_df, config=None):
    """Calculate trend, momentum, relative strength, and volatility metrics."""
    settings = _get_settings(config)

    if not isinstance(df.index, pd.DatetimeIndex):
        df = df.copy()
        df.index = pd.to_datetime(df.index)

    if not isinstance(benchmark_df.index, pd.DatetimeIndex):
        benchmark_df = benchmark_df.copy()
        benchmark_df.index = pd.to_datetime(benchmark_df.index)

    if isinstance(benchmark_df, pd.DataFrame):
        bench_close = benchmark_df["Close"] if "Close" in benchmark_df.columns else benchmark_df.iloc[:, 0]
    else:
        bench_close = benchmark_df

    df_close, benchmark_close = df["Close"].align(bench_close, join="inner")
    df = df.loc[df_close.index].copy()

    sma_trend = int(settings["sma_trend"])
    sma_fast = int(settings["sma_fast"])
    sma_slow = int(settings["sma_slow"])
    rsi_period = int(settings["rsi_period"])
    atr_period = int(settings["atr_period"])
    rs_period = int(settings["relative_strength_period"])
    rs_signal_period = int(settings["relative_strength_signal_period"])
    volume_average_period = int(settings["volume_average_period"])
    volume_spike_threshold = float(settings["relative_volume_alert_threshold"])
    fifty_two_week_window = int(settings["fifty_two_week_window"])

    df["SMA20"] = pd.to_numeric(ta.sma(df_close, length=sma_trend), errors="coerce")
    df["SMA50"] = pd.to_numeric(ta.sma(df_close, length=sma_fast), errors="coerce")
    df["SMA200"] = pd.to_numeric(ta.sma(df_close, length=sma_slow), errors="coerce")
    df["EMA20"] = pd.to_numeric(ta.ema(df_close, length=sma_trend), errors="coerce")
    df["EMA50"] = pd.to_numeric(ta.ema(df_close, length=sma_fast), errors="coerce")
    df["EMA200"] = pd.to_numeric(ta.ema(df_close, length=sma_slow), errors="coerce")

    macd = ta.macd(df_close)
    if macd is not None and not macd.empty:
        df["MACD"] = pd.to_numeric(macd.iloc[:, 0], errors="coerce")
        df["MACD_Histogram"] = pd.to_numeric(macd.iloc[:, 1], errors="coerce")
        df["MACD_Signal"] = pd.to_numeric(macd.iloc[:, 2], errors="coerce")
    else:
        df["MACD"] = np.nan
        df["MACD_Histogram"] = np.nan
        df["MACD_Signal"] = np.nan

    df["ROC_20"] = pd.to_numeric(ta.roc(df_close, length=sma_trend), errors="coerce")

    volume_col = df.get("Volume")
    if volume_col is not None:
        df["Vol_20_Avg"] = pd.to_numeric(ta.sma(volume_col, length=volume_average_period), errors="coerce")
        df["RV"] = volume_col / df["Vol_20_Avg"]
        df["Volume_Spike"] = (df["RV"] >= volume_spike_threshold).fillna(False)
    else:
        df["Vol_20_Avg"] = np.nan
        df["RV"] = np.nan
        df["Volume_Spike"] = False

    df["RS_Line"] = df_close / benchmark_close
    df["RS_SMA50"] = pd.to_numeric(ta.sma(df["RS_Line"], length=rs_period), errors="coerce")
    df["RS_SMA20"] = pd.to_numeric(ta.sma(df["RS_Line"], length=rs_signal_period), errors="coerce")
    df["MRS"] = ((df["RS_Line"] / df["RS_SMA50"]) - 1) * 100

    df["ATR"] = pd.to_numeric(ta.atr(df["High"], df["Low"], df_close, length=atr_period), errors="coerce")
    atr_safe = df["ATR"].replace(0, np.nan)
    df["Dist_SMA20"] = (df_close - df["SMA20"]) / atr_safe

    df["RSI"] = pd.to_numeric(ta.rsi(df_close, length=rsi_period), errors="coerce")

    ohlc_dict = {
        "Open": "first",
        "High": "max",
        "Low": "min",
        "Close": "last",
        "Volume": "sum",
    }
    active_ohlc = {k: v for k, v in ohlc_dict.items() if k in df.columns}

    df_weekly = df.resample("W").apply(active_ohlc)
    df_weekly["RSI_W"] = pd.to_numeric(ta.rsi(df_weekly["Close"], length=rsi_period), errors="coerce")
    df["RSI_Weekly"] = df_weekly["RSI_W"].reindex(df.index, method="ffill")

    df_monthly = df.resample("ME").apply(active_ohlc)
    df_monthly["RSI_M"] = pd.to_numeric(ta.rsi(df_monthly["Close"], length=rsi_period), errors="coerce")
    df["RSI_Monthly"] = df_monthly["RSI_M"].reindex(df.index, method="ffill")

    rolling_window = df_close.rolling(window=fifty_two_week_window, min_periods=1)
    df["High_52W"] = rolling_window.max()
    df["Low_52W"] = rolling_window.min()

    df["Golden_Cross"] = ((df["SMA50"] > df["SMA200"]) & (df["SMA50"].shift(1) <= df["SMA200"].shift(1))).fillna(False)
    df["RS_Breakout"] = ((df["MRS"] > 0) & (df["MRS"].shift(1) <= 0)).fillna(False)

    df.attrs["market_regime"] = get_market_regime_label(benchmark_df.loc[df.index], config=config)
    return df


def get_market_regime_label(spy_df, config=None):
    """Determine the market regime using the configured slow SMA."""
    settings = _get_settings(config)
    slow_length = int(settings["sma_slow"])

    try:
        if spy_df is None or len(spy_df) < slow_length:
            return "Unknown (Incomplete Data)"

        close = spy_df["Close"]
        sma_raw = ta.sma(close, length=slow_length)

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

        return "Bullish (Above SMA200)" if latest_close > latest_sma else "Bearish (Below SMA200)"
    except Exception as e:
        print(f"Regime Check Error: {e}")
        return "Neutral"


def calculate_market_leader_score(row):
    """Legacy score helper retained for compatibility."""
    score = 0

    for key in ("SMA200", "RSI_Weekly", "MRS"):
        val = row.get(key)
        if val is None or pd.isna(val):
            return 0

    if pd.notna(row.get("SMA50")) and pd.notna(row.get("SMA200")) and row["Close"] > row["SMA50"] > row["SMA200"]:
        score += 40
    elif row["Close"] > row["SMA200"]:
        score += 20

    if row["MRS"] > 0:
        score += 20
    if pd.notna(row.get("RS_Line")) and pd.notna(row.get("RS_SMA20")) and row["RS_Line"] > row["RS_SMA20"]:
        score += 10

    if row["RSI_Weekly"] > 50:
        score += 10
    if pd.notna(row.get("RSI_Monthly")) and row["RSI_Monthly"] > 50:
        score += 10
    if pd.notna(row.get("RV")) and row["RV"] > 1.5:
        score += 10

    if pd.notna(row.get("Dist_SMA20")) and row["Dist_SMA20"] > 3.0:
        score -= 20

    return max(0, min(score, 100))
