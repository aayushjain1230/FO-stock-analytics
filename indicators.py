import pandas as pd
import pandas_ta as ta

def calculate_metrics(df, benchmark_df):
    """
    Calculates technical indicators: SMAs (20, 50, 200), RSI, 
    and Relative Strength vs Benchmark with sensitive event triggers.
    """
    # 1. Trend Indicators (Moving Averages)
    df['SMA20'] = df['Close'].rolling(window=20).mean()
    df['SMA50'] = df['Close'].rolling(window=50).mean()
    df['SMA200'] = df['Close'].rolling(window=200).mean()

    # 2. RSI (Relative Strength Index)
    df['RSI'] = ta.rsi(df['Close'], length=14)

    # 3. 52-Week High Reference
    df['52W_High'] = df['Close'].rolling(window=252).max()

    # 4. Relative Strength (RS) vs Benchmark
    df['RS_Line'] = df['Close'] / benchmark_df['Close']
    df['RS_SMA20'] = df['RS_Line'].rolling(window=20).mean()

    # 5. Multi-Timeframe Analysis
    ohlc_dict = {'Open':'first', 'High':'max', 'Low':'min', 'Close':'last', 'Volume':'sum'}
    
    # Weekly RSI
    df_weekly = df.resample('W').apply(ohlc_dict)
    df_weekly['RSI_W'] = ta.rsi(df_weekly['Close'], length=14)
    df['RSI_Weekly'] = df_weekly['RSI_W'].reindex(df.index, method='ffill')

    # Monthly RSI - Using 'ME' to avoid FutureWarning
    df_monthly = df.resample('ME').apply(ohlc_dict)
    df_monthly['RSI_M'] = ta.rsi(df_monthly['Close'], length=14)
    df['RSI_Monthly'] = df_monthly['RSI_M'].reindex(df.index, method='ffill')

    # --- SENSITIVE EVENT TRIGGERS ---
    
    # 6. Trend Event: Golden Cross (SMA50 crosses above SMA200)
    df['Golden_Cross'] = (df['SMA50'] > df['SMA200']) & (df['SMA50'].shift(1) <= df['SMA200'].shift(1))

    # 7. Momentum Event: RSI Oversold/Overbought Reversals
    df['RSI_Oversold'] = (df['RSI'] < 30)
    df['RSI_Overbought'] = (df['RSI'] > 70)

    # 8. RS Breakout (Now 1% above average for higher sensitivity)
    # This will trigger more often than a simple cross
    df['RS_Breakout'] = (df['RS_Line'] > (df['RS_SMA20'] * 1.01))

    # 9. Volume Spike (Lowered to 1.5x for more frequent alerts)
    df['Vol_20_Avg'] = df['Volume'].rolling(window=20).mean()
    df['Volume_Spike'] = df['Volume'] > (df['Vol_20_Avg'] * 1.5)

    return df
