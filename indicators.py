import pandas as pd
import pandas_ta as ta

def calculate_metrics(df, benchmark_df):
    """
    Calculates technical indicators: SMAs (20, 50, 200), RSI, 
    and Relative Strength vs Benchmark.
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
    # Calculated as the ratio: Stock Price / Benchmark Price
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

    return df