import yfinance as yf
import pandas as pd

def get_price_history(ticker):
    """
    Fetches 2 years of daily data to ensure 52-week metrics are accurate.
    """
    try:
        stock = yf.Ticker(ticker)
        # Fetch 2y to ensure indicators like SMA200 have enough lead-in data
        df = stock.history(period="2y", interval="1d")
        
        if df.empty:
            return None
            
        # Remove timezone info so stock and benchmark dates match perfectly
        df.index = pd.to_datetime(df.index).tz_localize(None)
        return df

    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return None

    