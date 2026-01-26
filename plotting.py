import matplotlib
matplotlib.use('Agg')  # Required for running on GitHub Actions
import matplotlib.pyplot as plt
import pandas as pd
import math
import os

# Create the folder if it doesn't exist
if not os.path.exists('plots'):
    os.makedirs('plots')
    print("Created 'plots' directory.")

def _flatten_columns(df):
    """Helper to handle MultiIndex columns from yfinance batch downloads."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(-1)
    return df

def create_chart(ticker, df, benchmark_df, score=None):
    """
    Detailed single-stock chart with SMAs, Benchmark overlay, and Market Leader Score.
    """
    df = _flatten_columns(df.copy())
    benchmark_df = _flatten_columns(benchmark_df.copy())

    if 'Close' not in df.columns or 'Close' not in benchmark_df.columns:
        print(f"Skipping chart for {ticker}: 'Close' column missing")
        return

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(12, 10), sharex=True,
        gridspec_kw={'height_ratios': [3, 1]}
    )

    # --- TOP PANEL: PRICE, SMAs & BENCHMARK ---
    ax1.plot(df.index, df['Close'], color='black', label='Price', linewidth=1.5, zorder=5)
    for sma_col, color, label, style in [
        ('SMA20', 'cyan', '20-Day SMA', '--'),
        ('SMA50', 'orange', '50-Day SMA', '-'),
        ('SMA200', 'red', '200-Day SMA', '-')
    ]:
        if sma_col in df.columns:
            ax1.plot(df.index, df[sma_col], color=color, label=label, linestyle=style, alpha=0.8)

    # Overlay Normalized SPY Benchmark
    if not benchmark_df.empty:
        bench_norm = (benchmark_df['Close'] / benchmark_df['Close'].iloc[0]) * df['Close'].iloc[0]
        ax1.plot(df.index, bench_norm, color='gray', label='SPY (Bench)', alpha=0.3, linewidth=1)

    if '52W_High' in df.columns:
        ax1.axhline(y=df['52W_High'].iloc[-1], color='green', linestyle=':', alpha=0.5, label='52W High')

    if score is not None:
        color_box = 'green' if score >= 70 else 'orange' if score >= 40 else 'red'
        ax1.text(0.02, 0.95, f'JFO Score: {score}/100', transform=ax1.transAxes,
                 fontsize=14, fontweight='bold', color='white',
                 bbox=dict(facecolor=color_box, alpha=0.8, edgecolor='none'))

    ax1.set_title(f"{ticker} - Detailed Technical Analysis", fontsize=16, fontweight='bold')
    ax1.set_ylabel("Price (USD)")
    ax1.legend(loc='upper left', ncol=2, fontsize=9)
    ax1.grid(True, alpha=0.2)

    # --- BOTTOM PANEL: RELATIVE STRENGTH ---
    if 'RS_Line' in df.columns:
        ax2.plot(df.index, df['RS_Line'], color='purple', label='RS Line (Price/SPY)')
        if 'RS_SMA20' in df.columns:
            ax2.plot(df.index, df['RS_SMA20'], color='gray', linestyle=':', alpha=0.6)
            ax2.fill_between(df.index, df['RS_Line'], df['RS_SMA20'],
                             where=(df['RS_Line'] >= df['RS_SMA20']), color='green', alpha=0.1)

        ax2.set_ylabel("RS Ratio")
        ax2.legend(loc='upper left', fontsize=9)
        ax2.grid(True, alpha=0.2)

    plt.tight_layout()
    save_path = os.path.join('plots', f'{ticker}_analysis.png')
    plt.savefig(save_path, dpi=150)
    plt.close()

def create_comparison_chart(all_stock_data, benchmark_df):
    """
    Executive Dashboard showing ALL watchlist stocks.
    """
    if not all_stock_data or benchmark_df is None or 'Close' not in benchmark_df.columns:
        plt.figure(figsize=(10, 5))
        plt.text(0.5, 0.5, "No Watchlist Data Found to Plot", ha='center')
        plt.savefig('plots/no_data_error.png')
        plt.close()
        print("No data available for comparison. Created error image.")
        return

    benchmark_df = _flatten_columns(benchmark_df.copy())
    tickers = list(all_stock_data.keys())
    num_stocks = len(tickers)
    
    fig_height = 6 + (4 * num_stocks)
    fig = plt.figure(figsize=(15, fig_height))
    gs = fig.add_gridspec(num_stocks + 1, 2, height_ratios=[2] + [1.2]*num_stocks)

    # --- TOP PANEL: CUMULATIVE RETURNS % ---
    ax_main = fig.add_subplot(gs[0, :])
    bench_returns = (benchmark_df['Close'] / benchmark_df['Close'].iloc[0] - 1) * 100
    ax_main.plot(bench_returns.index, bench_returns, color='black', linewidth=3, label='S&P 500 (SPY)', zorder=10)

    for ticker in tickers:
        df = _flatten_columns(all_stock_data[ticker].copy())
        if 'Close' not in df.columns or df['Close'].isna().all():
            continue
        stock_returns = (df['Close'] / df['Close'].iloc[0] - 1) * 100
        ax_main.plot(stock_returns.index, stock_returns, label=f'{ticker}', alpha=0.8, linewidth=1.5)

    ax_main.set_title(f"Watchlist Performance Comparison ({num_stocks} Stocks)", fontsize=18, fontweight='bold')
    ax_main.set_ylabel("Return %")
    ax_main.legend(loc='upper left', ncol=min(num_stocks, 5), fontsize=10)
    ax_main.grid(True, alpha=0.3)

    # --- INDIVIDUAL ANALYSIS BLOCKS ---
    for i, ticker in enumerate(tickers):
        df = _flatten_columns(all_stock_data[ticker].copy())
        if 'Close' not in df.columns:
            continue

        row_idx = i + 1

        # COLUMN A: Price and SMAs
        ax_price = fig.add_subplot(gs[row_idx, 0])
        for col, color, style, label in [
            ('Close', 'black', '-', 'Price'),
            ('SMA20', 'cyan', '--', '20 SMA'),
            ('SMA50', 'orange', '-', '50 SMA'),
            ('SMA200', 'red', '-', '200 SMA')
        ]:
            if col in df.columns:
                ax_price.plot(df.index, df[col], color=color, linestyle=style, alpha=0.8, label=label, linewidth=1.2)

        ax_price.set_title(f"{ticker}: Price Trend", fontsize=12, fontweight='bold')
        ax_price.legend(fontsize=8, loc='upper left', ncol=2)
        ax_price.grid(True, alpha=0.1)
        ax_price.tick_params(axis='x', rotation=15, labelsize=8)

        # COLUMN B: Relative Strength
        ax_rs = fig.add_subplot(gs[row_idx, 1])
        if 'RS_Line' in df.columns:
            ax_rs.plot(df.index, df['RS_Line'], color='purple', label='RS Line', linewidth=1.2)
            if 'RS_SMA20' in df.columns:
                ax_rs.plot(df.index, df['RS_SMA20'], color='gray', linestyle='--', alpha=0.6)
                ax_rs.fill_between(df.index, df['RS_Line'], df['RS_SMA20'],
                                   where=(df['RS_Line'] >= df['RS_SMA20']), color='green', alpha=0.1)

            ax_rs.set_title(f"{ticker}: RS vs SPY", fontsize=12, fontweight='bold')
            ax_rs.grid(True, alpha=0.1)
            ax_rs.tick_params(axis='x', rotation=15, labelsize=8)

    plt.tight_layout()
    save_path = os.path.join('plots', 'executive_dashboard.png')
    plt.savefig(save_path, dpi=150)
    print(f"Full Executive dashboard saved to {save_path}")
    plt.close()
