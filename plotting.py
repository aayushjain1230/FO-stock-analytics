import matplotlib
matplotlib.use("Agg")  # Required for GitHub Actions / headless servers

import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
import os
import math


# ------------------------------------------------------------------
# Ensure plots directory exists
# ------------------------------------------------------------------
PLOTS_DIR = "plots"
os.makedirs(PLOTS_DIR, exist_ok=True)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Flattens MultiIndex columns (common with yfinance batch downloads).
    """
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(-1)
    return df


def _get_price_series(df: pd.DataFrame) -> pd.Series:
    """
    Robustly resolves a price column.
    Prevents CI-only failures when 'Close' is missing or renamed.
    """
    df = _flatten_columns(df)

    for col in ("Close", "close", "Adj Close", "adj_close"):
        if col in df.columns:
            series = pd.to_numeric(df[col], errors="coerce")
            if not series.isna().all():
                return series

    raise KeyError(f"No valid price column found. Columns: {list(df.columns)}")


# ------------------------------------------------------------------
# Single-stock detailed chart
# ------------------------------------------------------------------
def create_chart(ticker: str, df: pd.DataFrame, benchmark_df: pd.DataFrame, score: int | None = None):
    """
    Detailed single-stock technical chart.
    """
    try:
        df = _flatten_columns(df.copy())
        benchmark_df = _flatten_columns(benchmark_df.copy())

        price = _get_price_series(df)
        bench_price = _get_price_series(benchmark_df)

    except Exception as e:
        print(f"[Chart Skip] {ticker}: {e}")
        return

    fig, (ax1, ax2) = plt.subplots(
        2, 1, figsize=(12, 10), sharex=True,
        gridspec_kw={"height_ratios": [3, 1]}
    )

    # -------------------------------
    # PRICE + SMAs
    # -------------------------------
    ax1.plot(price.index, price, color="black", linewidth=1.5, label="Price", zorder=5)

    for col, color, style, label in [
        ("SMA20", "cyan", "--", "20 SMA"),
        ("SMA50", "orange", "-", "50 SMA"),
        ("SMA200", "red", "-", "200 SMA"),
    ]:
        if col in df.columns:
            ax1.plot(df.index, df[col], color=color, linestyle=style, alpha=0.8, label=label)

    # -------------------------------
    # Benchmark overlay (normalized)
    # -------------------------------
    if not bench_price.isna().all():
        bench_norm = (bench_price / bench_price.iloc[0]) * price.iloc[0]
        ax1.plot(
            bench_norm.index,
            bench_norm,
            color="gray",
            alpha=0.3,
            linewidth=1,
            label="Benchmark"
        )

    # -------------------------------
    # Score box
    # -------------------------------
    if score is not None:
        box_color = "green" if score >= 70 else "orange" if score >= 40 else "red"
        ax1.text(
            0.02, 0.95,
            f"JFO Score: {score}/100",
            transform=ax1.transAxes,
            fontsize=14,
            fontweight="bold",
            color="white",
            bbox=dict(facecolor=box_color, alpha=0.85, edgecolor="none")
        )

    ax1.set_title(f"{ticker} – Technical Analysis", fontsize=16, fontweight="bold")
    ax1.set_ylabel("Price")
    ax1.legend(loc="upper left", fontsize=9, ncol=2)
    ax1.grid(True, alpha=0.2)

    # -------------------------------
    # Relative Strength
    # -------------------------------
    if "RS_Line" in df.columns:
        ax2.plot(df.index, df["RS_Line"], color="purple", linewidth=1.2, label="RS Line")

        if "RS_SMA20" in df.columns:
            ax2.plot(df.index, df["RS_SMA20"], color="gray", linestyle="--", alpha=0.6)
            ax2.fill_between(
                df.index,
                df["RS_Line"],
                df["RS_SMA20"],
                where=(df["RS_Line"] >= df["RS_SMA20"]),
                color="green",
                alpha=0.1
            )

        ax2.set_ylabel("Relative Strength")
        ax2.legend(loc="upper left", fontsize=9)
        ax2.grid(True, alpha=0.2)

    plt.tight_layout()
    path = os.path.join(PLOTS_DIR, f"{ticker}_analysis.png")
    plt.savefig(path, dpi=150)
    plt.close()


# ------------------------------------------------------------------
# Executive comparison dashboard
# ------------------------------------------------------------------
def create_comparison_chart(all_stock_data: dict, benchmark_df: pd.DataFrame):
    """
    Executive-level dashboard comparing all watchlist stocks.
    """
    try:
        benchmark_df = _flatten_columns(benchmark_df.copy())
        bench_price = _get_price_series(benchmark_df)
    except Exception as e:
        print(f"[Dashboard Error] Benchmark invalid: {e}")
        _save_error_image("benchmark_error.png", "Benchmark data unavailable")
        return

    if not all_stock_data:
        _save_error_image("no_watchlist.png", "No watchlist data provided")
        return

    tickers = list(all_stock_data.keys())
    num_stocks = len(tickers)

    fig_height = 6 + (3.5 * num_stocks)
    fig = plt.figure(figsize=(15, fig_height))
    gs = fig.add_gridspec(num_stocks + 1, 2, height_ratios=[2] + [1.2] * num_stocks)

    # -------------------------------
    # TOP: Cumulative returns
    # -------------------------------
    ax_main = fig.add_subplot(gs[0, :])

    bench_returns = (bench_price / bench_price.iloc[0] - 1) * 100
    ax_main.plot(
        bench_returns.index,
        bench_returns,
        color="black",
        linewidth=3,
        label="Benchmark",
        zorder=10
    )

    for ticker in tickers:
        try:
            df = _flatten_columns(all_stock_data[ticker].copy())
            price = _get_price_series(df)
            returns = (price / price.iloc[0] - 1) * 100
            ax_main.plot(returns.index, returns, linewidth=1.4, alpha=0.8, label=ticker)
        except Exception:
            continue

    ax_main.set_title(f"Watchlist Performance Comparison ({num_stocks} Stocks)", fontsize=18, fontweight="bold")
    ax_main.set_ylabel("Return %")
    ax_main.legend(loc="upper left", ncol=min(5, num_stocks), fontsize=10)
    ax_main.grid(True, alpha=0.3)

    # -------------------------------
    # PER-STOCK BLOCKS
    # -------------------------------
    for i, ticker in enumerate(tickers):
        try:
            df = _flatten_columns(all_stock_data[ticker].copy())
            price = _get_price_series(df)
        except Exception:
            continue

        row = i + 1

        # PRICE PANEL
        ax_p = fig.add_subplot(gs[row, 0])
        ax_p.plot(price.index, price, color="black", linewidth=1.2, label="Price")

        for col, color in [("SMA20", "cyan"), ("SMA50", "orange"), ("SMA200", "red")]:
            if col in df.columns:
                ax_p.plot(df.index, df[col], color=color, linewidth=1, alpha=0.8)

        ax_p.set_title(f"{ticker} – Price Trend", fontsize=12, fontweight="bold")
        ax_p.grid(True, alpha=0.1)
        ax_p.tick_params(axis="x", rotation=15, labelsize=8)

        # RS PANEL
        ax_rs = fig.add_subplot(gs[row, 1])
        if "RS_Line" in df.columns:
            ax_rs.plot(df.index, df["RS_Line"], color="purple", linewidth=1.2)

            if "RS_SMA20" in df.columns:
                ax_rs.plot(df.index, df["RS_SMA20"], color="gray", linestyle="--", alpha=0.6)
                ax_rs.fill_between(
                    df.index,
                    df["RS_Line"],
                    df["RS_SMA20"],
                    where=(df["RS_Line"] >= df["RS_SMA20"]),
                    color="green",
                    alpha=0.1
                )

        ax_rs.set_title(f"{ticker} – Relative Strength", fontsize=12, fontweight="bold")
        ax_rs.grid(True, alpha=0.1)
        ax_rs.tick_params(axis="x", rotation=15, labelsize=8)

    plt.tight_layout()
    path = os.path.join(PLOTS_DIR, "executive_dashboard.png")
    plt.savefig(path, dpi=150)
    plt.close()
    print(f"Executive dashboard saved → {path}")


# ------------------------------------------------------------------
# Error image helper
# ------------------------------------------------------------------
def _save_error_image(filename: str, message: str):
    fig = plt.figure(figsize=(10, 5))
    plt.text(0.5, 0.5, message, ha="center", va="center", fontsize=14)
    plt.axis("off")
    plt.savefig(os.path.join(PLOTS_DIR, filename), dpi=150)
    plt.close()
