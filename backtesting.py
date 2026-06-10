import math
from typing import Dict

import numpy as np
import pandas as pd

from quant_analytics import cagr, max_drawdown, sharpe_ratio, sortino_ratio


TRADING_DAYS = 252


def _price_frame(data: Dict[str, pd.DataFrame] | pd.DataFrame) -> pd.DataFrame:
    if isinstance(data, pd.DataFrame):
        return data.apply(pd.to_numeric, errors="coerce").dropna(how="all")
    prices = {}
    for ticker, df in data.items():
        if "Adj Close" in df.columns:
            prices[ticker] = pd.to_numeric(df["Adj Close"], errors="coerce")
        else:
            prices[ticker] = pd.to_numeric(df["Close"], errors="coerce")
    return pd.DataFrame(prices).dropna(how="all")


def performance_report(returns: pd.Series) -> Dict[str, float]:
    returns = returns.dropna()
    wins = returns[returns > 0]
    losses = returns[returns < 0]
    gross_profit = wins.sum()
    gross_loss = abs(losses.sum())
    return {
        "cagr": round(float(cagr(returns)), 4),
        "sharpe_ratio": round(float(sharpe_ratio(returns)), 4),
        "sortino_ratio": round(float(sortino_ratio(returns)), 4),
        "max_drawdown": round(float(max_drawdown(returns)), 4),
        "win_rate": round(float((returns > 0).mean()), 4) if len(returns) else 0.0,
        "profit_factor": round(float(gross_profit / gross_loss), 4) if gross_loss else 0.0,
    }


def momentum_backtest(
    data: Dict[str, pd.DataFrame] | pd.DataFrame,
    lookback_days: int = 126,
    top_n: int = 5,
    rebalance_frequency: str = "ME",
    transaction_cost_bps: float = 5,
    slippage_bps: float = 5,
) -> Dict:
    prices = _price_frame(data).ffill().dropna(axis=1, how="all")
    daily_returns = prices.pct_change()
    rebalance_dates = prices.resample(rebalance_frequency).last().index
    positions = pd.DataFrame(0.0, index=prices.index, columns=prices.columns)
    cost_per_turnover = (transaction_cost_bps + slippage_bps) / 10000

    for date in rebalance_dates:
        if date not in prices.index:
            date = prices.index[prices.index.get_indexer([date], method="ffill")[0]]
        current_idx = prices.index.get_loc(date)
        if current_idx < lookback_days:
            continue
        momentum = prices.iloc[current_idx] / prices.iloc[current_idx - lookback_days] - 1
        selected = momentum.dropna().sort_values(ascending=False).head(top_n).index
        if len(selected) == 0:
            continue
        positions.loc[date:, :] = 0.0
        positions.loc[date:, selected] = 1 / len(selected)

    shifted_positions = positions.shift(1).reindex(daily_returns.index).fillna(0)
    gross_returns = (shifted_positions * daily_returns).sum(axis=1)
    turnover = positions.diff().abs().sum(axis=1).reindex(gross_returns.index).fillna(0)
    strategy_returns = gross_returns - turnover * cost_per_turnover
    return {
        "lookback_days": lookback_days,
        "top_n": top_n,
        "transaction_cost_bps": transaction_cost_bps,
        "slippage_bps": slippage_bps,
        "performance": performance_report(strategy_returns),
        "equity_curve": (1 + strategy_returns.fillna(0)).cumprod().round(6).to_dict(),
        "latest_positions": positions.iloc[-1][positions.iloc[-1] > 0].round(4).to_dict() if not positions.empty else {},
    }


def run_momentum_research(data: Dict[str, pd.DataFrame] | pd.DataFrame, top_n: int = 5) -> Dict[str, Dict]:
    horizons = {"3_month": 63, "6_month": 126, "12_month": 252, "24_month": 504}
    return {name: momentum_backtest(data, lookback_days=days, top_n=top_n) for name, days in horizons.items()}


def volatility_target_returns(returns: pd.Series, target_volatility: float = 0.10, lookback_days: int = 21, max_leverage: float = 2.0) -> pd.Series:
    realized = returns.rolling(lookback_days).std() * math.sqrt(TRADING_DAYS)
    leverage = (target_volatility / realized).clip(upper=max_leverage).shift(1).fillna(0)
    return returns * leverage


def compare_volatility_targeting(returns: pd.Series, targets=(0.05, 0.10, 0.15, 0.20)) -> Dict:
    base = performance_report(returns)
    comparisons = {"standard": base}
    for target in targets:
        targeted = volatility_target_returns(returns, target_volatility=target)
        report = performance_report(targeted)
        report["sharpe_improvement"] = round(report["sharpe_ratio"] - base["sharpe_ratio"], 4)
        report["drawdown_reduction"] = round(abs(base["max_drawdown"]) - abs(report["max_drawdown"]), 4)
        comparisons[f"target_{int(target * 100)}pct"] = report
    return comparisons


def pairs_trading_backtest(series_a: pd.Series, series_b: pd.Series, entry_z: float = 2.0, exit_z: float = 0.5, window: int = 60) -> Dict:
    frame = pd.concat([series_a, series_b], axis=1).dropna()
    frame.columns = ["a", "b"]
    spread = np.log(frame["a"]) - np.log(frame["b"])
    zscore = (spread - spread.rolling(window).mean()) / spread.rolling(window).std()
    signal = pd.Series(0, index=frame.index, dtype=float)
    signal[zscore > entry_z] = -1
    signal[zscore < -entry_z] = 1
    signal[zscore.abs() < exit_z] = 0
    signal = signal.replace(0, np.nan).ffill().fillna(0)
    pair_returns = signal.shift(1) * (frame["a"].pct_change() - frame["b"].pct_change())
    return {
        "correlation": round(float(frame["a"].pct_change().corr(frame["b"].pct_change())), 4),
        "latest_spread": round(float(spread.iloc[-1]), 4),
        "latest_zscore": round(float(zscore.iloc[-1]), 4) if pd.notna(zscore.iloc[-1]) else 0.0,
        "latest_signal": "long_a_short_b" if signal.iloc[-1] > 0 else "short_a_long_b" if signal.iloc[-1] < 0 else "exit",
        "performance": performance_report(pair_returns),
    }
