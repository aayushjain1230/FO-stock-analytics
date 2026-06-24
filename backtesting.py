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


def alpha_beta_attribution(strategy_returns: pd.Series, benchmark_returns: pd.Series) -> Dict:
    aligned = pd.concat(
        [strategy_returns.rename("strategy"), benchmark_returns.rename("benchmark")],
        axis=1,
        sort=False,
    ).dropna()
    if len(aligned) < 30:
        return {"available": False, "message": "Need at least 30 aligned return observations."}
    x = np.column_stack([np.ones(len(aligned)), aligned["benchmark"].values])
    alpha_daily, beta = np.linalg.lstsq(x, aligned["strategy"].values, rcond=None)[0]
    fitted = x @ np.array([alpha_daily, beta])
    residual = aligned["strategy"].values - fitted
    return {
        "available": True,
        "alpha_annualized": float(alpha_daily * TRADING_DAYS),
        "beta": float(beta),
        "residual_volatility": float(np.std(residual, ddof=1) * np.sqrt(TRADING_DAYS)),
        "benchmark_correlation": float(aligned.corr().iloc[0, 1]),
    }


def walk_forward_signal_backtest(
    price: pd.Series,
    signal_score: pd.Series,
    benchmark_price: pd.Series | None = None,
    train_window: int = 252,
    test_window: int = 63,
    entry_percentile: float = 0.75,
    transaction_cost_bps: float = 5,
    slippage_bps: float = 5,
) -> Dict:
    """
    Walk-forward, out-of-sample validation for a continuous long-only signal.

    Each test segment uses an entry threshold learned only from the preceding
    training window. Positions are shifted one day to avoid look-ahead bias.
    """
    frame = pd.concat(
        [
            pd.to_numeric(price, errors="coerce").rename("price"),
            pd.to_numeric(signal_score, errors="coerce").rename("signal"),
        ],
        axis=1,
    ).dropna()
    if len(frame) < train_window + test_window:
        return {"available": False, "message": "Insufficient history for walk-forward testing."}

    returns = frame["price"].pct_change().fillna(0)
    positions = pd.Series(0.0, index=frame.index)
    folds = []
    cursor = train_window
    while cursor < len(frame):
        test_end = min(cursor + test_window, len(frame))
        train = frame.iloc[cursor - train_window:cursor]
        threshold = float(train["signal"].quantile(entry_percentile))
        test = frame.iloc[cursor:test_end]
        positions.loc[test.index] = (test["signal"] >= threshold).astype(float)
        folds.append(
            {
                "train_start": str(train.index[0]),
                "train_end": str(train.index[-1]),
                "test_start": str(test.index[0]),
                "test_end": str(test.index[-1]),
                "entry_threshold": threshold,
            }
        )
        cursor = test_end

    gross = positions.shift(1).fillna(0) * returns
    turnover = positions.diff().abs().fillna(0)
    cost_rate = (transaction_cost_bps + slippage_bps) / 10000
    costs = turnover * cost_rate
    net = gross - costs
    out_of_sample = net.iloc[train_window:]
    attribution = {
        "gross_return": float((1 + gross.iloc[train_window:]).prod() - 1),
        "net_return": float((1 + out_of_sample).prod() - 1),
        "transaction_and_slippage_drag": float(costs.iloc[train_window:].sum()),
        "average_exposure": float(positions.iloc[train_window:].mean()),
        "turnover": float(turnover.iloc[train_window:].sum()),
    }

    benchmark_attribution = {}
    if benchmark_price is not None:
        benchmark_returns = pd.to_numeric(benchmark_price, errors="coerce").pct_change()
        benchmark_attribution = alpha_beta_attribution(out_of_sample, benchmark_returns)

    return {
        "available": True,
        "method": "rolling walk-forward out-of-sample",
        "folds": folds,
        "train_window": train_window,
        "test_window": test_window,
        "entry_percentile": entry_percentile,
        "transaction_cost_bps": transaction_cost_bps,
        "slippage_bps": slippage_bps,
        "performance": performance_report(out_of_sample),
        "performance_attribution": attribution,
        "alpha_beta": benchmark_attribution,
        "robustness": robustness_diagnostics(out_of_sample, folds),
        "equity_curve": {
            str(date): float(value)
            for date, value in (1 + out_of_sample).cumprod().round(6).items()
        },
    }


def robustness_diagnostics(returns: pd.Series, folds: list) -> Dict:
    clean = returns.dropna()
    if clean.empty:
        return {"sample_size": 0, "warning": "No out-of-sample returns."}
    fold_returns = []
    for fold in folds:
        segment = clean.loc[fold["test_start"]:fold["test_end"]]
        if not segment.empty:
            fold_returns.append(float((1 + segment).prod() - 1))
    positive_folds = sum(value > 0 for value in fold_returns)
    return {
        "sample_size": int(len(clean)),
        "fold_count": len(fold_returns),
        "positive_fold_pct": positive_folds / len(fold_returns) if fold_returns else 0.0,
        "worst_fold_return": min(fold_returns) if fold_returns else None,
        "best_fold_return": max(fold_returns) if fold_returns else None,
        "return_stability": float(np.std(fold_returns, ddof=1)) if len(fold_returns) > 1 else None,
        "warning": "A strategy is not robust if results depend on one fold or disappear after realistic costs.",
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
        "equity_curve": {
            str(date): float(value)
            for date, value in (1 + strategy_returns.fillna(0)).cumprod().round(6).items()
        },
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
