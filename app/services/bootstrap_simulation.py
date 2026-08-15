"""Historical-bootstrap scenario engine."""

from __future__ import annotations

from datetime import datetime

import numpy as np
import pandas as pd

from app.models.simulation import SimulationConfig, SimulationResult
from app.models.stock import StockSnapshot


def run_historical_bootstrap(snapshot: StockSnapshot, history: pd.DataFrame | None, config: SimulationConfig | None = None, market_condition: str | None = None) -> SimulationResult:
    """Run historical-return scenarios and describe them as model scenarios, not guarantees."""
    config = config or SimulationConfig()
    if history is None or history.empty or "Close" not in history or snapshot.price is None:
        return _insufficient(snapshot.ticker, config, snapshot.price, "Price history is unavailable.")
    close = pd.to_numeric(history["Close"], errors="coerce").dropna().tail(config.lookback_days + 1)
    returns = close.pct_change().dropna()
    if len(returns) < config.minimum_history:
        return _insufficient(snapshot.ticker, config, snapshot.price, f"Only {len(returns)} valid returns were available; {config.minimum_history} are required.")
    rng = np.random.default_rng(config.random_seed)
    sampled = _sample_paths(returns.to_numpy(), config, rng, market_condition)
    terminal_returns = np.prod(1 + sampled, axis=1) - 1
    paths = snapshot.price * np.cumprod(1 + sampled, axis=1)
    ending = paths[:, -1]
    drawdowns = _max_drawdowns(paths)
    pct_range = {str(p): float(np.percentile(ending, p)) for p in [5, 10, 25, 50, 75, 90, 95]}
    dd_range = {str(p): float(np.percentile(drawdowns, p)) for p in [50, 80, 90, 95]}
    higher = float((ending > snapshot.price).mean() * 100)
    down10 = float((terminal_returns <= -0.10).mean() * 100)
    event_warning = _event_warning(snapshot)
    confidence = "Medium" if len(returns) >= 180 and not event_warning else "Low"
    risk = "High" if down10 >= 25 or event_warning else "Moderate" if down10 >= 10 else "Lower"
    outlook = "Moderately Positive" if higher >= 58 else "Cautious" if higher <= 45 else "Mixed"
    limitations = ["Historical-return scenarios assume the sampled past remains relevant."]
    if event_warning:
        limitations.append(event_warning)
    explanation = (
        f"Across {config.simulations:,} historical-return scenarios, {higher:.0f}% finished above today's price. "
        "This is a conditional model result, not a guaranteed real-world probability."
    )
    return SimulationResult(
        ticker=snapshot.ticker,
        method=config.method,
        horizon_days=config.horizon_days,
        simulations=config.simulations,
        current_price=snapshot.price,
        median_ending_price=float(np.median(ending)),
        percentile_range=pct_range,
        scenarios_ending_higher_pct=higher,
        falling_more_than_5_pct=float((terminal_returns <= -0.05).mean() * 100),
        falling_more_than_10_pct=down10,
        gaining_more_than_5_pct=float((terminal_returns >= 0.05).mean() * 100),
        gaining_more_than_10_pct=float((terminal_returns >= 0.10).mean() * 100),
        max_drawdown_percentiles=dd_range,
        model_disagreement=_model_disagreement(returns, config),
        data_coverage="Good" if len(returns) >= config.lookback_days * 0.8 else "Partial",
        model_outlook=outlook,
        risk_level=risk,
        confidence=confidence,
        event_warning=event_warning,
        explanation=explanation,
        limitations=limitations,
        model_version=config.model_version,
    )


def _sample_paths(returns: np.ndarray, config: SimulationConfig, rng: np.random.Generator, market_condition: str | None) -> np.ndarray:
    if config.method == "block":
        return _block_bootstrap(returns, config, rng)
    if config.method == "regime_conditioned":
        conditioned = _regime_filter(returns, market_condition)
        return rng.choice(conditioned, size=(config.simulations, config.horizon_days), replace=True)
    return rng.choice(returns, size=(config.simulations, config.horizon_days), replace=True)


def _block_bootstrap(returns: np.ndarray, config: SimulationConfig, rng: np.random.Generator) -> np.ndarray:
    paths = np.empty((config.simulations, config.horizon_days))
    starts = np.arange(max(len(returns) - config.block_length + 1, 1))
    for row in range(config.simulations):
        values: list[float] = []
        while len(values) < config.horizon_days:
            start = int(rng.choice(starts))
            values.extend(returns[start : start + config.block_length].tolist())
        paths[row] = values[: config.horizon_days]
    return paths


def _regime_filter(returns: np.ndarray, market_condition: str | None) -> np.ndarray:
    if market_condition == "Weak":
        filtered = returns[returns <= np.nanmedian(returns)]
    elif market_condition == "Supportive":
        filtered = returns[returns >= np.nanmedian(returns)]
    else:
        filtered = returns
    return filtered if len(filtered) >= 20 else returns


def _max_drawdowns(paths: np.ndarray) -> np.ndarray:
    running_max = np.maximum.accumulate(paths, axis=1)
    drawdowns = paths / running_max - 1
    return drawdowns.min(axis=1) * 100


def _model_disagreement(returns: pd.Series, config: SimulationConfig) -> str:
    if len(returns) < 120:
        return "High"
    recent = returns.tail(30).std()
    longer = returns.tail(min(len(returns), config.lookback_days)).std()
    if longer == 0 or pd.isna(longer):
        return "High"
    ratio = float(recent / longer)
    return "High" if ratio > 1.8 or ratio < 0.55 else "Medium" if ratio > 1.35 or ratio < 0.75 else "Low"


def _event_warning(snapshot: StockSnapshot) -> str | None:
    if not snapshot.next_earnings_date:
        return None
    try:
        days = (datetime.fromisoformat(str(snapshot.next_earnings_date)[:10]).date() - datetime.utcnow().date()).days
    except Exception:
        return "Earnings timing is unclear, so forecast confidence is lower."
    if -1 <= days <= 10:
        return f"Earnings are in {days} days. Normal historical simulations may underestimate the upcoming move."
    return None


def _insufficient(ticker: str, config: SimulationConfig, price: float | None, reason: str) -> SimulationResult:
    return SimulationResult(ticker, config.method, config.horizon_days, config.simulations, price, None, {}, None, None, None, None, None, {}, "Unknown", "Insufficient", "Insufficient Data", "Insufficient Data", "Insufficient Data", None, "The engine abstained because it did not have enough reliable history.", [reason], model_version=config.model_version)
