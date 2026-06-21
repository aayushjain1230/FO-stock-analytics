"""Advanced derivatives research: volatility surfaces, local vol, and Heston."""

import math
from typing import Dict

import numpy as np
import pandas as pd
from scipy.stats import norm


def black_scholes_price(stock_price: float, strike: float, time_to_expiry: float, risk_free_rate: float, volatility: float, option_type: str = "call") -> float:
    t = max(float(time_to_expiry), 1e-9)
    sigma = max(float(volatility), 1e-9)
    d1 = (math.log(stock_price / strike) + (risk_free_rate + 0.5 * sigma**2) * t) / (sigma * math.sqrt(t))
    d2 = d1 - sigma * math.sqrt(t)
    if option_type.lower() == "put":
        return float(strike * math.exp(-risk_free_rate * t) * norm.cdf(-d2) - stock_price * norm.cdf(-d1))
    return float(stock_price * norm.cdf(d1) - strike * math.exp(-risk_free_rate * t) * norm.cdf(d2))


def build_volatility_surface(options_frame: pd.DataFrame) -> Dict:
    required = {"expiration", "strike", "option_type", "implied_volatility"}
    if options_frame is None or options_frame.empty or not required.issubset(options_frame.columns):
        return {"available": False, "message": "Need expiration, strike, option_type, and implied_volatility columns."}
    frame = options_frame.copy()
    frame["expiration"] = frame["expiration"].astype(str)
    frame["strike"] = pd.to_numeric(frame["strike"], errors="coerce")
    frame["implied_volatility"] = pd.to_numeric(frame["implied_volatility"], errors="coerce")
    frame = frame.dropna(subset=["strike", "implied_volatility"])
    surface = {}
    for expiry, group in frame.groupby("expiration"):
        ordered = group.sort_values("strike")
        surface[expiry] = {
            "min_iv": float(ordered["implied_volatility"].min()),
            "max_iv": float(ordered["implied_volatility"].max()),
            "mean_iv": float(ordered["implied_volatility"].mean()),
            "skew": float(ordered["implied_volatility"].iloc[0] - ordered["implied_volatility"].iloc[-1]) if len(ordered) > 1 else 0.0,
            "points": ordered[["strike", "option_type", "implied_volatility"]].to_dict("records"),
        }
    return {"available": True, "surface": surface, "interpretation": "Surface summarizes IV by strike and expiration; skew shows put/call downside demand."}


def dupire_local_volatility(surface_grid: pd.DataFrame, strike: float, maturity: float) -> Dict:
    required = {"strike", "maturity", "call_price"}
    if surface_grid is None or surface_grid.empty or not required.issubset(surface_grid.columns):
        return {"available": False, "message": "Need strike, maturity, and call_price grid."}
    grid = surface_grid.copy().sort_values(["maturity", "strike"])
    nearest_t = grid.iloc[(grid["maturity"] - maturity).abs().argsort()[:1]]["maturity"].iloc[0]
    slice_t = grid[grid["maturity"] == nearest_t].sort_values("strike")
    if len(slice_t) < 3:
        return {"available": False, "message": "Need at least 3 strikes at nearest maturity for curvature."}
    prices = slice_t["call_price"].values
    strikes = slice_t["strike"].values
    idx = int(np.argmin(np.abs(strikes - strike)))
    idx = min(max(idx, 1), len(strikes) - 2)
    dk1 = strikes[idx] - strikes[idx - 1]
    dk2 = strikes[idx + 1] - strikes[idx]
    second_k = 2 * ((prices[idx + 1] - prices[idx]) / dk2 - (prices[idx] - prices[idx - 1]) / dk1) / (dk1 + dk2)
    local_var = max(0.0, 2 * prices[idx] / max(strikes[idx] ** 2 * second_k, 1e-9))
    return {"available": True, "local_volatility": float(math.sqrt(local_var)), "nearest_maturity": float(nearest_t), "nearest_strike": float(strikes[idx])}


def heston_paths(stock_price: float, days: int, risk_free_rate: float, v0: float, kappa: float, theta: float, vol_of_vol: float, rho: float, simulations: int = 10000, seed: int = 42) -> Dict:
    rng = np.random.default_rng(seed)
    dt = 1 / 252
    prices = np.full(simulations, float(stock_price))
    variances = np.full(simulations, max(v0, 1e-9))
    for _ in range(days):
        z1 = rng.standard_normal(simulations)
        z2 = rho * z1 + math.sqrt(max(1 - rho**2, 0)) * rng.standard_normal(simulations)
        variances = np.maximum(variances + kappa * (theta - variances) * dt + vol_of_vol * np.sqrt(np.maximum(variances, 0) * dt) * z2, 1e-9)
        prices *= np.exp((risk_free_rate - 0.5 * variances) * dt + np.sqrt(variances * dt) * z1)
    return {
        "terminal_prices": prices,
        "terminal_variances": variances,
        "expected_terminal_price": float(prices.mean()),
        "p05": float(np.percentile(prices, 5)),
        "p50": float(np.percentile(prices, 50)),
        "p95": float(np.percentile(prices, 95)),
    }


def heston_option_price_mc(stock_price: float, strike: float, days: int, risk_free_rate: float, v0: float, kappa: float, theta: float, vol_of_vol: float, rho: float, option_type: str = "call", simulations: int = 10000) -> Dict:
    paths = heston_paths(stock_price, days, risk_free_rate, v0, kappa, theta, vol_of_vol, rho, simulations=simulations)
    terminal = paths["terminal_prices"]
    payoff = np.maximum(terminal - strike, 0) if option_type.lower() == "call" else np.maximum(strike - terminal, 0)
    price = math.exp(-risk_free_rate * days / 365) * payoff.mean()
    return {
        "model": "Heston stochastic volatility Monte Carlo",
        "heston_price": float(price),
        "probability_itm": float((payoff > 0).mean()),
        "terminal_distribution": {k: v for k, v in paths.items() if k not in {"terminal_prices", "terminal_variances"}},
        "parameters": {"v0": v0, "kappa": kappa, "theta": theta, "vol_of_vol": vol_of_vol, "rho": rho},
    }
