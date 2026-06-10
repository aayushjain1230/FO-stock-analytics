import math
from typing import Dict

import numpy as np
from scipy.optimize import brentq
from scipy.stats import norm


def _d1(stock_price: float, strike: float, time_to_expiry: float, risk_free_rate: float, volatility: float) -> float:
    return (math.log(stock_price / strike) + (risk_free_rate + 0.5 * volatility**2) * time_to_expiry) / (
        volatility * math.sqrt(time_to_expiry)
    )


def _d2(d1: float, volatility: float, time_to_expiry: float) -> float:
    return d1 - volatility * math.sqrt(time_to_expiry)


def black_scholes_price(
    stock_price: float,
    strike: float,
    days_to_expiry: float,
    risk_free_rate: float,
    volatility: float,
    option_type: str = "call",
) -> float:
    time_to_expiry = max(days_to_expiry / 365, 1e-9)
    volatility = max(volatility, 1e-9)
    d1 = _d1(stock_price, strike, time_to_expiry, risk_free_rate, volatility)
    d2 = _d2(d1, volatility, time_to_expiry)
    if option_type.lower() == "put":
        return strike * math.exp(-risk_free_rate * time_to_expiry) * norm.cdf(-d2) - stock_price * norm.cdf(-d1)
    return stock_price * norm.cdf(d1) - strike * math.exp(-risk_free_rate * time_to_expiry) * norm.cdf(d2)


def greeks(
    stock_price: float,
    strike: float,
    days_to_expiry: float,
    risk_free_rate: float,
    volatility: float,
    option_type: str = "call",
) -> Dict[str, float]:
    time_to_expiry = max(days_to_expiry / 365, 1e-9)
    volatility = max(volatility, 1e-9)
    d1 = _d1(stock_price, strike, time_to_expiry, risk_free_rate, volatility)
    d2 = _d2(d1, volatility, time_to_expiry)
    call = option_type.lower() == "call"
    delta = norm.cdf(d1) if call else norm.cdf(d1) - 1
    gamma = norm.pdf(d1) / (stock_price * volatility * math.sqrt(time_to_expiry))
    vega = stock_price * norm.pdf(d1) * math.sqrt(time_to_expiry) / 100
    if call:
        theta = (
            -(stock_price * norm.pdf(d1) * volatility) / (2 * math.sqrt(time_to_expiry))
            - risk_free_rate * strike * math.exp(-risk_free_rate * time_to_expiry) * norm.cdf(d2)
        ) / 365
        rho = strike * time_to_expiry * math.exp(-risk_free_rate * time_to_expiry) * norm.cdf(d2) / 100
    else:
        theta = (
            -(stock_price * norm.pdf(d1) * volatility) / (2 * math.sqrt(time_to_expiry))
            + risk_free_rate * strike * math.exp(-risk_free_rate * time_to_expiry) * norm.cdf(-d2)
        ) / 365
        rho = -strike * time_to_expiry * math.exp(-risk_free_rate * time_to_expiry) * norm.cdf(-d2) / 100
    return {
        "delta": round(float(delta), 4),
        "gamma": round(float(gamma), 4),
        "vega": round(float(vega), 4),
        "theta": round(float(theta), 4),
        "rho": round(float(rho), 4),
    }


def implied_volatility(
    market_price: float,
    stock_price: float,
    strike: float,
    days_to_expiry: float,
    risk_free_rate: float,
    option_type: str = "call",
) -> float:
    def objective(vol):
        return black_scholes_price(stock_price, strike, days_to_expiry, risk_free_rate, vol, option_type) - market_price

    try:
        return round(float(brentq(objective, 1e-6, 5.0, maxiter=200)), 6)
    except ValueError:
        return float("nan")


def break_even(strike: float, premium: float, option_type: str = "call") -> float:
    return strike + premium if option_type.lower() == "call" else strike - premium


def break_even_analysis(stock_price: float, strike: float, premium: float, option_type: str = "call") -> Dict[str, float]:
    be = break_even(strike, premium, option_type)
    required_move = (be / stock_price - 1) if option_type.lower() == "call" else (1 - be / stock_price)
    max_loss = premium
    return {
        "break_even": round(float(be), 4),
        "required_move_pct": round(float(required_move * 100), 2),
        "max_loss_per_share": round(float(max_loss), 4),
    }


def iv_rank_percentile(current_iv: float, historical_iv_values) -> Dict[str, float]:
    values = np.array([x for x in historical_iv_values if np.isfinite(x)], dtype=float)
    if values.size == 0:
        return {"iv_rank": 0.0, "iv_percentile": 0.0}
    iv_min = values.min()
    iv_max = values.max()
    iv_rank = 0.0 if iv_max == iv_min else (current_iv - iv_min) / (iv_max - iv_min)
    iv_percentile = (values <= current_iv).mean()
    return {"iv_rank": round(float(iv_rank * 100), 2), "iv_percentile": round(float(iv_percentile * 100), 2)}


def monte_carlo_option_price(
    stock_price: float,
    strike: float,
    days_to_expiry: float,
    risk_free_rate: float,
    volatility: float,
    option_type: str = "call",
    simulations: int = 10000,
    seed: int = 42,
) -> Dict[str, float]:
    rng = np.random.default_rng(seed)
    time_to_expiry = max(days_to_expiry / 365, 1e-9)
    terminal_prices = stock_price * np.exp(
        (risk_free_rate - 0.5 * volatility**2) * time_to_expiry
        + volatility * math.sqrt(time_to_expiry) * rng.standard_normal(simulations)
    )
    if option_type.lower() == "put":
        payoffs = np.maximum(strike - terminal_prices, 0)
    else:
        payoffs = np.maximum(terminal_prices - strike, 0)
    discounted = math.exp(-risk_free_rate * time_to_expiry) * payoffs
    probability_profit = float((payoffs > 0).mean())
    return {
        "monte_carlo_price": round(float(discounted.mean()), 4),
        "expected_stock_price": round(float(terminal_prices.mean()), 4),
        "probability_of_profit": round(probability_profit, 4),
        "p05": round(float(np.percentile(terminal_prices, 5)), 4),
        "p50": round(float(np.percentile(terminal_prices, 50)), 4),
        "p95": round(float(np.percentile(terminal_prices, 95)), 4),
    }


def option_lab_report(
    stock_price: float,
    strike: float,
    days_to_expiry: float,
    risk_free_rate: float,
    volatility: float,
    market_price: float | None = None,
    option_type: str = "call",
) -> Dict:
    fair_value = black_scholes_price(stock_price, strike, days_to_expiry, risk_free_rate, volatility, option_type)
    payload = {
        "fair_value": round(float(fair_value), 4),
        "greeks": greeks(stock_price, strike, days_to_expiry, risk_free_rate, volatility, option_type),
        "break_even": break_even_analysis(stock_price, strike, market_price or fair_value, option_type),
        "monte_carlo": monte_carlo_option_price(stock_price, strike, days_to_expiry, risk_free_rate, volatility, option_type),
    }
    if market_price is not None:
        iv = implied_volatility(market_price, stock_price, strike, days_to_expiry, risk_free_rate, option_type)
        payload["market_price"] = market_price
        payload["implied_volatility"] = iv
        payload["relative_value"] = "expensive" if market_price > fair_value else "cheap" if market_price < fair_value else "fair"
    return payload


def greek_explanations(greek_payload: Dict[str, float]) -> Dict[str, str]:
    return {
        "delta": f"Delta = {greek_payload.get('delta')} means the option changes by roughly that amount for a $1 stock move.",
        "gamma": f"Gamma = {greek_payload.get('gamma')} measures how quickly delta changes as the stock moves.",
        "vega": f"Vega = {greek_payload.get('vega')} estimates option price change for a 1 percentage point volatility move.",
        "theta": f"Theta = {greek_payload.get('theta')} estimates daily time decay, all else equal.",
        "rho": f"Rho = {greek_payload.get('rho')} estimates price sensitivity to a 1 percentage point rate move.",
    }
