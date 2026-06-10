from typing import Dict


def _pct(value):
    if value is None:
        return "N/A"
    return f"{value * 100:.2f}%"


def stock_research_note(ticker: str, latest, rating: Dict, quant_payload: Dict) -> str:
    risk = quant_payload.get("risk", {})
    momentum = quant_payload.get("momentum", {})
    volume = quant_payload.get("volume", {})
    trend = quant_payload.get("trend", {})
    quant_score = quant_payload.get("quant_score", {})

    close = latest.get("Close")
    sma50 = latest.get("SMA50") or trend.get("sma50")
    sma200 = latest.get("SMA200") or trend.get("sma200")
    trend_text = "above its 50-day and 200-day moving averages" if close and sma50 and sma200 and close > sma50 > sma200 else "not in a clean long-term uptrend"
    vol_text = quant_payload.get("volatility_regime", "Unknown").lower()
    momentum_text = "positive" if momentum.get("score", 0) >= 50 else "mixed or weak"
    volume_text = "with a notable volume spike" if volume.get("volume_spike") else "without unusual volume confirmation"

    return (
        f"{ticker} is {trend_text}. Momentum is {momentum_text} {volume_text}. "
        f"Annualized volatility is {_pct(risk.get('annualized_volatility'))}, max drawdown is {_pct(risk.get('maximum_drawdown'))}, "
        f"and the research score is {quant_score.get('score', 0)}/100 ({quant_score.get('label', 'N/A')}). "
        f"The current volatility regime is {vol_text}. This is an educational research summary, not financial advice."
    )


def option_research_note(ticker: str, option_payload: Dict) -> str:
    greeks = option_payload.get("greeks", {})
    breakeven = option_payload.get("break_even", {})
    relative_value = option_payload.get("relative_value", "fair")
    iv = option_payload.get("implied_volatility")
    iv_text = f" Implied volatility is {iv * 100:.2f}%." if isinstance(iv, float) and iv == iv else ""
    return (
        f"{ticker} option fair value is estimated at ${option_payload.get('fair_value', 0):.2f}; "
        f"the contract screens as {relative_value} versus the model price.{iv_text} "
        f"Delta is {greeks.get('delta')}, theta is {greeks.get('theta')}, and break-even is "
        f"${breakeven.get('break_even', 0):.2f} ({breakeven.get('required_move_pct', 0):.2f}% required move). "
        f"This describes model sensitivity and does not recommend a trade."
    )


def watchlist_summary(rows) -> str:
    if not rows:
        return "No watchlist rows were available for summary."
    leaders = sorted(rows, key=lambda row: row.get("quant_score", row.get("score", 0)), reverse=True)[:3]
    laggards = sorted(rows, key=lambda row: row.get("quant_score", row.get("score", 0)))[:3]
    leader_text = ", ".join(f"{row['ticker']} ({row.get('quant_score', row.get('score', 0))})" for row in leaders)
    laggard_text = ", ".join(f"{row['ticker']} ({row.get('quant_score', row.get('score', 0))})" for row in laggards)
    return f"Research leaders: {leader_text}. Weakest research profiles: {laggard_text}. Review risk, liquidity, and options data before forming any conclusion."
