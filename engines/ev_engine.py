"""Expected-value calculations for trades, strategies, options, and portfolios.

Every public function returns an explainable dictionary instead of a naked
number. That contract keeps dashboard pages useful to a human researcher:
formula, inputs, result, assumptions, weaknesses, and what could change the
answer are always present.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Mapping, Sequence


DEFAULT_ASSUMPTIONS = [
    "Probabilities are estimates, not facts.",
    "Payoffs are measured net of explicitly supplied costs only.",
    "Market regime and liquidity conditions do not materially change before exit.",
]


@dataclass(frozen=True)
class EVResult:
    """Standard expected-value result contract."""

    name: str
    formula: str
    inputs: Dict[str, Any]
    result: float
    explanation: str
    assumptions: Sequence[str]
    weaknesses: Sequence[str]
    what_would_change_result: Sequence[str]

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-friendly representation."""
        return {
            "name": self.name,
            "formula_used": self.formula,
            "inputs": self.inputs,
            "result": round(float(self.result), 6),
            "plain_english_explanation": self.explanation,
            "assumptions": list(self.assumptions),
            "weaknesses": list(self.weaknesses),
            "what_would_change_the_result": list(self.what_would_change_result),
        }


def _validate_probability(probability: float) -> float:
    if probability < 0 or probability > 1:
        raise ValueError("Probability must be between 0 and 1.")
    return float(probability)


def _base_result(name: str, formula: str, inputs: Dict[str, Any], result: float, explanation: str) -> Dict[str, Any]:
    return EVResult(
        name=name,
        formula=formula,
        inputs=inputs,
        result=result,
        explanation=explanation,
        assumptions=DEFAULT_ASSUMPTIONS,
        weaknesses=[
            "EV is highly sensitive to probability estimates.",
            "Tail events may be underrepresented if scenarios are incomplete.",
        ],
        what_would_change_result=[
            "Updated probability estimates.",
            "Different transaction costs, slippage, or position size.",
            "A market regime shift that changes payoff distribution.",
        ],
    ).as_dict()


def simple_expected_value(win_probability: float, win_payoff: float, loss_payoff: float) -> Dict[str, Any]:
    """Calculate EV for a binary outcome."""
    p = _validate_probability(win_probability)
    result = p * win_payoff + (1 - p) * loss_payoff
    return _base_result(
        "simple_expected_value",
        "EV = P(win) × win_payoff + P(loss) × loss_payoff",
        {"win_probability": p, "win_payoff": win_payoff, "loss_payoff": loss_payoff},
        result,
        f"Each unit risked is expected to return {result:.4f} before any omitted costs.",
    )


def weighted_expected_value(outcomes: Iterable[Mapping[str, float]]) -> Dict[str, Any]:
    """Calculate EV from a list of probability/payoff outcomes."""
    clean = [{"probability": _validate_probability(float(o["probability"])), "payoff": float(o["payoff"])} for o in outcomes]
    probability_sum = sum(o["probability"] for o in clean)
    if abs(probability_sum - 1) > 1e-6:
        raise ValueError("Outcome probabilities must sum to 1.")
    result = sum(o["probability"] * o["payoff"] for o in clean)
    return _base_result(
        "weighted_expected_value",
        "EV = Σ(probability_i × payoff_i)",
        {"outcomes": clean},
        result,
        f"The probability-weighted payoff across {len(clean)} scenarios is {result:.4f}.",
    )


def trade_expected_value(win_probability: float, average_win: float, average_loss: float, cost: float = 0.0) -> Dict[str, Any]:
    """Calculate trade EV after explicit per-trade cost."""
    base = simple_expected_value(win_probability, average_win, -abs(average_loss))
    result = float(base["result"]) - cost
    return _base_result(
        "trade_expected_value",
        "EV = P(win) × avg_win - P(loss) × avg_loss - cost",
        {"win_probability": win_probability, "average_win": average_win, "average_loss": average_loss, "cost": cost},
        result,
        f"The trade has expected value {result:.4f} after explicit costs.",
    )


def strategy_expected_value(trades: Iterable[Mapping[str, float]], cost_per_trade: float = 0.0) -> Dict[str, Any]:
    """Estimate strategy EV from realized or simulated trade returns."""
    values = [float(t.get("return", t.get("pnl", 0.0))) - cost_per_trade for t in trades]
    if not values:
        raise ValueError("At least one trade is required.")
    wins = [v for v in values if v > 0]
    result = sum(values) / len(values)
    return _base_result(
        "strategy_expected_value",
        "Strategy EV = mean(trade_return - cost_per_trade)",
        {"trade_count": len(values), "win_rate": len(wins) / len(values), "cost_per_trade": cost_per_trade},
        result,
        f"Average net trade EV is {result:.4f} across {len(values)} trades.",
    )


def payoff_matrix_expected_value(payoff_matrix: Mapping[str, Mapping[str, float]]) -> Dict[str, Any]:
    """Calculate EV from a named payoff matrix."""
    outcomes = [
        {"name": name, "probability": float(payload["probability"]), "payoff": float(payload["payoff"])}
        for name, payload in payoff_matrix.items()
    ]
    ev = weighted_expected_value(outcomes)
    ev["name"] = "payoff_matrix_expected_value"
    ev["inputs"] = {"payoff_matrix": payoff_matrix}
    return ev


def portfolio_expected_value(weights: Mapping[str, float], expected_returns: Mapping[str, float]) -> Dict[str, Any]:
    """Calculate weighted portfolio expected return."""
    missing = sorted(set(weights) - set(expected_returns))
    if missing:
        raise ValueError(f"Missing expected returns for: {', '.join(missing)}")
    result = sum(float(weights[ticker]) * float(expected_returns[ticker]) for ticker in weights)
    return _base_result(
        "portfolio_expected_value",
        "Portfolio EV = Σ(weight_i × expected_return_i)",
        {"weights": dict(weights), "expected_returns": dict(expected_returns)},
        result,
        f"Portfolio expected return is {result:.4f} for the supplied assumptions.",
    )


def option_payoff_expected_value(probabilities: Mapping[str, float], payoffs: Mapping[str, float], premium: float = 0.0) -> Dict[str, Any]:
    """Calculate option strategy EV from state probabilities and payoffs."""
    outcomes = []
    for state, probability in probabilities.items():
        outcomes.append({"probability": probability, "payoff": float(payoffs.get(state, 0.0)) - premium})
    result = weighted_expected_value(outcomes)
    result["name"] = "option_payoff_expected_value"
    result["formula_used"] = "Option EV = Σ(P(state) × (state_payoff - premium))"
    result["inputs"] = {"probabilities": dict(probabilities), "payoffs": dict(payoffs), "premium": premium}
    return result


def scenario_based_ev(scenarios: Mapping[str, Mapping[str, float]]) -> Dict[str, Any]:
    """Calculate EV from named market scenarios."""
    return payoff_matrix_expected_value(scenarios)


def ev_after_transaction_costs(expected_value: float, transaction_cost: float) -> Dict[str, Any]:
    """Subtract explicit transaction costs from EV."""
    return _base_result(
        "ev_after_transaction_costs",
        "Net EV = gross_EV - transaction_cost",
        {"expected_value": expected_value, "transaction_cost": transaction_cost},
        float(expected_value) - float(transaction_cost),
        "Transaction costs directly reduce expected value.",
    )


def ev_after_slippage(expected_value: float, slippage: float) -> Dict[str, Any]:
    """Subtract slippage estimate from EV."""
    return _base_result(
        "ev_after_slippage",
        "Net EV = gross_EV - slippage",
        {"expected_value": expected_value, "slippage": slippage},
        float(expected_value) - float(slippage),
        "Slippage reduces realized edge, especially in less liquid names.",
    )


def ev_by_market_regime(regime_probabilities: Mapping[str, float], regime_expected_values: Mapping[str, float]) -> Dict[str, Any]:
    """Calculate EV weighted by market-regime probabilities."""
    outcomes = [
        {"probability": probability, "payoff": regime_expected_values.get(regime, 0.0)}
        for regime, probability in regime_probabilities.items()
    ]
    result = weighted_expected_value(outcomes)
    result["name"] = "ev_by_market_regime"
    result["formula_used"] = "Regime EV = Σ(P(regime) × EV_given_regime)"
    result["inputs"] = {
        "regime_probabilities": dict(regime_probabilities),
        "regime_expected_values": dict(regime_expected_values),
    }
    return result
