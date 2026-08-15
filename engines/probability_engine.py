"""Probability, Bayesian updating, confidence, and uncertainty engine."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Mapping


ASSUMPTIONS = [
    "Input probabilities are calibrated estimates.",
    "Historical volatility and win/loss behavior remain relevant.",
    "No unmodeled discontinuity dominates the event being estimated.",
]


@dataclass(frozen=True)
class ProbabilityResult:
    """Standard probability result contract."""

    name: str
    formula: str
    inputs: Dict[str, Any]
    result: float
    explanation: str
    assumptions: list[str]
    weaknesses: list[str]
    what_would_change_result: list[str]

    def as_dict(self) -> Dict[str, Any]:
        """Return a JSON-friendly probability result."""
        return {
            "name": self.name,
            "formula_used": self.formula,
            "inputs": self.inputs,
            "result": round(float(self.result), 6),
            "plain_english_explanation": self.explanation,
            "assumptions": self.assumptions,
            "weaknesses": self.weaknesses,
            "what_would_change_the_result": self.what_would_change_result,
        }


def _validate_probability(value: float) -> float:
    value = float(value)
    if value < 0 or value > 1:
        raise ValueError("Probability must be between 0 and 1.")
    return value


def _result(name: str, formula: str, inputs: Dict[str, Any], result: float, explanation: str) -> Dict[str, Any]:
    return ProbabilityResult(
        name=name,
        formula=formula,
        inputs=inputs,
        result=result,
        explanation=explanation,
        assumptions=list(ASSUMPTIONS),
        weaknesses=[
            "Probability estimates can be poorly calibrated.",
            "Fat tails and regime shifts may make normal/binomial assumptions too optimistic.",
        ],
        what_would_change_result=[
            "New evidence changing base rates.",
            "Updated volatility or drawdown assumptions.",
            "A larger or cleaner historical sample.",
        ],
    ).as_dict()


def event_probability(successes: int, trials: int) -> Dict[str, Any]:
    """Estimate event probability from observed frequency."""
    if trials <= 0:
        raise ValueError("trials must be positive.")
    probability = successes / trials
    return _result(
        "event_probability",
        "P(event) = successes / trials",
        {"successes": successes, "trials": trials},
        probability,
        f"The event occurred {successes} times in {trials} trials, implying {probability:.2%}.",
    )


def conditional_probability(joint_probability: float, condition_probability: float) -> Dict[str, Any]:
    """Calculate P(A|B)."""
    if condition_probability <= 0:
        raise ValueError("condition_probability must be positive.")
    result = _validate_probability(joint_probability) / _validate_probability(condition_probability)
    return _result(
        "conditional_probability",
        "P(A|B) = P(A ∩ B) / P(B)",
        {"joint_probability": joint_probability, "condition_probability": condition_probability},
        min(1.0, result),
        "This estimates how likely A is once condition B is known.",
    )


def bayes_update(prior: float, likelihood_evidence_given_true: float, likelihood_evidence_given_false: float) -> Dict[str, Any]:
    """Update a prior probability using Bayes' theorem."""
    prior = _validate_probability(prior)
    lt = _validate_probability(likelihood_evidence_given_true)
    lf = _validate_probability(likelihood_evidence_given_false)
    denominator = lt * prior + lf * (1 - prior)
    if denominator == 0:
        raise ValueError("Bayes denominator is zero.")
    posterior = (lt * prior) / denominator
    return _result(
        "bayes_update",
        "P(H|E) = P(E|H)P(H) / [P(E|H)P(H)+P(E|¬H)P(¬H)]",
        {
            "prior": prior,
            "likelihood_evidence_given_true": lt,
            "likelihood_evidence_given_false": lf,
        },
        posterior,
        f"New evidence changes the probability from {prior:.2%} to {posterior:.2%}.",
    )


def binomial_probability(success_probability: float, successes: int, trials: int) -> Dict[str, Any]:
    """Calculate exact binomial probability."""
    p = _validate_probability(success_probability)
    if successes < 0 or trials < 0 or successes > trials:
        raise ValueError("successes must be between 0 and trials.")
    result = math.comb(trials, successes) * (p**successes) * ((1 - p) ** (trials - successes))
    return _result(
        "binomial_probability",
        "P(X=k)=C(n,k)p^k(1-p)^(n-k)",
        {"success_probability": p, "successes": successes, "trials": trials},
        result,
        f"Probability of exactly {successes} successes in {trials} trials is {result:.2%}.",
    )


def geometric_probability(success_probability: float, trial_number: int) -> Dict[str, Any]:
    """Calculate probability first success occurs on a trial."""
    p = _validate_probability(success_probability)
    if trial_number <= 0:
        raise ValueError("trial_number must be positive.")
    result = ((1 - p) ** (trial_number - 1)) * p
    return _result(
        "geometric_probability",
        "P(X=k)=(1-p)^(k-1)p",
        {"success_probability": p, "trial_number": trial_number},
        result,
        f"Probability of first success on trial {trial_number} is {result:.2%}.",
    )


def normal_distribution_probability(mean: float, standard_deviation: float, threshold: float, above: bool = True) -> Dict[str, Any]:
    """Estimate normal probability above or below a threshold."""
    if standard_deviation <= 0:
        raise ValueError("standard_deviation must be positive.")
    z = (threshold - mean) / standard_deviation
    cdf = 0.5 * (1 + math.erf(z / math.sqrt(2)))
    probability = 1 - cdf if above else cdf
    direction = "above" if above else "below"
    return _result(
        "normal_distribution_probability",
        "P(X threshold) from normal CDF using z=(threshold-mean)/std",
        {"mean": mean, "standard_deviation": standard_deviation, "threshold": threshold, "above": above},
        probability,
        f"Assuming normality, probability of ending {direction} {threshold} is {probability:.2%}.",
    )


def probability_of_profit(win_probability: float, breakeven_probability: float | None = None) -> Dict[str, Any]:
    """Return probability of profit with optional breakeven adjustment."""
    p = _validate_probability(win_probability)
    if breakeven_probability is not None:
        p = max(0.0, p - _validate_probability(breakeven_probability))
    return _result(
        "probability_of_profit",
        "P(profit) = P(win) - P(breakeven drag)",
        {"win_probability": win_probability, "breakeven_probability": breakeven_probability},
        p,
        f"Estimated probability of profit is {p:.2%}.",
    )


def probability_of_drawdown(expected_return: float, volatility: float, drawdown_threshold: float, horizon_scale: float = 1.0) -> Dict[str, Any]:
    """Estimate probability of breaching a drawdown threshold with a normal approximation."""
    scaled_vol = volatility * math.sqrt(horizon_scale)
    return normal_distribution_probability(expected_return * horizon_scale, scaled_vol, -abs(drawdown_threshold), above=False) | {"name": "probability_of_drawdown"}


def probability_of_hitting_target(expected_return: float, volatility: float, target_return: float, horizon_scale: float = 1.0) -> Dict[str, Any]:
    """Estimate probability of hitting a target return."""
    scaled_vol = volatility * math.sqrt(horizon_scale)
    return normal_distribution_probability(expected_return * horizon_scale, scaled_vol, target_return, above=True) | {"name": "probability_of_hitting_target"}


def confidence_score(sample_size: int, data_quality: float, signal_stability: float) -> Dict[str, Any]:
    """Score confidence from sample size, data quality, and stability."""
    if sample_size < 0:
        raise ValueError("sample_size cannot be negative.")
    size_score = min(sample_size / 100, 1.0)
    dq = _validate_probability(data_quality)
    stability = _validate_probability(signal_stability)
    score = 0.4 * size_score + 0.35 * dq + 0.25 * stability
    return _result(
        "confidence_score",
        "confidence = 40% sample_size_score + 35% data_quality + 25% signal_stability",
        {"sample_size": sample_size, "data_quality": dq, "signal_stability": stability},
        score,
        f"Confidence is {score:.2%}; low samples or unstable signals should be treated cautiously.",
    )


def uncertainty_score(missing_data_pct: float, volatility_pct: float, model_disagreement_pct: float) -> Dict[str, Any]:
    """Score uncertainty from missing data, volatility, and model disagreement."""
    score = min(1.0, max(0.0, 0.35 * missing_data_pct + 0.35 * volatility_pct + 0.30 * model_disagreement_pct))
    return _result(
        "uncertainty_score",
        "uncertainty = 35% missing_data + 35% volatility + 30% model_disagreement",
        {
            "missing_data_pct": missing_data_pct,
            "volatility_pct": volatility_pct,
            "model_disagreement_pct": model_disagreement_pct,
        },
        score,
        f"Uncertainty is {score:.2%}; higher uncertainty means smaller position sizes or more validation are needed.",
    )


def probability_table_for_asset(expected_return: float, volatility: float, targets: Iterable[float], horizons: Mapping[str, float]) -> Dict[str, Any]:
    """Build target-hit and drawdown probabilities across horizons."""
    rows = []
    for horizon_name, scale in horizons.items():
        for target in targets:
            rows.append(
                {
                    "horizon": horizon_name,
                    "target": target,
                    "probability_above_target": probability_of_hitting_target(expected_return, volatility, target, scale)["result"],
                    "probability_below_negative_target": probability_of_drawdown(expected_return, volatility, target, scale)["result"],
                }
            )
    return _result(
        "probability_table_for_asset",
        "Table = normal target probabilities across horizons",
        {"expected_return": expected_return, "volatility": volatility, "targets": list(targets), "horizons": dict(horizons)},
        rows[0]["probability_above_target"] if rows else 0.0,
        "Probability table created for stock or strategy target scenarios.",
    ) | {"rows": rows}
