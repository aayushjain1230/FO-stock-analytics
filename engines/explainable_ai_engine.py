"""Human-readable explanations for model, score, and strategy outputs."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Mapping


def explain_prediction(
    prediction_name: str,
    prediction_value: Any,
    feature_contributions: Mapping[str, float],
    confidence: float,
    assumptions: Iterable[str],
) -> Dict[str, Any]:
    """Create an explanation payload for a prediction or score."""
    ordered = sorted(feature_contributions.items(), key=lambda item: abs(item[1]), reverse=True)
    positives = [f"{name} added {value:.2f}" for name, value in ordered if value > 0][:5]
    negatives = [f"{name} subtracted {abs(value):.2f}" for name, value in ordered if value < 0][:5]
    return {
        "prediction_name": prediction_name,
        "prediction_value": prediction_value,
        "confidence": confidence,
        "top_positive_drivers": positives,
        "top_negative_drivers": negatives,
        "plain_english": (
            f"{prediction_name} is {prediction_value}. Confidence is {confidence:.0%}. "
            "Review the strongest positive and negative drivers before acting."
        ),
        "assumptions": list(assumptions),
        "what_could_invalidate": [
            "Feature relationships break down.",
            "New data contradicts the leading drivers.",
            "Model confidence drops or feature disagreement increases.",
        ],
    }


def counterfactual_explanation(target_outcome: str, required_changes: Mapping[str, Any]) -> Dict[str, Any]:
    """Explain what would need to change for a different outcome."""
    return {
        "target_outcome": target_outcome,
        "required_changes": dict(required_changes),
        "plain_english": "These are the smallest listed changes that would make the model thesis more plausible.",
    }
