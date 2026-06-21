"""Machine-learning research helpers for signal validation."""

from typing import Dict, Iterable, List

import numpy as np
import pandas as pd


def train_test_split_time(frame: pd.DataFrame, target_col: str, train_fraction: float = 0.70):
    clean = frame.dropna(subset=[target_col]).copy()
    split = max(1, min(len(clean) - 1, int(len(clean) * train_fraction))) if len(clean) > 1 else 0
    return clean.iloc[:split], clean.iloc[split:]


def linear_return_model(frame: pd.DataFrame, feature_cols: List[str], target_col: str = "future_return") -> Dict:
    clean = frame[feature_cols + [target_col]].apply(pd.to_numeric, errors="coerce").dropna()
    if len(clean) < max(20, len(feature_cols) + 5):
        return {"available": False, "message": "Need more labeled outcomes before training."}
    train, test = train_test_split_time(clean, target_col)
    x_train = np.column_stack([np.ones(len(train)), train[feature_cols].values])
    y_train = train[target_col].values
    coefficients = np.linalg.lstsq(x_train, y_train, rcond=None)[0]
    x_test = np.column_stack([np.ones(len(test)), test[feature_cols].values]) if len(test) else np.empty((0, len(feature_cols) + 1))
    predictions = x_test @ coefficients if len(test) else np.array([])
    test_mae = float(np.mean(np.abs(predictions - test[target_col].values))) if len(test) else None
    return {
        "available": True,
        "model": "linear regression",
        "features": feature_cols,
        "coefficients": {"intercept": float(coefficients[0]), **{col: float(coefficients[i + 1]) for i, col in enumerate(feature_cols)}},
        "train_size": int(len(train)),
        "test_size": int(len(test)),
        "test_mae": test_mae,
        "interpretation": "Weights are learned from historical outcomes; use out-of-sample error to check overfitting.",
    }


def logistic_probability_model(frame: pd.DataFrame, feature_cols: List[str], target_col: str = "outperformed", learning_rate: float = 0.05, iterations: int = 1000) -> Dict:
    clean = frame[feature_cols + [target_col]].apply(pd.to_numeric, errors="coerce").dropna()
    if len(clean) < max(30, len(feature_cols) + 10):
        return {"available": False, "message": "Need more labeled outcomes before logistic training."}
    train, test = train_test_split_time(clean, target_col)
    mean = train[feature_cols].mean()
    std = train[feature_cols].std(ddof=1).replace(0, 1)
    x = ((train[feature_cols] - mean) / std).values
    x = np.column_stack([np.ones(len(x)), x])
    y = train[target_col].values
    weights = np.zeros(x.shape[1])
    for _ in range(iterations):
        p = 1 / (1 + np.exp(-(x @ weights)))
        gradient = x.T @ (p - y) / len(y)
        weights -= learning_rate * gradient
    if len(test):
        xt = ((test[feature_cols] - mean) / std).values
        xt = np.column_stack([np.ones(len(xt)), xt])
        pt = 1 / (1 + np.exp(-(xt @ weights)))
        accuracy = float(((pt >= 0.5) == test[target_col].astype(bool).values).mean())
    else:
        accuracy = None
    return {
        "available": True,
        "model": "logistic probability model",
        "features": feature_cols,
        "coefficients": {"intercept": float(weights[0]), **{col: float(weights[i + 1]) for i, col in enumerate(feature_cols)}},
        "train_size": int(len(train)),
        "test_size": int(len(test)),
        "test_accuracy": accuracy,
        "interpretation": "Outputs probability of outperformance, but only after enough labeled examples exist.",
    }


def alternative_data_placeholder() -> Dict:
    return {
        "available": False,
        "supported_future_sources": ["news sentiment", "earnings transcripts", "web traffic", "app rankings", "social volume", "SEC filing velocity"],
        "note": "Alternative data requires licensed or explicitly configured providers before it should influence alerts.",
    }
