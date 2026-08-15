"""Central inspectable registry for advanced models."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class ModelRegistryEntry:
    """Metadata for one model family."""

    name: str
    version: str
    required_inputs: list[str]
    validation_status: str
    active: bool
    limitations: list[str]

    def to_dict(self) -> dict[str, Any]:
        """Return JSON-safe metadata."""
        return self.__dict__


MODEL_REGISTRY: dict[str, ModelRegistryEntry] = {
    "relative_value": ModelRegistryEntry("Relative Value", "v3_relative_value_1", ["adjusted_prices", "economic_peer_metadata"], "fixture-tested", True, ["Requires economically defensible peers; not a trade instruction."]),
    "cointegration": ModelRegistryEntry("Engle-Granger Style Cointegration", "v3_cointegration_1", ["aligned_log_prices"], "fixture-tested", True, ["Approximate residual stationarity check; rolling stability required."]),
    "ewma_volatility": ModelRegistryEntry("EWMA Volatility", "v3_ewma_vol_1", ["daily_returns"], "fixture-tested", True, ["Responds quickly to recent volatility but can overreact."]),
    "garch_volatility": ModelRegistryEntry("Restrained GARCH", "v3_garch_safe_1", ["daily_returns", "optional_arch_dependency"], "fallback-tested", False, ["Unavailable without a validated dependency; falls back to EWMA."]),
    "single_stock_factor": ModelRegistryEntry("Single-Stock Factor Proxy", "v3_factor_proxy_1", ["stock_returns", "market_returns", "sector_returns"], "fixture-tested", True, ["Uses transparent proxies; skips unavailable factors."]),
    "risk_metrics": ModelRegistryEntry("Risk-Adjusted Descriptive Metrics", "v3_risk_metrics_1", ["daily_returns"], "fixture-tested", True, ["Descriptive only; not predictive."]),
}


def registry_as_dict() -> dict[str, dict[str, Any]]:
    """Return all registered model metadata."""
    return {key: value.to_dict() for key, value in MODEL_REGISTRY.items()}
