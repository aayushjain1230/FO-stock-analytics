"""Central configuration defaults for the quant research platform."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class PlatformThresholds:
    """Research thresholds used across engines and alerts."""

    max_single_position_risk_pct: float = 35.0
    high_average_correlation: float = 0.65
    minimum_signal_sample_size: int = 30
    max_cost_edge_ratio: float = 0.60
    confidence_watch_threshold: float = 0.60


THRESHOLDS = PlatformThresholds()
