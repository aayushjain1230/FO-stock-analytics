"""Explicit background job entry points for scheduled operation."""

from __future__ import annotations

from app.models.simulation import SimulationConfig
from app.services.sec_identity import sec_identity_status


def daily_watchlist_analysis_job() -> str:
    """Placeholder entry point name for GitHub Actions or local cron."""
    return "Run `python main.py --analyze` for daily watchlist analysis."


def sec_sync_job_status() -> str:
    """Return SEC sync readiness without making network requests."""
    status = sec_identity_status()
    return status.message


def daily_telegram_brief_job() -> str:
    """Placeholder entry point for one concise daily report."""
    return "Run `python main.py --analyze --send-telegram` after market close."


def calibration_job_config() -> SimulationConfig:
    """Default manual/periodic calibration settings."""
    return SimulationConfig(simulations=500, random_seed=7)
