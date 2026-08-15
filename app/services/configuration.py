"""Configuration loading and validation."""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AppConfig:
    """Validated application configuration."""

    benchmark: str = "SPY"
    period: str = "6mo"
    interval: str = "1d"
    daily_brief_enabled: bool = True
    alert_time: str = "16:15"
    quiet_hours: str = "21:00-08:00"
    explanation_depth: str = "Simple"


def load_config(path: Path = Path("config/config.json")) -> AppConfig:
    """Load optional local config with safe defaults."""
    raw: dict = {}
    if path.exists():
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise ValueError(f"Invalid config JSON: {exc}") from exc
    settings = raw.get("settings", {}) if isinstance(raw, dict) else {}
    explanation_depth = settings.get("explanation_depth", "Simple")
    if explanation_depth not in {"Simple", "Detailed"}:
        raise ValueError("settings.explanation_depth must be Simple or Detailed.")
    return AppConfig(
        benchmark=str(raw.get("benchmark", "SPY")).upper() if isinstance(raw, dict) else "SPY",
        period=str(settings.get("period", "6mo")),
        interval=str(settings.get("interval", "1d")),
        daily_brief_enabled=bool(settings.get("daily_brief_enabled", True)),
        alert_time=str(settings.get("alert_time", "16:15")),
        quiet_hours=str(settings.get("quiet_hours", "21:00-08:00")),
        explanation_depth=explanation_depth,
    )


def telegram_status() -> dict[str, str | bool]:
    """Return redacted Telegram configuration status."""
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    return {
        "configured": bool(token and chat_id),
        "token": "configured" if token else "missing",
        "chat_id": "configured" if chat_id else "missing",
    }
