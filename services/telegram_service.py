"""Telegram service wrapper."""

from __future__ import annotations

from typing import Any, Dict

import telegram_notifier


def build_portfolio_briefing(quant: Dict[str, Any], portfolio: Dict[str, Any]) -> str:
    """Build the default professional Telegram briefing."""
    return telegram_notifier.TelegramMessageBuilder(quant, portfolio).build_detailed_report()
