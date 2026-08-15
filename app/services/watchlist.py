"""Session-safe watchlist management."""

from __future__ import annotations

import json
import re
from pathlib import Path

DEFAULT_WATCHLIST_PATH = Path("config/default_watchlist.json")
LOCAL_WATCHLIST_PATH = Path("watchlist.json")
MAX_WATCHLIST_SIZE = 25
TICKER_RE = re.compile(r"^[A-Z][A-Z0-9.-]{0,9}$")


def normalize_ticker(ticker: str) -> str:
    """Normalize a ticker symbol for display and data lookup."""
    return ticker.strip().upper().replace("/", "-")


def is_valid_ticker(ticker: str) -> bool:
    """Return whether a ticker is syntactically valid."""
    return bool(TICKER_RE.match(normalize_ticker(ticker)))


def load_default_watchlist(path: Path = DEFAULT_WATCHLIST_PATH) -> list[str]:
    """Load the source-controlled default watchlist."""
    if not path.exists():
        return ["AAPL", "MSFT", "NVDA"]
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return ["AAPL", "MSFT", "NVDA"]
    return clean_watchlist(data)


def load_cli_watchlist(path: Path = LOCAL_WATCHLIST_PATH) -> list[str]:
    """Load the local CLI watchlist, falling back to defaults."""
    if not path.exists():
        return load_default_watchlist()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return load_default_watchlist()
    return clean_watchlist(data)


def save_cli_watchlist(tickers: list[str], path: Path = LOCAL_WATCHLIST_PATH) -> None:
    """Persist a local CLI watchlist. Streamlit sessions do not use this."""
    path.write_text(json.dumps(clean_watchlist(tickers), indent=2), encoding="utf-8")


def clean_watchlist(tickers: list[str]) -> list[str]:
    """Normalize, validate, de-duplicate, and cap a watchlist."""
    clean: list[str] = []
    for raw in tickers:
        ticker = normalize_ticker(str(raw))
        if ticker and is_valid_ticker(ticker) and ticker not in clean:
            clean.append(ticker)
        if len(clean) >= MAX_WATCHLIST_SIZE:
            break
    return clean


def add_ticker(tickers: list[str], ticker: str) -> tuple[list[str], str | None]:
    """Add one ticker to a watchlist with validation."""
    normalized = normalize_ticker(ticker)
    if not is_valid_ticker(normalized):
        return tickers, f"{ticker} is not a valid ticker symbol."
    if normalized in tickers:
        return tickers, f"{normalized} is already in the watchlist."
    if len(tickers) >= MAX_WATCHLIST_SIZE:
        return tickers, f"Watchlist limit is {MAX_WATCHLIST_SIZE} tickers."
    return tickers + [normalized], None


def remove_ticker(tickers: list[str], ticker: str) -> list[str]:
    """Remove one ticker from a watchlist."""
    normalized = normalize_ticker(ticker)
    return [item for item in tickers if item != normalized]
