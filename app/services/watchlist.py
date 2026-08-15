"""Session-safe watchlist management."""

from __future__ import annotations

import json
import os
import re
from dataclasses import dataclass
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


@dataclass(frozen=True)
class ResolvedWatchlist:
    """Resolved watchlist plus source and rejected symbols."""

    tickers: list[str]
    source: str
    rejected: list[str]


def split_ticker_input(value: str | list[str] | None) -> list[str]:
    """Split CLI/workflow/env ticker input without shell evaluation."""
    if value is None:
        return []
    if isinstance(value, list):
        pieces: list[str] = []
        for item in value:
            pieces.extend(split_ticker_input(item))
        return pieces
    text = str(value).strip()
    if not text:
        return []
    delimiter = "," if "," in text else None
    if delimiter is None and re.search(r"[^A-Za-z0-9.\-/\s]", text):
        return [text]
    pieces = text.split(delimiter) if delimiter else text.split()
    return [piece.strip() for piece in pieces if piece.strip()]


def clean_watchlist_with_rejections(tickers: list[str]) -> tuple[list[str], list[str]]:
    """Normalize and validate symbols, returning rejected raw values."""
    clean: list[str] = []
    rejected: list[str] = []
    for raw in tickers:
        ticker = normalize_ticker(str(raw))
        if not ticker:
            continue
        if not is_valid_ticker(ticker):
            rejected.append(str(raw))
            continue
        if ticker not in clean:
            clean.append(ticker)
        if len(clean) >= MAX_WATCHLIST_SIZE:
            break
    return clean, rejected


def resolve_watchlist(manual_input: str | list[str] | None = None, env_value: str | None = None, default_path: Path = DEFAULT_WATCHLIST_PATH) -> ResolvedWatchlist:
    """Resolve watchlist by priority: manual input, WATCHLIST_TICKERS, default file."""
    sources = [
        ("manual input", split_ticker_input(manual_input)),
        ("WATCHLIST_TICKERS", split_ticker_input(env_value if env_value is not None else os.getenv("WATCHLIST_TICKERS"))),
        ("config/default_watchlist.json", load_default_watchlist(default_path)),
    ]
    all_rejected: list[str] = []
    for source, raw_tickers in sources:
        if not raw_tickers:
            continue
        clean, rejected = clean_watchlist_with_rejections(raw_tickers)
        all_rejected.extend(rejected)
        if clean:
            return ResolvedWatchlist(clean, source, all_rejected)
        if source != "config/default_watchlist.json":
            raise ValueError(f"No valid tickers were provided from {source}. Rejected: {', '.join(rejected) or 'none'}")
    raise ValueError("No valid tickers were provided. Check manual input, WATCHLIST_TICKERS, or config/default_watchlist.json.")
