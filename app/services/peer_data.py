"""Economic peer selection before statistical pair testing."""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from app.models.pair_result import PeerCandidate
from app.services.watchlist import normalize_ticker

PEER_UNIVERSE_PATH = Path("config/peer_universe.json")


def load_peer_universe(path: Path = PEER_UNIVERSE_PATH) -> dict:
    """Load curated peer metadata with documentation-friendly reasons."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {"version": "missing", "relationships": []}


def select_peer_candidates(tickers: list[str], histories: dict[str, pd.DataFrame], min_overlap: int = 252, path: Path = PEER_UNIVERSE_PATH) -> list[PeerCandidate]:
    """Return only economically defensible peer candidates with sufficient history."""
    wanted = {normalize_ticker(ticker) for ticker in tickers}
    output: list[PeerCandidate] = []
    for rel in load_peer_universe(path).get("relationships", []):
        a = normalize_ticker(rel.get("ticker_a", ""))
        b = normalize_ticker(rel.get("ticker_b", ""))
        if a not in wanted and b not in wanted:
            continue
        if not _has_overlap(histories.get(a), histories.get(b), min_overlap):
            continue
        reasons = list(rel.get("reasons", []))
        reasons.append(f"At least {min_overlap} trading days of overlapping history")
        output.append(PeerCandidate(a, b, reasons, "Medium", rel))
    return output


def _has_overlap(left: pd.DataFrame | None, right: pd.DataFrame | None, min_overlap: int) -> bool:
    if left is None or right is None or left.empty or right.empty:
        return False
    if "Close" not in left or "Close" not in right:
        return False
    joined = pd.concat([left["Close"].rename("a"), right["Close"].rename("b")], axis=1, sort=False).dropna()
    return len(joined) >= min_overlap
