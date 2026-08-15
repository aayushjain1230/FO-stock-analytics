"""Transactional runtime persistence for analysis results."""

from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from typing import Any

DB_PATH = Path("state/v2_runtime.sqlite3")


class RuntimeStore:
    """Small repository layer so SQLite can be replaced later."""

    def __init__(self, path: Path = DB_PATH) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def initialize(self) -> None:
        """Create safe schemas if they do not exist."""
        with sqlite3.connect(self.path) as conn:
            conn.execute("CREATE TABLE IF NOT EXISTS filings (identity_key TEXT PRIMARY KEY, ticker TEXT NOT NULL, accession_number TEXT NOT NULL, payload TEXT NOT NULL)")
            conn.execute("CREATE TABLE IF NOT EXISTS whale_results (cache_key TEXT PRIMARY KEY, ticker TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL)")
            conn.execute("CREATE TABLE IF NOT EXISTS simulation_results (cache_key TEXT PRIMARY KEY, ticker TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL)")
            conn.execute("CREATE TABLE IF NOT EXISTS calibration_results (cache_key TEXT PRIMARY KEY, ticker TEXT NOT NULL, payload TEXT NOT NULL, created_at TEXT NOT NULL)")
            conn.execute("CREATE TABLE IF NOT EXISTS alert_dedupe (dedupe_key TEXT PRIMARY KEY, sent_at TEXT NOT NULL)")

    def upsert_json(self, table: str, key: str, ticker: str, payload: dict[str, Any], created_at: str) -> None:
        """Transactionally upsert a JSON payload into an approved table."""
        if table not in {"whale_results", "simulation_results", "calibration_results"}:
            raise ValueError("Unsupported runtime table.")
        with sqlite3.connect(self.path) as conn:
            conn.execute(f"INSERT OR REPLACE INTO {table} (cache_key, ticker, payload, created_at) VALUES (?, ?, ?, ?)", (key, ticker, json.dumps(payload, default=str), created_at))

    def get_json(self, table: str, key: str) -> dict[str, Any] | None:
        """Read a cached JSON payload."""
        if table not in {"whale_results", "simulation_results", "calibration_results"}:
            raise ValueError("Unsupported runtime table.")
        with sqlite3.connect(self.path) as conn:
            row = conn.execute(f"SELECT payload FROM {table} WHERE cache_key = ?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    def status(self) -> dict[str, Any]:
        """Return table counts for Settings."""
        with sqlite3.connect(self.path) as conn:
            counts = {
                table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in ["filings", "whale_results", "simulation_results", "calibration_results", "alert_dedupe"]
            }
        return {"path": str(self.path), "counts": counts}
