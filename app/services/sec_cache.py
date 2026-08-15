"""Small JSON cache for respectful SEC synchronization."""

from __future__ import annotations

import hashlib
import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

CACHE_DIR = Path("cache/sec")


@dataclass(frozen=True)
class CacheEntry:
    """Cached value with metadata useful for conditional requests."""

    value: Any
    timestamp: float
    etag: str | None = None
    last_modified: str | None = None


class SecCache:
    """File-backed SEC cache isolated from source-controlled data."""

    def __init__(self, root: Path = CACHE_DIR) -> None:
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)

    def get(self, namespace: str, key: str, ttl_seconds: int) -> CacheEntry | None:
        """Return a fresh cache entry or None."""
        path = self._path(namespace, key)
        if not path.exists():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
            timestamp = float(data.get("timestamp", 0))
            if time.time() - timestamp > ttl_seconds:
                return None
            return CacheEntry(data.get("value"), timestamp, data.get("etag"), data.get("last_modified"))
        except Exception:
            return None

    def set(self, namespace: str, key: str, value: Any, etag: str | None = None, last_modified: str | None = None) -> CacheEntry:
        """Persist a JSON cache entry."""
        path = self._path(namespace, key)
        path.parent.mkdir(parents=True, exist_ok=True)
        entry = CacheEntry(value=value, timestamp=time.time(), etag=etag, last_modified=last_modified)
        path.write_text(json.dumps(entry.__dict__, indent=2, default=str), encoding="utf-8")
        return entry

    def status(self) -> dict[str, Any]:
        """Return cache status without exposing content."""
        files = list(self.root.rglob("*.json")) if self.root.exists() else []
        return {"path": str(self.root), "entries": len(files)}

    def _path(self, namespace: str, key: str) -> Path:
        safe_namespace = "".join(ch for ch in namespace if ch.isalnum() or ch in "_-") or "default"
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        return self.root / safe_namespace / f"{digest}.json"
