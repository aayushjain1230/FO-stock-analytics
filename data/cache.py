"""Small file-cache helpers."""

from __future__ import annotations

import os
import time
from typing import Callable, TypeVar

from data.data_loader import load_json, save_json

T = TypeVar("T")


def cached_json(path: str, ttl_seconds: int, producer: Callable[[], T]) -> T:
    """Return cached JSON if fresh; otherwise produce and persist a value."""
    payload = load_json(path, {})
    if payload and time.time() - payload.get("cached_at", 0) <= ttl_seconds:
        return payload.get("data")
    data = producer()
    save_json(path, {"cached_at": time.time(), "data": data})
    return data
