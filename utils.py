"""
Utility functions for retry logic, caching, and common operations.
"""

import time
import functools
import hashlib
import json
from pathlib import Path
from typing import Callable, Optional
from io import StringIO

import requests
import pandas as pd
from logger_config import logger

# ============================================================
# CACHE CONFIGURATION
# ============================================================

CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)

# ============================================================
# RETRY DECORATOR
# ============================================================

def retry_on_failure(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            last_exception = None

            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:
                        logger.warning(
                            f"{func.__name__} failed "
                            f"(attempt {attempt + 1}/{max_retries}): {e}. "
                            f"Retrying in {current_delay:.1f}s..."
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(
                            f"{func.__name__} failed after {max_retries} attempts",
                            exc_info=True
                        )
            raise last_exception
        return wrapper
    return decorator

# ============================================================
# DISK CACHE DECORATOR (SAFE)
# ============================================================

def _stable_cache_key(base: str, args, kwargs) -> str:
    payload = {
        "base": base,
        "args": [str(a) for a in args],
        "kwargs": {k: str(v) for k, v in sorted(kwargs.items())},
    }
    raw = json.dumps(payload, sort_keys=True)
    return hashlib.md5(raw.encode()).hexdigest()


def cache_result(cache_key: str, ttl_seconds: int = 3600):
    """
    Cache JSON-safe results only.
    Do NOT use this decorator for DataFrames.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            cache_hash = _stable_cache_key(cache_key, args, kwargs)
            cache_file = CACHE_DIR / f"{cache_hash}.json"

            # ----------------------------
            # Cache read
            # ----------------------------
            if cache_file.exists():
                try:
                    with open(cache_file, "r") as f:
                        cached = json.load(f)

                    if time.time() - cached["timestamp"] < ttl_seconds:
                        logger.debug(f"Cache hit for {func.__name__}")
                        return cached["result"]

                except Exception as e:
                    logger.warning(f"Cache read failed: {e}")

            # ----------------------------
            # Cache miss
            # ----------------------------
            result = func(*args, **kwargs)

            # Enforce JSON safety
            try:
                json.dumps(result)
            except TypeError:
                logger.warning(
                    f"Result from {func.__name__} is not JSON-serializable; skipping cache"
                )
                return result

            # ----------------------------
            # Cache write
            # ----------------------------
            try:
                with open(cache_file, "w") as f:
                    json.dump(
                        {
                            "timestamp": time.time(),
                            "result": result,
                        },
                        f,
                        indent=2
                    )
            except Exception as e:
                logger.warning(f"Cache write failed: {e}")

            return result
        return wrapper
    return decorator

# ============================================================
# SAFE HTTP REQUEST
# ============================================================

@retry_on_failure(max_retries=3, delay=2.0)
def safe_request(
    url: str,
    headers: Optional[dict] = None,
    timeout: int = 15
) -> requests.Response:
    response = requests.get(url, headers=headers or {}, timeout=timeout)
    response.raise_for_status()

    if not response.text or len(response.text) < 50:
        raise ValueError("Empty or invalid HTTP response body")

    return response

# ============================================================
# HTML TABLE PARSER
# ============================================================

def read_html_table(html_text: str, table_index: int = 0) -> pd.DataFrame:
    tables = pd.read_html(StringIO(html_text))

    if not tables:
        raise ValueError("No HTML tables found")

    if table_index >= len(tables):
        raise IndexError(
            f"Requested table_index {table_index}, but only {len(tables)} tables found"
        )

    return tables[table_index]

# ============================================================
# TICKER VALIDATION
# ============================================================

def validate_ticker(ticker: str) -> bool:
    if not ticker or not isinstance(ticker, str):
        return False

    t = ticker.upper().strip()

    if not (1 <= len(t) <= 5):
        return False

    if not t[0].isalnum():
        return False

    if not all(c.isalnum() or c in {".", "-"} for c in t):
        return False

    if ".." in t or "--" in t:
        return False

    return True

# ============================================================
# FORMAT HELPERS
# ============================================================

def format_percentage(value: float, decimals: int = 2) -> str:
    return f"{value:.{decimals}f}%"

def format_currency(value: float, decimals: int = 2) -> str:
    return f"${value:,.{decimals}f}"
