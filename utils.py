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
    """
    Decorator for retrying functions on failure.
    """
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
# DISK CACHE DECORATOR
# ============================================================

def cache_result(cache_key: str, ttl_seconds: int = 3600):
    """
    Decorator for caching function results to disk.

    The cached payload is JSON-serialized and TTL-validated.
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            cache_hash = hashlib.md5(
                f"{cache_key}_{args}_{kwargs}".encode()
            ).hexdigest()

            cache_file = CACHE_DIR / f"{cache_hash}.json"

            # ----------------------------
            # Cache read
            # ----------------------------
            if cache_file.exists():
                try:
                    with open(cache_file, "r") as f:
                        cached_data = json.load(f)

                    cache_time = cached_data.get("timestamp", 0)
                    if time.time() - cache_time < ttl_seconds:
                        logger.debug(f"Cache hit for {func.__name__}")
                        return cached_data["result"]

                    logger.debug(f"Cache expired for {func.__name__}")

                except Exception as e:
                    logger.warning(f"Cache read failed: {e}")

            # ----------------------------
            # Cache miss → execute function
            # ----------------------------
            result = func(*args, **kwargs)

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
                        default=str
                    )
                logger.debug(f"Cached result for {func.__name__}")
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
    """
    Makes an HTTP GET request with retries and status validation.
    """
    response = requests.get(url, headers=headers or {}, timeout=timeout)
    response.raise_for_status()
    return response

# ============================================================
# HTML TABLE PARSER (CRITICAL FIX)
# ============================================================

def read_html_table(html_text: str, table_index: int = 0) -> pd.DataFrame:
    """
    Safely parse HTML tables from raw HTML text.

    This prevents pandas from misinterpreting HTML as a file path,
    which causes CI-only failures.
    """
    tables = pd.read_html(StringIO(html_text))
    if not tables:
        raise ValueError("No HTML tables found")
    return tables[table_index]

# ============================================================
# TICKER VALIDATION
# ============================================================

def validate_ticker(ticker: str) -> bool:
    """
    Validates a ticker symbol format.
    """
    if not ticker or not isinstance(ticker, str):
        return False

    t = ticker.upper().strip()

    if not (1 <= len(t) <= 5):
        return False

    if not all(c.isalnum() or c in {".", "-"} for c in t):
        return False

    return True

# ============================================================
# FORMAT HELPERS
# ============================================================

def format_percentage(value: float, decimals: int = 2) -> str:
    return f"{value:.{decimals}f}%"

def format_currency(value: float, decimals: int = 2) -> str:
    return f"${value:,.{decimals}f}"
