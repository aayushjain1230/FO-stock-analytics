"""
Utility functions for retry logic, caching, and common operations.
"""
import time
import functools
import hashlib
import json
import os
from pathlib import Path
from typing import Callable, Any, Optional
import requests
from logger_config import logger

# Cache directory
CACHE_DIR = Path("cache")
CACHE_DIR.mkdir(exist_ok=True)

def retry_on_failure(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """
    Decorator for retrying functions on failure.
    
    Args:
        max_retries: Maximum number of retry attempts
        delay: Initial delay between retries in seconds
        backoff: Multiplier for delay after each retry
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
                            f"{func.__name__} failed (attempt {attempt + 1}/{max_retries}): {e}. "
                            f"Retrying in {current_delay:.1f}s..."
                        )
                        time.sleep(current_delay)
                        current_delay *= backoff
                    else:
                        logger.error(f"{func.__name__} failed after {max_retries} attempts: {e}")
            
            raise last_exception
        return wrapper
    return decorator

def cache_result(cache_key: str, ttl_seconds: int = 3600):
    """
    Decorator for caching function results to disk.
    
    Args:
        cache_key: Unique identifier for the cache
        ttl_seconds: Time-to-live in seconds (default 1 hour)
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            # Generate cache file path
            cache_hash = hashlib.md5(f"{cache_key}_{args}_{kwargs}".encode()).hexdigest()
            cache_file = CACHE_DIR / f"{cache_hash}.json"
            
            # Check if cache exists and is valid
            if cache_file.exists():
                try:
                    with open(cache_file, 'r') as f:
                        cached_data = json.load(f)
                    
                    cache_time = cached_data.get('timestamp', 0)
                    if time.time() - cache_time < ttl_seconds:
                        logger.debug(f"Cache hit for {func.__name__}")
                        return cached_data['result']
                    else:
                        logger.debug(f"Cache expired for {func.__name__}")
                except Exception as e:
                    logger.warning(f"Error reading cache: {e}")
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            
            try:
                with open(cache_file, 'w') as f:
                    json.dump({
                        'timestamp': time.time(),
                        'result': result
                    }, f, default=str)
                logger.debug(f"Cached result for {func.__name__}")
            except Exception as e:
                logger.warning(f"Error writing cache: {e}")
            
            return result
        return wrapper
    return decorator

@retry_on_failure(max_retries=3, delay=2.0)
def safe_request(url: str, headers: Optional[dict] = None, timeout: int = 15) -> requests.Response:
    """
    Makes an HTTP request with retry logic and error handling.
    
    Args:
        url: URL to request
        headers: Optional headers dictionary
        timeout: Request timeout in seconds
    
    Returns:
        Response object
    
    Raises:
        requests.RequestException: If request fails after retries
    """
    try:
        response = requests.get(url, headers=headers or {}, timeout=timeout)
        response.raise_for_status()
        return response
    except requests.RequestException as e:
        logger.error(f"Request failed for {url}: {e}")
        raise

def validate_ticker(ticker: str) -> bool:
    """
    Validates a ticker symbol format.
    
    Args:
        ticker: Ticker symbol to validate
    
    Returns:
        True if valid, False otherwise
    """
    if not ticker or not isinstance(ticker, str):
        return False
    
    ticker_upper = ticker.upper().strip()
    
    # Basic validation: 1-5 characters, alphanumeric and dots/dashes
    if len(ticker_upper) < 1 or len(ticker_upper) > 5:
        return False
    
    if not all(c.isalnum() or c in ['.', '-'] for c in ticker_upper):
        return False
    
    return True

def format_percentage(value: float, decimals: int = 2) -> str:
    """Format a float as a percentage string."""
    return f"{value:.{decimals}f}%"

def format_currency(value: float, decimals: int = 2) -> str:
    """Format a float as a currency string."""
    return f"${value:,.{decimals}f}"
