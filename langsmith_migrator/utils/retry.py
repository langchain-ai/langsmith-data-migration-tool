"""Retry utilities for API calls."""

import time
import requests
from functools import wraps
from typing import Callable


class RateLimitError(Exception):
    """Rate limit exceeded error."""
    pass


class APIError(Exception):
    """Base exception for API errors."""

    def __init__(self, message: str, status_code: int = None, request_info: dict = None):
        super().__init__(message)
        self.status_code = status_code
        self.request_info = request_info


def retry_on_failure(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """Decorator to retry failed API calls with exponential backoff."""

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay

            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except RateLimitError as e:
                    # Always retry rate limits with longer delay
                    last_exception = e
                    time.sleep(current_delay * 2)
                    current_delay *= backoff
                except APIError as e:
                    last_exception = e
                    if e.status_code and e.status_code >= 500:
                        # Retry server errors
                        if attempt < max_retries - 1:
                            time.sleep(current_delay)
                            current_delay *= backoff
                        continue
                    else:
                        # Don't retry client errors
                        raise
                except requests.exceptions.ConnectionError as e:
                    # Retry connection errors
                    last_exception = e
                    if attempt < max_retries - 1:
                        time.sleep(current_delay)
                        current_delay *= backoff
                    continue

            # All retries exhausted
            raise last_exception

        return wrapper
    return decorator
