"""Retry utilities for API calls."""

import time
import requests
import socket
from functools import wraps
from typing import Callable, Optional


# Maximum backoff delay in seconds to prevent indefinite waits
MAX_BACKOFF_SECONDS = 60.0


class RateLimitError(Exception):
    """Rate limit exceeded error."""

    def __init__(self, message: str, retry_after: Optional[float] = None):
        super().__init__(message)
        self.retry_after = retry_after

    def __str__(self):
        msg = super().__str__()
        if self.retry_after:
            return f"{msg} (retry after {self.retry_after}s)"
        return msg


class APIError(Exception):
    """Base exception for API errors."""

    def __init__(self, message: str, status_code: int = None, request_info: dict = None):
        super().__init__(message)
        self.status_code = status_code
        self.request_info = request_info

    def __str__(self):
        msg = super().__str__()
        if self.request_info:
            return f"{msg} | Request Info: {self.request_info}"
        return msg


class AuthenticationError(APIError):
    """Authentication failed (401/403) - invalid or expired API key."""

    def __init__(self, message: str, status_code: int, request_info: dict = None):
        super().__init__(message, status_code, request_info)


class ConflictError(APIError):
    """Resource conflict (409) - duplicate or concurrent modification."""

    def __init__(self, message: str, request_info: dict = None):
        super().__init__(message, 409, request_info)


def retry_on_failure(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """
    Decorator to retry failed API calls with exponential backoff.

    Features:
    - Respects Retry-After headers for rate limiting
    - Has a maximum backoff cap to prevent indefinite waits
    - Handles various network errors (connection, timeout, read)
    - Provides clear error messages for auth failures
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay

            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except RateLimitError as e:
                    # Always retry rate limits
                    last_exception = e
                    # Use Retry-After if provided, otherwise use exponential backoff
                    if e.retry_after:
                        wait_time = min(e.retry_after, MAX_BACKOFF_SECONDS)
                    else:
                        wait_time = min(current_delay * 2, MAX_BACKOFF_SECONDS)
                    time.sleep(wait_time)
                    current_delay = min(current_delay * backoff, MAX_BACKOFF_SECONDS)
                except AuthenticationError:
                    # Never retry auth errors - they won't succeed without user intervention
                    raise
                except ConflictError:
                    # Don't retry conflicts by default - caller should handle deduplication
                    raise
                except APIError as e:
                    last_exception = e
                    if e.status_code and e.status_code >= 500:
                        # Retry server errors
                        if attempt < max_retries - 1:
                            wait_time = min(current_delay, MAX_BACKOFF_SECONDS)
                            time.sleep(wait_time)
                            current_delay = min(current_delay * backoff, MAX_BACKOFF_SECONDS)
                        continue
                    else:
                        # Don't retry other client errors
                        raise
                except (
                    requests.exceptions.ConnectionError,
                    requests.exceptions.Timeout,
                    requests.exceptions.ReadTimeout,
                    socket.timeout,
                ) as e:
                    # Retry network errors
                    last_exception = e
                    if attempt < max_retries - 1:
                        wait_time = min(current_delay, MAX_BACKOFF_SECONDS)
                        time.sleep(wait_time)
                        current_delay = min(current_delay * backoff, MAX_BACKOFF_SECONDS)
                    continue

            # All retries exhausted
            raise last_exception

        return wrapper
    return decorator
