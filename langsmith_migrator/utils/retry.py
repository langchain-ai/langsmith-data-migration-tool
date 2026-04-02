"""Retry utilities for API calls."""

import random
import time
import requests
import socket
from functools import wraps
from typing import Callable, Optional
from urllib.parse import urlparse, urlunparse


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


def _sanitize_url(url: str) -> str:
    """Strip query parameters that may contain secrets."""
    parsed = urlparse(url)
    if parsed.query:
        return urlunparse(parsed._replace(query="<redacted>"))
    return url


class APIError(Exception):
    """Base exception for API errors."""

    def __init__(self, message: str, status_code: int = None, request_info: dict = None):
        super().__init__(message)
        self.status_code = status_code
        self.request_info = request_info

    def __str__(self):
        msg = super().__str__()
        if self.request_info:
            safe_info = {**self.request_info}
            if "url" in safe_info:
                safe_info["url"] = _sanitize_url(safe_info["url"])
            return f"{msg} | Request Info: {safe_info}"
        return msg


class AuthenticationError(APIError):
    """Authentication failed (401/403) - invalid or expired API key."""

    def __init__(self, message: str, status_code: int, request_info: dict = None):
        super().__init__(message, status_code, request_info)


class ConflictError(APIError):
    """Resource conflict (409) - duplicate or concurrent modification."""

    def __init__(self, message: str, request_info: dict = None):
        super().__init__(message, 409, request_info)


def _jittered_sleep(wait_time: float) -> None:
    """Sleep with jitter to avoid thundering herd."""
    jitter = random.uniform(0, 0.25 * wait_time)
    time.sleep(wait_time + jitter)


def retry_on_failure(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """
    Decorator to retry failed API calls with exponential backoff.

    Args:
        max_retries: Total number of attempts (must be >= 1).
        delay: Initial backoff delay in seconds.
        backoff: Multiplier applied to delay after each retry.

    Features:
    - Respects Retry-After headers for rate limiting
    - Has a maximum backoff cap to prevent indefinite waits
    - Adds jitter to avoid thundering herd
    - Handles various network errors (connection, timeout, read)
    - Provides clear error messages for auth failures
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            current_delay = delay
            attempts = max(max_retries, 1)

            for attempt in range(attempts):
                try:
                    return func(*args, **kwargs)
                except RateLimitError as e:
                    last_exception = e
                    if e.retry_after:
                        wait_time = min(e.retry_after, MAX_BACKOFF_SECONDS)
                    else:
                        wait_time = min(current_delay, MAX_BACKOFF_SECONDS)
                    _jittered_sleep(wait_time)
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
                        if attempt < attempts - 1:
                            wait_time = min(current_delay, MAX_BACKOFF_SECONDS)
                            _jittered_sleep(wait_time)
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
                    if attempt < attempts - 1:
                        wait_time = min(current_delay, MAX_BACKOFF_SECONDS)
                        _jittered_sleep(wait_time)
                        current_delay = min(current_delay * backoff, MAX_BACKOFF_SECONDS)
                    continue

            # All retries exhausted
            raise last_exception

        return wrapper
    return decorator


def request_with_retry(
    session: requests.Session,
    method: str,
    url: str,
    max_retries: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    **kwargs,
) -> requests.Response:
    """Execute an HTTP request with retry logic for raw requests.Session calls.

    Provides the same retry semantics as ``retry_on_failure`` but works
    with bare ``requests.Session`` objects used by the prompt, rules, and
    dataset migrators for operations that cannot go through
    ``EnhancedAPIClient``.

    Args:
        session: The requests session to use.
        method: HTTP method (``"GET"``, ``"POST"``, etc.).
        url: The full URL to request.
        max_retries: Total attempts (>= 1).
        delay: Initial backoff delay in seconds.
        backoff: Multiplier applied to delay after each retry.
        **kwargs: Forwarded to ``session.request``.

    Returns:
        The :class:`requests.Response` object.

    Raises:
        requests.exceptions.HTTPError: After all retries exhausted for
            retryable status codes, or immediately for non-retryable ones.
        requests.exceptions.ConnectionError / Timeout: After all retries.
    """
    last_exception: Exception | None = None
    current_delay = delay
    attempts = max(max_retries, 1)

    for attempt in range(attempts):
        try:
            response = session.request(method, url, **kwargs)

            if response.ok:
                return response

            status = response.status_code

            if status == 429:
                last_exception = requests.exceptions.HTTPError(response=response)
                retry_after = response.headers.get("Retry-After")
                if retry_after:
                    try:
                        wait_time = min(float(retry_after), MAX_BACKOFF_SECONDS)
                    except (ValueError, TypeError):
                        wait_time = min(current_delay, MAX_BACKOFF_SECONDS)
                else:
                    wait_time = min(current_delay, MAX_BACKOFF_SECONDS)
                _jittered_sleep(wait_time)
                current_delay = min(current_delay * backoff, MAX_BACKOFF_SECONDS)
                continue

            if status >= 500:
                last_exception = requests.exceptions.HTTPError(response=response)
                if attempt < attempts - 1:
                    _jittered_sleep(min(current_delay, MAX_BACKOFF_SECONDS))
                    current_delay = min(current_delay * backoff, MAX_BACKOFF_SECONDS)
                continue

            # Non-retryable status — raise immediately
            response.raise_for_status()

        except (
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ReadTimeout,
            socket.timeout,
        ) as exc:
            last_exception = exc
            if attempt < attempts - 1:
                _jittered_sleep(min(current_delay, MAX_BACKOFF_SECONDS))
                current_delay = min(current_delay * backoff, MAX_BACKOFF_SECONDS)
            continue

    if last_exception is not None:
        raise last_exception
    raise RuntimeError("request_with_retry: unexpected state")
