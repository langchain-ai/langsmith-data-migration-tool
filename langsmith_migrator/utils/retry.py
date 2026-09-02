"""Retry utilities for API calls."""

import time
import requests
import socket
from functools import wraps
from typing import Callable, Optional


# Maximum backoff delay in seconds to prevent indefinite waits
MAX_BACKOFF_SECONDS = 60.0

# A 429 gets its own, larger budget than a server error, because it is the one
# failure where the server states exactly how long to wait. Sharing the general
# three attempts meant a burst of rate limits under concurrent readers gave up
# after ~6 s of backoff against a limit measured in minutes - observed killing a
# whole project one window into a trace export at --prefetch-windows 6.
RATE_LIMIT_RETRIES = 6


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


class UpstreamRejectionError(APIError):
    """An auth-shaped rejection (401/403) that did not come from LangSmith.

    LangSmith always returns JSON error bodies, so a non-JSON body on a 401/403
    means an intermediary - a proxy, load balancer, or WAF - refused the request
    before it reached LangSmith. Those rejections are frequently transient
    (rate-based rules, a dropped VPN, a changed egress IP), so unlike
    AuthenticationError this is retryable.
    """

    def __init__(self, message: str, status_code: int, request_info: dict = None):
        super().__init__(message, status_code, request_info)


def retry_upstream_rejections(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            current_delay = delay
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except UpstreamRejectionError:
                    if attempt >= max_retries - 1:
                        raise
                    time.sleep(min(current_delay, MAX_BACKOFF_SECONDS))
                    current_delay = min(current_delay * backoff, MAX_BACKOFF_SECONDS)

        return wrapper

    return decorator


def _await_rate_limit(func: Callable, args, kwargs, delay: float, backoff: float):
    """Call ``func``, waiting out 429s on their own budget.

    Wraps the call rather than restructuring the attempt loop below, so a
    rate limit does not consume an attempt that a server error needs. Once the
    budget is spent the ``RateLimitError`` propagates to that loop, which still
    treats it as the retryable error it always was.
    """
    wait = delay
    for _ in range(RATE_LIMIT_RETRIES):
        try:
            return func(*args, **kwargs)
        except RateLimitError as e:
            time.sleep(min(e.retry_after or wait * 2, MAX_BACKOFF_SECONDS))
            wait = min(wait * backoff, MAX_BACKOFF_SECONDS)
    return func(*args, **kwargs)


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
                    return _await_rate_limit(func, args, kwargs, current_delay, backoff)
                except RateLimitError:
                    # _await_rate_limit has already spent RATE_LIMIT_RETRIES
                    # honouring the server's own Retry-After. Reaching here means
                    # the limit is sustained rather than a burst, so more of the
                    # same backoff will not help - and multiplying the two
                    # budgets would wait for minutes before saying so.
                    raise
                except UpstreamRejectionError as e:
                    # An intermediary refused this before LangSmith saw it. Often
                    # transient, so retry rather than killing the caller's work item.
                    # Must precede the APIError clause below, which would re-raise it.
                    last_exception = e
                    if attempt < max_retries - 1:
                        wait_time = min(current_delay, MAX_BACKOFF_SECONDS)
                        time.sleep(wait_time)
                        current_delay = min(current_delay * backoff, MAX_BACKOFF_SECONDS)
                    continue
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
                    # A truncated response body. Not a ConnectionError subclass,
                    # so it needs naming: it is the usual way a full connection
                    # pool surfaces once several readers are in flight.
                    requests.exceptions.ChunkedEncodingError,
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
