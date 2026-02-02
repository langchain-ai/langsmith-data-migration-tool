"""Simplified API client with improved separation of concerns."""

import time
import requests
from typing import Dict, Any, Optional, List, Generator, Tuple
from dataclasses import dataclass
from rich.console import Console

from ..utils.retry import (
    retry_on_failure,
    APIError,
    RateLimitError,
    AuthenticationError,
    ConflictError,
)
from ..utils.pagination import PaginationHelper


class NotFoundError(APIError):
    """Resource not found error."""
    pass


@dataclass
class BatchItemResult:
    """Result for a single item in a batch operation."""
    success: bool
    data: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    item_index: int = -1


class BatchResult:
    """
    Result of a batch operation with proper error tracking.

    Provides detailed information about which items succeeded and failed,
    preventing silent data loss.
    """

    def __init__(self):
        self.items: List[BatchItemResult] = []

    def add_success(self, data: Dict[str, Any], index: int):
        """Record a successful item."""
        self.items.append(BatchItemResult(success=True, data=data, item_index=index))

    def add_failure(self, error: str, index: int):
        """Record a failed item."""
        self.items.append(BatchItemResult(success=False, error=error, item_index=index))

    @property
    def successes(self) -> List[BatchItemResult]:
        """Get all successful items."""
        return [item for item in self.items if item.success]

    @property
    def failures(self) -> List[BatchItemResult]:
        """Get all failed items."""
        return [item for item in self.items if not item.success]

    @property
    def success_count(self) -> int:
        """Count of successful items."""
        return len(self.successes)

    @property
    def failure_count(self) -> int:
        """Count of failed items."""
        return len(self.failures)

    @property
    def all_succeeded(self) -> bool:
        """Check if all items succeeded."""
        return self.failure_count == 0

    def get_responses(self) -> List[Optional[Dict[str, Any]]]:
        """Get responses in order, with None for failed items (legacy compatibility)."""
        # Sort by item_index to maintain order
        sorted_items = sorted(self.items, key=lambda x: x.item_index)
        return [item.data if item.success else None for item in sorted_items]


class EnhancedAPIClient:
    """Enhanced API client with retry logic, streaming, and better error handling."""

    def __init__(
        self,
        base_url: str,
        headers: Dict[str, str],
        verify_ssl: bool = True,
        timeout: int = 30,
        max_retries: int = 3,
        rate_limit_delay: float = 0.1,
        verbose: bool = False
    ):
        """
        Initialize the API client.

        Args:
            base_url: Base URL for the API
            headers: Headers to include in requests
            verify_ssl: Whether to verify SSL certificates
            timeout: Request timeout in seconds
            max_retries: Maximum number of retries for failed requests
            rate_limit_delay: Delay between requests to avoid rate limits
            verbose: Whether to log verbose output
        """
        self.base_url = base_url.rstrip('/')
        self.headers = headers
        self.verify_ssl = verify_ssl
        self.timeout = timeout
        self.max_retries = max_retries
        self.rate_limit_delay = rate_limit_delay
        self.verbose = verbose
        self.console = Console()

        # Track request statistics
        self.request_count = 0
        self.error_count = 0

        # Session for connection pooling
        self.session = requests.Session()
        self.session.headers.update(headers)
        self.session.verify = verify_ssl

    def _prepare_url(self, endpoint: str) -> str:
        """Prepare full URL from endpoint."""
        if endpoint.startswith('http'):
            return endpoint
        return f"{self.base_url}{endpoint}"

    def _handle_response(self, response: requests.Response, endpoint: str) -> Dict[str, Any]:
        """Handle API response and raise appropriate errors."""
        self.request_count += 1

        request_info = {
            "endpoint": endpoint,
            "method": response.request.method,
            "url": response.request.url,
        }

        # Helper to safely extract error details from response
        def get_error_detail() -> str:
            try:
                error_data = response.json()
                return error_data.get("detail", error_data.get("message", str(error_data)))
            except (ValueError, AttributeError):
                return response.text[:500] if response.text else "No response body"

        # 401 Unauthorized - invalid API key
        if response.status_code == 401:
            self.error_count += 1
            error_detail = get_error_detail()
            raise AuthenticationError(
                f"Authentication failed for {endpoint}: {error_detail}. "
                "Please check that your API key is valid and not expired.",
                status_code=401,
                request_info=request_info
            )

        # 403 Forbidden - valid key but insufficient permissions
        if response.status_code == 403:
            self.error_count += 1
            error_detail = get_error_detail()
            raise AuthenticationError(
                f"Access denied for {endpoint}: {error_detail}. "
                "Your API key may lack permission for this operation.",
                status_code=403,
                request_info=request_info
            )

        # 404 Not Found
        if response.status_code == 404:
            error_detail = get_error_detail()
            request_info["body"] = response.request.body[:1000] if response.request.body else "None"
            raise NotFoundError(
                f"Resource not found: {endpoint} - {error_detail}",
                status_code=404,
                request_info=request_info
            )

        # 409 Conflict - duplicate resource or concurrent modification
        if response.status_code == 409:
            self.error_count += 1
            error_detail = get_error_detail()
            raise ConflictError(
                f"Resource conflict at {endpoint}: {error_detail}. "
                "This resource may already exist or was modified concurrently.",
                request_info=request_info
            )

        # 429 Rate Limited
        if response.status_code == 429:
            # Parse Retry-After header if present
            retry_after = None
            retry_after_header = response.headers.get("Retry-After")
            if retry_after_header:
                try:
                    retry_after = float(retry_after_header)
                except (ValueError, TypeError):
                    pass

            raise RateLimitError(
                f"Rate limit exceeded for {endpoint}",
                retry_after=retry_after
            )

        # Other errors
        if not response.ok:
            self.error_count += 1
            error_detail = get_error_detail()
            raise APIError(
                f"API request failed: {response.status_code} - {error_detail}",
                status_code=response.status_code,
                request_info=request_info
            )

        # Success - parse JSON response
        try:
            json_response = response.json()
            # Validate response is dict or list as expected
            if json_response is None:
                return {}
            return json_response
        except ValueError as e:
            raise APIError(
                f"Invalid JSON response from {endpoint}: {e}",
                request_info=request_info
            )

    @retry_on_failure(max_retries=3)
    def get(self, endpoint: str, params: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Make a GET request.

        Args:
            endpoint: API endpoint
            params: Query parameters

        Returns:
            JSON response as dictionary
        """
        url = self._prepare_url(endpoint)

        if self.verbose:
            self.console.print(f"[dim]GET {url}[/dim]")

        # Add rate limiting delay
        if self.rate_limit_delay > 0:
            time.sleep(self.rate_limit_delay)

        response = self.session.get(url, params=params, timeout=self.timeout)
        return self._handle_response(response, endpoint)

    @retry_on_failure(max_retries=3)
    def post(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Make a POST request.

        Args:
            endpoint: API endpoint
            data: JSON data to send

        Returns:
            JSON response as dictionary
        """
        url = self._prepare_url(endpoint)

        if self.verbose:
            self.console.print(f"[dim]POST {url}[/dim]")

        # Add rate limiting delay
        if self.rate_limit_delay > 0:
            time.sleep(self.rate_limit_delay)

        response = self.session.post(url, json=data, timeout=self.timeout)
        return self._handle_response(response, endpoint)

    @retry_on_failure(max_retries=1)  # Reduce retries for PATCH - if it fails once, likely to keep failing
    def patch(self, endpoint: str, data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Make a PATCH request.

        Args:
            endpoint: API endpoint
            data: JSON data to send

        Returns:
            JSON response as dictionary
        """
        url = self._prepare_url(endpoint)

        if self.verbose:
            self.console.print(f"[dim]PATCH {url}[/dim]")

        # Add rate limiting delay
        if self.rate_limit_delay > 0:
            time.sleep(self.rate_limit_delay)

        # Use shorter timeout for PATCH - if it's slow, server is likely overloaded
        response = self.session.patch(url, json=data, timeout=15)
        return self._handle_response(response, endpoint)

    def get_paginated(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        page_size: int = 100
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Get paginated results, yielding one item at a time.

        Args:
            endpoint: API endpoint
            params: Query parameters
            page_size: Number of items per page

        Yields:
            Individual items from paginated results
        """
        yield from PaginationHelper.paginate(
            self.get,
            endpoint,
            params,
            page_size
        )

    def post_batch(
        self,
        endpoint: str,
        items: List[Dict[str, Any]],
        batch_size: int = 100
    ) -> List[Optional[Dict[str, Any]]]:
        """
        Post items in batches with smart retry logic (binary splitting).

        Args:
            endpoint: API endpoint
            items: List of items to post
            batch_size: Number of items per batch

        Returns:
            List of responses (None for failed items)
        """
        result = BatchResult()
        start_index = 0

        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]

            if self.verbose:
                self.console.print(f"[dim]Posting batch {i//batch_size + 1} ({len(batch)} items)[/dim]")

            # Use recursive smart batching
            self._post_batch_recursive(endpoint, batch, result, start_index)
            start_index += len(batch)

        # Log summary of failures if any occurred
        if result.failure_count > 0:
            self.console.print(
                f"[yellow]Batch operation completed with {result.failure_count} failure(s) "
                f"out of {len(items)} items[/yellow]"
            )
            # Log first few errors for debugging
            for failure in result.failures[:3]:
                self.console.print(f"[red]  Item {failure.item_index}: {failure.error}[/red]")
            if result.failure_count > 3:
                self.console.print(f"[red]  ... and {result.failure_count - 3} more failures[/red]")

        return result.get_responses()

    def post_batch_detailed(
        self,
        endpoint: str,
        items: List[Dict[str, Any]],
        batch_size: int = 100
    ) -> BatchResult:
        """
        Post items in batches with detailed error tracking.

        Unlike post_batch(), this returns a BatchResult with full error information
        for each failed item, enabling proper error handling and reporting.

        Args:
            endpoint: API endpoint
            items: List of items to post
            batch_size: Number of items per batch

        Returns:
            BatchResult with success/failure details for each item
        """
        result = BatchResult()
        start_index = 0

        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]

            if self.verbose:
                self.console.print(f"[dim]Posting batch {i//batch_size + 1} ({len(batch)} items)[/dim]")

            self._post_batch_recursive(endpoint, batch, result, start_index)
            start_index += len(batch)

        return result

    def _post_batch_recursive(
        self,
        endpoint: str,
        items: List[Dict[str, Any]],
        result: BatchResult,
        start_index: int
    ) -> None:
        """
        Recursively post batches, splitting them on failure to isolate bad records.
        Records results in the BatchResult object for proper error tracking.
        """
        if not items:
            return

        try:
            # Try to post the whole batch
            response = self.post(endpoint, items)

            # Normalize response to list
            if isinstance(response, list):
                responses = response
            elif isinstance(response, dict):
                # Single response for batch - assume success for all items
                responses = [response] * len(items)
            else:
                responses = [{}] * len(items)

            # Record successes
            for i, resp in enumerate(responses):
                if resp is not None:
                    result.add_success(resp, start_index + i)
                else:
                    result.add_failure("Empty response from API", start_index + i)

        except ConflictError as e:
            # 409 Conflict - item may already exist
            if len(items) == 1:
                if self.verbose:
                    self.console.print(f"[yellow]Item {start_index} conflict: {e}[/yellow]")
                result.add_failure(f"Conflict: {e}", start_index)
            else:
                # Split to find the conflicting item
                mid = len(items) // 2
                self._post_batch_recursive(endpoint, items[:mid], result, start_index)
                self._post_batch_recursive(endpoint, items[mid:], result, start_index + mid)

        except APIError as e:
            # Base case: if batch size is 1, it's a definitive failure for this item
            if len(items) == 1:
                error_msg = str(e)
                if self.verbose:
                    self.console.print(f"[red]Item {start_index} failed: {error_msg}[/red]")
                result.add_failure(error_msg, start_index)
            else:
                # Recursive step: Split and retry to isolate the failing item
                mid = len(items) // 2
                if self.verbose:
                    self.console.print(
                        f"[yellow]Batch failed, splitting into {len(items[:mid])} and "
                        f"{len(items[mid:])} items to isolate error[/yellow]"
                    )
                self._post_batch_recursive(endpoint, items[:mid], result, start_index)
                self._post_batch_recursive(endpoint, items[mid:], result, start_index + mid)

    def test_connection(self) -> Tuple[bool, Optional[str]]:
        """
        Test the connection to the API.

        Returns:
            Tuple of (success: bool, error_message: Optional[str])
        """
        # Try multiple possible endpoints to find one that works
        has_api_version = '/api/v1' in self.base_url or '/api/v2' in self.base_url

        if has_api_version:
            endpoints_to_try = ["/datasets", "/health", "/"]
        else:
            endpoints_to_try = ["/api/v1/datasets", "/datasets", "/health", "/"]

        last_error = None

        for endpoint in endpoints_to_try:
            try:
                params = {"limit": 1} if "datasets" in endpoint else None
                self.get(endpoint, params=params)
                return True, None
            except AuthenticationError as e:
                # Auth errors won't be fixed by trying different endpoints
                error_msg = (
                    f"Authentication failed: {e}. "
                    "Please verify your API key is correct and not expired."
                )
                if self.verbose:
                    self.console.print(f"[red]{error_msg}[/red]")
                return False, error_msg
            except NotFoundError:
                # Endpoint doesn't exist, try next one
                continue
            except RateLimitError as e:
                # Rate limited even during connection test
                error_msg = f"Rate limited during connection test: {e}"
                if self.verbose:
                    self.console.print(f"[yellow]{error_msg}[/yellow]")
                last_error = error_msg
                continue
            except APIError as e:
                # Other API error, record but try next endpoint
                last_error = str(e)
                continue
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                # Network error
                error_msg = f"Connection failed: {e}"
                if self.verbose:
                    self.console.print(f"[red]{error_msg}[/red]")
                return False, error_msg
            except Exception as e:
                # Unexpected error - log but continue
                last_error = f"Unexpected error: {e}"
                if self.verbose:
                    self.console.print(f"[red]{last_error}[/red]")
                continue

        # All endpoints failed
        error_msg = last_error or "All API endpoints returned errors or not found"
        if self.verbose:
            self.console.print(f"[red]Connection test failed: {error_msg}[/red]")
        return False, error_msg

    def get_statistics(self) -> Dict[str, int]:
        """Get request statistics."""
        return {
            "requests": self.request_count,
            "errors": self.error_count,
            "success_rate": (
                (self.request_count - self.error_count) / self.request_count
                if self.request_count > 0 else 0
            )
        }

    def close(self):
        """Close the session."""
        self.session.close()
