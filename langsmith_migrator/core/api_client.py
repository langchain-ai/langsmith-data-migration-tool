"""Simplified API client with improved separation of concerns."""

import time
import requests
from typing import Dict, Any, Optional, List, Generator
from rich.console import Console

from ..utils.retry import retry_on_failure, APIError, RateLimitError
from ..utils.pagination import PaginationHelper


class NotFoundError(APIError):
    """Resource not found error."""
    pass


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

        if response.status_code == 404:
            error_detail = ""
            try:
                error_data = response.json()
                error_detail = error_data.get("detail", error_data.get("message", ""))
            except:
                error_detail = response.text[:200]

            raise NotFoundError(
                f"Resource not found: {endpoint} - {error_detail}",
                status_code=404,
                request_info={
                    "endpoint": endpoint,
                    "url": response.request.url,
                    "body": response.request.body[:1000] if response.request.body else "None",
                    "headers": str(response.request.headers)
                }
            )

        if response.status_code == 429:
            raise RateLimitError(
                f"Rate limit exceeded for {endpoint}",
                status_code=429,
                request_info={"endpoint": endpoint}
            )

        if not response.ok:
            self.error_count += 1
            error_detail = ""
            try:
                error_data = response.json()
                error_detail = error_data.get("detail", error_data.get("message", ""))
            except:
                error_detail = response.text[:500]

            raise APIError(
                f"API request failed: {response.status_code} - {error_detail}",
                status_code=response.status_code,
                request_info={
                    "endpoint": endpoint,
                    "method": response.request.method
                }
            )

        try:
            json_response = response.json()
            return json_response if json_response is not None else {}
        except ValueError as e:
            raise APIError(
                f"Invalid JSON response from {endpoint}: {e}",
                request_info={"endpoint": endpoint}
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
    ) -> List[Dict[str, Any]]:
        """
        Post items in batches with smart retry logic (binary splitting).

        Args:
            endpoint: API endpoint
            items: List of items to post
            batch_size: Number of items per batch

        Returns:
            List of responses
        """
        responses = []

        for i in range(0, len(items), batch_size):
            batch = items[i:i + batch_size]

            if self.verbose:
                self.console.print(f"[dim]Posting batch {i//batch_size + 1} ({len(batch)} items)[/dim]")

            # Use recursive smart batching
            batch_responses = self._post_batch_recursive(endpoint, batch)
            responses.extend(batch_responses)

        return responses

    def _post_batch_recursive(self, endpoint: str, items: List[Dict[str, Any]]) -> List[Any]:
        """
        Recursively post batches, splitting them on failure to isolate bad records.
        """
        if not items:
            return []

        try:
            # Try to post the whole batch
            response = self.post(endpoint, items)

            # Normalize response to list
            if isinstance(response, list):
                return response
            # Fallback for endpoints that might return single object for batch (unlikely but safe)
            return [response] * len(items)

        except APIError as e:
            # Base case: if batch size is 1, it's a definitive failure for this item
            if len(items) == 1:
                if self.verbose:
                    self.console.print(f"[red]Item failed permanently: {e}[/red]")
                return [None]

            # Recursive step: Split and retry
            mid = len(items) // 2
            left = items[:mid]
            right = items[mid:]

            if self.verbose:
                self.console.print(f"[yellow]Batch failed, splitting into {len(left)} and {len(right)} items to isolate error[/yellow]")

            return (
                self._post_batch_recursive(endpoint, left) +
                self._post_batch_recursive(endpoint, right)
            )

    def test_connection(self) -> bool:
        """
        Test the connection to the API.

        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Try multiple possible endpoints to find one that works
            has_api_version = '/api/v1' in self.base_url or '/api/v2' in self.base_url

            if has_api_version:
                endpoints_to_try = ["/datasets", "/health", "/"]
            else:
                endpoints_to_try = ["/api/v1/datasets", "/datasets", "/health", "/"]

            for endpoint in endpoints_to_try:
                try:
                    params = {"limit": 1} if "datasets" in endpoint else None
                    self.get(endpoint, params=params)
                    return True
                except NotFoundError:
                    continue  # Try next endpoint
                except Exception:
                    continue  # Try next endpoint

            # If all endpoints failed, return False
            return False
        except Exception as e:
            if self.verbose:
                self.console.print(f"[red]Connection test failed: {e}[/red]")
            return False

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
