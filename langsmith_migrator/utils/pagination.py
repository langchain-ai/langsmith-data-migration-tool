"""Pagination utilities for API calls."""

from typing import Dict, Any, Optional, Generator, Callable


class PaginationHelper:
    """Helper for paginating through API results."""

    @staticmethod
    def paginate(
        fetch_fn: Callable,
        endpoint: str,
        params: Optional[Dict] = None,
        page_size: int = 100
    ) -> Generator[Dict[str, Any], None, None]:
        """
        Paginate through API results.

        Args:
            fetch_fn: Function to fetch data (e.g., client.get)
            endpoint: API endpoint
            params: Query parameters
            page_size: Number of items per page

        Yields:
            Individual items from paginated results
        """
        if params is None:
            params = {}

        params["limit"] = page_size
        offset = 0

        while True:
            params["offset"] = offset

            try:
                response = fetch_fn(endpoint, params)
            except Exception:
                # No more results or error
                break

            # Handle different response formats
            items = PaginationHelper._extract_items(response)

            if not items:
                break

            for item in items:
                if item is not None:
                    yield item

            # Check if we got fewer items than requested (end of data)
            if len(items) < page_size:
                break

            offset += len(items)

    @staticmethod
    def _extract_items(response: Any) -> list:
        """Extract items from response."""
        if isinstance(response, list):
            return response
        elif isinstance(response, dict):
            # Try common keys for paginated data
            items = response.get("items", response.get("data", response.get("results", [])))
            if not items and not isinstance(items, list):
                # Might be a single item response
                items = [response]
            return items if isinstance(items, list) else []
        return []
