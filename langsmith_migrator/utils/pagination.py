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
        seen_ids = set()  # Track IDs we've already yielded to prevent infinite loops
        max_iterations = 10000  # Safety limit to prevent truly infinite loops
        iterations = 0

        while iterations < max_iterations:
            iterations += 1
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

            # Track how many new items we found in this page
            new_items_count = 0

            for item in items:
                if item is not None:
                    # Try to get an ID to detect duplicates
                    item_id = None
                    if isinstance(item, dict):
                        item_id = item.get('id') or item.get('_id') or item.get('uuid')

                    # If we have an ID and we've seen it before, skip it
                    if item_id and item_id in seen_ids:
                        continue

                    # Track this ID if we have one
                    if item_id:
                        seen_ids.add(item_id)

                    new_items_count += 1
                    yield item

            # If we didn't find any new items, we're seeing duplicates - stop
            if new_items_count == 0:
                break

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
