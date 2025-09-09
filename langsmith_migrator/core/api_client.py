"""Enhanced API client with retry logic and better error handling."""

import time
import requests
from typing import Dict, Any, Optional, List, Generator
from functools import wraps
from rich.console import Console


class APIError(Exception):
    """Base exception for API errors."""
    def __init__(self, message: str, status_code: Optional[int] = None, 
                 request_info: Optional[Dict[str, Any]] = None):
        super().__init__(message)
        self.status_code = status_code
        self.request_info = request_info


class RateLimitError(APIError):
    """Rate limit exceeded error."""
    pass


class NotFoundError(APIError):
    """Resource not found error."""
    pass


def retry_on_failure(max_retries: int = 3, delay: float = 1.0, backoff: float = 2.0):
    """Decorator to retry failed API calls with exponential backoff."""
    def decorator(func):
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


class EnhancedAPIClient:
    """Enhanced API client with retry logic, streaming, and better error handling."""
    
    def __init__(self, base_url: str, headers: Dict[str, str], 
                 verify_ssl: bool = True, timeout: int = 30,
                 max_retries: int = 3, rate_limit_delay: float = 0.1,
                 verbose: bool = False):
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
            raise NotFoundError(
                f"Resource not found: {endpoint}",
                status_code=404,
                request_info={"endpoint": endpoint}
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
            return response.json()
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
    
    def get_paginated(self, endpoint: str, params: Optional[Dict] = None,
                     page_size: int = 100) -> Generator[Dict[str, Any], None, None]:
        """
        Get paginated results, yielding one item at a time.
        
        Args:
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
                response = self.get(endpoint, params)
            except NotFoundError:
                # No more results
                break
            
            # Handle different response formats
            if isinstance(response, list):
                items = response
            elif isinstance(response, dict):
                # Try common keys for paginated data
                items = response.get("items", response.get("data", response.get("results", [])))
                if not items and not isinstance(items, list):
                    # Might be a single item response
                    items = [response]
            else:
                break
            
            if not items:
                break
            
            for item in items:
                yield item
            
            # Check if we got fewer items than requested (end of data)
            if len(items) < page_size:
                break
            
            offset += len(items)
    
    def post_batch(self, endpoint: str, items: List[Dict[str, Any]], 
                  batch_size: int = 100) -> List[Dict[str, Any]]:
        """
        Post items in batches to avoid overwhelming the API.
        
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
            
            try:
                response = self.post(endpoint, batch)
                
                # Handle different response formats
                if isinstance(response, list):
                    responses.extend(response)
                else:
                    responses.append(response)
                    
            except APIError as e:
                # Log error but continue with next batch
                if self.verbose:
                    self.console.print(f"[red]Batch {i//batch_size + 1} failed: {e}[/red]")
                # Add None for failed items to maintain index alignment
                responses.extend([None] * len(batch))
        
        return responses
    
    def test_connection(self) -> bool:
        """
        Test the connection to the API.
        
        Returns:
            True if connection successful, False otherwise
        """
        try:
            # Try to get a simple endpoint
            self.get("/datasets", params={"limit": 1})
            return True
        except Exception as e:
            if self.verbose:
                self.console.print(f"[red]Connection test failed: {e}[/red]")
            return False
    
    def get_statistics(self) -> Dict[str, int]:
        """Get request statistics."""
        return {
            "requests": self.request_count,
            "errors": self.error_count,
            "success_rate": (self.request_count - self.error_count) / self.request_count if self.request_count > 0 else 0
        }
    
    def close(self):
        """Close the session."""
        self.session.close()