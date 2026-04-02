"""Tests for EnhancedAPIClient."""

import pytest
import requests
from unittest.mock import Mock, patch, MagicMock

from langsmith_migrator.core.api_client import (
    EnhancedAPIClient,
    NotFoundError,
    BatchResult,
    BatchItemResult,
)
from langsmith_migrator.utils.retry import (
    APIError,
    AuthenticationError,
    ConflictError,
    RateLimitError,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_response(status_code=200, json_data=None, text="", headers=None):
    """Create a mock requests.Response with the given properties."""
    resp = Mock(spec=requests.Response)
    resp.status_code = status_code
    resp.ok = 200 <= status_code < 400
    resp.text = text
    resp.headers = headers or {}
    resp.request = Mock()
    resp.request.method = "GET"
    resp.request.url = "https://api.test.com/api/v1/test"
    resp.request.body = None

    if json_data is not None:
        resp.json.return_value = json_data
    elif status_code >= 400:
        # For error responses, default json() to return a dict with detail
        resp.json.return_value = {"detail": text or "error"}
    else:
        # For 2xx with no json_data, json() returns None by default
        resp.json.return_value = None

    return resp


def make_client(**kwargs) -> EnhancedAPIClient:
    """Create an EnhancedAPIClient with safe defaults for testing."""
    defaults = dict(
        base_url="https://api.test.com/api/v1",
        headers={"X-API-Key": "test-key"},
        verify_ssl=False,
        timeout=5,
        max_retries=1,
        rate_limit_delay=0,  # no sleeps in tests
        verbose=False,
    )
    defaults.update(kwargs)
    return EnhancedAPIClient(**defaults)


# ===========================================================================
# _handle_response tests
# ===========================================================================

class TestHandleResponse:
    """Tests for _handle_response — status code → exception mapping."""

    def setup_method(self):
        self.client = make_client()

    # --- Error status codes ------------------------------------------------

    def test_401_raises_authentication_error(self):
        resp = make_response(401, json_data={"detail": "Invalid API key"})
        with pytest.raises(AuthenticationError, match="Authentication failed") as exc_info:
            self.client._handle_response(resp, "/test")
        assert exc_info.value.status_code == 401

    def test_403_raises_authentication_error(self):
        resp = make_response(403, json_data={"detail": "Forbidden"})
        with pytest.raises(AuthenticationError, match="Access denied") as exc_info:
            self.client._handle_response(resp, "/test")
        assert exc_info.value.status_code == 403

    def test_404_raises_not_found_error(self):
        resp = make_response(404, json_data={"detail": "Not found"})
        with pytest.raises(NotFoundError, match="Resource not found"):
            self.client._handle_response(resp, "/test")

    def test_409_raises_conflict_error(self):
        resp = make_response(409, json_data={"detail": "Already exists"})
        with pytest.raises(ConflictError, match="Resource conflict"):
            self.client._handle_response(resp, "/test")

    def test_429_raises_rate_limit_error_without_retry_after(self):
        resp = make_response(429)
        with pytest.raises(RateLimitError, match="Rate limit exceeded") as exc_info:
            self.client._handle_response(resp, "/test")
        assert exc_info.value.retry_after is None

    def test_429_raises_rate_limit_error_with_retry_after(self):
        resp = make_response(429, headers={"Retry-After": "30"})
        with pytest.raises(RateLimitError) as exc_info:
            self.client._handle_response(resp, "/test")
        assert exc_info.value.retry_after == 30.0

    def test_429_invalid_retry_after_is_ignored(self):
        resp = make_response(429, headers={"Retry-After": "not-a-number"})
        with pytest.raises(RateLimitError) as exc_info:
            self.client._handle_response(resp, "/test")
        assert exc_info.value.retry_after is None

    def test_500_raises_api_error(self):
        resp = make_response(500, json_data={"detail": "Internal server error"})
        with pytest.raises(APIError, match="API request failed"):
            self.client._handle_response(resp, "/test")

    def test_502_raises_api_error(self):
        resp = make_response(502, json_data={"detail": "Bad gateway"})
        with pytest.raises(APIError, match="API request failed"):
            self.client._handle_response(resp, "/test")

    def test_503_raises_api_error(self):
        resp = make_response(503, json_data={"detail": "Service unavailable"})
        with pytest.raises(APIError, match="API request failed"):
            self.client._handle_response(resp, "/test")

    # --- Error detail extraction formats -----------------------------------

    def test_error_detail_from_detail_field(self):
        resp = make_response(500, json_data={"detail": "Something broke"})
        with pytest.raises(APIError, match="Something broke"):
            self.client._handle_response(resp, "/test")

    def test_error_detail_from_message_field(self):
        resp = make_response(500, json_data={"message": "Internal error"})
        with pytest.raises(APIError, match="Internal error"):
            self.client._handle_response(resp, "/test")

    def test_error_detail_from_error_field(self):
        resp = make_response(500, json_data={"error": "Server error"})
        with pytest.raises(APIError, match="Server error"):
            self.client._handle_response(resp, "/test")

    def test_error_detail_validation_errors_list(self):
        """When 'detail' is a list of validation errors, they get formatted."""
        detail = [
            {"loc": ["body", "name"], "msg": "field required", "type": "value_error"},
            {"loc": ["body", "age"], "msg": "not a valid integer", "type": "type_error"},
        ]
        resp = make_response(422, json_data={"detail": detail})
        # 422 falls through to the generic "not response.ok" branch
        with pytest.raises(APIError):
            self.client._handle_response(resp, "/test")

    def test_error_detail_falls_back_to_text(self):
        """When json() raises ValueError, response.text is used."""
        resp = make_response(500, text="plain text error")
        resp.json.side_effect = ValueError("No JSON")
        with pytest.raises(APIError, match="plain text error"):
            self.client._handle_response(resp, "/test")

    # --- Success responses -------------------------------------------------

    def test_200_with_valid_json_dict(self):
        resp = make_response(200, json_data={"id": "abc", "name": "test"})
        result = self.client._handle_response(resp, "/test")
        assert result == {"id": "abc", "name": "test"}

    def test_200_with_valid_json_list(self):
        resp = make_response(200, json_data=[{"id": "1"}, {"id": "2"}])
        result = self.client._handle_response(resp, "/test")
        assert result == [{"id": "1"}, {"id": "2"}]

    def test_200_with_null_json_returns_empty_dict(self):
        resp = make_response(200, json_data=None)
        # json() returns None by default in our helper
        result = self.client._handle_response(resp, "/test")
        assert result == {}

    def test_200_with_invalid_json_raises_api_error(self):
        resp = make_response(200)
        resp.json.side_effect = ValueError("Expecting value")
        with pytest.raises(APIError, match="Invalid JSON response"):
            self.client._handle_response(resp, "/test")

    # --- Counter tracking --------------------------------------------------

    def test_request_count_incremented_on_success(self):
        resp = make_response(200, json_data={"ok": True})
        self.client._handle_response(resp, "/test")
        assert self.client.request_count == 1

    def test_error_count_incremented_on_401(self):
        resp = make_response(401, json_data={"detail": "bad key"})
        with pytest.raises(AuthenticationError):
            self.client._handle_response(resp, "/test")
        assert self.client.error_count == 1

    def test_error_count_incremented_on_500(self):
        resp = make_response(500, json_data={"detail": "oops"})
        with pytest.raises(APIError):
            self.client._handle_response(resp, "/test")
        assert self.client.error_count == 1


# ===========================================================================
# HTTP method tests (get, post, patch)
# ===========================================================================

class TestHTTPMethods:
    """Tests for get(), post(), patch() methods."""

    def setup_method(self):
        self.client = make_client()

    def test_get_returns_parsed_json(self):
        mock_resp = make_response(200, json_data={"items": [1, 2, 3]})
        self.client.session.get = Mock(return_value=mock_resp)

        result = self.client.get("/datasets")
        assert result == {"items": [1, 2, 3]}
        self.client.session.get.assert_called_once_with(
            "https://api.test.com/api/v1/datasets",
            params=None,
            timeout=5,
        )

    def test_get_with_params(self):
        mock_resp = make_response(200, json_data=[])
        self.client.session.get = Mock(return_value=mock_resp)

        self.client.get("/datasets", params={"limit": 10})
        self.client.session.get.assert_called_once_with(
            "https://api.test.com/api/v1/datasets",
            params={"limit": 10},
            timeout=5,
        )

    def test_post_returns_parsed_json(self):
        mock_resp = make_response(200, json_data={"id": "new-123"})
        self.client.session.post = Mock(return_value=mock_resp)

        result = self.client.post("/datasets", {"name": "test"})
        assert result == {"id": "new-123"}
        self.client.session.post.assert_called_once_with(
            "https://api.test.com/api/v1/datasets",
            json={"name": "test"},
            timeout=5,
        )

    def test_patch_returns_parsed_json(self):
        mock_resp = make_response(200, json_data={"id": "123", "name": "updated"})
        self.client.session.patch = Mock(return_value=mock_resp)

        result = self.client.patch("/datasets/123", {"name": "updated"})
        assert result == {"id": "123", "name": "updated"}
        # patch uses a hardcoded timeout=15
        self.client.session.patch.assert_called_once_with(
            "https://api.test.com/api/v1/datasets/123",
            json={"name": "updated"},
            timeout=15,
        )


# ===========================================================================
# URL preparation tests
# ===========================================================================

class TestPrepareURL:
    """Tests for _prepare_url — relative vs absolute URL handling."""

    def setup_method(self):
        self.client = make_client()

    def test_relative_endpoint(self):
        assert self.client._prepare_url("/datasets") == "https://api.test.com/api/v1/datasets"

    def test_absolute_url_passthrough(self):
        url = "https://other.host.com/api/v1/things"
        assert self.client._prepare_url(url) == url

    def test_base_url_trailing_slash_stripped(self):
        client = make_client(base_url="https://api.test.com/api/v1/")
        assert client._prepare_url("/datasets") == "https://api.test.com/api/v1/datasets"


# ===========================================================================
# Workspace scoping tests
# ===========================================================================

class TestWorkspaceScoping:
    """Tests for set_workspace — X-Tenant-Id header management."""

    def test_set_workspace_adds_header(self):
        client = make_client()
        client.set_workspace("ws-123")
        assert client.session.headers["X-Tenant-Id"] == "ws-123"

    def test_set_workspace_none_removes_header(self):
        client = make_client(workspace_id="ws-old")
        assert client.session.headers["X-Tenant-Id"] == "ws-old"
        client.set_workspace(None)
        assert "X-Tenant-Id" not in client.session.headers

    def test_set_workspace_none_safe_when_header_absent(self):
        client = make_client()
        # Should not raise even if header was never set
        client.set_workspace(None)
        assert "X-Tenant-Id" not in client.session.headers

    def test_workspace_id_in_constructor(self):
        client = make_client(workspace_id="ws-init")
        assert client.session.headers["X-Tenant-Id"] == "ws-init"

    def test_set_workspace_overwrites_previous(self):
        client = make_client(workspace_id="ws-1")
        client.set_workspace("ws-2")
        assert client.session.headers["X-Tenant-Id"] == "ws-2"


# ===========================================================================
# Batch operation tests
# ===========================================================================

class TestPostBatch:
    """Tests for post_batch and the recursive binary-split logic."""

    def setup_method(self):
        self.client = make_client()

    def test_successful_batch(self):
        """All items succeed in a single batch post."""
        items = [{"name": "a"}, {"name": "b"}, {"name": "c"}]
        response_list = [{"id": "1"}, {"id": "2"}, {"id": "3"}]

        mock_resp = make_response(200, json_data=response_list)
        self.client.session.post = Mock(return_value=mock_resp)

        results = self.client.post_batch("/datasets", items, batch_size=10)
        assert results == [{"id": "1"}, {"id": "2"}, {"id": "3"}]

    def test_batch_single_failure_isolated_via_splitting(self):
        """When a batch of 2 fails, splitting isolates the bad item.

        We mock client.post (not session.post) to bypass the retry decorator
        and test the binary-split logic directly.
        """
        items = [{"name": "good"}, {"name": "bad"}]

        call_count = 0
        original_post = self.client.post

        def mock_post(endpoint, data):
            nonlocal call_count
            call_count += 1

            # First call: full batch of 2 → APIError
            if call_count == 1:
                raise APIError("batch error", status_code=500)

            # Second call: first item alone → success
            if call_count == 2:
                return [{"id": "good-1"}]

            # Third call: second item alone → APIError
            raise APIError("bad item", status_code=500)

        self.client.post = mock_post
        results = self.client.post_batch("/datasets", items, batch_size=10)

        # First item should succeed, second should be None (failed)
        assert len(results) == 2
        assert results[0] is not None
        assert results[1] is None

    def test_batch_conflict_triggers_splitting(self):
        """A 409 Conflict in a batch triggers binary splitting.

        We mock client.post to bypass the retry decorator.
        """
        items = [{"name": "exists"}, {"name": "new"}]
        call_count = 0

        def mock_post(endpoint, data):
            nonlocal call_count
            call_count += 1

            if call_count == 1:
                # Full batch gets a 409
                raise ConflictError("Already exists")

            if call_count == 2:
                # First item alone — conflict
                raise ConflictError("Already exists")

            # Second item succeeds
            return [{"id": "new-1"}]

        self.client.post = mock_post
        results = self.client.post_batch("/datasets", items, batch_size=10)

        # First item failed (conflict), second succeeded
        assert results[0] is None
        assert results[1] is not None

    def test_batch_all_succeed_with_dict_response(self):
        """When the API returns a single dict for the whole batch, all items get that response."""
        items = [{"name": "a"}, {"name": "b"}]
        mock_resp = make_response(200, json_data={"status": "ok"})
        self.client.session.post = Mock(return_value=mock_resp)

        results = self.client.post_batch("/datasets", items, batch_size=10)
        # Both items get the same dict response
        assert len(results) == 2
        assert all(r == {"status": "ok"} for r in results)

    def test_batch_empty_items(self):
        """Posting an empty list returns an empty list."""
        results = self.client.post_batch("/datasets", [], batch_size=10)
        assert results == []


# ===========================================================================
# BatchResult data structure tests
# ===========================================================================

class TestBatchResult:
    """Tests for the BatchResult helper class."""

    def test_empty_batch_result(self):
        br = BatchResult()
        assert br.success_count == 0
        assert br.failure_count == 0
        assert br.all_succeeded is True
        assert br.get_responses() == []

    def test_all_successes(self):
        br = BatchResult()
        br.add_success({"id": "1"}, 0)
        br.add_success({"id": "2"}, 1)
        assert br.success_count == 2
        assert br.failure_count == 0
        assert br.all_succeeded is True

    def test_mixed_results(self):
        br = BatchResult()
        br.add_success({"id": "1"}, 0)
        br.add_failure("oops", 1)
        br.add_success({"id": "3"}, 2)
        assert br.success_count == 2
        assert br.failure_count == 1
        assert br.all_succeeded is False

    def test_get_responses_ordered(self):
        br = BatchResult()
        # Add out of order
        br.add_success({"id": "2"}, 1)
        br.add_failure("err", 0)
        br.add_success({"id": "3"}, 2)
        responses = br.get_responses()
        assert responses == [None, {"id": "2"}, {"id": "3"}]


# ===========================================================================
# Connection testing
# ===========================================================================

class TestTestConnection:
    """Tests for test_connection()."""

    def test_connection_success(self):
        client = make_client()
        mock_resp = make_response(200, json_data=[])
        client.session.get = Mock(return_value=mock_resp)

        success, error = client.test_connection()
        assert success is True
        assert error is None

    def test_connection_auth_failure(self):
        client = make_client()
        mock_resp = make_response(401, json_data={"detail": "Invalid key"})
        client.session.get = Mock(return_value=mock_resp)

        success, error = client.test_connection()
        assert success is False
        assert "Authentication failed" in error

    def test_connection_network_error(self):
        client = make_client()
        client.session.get = Mock(side_effect=requests.exceptions.ConnectionError("refused"))

        success, error = client.test_connection()
        assert success is False
        assert "Connection failed" in error

    def test_connection_all_endpoints_404(self):
        """When every endpoint returns 404, connection test fails."""
        client = make_client()
        mock_resp = make_response(404, json_data={"detail": "Not found"})
        client.session.get = Mock(return_value=mock_resp)

        success, error = client.test_connection()
        assert success is False
        assert error is not None

    def test_connection_first_endpoint_404_second_succeeds(self):
        """Falls through 404s and succeeds on a later endpoint."""
        client = make_client()
        call_count = 0

        def mock_get(url, params=None, timeout=None):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return make_response(404, json_data={"detail": "Not found"})
            return make_response(200, json_data={"ok": True})

        client.session.get = mock_get
        success, error = client.test_connection()
        assert success is True
        assert error is None

    def test_connection_timeout(self):
        client = make_client()
        client.session.get = Mock(side_effect=requests.exceptions.Timeout("timed out"))

        success, error = client.test_connection()
        assert success is False
        assert "Connection failed" in error


# ===========================================================================
# Statistics
# ===========================================================================

class TestStatistics:
    """Tests for get_statistics()."""

    def test_initial_stats(self):
        client = make_client()
        stats = client.get_statistics()
        assert stats == {"requests": 0, "errors": 0, "success_rate": 0}

    def test_stats_after_successful_request(self):
        client = make_client()
        resp = make_response(200, json_data={"ok": True})
        client._handle_response(resp, "/test")
        stats = client.get_statistics()
        assert stats["requests"] == 1
        assert stats["errors"] == 0
        assert stats["success_rate"] == 1.0

    def test_stats_after_error(self):
        client = make_client()
        resp = make_response(500, json_data={"detail": "error"})
        with pytest.raises(APIError):
            client._handle_response(resp, "/test")
        stats = client.get_statistics()
        assert stats["requests"] == 1
        assert stats["errors"] == 1
        assert stats["success_rate"] == 0.0


# ===========================================================================
# Close / cleanup
# ===========================================================================

class TestClose:
    """Tests for close()."""

    def test_close_closes_session(self):
        client = make_client()
        client.session.close = Mock()
        client.close()
        client.session.close.assert_called_once()
