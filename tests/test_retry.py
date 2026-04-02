"""Tests for retry_on_failure decorator and request_with_retry function."""

import socket
from unittest.mock import patch, MagicMock, Mock
import requests
import requests.exceptions
import pytest

from langsmith_migrator.utils.retry import (
    retry_on_failure,
    request_with_retry,
    _sanitize_url,
    APIError,
    AuthenticationError,
    ConflictError,
    RateLimitError,
    MAX_BACKOFF_SECONDS,
)


class TestRetryOnFailure:
    """Tests for the retry_on_failure decorator."""

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_succeeds_first_try(self, mock_sleep):
        @retry_on_failure(max_retries=3)
        def fn():
            return "ok"

        assert fn() == "ok"
        mock_sleep.assert_not_called()

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_retries_rate_limit_then_succeeds(self, mock_sleep):
        call_count = 0

        @retry_on_failure(max_retries=3)
        def fn():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise RateLimitError("rate limited")
            return "ok"

        assert fn() == "ok"
        assert call_count == 3
        assert mock_sleep.call_count == 2

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_rate_limit_respects_retry_after(self, mock_sleep):
        @retry_on_failure(max_retries=2, delay=1.0)
        def fn():
            raise RateLimitError("rate limited", retry_after=5.0)

        with pytest.raises(RateLimitError):
            fn()

        for call in mock_sleep.call_args_list:
            wait_time = call[0][0]
            assert wait_time == 5.0

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_rate_limit_without_retry_after_uses_current_delay(self, mock_sleep):
        @retry_on_failure(max_retries=2, delay=1.0, backoff=2.0)
        def fn():
            raise RateLimitError("rate limited")

        with pytest.raises(RateLimitError):
            fn()

        first_wait = mock_sleep.call_args_list[0][0][0]
        assert first_wait == 1.0

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_backoff_capped_at_max(self, mock_sleep):
        @retry_on_failure(max_retries=5, delay=50.0, backoff=3.0)
        def fn():
            raise RateLimitError("rate limited")

        with pytest.raises(RateLimitError):
            fn()

        for call in mock_sleep.call_args_list:
            wait_time = call[0][0]
            assert wait_time <= MAX_BACKOFF_SECONDS

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_auth_error_not_retried(self, mock_sleep):
        call_count = 0

        @retry_on_failure(max_retries=3)
        def fn():
            nonlocal call_count
            call_count += 1
            raise AuthenticationError("bad key", status_code=401)

        with pytest.raises(AuthenticationError):
            fn()

        assert call_count == 1
        mock_sleep.assert_not_called()

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_conflict_error_not_retried(self, mock_sleep):
        call_count = 0

        @retry_on_failure(max_retries=3)
        def fn():
            nonlocal call_count
            call_count += 1
            raise ConflictError("conflict")

        with pytest.raises(ConflictError):
            fn()

        assert call_count == 1
        mock_sleep.assert_not_called()

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_server_error_retried(self, mock_sleep):
        call_count = 0

        @retry_on_failure(max_retries=3)
        def fn():
            nonlocal call_count
            call_count += 1
            raise APIError("server error", status_code=500)

        with pytest.raises(APIError):
            fn()

        assert call_count == 3
        assert mock_sleep.call_count == 2

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_client_error_not_retried(self, mock_sleep):
        call_count = 0

        @retry_on_failure(max_retries=3)
        def fn():
            nonlocal call_count
            call_count += 1
            raise APIError("bad request", status_code=400)

        with pytest.raises(APIError):
            fn()

        assert call_count == 1
        mock_sleep.assert_not_called()

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_network_errors_retried(self, mock_sleep):
        for exc_class in [
            requests.exceptions.ConnectionError,
            requests.exceptions.Timeout,
            requests.exceptions.ReadTimeout,
            socket.timeout,
        ]:
            call_count = 0

            @retry_on_failure(max_retries=2)
            def fn():
                nonlocal call_count
                call_count += 1
                raise exc_class("network error")

            with pytest.raises(exc_class):
                fn()

            assert call_count == 2

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_max_retries_zero_calls_once(self, mock_sleep):
        call_count = 0

        @retry_on_failure(max_retries=0)
        def fn():
            nonlocal call_count
            call_count += 1
            raise APIError("server error", status_code=500)

        with pytest.raises(APIError):
            fn()

        assert call_count == 1
        mock_sleep.assert_not_called()

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_max_retries_zero_succeeds(self, mock_sleep):
        @retry_on_failure(max_retries=0)
        def fn():
            return "ok"

        assert fn() == "ok"

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_server_error_then_success(self, mock_sleep):
        call_count = 0

        @retry_on_failure(max_retries=3)
        def fn():
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise APIError("server error", status_code=502)
            return "recovered"

        assert fn() == "recovered"
        assert call_count == 2
        assert mock_sleep.call_count == 1


# ---------------------------------------------------------------------------
# Helpers for request_with_retry tests
# ---------------------------------------------------------------------------


def make_response(status_code=200, headers=None):
    """Create a mock requests.Response with the given status code."""
    resp = Mock(spec=requests.Response)
    resp.status_code = status_code
    resp.ok = 200 <= status_code < 400
    resp.headers = headers or {}
    resp.raise_for_status = Mock()
    if status_code >= 400:
        resp.raise_for_status.side_effect = requests.exceptions.HTTPError(
            response=resp
        )
    return resp


class TestRequestWithRetry:
    """Tests for the request_with_retry function."""

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_success_on_first_try(self, mock_sleep):
        session = Mock(spec=requests.Session)
        resp_200 = make_response(200)
        session.request.return_value = resp_200

        result = request_with_retry(session, "GET", "https://example.com/api")

        assert result is resp_200
        session.request.assert_called_once_with("GET", "https://example.com/api")
        mock_sleep.assert_not_called()

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_retries_on_429_then_succeeds(self, mock_sleep):
        session = Mock(spec=requests.Session)
        resp_429 = make_response(429)
        resp_200 = make_response(200)
        session.request.side_effect = [resp_429, resp_200]

        result = request_with_retry(
            session, "GET", "https://example.com/api", max_retries=3
        )

        assert result is resp_200
        assert session.request.call_count == 2
        assert mock_sleep.call_count == 1

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_retries_on_429_with_retry_after_header(self, mock_sleep):
        session = Mock(spec=requests.Session)
        resp_429 = make_response(429, headers={"Retry-After": "7"})
        resp_200 = make_response(200)
        session.request.side_effect = [resp_429, resp_200]

        request_with_retry(
            session, "POST", "https://example.com/api", max_retries=3, delay=1.0
        )

        # The wait time should be the Retry-After value (7), not the default delay (1)
        wait_time = mock_sleep.call_args_list[0][0][0]
        assert wait_time == 7.0

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_retries_on_500_then_succeeds(self, mock_sleep):
        session = Mock(spec=requests.Session)
        resp_500 = make_response(500)
        resp_200 = make_response(200)
        session.request.side_effect = [resp_500, resp_200]

        result = request_with_retry(
            session, "GET", "https://example.com/api", max_retries=3
        )

        assert result is resp_200
        assert session.request.call_count == 2
        assert mock_sleep.call_count == 1

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_does_not_retry_on_400(self, mock_sleep):
        session = Mock(spec=requests.Session)
        resp_400 = make_response(400)
        session.request.return_value = resp_400

        with pytest.raises(requests.exceptions.HTTPError):
            request_with_retry(
                session, "POST", "https://example.com/api", max_retries=3
            )

        session.request.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_does_not_retry_on_401(self, mock_sleep):
        session = Mock(spec=requests.Session)
        resp_401 = make_response(401)
        session.request.return_value = resp_401

        with pytest.raises(requests.exceptions.HTTPError):
            request_with_retry(
                session, "GET", "https://example.com/api", max_retries=3
            )

        session.request.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_retries_on_connection_error(self, mock_sleep):
        session = Mock(spec=requests.Session)
        resp_200 = make_response(200)
        session.request.side_effect = [
            requests.exceptions.ConnectionError("connection refused"),
            resp_200,
        ]

        result = request_with_retry(
            session, "GET", "https://example.com/api", max_retries=3
        )

        assert result is resp_200
        assert session.request.call_count == 2
        assert mock_sleep.call_count == 1

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_max_retries_zero_calls_once(self, mock_sleep):
        session = Mock(spec=requests.Session)
        resp_500 = make_response(500)
        session.request.return_value = resp_500

        with pytest.raises(requests.exceptions.HTTPError):
            request_with_retry(
                session, "GET", "https://example.com/api", max_retries=0
            )

        session.request.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_all_retries_exhausted_raises_last_exception(self, mock_sleep):
        session = Mock(spec=requests.Session)
        session.request.side_effect = requests.exceptions.ConnectionError("down")

        with pytest.raises(requests.exceptions.ConnectionError, match="down"):
            request_with_retry(
                session, "GET", "https://example.com/api", max_retries=3
            )

        assert session.request.call_count == 3
        assert mock_sleep.call_count == 2

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_429_sleeps_even_on_last_attempt(self, mock_sleep):
        """Verify that 429 always sleeps, even on the final attempt (before raising)."""
        session = Mock()
        resp_429 = make_response(429)
        session.request.return_value = resp_429

        with pytest.raises(requests.exceptions.HTTPError):
            request_with_retry(session, "GET", "http://test.com", max_retries=1)

        # Should have slept once (on the only attempt), then raised
        assert mock_sleep.call_count == 1

    @patch("langsmith_migrator.utils.retry._jittered_sleep")
    def test_kwargs_forwarded_to_session_request(self, mock_sleep):
        session = Mock(spec=requests.Session)
        resp_200 = make_response(200)
        session.request.return_value = resp_200

        request_with_retry(
            session,
            "POST",
            "https://example.com/api",
            headers={"Authorization": "Bearer tok"},
            timeout=30,
            json={"key": "value"},
        )

        session.request.assert_called_once_with(
            "POST",
            "https://example.com/api",
            headers={"Authorization": "Bearer tok"},
            timeout=30,
            json={"key": "value"},
        )


class TestAPIErrorSanitization:
    """Tests for URL sanitization in APIError.__str__."""

    def test_url_with_query_params_is_redacted(self):
        err = APIError(
            "failed",
            status_code=500,
            request_info={
                "url": "https://api.example.com/v1/runs?api_key=secret123&other=val",
                "method": "GET",
            },
        )
        result = str(err)
        assert "secret123" not in result
        assert "other=val" not in result
        assert "<redacted>" in result
        assert "https://api.example.com/v1/runs" in result
        assert "method" in result

    def test_url_without_query_params_preserved(self):
        err = APIError(
            "failed",
            status_code=500,
            request_info={
                "url": "https://api.example.com/v1/runs",
                "method": "POST",
            },
        )
        result = str(err)
        assert "https://api.example.com/v1/runs" in result
        assert "<redacted>" not in result

    def test_no_request_info_works_normally(self):
        err = APIError("something went wrong", status_code=502)
        result = str(err)
        assert result == "something went wrong"
        assert "Request Info" not in result

    def test_request_info_without_url_key(self):
        err = APIError(
            "failed",
            status_code=500,
            request_info={"method": "GET", "endpoint": "/v1/runs"},
        )
        result = str(err)
        assert "method" in result
        assert "endpoint" in result
        assert "<redacted>" not in result

    def test_sanitize_url_helper_with_query(self):
        assert _sanitize_url("https://host.com/path?key=abc") == "https://host.com/path?<redacted>"

    def test_sanitize_url_helper_without_query(self):
        assert _sanitize_url("https://host.com/path") == "https://host.com/path"
