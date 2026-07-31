"""Boundary tests for the real EnhancedAPIClient request methods."""

from __future__ import annotations

import json
from unittest.mock import Mock

import pytest
import requests

from langsmith_migrator.core.api_client import (
    ConflictError,
    EnhancedAPIClient,
    NotFoundError,
    sanitize_upstream_text,
)
from langsmith_migrator.utils.retry import (
    APIError,
    AuthenticationError,
    RateLimitError,
    UpstreamRejectionError,
)

# The verbatim body a Google Cloud Armor policy returned in front of a customer's
# self-hosted LangSmith. LangSmith itself never answers in HTML.
EDGE_403_BODY = (
    '<!doctype html><meta charset="utf-8">'
    '<meta name=viewport content="width=device-width, initial-scale=1">'
    "<title>403</title>403 Forbidden"
)


def _response(
    method: str,
    url: str,
    status_code: int,
    *,
    json_body=None,
    text_body: str = "",
    headers: dict[str, str] | None = None,
) -> requests.Response:
    """Build a requests.Response object for client boundary tests."""
    response = requests.Response()
    response.status_code = status_code
    response.headers.update(headers or {})
    if json_body is not None:
        response._content = json.dumps(json_body).encode("utf-8")
        response.headers.setdefault("Content-Type", "application/json")
    else:
        response._content = text_body.encode("utf-8")
    prepared = requests.Request(method=method, url=url).prepare()
    response.request = prepared
    response.url = url
    return response


def _client() -> EnhancedAPIClient:
    return EnhancedAPIClient(
        base_url="https://langsmith.example.com/api/v1",
        headers={"X-API-Key": "test-key"},
        timeout=12,
        rate_limit_delay=0,
    )


def test_post_uses_prepared_url_payload_and_timeout(monkeypatch):
    client = _client()
    url = "https://langsmith.example.com/api/v1/orgs/current/members"
    post_mock = Mock(return_value=_response("POST", url, 201, json_body={"id": "member-1"}))
    monkeypatch.setattr(client.session, "post", post_mock)

    result = client.post("/orgs/current/members", {"email": "alice@example.com"})

    assert result == {"id": "member-1"}
    post_mock.assert_called_once_with(
        url,
        json={"email": "alice@example.com"},
        timeout=12,
    )


def test_get_uses_prepared_url_query_params_and_timeout(monkeypatch):
    client = _client()
    url = "https://langsmith.example.com/api/v1/workspaces"
    get_mock = Mock(return_value=_response("GET", url, 200, json_body=[{"id": "ws-1"}]))
    monkeypatch.setattr(client.session, "get", get_mock)

    result = client.get("/workspaces", params={"limit": 10})

    assert result == [{"id": "ws-1"}]
    get_mock.assert_called_once_with(
        url,
        params={"limit": 10},
        timeout=12,
    )


def test_set_workspace_updates_scoping_header():
    client = _client()

    client.set_workspace("ws-1")
    assert client.session.headers["X-Tenant-Id"] == "ws-1"

    client.set_workspace(None)
    assert "X-Tenant-Id" not in client.session.headers


def test_post_translates_conflict_to_conflict_error(monkeypatch):
    client = _client()
    url = "https://langsmith.example.com/api/v1/orgs/current/members"
    post_mock = Mock(
        return_value=_response("POST", url, 409, json_body={"detail": "already exists"})
    )
    monkeypatch.setattr(client.session, "post", post_mock)

    with pytest.raises(ConflictError, match="Resource conflict"):
        client.post("/orgs/current/members", {"email": "alice@example.com"})


def test_get_translates_authentication_error_without_retry(monkeypatch):
    client = _client()
    url = "https://langsmith.example.com/api/v1/orgs/current/members"
    get_mock = Mock(
        return_value=_response("GET", url, 401, json_body={"detail": "invalid API key"})
    )
    monkeypatch.setattr(client.session, "get", get_mock)

    with pytest.raises(AuthenticationError, match="Authentication failed"):
        client.get("/orgs/current/members")

    assert get_mock.call_count == 1


def test_html_bodied_403_is_an_upstream_rejection_and_is_retried(monkeypatch):
    """A 403 whose body is not JSON came from a proxy/WAF, not from LangSmith."""
    client = _client()
    url = "https://langsmith.example.com/api/v1/sessions"
    post_mock = Mock(
        return_value=_response(
            "POST", url, 403, text_body=EDGE_403_BODY, headers={"Content-Type": "text/html"}
        )
    )
    monkeypatch.setattr(client.session, "post", post_mock)
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    with pytest.raises(UpstreamRejectionError, match="came from an intermediary"):
        client.post("/sessions", {"name": "experiment-1"})

    # Retried rather than killing the caller's work item on the first refusal.
    assert post_mock.call_count == 3


@pytest.mark.parametrize(
    ("method_name", "payload"),
    [
        ("patch", {"name": "updated"}),
        ("put", {"name": "updated"}),
        ("delete", None),
    ],
)
def test_idempotent_write_methods_retry_upstream_rejections(
    monkeypatch,
    method_name,
    payload,
):
    client = _client()
    endpoint = "/resources/resource-1"
    url = f"https://langsmith.example.com/api/v1{endpoint}"
    request_method = method_name.upper()
    request_mock = Mock(
        side_effect=[
            _response(request_method, url, 403, text_body=EDGE_403_BODY),
            _response(request_method, url, 204),
        ]
    )
    monkeypatch.setattr(client.session, method_name, request_mock)
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    if payload is None:
        result = getattr(client, method_name)(endpoint)
    else:
        result = getattr(client, method_name)(endpoint, payload)

    assert result == {}
    assert request_mock.call_count == 2


def test_patch_does_not_retry_ambiguous_network_failures(monkeypatch):
    client = _client()
    patch_mock = Mock(side_effect=requests.exceptions.ConnectionError("response lost"))
    monkeypatch.setattr(client.session, "patch", patch_mock)

    with pytest.raises(requests.exceptions.ConnectionError, match="response lost"):
        client.patch("/resources/resource-1", {"name": "updated"})

    assert patch_mock.call_count == 1


def test_html_bodied_401_is_an_upstream_rejection(monkeypatch):
    client = _client()
    url = "https://langsmith.example.com/api/v1/sessions/abc"
    get_mock = Mock(return_value=_response("GET", url, 401, text_body="<html>401</html>"))
    monkeypatch.setattr(client.session, "get", get_mock)
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    with pytest.raises(UpstreamRejectionError) as excinfo:
        client.get("/sessions/abc")

    assert excinfo.value.status_code == 401
    assert get_mock.call_count == 3


def test_json_bodied_403_stays_a_permission_error_without_retry(monkeypatch):
    """A real LangSmith permission failure must still fail fast, in one request."""
    client = _client()
    url = "https://langsmith.example.com/api/v1/orgs/current/scim/tokens"
    post_mock = Mock(
        return_value=_response(
            "POST",
            url,
            403,
            json_body={"detail": "missing permission organization:manage"},
        )
    )
    monkeypatch.setattr(client.session, "post", post_mock)

    with pytest.raises(AuthenticationError, match="Access denied"):
        client.post("/orgs/current/scim/tokens", {})

    assert post_mock.call_count == 1


def test_upstream_rejection_message_is_sanitized(monkeypatch):
    """The borrowed body is untrusted, so it must not carry escape sequences through."""
    client = _client()
    url = "https://langsmith.example.com/api/v1/sessions"
    hostile = "<html>\x1b[31mdenied\x1b[0m\n\n\tby   policy\x00</html>"
    post_mock = Mock(return_value=_response("POST", url, 403, text_body=hostile))
    monkeypatch.setattr(client.session, "post", post_mock)
    monkeypatch.setattr("time.sleep", lambda _seconds: None)

    with pytest.raises(UpstreamRejectionError) as excinfo:
        client.post("/sessions", {})

    message = str(excinfo.value)
    assert "\x1b" not in message
    assert "\x00" not in message
    assert "<html>[31mdenied[0m by policy</html>" in message


def test_sanitize_upstream_text_truncates_and_handles_empty():
    assert sanitize_upstream_text("") == "no response body"
    assert sanitize_upstream_text(None) == "no response body"
    assert sanitize_upstream_text("\x00\x01") == "no response body"
    long_body = "a" * 500
    sanitized = sanitize_upstream_text(long_body, limit=200)
    assert sanitized == "a" * 200 + "..."


def test_invalid_json_response_carries_status_code(monkeypatch):
    """Without a status code the retry layer treats even a 5xx as terminal."""
    client = _client()
    url = "https://langsmith.example.com/api/v1/sessions"
    get_mock = Mock(return_value=_response("GET", url, 200, text_body="not-json"))
    monkeypatch.setattr(client.session, "get", get_mock)

    with pytest.raises(APIError) as excinfo:
        client.get("/sessions")

    assert excinfo.value.status_code == 200


def test_patch_uses_fixed_timeout_and_handles_no_content(monkeypatch):
    client = _client()
    url = "https://langsmith.example.com/api/v1/workspaces/current/members/member-1"
    patch_mock = Mock(return_value=_response("PATCH", url, 204))
    monkeypatch.setattr(client.session, "patch", patch_mock)

    result = client.patch("/workspaces/current/members/member-1", {"role_id": "role-1"})

    assert result == {}
    patch_mock.assert_called_once_with(
        url,
        json={"role_id": "role-1"},
        timeout=15,
    )


def test_delete_uses_fixed_timeout_and_handles_no_content(monkeypatch):
    client = _client()
    url = "https://langsmith.example.com/api/v1/workspaces/current/members/member-1"
    delete_mock = Mock(return_value=_response("DELETE", url, 204))
    monkeypatch.setattr(client.session, "delete", delete_mock)

    result = client.delete("/workspaces/current/members/member-1")

    assert result == {}
    delete_mock.assert_called_once_with(url, timeout=15)


def test_delete_translates_not_found_to_not_found_error(monkeypatch):
    client = _client()
    url = "https://langsmith.example.com/api/v1/orgs/current/members/member-1"
    delete_mock = Mock(return_value=_response("DELETE", url, 404, json_body={"detail": "missing"}))
    monkeypatch.setattr(client.session, "delete", delete_mock)

    with pytest.raises(NotFoundError, match="Resource not found"):
        client.delete("/orgs/current/members/member-1")


def test_handle_response_surfaces_retry_after_on_rate_limit():
    client = _client()
    url = "https://langsmith.example.com/api/v1/orgs/current/members"
    response = _response(
        "GET",
        url,
        429,
        json_body={"detail": "too many requests"},
        headers={"Retry-After": "7"},
    )

    with pytest.raises(RateLimitError, match="Rate limit exceeded") as exc_info:
        client._handle_response(response, "/orgs/current/members")

    assert exc_info.value.retry_after == 7.0


def test_get_raises_api_error_on_invalid_json_success_response(monkeypatch):
    client = _client()
    url = "https://langsmith.example.com/api/v1/workspaces"
    get_mock = Mock(return_value=_response("GET", url, 200, text_body="not-json"))
    monkeypatch.setattr(client.session, "get", get_mock)

    with pytest.raises(APIError, match="Invalid JSON response"):
        client.get("/workspaces")

    assert get_mock.call_count == 1


def test_prepare_url_root_relative_bypasses_api_v1():
    """Endpoints starting with /v1/ should resolve to the host root, not /api/v1."""
    client = _client()
    url = client._prepare_url("/v1/fleet/agents")
    assert url == "https://langsmith.example.com/v1/fleet/agents"


def test_prepare_url_normal_endpoint_uses_api_v1_base():
    """Normal endpoints should still append to the /api/v1 base URL."""
    client = _client()
    url = client._prepare_url("/datasets")
    assert url == "https://langsmith.example.com/api/v1/datasets"


def test_prepare_url_absolute_url_passthrough():
    """Absolute URLs should be used as-is."""
    client = _client()
    url = client._prepare_url("https://other.example.com/api/v1/foo")
    assert url == "https://other.example.com/api/v1/foo"

