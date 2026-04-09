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
    post_mock = Mock(
        return_value=_response("POST", url, 201, json_body={"id": "member-1"})
    )
    monkeypatch.setattr(client.session, "post", post_mock)

    result = client.post("/orgs/current/members", {"email": "alice@example.com"})

    assert result == {"id": "member-1"}
    post_mock.assert_called_once_with(
        url,
        json={"email": "alice@example.com"},
        timeout=12,
    )


def test_post_translates_conflict_to_conflict_error(monkeypatch):
    client = _client()
    url = "https://langsmith.example.com/api/v1/orgs/current/members"
    post_mock = Mock(
        return_value=_response("POST", url, 409, json_body={"detail": "already exists"})
    )
    monkeypatch.setattr(client.session, "post", post_mock)

    with pytest.raises(ConflictError, match="Resource conflict"):
        client.post("/orgs/current/members", {"email": "alice@example.com"})


def test_patch_uses_fixed_timeout_and_handles_no_content(monkeypatch):
    client = _client()
    url = "https://langsmith.example.com/api/v1/tenants/current/members/member-1"
    patch_mock = Mock(return_value=_response("PATCH", url, 204))
    monkeypatch.setattr(client.session, "patch", patch_mock)

    result = client.patch("/tenants/current/members/member-1", {"role_id": "role-1"})

    assert result == {}
    patch_mock.assert_called_once_with(
        url,
        json={"role_id": "role-1"},
        timeout=15,
    )


def test_delete_uses_fixed_timeout_and_handles_no_content(monkeypatch):
    client = _client()
    url = "https://langsmith.example.com/api/v1/tenants/current/members/member-1"
    delete_mock = Mock(return_value=_response("DELETE", url, 204))
    monkeypatch.setattr(client.session, "delete", delete_mock)

    result = client.delete("/tenants/current/members/member-1")

    assert result == {}
    delete_mock.assert_called_once_with(url, timeout=15)


def test_delete_translates_not_found_to_not_found_error(monkeypatch):
    client = _client()
    url = "https://langsmith.example.com/api/v1/orgs/current/members/member-1"
    delete_mock = Mock(
        return_value=_response("DELETE", url, 404, json_body={"detail": "missing"})
    )
    monkeypatch.setattr(client.session, "delete", delete_mock)

    with pytest.raises(NotFoundError, match="Resource not found"):
        client.delete("/orgs/current/members/member-1")
