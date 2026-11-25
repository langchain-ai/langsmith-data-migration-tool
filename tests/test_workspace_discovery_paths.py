"""Workspace discovery builds paths from the base URL, once."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from langsmith_migrator.core.api_client import APIError, NotFoundError
from langsmith_migrator.utils.workspace import create_workspace, discover_workspaces


def _client(base_url: str) -> Mock:
    client = Mock()
    client.base_url = base_url
    return client


@pytest.mark.parametrize(
    "base_url,expected",
    [
        ("https://byoc.example.com/api/v1", "/workspaces"),
        ("https://self-hosted.example.com", "/api/v1/workspaces"),
    ],
)
def test_discovery_never_doubles_the_api_prefix(base_url, expected):
    client = _client(base_url)
    client.get.return_value = [{"id": "ws-1"}]

    result = discover_workspaces(client)

    assert result["workspaces"] == [{"id": "ws-1"}]
    client.get.assert_called_once_with(expected)


def test_discovery_falls_through_405_to_org_endpoint():
    client = _client("https://byoc.example.com/api/v1")
    client.get.side_effect = [
        APIError("Method Not Allowed", status_code=405),
        [{"id": "ws-1"}],
    ]

    result = discover_workspaces(client)

    assert result["workspaces"] == [{"id": "ws-1"}]
    assert [c.args[0] for c in client.get.call_args_list] == [
        "/workspaces",
        "/orgs/current/workspaces",
    ]
    assert all("/api/v1/api/v1" not in c.args[0] for c in client.get.call_args_list)


def test_create_workspace_uses_one_prefixed_path():
    client = _client("https://byoc.example.com/api/v1")
    client.post.return_value = {"id": "ws-new"}

    assert create_workspace(client, "New WS") == {"id": "ws-new"}
    client.post.assert_called_once_with("/workspaces", {"display_name": "New WS"})


def test_create_workspace_reports_missing_route():
    client = _client("https://byoc.example.com/api/v1")
    client.post.side_effect = NotFoundError("nope", status_code=404)

    with pytest.raises(NotFoundError):
        create_workspace(client, "New WS")
