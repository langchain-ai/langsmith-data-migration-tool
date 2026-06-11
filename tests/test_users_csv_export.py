"""Tests for users CSV export helpers in the CLI."""

from pathlib import Path
from types import SimpleNamespace

from langsmith_migrator.cli.main import (
    _collect_users_export_rows,
    _load_members_csv,
    _resolve_csv_role_names,
    _role_export_labels,
    _write_members_csv,
)

ROLES = [
    {"id": "role-org-admin", "name": "ORGANIZATION_ADMIN", "display_name": "Admin"},
    {"id": "role-org-user", "name": "ORGANIZATION_USER", "display_name": ""},
    {"id": "role-ws-admin", "name": "WORKSPACE_ADMIN", "display_name": ""},
    {"id": "role-ws-user", "name": "WORKSPACE_USER", "display_name": ""},
    {"id": "role-custom", "name": "CUSTOM", "display_name": "Data Scientist"},
]


class FakeClient:
    """Minimal EnhancedAPIClient stand-in for export row collection."""

    def __init__(self, org_members, ws_members_by_id, initial_workspace="original-ws"):
        headers = {}
        if initial_workspace:
            headers["X-Tenant-Id"] = initial_workspace
        self.session = SimpleNamespace(headers=headers)
        self._org_members = org_members
        self._ws_members_by_id = ws_members_by_id

    def set_workspace(self, workspace_id):
        if workspace_id:
            self.session.headers["X-Tenant-Id"] = workspace_id
        else:
            self.session.headers.pop("X-Tenant-Id", None)

    def get_paginated(self, endpoint):
        if endpoint == "/orgs/current/members/active":
            yield from self._org_members
        elif endpoint == "/workspaces/current/members/active":
            current_ws = self.session.headers.get("X-Tenant-Id")
            yield from self._ws_members_by_id.get(current_ws, [])
        else:
            raise AssertionError(f"Unexpected endpoint: {endpoint}")


# ── _role_export_labels ──


def test_role_export_labels_builtin_and_custom():
    labels = _role_export_labels(ROLES)

    assert labels["role-org-admin"] == "Organization Admin"
    assert labels["role-org-user"] == "Organization User"
    assert labels["role-ws-admin"] == "Workspace Admin"
    assert labels["role-custom"] == "Data Scientist"


def test_role_export_labels_unknown_builtin_falls_back_to_raw_name():
    labels = _role_export_labels([{"id": "r1", "name": "ORGANIZATION_WIZARD"}])

    assert labels["r1"] == "ORGANIZATION_WIZARD"


def test_role_export_labels_custom_without_display_name_is_empty():
    labels = _role_export_labels([{"id": "r1", "name": "CUSTOM", "display_name": "  "}])

    assert labels["r1"] == ""


def test_role_export_labels_skips_roles_without_id():
    labels = _role_export_labels([{"name": "ORGANIZATION_ADMIN"}, {"id": "", "name": "CUSTOM"}])

    assert labels == {}


# ── _collect_users_export_rows ──


def test_collect_users_export_rows_org_and_workspace_members():
    client = FakeClient(
        org_members=[
            {"email": "Alice@Example.com", "role_id": "role-org-admin"},
            {"email": "bob@example.com", "role_id": "role-org-user"},
        ],
        ws_members_by_id={
            "ws-1": [{"email": "alice@example.com", "role_id": "role-ws-admin"}],
            "ws-2": [{"email": "bob@example.com", "role_id": "role-custom"}],
        },
    )
    workspaces = [
        {"id": "ws-1", "display_name": "Workspace One"},
        {"id": "ws-2", "display_name": "Workspace Two"},
    ]

    rows, unresolved = _collect_users_export_rows(client, _role_export_labels(ROLES), workspaces)

    assert unresolved == []
    assert rows == [
        {
            "email": "alice@example.com",
            "langsmith_role": "Organization Admin",
            "workspace_id": "",
            "workspace_name": "",
        },
        {
            "email": "bob@example.com",
            "langsmith_role": "Organization User",
            "workspace_id": "",
            "workspace_name": "",
        },
        {
            "email": "alice@example.com",
            "langsmith_role": "Workspace Admin",
            "workspace_id": "ws-1",
            "workspace_name": "Workspace One",
        },
        {
            "email": "bob@example.com",
            "langsmith_role": "Data Scientist",
            "workspace_id": "ws-2",
            "workspace_name": "Workspace Two",
        },
    ]


def test_collect_users_export_rows_restores_original_workspace_header():
    client = FakeClient(
        org_members=[],
        ws_members_by_id={"ws-1": []},
        initial_workspace="original-ws",
    )

    _collect_users_export_rows(client, {}, [{"id": "ws-1"}])

    assert client.session.headers["X-Tenant-Id"] == "original-ws"


def test_collect_users_export_rows_clears_workspace_header_when_none_was_set():
    client = FakeClient(org_members=[], ws_members_by_id={"ws-1": []}, initial_workspace=None)

    _collect_users_export_rows(client, {}, [{"id": "ws-1"}])

    assert "X-Tenant-Id" not in client.session.headers


def test_collect_users_export_rows_reports_unresolved_roles():
    client = FakeClient(
        org_members=[{"email": "alice@example.com", "role_id": "unknown-role"}],
        ws_members_by_id={"ws-1": [{"email": "bob@example.com", "role_id": ""}]},
    )

    rows, unresolved = _collect_users_export_rows(
        client, _role_export_labels(ROLES), [{"id": "ws-1", "display_name": "One"}]
    )

    assert [row["langsmith_role"] for row in rows] == ["", ""]
    assert unresolved == [
        "alice@example.com (org, role_id=unknown-role)",
        "bob@example.com (workspace ws-1, role_id=<none>)",
    ]


def test_collect_users_export_rows_skips_members_without_email():
    client = FakeClient(
        org_members=[{"email": "", "role_id": "role-org-user"}, "not-a-dict"],
        ws_members_by_id={},
    )

    rows, unresolved = _collect_users_export_rows(client, _role_export_labels(ROLES), [])

    assert rows == []
    assert unresolved == []


# ── _write_members_csv / round-trip ──


def test_export_rows_round_trip_through_members_csv_import(tmp_path: Path):
    client = FakeClient(
        org_members=[
            {"email": "alice@example.com", "role_id": "role-org-admin"},
            {"email": "bob@example.com", "role_id": "role-org-user"},
        ],
        ws_members_by_id={
            "ws-1": [
                {"email": "alice@example.com", "role_id": "role-ws-admin"},
                {"email": "bob@example.com", "role_id": "role-custom"},
            ],
        },
    )
    rows, unresolved = _collect_users_export_rows(
        client,
        _role_export_labels(ROLES),
        [{"id": "ws-1", "display_name": "Workspace One"}],
    )
    assert unresolved == []

    csv_path = tmp_path / "users.csv"
    _write_members_csv(str(csv_path), rows)

    loaded_rows = _load_members_csv(str(csv_path))
    resolved_rows, org_user_role_id = _resolve_csv_role_names(loaded_rows, ROLES)

    assert org_user_role_id == "role-org-user"
    resolved_by_key = {(row["email"], row["workspace_id"]): row for row in resolved_rows}
    assert resolved_by_key[("alice@example.com", "")]["role_id"] == "role-org-admin"
    assert resolved_by_key[("bob@example.com", "")]["role_id"] == "role-org-user"
    assert resolved_by_key[("alice@example.com", "ws-1")]["role_id"] == "role-ws-admin"
    assert resolved_by_key[("bob@example.com", "ws-1")]["role_id"] == "role-custom"
    assert all(row["workspace_name"] == "Workspace One" for row in loaded_rows if row["workspace_id"])
