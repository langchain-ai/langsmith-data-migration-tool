"""Tests for CSV member loading helpers in users CLI."""

from pathlib import Path

import click
import pytest

from langsmith_migrator.cli.main import (
    _csv_rows_for_workspace,
    _csv_rows_to_org_members,
    _load_members_csv,
    _resolve_csv_role_names,
)


# ── _load_members_csv ──


def test_load_members_csv_requires_columns(tmp_path: Path):
    csv_path = tmp_path / "members.csv"
    csv_path.write_text("email,role_id\nalice@example.com,role-1\n", encoding="utf-8")

    with pytest.raises(click.ClickException, match="missing required columns"):
        _load_members_csv(str(csv_path))


def test_load_members_csv_rejects_empty_langsmith_role(tmp_path: Path):
    csv_path = tmp_path / "members.csv"
    csv_path.write_text(
        "email,langsmith_role,workspace_id\nalice@example.com,,ws-1\n",
        encoding="utf-8",
    )

    with pytest.raises(click.ClickException, match="empty langsmith_role"):
        _load_members_csv(str(csv_path))


def test_load_members_csv_rejects_empty_file(tmp_path: Path):
    csv_path = tmp_path / "members.csv"
    csv_path.write_text("email,langsmith_role,workspace_id\n", encoding="utf-8")

    with pytest.raises(click.ClickException, match="empty"):
        _load_members_csv(str(csv_path))


def test_load_members_csv_normalizes_email(tmp_path: Path):
    csv_path = tmp_path / "members.csv"
    csv_path.write_text(
        "email,langsmith_role,workspace_id\nAlice@Example.COM,Workspace Admin,ws-1\n",
        encoding="utf-8",
    )

    rows = _load_members_csv(str(csv_path))

    assert rows == [
        {"email": "alice@example.com", "langsmith_role": "Workspace Admin", "workspace_id": "ws-1"}
    ]


def test_load_members_csv_accepts_utf8_bom_header(tmp_path: Path):
    csv_path = tmp_path / "members.csv"
    csv_path.write_text(
        "email,langsmith_role,workspace_id\nalice@example.com,Organization Admin,\n",
        encoding="utf-8-sig",
    )

    rows = _load_members_csv(str(csv_path))

    assert rows == [
        {"email": "alice@example.com", "langsmith_role": "Organization Admin", "workspace_id": ""}
    ]


def test_load_members_csv_allows_empty_workspace_id(tmp_path: Path):
    csv_path = tmp_path / "members.csv"
    csv_path.write_text(
        "email,langsmith_role,workspace_id,workspace_name\n"
        "alice@example.com,Organization Admin,,\n",
        encoding="utf-8",
    )

    rows = _load_members_csv(str(csv_path))

    assert rows == [
        {"email": "alice@example.com", "langsmith_role": "Organization Admin", "workspace_id": ""}
    ]


def test_load_members_csv_strips_whitespace(tmp_path: Path):
    csv_path = tmp_path / "members.csv"
    csv_path.write_text(
        "email,langsmith_role,workspace_id\n"
        "alice@example.com, Workspace Admin ,ws-1\n",
        encoding="utf-8",
    )

    rows = _load_members_csv(str(csv_path))

    assert rows[0]["langsmith_role"] == "Workspace Admin"


# ── _resolve_csv_role_names ──


SOURCE_ROLES = [
    {"id": "src-admin", "name": "ORGANIZATION_ADMIN", "display_name": "Admin", "permissions": []},
    {"id": "src-user", "name": "ORGANIZATION_USER", "display_name": "User", "permissions": []},
    {"id": "src-ws-admin", "name": "WORKSPACE_ADMIN", "display_name": "Workspace Admin", "permissions": []},
    {"id": "src-custom-1", "name": "CUSTOM", "display_name": "Data Scientist", "permissions": []},
    {"id": "src-custom-2", "name": "CUSTOM", "display_name": "maintainer-dl-general", "permissions": []},
]


def test_resolve_csv_role_names_builtin_roles():
    rows = [
        {"email": "a@example.com", "langsmith_role": "Organization Admin", "workspace_id": ""},
        {"email": "b@example.com", "langsmith_role": "Workspace Admin", "workspace_id": "ws-1"},
    ]

    resolved, org_user_id = _resolve_csv_role_names(rows, SOURCE_ROLES)

    assert resolved[0]["role_id"] == "src-admin"
    assert resolved[1]["role_id"] == "src-ws-admin"
    assert org_user_id == "src-user"


def test_resolve_csv_role_names_custom_roles():
    rows = [
        {"email": "a@example.com", "langsmith_role": "Data Scientist", "workspace_id": "ws-1"},
    ]

    resolved, _ = _resolve_csv_role_names(rows, SOURCE_ROLES)

    assert resolved[0]["role_id"] == "src-custom-1"


def test_resolve_csv_role_names_case_insensitive():
    rows = [
        {"email": "a@example.com", "langsmith_role": "organization admin", "workspace_id": ""},
        {"email": "b@example.com", "langsmith_role": "ORGANIZATION_ADMIN", "workspace_id": ""},
        {"email": "c@example.com", "langsmith_role": "data scientist", "workspace_id": "ws-1"},
    ]

    resolved, _ = _resolve_csv_role_names(rows, SOURCE_ROLES)

    assert resolved[0]["role_id"] == "src-admin"
    assert resolved[1]["role_id"] == "src-admin"
    assert resolved[2]["role_id"] == "src-custom-1"


def test_resolve_csv_role_names_unknown_role_error():
    rows = [
        {"email": "a@example.com", "langsmith_role": "Nonexistent Role", "workspace_id": ""},
    ]

    with pytest.raises(click.ClickException, match="Could not resolve role name"):
        _resolve_csv_role_names(rows, SOURCE_ROLES)


def test_resolve_csv_role_names_builtin_alias_ignores_unrelated_collision():
    rows = [
        {"email": "a@example.com", "langsmith_role": "Organization Admin", "workspace_id": ""},
    ]
    source_roles = [
        {"id": "src-custom-admin", "name": "CUSTOM", "display_name": "Admin", "permissions": []},
        {"id": "src-admin", "name": "ORGANIZATION_ADMIN", "display_name": "Admin", "permissions": []},
    ]

    resolved, _ = _resolve_csv_role_names(rows, source_roles)

    assert resolved[0]["role_id"] == "src-admin"


def test_resolve_csv_role_names_builtin_alias_preferred_over_custom_collision():
    rows = [
        {"email": "a@example.com", "langsmith_role": "Organization Admin", "workspace_id": ""},
    ]
    source_roles = [
        {"id": "src-custom-admin", "name": "CUSTOM", "display_name": "Organization Admin", "permissions": []},
        {"id": "src-admin", "name": "ORGANIZATION_ADMIN", "display_name": "Admin", "permissions": []},
    ]

    resolved, _ = _resolve_csv_role_names(rows, source_roles)

    assert resolved[0]["role_id"] == "src-admin"


def test_resolve_csv_role_names_ambiguous_display_name_rejected_regardless_of_order():
    rows = [
        {"email": "a@example.com", "langsmith_role": "Admin", "workspace_id": ""},
    ]
    source_roles = [
        {"id": "src-custom-admin", "name": "CUSTOM", "display_name": "Admin", "permissions": []},
        {"id": "src-admin", "name": "ORGANIZATION_ADMIN", "display_name": "Admin", "permissions": []},
    ]

    with pytest.raises(click.ClickException, match="Ambiguous role name"):
        _resolve_csv_role_names(rows, source_roles)


def test_resolve_csv_role_names_builtin_identifier_preferred_over_custom_collision():
    rows = [
        {"email": "a@example.com", "langsmith_role": "ORGANIZATION_ADMIN", "workspace_id": ""},
    ]
    source_roles = [
        {"id": "src-admin", "name": "ORGANIZATION_ADMIN", "display_name": "Admin", "permissions": []},
        {"id": "src-custom-admin", "name": "CUSTOM", "display_name": "ORGANIZATION_ADMIN", "permissions": []},
    ]

    resolved, _ = _resolve_csv_role_names(rows, source_roles)

    assert resolved[0]["role_id"] == "src-admin"


def test_resolve_csv_role_names_whitespace_trimmed():
    """The resolver strips whitespace from langsmith_role before lookup."""
    rows = [
        {"email": "a@example.com", "langsmith_role": " Organization Admin ", "workspace_id": ""},
    ]

    resolved, _ = _resolve_csv_role_names(rows, SOURCE_ROLES)
    assert resolved[0]["role_id"] == "src-admin"


def test_resolve_csv_role_names_returns_org_user_role_id():
    rows = [
        {"email": "a@example.com", "langsmith_role": "Workspace Admin", "workspace_id": "ws-1"},
    ]

    _, org_user_id = _resolve_csv_role_names(rows, SOURCE_ROLES)

    assert org_user_id == "src-user"


# ── _csv_rows_to_org_members ──


def test_csv_rows_to_org_members_org_rows_used():
    rows = [
        {"email": "alice@example.com", "role_id": "role-admin", "workspace_id": ""},
        {"email": "bob@example.com", "role_id": "role-ws-admin", "workspace_id": "ws-1"},
    ]

    members = _csv_rows_to_org_members(rows, default_org_role_id="role-user")

    by_email = {m["email"]: m for m in members}
    assert by_email["alice@example.com"]["role_id"] == "role-admin"
    assert by_email["bob@example.com"]["role_id"] == "role-user"


def test_csv_rows_to_org_members_workspace_only_gets_default():
    rows = [
        {"email": "alice@example.com", "role_id": "role-ws-admin", "workspace_id": "ws-1"},
        {"email": "alice@example.com", "role_id": "role-ws-admin", "workspace_id": "ws-2"},
    ]

    members = _csv_rows_to_org_members(rows, default_org_role_id="role-user")

    assert len(members) == 1
    assert members[0]["role_id"] == "role-user"


def test_csv_rows_to_org_members_conflicting_org_roles_rejected():
    rows = [
        {"email": "alice@example.com", "role_id": "role-admin", "workspace_id": ""},
        {"email": "alice@example.com", "role_id": "role-user", "workspace_id": ""},
    ]

    with pytest.raises(click.ClickException, match="conflicting org-level roles"):
        _csv_rows_to_org_members(rows, default_org_role_id="role-user")


def test_csv_rows_to_org_members_dedupes_identical_org_rows():
    rows = [
        {"email": "alice@example.com", "role_id": "role-admin", "workspace_id": ""},
        {"email": "alice@example.com", "role_id": "role-admin", "workspace_id": ""},
    ]

    members = _csv_rows_to_org_members(rows, default_org_role_id="role-user")

    assert len(members) == 1
    assert members[0]["role_id"] == "role-admin"


# ── _csv_rows_for_workspace ──


def test_csv_rows_for_workspace_dedupes_by_email():
    rows = [
        {"email": "alice@example.com", "role_id": "role-1", "workspace_id": "ws-1"},
        {"email": "alice@example.com", "role_id": "role-1", "workspace_id": "ws-1"},
        {"email": "bob@example.com", "role_id": "role-2", "workspace_id": "ws-2"},
    ]

    selected = _csv_rows_for_workspace(rows, "ws-1")

    assert selected == [
        {
            "id": "ws-1:alice@example.com",
            "email": "alice@example.com",
            "role_id": "role-1",
            "full_name": "",
        }
    ]


def test_csv_rows_for_workspace_rejects_conflicting_roles():
    rows = [
        {"email": "alice@example.com", "role_id": "role-1", "workspace_id": "ws-1"},
        {"email": "alice@example.com", "role_id": "role-2", "workspace_id": "ws-1"},
    ]

    with pytest.raises(click.ClickException, match="conflicting role_id"):
        _csv_rows_for_workspace(rows, "ws-1")


def test_csv_rows_for_workspace_ignores_org_rows():
    rows = [
        {"email": "alice@example.com", "role_id": "role-admin", "workspace_id": ""},
        {"email": "alice@example.com", "role_id": "role-ws", "workspace_id": "ws-1"},
    ]

    selected = _csv_rows_for_workspace(rows, "ws-1")

    assert len(selected) == 1
    assert selected[0]["role_id"] == "role-ws"
