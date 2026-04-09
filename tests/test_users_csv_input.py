"""Tests for CSV member loading helpers in users CLI."""

from pathlib import Path

import click
import pytest

from langsmith_migrator.cli.main import (
    _configure_single_instance,
    _csv_rows_for_workspace,
    _csv_rows_to_org_members,
    _load_members_csv,
    _normalize_csv_role_scopes,
    _resolve_single_instance_workspace_ids,
    _resolve_csv_role_names,
)
from langsmith_migrator.utils.config import Config


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


# ── _normalize_csv_role_scopes ──


def test_normalize_csv_role_scopes_treats_workspace_org_admin_as_org_access():
    rows = [
        {
            "email": "alice@example.com",
            "langsmith_role": "Organization Admin",
            "role_id": "src-admin",
            "role_name": "ORGANIZATION_ADMIN",
            "workspace_id": "ws-1",
        }
    ]

    normalized, rewritten = _normalize_csv_role_scopes(rows)

    assert rewritten == 1
    assert normalized == [
        {
            "email": "alice@example.com",
            "langsmith_role": "Organization Admin",
            "role_id": "src-admin",
            "role_name": "ORGANIZATION_ADMIN",
            "workspace_id": "",
        }
    ]


def test_normalize_csv_role_scopes_rejects_workspace_role_on_org_row():
    rows = [
        {
            "email": "alice@example.com",
            "langsmith_role": "Workspace Admin",
            "role_id": "src-ws-admin",
            "role_name": "WORKSPACE_ADMIN",
            "workspace_id": "",
        }
    ]

    with pytest.raises(click.ClickException, match="workspace-scoped and cannot be used on an org-level row"):
        _normalize_csv_role_scopes(rows)


def test_normalize_csv_role_scopes_rejects_org_user_on_workspace_row():
    rows = [
        {
            "email": "alice@example.com",
            "langsmith_role": "Organization User",
            "role_id": "src-user",
            "role_name": "ORGANIZATION_USER",
            "workspace_id": "ws-1",
        }
    ]

    with pytest.raises(click.ClickException, match="org-scoped and cannot be used on a workspace row"):
        _normalize_csv_role_scopes(rows)


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


def test_configure_single_instance_uses_destination_when_available():
    config = Config(
        source_api_key="",
        dest_api_key="dest-key",
        source_url="https://source.example",
        dest_url="https://dest.example",
    )
    config.source.api_key = ""
    config.source.base_url = "https://source.example"
    config.destination.api_key = "dest-key"
    config.destination.base_url = "https://dest.example"

    _configure_single_instance(config)

    assert config.source.api_key == "dest-key"
    assert config.destination.api_key == "dest-key"
    assert config.source.base_url == "https://dest.example"
    assert config.destination.base_url == "https://dest.example"


def test_configure_single_instance_accepts_source_only_credentials():
    config = Config(
        source_api_key="source-key",
        dest_api_key="",
        source_url="https://source.example",
        dest_url="https://api.smith.langchain.com",
    )
    config.source.api_key = "source-key"
    config.source.base_url = "https://source.example"
    config.destination.api_key = ""
    config.destination.base_url = "https://api.smith.langchain.com"

    _configure_single_instance(config)

    assert config.source.api_key == "source-key"
    assert config.destination.api_key == "source-key"
    assert config.source.base_url == "https://source.example"
    assert config.destination.base_url == "https://source.example"


def test_configure_single_instance_rejects_missing_credentials():
    config = Config(
        source_api_key="",
        dest_api_key="",
        source_url="https://source.example",
        dest_url="https://dest.example",
    )
    config.source.api_key = ""
    config.source.base_url = "https://source.example"
    config.destination.api_key = ""
    config.destination.base_url = "https://dest.example"

    with pytest.raises(click.ClickException, match="requires a target API key and URL"):
        _configure_single_instance(config)


def test_configure_single_instance_rejects_ambiguous_targets():
    config = Config(
        source_api_key="source-key",
        dest_api_key="dest-key",
        source_url="https://source.example",
        dest_url="https://dest.example",
    )
    config.source.api_key = "source-key"
    config.source.base_url = "https://source.example"
    config.destination.api_key = "dest-key"
    config.destination.base_url = "https://dest.example"

    with pytest.raises(click.ClickException, match="found multiple configured LangSmith targets"):
        _configure_single_instance(config)


def test_configure_single_instance_accepts_equivalent_normalized_urls():
    config = Config(
        source_api_key="same-key",
        dest_api_key="same-key",
        source_url="https://same.example/api/v1",
        dest_url="https://same.example",
    )
    config.source.api_key = "same-key"
    config.source.base_url = "https://same.example/api/v1"
    config.destination.api_key = "same-key"
    config.destination.base_url = "https://same.example"

    _configure_single_instance(config)

    assert config.source.api_key == "same-key"
    assert config.destination.api_key == "same-key"
    assert config.source.base_url == "https://same.example"
    assert config.destination.base_url == "https://same.example"


def test_resolve_single_instance_workspace_ids_validates_csv_workspace_ids():
    rows = [
        {"email": "alice@example.com", "role_id": "role-1", "workspace_id": "ws-1"},
    ]

    with pytest.raises(click.ClickException, match="unknown workspace_id"):
        _resolve_single_instance_workspace_ids(
            rows,
            {"ws-2"},
            source_of_truth=False,
        )


def test_resolve_single_instance_workspace_ids_source_of_truth_uses_all_workspaces():
    rows = [
        {"email": "alice@example.com", "role_id": "role-1", "workspace_id": "ws-2"},
    ]

    workspace_ids = _resolve_single_instance_workspace_ids(
        rows,
        {"ws-1", "ws-2"},
        source_of_truth=True,
    )

    assert workspace_ids == ["ws-1", "ws-2"]
