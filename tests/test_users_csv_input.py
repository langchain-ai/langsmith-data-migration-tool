"""Tests for CSV member loading helpers in users CLI."""

from pathlib import Path

import click
import pytest

from langsmith_migrator.cli.main import (
    _build_single_instance_users_plan,
    _configure_single_instance,
    _csv_rows_for_workspace,
    _csv_rows_to_org_members,
    _load_members_csv,
    _normalize_single_instance_url,
    _normalize_csv_role_scopes,
    _resolve_single_instance_workspace_ids,
    _resolve_csv_role_names,
)
from langsmith_migrator.core.migrators.user_role import (
    make_workspace_role_union_id,
)
from langsmith_migrator.utils.config import Config


# ── _load_members_csv ──


def test_load_members_csv_requires_columns(tmp_path: Path):
    csv_path = tmp_path / "members.csv"
    csv_path.write_text("email,role_id\nalice@example.com,role-1\n", encoding="utf-8")

    with pytest.raises(click.ClickException, match="missing required columns"):
        _load_members_csv(str(csv_path))


def test_load_members_csv_rejects_empty_langsmith_role_with_workspace_guidance(tmp_path: Path):
    csv_path = tmp_path / "members.csv"
    csv_path.write_text(
        "email,langsmith_role,workspace_id\nalice@example.com,,ws-1\n",
        encoding="utf-8",
    )

    with pytest.raises(click.ClickException) as exc_info:
        _load_members_csv(str(csv_path))

    message = str(exc_info.value)
    assert "row 2 for alice@example.com in workspace ws-1" in message
    assert "empty langsmith_role" in message
    assert "Workspace Admin" in message
    assert "leave workspace_id empty" in message


def test_load_members_csv_rejects_empty_langsmith_role_on_org_row_with_org_guidance(
    tmp_path: Path,
):
    csv_path = tmp_path / "members.csv"
    csv_path.write_text(
        "email,langsmith_role,workspace_id\nalice@example.com,,\n",
        encoding="utf-8",
    )

    with pytest.raises(click.ClickException) as exc_info:
        _load_members_csv(str(csv_path))

    message = str(exc_info.value)
    assert "row 2 for alice@example.com" in message
    assert "empty langsmith_role" in message
    assert "Organization User" in message
    assert "Organization Admin" in message


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


def test_load_members_csv_preserves_non_empty_workspace_name(tmp_path: Path):
    csv_path = tmp_path / "members.csv"
    csv_path.write_text(
        "email,langsmith_role,workspace_id,workspace_name\n"
        "alice@example.com,Workspace Admin,ws-1,Workspace A\n",
        encoding="utf-8",
    )

    rows = _load_members_csv(str(csv_path))

    assert rows == [
        {
            "email": "alice@example.com",
            "langsmith_role": "Workspace Admin",
            "workspace_id": "ws-1",
            "workspace_name": "Workspace A",
        }
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
    {"id": "src-operator", "name": "ORGANIZATION_OPERATOR", "display_name": "Operator", "permissions": []},
    {"id": "src-user", "name": "ORGANIZATION_USER", "display_name": "User", "permissions": []},
    {"id": "src-viewer", "name": "ORGANIZATION_VIEWER", "display_name": "Viewer", "permissions": []},
    {"id": "src-ws-admin", "name": "WORKSPACE_ADMIN", "display_name": "Workspace Admin", "permissions": []},
    {"id": "src-ws-user", "name": "WORKSPACE_USER", "display_name": "Collaborator", "permissions": []},
    {"id": "src-ws-viewer", "name": "WORKSPACE_VIEWER", "display_name": "Workspace Read-only", "permissions": []},
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


def test_resolve_csv_role_names_preserves_workspace_name():
    rows = [
        {
            "email": "b@example.com",
            "langsmith_role": "Workspace Admin",
            "workspace_id": "ws-1",
            "workspace_name": "Workspace A",
        },
    ]

    resolved, _ = _resolve_csv_role_names(rows, SOURCE_ROLES)

    assert resolved[0]["workspace_name"] == "Workspace A"


def test_resolve_csv_role_names_extended_builtin_aliases():
    rows = [
        {"email": "a@example.com", "langsmith_role": "Organization Viewer", "workspace_id": ""},
        {"email": "b@example.com", "langsmith_role": "Workspace User", "workspace_id": "ws-1"},
        {"email": "c@example.com", "langsmith_role": "WORKSPACE_VIEWER", "workspace_id": "ws-1"},
    ]

    resolved, _ = _resolve_csv_role_names(rows, SOURCE_ROLES)

    assert resolved[0]["role_id"] == "src-viewer"
    assert resolved[1]["role_id"] == "src-ws-user"
    assert resolved[2]["role_id"] == "src-ws-viewer"


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
        {"email": "c@example.com", "langsmith_role": "DATA SCIENTIST", "workspace_id": "ws-1"},
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


def test_resolve_csv_role_names_case_insensitive_conflict_calls_out_candidates():
    rows = [
        {"email": "a@example.com", "langsmith_role": "DATA SCIENTIST", "workspace_id": "ws-1"},
    ]
    source_roles = [
        {"id": "src-custom-1", "name": "CUSTOM", "display_name": "Data Scientist", "permissions": []},
        {"id": "src-custom-2", "name": "CUSTOM", "display_name": "DATA SCIENTIST", "permissions": []},
    ]

    with pytest.raises(click.ClickException) as exc_info:
        _resolve_csv_role_names(rows, source_roles)

    message = str(exc_info.value)
    assert "matched multiple roles case-insensitively" in message
    assert "Data Scientist (src-custom-1)" in message
    assert "DATA SCIENTIST (src-custom-2)" in message
    assert "Available roles (case-insensitive)" in message


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
            "langsmith_role": "Workspace User",
            "role_id": "src-ws-user",
            "role_name": "WORKSPACE_USER",
            "workspace_id": "",
        }
    ]

    with pytest.raises(click.ClickException, match="workspace-scoped and cannot be used on an org-level row"):
        _normalize_csv_role_scopes(rows)


def test_normalize_csv_role_scopes_rejects_org_user_on_workspace_row():
    rows = [
        {
            "email": "alice@example.com",
            "langsmith_role": "Organization Viewer",
            "role_id": "src-viewer",
            "role_name": "ORGANIZATION_VIEWER",
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


def test_csv_rows_to_org_members_single_instance_workspace_only_carries_workspace_invite():
    rows = [
        {"email": "alice@example.com", "role_id": "role-ws-admin", "workspace_id": "ws-2"},
        {"email": "alice@example.com", "role_id": "role-ws-admin", "workspace_id": "ws-1"},
    ]

    members = _csv_rows_to_org_members(
        rows,
        default_org_role_id="role-user",
        direct_workspace_invites=True,
    )

    assert members == [
        {
            "id": "alice@example.com",
            "email": "alice@example.com",
            "role_id": "role-user",
            "full_name": "",
            "workspace_ids": ["ws-1", "ws-2"],
            "workspace_role_id": "role-ws-admin",
        }
    ]


def test_csv_rows_to_org_members_single_instance_org_row_carries_workspace_invite():
    rows = [
        {"email": "alice@example.com", "role_id": "role-user", "workspace_id": ""},
        {"email": "alice@example.com", "role_id": "role-ws-admin", "workspace_id": "ws-2"},
        {"email": "alice@example.com", "role_id": "role-ws-admin", "workspace_id": "ws-1"},
    ]

    members = _csv_rows_to_org_members(
        rows,
        default_org_role_id="role-user",
        direct_workspace_invites=True,
    )

    assert members == [
        {
            "id": "alice@example.com",
            "email": "alice@example.com",
            "role_id": "role-user",
            "full_name": "",
            "workspace_ids": ["ws-1", "ws-2"],
            "workspace_role_id": "role-ws-admin",
        }
    ]


def test_csv_rows_to_org_members_single_instance_mixed_workspace_roles_omit_direct_invite():
    rows = [
        {"email": "alice@example.com", "role_id": "role-ws-admin", "workspace_id": "ws-1"},
        {"email": "alice@example.com", "role_id": "role-ws-viewer", "workspace_id": "ws-2"},
    ]

    members = _csv_rows_to_org_members(
        rows,
        default_org_role_id="role-user",
        direct_workspace_invites=True,
    )

    assert members == [
        {
            "id": "alice@example.com",
            "email": "alice@example.com",
            "role_id": "role-user",
            "full_name": "",
        }
    ]


def test_csv_rows_to_org_members_single_instance_combines_duplicate_custom_roles_for_invite():
    rows = [
        {
            "email": "alice@example.com",
            "role_id": "src-custom-1",
            "role_name": "CUSTOM",
            "workspace_id": "ws-1",
        },
        {
            "email": "alice@example.com",
            "role_id": "src-custom-2",
            "role_name": "CUSTOM",
            "workspace_id": "ws-1",
        },
    ]

    members = _csv_rows_to_org_members(
        rows,
        default_org_role_id="role-user",
        direct_workspace_invites=True,
    )

    assert members == [
        {
            "id": "alice@example.com",
            "email": "alice@example.com",
            "role_id": "role-user",
            "full_name": "",
            "workspace_ids": ["ws-1"],
            "workspace_role_id": make_workspace_role_union_id(
                {"src-custom-1", "src-custom-2"}
            ),
        }
    ]


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


def test_csv_rows_for_workspace_combines_custom_roles():
    rows = [
        {
            "email": "alice@example.com",
            "role_id": "src-custom-1",
            "role_name": "CUSTOM",
            "workspace_id": "ws-1",
        },
        {
            "email": "alice@example.com",
            "role_id": "src-custom-2",
            "role_name": "CUSTOM",
            "workspace_id": "ws-1",
        },
    ]

    selected = _csv_rows_for_workspace(rows, "ws-1")

    assert selected == [
        {
            "id": "ws-1:alice@example.com",
            "email": "alice@example.com",
            "role_id": make_workspace_role_union_id(
                {"src-custom-1", "src-custom-2"}
            ),
            "full_name": "",
        }
    ]


def test_csv_rows_for_workspace_picks_highest_builtin_role():
    rows = [
        {
            "email": "alice@example.com",
            "role_id": "src-ws-viewer",
            "role_name": "WORKSPACE_VIEWER",
            "workspace_id": "ws-1",
        },
        {
            "email": "alice@example.com",
            "role_id": "src-ws-admin",
            "role_name": "WORKSPACE_ADMIN",
            "workspace_id": "ws-1",
        },
        {
            "email": "alice@example.com",
            "role_id": "src-ws-user",
            "role_name": "WORKSPACE_USER",
            "workspace_id": "ws-1",
        },
    ]

    selected = _csv_rows_for_workspace(rows, "ws-1")

    assert selected[0]["role_id"] == "src-ws-admin"


def test_csv_rows_for_workspace_unions_custom_role_with_workspace_admin():
    rows = [
        {
            "email": "alice@example.com",
            "role_id": "src-ws-admin",
            "role_name": "WORKSPACE_ADMIN",
            "workspace_id": "ws-1",
        },
        {
            "email": "alice@example.com",
            "role_id": "src-custom-1",
            "role_name": "CUSTOM",
            "workspace_id": "ws-1",
        },
    ]

    selected = _csv_rows_for_workspace(rows, "ws-1")

    assert selected[0]["role_id"] == make_workspace_role_union_id(
        {"src-ws-admin", "src-custom-1"}
    )


def test_csv_rows_to_org_members_unions_custom_role_with_workspace_admin_for_invite():
    rows = [
        {
            "email": "alice@example.com",
            "role_id": "src-ws-admin",
            "role_name": "WORKSPACE_ADMIN",
            "workspace_id": "ws-1",
        },
        {
            "email": "alice@example.com",
            "role_id": "src-custom-1",
            "role_name": "CUSTOM",
            "workspace_id": "ws-1",
        },
    ]

    members = _csv_rows_to_org_members(
        rows,
        default_org_role_id="role-user",
        direct_workspace_invites=True,
    )

    assert members[0]["workspace_role_id"] == make_workspace_role_union_id(
        {"src-ws-admin", "src-custom-1"}
    )


def test_csv_rows_for_workspace_ignores_org_rows():
    rows = [
        {"email": "alice@example.com", "role_id": "role-admin", "workspace_id": ""},
        {"email": "alice@example.com", "role_id": "role-ws", "workspace_id": "ws-1"},
    ]

    selected = _csv_rows_for_workspace(rows, "ws-1")

    assert len(selected) == 1
    assert selected[0]["role_id"] == "role-ws"


@pytest.mark.parametrize(
    ("url", "expected"),
    [
        ("https://same.example/api/v1", "https://same.example"),
        ("https://same.example/api/v2/", "https://same.example"),
        ("HTTPS://Same.Example/API/V2///", "https://same.example"),
        ("https://same.example/custom/path/", "https://same.example/custom/path"),
    ],
)
def test_normalize_single_instance_url_strips_api_suffixes_and_normalizes_case(
    url: str, expected: str
):
    assert _normalize_single_instance_url(url) == expected


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


def test_build_single_instance_users_plan_rejects_workspace_name_mismatch():
    rows = [
        {
            "email": "alice@example.com",
            "role_id": "role-ws-admin",
            "workspace_id": "ws-1",
            "workspace_name": "Workspace B",
        },
    ]

    with pytest.raises(click.ClickException) as exc_info:
        _build_single_instance_users_plan(
            rows,
            available_workspaces=[{"id": "ws-1", "display_name": "Workspace A"}],
            default_org_role_id="role-user",
            source_of_truth=False,
        )

    message = str(exc_info.value)
    assert "workspace_name mismatch" in message
    assert "Workspace B" in message
    assert "Workspace A" in message


def test_build_single_instance_users_plan_authoritative_includes_empty_workspaces():
    rows = [
        {
            "email": "alice@example.com",
            "role_id": "role-ws-admin",
            "workspace_id": "ws-1",
        },
    ]

    plan = _build_single_instance_users_plan(
        rows,
        available_workspaces=[
            {"id": "ws-1", "display_name": "Workspace A"},
            {"id": "ws-2", "display_name": "Workspace B"},
        ],
        default_org_role_id="role-user",
        source_of_truth=True,
    )

    assert plan.workspace_ids == ["ws-1", "ws-2"]
    assert plan.workspace_members_by_id["ws-1"] == [
        {
            "id": "ws-1:alice@example.com",
            "email": "alice@example.com",
            "role_id": "role-ws-admin",
            "full_name": "",
        }
    ]
    assert plan.workspace_members_by_id["ws-2"] == []


def test_build_single_instance_users_plan_notes_mixed_workspace_roles():
    rows = [
        {
            "email": "alice@example.com",
            "role_id": "role-ws-admin",
            "workspace_id": "ws-1",
        },
        {
            "email": "alice@example.com",
            "role_id": "role-ws-viewer",
            "workspace_id": "ws-2",
        },
    ]

    plan = _build_single_instance_users_plan(
        rows,
        available_workspaces=[
            {"id": "ws-1", "display_name": "Workspace A"},
            {"id": "ws-2", "display_name": "Workspace B"},
        ],
        default_org_role_id="role-user",
        source_of_truth=False,
    )

    assert plan.org_members == [
        {
            "id": "alice@example.com",
            "email": "alice@example.com",
            "role_id": "role-user",
            "full_name": "",
        }
    ]
    assert any(
        "alice@example.com has multiple workspace roles" in note
        for note in plan.operator_notes
    )


def test_build_single_instance_users_plan_notes_org_row_with_mixed_workspace_roles():
    rows = [
        {
            "email": "alice@example.com",
            "role_id": "role-user",
            "workspace_id": "",
        },
        {
            "email": "alice@example.com",
            "role_id": "role-ws-admin",
            "workspace_id": "ws-1",
        },
        {
            "email": "alice@example.com",
            "role_id": "role-ws-viewer",
            "workspace_id": "ws-2",
        },
    ]

    plan = _build_single_instance_users_plan(
        rows,
        available_workspaces=[
            {"id": "ws-1", "display_name": "Workspace A"},
            {"id": "ws-2", "display_name": "Workspace B"},
        ],
        default_org_role_id="role-user",
        source_of_truth=False,
    )

    assert any(
        "alice@example.com has multiple workspace roles" in note
        for note in plan.operator_notes
    )
