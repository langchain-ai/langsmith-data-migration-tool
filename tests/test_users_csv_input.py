"""Tests for CSV member loading helpers in users CLI."""

from pathlib import Path

import click
import pytest

from langsmith_migrator.cli.main import (
    _csv_rows_for_workspace,
    _csv_rows_to_org_members,
    _load_members_csv,
)


def test_load_members_csv_requires_columns(tmp_path: Path):
    csv_path = tmp_path / "members.csv"
    csv_path.write_text("email,role_id\nalice@example.com,role-1\n", encoding="utf-8")

    with pytest.raises(click.ClickException, match="missing required columns"):
        _load_members_csv(str(csv_path))


def test_load_members_csv_rejects_empty_values(tmp_path: Path):
    csv_path = tmp_path / "members.csv"
    csv_path.write_text(
        "email,role_id,workspace_id\nalice@example.com,,ws-1\n",
        encoding="utf-8",
    )

    with pytest.raises(click.ClickException, match="empty role_id"):
        _load_members_csv(str(csv_path))


def test_load_members_csv_rejects_empty_file(tmp_path: Path):
    csv_path = tmp_path / "members.csv"
    csv_path.write_text("email,role_id,workspace_id\n", encoding="utf-8")

    with pytest.raises(click.ClickException, match="empty"):
        _load_members_csv(str(csv_path))


def test_load_members_csv_normalizes_email(tmp_path: Path):
    csv_path = tmp_path / "members.csv"
    csv_path.write_text(
        "email,role_id,workspace_id\nAlice@Example.COM,role-1,ws-1\n",
        encoding="utf-8",
    )

    rows = _load_members_csv(str(csv_path))

    assert rows == [
        {"email": "alice@example.com", "role_id": "role-1", "workspace_id": "ws-1"}
    ]


def test_load_members_csv_accepts_utf8_bom_header(tmp_path: Path):
    csv_path = tmp_path / "members.csv"
    csv_path.write_text(
        "email,role_id,workspace_id\nalice@example.com,role-1,ws-1\n",
        encoding="utf-8-sig",
    )

    rows = _load_members_csv(str(csv_path))

    assert rows == [
        {"email": "alice@example.com", "role_id": "role-1", "workspace_id": "ws-1"}
    ]


def test_csv_rows_to_org_members_rejects_conflicting_roles():
    rows = [
        {"email": "alice@example.com", "role_id": "role-1", "workspace_id": "ws-1"},
        {"email": "alice@example.com", "role_id": "role-2", "workspace_id": "ws-2"},
    ]

    with pytest.raises(click.ClickException, match="conflicting role_id"):
        _csv_rows_to_org_members(rows)


def test_csv_rows_to_org_members_dedupes_identical_rows():
    rows = [
        {"email": "alice@example.com", "role_id": "role-1", "workspace_id": "ws-1"},
        {"email": "alice@example.com", "role_id": "role-1", "workspace_id": "ws-2"},
    ]

    members = _csv_rows_to_org_members(rows)

    assert members == [
        {
            "id": "alice@example.com",
            "email": "alice@example.com",
            "role_id": "role-1",
            "full_name": "",
        }
    ]


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
