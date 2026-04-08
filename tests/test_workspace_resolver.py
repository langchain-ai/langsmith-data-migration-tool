"""Tests for workspace resolution safeguards."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from langsmith_migrator.cli.tui_workspace_mapper import WorkspaceProjectResult
from langsmith_migrator.utils.migration_config import MigrationFileConfig
from langsmith_migrator.utils import workspace_resolver


class _FakeClient:
    def __init__(self, base_url: str) -> None:
        self.base_url = base_url
        self.session = SimpleNamespace(headers={})

    def set_workspace(self, workspace_id: str | None) -> None:
        if workspace_id is None:
            self.session.headers.pop("X-Tenant-Id", None)
        else:
            self.session.headers["X-Tenant-Id"] = workspace_id


class _FakeConsole:
    def __init__(self) -> None:
        self.buffer = ""

    def print(self, *args, end: str = "\n", **kwargs) -> None:  # noqa: ANN001
        del kwargs
        self.buffer += "".join(str(arg) for arg in args) + end

    @property
    def text(self) -> str:
        return self.buffer


def test_resolve_workspace_context_reuses_valid_saved_mapping_non_interactive(
    monkeypatch: pytest.MonkeyPatch,
):
    source_client = _FakeClient("https://source.example/api/v1")
    dest_client = _FakeClient("https://dest.example/api/v1")
    console = _FakeConsole()

    source_workspaces = [
        {"id": "src-1", "display_name": "Source One"},
        {"id": "src-2", "display_name": "Source Two"},
    ]
    dest_workspaces = [{"id": "dst-1", "display_name": "Dest One"}]
    monkeypatch.setattr(
        workspace_resolver,
        "list_workspaces",
        lambda client: source_workspaces if client is source_client else dest_workspaces,
    )

    saved_config = MigrationFileConfig(
        workspace_mapping={"src-1": "dst-1"},
        workspace_mapping_source_url="https://source.example/api/v1",
        workspace_mapping_destination_url="https://dest.example/api/v1",
    )

    result = workspace_resolver.resolve_workspace_context(
        source_client,
        dest_client,
        console,
        saved_config=saved_config,
        non_interactive=True,
    )

    assert result is not None
    assert result.workspace_mapping == {"src-1": "dst-1"}


def test_resolve_workspace_context_ignores_stale_saved_mapping_before_launching_tui(
    monkeypatch: pytest.MonkeyPatch,
):
    source_client = _FakeClient("https://source.example/api/v1")
    dest_client = _FakeClient("https://dest.example/api/v1")
    console = _FakeConsole()

    source_workspaces = [
        {"id": "src-1", "display_name": "Source One"},
        {"id": "src-2", "display_name": "Source Two"},
    ]
    dest_workspaces = [
        {"id": "dst-1", "display_name": "Dest One"},
        {"id": "dst-2", "display_name": "Dest Two"},
    ]
    monkeypatch.setattr(
        workspace_resolver,
        "list_workspaces",
        lambda client: source_workspaces if client is source_client else dest_workspaces,
    )

    saved_config = MigrationFileConfig(
        workspace_mapping={"src-1": "dst-1"},
        workspace_mapping_source_url="https://other-source.example/api/v1",
        workspace_mapping_destination_url="https://other-dest.example/api/v1",
    )

    tui_result = WorkspaceProjectResult(
        workspace_mapping={"src-2": "dst-2"},
        project_mappings={},
        workspaces_to_create=[],
    )
    build_tui = Mock(return_value=tui_result)
    monkeypatch.setattr(workspace_resolver, "build_workspace_mapping_tui", build_tui)
    monkeypatch.setattr(workspace_resolver, "save_config", lambda config: Path("/tmp/config.json"))

    result = workspace_resolver.resolve_workspace_context(
        source_client,
        dest_client,
        console,
        saved_config=saved_config,
        non_interactive=False,
    )

    assert result == tui_result
    assert build_tui.call_args.kwargs["existing_mapping"] is None
    assert "Saved workspace mapping does not match the current instances" in console.text


def test_resolve_workspace_context_raises_in_non_interactive_mode_without_valid_mapping(
    monkeypatch: pytest.MonkeyPatch,
):
    source_client = _FakeClient("https://source.example/api/v1")
    dest_client = _FakeClient("https://dest.example/api/v1")
    console = _FakeConsole()

    source_workspaces = [
        {"id": "src-1", "display_name": "Source One"},
        {"id": "src-2", "display_name": "Source Two"},
    ]
    dest_workspaces = [{"id": "dst-1", "display_name": "Dest One"}]
    monkeypatch.setattr(
        workspace_resolver,
        "list_workspaces",
        lambda client: source_workspaces if client is source_client else dest_workspaces,
    )

    with pytest.raises(workspace_resolver.WorkspaceResolutionError):
        workspace_resolver.resolve_workspace_context(
            source_client,
            dest_client,
            console,
            saved_config=None,
            non_interactive=True,
        )

    assert "Workspace mapping is required in non-interactive mode" in console.text
