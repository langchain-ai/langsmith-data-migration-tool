"""Unit tests for workspace mapper cursor behavior."""

from langsmith_migrator.cli.tui_workspace_mapper import (
    ProjectMappingScreen,
    WorkspaceMapperApp,
    WsMappingStatus,
)


class _FakeTable:
    def __init__(self, cursor_row: int) -> None:
        self.cursor_row = cursor_row
        self.moved_to_row = None

    def move_cursor(self, row: int) -> None:
        self.moved_to_row = row


def test_skip_preserves_workspace_row_position(monkeypatch):
    source_workspaces = [
        {"id": "src-1", "name": "Source A"},
        {"id": "src-2", "name": "Source B"},
        {"id": "src-3", "name": "Source C"},
    ]
    app = WorkspaceMapperApp(source_workspaces=source_workspaces, dest_workspaces=[])
    app.filtered_indices = [0, 1, 2]

    fake_table = _FakeTable(cursor_row=1)

    monkeypatch.setattr(app, "_modal_is_active", lambda: False)
    monkeypatch.setattr(app, "_refresh_table", lambda: None)
    monkeypatch.setattr(app, "_update_stats", lambda: None)
    monkeypatch.setattr(app, "query_one", lambda *args, **kwargs: fake_table)

    app.action_skip()

    assert app.mappings[1].status == WsMappingStatus.SKIPPED
    assert fake_table.moved_to_row == 1


def test_skip_preserves_project_row_position(monkeypatch):
    source_projects = [{"name": "Project A"}, {"name": "Project B"}, {"name": "Project C"}]
    screen = ProjectMappingScreen(
        source_ws_name="Source Workspace",
        dest_ws_name="Dest Workspace",
        source_projects=source_projects,
        dest_projects=[],
    )
    screen.filtered_indices = [0, 1, 2]

    fake_table = _FakeTable(cursor_row=1)

    monkeypatch.setattr(screen, "_modal_is_active", lambda: False)
    monkeypatch.setattr(screen, "_refresh_table", lambda: None)
    monkeypatch.setattr(screen, "_update_stats", lambda: None)
    monkeypatch.setattr(screen, "query_one", lambda *args, **kwargs: fake_table)

    screen.action_skip()

    assert screen.mappings[1].status == screen._ProjStatus.SKIPPED
    assert fake_table.moved_to_row == 1
