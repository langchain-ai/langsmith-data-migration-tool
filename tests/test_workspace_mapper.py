"""Unit tests for workspace mapper cursor behavior."""

from types import SimpleNamespace

from langsmith_migrator.cli.tui_workspace_mapper import (
    ProjectMappingScreen,
    WorkspaceMapperApp,
    WsDestinationPickerScreen,
    WsMappingStatus,
    _step_table_cursor,
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


def test_enter_on_workspace_table_row_opens_destination_picker(monkeypatch):
    """RowSelected from the main table (Enter key) must trigger action_assign."""
    app = WorkspaceMapperApp(
        source_workspaces=[{"id": "src-1", "name": "Source A"}], dest_workspaces=[]
    )
    calls = []
    monkeypatch.setattr(app, "action_assign", lambda: calls.append("assign"))

    app.on_data_table_row_selected(
        SimpleNamespace(data_table=SimpleNamespace(id="ws-main-table"))
    )

    assert calls == ["assign"]


def test_workspace_row_selected_ignores_other_tables(monkeypatch):
    """RowSelected events bubbling from modal picker tables must be ignored."""
    app = WorkspaceMapperApp(
        source_workspaces=[{"id": "src-1", "name": "Source A"}], dest_workspaces=[]
    )
    calls = []
    monkeypatch.setattr(app, "action_assign", lambda: calls.append("assign"))

    app.on_data_table_row_selected(
        SimpleNamespace(data_table=SimpleNamespace(id="ws-dest-table"))
    )

    assert calls == []


def test_enter_on_project_mapping_table_row_opens_destination_picker(monkeypatch):
    """RowSelected from the project mapping table (Enter key) must trigger action_assign."""
    screen = ProjectMappingScreen(
        source_ws_name="Source Workspace",
        dest_ws_name="Dest Workspace",
        source_projects=[{"name": "Project A"}],
        dest_projects=[],
    )
    calls = []
    monkeypatch.setattr(screen, "action_assign", lambda: calls.append("assign"))

    screen.on_data_table_row_selected(SimpleNamespace(data_table=SimpleNamespace(id="pm-table")))
    screen.on_data_table_row_selected(SimpleNamespace(data_table=SimpleNamespace(id="pdp-table")))

    assert calls == ["assign"]


class _FakePickerWidgets:
    """Stand-ins for the picker's DataTable and no-matches Static."""

    def __init__(self) -> None:
        self.rows = []
        self.message = None

    def clear(self) -> None:
        self.rows = []

    def add_row(self, *cells, key=None) -> None:
        self.rows.append(cells)

    def update(self, text) -> None:
        self.message = text


def _picker_refresh(picker: WsDestinationPickerScreen) -> _FakePickerWidgets:
    widgets = _FakePickerWidgets()
    picker.query_one = lambda *args, **kwargs: widgets  # type: ignore[method-assign]
    picker._refresh_table()
    return widgets


def test_destination_picker_explains_empty_destination_list():
    picker = WsDestinationPickerScreen("Development", [], {})

    widgets = _picker_refresh(picker)

    assert widgets.rows == []
    assert "no destination workspaces found" in widgets.message


def test_destination_picker_opens_unfiltered():
    """The picker must not pre-fill the search with the source name."""
    picker = WsDestinationPickerScreen(
        "Development", [{"id": "dst-1", "name": "Production"}], {}
    )

    widgets = _picker_refresh(picker)

    assert picker.filter_text == ""
    assert len(widgets.rows) == 1
    assert widgets.message == ""


def test_destination_picker_explains_filter_with_no_matches():
    picker = WsDestinationPickerScreen(
        "Development", [{"id": "dst-1", "name": "Production"}], {}
    )
    picker.filter_text = "staging"

    widgets = _picker_refresh(picker)

    assert widgets.rows == []
    assert "clear the search to see all 1 workspaces" in widgets.message


def test_step_table_cursor_moves_and_clamps():
    table = _FakeTable(cursor_row=0)
    table.row_count = 3

    _step_table_cursor(table, 1)
    assert table.moved_to_row == 1

    table.cursor_row = 2
    _step_table_cursor(table, 1)
    assert table.moved_to_row == 2  # clamped at last row

    table.cursor_row = 0
    _step_table_cursor(table, -1)
    assert table.moved_to_row == 0  # clamped at first row


def test_step_table_cursor_ignores_empty_table():
    table = _FakeTable(cursor_row=0)
    table.row_count = 0

    _step_table_cursor(table, 1)

    assert table.moved_to_row is None


def test_destination_picker_arrow_actions_move_table_cursor():
    picker = WsDestinationPickerScreen(
        "Development", [{"id": "dst-1", "name": "Demo"}, {"id": "dst-2", "name": "Prod"}], {}
    )
    table = _FakeTable(cursor_row=0)
    table.row_count = 2
    picker.query_one = lambda *args, **kwargs: table  # type: ignore[method-assign]

    picker.action_cursor_down()
    assert table.moved_to_row == 1

    table.cursor_row = 1
    picker.action_cursor_up()
    assert table.moved_to_row == 0


def test_destination_picker_shows_rows_when_filter_matches():
    picker = WsDestinationPickerScreen(
        "Development", [{"id": "dst-1", "name": "Development EU"}], {}
    )
    picker.filter_text = "development"

    widgets = _picker_refresh(picker)

    assert len(widgets.rows) == 1
    assert widgets.message == ""
