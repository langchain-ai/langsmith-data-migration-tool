"""Interactive workspace + project mapping builder using Textual TUI.

Presents the workspace mapping as the top-level view.  From there the user
can drill into any *mapped* workspace pair to configure project mappings
within that pair.  The hierarchy is:

    Organization
      └── Workspace  (mapped here)
            └── Projects  (mapped per workspace pair via 'p' key)
                  └── Resources  (migrated after TUI exits)
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Callable, Dict, List, Optional

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.reactive import reactive
from textual.screen import ModalScreen, Screen
from textual.widgets import DataTable, Footer, Header, Input, Static

from ..utils.workspace import get_workspace_name as _ws_label


# ═══════════════════════════════════════════════════════════════════════════
# Data types
# ═══════════════════════════════════════════════════════════════════════════

class WsMappingStatus(Enum):
    UNMAPPED = "unmapped"
    AUTO_MATCHED = "auto-matched"
    MAPPED = "mapped"
    CREATE_NEW = "create-new"
    SKIPPED = "skipped"


@dataclass
class WorkspaceMapping:
    source_id: str
    source_name: str
    dest_id: Optional[str] = None
    dest_name: Optional[str] = None
    status: WsMappingStatus = WsMappingStatus.UNMAPPED
    create_new_name: Optional[str] = None
    project_mapping: Dict[str, str] = field(default_factory=dict)  # src_name -> dst_name


@dataclass
class WorkspaceProjectResult:
    """Everything the caller needs after the TUI exits."""
    workspace_mapping: Dict[str, str]           # src_ws_id -> dst_ws_id
    project_mappings: Dict[str, Dict[str, str]] # src_ws_id -> {src_proj_name: dst_proj_name}
    workspaces_to_create: List[Dict]            # [{source_id, display_name}]


WS_STATUS_STYLES = {
    WsMappingStatus.UNMAPPED: ("", "red"),
    WsMappingStatus.AUTO_MATCHED: ("auto-match", "dim green"),
    WsMappingStatus.MAPPED: ("mapped", "bold green"),
    WsMappingStatus.CREATE_NEW: ("create-new", "cyan"),
    WsMappingStatus.SKIPPED: ("--", "dim"),
}


# ═══════════════════════════════════════════════════════════════════════════
# Modal: pick a destination workspace
# ═══════════════════════════════════════════════════════════════════════════

class WsDestinationPickerScreen(ModalScreen[Optional[str]]):
    """Modal screen for picking a destination workspace."""

    CSS = """
    WsDestinationPickerScreen { align: center middle; }
    #ws-dest-dialog {
        width: 80; max-height: 80%; background: $surface;
        border: thick $accent; padding: 1 2;
    }
    #ws-dest-title { text-align: center; text-style: bold; margin-bottom: 1; }
    #ws-dest-input { width: 100%; margin-bottom: 1; }
    #ws-dest-table { height: 1fr; min-height: 8; }
    #ws-dest-no-matches { text-align: center; color: $text-muted; margin: 1 0; }
    #ws-dest-help { text-align: center; color: $text-muted; margin-top: 1; }
    DataTable > .datatable--cursor { background: $accent; }
    DataTable:focus > .datatable--cursor { background: $accent-darken-1; }
    """

    BINDINGS = [Binding("escape", "cancel", "Cancel", show=True)]

    def __init__(
        self, source_name: str, dest_workspaces: List[Dict], reverse_map: Dict[str, str],
    ) -> None:
        super().__init__()
        self.source_name = source_name
        self.dest_workspaces = dest_workspaces
        self.dest_names = [_ws_label(ws) for ws in dest_workspaces]
        self.reverse_map = reverse_map
        self.filtered_indices: List[int] = list(range(len(self.dest_names)))
        self.filter_text = source_name
        self._filter_timer = None

    def compose(self) -> ComposeResult:
        with Vertical(id="ws-dest-dialog"):
            yield Static(f"Destination for: [bold]{self.source_name}[/bold]", id="ws-dest-title")
            yield Input(value=self.source_name, id="ws-dest-input")
            yield DataTable(id="ws-dest-table", cursor_type="row", zebra_stripes=True)
            yield Static("", id="ws-dest-no-matches")
            yield Static("Enter: select | Up/Down: browse | Esc: cancel", id="ws-dest-help")

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.add_column("Destination Workspace", width=40)
        table.add_column("Mapped From", width=30)
        self._refresh_table()
        inp = self.query_one("#ws-dest-input", Input)
        inp.focus()
        inp.action_select_all()

    def _refresh_table(self) -> None:
        table = self.query_one(DataTable)
        table.clear()
        search = self.filter_text.lower()
        self.filtered_indices = []
        for idx, name in enumerate(self.dest_names):
            if search and search not in name.lower():
                continue
            self.filtered_indices.append(idx)
            mapped_from = self.reverse_map.get(self.dest_workspaces[idx].get("id", ""), "")
            indicator = f"<- {mapped_from}" if mapped_from else ""
            table.add_row(name, indicator, key=str(idx))
        no_matches = self.query_one("#ws-dest-no-matches", Static)
        if search and not self.filtered_indices:
            no_matches.update("[dim](no matching workspaces)[/dim]")
        else:
            no_matches.update("")

    def on_input_changed(self, event: Input.Changed) -> None:
        self.filter_text = event.value
        if self._filter_timer:
            self._filter_timer.stop()
        self._filter_timer = self.set_timer(0.2, self._debounced_refresh)

    def _debounced_refresh(self) -> None:
        self._refresh_table()
        self._filter_timer = None

    def on_input_submitted(self, event: Input.Submitted) -> None:
        table = self.query_one(DataTable)
        cursor = table.cursor_row
        if 0 <= cursor < len(self.filtered_indices):
            idx = self.filtered_indices[cursor]
            self.dismiss(self.dest_workspaces[idx].get("id"))

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        table = self.query_one(DataTable)
        cursor = table.cursor_row
        if 0 <= cursor < len(self.filtered_indices):
            idx = self.filtered_indices[cursor]
            self.dismiss(self.dest_workspaces[idx].get("id"))

    def action_cancel(self) -> None:
        self.dismiss(None)


# ═══════════════════════════════════════════════════════════════════════════
# Modal: create a new workspace on the destination
# ═══════════════════════════════════════════════════════════════════════════

class CreateWorkspaceScreen(ModalScreen[Optional[str]]):
    CSS = """
    CreateWorkspaceScreen { align: center middle; }
    #create-ws-dialog {
        width: 60; height: auto; background: $surface;
        border: thick $accent; padding: 1 2;
    }
    #create-ws-title { text-align: center; text-style: bold; margin-bottom: 1; }
    #create-ws-hint { color: $text-muted; text-align: center; margin-top: 1; }
    #create-ws-help { text-align: center; color: $text-muted; margin-top: 1; }
    """
    BINDINGS = [Binding("escape", "cancel", "Cancel", show=True)]

    def __init__(self, source_name: str) -> None:
        super().__init__()
        self.source_name = source_name

    def compose(self) -> ComposeResult:
        with Vertical(id="create-ws-dialog"):
            yield Static(f"Create New Workspace for: [bold]{self.source_name}[/bold]", id="create-ws-title")
            yield Input(value=self.source_name, id="create-ws-input")
            yield Static("(This workspace will be created on the destination)", id="create-ws-hint")
            yield Static("Enter: confirm | Esc: cancel", id="create-ws-help")

    def on_mount(self) -> None:
        inp = self.query_one("#create-ws-input", Input)
        inp.focus()
        inp.action_select_all()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        value = event.value.strip()
        if value:
            self.dismiss(value)

    def action_cancel(self) -> None:
        self.dismiss(None)


# ═══════════════════════════════════════════════════════════════════════════
# Screen: per-workspace project mapping (pushed from main screen)
# ═══════════════════════════════════════════════════════════════════════════

class ProjectMappingScreen(Screen[Optional[Dict[str, str]]]):
    """Full-screen project mapper scoped to one workspace pair.

    Re-uses the text-input-first pattern from tui_project_mapper.py but lives
    as a *Screen* inside the workspace-mapper App so we stay in-process.
    """

    CSS = """
    #pm-header { dock: top; height: auto; background: $panel; padding: 0 1; }
    #pm-ws-label { text-style: bold; }
    #pm-help { color: $text-muted; }
    #pm-table { height: 1fr; }
    #pm-stats { dock: bottom; height: 3; background: $panel; padding: 0 1; content-align: center middle; }
    DataTable > .datatable--cursor { background: $accent; }
    DataTable:focus > .datatable--cursor { background: $accent-darken-1; }
    """

    BINDINGS = [
        Binding("enter", "assign", "Edit destination", show=True),
        Binding("s", "skip", "Skip", show=True, priority=True),
        Binding("m", "same_name", "Same name", show=True, priority=True),
        Binding("u", "unmap", "Unmap", show=True, priority=True),
        Binding("a", "auto_match_all", "Auto-match all", show=True, priority=True),
        Binding("ctrl+s", "confirm", "Save", show=True),
        Binding("escape", "back", "Back", show=True),
    ]

    class _ProjStatus(Enum):
        UNMAPPED = "unmapped"
        AUTO_MATCHED = "auto-matched"
        MAPPED = "mapped"
        SAME_NAME = "same-name"
        SKIPPED = "skipped"

    _STATUS_STYLES = {
        _ProjStatus.UNMAPPED: ("", "red"),
        _ProjStatus.AUTO_MATCHED: ("auto-match", "dim green"),
        _ProjStatus.MAPPED: ("mapped", "bold green"),
        _ProjStatus.SAME_NAME: ("same-name", "yellow"),
        _ProjStatus.SKIPPED: ("--", "dim"),
    }

    @dataclass
    class _PM:
        source_name: str
        dest_name: Optional[str] = None
        status: "ProjectMappingScreen._ProjStatus" = None  # type: ignore[assignment]

        def __post_init__(self):
            if self.status is None:
                self.status = ProjectMappingScreen._ProjStatus.UNMAPPED

    def __init__(
        self,
        source_ws_name: str,
        dest_ws_name: str,
        source_projects: List[Dict],
        dest_projects: List[Dict],
        existing_mapping: Optional[Dict[str, str]] = None,
    ) -> None:
        super().__init__()
        self.source_ws_name = source_ws_name
        self.dest_ws_name = dest_ws_name

        self.src_names = sorted({p["name"] for p in source_projects if "name" in p})
        self.dest_names = sorted({p["name"] for p in dest_projects if "name" in p})
        self.dest_name_set = set(self.dest_names)
        existing = existing_mapping or {}

        self.mappings: List[ProjectMappingScreen._PM] = []
        for name in self.src_names:
            if name in existing:
                dest = existing[name]
                st = (
                    self._status_for_same(name, dest)
                    if dest == name
                    else self._ProjStatus.MAPPED
                )
                self.mappings.append(self._PM(name, dest, st))
            elif name in self.dest_name_set:
                self.mappings.append(self._PM(name, name, self._ProjStatus.AUTO_MATCHED))
            else:
                self.mappings.append(self._PM(name))

        self.filtered_indices: List[int] = list(range(len(self.mappings)))
        self.result: Optional[Dict[str, str]] = None
        self._pending_assign_idx: Optional[int] = None

    # -- helpers --

    def _status_for_same(self, name: str, dest: str) -> _ProjStatus:
        return self._ProjStatus.AUTO_MATCHED if name in self.dest_name_set else self._ProjStatus.SAME_NAME

    def _modal_is_active(self) -> bool:
        return isinstance(self.app.screen, ModalScreen)

    def _current_index(self) -> Optional[int]:
        table = self.query_one("#pm-table", DataTable)
        cursor = table.cursor_row
        if 0 <= cursor < len(self.filtered_indices):
            return self.filtered_indices[cursor]
        return None

    def _reverse_map(self) -> Dict[str, str]:
        return {
            m.dest_name: m.source_name
            for m in self.mappings
            if m.dest_name and m.status not in (self._ProjStatus.UNMAPPED, self._ProjStatus.SKIPPED)
        }

    def _row_for_index(self, index: int) -> Optional[int]:
        try:
            return self.filtered_indices.index(index)
        except ValueError:
            return None

    # -- compose / mount --

    def compose(self) -> ComposeResult:
        with Vertical(id="pm-header"):
            yield Static(
                f"Projects: [bold cyan]{self.source_ws_name}[/bold cyan] -> [bold cyan]{self.dest_ws_name}[/bold cyan]",
                id="pm-ws-label",
            )
            yield Static(
                "Enter: edit dest | s: skip | m: same name | u: unmap | a: auto-match | Ctrl+S: save | Esc: back",
                id="pm-help",
            )
        yield DataTable(id="pm-table", cursor_type="row", zebra_stripes=True, show_cursor=True)
        yield Static("", id="pm-stats")

    def on_mount(self) -> None:
        table = self.query_one("#pm-table", DataTable)
        table.add_column("Source Project", width=35)
        table.add_column("Destination", width=35)
        table.add_column("Status", width=15)
        self._refresh_table()
        self._update_stats()
        self.set_focus(table)

    # -- table rendering --

    def _refresh_table(self) -> None:
        table = self.query_one("#pm-table", DataTable)
        table.clear()
        self.filtered_indices = []
        for idx, m in enumerate(self.mappings):
            self.filtered_indices.append(idx)
            _label, style = self._STATUS_STYLES[m.status]
            dest_display = m.dest_name or _label
            status_display = f"[{style}]{m.status.value}[/{style}]"
            table.add_row(m.source_name, dest_display, status_display, key=str(idx))

    def _update_stats(self) -> None:
        mapped = skipped = unmapped = 0
        for m in self.mappings:
            if m.status in (self._ProjStatus.MAPPED, self._ProjStatus.AUTO_MATCHED, self._ProjStatus.SAME_NAME):
                mapped += 1
            elif m.status == self._ProjStatus.SKIPPED:
                skipped += 1
            else:
                unmapped += 1
        self.query_one("#pm-stats", Static).update(
            f"Mapped: {mapped} | Skipped: {skipped} | Unmapped: {unmapped} | Total: {len(self.mappings)}"
        )

    def _refresh_and_stats(self, preserve_index: Optional[int] = None) -> None:
        if preserve_index is None:
            preserve_index = self._current_index()
        self._refresh_table()
        self._update_stats()
        if preserve_index is None:
            return
        row = self._row_for_index(preserve_index)
        if row is not None:
            self.query_one("#pm-table", DataTable).move_cursor(row=row)

    # -- key actions --

    def action_assign(self) -> None:
        if self._modal_is_active():
            return
        idx = self._current_index()
        if idx is None:
            return
        self._pending_assign_idx = idx
        m = self.mappings[idx]
        self.app.push_screen(
            _ProjectDestPickerScreen(m.source_name, self.dest_names, self._reverse_map()),
            callback=self._on_dest_picked,
        )

    def _on_dest_picked(self, result: Optional[str]) -> None:
        idx = self._pending_assign_idx
        if idx is None or result is None:
            return
        m = self.mappings[idx]
        m.dest_name = result
        m.status = self._status_for_same(m.source_name, result) if result == m.source_name else self._ProjStatus.MAPPED
        self._refresh_and_stats(preserve_index=idx)

    def action_skip(self) -> None:
        if self._modal_is_active():
            return
        idx = self._current_index()
        if idx is None:
            return
        m = self.mappings[idx]
        m.dest_name = None
        m.status = self._ProjStatus.SKIPPED
        self._refresh_and_stats(preserve_index=idx)

    def action_same_name(self) -> None:
        if self._modal_is_active():
            return
        idx = self._current_index()
        if idx is None:
            return
        m = self.mappings[idx]
        m.dest_name = m.source_name
        m.status = self._status_for_same(m.source_name, m.source_name)
        self._refresh_and_stats(preserve_index=idx)

    def action_unmap(self) -> None:
        if self._modal_is_active():
            return
        idx = self._current_index()
        if idx is None:
            return
        m = self.mappings[idx]
        m.dest_name = None
        m.status = self._ProjStatus.UNMAPPED
        self._refresh_and_stats(preserve_index=idx)

    def action_auto_match_all(self) -> None:
        if self._modal_is_active():
            return
        for m in self.mappings:
            if m.status == self._ProjStatus.UNMAPPED and m.source_name in self.dest_name_set:
                m.dest_name = m.source_name
                m.status = self._ProjStatus.AUTO_MATCHED
        self._refresh_and_stats()

    def action_confirm(self) -> None:
        if self._modal_is_active():
            return
        result: Dict[str, str] = {}
        for m in self.mappings:
            if m.dest_name and m.status not in (self._ProjStatus.UNMAPPED, self._ProjStatus.SKIPPED):
                result[m.source_name] = m.dest_name
        self.dismiss(result)

    def action_back(self) -> None:
        if self._modal_is_active():
            return
        self.dismiss(None)


# ---------------------------------------------------------------------------
# Modal: project destination picker (used inside ProjectMappingScreen)
# ---------------------------------------------------------------------------

class _ProjectDestPickerScreen(ModalScreen[Optional[str]]):
    """Pick or type a destination project name."""

    CSS = """
    _ProjectDestPickerScreen { align: center middle; }
    #pdp-dialog {
        width: 80; max-height: 80%; background: $surface;
        border: thick $accent; padding: 1 2;
    }
    #pdp-title { text-align: center; text-style: bold; margin-bottom: 1; }
    #pdp-input { width: 100%; margin-bottom: 1; }
    #pdp-table { height: 1fr; min-height: 8; }
    #pdp-no-matches { text-align: center; color: $text-muted; margin: 1 0; }
    #pdp-help { text-align: center; color: $text-muted; margin-top: 1; }
    DataTable > .datatable--cursor { background: $accent; }
    DataTable:focus > .datatable--cursor { background: $accent-darken-1; }
    """
    BINDINGS = [Binding("escape", "cancel", "Cancel", show=True)]

    def __init__(self, source_name: str, dest_names: List[str], reverse_map: Dict[str, str]) -> None:
        super().__init__()
        self.source_name = source_name
        self.dest_names = dest_names
        self.reverse_map = reverse_map
        self.filtered_indices: List[int] = list(range(len(dest_names)))
        self.filter_text = source_name
        self._filter_timer = None

    def compose(self) -> ComposeResult:
        with Vertical(id="pdp-dialog"):
            yield Static(f"Destination for: [bold]{self.source_name}[/bold]", id="pdp-title")
            yield Input(value=self.source_name, id="pdp-input")
            yield DataTable(id="pdp-table", cursor_type="row", zebra_stripes=True)
            yield Static("", id="pdp-no-matches")
            yield Static("Enter: confirm name | Up/Down: browse | Esc: cancel", id="pdp-help")

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.add_column("Destination Project", width=40)
        table.add_column("Mapped From", width=30)
        self._refresh_table()
        inp = self.query_one("#pdp-input", Input)
        inp.focus()
        inp.action_select_all()

    def _refresh_table(self) -> None:
        table = self.query_one(DataTable)
        table.clear()
        search = self.filter_text.lower()
        self.filtered_indices = []
        for idx, name in enumerate(self.dest_names):
            if search and search not in name.lower():
                continue
            self.filtered_indices.append(idx)
            mapped_from = self.reverse_map.get(name, "")
            indicator = f"<- {mapped_from}" if mapped_from else ""
            table.add_row(name, indicator, key=str(idx))
        no_matches = self.query_one("#pdp-no-matches", Static)
        if search and not self.filtered_indices:
            no_matches.update("[dim](no matching projects — Enter to use as new name)[/dim]")
        else:
            no_matches.update("")

    def on_input_changed(self, event: Input.Changed) -> None:
        self.filter_text = event.value
        if self._filter_timer:
            self._filter_timer.stop()
        self._filter_timer = self.set_timer(0.2, self._debounced_refresh)

    def _debounced_refresh(self) -> None:
        self._refresh_table()
        self._filter_timer = None

    def on_input_submitted(self, event: Input.Submitted) -> None:
        value = event.value.strip()
        if value:
            self.dismiss(value)

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        table = self.query_one(DataTable)
        cursor = table.cursor_row
        if 0 <= cursor < len(self.filtered_indices):
            idx = self.filtered_indices[cursor]
            inp = self.query_one("#pdp-input", Input)
            inp.value = self.dest_names[idx]
            inp.focus()
            inp.action_end()

    def action_cancel(self) -> None:
        self.dismiss(None)


# ═══════════════════════════════════════════════════════════════════════════
# Main App: combined Workspace + Project Mapper
# ═══════════════════════════════════════════════════════════════════════════

class WorkspaceMapperApp(App):
    """Full-screen TUI for mapping source workspaces to destination workspaces,
    with drill-down project mapping per workspace pair.

    Hierarchy shown to user:
        Workspace (this table) -> press 'p' -> Projects (pushed screen)
    """

    CSS = """
    Screen { background: $surface; }
    #ws-search-container { dock: top; height: auto; background: $panel; padding: 0 1; }
    #ws-help-text { color: $text-muted; }
    #ws-main-table { height: 1fr; }
    #ws-stats { dock: bottom; height: 3; background: $panel; padding: 0 1; content-align: center middle; }
    DataTable > .datatable--cursor { background: $accent; }
    DataTable:focus > .datatable--cursor { background: $accent-darken-1; }
    """

    BINDINGS = [
        Binding("enter", "assign", "Pick destination", show=True),
        Binding("p", "map_projects", "Map projects", show=True, priority=True),
        Binding("n", "create_new", "Create new", show=True, priority=True),
        Binding("s", "skip", "Skip", show=True, priority=True),
        Binding("a", "auto_match_all", "Auto-match all", show=True, priority=True),
        Binding("c", "create_all_new", "Create all unmapped", show=True, priority=True),
        Binding("u", "unmap", "Unmap", show=True, priority=True),
        Binding("ctrl+s", "confirm", "Save", show=True),
        Binding("escape", "quit_app", "Cancel", show=True),
        Binding("q", "quit_app", "Quit", show=False),
    ]

    filter_text: reactive[str] = reactive("")

    def __init__(
        self,
        source_workspaces: List[Dict],
        dest_workspaces: List[Dict],
        fetch_projects: Optional[Callable[[str, str], List[Dict]]] = None,
        existing_mapping: Optional[Dict[str, str]] = None,
        existing_project_mappings: Optional[Dict[str, Dict[str, str]]] = None,
    ) -> None:
        super().__init__()
        self.source_workspaces = source_workspaces
        self.dest_workspaces = dest_workspaces
        self.fetch_projects = fetch_projects  # (ws_id, "source"|"dest") -> [project dicts]

        # Lookup tables
        self.dest_id_to_ws = {ws.get("id", ""): ws for ws in dest_workspaces}
        self.dest_name_set = {_ws_label(ws).lower() for ws in dest_workspaces}
        self.dest_id_by_name: Dict[str, str] = {}
        for ws in dest_workspaces:
            self.dest_id_by_name[_ws_label(ws).lower()] = ws.get("id", "")

        existing = existing_mapping or {}
        existing_pm = existing_project_mappings or {}

        self.mappings: List[WorkspaceMapping] = []
        for ws in source_workspaces:
            src_id = ws.get("id", "")
            src_name = _ws_label(ws)
            pm = existing_pm.get(src_id, {})
            if src_id in existing:
                dest_id = existing[src_id]
                dest_ws = self.dest_id_to_ws.get(dest_id)
                dest_name = _ws_label(dest_ws) if dest_ws else dest_id
                self.mappings.append(WorkspaceMapping(
                    src_id, src_name, dest_id, dest_name,
                    WsMappingStatus.MAPPED, project_mapping=pm,
                ))
            elif src_name.lower() in self.dest_id_by_name:
                dest_id = self.dest_id_by_name[src_name.lower()]
                dest_ws = self.dest_id_to_ws.get(dest_id)
                dest_name = _ws_label(dest_ws) if dest_ws else dest_id
                self.mappings.append(WorkspaceMapping(
                    src_id, src_name, dest_id, dest_name,
                    WsMappingStatus.AUTO_MATCHED, project_mapping=pm,
                ))
            else:
                self.mappings.append(WorkspaceMapping(src_id, src_name, project_mapping=pm))

        self.filtered_indices: List[int] = list(range(len(self.mappings)))
        self.result: Optional[WorkspaceProjectResult] = None
        self._filter_timer = None
        self._pending_assign_idx: Optional[int] = None
        self._pending_create_idx: Optional[int] = None

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical(id="ws-search-container"):
            yield Input(placeholder="Search workspaces...", id="ws-search-input")
            yield Static(
                "Enter: pick dest | p: map projects | n: create new | c: create all | s: skip | a: auto-match | u: unmap | Ctrl+S: save | Esc: cancel",
                id="ws-help-text",
            )
        yield DataTable(id="ws-main-table", cursor_type="row", zebra_stripes=True, show_cursor=True)
        yield Static("", id="ws-stats")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.add_column("Source Workspace", width=30)
        table.add_column("Destination", width=30)
        table.add_column("Status", width=14)
        table.add_column("Projects", width=10)
        self._refresh_table()
        self._update_stats()
        self.set_focus(table)

    # -- table rendering --

    def _refresh_table(self) -> None:
        table = self.query_one(DataTable)
        table.clear()
        search = self.filter_text.lower()
        self.filtered_indices = []

        for idx, m in enumerate(self.mappings):
            if search and search not in m.source_name.lower():
                continue
            self.filtered_indices.append(idx)

            _label, style = WS_STATUS_STYLES[m.status]
            if m.status == WsMappingStatus.CREATE_NEW:
                dest_display = f"[NEW] {m.create_new_name or m.source_name}"
            else:
                dest_display = m.dest_name or _label
            status_display = f"[{style}]{m.status.value}[/{style}]"

            proj_count = len(m.project_mapping) if m.project_mapping else 0
            proj_display = str(proj_count) if proj_count else "[dim]-[/dim]"

            table.add_row(m.source_name, dest_display, status_display, proj_display, key=str(idx))

    def _update_stats(self) -> None:
        mapped = skipped = unmapped = create_new = total_projects = 0
        for m in self.mappings:
            if m.status in (WsMappingStatus.MAPPED, WsMappingStatus.AUTO_MATCHED):
                mapped += 1
            elif m.status == WsMappingStatus.SKIPPED:
                skipped += 1
            elif m.status == WsMappingStatus.CREATE_NEW:
                create_new += 1
            elif m.status == WsMappingStatus.UNMAPPED:
                unmapped += 1
            total_projects += len(m.project_mapping) if m.project_mapping else 0
        total = len(self.mappings)
        self.query_one("#ws-stats", Static).update(
            f"Mapped: {mapped} | Create new: {create_new} | Skipped: {skipped} | "
            f"Unmapped: {unmapped} | Total: {total} | Project mappings: {total_projects}"
        )

    # -- helpers --

    def _modal_is_active(self) -> bool:
        return isinstance(self.screen, ModalScreen)

    def _current_index(self) -> Optional[int]:
        table = self.query_one(DataTable)
        cursor = table.cursor_row
        if 0 <= cursor < len(self.filtered_indices):
            return self.filtered_indices[cursor]
        return None

    def _reverse_map(self) -> Dict[str, str]:
        return {
            m.dest_id: m.source_name
            for m in self.mappings
            if m.dest_id and m.status in (WsMappingStatus.MAPPED, WsMappingStatus.AUTO_MATCHED)
        }

    def _row_for_index(self, index: int) -> Optional[int]:
        try:
            return self.filtered_indices.index(index)
        except ValueError:
            return None

    def _refresh_and_stats(self, preserve_index: Optional[int] = None) -> None:
        if preserve_index is None:
            preserve_index = self._current_index()
        self._refresh_table()
        self._update_stats()
        if preserve_index is None:
            return
        row = self._row_for_index(preserve_index)
        if row is not None:
            self.query_one("#ws-main-table", DataTable).move_cursor(row=row)

    # -- search --

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id != "ws-search-input":
            return
        self.filter_text = event.value
        if self._filter_timer:
            self._filter_timer.stop()
        self._filter_timer = self.set_timer(0.3, self._debounced_refresh)

    def _debounced_refresh(self) -> None:
        self._refresh_table()
        self._update_stats()
        table = self.query_one(DataTable)
        self.set_focus(table)
        self._filter_timer = None

    # -- key actions --

    def action_assign(self) -> None:
        if self._modal_is_active():
            return
        idx = self._current_index()
        if idx is None:
            return
        m = self.mappings[idx]
        self._pending_assign_idx = idx
        self.push_screen(
            WsDestinationPickerScreen(m.source_name, self.dest_workspaces, self._reverse_map()),
            callback=self._on_dest_picked,
        )

    def _on_dest_picked(self, result: Optional[str]) -> None:
        idx = self._pending_assign_idx
        if idx is None or result is None:
            return
        m = self.mappings[idx]
        m.dest_id = result
        dest_ws = self.dest_id_to_ws.get(result)
        m.dest_name = _ws_label(dest_ws) if dest_ws else result
        m.status = WsMappingStatus.MAPPED
        m.create_new_name = None
        self._refresh_and_stats(preserve_index=idx)

    def action_create_new(self) -> None:
        if self._modal_is_active():
            return
        idx = self._current_index()
        if idx is None:
            return
        m = self.mappings[idx]
        self._pending_create_idx = idx
        self.push_screen(
            CreateWorkspaceScreen(m.source_name),
            callback=self._on_create_name,
        )

    def _on_create_name(self, result: Optional[str]) -> None:
        idx = self._pending_create_idx
        if idx is None or result is None:
            return
        m = self.mappings[idx]
        m.status = WsMappingStatus.CREATE_NEW
        m.create_new_name = result
        m.dest_id = None
        m.dest_name = None
        self._refresh_and_stats(preserve_index=idx)

    def action_map_projects(self) -> None:
        """Drill into project mapping for the selected workspace pair."""
        if self._modal_is_active():
            return
        idx = self._current_index()
        if idx is None:
            return
        m = self.mappings[idx]

        if m.status in (WsMappingStatus.UNMAPPED, WsMappingStatus.SKIPPED):
            self.notify("Map or create a destination workspace first", severity="warning")
            return

        if not self.fetch_projects:
            self.notify("Project fetching not available", severity="warning")
            return

        # Fetch projects scoped to each workspace
        try:
            src_projects = self.fetch_projects(m.source_id, "source")
            dest_ws_id = m.dest_id or ""
            dst_projects = self.fetch_projects(dest_ws_id, "dest") if dest_ws_id else []
        except Exception as e:
            self.notify(f"Failed to fetch projects: {e}", severity="error")
            return

        dest_name = m.dest_name or m.create_new_name or m.source_name
        self._pending_assign_idx = idx
        self.push_screen(
            ProjectMappingScreen(
                m.source_name, dest_name, src_projects, dst_projects, m.project_mapping,
            ),
            callback=self._on_project_mapping_done,
        )

    def _on_project_mapping_done(self, result: Optional[Dict[str, str]]) -> None:
        idx = self._pending_assign_idx
        if idx is None or result is None:
            return
        self.mappings[idx].project_mapping = result
        self._refresh_and_stats(preserve_index=idx)

    def action_skip(self) -> None:
        if self._modal_is_active():
            return
        idx = self._current_index()
        if idx is None:
            return
        m = self.mappings[idx]
        m.dest_id = None
        m.dest_name = None
        m.create_new_name = None
        m.status = WsMappingStatus.SKIPPED
        self._refresh_and_stats(preserve_index=idx)

    def action_unmap(self) -> None:
        if self._modal_is_active():
            return
        idx = self._current_index()
        if idx is None:
            return
        m = self.mappings[idx]
        m.dest_id = None
        m.dest_name = None
        m.create_new_name = None
        m.status = WsMappingStatus.UNMAPPED
        self._refresh_and_stats(preserve_index=idx)

    def action_auto_match_all(self) -> None:
        if self._modal_is_active():
            return
        for m in self.mappings:
            if m.status == WsMappingStatus.UNMAPPED and m.source_name.lower() in self.dest_id_by_name:
                dest_id = self.dest_id_by_name[m.source_name.lower()]
                dest_ws = self.dest_id_to_ws.get(dest_id)
                m.dest_id = dest_id
                m.dest_name = _ws_label(dest_ws) if dest_ws else dest_id
                m.status = WsMappingStatus.AUTO_MATCHED
        self._refresh_and_stats()

    def action_create_all_new(self) -> None:
        if self._modal_is_active():
            return
        for m in self.mappings:
            if m.status == WsMappingStatus.UNMAPPED:
                m.status = WsMappingStatus.CREATE_NEW
                m.create_new_name = m.source_name
                m.dest_id = None
                m.dest_name = None
        self._refresh_and_stats()

    def action_confirm(self) -> None:
        if self._modal_is_active():
            return
        ws_mapping: Dict[str, str] = {}
        project_mappings: Dict[str, Dict[str, str]] = {}
        workspaces_to_create: List[Dict] = []

        for m in self.mappings:
            if m.status in (WsMappingStatus.MAPPED, WsMappingStatus.AUTO_MATCHED) and m.dest_id:
                ws_mapping[m.source_id] = m.dest_id
                if m.project_mapping:
                    project_mappings[m.source_id] = m.project_mapping
            elif m.status == WsMappingStatus.CREATE_NEW and m.create_new_name:
                workspaces_to_create.append({
                    "source_id": m.source_id,
                    "display_name": m.create_new_name,
                })
                if m.project_mapping:
                    project_mappings[m.source_id] = m.project_mapping

        self.result = WorkspaceProjectResult(ws_mapping, project_mappings, workspaces_to_create)
        self.exit()

    def action_quit_app(self) -> None:
        if self._modal_is_active():
            return
        self.result = None
        self.exit()


# ═══════════════════════════════════════════════════════════════════════════
# Public API
# ═══════════════════════════════════════════════════════════════════════════

def build_workspace_mapping_tui(
    source_workspaces: List[Dict],
    dest_workspaces: List[Dict],
    fetch_projects: Optional[Callable[[str, str], List[Dict]]] = None,
    existing_mapping: Optional[Dict[str, str]] = None,
    existing_project_mappings: Optional[Dict[str, Dict[str, str]]] = None,
) -> Optional[WorkspaceProjectResult]:
    """Launch the interactive workspace + project mapper TUI.

    Args:
        source_workspaces: List of source workspace dicts (must have 'id').
        dest_workspaces: List of destination workspace dicts (must have 'id').
        fetch_projects: Callback ``(ws_id, "source"|"dest") -> [project dicts]``.
            If None, the 'p' key is disabled.
        existing_mapping: Pre-existing source_ws_id -> dest_ws_id mapping.
        existing_project_mappings: Pre-existing per-workspace project mappings.

    Returns:
        WorkspaceProjectResult or None if cancelled.
    """
    if not source_workspaces:
        return WorkspaceProjectResult({}, {}, [])

    app = WorkspaceMapperApp(
        source_workspaces, dest_workspaces, fetch_projects,
        existing_mapping, existing_project_mappings,
    )
    app.title = "Workspace & Project Mapper"
    app.sub_title = "Map workspaces, then press 'p' to map projects within each pair"
    app.run()
    return app.result
