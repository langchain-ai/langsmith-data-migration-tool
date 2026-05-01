"""Interactive project mapping builder using Textual TUI."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional

from textual.app import App, ComposeResult
from textual.binding import Binding
from textual.containers import Vertical
from textual.reactive import reactive
from textual.screen import ModalScreen
from textual.widgets import DataTable, Footer, Header, Input, Static


class MappingStatus(Enum):
    UNMAPPED = "unmapped"
    AUTO_MATCHED = "auto-matched"
    MAPPED = "mapped"
    SAME_NAME = "same-name"
    SKIPPED = "skipped"


@dataclass
class ProjectMapping:
    source_name: str
    dest_name: Optional[str] = None
    status: MappingStatus = MappingStatus.UNMAPPED
    source_id: Optional[str] = None
    source_label: Optional[str] = None
    dest_id: Optional[str] = None
    dest_label: Optional[str] = None


def _short_id(project_id: Optional[str]) -> str:
    """Return a compact ID prefix for display."""
    return str(project_id)[:8] if project_id else "no-id"


def _workspace_ids_from_value(value) -> set[str]:
    """Extract workspace IDs from common project metadata shapes."""
    if isinstance(value, str):
        return {value} if value else set()
    if isinstance(value, dict):
        ids = set()
        for key in ("id", "tenant_id", "workspace_id"):
            nested = value.get(key)
            if isinstance(nested, str) and nested:
                ids.add(nested)
        return ids
    if isinstance(value, (list, tuple, set)):
        ids = set()
        for item in value:
            ids.update(_workspace_ids_from_value(item))
        return ids
    return set()


def _project_workspace_ids(project: Dict) -> list[str]:
    """Return sorted workspace IDs advertised on a project/session record."""
    ids = set()
    for key in ("tenant_id", "workspace_id", "workspace_ids", "tenant", "workspace"):
        if key in project:
            ids.update(_workspace_ids_from_value(project[key]))
    return sorted(ids)


def _project_label(project: Dict) -> str:
    """Display a project by name, short ID, and workspace metadata when present."""
    name = project.get("name") or project.get("id") or "Unnamed Project"
    label = f"{name} ({_short_id(project.get('id'))})"
    workspaces = _project_workspace_ids(project)
    if workspaces:
        label = f"{label} · ws:{','.join(workspaces)}"
    return label


def _project_sort_key(project: Dict) -> tuple[str, str]:
    return (str(project.get("name") or ""), str(project.get("id") or ""))


STATUS_STYLES = {
    MappingStatus.UNMAPPED: ("", "red"),
    MappingStatus.AUTO_MATCHED: ("auto-match", "dim green"),
    MappingStatus.MAPPED: ("mapped", "bold green"),
    MappingStatus.SAME_NAME: ("same-name", "yellow"),
    MappingStatus.SKIPPED: ("--", "dim"),
}


# ---------------------------------------------------------------------------
# Modal: Destination Picker
# ---------------------------------------------------------------------------


class DestinationPickerScreen(ModalScreen[Optional[str]]):
    """Modal screen for picking a destination project via text input with suggestions."""

    CSS = """
    DestinationPickerScreen {
        align: center middle;
    }

    #dest-dialog {
        width: 80;
        max-height: 80%;
        background: $surface;
        border: thick $accent;
        padding: 1 2;
    }

    #dest-title {
        text-align: center;
        text-style: bold;
        margin-bottom: 1;
    }

    #dest-input {
        width: 100%;
        margin-bottom: 1;
    }

    #dest-table {
        height: 1fr;
        min-height: 8;
    }

    #dest-no-matches {
        text-align: center;
        color: $text-muted;
        margin: 1 0;
    }

    #dest-help {
        text-align: center;
        color: $text-muted;
        margin-top: 1;
    }

    DataTable > .datatable--cursor {
        background: $accent;
    }

    DataTable:focus > .datatable--cursor {
        background: $accent-darken-1;
    }
    """

    BINDINGS = [
        Binding("escape", "cancel", "Cancel", show=True),
    ]

    def __init__(
        self,
        source_label: str,
        dest_labels: List[str],
        reverse_map: Dict[str, str],
        *,
        initial_value: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.source_label = source_label
        self.dest_labels = dest_labels  # already sorted by caller
        self.reverse_map = reverse_map  # dest_label -> source_label that mapped to it
        self.filtered_indices: List[int] = list(range(len(self.dest_labels)))
        self.filter_text = initial_value or source_label
        self._filter_timer = None

    def compose(self) -> ComposeResult:
        with Vertical(id="dest-dialog"):
            yield Static(f"Destination for: [bold]{self.source_label}[/bold]", id="dest-title")
            yield Input(value=self.filter_text, id="dest-input")
            yield DataTable(id="dest-table", cursor_type="row", zebra_stripes=True)
            yield Static("", id="dest-no-matches")
            yield Static(
                "Enter: confirm name | ↑↓: browse suggestions | Esc: cancel", id="dest-help"
            )

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.add_column("Destination Project", width=40)
        table.add_column("Mapped From", width=30)
        self._refresh_table()
        inp = self.query_one("#dest-input", Input)
        inp.focus()
        inp.action_select_all()

    def _refresh_table(self) -> None:
        table = self.query_one(DataTable)
        table.clear()
        search = self.filter_text.lower()
        self.filtered_indices = []

        for idx, label in enumerate(self.dest_labels):
            if search and search not in label.lower():
                continue
            self.filtered_indices.append(idx)
            mapped_from = self.reverse_map.get(label, "")
            indicator = f"<- {mapped_from}" if mapped_from else ""
            table.add_row(label, indicator, key=str(idx))

        no_matches = self.query_one("#dest-no-matches", Static)
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
        """Confirm whatever text is in the input."""
        value = event.value.strip()
        if value:
            self.dismiss(value)

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        """Fill the input with the selected suggestion and refocus input."""
        table = self.query_one(DataTable)
        cursor = table.cursor_row
        if 0 <= cursor < len(self.filtered_indices):
            idx = self.filtered_indices[cursor]
            inp = self.query_one("#dest-input", Input)
            inp.value = self.dest_labels[idx]
            inp.focus()
            inp.action_end()

    def action_cancel(self) -> None:
        self.dismiss(None)


# ---------------------------------------------------------------------------
# Modal: Custom Name Input
# ---------------------------------------------------------------------------


class CustomNameScreen(ModalScreen[Optional[str]]):
    """Modal screen for entering a custom destination project name."""

    CSS = """
    CustomNameScreen {
        align: center middle;
    }

    #custom-dialog {
        width: 60;
        height: auto;
        background: $surface;
        border: thick $accent;
        padding: 1 2;
    }

    #custom-title {
        text-align: center;
        text-style: bold;
        margin-bottom: 1;
    }

    #custom-hint {
        color: $text-muted;
        text-align: center;
        margin-top: 1;
    }

    #custom-help {
        text-align: center;
        color: $text-muted;
        margin-top: 1;
    }
    """

    BINDINGS = [
        Binding("escape", "cancel", "Cancel", show=True),
    ]

    def __init__(self, source_name: str) -> None:
        super().__init__()
        self.source_name = source_name

    def compose(self) -> ComposeResult:
        with Vertical(id="custom-dialog"):
            yield Static(
                f"Custom Destination Name for: [bold]{self.source_name}[/bold]", id="custom-title"
            )
            yield Input(value=self.source_name, id="custom-input")
            yield Static("(This project will be created in the destination)", id="custom-hint")
            yield Static("Enter: confirm | Esc: cancel", id="custom-help")

    def on_mount(self) -> None:
        inp = self.query_one("#custom-input", Input)
        inp.focus()
        # Select all text so user can easily replace
        inp.action_select_all()

    def on_input_submitted(self, event: Input.Submitted) -> None:
        value = event.value.strip()
        if value:
            self.dismiss(value)

    def action_cancel(self) -> None:
        self.dismiss(None)


# ---------------------------------------------------------------------------
# Main App: Project Mapper
# ---------------------------------------------------------------------------


class ProjectMapperApp(App):
    """Full-screen TUI for mapping source projects to destination projects."""

    CSS = """
    Screen {
        background: $surface;
    }

    #search-container {
        dock: top;
        height: auto;
        background: $panel;
        padding: 0 1;
    }

    #help-text {
        color: $text-muted;
    }

    #main-table {
        height: 1fr;
    }

    #stats {
        dock: bottom;
        height: 3;
        background: $panel;
        padding: 0 1;
        content-align: center middle;
    }

    DataTable > .datatable--cursor {
        background: $accent;
    }

    DataTable:focus > .datatable--cursor {
        background: $accent-darken-1;
    }
    """

    BINDINGS = [
        Binding("enter", "assign", "Edit destination", show=True),
        Binding("space", "assign", "Edit destination", show=False),
        Binding("s", "skip", "Skip", show=True, priority=True),
        Binding("m", "same_name", "Same name", show=True, priority=True),
        Binding("u", "unmap", "Unmap", show=True, priority=True),
        Binding("a", "auto_match_all", "Auto-match all", show=True, priority=True),
        Binding("slash", "focus_search", "Search", show=True),
        Binding("ctrl+s", "confirm", "Save", show=True),
        Binding("escape", "quit_app", "Cancel", show=True),
        Binding("q", "quit_app", "Quit", show=False),
    ]

    filter_text: reactive[str] = reactive("")

    def __init__(
        self,
        source_projects: List[Dict],
        dest_projects: List[Dict],
        existing_mapping: Optional[Dict[str, str]] = None,
        *,
        return_ids: bool = False,
    ) -> None:
        super().__init__()
        self.return_ids = return_ids
        self.source_projects = sorted(
            [p for p in source_projects if p.get("name") and p.get("id")],
            key=_project_sort_key,
        )
        self.dest_projects = sorted(
            [p for p in dest_projects if p.get("name") and p.get("id")],
            key=_project_sort_key,
        )
        self.src_names = [p["name"] for p in self.source_projects]
        self.dest_names = [p["name"] for p in self.dest_projects]
        self.dest_label_to_project = {
            _project_label(project): project for project in self.dest_projects
        }
        self.dest_labels = list(self.dest_label_to_project)
        self.dest_by_id = {
            project["id"]: project for project in self.dest_projects if project.get("id")
        }
        self.dest_projects_by_name: Dict[str, List[Dict]] = {}
        for project in self.dest_projects:
            self.dest_projects_by_name.setdefault(project["name"], []).append(project)
        self.dest_name_set = {
            name for name, projects in self.dest_projects_by_name.items() if len(projects) == 1
        }

        # Build mappings list
        self.mappings: List[ProjectMapping] = []
        existing = existing_mapping or {}

        for project in self.source_projects:
            source_id = project["id"]
            name = project["name"]
            source_label = _project_label(project)
            existing_key = source_id if self.return_ids else name
            if existing_key in existing:
                dest = existing[existing_key]
                dest_project = self._resolve_dest_project(dest)
                dest_name = dest_project.get("name") if dest_project else dest
                dest_id = (
                    dest_project.get("id") if dest_project else (dest if self.return_ids else None)
                )
                dest_label = _project_label(dest_project) if dest_project else dest_name
                if dest_name == name:
                    status = self._status_for_same_name(name)
                else:
                    status = MappingStatus.MAPPED
                self.mappings.append(
                    ProjectMapping(
                        name,
                        dest_name,
                        status,
                        source_id=source_id,
                        source_label=source_label,
                        dest_id=dest_id,
                        dest_label=dest_label,
                    )
                )
            elif name in self.dest_name_set:
                dest_project = self.dest_projects_by_name[name][0]
                self.mappings.append(
                    ProjectMapping(
                        name,
                        dest_project["name"],
                        MappingStatus.AUTO_MATCHED,
                        source_id=source_id,
                        source_label=source_label,
                        dest_id=dest_project["id"],
                        dest_label=_project_label(dest_project),
                    )
                )
            else:
                self.mappings.append(
                    ProjectMapping(
                        name,
                        None,
                        MappingStatus.UNMAPPED,
                        source_id=source_id,
                        source_label=source_label,
                    )
                )

        self.filtered_indices: List[int] = list(range(len(self.mappings)))
        self.result: Optional[Dict[str, str]] = None
        self._filter_timer = None

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical(id="search-container"):
            yield Input(placeholder="Search source projects... (/ to focus)", id="search-input")
            yield Static(
                "Enter/Space: edit destination | s: skip | m: same name | u: unmap | a: auto-match all | Ctrl+S: save | Esc: cancel",
                id="help-text",
            )
        yield DataTable(id="main-table", cursor_type="row", zebra_stripes=True, show_cursor=True)
        yield Static("", id="stats")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.add_column("Source Project", width=35)
        table.add_column("Destination", width=35)
        table.add_column("Status", width=15)
        self._refresh_table()
        self._update_stats()
        self.set_focus(table)

    # -- Table rendering ----------------------------------------------------

    def _refresh_table(self) -> None:
        table = self.query_one(DataTable)
        table.clear()
        search = self.filter_text.lower()
        self.filtered_indices = []

        for idx, m in enumerate(self.mappings):
            source_display = m.source_label or m.source_name
            if search and search not in source_display.lower():
                continue
            self.filtered_indices.append(idx)

            dest_label, style = STATUS_STYLES[m.status]
            dest_display = m.dest_label or m.dest_name or dest_label
            status_display = f"[{style}]{m.status.value}[/{style}]"

            table.add_row(source_display, dest_display, status_display, key=str(idx))

    def _update_stats(self) -> None:
        mapped = skipped = unmapped = 0
        for m in self.mappings:
            if m.status in (
                MappingStatus.MAPPED,
                MappingStatus.AUTO_MATCHED,
                MappingStatus.SAME_NAME,
            ):
                mapped += 1
            elif m.status == MappingStatus.SKIPPED:
                skipped += 1
            elif m.status == MappingStatus.UNMAPPED:
                unmapped += 1
        total = len(self.mappings)
        self.query_one("#stats", Static).update(
            f"Mapped: {mapped} | Skipped: {skipped} | Unmapped: {unmapped} | Total: {total}"
        )

    # -- Helpers ------------------------------------------------------------

    def _modal_is_active(self) -> bool:
        """Return True if a modal screen is on top (so app bindings should no-op)."""
        return isinstance(self.screen, ModalScreen)

    def _current_index(self) -> Optional[int]:
        table = self.query_one(DataTable)
        cursor = table.cursor_row
        if 0 <= cursor < len(self.filtered_indices):
            return self.filtered_indices[cursor]
        return None

    def _current_mapping(self) -> Optional[ProjectMapping]:
        idx = self._current_index()
        return self.mappings[idx] if idx is not None else None

    def _status_for_same_name(self, name: str) -> MappingStatus:
        """Return AUTO_MATCHED if dest exists, SAME_NAME otherwise."""
        return MappingStatus.AUTO_MATCHED if name in self.dest_name_set else MappingStatus.SAME_NAME

    def _reverse_map(self) -> Dict[str, str]:
        """Build dest_label -> source_label for currently assigned mappings."""
        return {
            m.dest_label or m.dest_name: m.source_label or m.source_name
            for m in self.mappings
            if (m.dest_label or m.dest_name)
            and m.status not in (MappingStatus.UNMAPPED, MappingStatus.SKIPPED)
        }

    def _resolve_dest_project(self, value: str) -> Optional[Dict]:
        """Resolve a destination selection by ID, display label, or unique name."""
        if value in self.dest_by_id:
            return self.dest_by_id[value]
        if value in self.dest_label_to_project:
            return self.dest_label_to_project[value]
        projects = self.dest_projects_by_name.get(value, [])
        if len(projects) == 1:
            return projects[0]
        return None

    def _set_destination(self, mapping: ProjectMapping, value: str) -> None:
        """Assign a destination project selection to a mapping row."""
        dest_project = self._resolve_dest_project(value)
        if dest_project:
            mapping.dest_name = dest_project["name"]
            mapping.dest_id = dest_project["id"]
            mapping.dest_label = _project_label(dest_project)
        else:
            mapping.dest_name = value
            mapping.dest_id = None
            mapping.dest_label = value
        mapping.status = (
            self._status_for_same_name(mapping.source_name)
            if mapping.dest_name == mapping.source_name
            else MappingStatus.MAPPED
        )

    def _set_same_name_destination(self, mapping: ProjectMapping) -> None:
        """Assign a destination with the same name when it resolves unambiguously."""
        projects = self.dest_projects_by_name.get(mapping.source_name, [])
        if len(projects) == 1:
            dest_project = projects[0]
            mapping.dest_name = dest_project["name"]
            mapping.dest_id = dest_project["id"]
            mapping.dest_label = _project_label(dest_project)
        else:
            mapping.dest_name = mapping.source_name
            mapping.dest_id = None
            mapping.dest_label = mapping.source_name
        mapping.status = self._status_for_same_name(mapping.source_name)

    def _refresh_and_stats(self) -> None:
        self._refresh_table()
        self._update_stats()

    # -- Search -------------------------------------------------------------

    def on_input_changed(self, event: Input.Changed) -> None:
        if event.input.id != "search-input":
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

    def action_focus_search(self) -> None:
        self.query_one("#search-input", Input).focus()

    # -- Key actions --------------------------------------------------------

    def action_assign(self) -> None:
        if self._modal_is_active():
            return
        idx = self._current_index()
        if idx is None:
            return
        m = self.mappings[idx]
        # Capture index now so the callback doesn't depend on cursor position
        self._pending_assign_idx = idx
        self.push_screen(
            DestinationPickerScreen(
                m.source_label or m.source_name,
                self.dest_labels,
                self._reverse_map(),
                initial_value=m.source_name,
            ),
            callback=self._on_dest_picked,
        )

    def _on_dest_picked(self, result: Optional[str]) -> None:
        idx = self._pending_assign_idx
        if result is None:
            return
        m = self.mappings[idx]
        self._set_destination(m, result)
        self._refresh_and_stats()

    def action_skip(self) -> None:
        if self._modal_is_active():
            return
        idx = self._current_index()
        if idx is None:
            return
        m = self.mappings[idx]
        m.dest_name = None
        m.dest_id = None
        m.dest_label = None
        m.status = MappingStatus.SKIPPED
        self._refresh_and_stats()

    def action_same_name(self) -> None:
        if self._modal_is_active():
            return
        idx = self._current_index()
        if idx is None:
            return
        m = self.mappings[idx]
        self._set_same_name_destination(m)
        self._refresh_and_stats()

    def action_unmap(self) -> None:
        if self._modal_is_active():
            return
        idx = self._current_index()
        if idx is None:
            return
        m = self.mappings[idx]
        m.dest_name = None
        m.dest_id = None
        m.dest_label = None
        m.status = MappingStatus.UNMAPPED
        self._refresh_and_stats()

    def action_auto_match_all(self) -> None:
        if self._modal_is_active():
            return
        for m in self.mappings:
            if m.status == MappingStatus.UNMAPPED and m.source_name in self.dest_name_set:
                self._set_same_name_destination(m)
        self._refresh_and_stats()

    def action_confirm(self) -> None:
        if self._modal_is_active():
            return
        self.result = {}
        for m in self.mappings:
            if m.dest_name and m.status not in (MappingStatus.UNMAPPED, MappingStatus.SKIPPED):
                if self.return_ids:
                    if m.source_id and m.dest_id:
                        self.result[m.source_id] = m.dest_id
                else:
                    self.result[m.source_name] = m.dest_name
        self.exit()

    def action_quit_app(self) -> None:
        if self._modal_is_active():
            return
        self.result = None
        self.exit()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def build_project_mapping_tui(
    source_projects: List[Dict],
    dest_projects: List[Dict],
    existing_mapping: Optional[Dict[str, str]] = None,
) -> Optional[Dict[str, str]]:
    """Launch the interactive project mapper TUI.

    Args:
        source_projects: List of source project dicts (must have 'name').
        dest_projects: List of destination project dicts (must have 'name').
        existing_mapping: Optional pre-existing mapping to restore.

    Returns:
        Dict mapping source_project_name -> dest_project_name, or None if cancelled.
    """
    if not source_projects:
        return {}

    app = ProjectMapperApp(source_projects, dest_projects, existing_mapping)
    app.title = "Project Mapper"
    app.sub_title = "Map source projects to destination projects"
    app.run()
    return app.result


def build_project_id_mapping_tui(
    source_projects: List[Dict],
    dest_projects: List[Dict],
    existing_mapping: Optional[Dict[str, str]] = None,
) -> Optional[Dict[str, str]]:
    """Launch the interactive project mapper and return source ID -> destination ID.

    Source projects are displayed one row per source record so duplicate names remain
    independently selectable.
    """
    if not source_projects:
        return {}

    app = ProjectMapperApp(
        source_projects,
        dest_projects,
        existing_mapping,
        return_ids=True,
    )
    app.title = "Project Mapper"
    app.sub_title = "Map source project IDs to destination project IDs"
    app.run()
    return app.result
