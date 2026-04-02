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
        source_name: str,
        dest_names: List[str],
        reverse_map: Dict[str, str],
    ) -> None:
        super().__init__()
        self.source_name = source_name
        self.dest_names = dest_names  # already sorted by caller
        self.reverse_map = reverse_map  # dest_name -> source_name that mapped to it
        self.filtered_indices: List[int] = list(range(len(self.dest_names)))
        self.filter_text = source_name
        self._filter_timer = None

    def compose(self) -> ComposeResult:
        with Vertical(id="dest-dialog"):
            yield Static(f"Destination for: [bold]{self.source_name}[/bold]", id="dest-title")
            yield Input(value=self.source_name, id="dest-input")
            yield DataTable(id="dest-table", cursor_type="row", zebra_stripes=True)
            yield Static("", id="dest-no-matches")
            yield Static("Enter: confirm name | ↑↓: browse suggestions | Esc: cancel", id="dest-help")

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

        for idx, name in enumerate(self.dest_names):
            if search and search not in name.lower():
                continue
            self.filtered_indices.append(idx)
            mapped_from = self.reverse_map.get(name, "")
            indicator = f"<- {mapped_from}" if mapped_from else ""
            table.add_row(name, indicator, key=str(idx))

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
            inp.value = self.dest_names[idx]
            inp.focus()
            inp.action_end()

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
    ) -> None:
        super().__init__()
        self.src_names = sorted({p["name"] for p in source_projects if "name" in p})
        self.dest_names = sorted({p["name"] for p in dest_projects if "name" in p})
        self.dest_name_set = set(self.dest_names)

        # Build mappings list
        self.mappings: List[ProjectMapping] = []
        existing = existing_mapping or {}

        for name in self.src_names:
            if name in existing:
                dest = existing[name]
                if dest == name:
                    status = self._status_for_same_name(name)
                else:
                    status = MappingStatus.MAPPED
                self.mappings.append(ProjectMapping(name, dest, status))
            elif name in self.dest_name_set:
                self.mappings.append(ProjectMapping(name, name, MappingStatus.AUTO_MATCHED))
            else:
                self.mappings.append(ProjectMapping(name, None, MappingStatus.UNMAPPED))

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
            if search and search not in m.source_name.lower():
                continue
            self.filtered_indices.append(idx)

            dest_label, style = STATUS_STYLES[m.status]
            dest_display = m.dest_name or dest_label
            status_display = f"[{style}]{m.status.value}[/{style}]"

            table.add_row(m.source_name, dest_display, status_display, key=str(idx))

    def _update_stats(self) -> None:
        mapped = skipped = unmapped = 0
        for m in self.mappings:
            if m.status in (MappingStatus.MAPPED, MappingStatus.AUTO_MATCHED, MappingStatus.SAME_NAME):
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
        """Build dest_name -> source_name for currently assigned mappings."""
        return {
            m.dest_name: m.source_name
            for m in self.mappings
            if m.dest_name and m.status not in (MappingStatus.UNMAPPED, MappingStatus.SKIPPED)
        }

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
            DestinationPickerScreen(m.source_name, self.dest_names, self._reverse_map()),
            callback=self._on_dest_picked,
        )

    def _on_dest_picked(self, result: Optional[str]) -> None:
        idx = self._pending_assign_idx
        if result is None:
            return
        m = self.mappings[idx]
        m.dest_name = result
        m.status = (
            self._status_for_same_name(result)
            if result == m.source_name
            else MappingStatus.MAPPED
        )
        self._refresh_and_stats()

    def action_skip(self) -> None:
        if self._modal_is_active():
            return
        idx = self._current_index()
        if idx is None:
            return
        m = self.mappings[idx]
        m.dest_name = None
        m.status = MappingStatus.SKIPPED
        self._refresh_and_stats()

    def action_same_name(self) -> None:
        if self._modal_is_active():
            return
        idx = self._current_index()
        if idx is None:
            return
        m = self.mappings[idx]
        m.dest_name = m.source_name
        m.status = self._status_for_same_name(m.source_name)
        self._refresh_and_stats()

    def action_unmap(self) -> None:
        if self._modal_is_active():
            return
        idx = self._current_index()
        if idx is None:
            return
        m = self.mappings[idx]
        m.dest_name = None
        m.status = MappingStatus.UNMAPPED
        self._refresh_and_stats()

    def action_auto_match_all(self) -> None:
        if self._modal_is_active():
            return
        for m in self.mappings:
            if m.status == MappingStatus.UNMAPPED and m.source_name in self.dest_name_set:
                m.dest_name = m.source_name
                m.status = MappingStatus.AUTO_MATCHED
        self._refresh_and_stats()

    def action_confirm(self) -> None:
        if self._modal_is_active():
            return
        self.result = {}
        for m in self.mappings:
            if m.dest_name and m.status not in (MappingStatus.UNMAPPED, MappingStatus.SKIPPED):
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
