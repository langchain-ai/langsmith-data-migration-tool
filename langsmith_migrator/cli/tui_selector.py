"""Textual-based TUI selector for interactive item selection."""

from typing import List, Dict, Any, Optional
from textual.app import App, ComposeResult
from textual.widgets import DataTable, Header, Footer, Input, Static
from textual.binding import Binding
from textual.containers import Vertical
from textual.reactive import reactive


class ItemSelector(App):
    CSS = """
    Screen {
        background: $surface;
    }

    #search-container {
        dock: top;
        height: 3;
        background: $panel;
        padding: 0 1;
    }

    Input {
        width: 100%;
    }

    #stats {
        dock: bottom;
        height: 3;
        background: $panel;
        padding: 0 1;
        content-align: center middle;
    }

    DataTable {
        height: 1fr;
    }

    DataTable > .datatable--cursor {
        background: $accent;
    }

    DataTable:focus > .datatable--cursor {
        background: $accent-darken-1;
    }
    """

    BINDINGS = [
        Binding("space", "toggle_row", "Toggle", show=True),
        Binding("a", "select_all", "Select All", show=True),
        Binding("n", "select_none", "Clear", show=True),
        Binding("enter", "confirm", "Confirm", show=True),
        Binding("escape", "quit", "Cancel", show=True),
        Binding("q", "quit", "Quit", show=False),
        Binding("/", "focus_search", "Search", show=True),
    ]

    selected_items: reactive[set] = reactive(set)
    filter_text: reactive[str] = reactive("")

    def __init__(self, items: List[Dict[str, Any]], columns: List[Dict[str, str]]):
        super().__init__()
        self.items = items
        self.columns = columns
        self.filtered_indices = list(range(len(items)))
        self.result = None
        self.filter_timer = None

    def compose(self) -> ComposeResult:
        yield Header()
        with Vertical(id="search-container"):
            yield Input(placeholder="Type to search... (press / to focus)", id="search-input")
            yield Static("Keys: ↑↓ navigate | Space toggle | a select all | n clear | / search | Enter confirm | Esc cancel", id="help-text")
        yield DataTable(id="dataset-table", cursor_type="row", zebra_stripes=True, show_cursor=True)
        yield Static("", id="stats")
        yield Footer()

    def on_mount(self) -> None:
        table = self.query_one(DataTable)
        table.cursor_type = "row"

        table.add_column("", width=3)
        for col in self.columns:
            table.add_column(col["title"], width=col.get("width", 20))

        self._refresh_table()
        self._update_stats()
        self.set_focus(table)

    def _refresh_table(self) -> None:
        table = self.query_one(DataTable)
        table.clear()

        search = self.filter_text.lower()
        self.filtered_indices = []

        for idx, item in enumerate(self.items):
            if search:
                match = False
                for col in self.columns:
                    value = str(item.get(col["key"], "")).lower()
                    if search in value:
                        match = True
                        break
                if not match:
                    continue

            self.filtered_indices.append(idx)

            checkbox = "✓" if idx in self.selected_items else " "
            row_data = [checkbox]
            for col in self.columns:
                value = str(item.get(col["key"], ""))
                max_width = col.get("width", 20) - 2
                if len(value) > max_width:
                    value = value[:max_width-3] + "..."
                row_data.append(value)

            table.add_row(*row_data, key=str(idx))

    def _update_stats(self) -> None:
        stats = self.query_one("#stats", Static)
        total = len(self.items)
        selected = len(self.selected_items)
        visible = len(self.filtered_indices)

        if self.filter_text:
            stats.update(f"Selected: {selected}/{total} | Filtered: {visible}/{total}")
        else:
            stats.update(f"Selected: {selected}/{total}")

    def on_input_changed(self, event: Input.Changed) -> None:
        self.filter_text = event.value
        if self.filter_timer:
            self.filter_timer.stop()
        self.filter_timer = self.set_timer(0.3, self._debounced_refresh)

    def _debounced_refresh(self) -> None:
        self._refresh_table()
        self._update_stats()
        table = self.query_one(DataTable)
        self.set_focus(table)
        self.filter_timer = None

    def action_focus_search(self) -> None:
        search_input = self.query_one("#search-input", Input)
        search_input.focus()

    def action_toggle_row(self) -> None:
        table = self.query_one(DataTable)
        cursor_row = table.cursor_row

        if cursor_row >= 0 and cursor_row < len(self.filtered_indices):
            item_idx = self.filtered_indices[cursor_row]

            if item_idx in self.selected_items:
                self.selected_items.remove(item_idx)
            else:
                self.selected_items.add(item_idx)

            self._refresh_table()
            self._update_stats()

            if cursor_row < len(self.filtered_indices) - 1:
                table.move_cursor(row=cursor_row + 1)

    def action_select_all(self) -> None:
        if self.filter_text:
            self.selected_items.update(self.filtered_indices)
        else:
            self.selected_items = set(range(len(self.items)))
        self._refresh_table()
        self._update_stats()

    def action_select_none(self) -> None:
        self.selected_items.clear()
        self._refresh_table()
        self._update_stats()

    def on_data_table_row_selected(self, event: DataTable.RowSelected) -> None:
        event.prevent_default()
        event.stop()
        self.action_confirm()

    def action_confirm(self) -> None:
        self.result = [self.items[idx] for idx in sorted(self.selected_items)]
        self.exit()

    def action_quit(self) -> None:
        self.result = None
        self.exit()


def select_items(
    items: List[Dict[str, Any]],
    title: str = "Select Items",
    columns: Optional[List[Dict[str, str]]] = None
) -> List[Dict[str, Any]]:
    """
    Display an interactive TUI for selecting items.

    Args:
        items: List of items to select from
        title: Title for the selector window
        columns: Column definitions for display

    Returns:
        List of selected items
    """
    if columns is None:
        columns = [
            {"key": "name", "title": "Name", "width": 40},
            {"key": "id", "title": "ID", "width": 36},
        ]

    app = ItemSelector(items, columns)
    app.title = title
    app.sub_title = "Space: toggle | a: select all | n: clear | /: search | Enter: confirm"
    app.run()

    return app.result or []
