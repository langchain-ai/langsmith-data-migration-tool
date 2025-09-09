"""Interactive selector with improved UX for resource selection."""

from typing import List, Dict, Any, Set, Optional
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.prompt import Prompt, IntPrompt, Confirm
from rich.columns import Columns
from rich.text import Text
import time


class InteractiveSelector:
    """Enhanced interactive selector with checkbox-style selection."""
    
    def __init__(self, 
                 items: List[Dict[str, Any]], 
                 title: str = "Select items",
                 columns: List[Dict[str, str]] = None,
                 console: Console = None):
        """
        Initialize the interactive selector.
        
        Args:
            items: List of items to select from
            title: Title for the selector
            columns: Column definitions [{"key": "name", "title": "Name", "width": 30}]
            console: Rich console instance
        """
        self.items = items
        self.title = title
        self.console = console or Console()
        self.selected: Set[int] = set()
        
        # Default columns if not provided
        self.columns = columns or [
            {"key": "name", "title": "Name", "width": 40},
            {"key": "id", "title": "ID", "width": 36},
            {"key": "description", "title": "Description", "width": 50}
        ]
    
    def display_items(self, page_start: int = 0, page_size: int = 20, filter_text: str = "") -> List[int]:
        """Display items in a table format with checkboxes."""
        # Filter items if needed
        if filter_text:
            filtered_indices = []
            search_lower = filter_text.lower()
            
            for idx, item in enumerate(self.items):
                # Search in all string values
                for col in self.columns:
                    value = str(item.get(col["key"], "")).lower()
                    if search_lower in value:
                        filtered_indices.append(idx)
                        break
        else:
            filtered_indices = list(range(len(self.items)))
        
        # Create table
        table = Table(
            title=f"{self.title} ({len(self.selected)}/{len(self.items)} selected)",
            show_header=True,
            header_style="cyan",
            title_style="bold",
            border_style="dim",
            box=None
        )
        
        # Add columns
        table.add_column("", width=3, justify="center")  # Checkbox
        table.add_column("#", width=4, justify="right", style="dim")  # Number
        
        for col in self.columns:
            table.add_column(
                col["title"], 
                width=col.get("width", 30),
                overflow="ellipsis"
            )
        
        # Calculate page boundaries
        page_end = min(page_start + page_size, len(filtered_indices))
        page_items = filtered_indices[page_start:page_end]
        
        # Add rows
        for display_num, item_idx in enumerate(page_items, 1):
            item = self.items[item_idx]
            
            # Checkbox
            checkbox = "✓" if item_idx in self.selected else " "
            checkbox_text = Text(checkbox, style="green" if item_idx in self.selected else "")
            
            # Build row
            row_data = [checkbox_text, str(display_num)]
            
            for col in self.columns:
                value = str(item.get(col["key"], ""))[:col.get("width", 30)]
                row_data.append(value)
            
            style = "green" if item_idx in self.selected else ""
            table.add_row(*row_data, style=style)
        
        self.console.print(table)
        
        return page_items
    
    def run(self) -> List[Dict[str, Any]]:
        """
        Run the interactive selector with a simplified interface.
        
        Returns:
            List of selected items.
        """
        page_start = 0
        page_size = 20
        filter_text = ""
        
        while True:
            # Clear screen
            self.console.clear()
            
            # Display current page
            page_items = self.display_items(page_start, page_size, filter_text)
            
            if not page_items:
                self.console.print("[yellow]No items to display[/yellow]")
                if filter_text:
                    self.console.print(f"Current filter: '{filter_text}'")
                    if Confirm.ask("Clear filter?"):
                        filter_text = ""
                        continue
                else:
                    break
            
            # Display menu more compactly
            self.console.print("\n[dim]Commands: 1-20 (toggle) | a (all visible) | A (ALL) | n (none) | f (filter) | Enter (confirm) | q (cancel)[/dim]")
            
            # Get user input
            choice = Prompt.ask("\nCommand", default="").strip().lower()
            
            if choice == "":  # Enter pressed - confirm
                break
            elif choice == "q":  # Cancel
                self.selected.clear()
                break
            elif choice == "a":  # Select all visible
                self.selected.update(page_items)
            elif choice == "A":  # Select ALL items
                self.selected = set(range(len(self.items)))
            elif choice == "n":  # Clear selections
                self.selected.clear()
            elif choice == "f":  # Filter
                filter_text = Prompt.ask("Enter search text", default=filter_text)
                page_start = 0  # Reset to first page
            elif choice == "c":  # Clear filter
                filter_text = ""
                page_start = 0
            elif choice == ">":  # Next page
                if page_start + page_size < len(self.items):
                    page_start += page_size
            elif choice == "<":  # Previous page
                if page_start > 0:
                    page_start = max(0, page_start - page_size)
            elif choice.isdigit():  # Toggle by number
                num = int(choice)
                if 1 <= num <= len(page_items):
                    item_idx = page_items[num - 1]
                    if item_idx in self.selected:
                        self.selected.remove(item_idx)
                    else:
                        self.selected.add(item_idx)
            elif "-" in choice:  # Range selection (e.g., "1-5")
                try:
                    parts = choice.split("-")
                    if len(parts) == 2:
                        start = int(parts[0])
                        end = int(parts[1])
                        for num in range(start, min(end + 1, len(page_items) + 1)):
                            if 1 <= num <= len(page_items):
                                item_idx = page_items[num - 1]
                                self.selected.add(item_idx)
                except:
                    pass
        
        # Return selected items
        return [self.items[idx] for idx in sorted(self.selected)]


class ProgressTracker:
    """Enhanced progress tracking with multi-level display."""
    
    def __init__(self, console: Console = None):
        """Initialize progress tracker."""
        self.console = console or Console()
        self.stages: List[Dict[str, Any]] = []
        self.current_stage = None
        self.errors: List[Dict[str, Any]] = []
        self.warnings: List[Dict[str, Any]] = []
    
    def __enter__(self):
        """Context manager entry."""
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        pass
    
    def add_stage(self, name: str, total: int = 0) -> str:
        """Add a new stage to track."""
        stage_id = f"stage_{len(self.stages)}"
        self.stages.append({
            "id": stage_id,
            "name": name,
            "total": total,
            "completed": 0,
            "status": "pending",
            "start_time": None,
            "end_time": None,
            "current_item": None
        })
        return stage_id
    
    def start_stage(self, stage_id: str):
        """Start a stage."""
        stage = self._get_stage(stage_id)
        if stage:
            stage["status"] = "in_progress"
            stage["start_time"] = time.time()
            self.current_stage = stage_id
    
    def update_stage(self, stage_id: str, completed: int = None, current_item: str = None):
        """Update stage progress."""
        stage = self._get_stage(stage_id)
        if stage:
            if completed is not None:
                stage["completed"] = completed
            if current_item is not None:
                stage["current_item"] = current_item
    
    def complete_stage(self, stage_id: str):
        """Mark a stage as complete."""
        stage = self._get_stage(stage_id)
        if stage:
            stage["status"] = "completed"
            stage["end_time"] = time.time()
            stage["completed"] = stage["total"]
    
    def add_error(self, item: str, error: str, stage_id: str = None):
        """Add an error to tracking."""
        self.errors.append({
            "item": item,
            "error": error,
            "stage_id": stage_id or self.current_stage,
            "timestamp": time.time()
        })
    
    def add_warning(self, message: str):
        """Add a warning to tracking."""
        self.warnings.append({
            "message": message,
            "timestamp": time.time()
        })
    
    def display_progress(self):
        """Display current progress."""
        if not self.stages:
            return
        
        # Display stages
        for stage in self.stages:
            status_icon = {
                "pending": "⏸",
                "in_progress": "▶",
                "completed": "✓",
                "failed": "✗"
            }.get(stage["status"], "?")
            
            progress = ""
            if stage["total"] > 0:
                percentage = (stage["completed"] / stage["total"]) * 100
                progress = f" [{stage['completed']}/{stage['total']}] {percentage:.1f}%"
            
            status_style = {
                "pending": "dim",
                "in_progress": "yellow",
                "completed": "green",
                "failed": "red"
            }.get(stage["status"], "")
            
            self.console.print(f"[{status_style}]{status_icon} {stage['name']}{progress}[/{status_style}]")
            
            if stage["status"] == "in_progress" and stage["current_item"]:
                self.console.print(f"    [dim]Processing: {stage['current_item']}[/dim]")
        
        # Display errors if any
        if self.errors:
            self.console.print(f"\n[red]Errors: {len(self.errors)}[/red]")
            for error in self.errors[-3:]:  # Show last 3 errors
                self.console.print(f"  [red]• {error['item']}: {error['error'][:100]}[/red]")
    
    def _get_stage(self, stage_id: str) -> Optional[Dict[str, Any]]:
        """Get stage by ID."""
        for stage in self.stages:
            if stage["id"] == stage_id:
                return stage
        return None