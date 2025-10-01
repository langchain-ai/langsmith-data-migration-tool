"""Base migrator class with shared functionality."""

from typing import Any
from rich.console import Console

from ..api_client import EnhancedAPIClient
from ...utils.state import MigrationState


class BaseMigrator:
    """Base class for all migrators."""

    def __init__(
        self,
        source_client: EnhancedAPIClient,
        dest_client: EnhancedAPIClient,
        state: MigrationState,
        config: Any
    ):
        """Initialize base migrator."""
        self.source = source_client
        self.dest = dest_client
        self.state = state
        self.config = config
        self.console = Console()

    def log(self, message: str, level: str = "info"):
        """Log a message if verbose mode is enabled."""
        if not self.config.migration.verbose:
            return

        style = {
            "info": "dim",
            "success": "green",
            "warning": "yellow",
            "error": "red"
        }.get(level, "")

        self.console.print(f"[{style}]{message}[/{style}]")
