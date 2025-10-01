"""Prompt migration logic."""

from typing import Dict, List, Any

from .base import BaseMigrator
from ..api_client import NotFoundError


class PromptMigrator(BaseMigrator):
    """Handles prompt migration."""

    def list_prompts(self) -> List[Dict[str, Any]]:
        """List all prompts."""
        try:
            response = self.source.get("/prompts")
            return response if isinstance(response, list) else []
        except NotFoundError:
            return []

    def migrate_prompt(self, prompt_id: str):
        """Migrate a single prompt."""
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would migrate prompt: {prompt_id}")
            return

        # Note: This would need the LangSmith client for prompt operations
        # For now, we'll use a placeholder
        self.log(f"Prompt migration requires LangSmith client: {prompt_id}", "warning")
