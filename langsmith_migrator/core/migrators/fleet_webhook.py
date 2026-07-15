"""Fleet webhook migration logic."""

from typing import Dict, List, Any, Optional

from .base import BaseMigrator
from ..api_client import NotFoundError


class FleetWebhookMigrator(BaseMigrator):
    """Handles migration of Fleet platform webhooks.

    Webhooks are at ``/platform/fleet-webhooks`` (not under ``/fleet/``).
    The webhook create payload does not reference an agent_id, so webhooks
    are independent of agent migration.
    """

    def list_webhooks(self) -> List[Dict[str, Any]]:
        """List all webhooks from the source workspace."""
        webhooks = []
        try:
            for webhook in self.source.get_cursor_paginated(
                "/v1/platform/fleet-webhooks"
            ):
                if isinstance(webhook, dict):
                    webhooks.append(webhook)
        except NotFoundError:
            self.log("Fleet webhooks endpoint not found", "warning")
        except Exception as e:
            self.log(f"Failed to list Fleet webhooks: {e}", "warning")
        return webhooks

    def find_existing_webhook(self, name: str) -> Optional[str]:
        """Check if a webhook with the same name exists in destination."""
        try:
            for webhook in self.dest.get_cursor_paginated(
                "/v1/platform/fleet-webhooks"
            ):
                if isinstance(webhook, dict) and webhook.get("name") == name:
                    return webhook.get("id")
        except Exception as e:
            self.log(f"Failed to check for existing webhook: {e}", "warning")
        return None

    def create_webhook(self, webhook: Dict[str, Any]) -> Optional[str]:
        """Create a webhook in the destination workspace.

        Returns the destination webhook ID, or None if skipped/failed.
        """
        name = webhook.get("name", "")
        existing_id = self.find_existing_webhook(name)

        if existing_id:
            self.log(f"Webhook '{name}' already exists, skipping", "warning")
            return existing_id

        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create webhook: {name}")
            return f"dry-run-{webhook.get('id', name)}"

        payload: Dict[str, Any] = {
            "name": name,
            "url": webhook.get("url", ""),
        }

        form_schema = webhook.get("form_schema")
        if form_schema:
            payload["form_schema"] = form_schema

        headers = webhook.get("headers")
        if headers:
            payload["headers"] = headers

        try:
            response = self.dest.post("/v1/platform/fleet-webhooks", payload)
            if isinstance(response, dict) and "id" in response:
                return response["id"]
        except Exception as e:
            self.log(f"Failed to create webhook '{name}': {e}", "error")

        return None
