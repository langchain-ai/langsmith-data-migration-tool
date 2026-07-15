"""Fleet trigger migration logic."""

from typing import Dict, List, Any, Optional

from .base import BaseMigrator
from ..api_client import NotFoundError


class FleetTriggerMigrator(BaseMigrator):
    """Handles migration of Fleet triggers."""

    def list_triggers(self) -> List[Dict[str, Any]]:
        """List all triggers from the source workspace."""
        triggers = []
        try:
            for trigger in self.source.get_cursor_paginated("/v1/fleet/triggers"):
                if isinstance(trigger, dict):
                    triggers.append(trigger)
        except NotFoundError:
            self.log("Fleet triggers endpoint not found", "warning")
        except Exception as e:
            self.log(f"Failed to list Fleet triggers: {e}", "warning")
        return triggers

    def create_trigger(
        self,
        trigger: Dict[str, Any],
        agent_id_map: Dict[str, str],
    ) -> Optional[str]:
        """Create a trigger in the destination with remapped agent ID.

        Args:
            trigger: Source trigger record.
            agent_id_map: Mapping of source agent IDs to destination agent IDs.

        Returns the destination trigger ID, or None if skipped/failed.
        """
        source_agent_id = trigger.get("agent_id", "")
        dest_agent_id = agent_id_map.get(source_agent_id)

        if not dest_agent_id:
            self.log(
                f"Skipping trigger '{trigger.get('name', '')}': "
                f"agent {source_agent_id} not in mapping",
                "warning",
            )
            return None

        if self.config.migration.dry_run:
            self.log(
                f"[DRY RUN] Would create trigger '{trigger.get('name', '')}' "
                f"for agent {dest_agent_id}"
            )
            return f"dry-run-{trigger.get('id', '')}"

        payload: Dict[str, Any] = {
            "agent_id": dest_agent_id,
            "template_id": trigger.get("template_id", ""),
            "config": trigger.get("config", {}),
        }

        name = trigger.get("name")
        if name:
            payload["name"] = name

        status = trigger.get("status")
        if status:
            payload["status"] = status

        registration_id = trigger.get("registration_id")
        if registration_id:
            payload["registration_id"] = registration_id

        try:
            response = self.dest.post("/v1/fleet/triggers", payload)
            if isinstance(response, dict) and "id" in response:
                return response["id"]
        except Exception as e:
            self.log(
                f"Failed to create trigger '{trigger.get('name', '')}': {e}",
                "error",
            )

        return None
