"""Fleet schedule migration logic."""

from typing import Dict, List, Any, Optional

from .base import BaseMigrator
from ..api_client import ConflictError, NotFoundError


class FleetScheduleMigrator(BaseMigrator):
    """Handles migration of Fleet agent cron schedules."""

    def list_schedules(self, agent_id: str) -> List[Dict[str, Any]]:
        """List all schedules for a source agent."""
        schedules = []
        try:
            for schedule in self.source.get_cursor_paginated(
                f"/v1/fleet/agents/{agent_id}/schedules"
            ):
                if isinstance(schedule, dict):
                    schedules.append(schedule)
        except NotFoundError:
            self.log(f"Schedules endpoint not found for agent {agent_id}", "warning")
        except Exception as e:
            self.log(f"Failed to list schedules for agent {agent_id}: {e}", "warning")
        return schedules

    def list_dest_schedules(self, dest_agent_id: str) -> List[Dict[str, Any]]:
        """List existing schedules on the destination agent."""
        schedules = []
        try:
            for schedule in self.dest.get_cursor_paginated(
                f"/v1/fleet/agents/{dest_agent_id}/schedules"
            ):
                if isinstance(schedule, dict):
                    schedules.append(schedule)
        except Exception as e:
            self.log(f"Failed to list dest schedules: {e}", "warning")
        return schedules

    def create_schedule(
        self,
        dest_agent_id: str,
        schedule: Dict[str, Any],
    ) -> Optional[str]:
        """Create a schedule on the destination agent.

        Skips if a schedule with the same cron already exists on the destination.

        Returns the destination schedule ID, or None if skipped/failed.
        """
        cron = schedule.get("cron", "")

        # Check for existing schedule with same cron
        existing = self.list_dest_schedules(dest_agent_id)
        for existing_sched in existing:
            if existing_sched.get("cron") == cron:
                self.log(
                    f"Schedule with cron '{cron}' already exists for agent "
                    f"{dest_agent_id}, skipping",
                    "warning",
                )
                return existing_sched.get("id")

        if self.config.migration.dry_run:
            self.log(
                f"[DRY RUN] Would create schedule '{schedule.get('display_name', '')}' "
                f"for agent {dest_agent_id}"
            )
            return f"dry-run-{schedule.get('id', '')}"

        payload: Dict[str, Any] = {
            "cron": cron,
        }

        input_message = schedule.get("input_message")
        if input_message:
            payload["input_message"] = input_message

        display_name = schedule.get("display_name")
        if display_name:
            payload["display_name"] = display_name

        enabled = schedule.get("enabled")
        if enabled is not None:
            payload["enabled"] = enabled

        try:
            response = self.dest.post(
                f"/v1/fleet/agents/{dest_agent_id}/schedules", payload
            )
            if isinstance(response, dict) and "id" in response:
                return response["id"]
        except ConflictError:
            self.log(
                f"Schedule with cron '{cron}' already exists for agent "
                f"{dest_agent_id}, skipping",
                "warning",
            )
            return None
        except Exception as e:
            self.log(
                f"Failed to create schedule for agent {dest_agent_id}: {e}",
                "error",
            )

        return None
