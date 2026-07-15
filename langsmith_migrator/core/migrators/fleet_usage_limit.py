"""Fleet usage limit (spend limit) migration logic."""

from typing import Dict, List, Any, Optional

from .base import BaseMigrator
from ..api_client import NotFoundError


class FleetUsageLimitMigrator(BaseMigrator):
    """Handles migration of Fleet spend limits.

    Spend limits are at ``/platform/fleet/usage/limits`` and use a
    ``subject_type`` (``"agent"`` or ``"user"``) + ``subject_id`` pattern.
    A nil ``subject_id`` means a global default. Agent subject IDs are
    remapped using the agent ID mapping from the agent migration phase.
    """

    def list_limits(self) -> List[Dict[str, Any]]:
        """List all spend limits from the source workspace."""
        limits = []
        try:
            for limit in self.source.get_cursor_paginated(
                "/v1/platform/fleet/usage/limits"
            ):
                if isinstance(limit, dict):
                    limits.append(limit)
        except NotFoundError:
            self.log("Fleet usage limits endpoint not found", "warning")
        except Exception as e:
            self.log(f"Failed to list Fleet usage limits: {e}", "warning")
        return limits

    def create_limit(
        self,
        limit: Dict[str, Any],
        agent_id_map: Dict[str, str],
        user_id_map: Optional[Dict[str, str]] = None,
    ) -> Optional[str]:
        """Create a spend limit in the destination with remapped subject ID.

        Args:
            limit: Source spend limit record.
            agent_id_map: Mapping of source to destination agent IDs.
            user_id_map: Optional mapping of source to destination user IDs.

        Returns the destination limit ID, or None if failed.
        """
        subject_type = limit.get("subject_type", "")
        subject_id = limit.get("subject_id")

        remapped_subject_id = subject_id
        if subject_id:
            if subject_type == "agent":
                remapped_subject_id = agent_id_map.get(subject_id)
                if not remapped_subject_id:
                    self.log(
                        f"Skipping spend limit: agent {subject_id} not in mapping",
                        "warning",
                    )
                    return None
            elif subject_type == "user" and user_id_map:
                remapped_subject_id = user_id_map.get(subject_id, subject_id)

        if self.config.migration.dry_run:
            self.log(
                f"[DRY RUN] Would create spend limit for {subject_type} "
                f"{remapped_subject_id or 'global'}"
            )
            return f"dry-run-{limit.get('id', '')}"

        payload: Dict[str, Any] = {
            "subject_type": subject_type,
            "limit_usd": limit.get("limit_usd", 0),
        }

        if remapped_subject_id:
            payload["subject_id"] = remapped_subject_id

        try:
            response = self.dest.post("/v1/platform/fleet/usage/limits", payload)
            if isinstance(response, dict) and "id" in response:
                return response["id"]
        except Exception as e:
            self.log(
                f"Failed to create spend limit for {subject_type} "
                f"{remapped_subject_id or 'global'}: {e}",
                "error",
            )

        return None
