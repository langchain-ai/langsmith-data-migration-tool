"""Fleet sandbox policy migration logic."""

from typing import Dict, List, Any, Optional

from .base import BaseMigrator
from ..api_client import NotFoundError


class FleetSandboxPolicyMigrator(BaseMigrator):
    """Handles migration of Fleet sandbox policies.

    Sandbox policies are at ``/platform/fleet/sandboxes/policies`` and
    define isolation rules for agent sandboxed execution. Policy IDs are
    referenced by agents via ``backend.sandbox_config.policy_ids``.
    """

    def list_policies(self) -> List[Dict[str, Any]]:
        """List all sandbox policies from the source workspace."""
        policies = []
        try:
            for policy in self.source.get_cursor_paginated(
                "/v1/platform/fleet/sandboxes/policies"
            ):
                if isinstance(policy, dict):
                    policies.append(policy)
        except NotFoundError:
            self.log("Fleet sandbox policies endpoint not found", "warning")
        except Exception as e:
            self.log(f"Failed to list Fleet sandbox policies: {e}", "warning")
        return policies

    def find_existing_policy(self, name: str) -> Optional[str]:
        """Check if a policy with the same name exists in destination."""
        try:
            for policy in self.dest.get_cursor_paginated(
                "/v1/platform/fleet/sandboxes/policies"
            ):
                if isinstance(policy, dict) and policy.get("name") == name:
                    return policy.get("id")
        except Exception as e:
            self.log(f"Failed to check for existing sandbox policy: {e}", "warning")
        return None

    def create_policy(self, policy: Dict[str, Any]) -> Optional[str]:
        """Create a sandbox policy in the destination workspace.

        Returns the destination policy ID, or None if skipped/failed.
        """
        name = policy.get("name", "")
        if name:
            existing_id = self.find_existing_policy(name)
            if existing_id:
                self.log(
                    f"Sandbox policy '{name}' already exists, skipping",
                    "warning",
                )
                return existing_id

        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create sandbox policy: {name}")
            return f"dry-run-{policy.get('id', name)}"

        # Copy all fields except internal ones
        payload = {
            k: v for k, v in policy.items()
            if k not in ("id", "tenant_id", "created_at", "updated_at")
        }

        try:
            response = self.dest.post(
                "/v1/platform/fleet/sandboxes/policies", payload
            )
            if isinstance(response, dict) and "id" in response:
                return response["id"]
        except Exception as e:
            self.log(f"Failed to create sandbox policy '{name}': {e}", "error")

        return None
