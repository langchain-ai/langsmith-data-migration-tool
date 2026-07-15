"""Fleet secrets migration logic."""

from typing import Dict, List, Any

from .base import BaseMigrator
from ..api_client import NotFoundError


class FleetSecretMigrator(BaseMigrator):
    """Handles migration of Fleet workspace secrets.

    Secret values are write-only on the source API: ``GET /fleet/secrets``
    returns only ``{name, set}`` without the value. This migrator exports
    the list of secret names and creates placeholder entries on the
    destination. The customer must re-enter values manually.
    """

    def list_secrets(self) -> List[Dict[str, Any]]:
        """List all secret names from the source workspace."""
        secrets = []
        try:
            for secret in self.source.get_cursor_paginated("/v1/fleet/secrets"):
                if isinstance(secret, dict):
                    secrets.append(secret)
        except NotFoundError:
            self.log("Fleet secrets endpoint not found", "warning")
        except Exception as e:
            self.log(f"Failed to list Fleet secrets: {e}", "warning")
        return secrets

    def list_dest_secrets(self) -> List[str]:
        """List secret names already present on the destination."""
        names = []
        try:
            for secret in self.dest.get_cursor_paginated("/v1/fleet/secrets"):
                if isinstance(secret, dict):
                    names.append(secret.get("name", ""))
        except Exception as e:
            self.log(f"Failed to list destination secrets: {e}", "warning")
        return names

    def create_secret_placeholder(self, name: str) -> bool:
        """Create a placeholder secret entry on the destination.

        The value is set to an empty string. The customer must update it
        with the real value via the Fleet UI or API.

        Returns True if created, False if it already existed or failed.
        """
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create secret placeholder: {name}")
            return True

        try:
            self.dest.put(f"/v1/fleet/secrets/{name}", {"value": ""})
            self.log(f"Created secret placeholder: {name}", "success")
            return True
        except Exception as e:
            self.log(f"Failed to create secret placeholder '{name}': {e}", "error")
            return False
