"""Custom RBAC role migration logic."""

from typing import Dict, List, Any, Optional

from .base import BaseMigrator

# System roles that exist on every LangSmith instance and cannot be created/deleted.
SYSTEM_ROLE_NAMES = {"Admin", "Viewer", "Editor"}


def _is_custom_role(role: Dict[str, Any]) -> bool:
    """Return True if the role is a custom (non-system) role."""
    return not role.get("is_system") and role.get("name") not in SYSTEM_ROLE_NAMES


class RoleMigrator(BaseMigrator):
    """Handles migration of custom RBAC roles between orgs."""

    def list_roles(self, client=None) -> List[Dict[str, Any]]:
        """List all roles (system + custom) from a client.

        Args:
            client: API client to use. Defaults to source client.
        """
        client = client or self.source
        try:
            response = client.get("/orgs/current/roles")
            roles = response if isinstance(response, list) else response.get("roles", [])
            return [r for r in roles if isinstance(r, dict)]
        except Exception as e:
            self.log(f"Failed to list roles: {e}", "error")
            return []

    def list_custom_roles(self, client=None) -> List[Dict[str, Any]]:
        """List only custom (non-system) roles from a client."""
        return [r for r in self.list_roles(client) if _is_custom_role(r)]

    def build_role_id_map(self) -> Dict[str, str]:
        """Build a mapping of source role IDs to destination role IDs by name.

        Matches ALL roles (system + custom) so that user role assignments
        can be translated to the destination org.
        """
        source_roles = self.list_roles(self.source)
        dest_roles = self.list_roles(self.dest)

        dest_by_name: Dict[str, str] = {
            r["name"]: r["id"]
            for r in dest_roles
            if r.get("name") and r.get("id")
        }

        role_id_map: Dict[str, str] = {
            r["id"]: dest_by_name[r["name"]]
            for r in source_roles
            if r.get("name") and r.get("id") and r["name"] in dest_by_name
        }

        self.log(
            f"Role ID map: {len(role_id_map)} mapped "
            f"({len(source_roles)} source, {len(dest_roles)} dest)",
            "info",
        )
        return role_id_map

    def create_custom_role(
        self,
        role: Dict[str, Any],
        dest_roles_by_name: Optional[Dict[str, Dict[str, Any]]] = None,
    ) -> Optional[str]:
        """Create or update a custom role on the destination.

        Args:
            role: Source role dict.
            dest_roles_by_name: Pre-fetched destination custom roles keyed by name.
                If None, fetches from the API (slower when called in a loop).

        Respects dry_run and skip_existing configuration.
        """
        role_name = role.get("name", "unnamed")

        # Use cached lookup if available, otherwise fetch
        if dest_roles_by_name is not None:
            existing = dest_roles_by_name.get(role_name)
        else:
            existing = self._find_existing_custom_role(role_name)

        if existing:
            if self.config.migration.skip_existing:
                self.log(f"Role '{role_name}' already exists, skipping", "warning")
                return existing.get("id")
            self.log(f"Role '{role_name}' exists, updating...", "info")
            return self._update_custom_role(existing["id"], role)

        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create role: {role_name}")
            return f"dry-run-{role.get('id', 'role')}"

        payload = {
            "name": role_name,
            "description": role.get("description"),
            "permissions": role.get("permissions", []),
        }
        payload = {k: v for k, v in payload.items() if v is not None}

        try:
            response = self.dest.post("/orgs/current/roles", payload)
            if not isinstance(response, dict) or "id" not in response:
                self.log(f"Unexpected response creating role: {response}", "error")
                return None
            self.log(f"Created role: {role_name} ({response['id']})", "success")
            return response["id"]
        except Exception as e:
            self.log(f"Failed to create role '{role_name}': {e}", "error")
            return None

    def get_dest_custom_roles_by_name(self) -> Dict[str, Dict[str, Any]]:
        """Fetch destination custom roles once, keyed by name for O(1) lookup."""
        return {r["name"]: r for r in self.list_custom_roles(self.dest) if r.get("name")}

    def _find_existing_custom_role(self, name: str) -> Optional[Dict[str, Any]]:
        """Find an existing custom role on the destination by name."""
        for r in self.list_custom_roles(self.dest):
            if r.get("name") == name:
                return r
        return None

    def _update_custom_role(self, dest_role_id: str, role: Dict[str, Any]) -> Optional[str]:
        """Update an existing custom role on the destination."""
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would update role: {role.get('name')} ({dest_role_id})")
            return dest_role_id

        payload = {
            "name": role.get("name"),
            "description": role.get("description"),
            "permissions": role.get("permissions", []),
        }
        payload = {k: v for k, v in payload.items() if v is not None}

        try:
            self.dest.patch(f"/orgs/current/roles/{dest_role_id}", payload)
            self.log(f"Updated role: {role.get('name')} ({dest_role_id})", "success")
            return dest_role_id
        except Exception as e:
            self.log(f"Failed to update role {dest_role_id}: {e}", "error")
            return None
