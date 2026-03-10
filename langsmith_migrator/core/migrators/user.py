"""Organization member and workspace member migration logic."""

from typing import Dict, List, Any, Optional

from .base import BaseMigrator

_MEMBER_STATUSES = ("active", "pending")


class UserMigrator(BaseMigrator):
    """Handles migration of org members and workspace memberships."""

    def __init__(self, source_client, dest_client, state, config,
                 role_id_map: Optional[Dict[str, str]] = None):
        super().__init__(source_client, dest_client, state, config)
        self.role_id_map = role_id_map or {}

    # ------------------------------------------------------------------
    # Org-level member operations
    # ------------------------------------------------------------------

    def _list_members(self, client, endpoint_prefix: str,
                      key_by: Optional[str] = None) -> Any:
        """Fetch members across active/pending statuses.

        Args:
            client: API client (source or dest).
            endpoint_prefix: e.g. "/orgs/current/members".
            key_by: If set, return a dict keyed by this member field.
                    Otherwise return a flat list.
        """
        result_list: List[Dict[str, Any]] = []
        result_dict: Dict[str, Dict[str, Any]] = {}

        for status in _MEMBER_STATUSES:
            try:
                response = client.get(f"{endpoint_prefix}/{status}")
                items = response if isinstance(response, list) else response.get("members", [])
                for m in items:
                    if isinstance(m, dict):
                        m.setdefault("status", status)
                        if key_by:
                            key_val = m.get(key_by)
                            if key_val:
                                result_dict[key_val] = m
                        else:
                            result_list.append(m)
            except Exception as e:
                self.log(f"Failed to list {status} members from {endpoint_prefix}: {e}", "warning")

        return result_dict if key_by else result_list

    def list_org_members(self) -> List[Dict[str, Any]]:
        """List all org members (active + pending) from source."""
        return self._list_members(self.source, "/orgs/current/members")

    def list_dest_org_members(self) -> Dict[str, Dict[str, Any]]:
        """List all org members on the destination, keyed by email for O(1) lookup."""
        return self._list_members(self.dest, "/orgs/current/members", key_by="email")

    def _map_role_id(self, source_role_id: Optional[str]) -> Optional[str]:
        """Map a source role ID to the destination role ID."""
        if not source_role_id:
            return None
        dest_id = self.role_id_map.get(source_role_id)
        if not dest_id:
            self.log(
                f"No mapping for role ID {source_role_id}. "
                "Run 'roles' command first to migrate custom roles.",
                "warning",
            )
        return dest_id

    def invite_or_update_org_member(
        self,
        member: Dict[str, Any],
        dest_members_by_email: Dict[str, Dict[str, Any]],
    ) -> Optional[str]:
        """Invite a new member or update an existing member's role on the destination.

        Returns the destination identity_id on success, or None on failure/skip.
        """
        email = member.get("email")
        if not email:
            self.log("Member has no email, skipping", "warning")
            return None

        source_role_id = member.get("role_id")
        dest_role_id = self._map_role_id(source_role_id)
        if source_role_id and not dest_role_id:
            self.log(f"Skipping {email}: unmapped role {source_role_id}", "warning")
            return None

        existing = dest_members_by_email.get(email)

        if existing:
            return self._update_org_member(existing, dest_role_id, email)
        return self._invite_org_member(email, dest_role_id)

    def _update_org_member(
        self, existing: Dict[str, Any], dest_role_id: Optional[str], email: str
    ) -> Optional[str]:
        """Update role of an existing org member."""
        dest_identity_id = existing.get("id") or existing.get("identity_id")
        if not dest_identity_id:
            self.log(f"Cannot update {email}: no identity_id on dest member", "warning")
            return None

        # Skip update if role already matches
        if dest_role_id and existing.get("role_id") == dest_role_id:
            self.log(f"{email} already has correct role, skipping update", "info")
            return dest_identity_id

        if not dest_role_id:
            self.log(f"{email} exists on dest, no role update needed", "info")
            return dest_identity_id

        if self.config.migration.skip_existing:
            self.log(f"{email} already exists, skipping (--skip-existing)", "warning")
            return dest_identity_id

        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would update role for {email}")
            return dest_identity_id

        try:
            self.dest.patch(
                f"/orgs/current/members/{dest_identity_id}",
                {"role_id": dest_role_id},
            )
            self.log(f"Updated role for {email}", "success")
            return dest_identity_id
        except Exception as e:
            self.log(f"Failed to update {email}: {e}", "error")
            return None

    def _invite_org_member(self, email: str, dest_role_id: Optional[str]) -> Optional[str]:
        """Invite a new member to the destination org."""
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would invite {email}")
            return f"dry-run-{email}"

        invite_payload: Dict[str, Any] = {"email": email}
        if dest_role_id:
            invite_payload["role_id"] = dest_role_id

        try:
            response = self.dest.post(
                "/orgs/current/members/batch",
                {"members": [invite_payload]},
            )
            # Response may contain the created member(s)
            created = []
            if isinstance(response, dict):
                created = response.get("members", response.get("created", []))
            elif isinstance(response, list):
                created = response

            if created and isinstance(created[0], dict):
                new_id = created[0].get("id") or created[0].get("identity_id")
                self.log(f"Invited {email} ({new_id})", "success")
                return new_id

            # API returned 200 but no identity_id — cannot confirm membership
            self.log(
                f"Invited {email} but could not confirm identity_id; "
                "workspace membership may not migrate for this user",
                "warning",
            )
            return None
        except Exception as e:
            self.log(f"Failed to invite {email}: {e}", "error")
            return None

    def migrate_org_members(
        self, members: List[Dict[str, Any]]
    ) -> Dict[str, str]:
        """Migrate a list of org members to the destination.

        Returns a mapping of {source_identity_id: dest_identity_id}.
        """
        dest_members = self.list_dest_org_members()
        identity_map: Dict[str, str] = {}

        for member in members:
            src_id = member.get("id") or member.get("identity_id")
            dest_id = self.invite_or_update_org_member(member, dest_members)
            if src_id and dest_id:
                identity_map[src_id] = dest_id

        return identity_map

    # ------------------------------------------------------------------
    # Workspace-level member operations
    # ------------------------------------------------------------------

    def list_workspace_members(self) -> List[Dict[str, Any]]:
        """List workspace members using the current workspace context."""
        try:
            response = self.source.get("/workspaces/current/members")
            members = response if isinstance(response, list) else response.get("members", [])
            return [m for m in members if isinstance(m, dict)]
        except Exception as e:
            self.log(f"Failed to list workspace members: {e}", "error")
            return []

    def list_dest_workspace_members(self) -> Dict[str, Dict[str, Any]]:
        """List workspace members on destination, keyed by user_id."""
        members_by_user: Dict[str, Dict[str, Any]] = {}
        try:
            response = self.dest.get("/workspaces/current/members")
            items = response if isinstance(response, list) else response.get("members", [])
            for m in items:
                if isinstance(m, dict):
                    uid = m.get("user_id") or m.get("id")
                    if uid:
                        members_by_user[uid] = m
        except Exception as e:
            self.log(f"Failed to list dest workspace members: {e}", "error")
        return members_by_user

    def add_or_update_workspace_member(
        self,
        member: Dict[str, Any],
        dest_ws_members: Dict[str, Dict[str, Any]],
        org_identity_map: Dict[str, str],
    ) -> bool:
        """Add or update a single workspace member.

        Args:
            member: Source workspace member dict.
            dest_ws_members: Destination workspace members keyed by user_id.
            org_identity_map: Mapping of source identity_id -> dest identity_id
                from the org-level migration step.

        Returns:
            True on success, False on failure/skip.
        """
        src_user_id = member.get("user_id") or member.get("id")
        if not src_user_id:
            return False

        # Resolve destination user ID via org identity map
        dest_user_id = org_identity_map.get(src_user_id, src_user_id)

        role_id = member.get("role_id")
        dest_role_id = self._map_role_id(role_id) if role_id else None

        existing = dest_ws_members.get(dest_user_id)

        if existing:
            if self.config.migration.skip_existing:
                self.log(f"Workspace member {dest_user_id} exists, skipping", "warning")
                return True

            if dest_role_id and existing.get("role_id") != dest_role_id:
                if self.config.migration.dry_run:
                    self.log(f"[DRY RUN] Would update workspace role for {dest_user_id}")
                    return True
                try:
                    self.dest.patch(
                        f"/workspaces/current/members/{dest_user_id}",
                        {"role_id": dest_role_id},
                    )
                    self.log(f"Updated workspace role for {dest_user_id}", "success")
                except Exception as e:
                    self.log(f"Failed to update workspace member {dest_user_id}: {e}", "error")
                    return False
            return True

        # Add to workspace
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would add {dest_user_id} to workspace")
            return True

        payload: Dict[str, Any] = {"user_id": dest_user_id}
        if dest_role_id:
            payload["role_id"] = dest_role_id

        try:
            self.dest.post("/workspaces/current/members", payload)
            self.log(f"Added {dest_user_id} to workspace", "success")
            return True
        except Exception as e:
            self.log(f"Failed to add {dest_user_id} to workspace: {e}", "error")
            return False

    def migrate_workspace_members(
        self,
        org_identity_map: Dict[str, str],
    ) -> int:
        """Migrate workspace members for the current workspace context.

        Args:
            org_identity_map: Mapping from org-level migration.

        Returns:
            Number of successfully migrated members.
        """
        src_members = self.list_workspace_members()
        dest_ws_members = self.list_dest_workspace_members()

        success = 0
        for member in src_members:
            if self.add_or_update_workspace_member(member, dest_ws_members, org_identity_map):
                success += 1

        return success
