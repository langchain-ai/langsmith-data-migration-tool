"""User and role migration logic."""

from typing import Any, Dict, List, Optional, Tuple

from .base import BaseMigrator
from ...utils.retry import APIError, AuthenticationError, ConflictError


class UserRoleMigrator(BaseMigrator):
    """Handles migration of custom roles, org members, and workspace members.

    Migration proceeds in three strict phases:
      1. Role sync      – match built-in roles by name, create/update custom roles
      2. Org members     – invite missing members, update roles for existing ones
      3. Workspace members – per workspace pair, add/update workspace memberships

    Phases 1-2 are org-scoped (no X-Tenant-Id). Phase 3 requires workspace context.
    """

    def __init__(self, source_client, dest_client, state, config):
        super().__init__(source_client, dest_client, state, config)
        self._role_id_map: Dict[str, str] = {}
        self._dest_email_to_identity: Optional[Dict[str, Dict[str, Any]]] = None
        self._last_org_member_removals = 0
        self._last_workspace_member_removals = 0

    # ------------------------------------------------------------------
    # Phase 0: data fetching
    # ------------------------------------------------------------------

    def list_source_roles(self) -> List[Dict[str, Any]]:
        """Fetch all roles from the source organisation."""
        return self.source.get("/orgs/current/roles")

    def list_dest_roles(self) -> List[Dict[str, Any]]:
        """Fetch all roles from the destination organisation."""
        return self.dest.get("/orgs/current/roles")

    def list_source_org_members(self) -> List[Dict[str, Any]]:
        """Fetch active org members from source (paginated)."""
        members = []
        for member in self.source.get_paginated("/orgs/current/members/active"):
            if isinstance(member, dict):
                members.append(member)
        return members

    def list_dest_org_members(self) -> List[Dict[str, Any]]:
        """Fetch active org members from destination (paginated)."""
        members = []
        for member in self.dest.get_paginated("/orgs/current/members/active"):
            if isinstance(member, dict):
                members.append(member)
        return members

    def list_source_pending_org_members(self) -> List[Dict[str, Any]]:
        """Fetch pending org invites from source (paginated)."""
        members = []
        for member in self.source.get_paginated("/orgs/current/members/pending"):
            if isinstance(member, dict):
                members.append(member)
        return members

    def list_dest_pending_org_members(self) -> List[Dict[str, Any]]:
        """Fetch pending org invites from destination (paginated)."""
        members = []
        for member in self.dest.get_paginated("/orgs/current/members/pending"):
            if isinstance(member, dict):
                members.append(member)
        return members

    def list_source_workspace_members(self) -> List[Dict[str, Any]]:
        """Fetch active workspace members from source (requires X-Tenant-Id)."""
        members = []
        for member in self.source.get_paginated("/tenants/current/members/active"):
            if isinstance(member, dict):
                members.append(member)
        return members

    def list_dest_workspace_members(self) -> List[Dict[str, Any]]:
        """Fetch active workspace members from destination (requires X-Tenant-Id)."""
        members = []
        for member in self.dest.get_paginated("/tenants/current/members/active"):
            if isinstance(member, dict):
                members.append(member)
        return members

    def ensure_dest_email_index(self, force: bool = False) -> Dict[str, Dict[str, Any]]:
        """Fetch and cache the destination org-member email index."""
        if self._dest_email_to_identity is None or force:
            dest_members = self.list_dest_org_members()
            self._dest_email_to_identity = {
                (member.get("email") or "").lower(): member
                for member in dest_members
                if member.get("email")
            }
        return self._dest_email_to_identity

    # ------------------------------------------------------------------
    # Phase 1: role synchronisation
    # ------------------------------------------------------------------

    def build_role_mapping(
        self,
        custom_role_ids: set[str] | None = None,
    ) -> Dict[str, str]:
        """Build source_role_id -> dest_role_id mapping.

        1. Fetches roles from both sides.
        2. Matches built-in roles by ``name``.
        3. Creates/updates custom roles on destination, matched by ``display_name``.
        4. Persists the mapping in ``state.id_mappings["roles"]``.

        Args:
            custom_role_ids: Restrict custom-role syncing to this set of source
                role IDs. Pass ``None`` to retain the legacy sync-all behavior.
                Passing an empty set will only map built-in roles.
        """
        source_roles = self.list_source_roles()
        dest_roles = self.list_dest_roles()

        self.log(
            f"Found {len(source_roles)} source roles, {len(dest_roles)} destination roles"
        )

        mapping = dict(self._role_id_map)
        mapping.update(self._match_builtin_roles(source_roles, dest_roles))

        custom_mapping = self._sync_custom_roles(
            source_roles,
            dest_roles,
            only_ids=custom_role_ids,
        )
        mapping.update(custom_mapping)

        self._role_id_map = mapping

        # Persist in state for resume
        if self.state:
            for src_id, dst_id in mapping.items():
                self.state.set_mapped_id("roles", src_id, dst_id)
            self.persist_state()

        return mapping

    def get_source_custom_roles(self) -> List[Dict[str, Any]]:
        """Return only custom roles from the source organisation."""
        return [
            role for role in self.list_source_roles() if role.get("name") == "CUSTOM"
        ]

    def _match_builtin_roles(
        self,
        source_roles: List[Dict[str, Any]],
        dest_roles: List[Dict[str, Any]],
    ) -> Dict[str, str]:
        """Match built-in roles by their ``name`` field."""
        dest_by_name: Dict[str, str] = {}
        for role in dest_roles:
            if role.get("name") != "CUSTOM":
                dest_by_name[role["name"]] = role["id"]

        mapping: Dict[str, str] = {}
        for role in source_roles:
            name = role.get("name", "")
            if name == "CUSTOM":
                continue
            dest_id = dest_by_name.get(name)
            if dest_id:
                mapping[role["id"]] = dest_id
                self.log(f"Matched built-in role: {name}")
            else:
                self.log(
                    f"Built-in role '{name}' not found on destination", "warning"
                )

        return mapping

    def _sync_custom_roles(
        self,
        source_roles: List[Dict[str, Any]],
        dest_roles: List[Dict[str, Any]],
        only_ids: set[str] | None = None,
    ) -> Dict[str, str]:
        """Sync custom roles by ``display_name``.

        Creates missing custom roles on the destination, or updates existing
        ones when permissions differ (unless skip_existing is set).
        """
        dest_by_display: Dict[str, Dict[str, Any]] = {}
        for role in dest_roles:
            if role.get("name") == "CUSTOM":
                dest_by_display[role.get("display_name", "")] = role

        mapping: Dict[str, str] = {}

        for role in source_roles:
            if role.get("name") != "CUSTOM":
                continue
            if only_ids is not None and role["id"] not in only_ids:
                continue

            display_name = role.get("display_name", "")
            existing = dest_by_display.get(display_name)

            if existing:
                mapping[role["id"]] = existing["id"]
                if self.config.migration.skip_existing:
                    self.log(f"Custom role '{display_name}' exists, skipping")
                    continue

                # Check if permissions differ
                src_perms = set(role.get("permissions", []))
                dst_perms = set(existing.get("permissions", []))
                if src_perms != dst_perms:
                    if not self._update_custom_role(existing["id"], role):
                        self.log(
                            f"Custom role '{display_name}' mapped but permissions may be stale",
                            "warning",
                        )
                        self.record_issue(
                            "data_integrity",
                            "custom_role_update_failed",
                            f"Failed to update permissions for custom role '{display_name}'",
                            evidence={"role": role, "dest_role_id": existing["id"]},
                        )
                else:
                    self.log(f"Custom role '{display_name}' already up to date")
            else:
                dest_id = self._create_custom_role(role)
                if dest_id:
                    mapping[role["id"]] = dest_id

        return mapping

    def _create_custom_role(self, role: Dict[str, Any]) -> Optional[str]:
        """Create a custom role on the destination."""
        display_name = role.get("display_name", "")
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create custom role: {display_name}")
            return f"dry-run-{role['id']}"

        payload = {
            "display_name": display_name,
            "description": role.get("description", ""),
            "permissions": role.get("permissions", []),
        }

        try:
            response = self.dest.post("/orgs/current/roles", payload)
            dest_id = response.get("id") if isinstance(response, dict) else None
            if not dest_id:
                raise APIError(
                    f"Invalid response creating role: {response}"
                )
            self.log(f"Created custom role: {display_name}", "success")
            return dest_id
        except APIError as e:
            self.log(f"Failed to create custom role '{display_name}': {e}", "error")
            self.record_issue(
                "capability",
                "custom_role_create_failed",
                f"Failed to create custom role '{display_name}': {e}",
                evidence={"role": role, "error": str(e)},
            )
            return None

    def _update_custom_role(
        self, dest_role_id: str, source_role: Dict[str, Any]
    ) -> bool:
        """Update a custom role's permissions on the destination.

        Returns True on success, False on failure.
        """
        display_name = source_role.get("display_name", "")
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would update custom role: {display_name}")
            return True

        payload = {
            "display_name": display_name,
            "description": source_role.get("description", ""),
            "permissions": source_role.get("permissions", []),
        }

        try:
            self.dest.patch(f"/orgs/current/roles/{dest_role_id}", payload)
            self.log(f"Updated custom role: {display_name}", "success")
            return True
        except APIError as e:
            self.log(
                f"Failed to update custom role '{display_name}': {e}", "error"
            )
            return False

    # ------------------------------------------------------------------
    # Phase 2: organisation member migration
    # ------------------------------------------------------------------

    def migrate_org_members(
        self,
        selected_members: List[Dict[str, Any]],
        *,
        remove_missing: bool = False,
        remove_pending: bool = False,
    ) -> Tuple[int, int, int]:
        """Migrate selected org members from source to destination.

        Returns ``(migrated, skipped, failed)`` counts.
        """
        dest_members = self.list_dest_org_members()
        pending_dest_members = (
            self.list_dest_pending_org_members() if remove_pending else []
        )

        dest_by_email: Dict[str, Dict[str, Any]] = {}
        for m in dest_members:
            email = (m.get("email") or "").lower()
            if email:
                dest_by_email[email] = m

        pending_by_email: Dict[str, Dict[str, Any]] = {}
        for m in pending_dest_members:
            email = (m.get("email") or "").lower()
            if email:
                pending_by_email[email] = m

        self._dest_email_to_identity = dest_by_email

        migrated = skipped = failed = 0
        removed = 0
        desired_emails: set[str] = set()

        for member in selected_members:
            email = (member.get("email") or "").lower()
            if not email:
                self.log("Skipping member with no email", "warning")
                skipped += 1
                continue
            desired_emails.add(email)

            if member.get("_pending"):
                self.log(f"Processing pending invite: {email}")

            item_id = f"org_member_{email}"
            self.ensure_item(
                item_id, "org_member", email,
                member.get("id", email),
                metadata={"member": member},
            )

            source_role_id = (member.get("role_id") or "").strip() or None
            mapped_role_id = self._role_id_map.get(source_role_id) if source_role_id else None

            if source_role_id and not mapped_role_id:
                self.log(
                    f"No role mapping for {email} (role_id={source_role_id})",
                    "warning",
                )
                self.mark_blocked(
                    item_id, "unmapped_role",
                    next_action="Run phase 1 (role sync) and retry.",
                    evidence={"email": email, "source_role_id": source_role_id},
                )
                failed += 1
                continue

            dest_member = dest_by_email.get(email)
            pending_member = pending_by_email.get(email)

            if dest_member:
                if self.config.migration.skip_existing:
                    self.log(f"Org member '{email}' exists, skipping")
                    self.mark_migrated(item_id, outcome_code="org_member_skipped_existing")
                    skipped += 1
                    continue

                dest_role_id = dest_member.get("role_id")
                if mapped_role_id and dest_role_id != mapped_role_id:
                    try:
                        self._update_org_member_role(
                            dest_member["id"], mapped_role_id
                        )
                        self.mark_migrated(item_id, outcome_code="org_member_migrated")
                        migrated += 1
                    except (AuthenticationError, APIError) as e:
                        self.log(
                            f"Failed to update role for '{email}': {e}",
                            "error",
                        )
                        self.mark_blocked(
                            item_id, "org_member_role_update_failed",
                            next_action="Re-run `langsmith-migrator users`.",
                            evidence={"email": email, "error": str(e)},
                        )
                        failed += 1
                else:
                    self.log(f"Org member '{email}' already has correct role")
                    self.mark_migrated(item_id, outcome_code="org_member_skipped_existing")
                    skipped += 1
            elif pending_member:
                dest_role_id = pending_member.get("role_id")
                if mapped_role_id and dest_role_id != mapped_role_id:
                    if self.config.migration.skip_existing:
                        self.log(
                            f"Org invite for '{email}' exists, skipping role update",
                            "warning",
                        )
                        self.mark_migrated(
                            item_id,
                            outcome_code="org_member_skipped_existing",
                        )
                        skipped += 1
                        continue

                    try:
                        self._remove_org_member(
                            pending_member["id"],
                            pending=True,
                        )
                        self._invite_org_member(email, mapped_role_id)
                        self.mark_migrated(
                            item_id,
                            outcome_code="org_member_migrated",
                        )
                        migrated += 1
                    except (AuthenticationError, APIError) as e:
                        self.log(
                            f"Failed to replace pending invite for '{email}': {e}",
                            "error",
                        )
                        self.mark_blocked(
                            item_id,
                            "org_member_pending_invite_replace_failed",
                            next_action="Re-run `langsmith-migrator users`.",
                            evidence={"email": email, "error": str(e)},
                        )
                        failed += 1
                else:
                    self.log(
                        f"Invite for '{email}' already pending, skipping",
                        "warning",
                    )
                    self.mark_migrated(
                        item_id,
                        outcome_code="org_member_skipped_existing",
                    )
                    skipped += 1
            else:
                try:
                    self._invite_org_member(email, mapped_role_id)
                    self.mark_migrated(item_id, outcome_code="org_member_migrated")
                    migrated += 1
                except ConflictError:
                    self.log(
                        f"Invite for '{email}' already pending, skipping",
                        "warning",
                    )
                    self.mark_migrated(item_id, outcome_code="org_member_skipped_existing")
                    skipped += 1
                except (AuthenticationError, APIError) as e:
                    self.log(f"Failed to invite '{email}': {e}", "error")
                    self.mark_blocked(
                        item_id, "org_member_invite_failed",
                        next_action="Re-run `langsmith-migrator users`.",
                        evidence={"email": email, "error": str(e)},
                    )
                    failed += 1

        if remove_missing:
            extra_members = [
                member
                for email, member in dest_by_email.items()
                if email not in desired_emails
            ]
            extra_members.extend(
                member
                for email, member in pending_by_email.items()
                if email not in desired_emails
            )
            extra_members.sort(key=lambda member: (member.get("email") or "").lower())

            pending_extra_ids = {
                member.get("id")
                for email, member in pending_by_email.items()
                if email not in desired_emails
            }
            for member in extra_members:
                email = (member.get("email") or "").lower()
                item_id = f"org_member_{email}"
                self.ensure_item(
                    item_id,
                    "org_member",
                    email,
                    member.get("id", email),
                    metadata={"member": member},
                )
                try:
                    self._remove_org_member(
                        member["id"],
                        pending=member.get("id") in pending_extra_ids,
                    )
                    self.mark_migrated(
                        item_id,
                        outcome_code="org_member_removed_from_source_of_truth",
                    )
                    migrated += 1
                    removed += 1
                except (AuthenticationError, APIError) as e:
                    self.log(
                        f"Failed to remove org member '{email}': {e}",
                        "error",
                    )
                    self.mark_blocked(
                        item_id,
                        "org_member_remove_failed",
                        next_action="Re-run `langsmith-migrator users`.",
                        evidence={"email": email, "error": str(e)},
                    )
                    failed += 1

        self._last_org_member_removals = removed
        return migrated, skipped, failed

    def _invite_org_member(
        self,
        email: str,
        role_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Invite a new member to the destination org."""
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would invite {email}")
            return {"id": f"dry-run-{email}"}

        payload: Dict[str, Any] = {"email": email}
        if role_id:
            payload["role_id"] = role_id

        response = self.dest.post("/orgs/current/members", payload)
        self.log(f"Invited {email}", "success")
        return response

    def _update_org_member_role(self, identity_id: str, role_id: str) -> None:
        """Update an existing org member's role."""
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would update org member role: {identity_id}")
            return

        self.dest.patch(
            f"/orgs/current/members/{identity_id}", {"role_id": role_id}
        )
        self.log(f"Updated org member role: {identity_id}", "success")

    def _remove_org_member(
        self,
        identity_id: str,
        *,
        pending: bool = False,
    ) -> None:
        """Remove an active org member or cancel a pending invite."""
        if self.config.migration.dry_run:
            action = "cancel org invite" if pending else "remove org member"
            self.log(f"[DRY RUN] Would {action}: {identity_id}")
            return

        self.dest.delete(f"/orgs/current/members/{identity_id}")
        action = "Cancelled org invite" if pending else "Removed org member"
        self.log(f"{action}: {identity_id}", "success")

    # ------------------------------------------------------------------
    # Phase 3: workspace member migration
    # ------------------------------------------------------------------

    def migrate_workspace_members_from_csv_rows(
        self, csv_rows: List[Dict[str, Any]]
    ) -> Tuple[int, int, int]:
        """Migrate members for the active source workspace from CSV-shaped rows.

        CSV rows must include ``email``, ``role_id``, and ``workspace_id`` fields.
        The current source workspace is inferred from ``X-Tenant-Id``.
        """
        source_workspace_id = self.source.session.headers.get("X-Tenant-Id")
        if not source_workspace_id:
            raise APIError(
                "Workspace context is required to migrate workspace members from CSV",
                request_info={},
            )

        selected_by_email: Dict[str, Dict[str, Any]] = {}
        for row in csv_rows:
            if row.get("workspace_id") != source_workspace_id:
                continue

            email = (row.get("email") or "").strip().lower()
            role_id = (row.get("role_id") or "").strip()
            if not email:
                continue

            existing = selected_by_email.get(email)
            if existing and existing["role_id"] != role_id:
                raise APIError(
                    (
                        "Conflicting workspace role_id values in CSV rows for "
                        f"{email} in workspace {source_workspace_id}"
                    ),
                    request_info={},
                )
            if not existing:
                selected_by_email[email] = {
                    "id": row.get("id") or f"{source_workspace_id}:{email}",
                    "email": email,
                    "role_id": role_id,
                    "full_name": row.get("full_name", ""),
                }

        return self.migrate_workspace_members(
            selected_members=list(selected_by_email.values())
        )

    def migrate_workspace_members(
        self,
        selected_members: Optional[List[Dict[str, Any]]] = None,
        *,
        remove_missing: bool = False,
    ) -> Tuple[int, int, int]:
        """Migrate workspace members for the currently scoped workspace pair.

        Assumes:
          - Role mapping is built (phase 1).
          - Org members are migrated (phase 2).
          - X-Tenant-Id is set on both clients.

        Returns ``(migrated, skipped, failed)`` counts.
        """
        source_members = (
            selected_members
            if selected_members is not None
            else self.list_source_workspace_members()
        )

        dest_members = self.list_dest_workspace_members()

        dest_by_email: Dict[str, Dict[str, Any]] = {}
        for m in dest_members:
            email = (m.get("email") or "").lower()
            if email:
                dest_by_email[email] = m

        dest_identity = self._dest_email_to_identity or {}

        ws_pair = self.workspace_pair()
        src_ws = ws_pair.get("source")
        if not src_ws:
            raise APIError(
                "Active source workspace is required for workspace member migration",
                request_info={},
            )

        if not source_members and not remove_missing:
            self._last_workspace_member_removals = 0
            return 0, 0, 0

        migrated = skipped = failed = 0
        removed = 0
        desired_emails: set[str] = set()

        for member in source_members:
            email = (member.get("email") or "").lower()
            if not email:
                skipped += 1
                continue
            desired_emails.add(email)

            item_id = f"ws_member_{src_ws}_{email}"
            self.ensure_item(
                item_id, "ws_member", email,
                member.get("id", email),
                metadata={"member": member, "workspace_pair": ws_pair},
            )

            source_role_id = (member.get("role_id") or "").strip() or None
            mapped_role_id = (
                self._role_id_map.get(source_role_id)
                if source_role_id
                else None
            )

            if source_role_id and not mapped_role_id:
                self.log(
                    f"No role mapping for workspace member {email}", "warning"
                )
                self.mark_blocked(
                    item_id, "unmapped_role",
                    next_action="Run phase 1 (role sync) and retry.",
                    evidence={"email": email, "source_role_id": source_role_id},
                )
                failed += 1
                continue

            dest_member = dest_by_email.get(email)

            if dest_member:
                if self.config.migration.skip_existing:
                    self.mark_migrated(item_id, outcome_code="ws_member_skipped_existing")
                    skipped += 1
                    continue

                dest_role_id = dest_member.get("role_id")
                if mapped_role_id and dest_role_id != mapped_role_id:
                    try:
                        self._update_workspace_member_role(
                            dest_member["id"], mapped_role_id
                        )
                        self.mark_migrated(item_id, outcome_code="ws_member_migrated")
                        migrated += 1
                    except (AuthenticationError, APIError) as e:
                        self.log(
                            f"Failed to update workspace role for '{email}': {e}",
                            "error",
                        )
                        self.mark_blocked(
                            item_id, "ws_member_role_update_failed",
                            next_action="Re-run `langsmith-migrator users`.",
                            evidence={"email": email, "error": str(e)},
                        )
                        failed += 1
                else:
                    self.mark_migrated(item_id, outcome_code="ws_member_skipped_existing")
                    skipped += 1
            else:
                dest_org_member = dest_identity.get(email)
                if not dest_org_member:
                    self.log(
                        f"Cannot add '{email}' to workspace: not an org member",
                        "warning",
                    )
                    self.mark_blocked(
                        item_id, "ws_member_not_in_org",
                        next_action="Migrate the user as an org member first, then retry.",
                        evidence={"email": email},
                    )
                    failed += 1
                    continue

                try:
                    self._add_workspace_member(
                        dest_org_member["id"], mapped_role_id
                    )
                    self.mark_migrated(item_id, outcome_code="ws_member_migrated")
                    migrated += 1
                except ConflictError:
                    self.log(
                        f"Workspace member '{email}' already exists",
                        "warning",
                    )
                    self.mark_migrated(item_id, outcome_code="ws_member_skipped_existing")
                    skipped += 1
                except (AuthenticationError, APIError) as e:
                    self.log(
                        f"Failed to add '{email}' to workspace: {e}", "error"
                    )
                    self.mark_blocked(
                        item_id, "ws_member_add_failed",
                        next_action="Re-run `langsmith-migrator users`.",
                        evidence={"email": email, "error": str(e)},
                    )
                    failed += 1

        if remove_missing:
            extra_members = [
                member
                for email, member in dest_by_email.items()
                if email not in desired_emails
            ]
            extra_members.sort(key=lambda member: (member.get("email") or "").lower())

            for member in extra_members:
                email = (member.get("email") or "").lower()
                item_id = f"ws_member_{src_ws}_{email}"
                self.ensure_item(
                    item_id,
                    "ws_member",
                    email,
                    member.get("id", email),
                    metadata={"member": member, "workspace_pair": ws_pair},
                )
                try:
                    self._remove_workspace_member(member["id"])
                    self.mark_migrated(
                        item_id,
                        outcome_code="ws_member_removed_from_source_of_truth",
                    )
                    migrated += 1
                    removed += 1
                except (AuthenticationError, APIError) as e:
                    self.log(
                        f"Failed to remove workspace member '{email}': {e}",
                        "error",
                    )
                    self.mark_blocked(
                        item_id,
                        "ws_member_remove_failed",
                        next_action="Re-run `langsmith-migrator users`.",
                        evidence={"email": email, "error": str(e)},
                    )
                    failed += 1

        self._last_workspace_member_removals = removed
        return migrated, skipped, failed

    def _add_workspace_member(
        self, org_identity_id: str, role_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Add an org member to the current workspace."""
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would add workspace member: {org_identity_id}")
            return {"id": f"dry-run-{org_identity_id}"}

        payload: Dict[str, Any] = {"org_identity_id": org_identity_id}
        if role_id:
            payload["role_id"] = role_id

        response = self.dest.post("/tenants/current/members", payload)
        self.log(f"Added workspace member: {org_identity_id}", "success")
        return response

    def _update_workspace_member_role(
        self, identity_id: str, role_id: str
    ) -> None:
        """Update a workspace member's role."""
        if self.config.migration.dry_run:
            self.log(
                f"[DRY RUN] Would update workspace member role: {identity_id}"
            )
            return

        self.dest.patch(
            f"/tenants/current/members/{identity_id}", {"role_id": role_id}
        )
        self.log(f"Updated workspace member role: {identity_id}", "success")

    def _remove_workspace_member(self, identity_id: str) -> None:
        """Remove a workspace member from the current workspace."""
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would remove workspace member: {identity_id}")
            return

        self.dest.delete(f"/tenants/current/members/{identity_id}")
        self.log(f"Removed workspace member: {identity_id}", "success")

    # ------------------------------------------------------------------
    # Capability probing
    # ------------------------------------------------------------------

    def probe_capabilities(self) -> None:
        """Test API endpoints to verify roles/members APIs are accessible."""
        for label, client, scope in [
            ("source", self.source, "source"),
            ("destination", self.dest, "dest"),
        ]:
            # Roles endpoint
            try:
                client.get("/orgs/current/roles")
                self.record_capability(
                    scope, "roles_api", supported=True, probe="/orgs/current/roles"
                )
            except Exception as e:
                self.record_capability(
                    scope,
                    "roles_api",
                    supported=False,
                    detail=str(e),
                    probe="/orgs/current/roles",
                )

            # Members endpoint
            try:
                client.get(
                    "/orgs/current/members/active", params={"limit": 1}
                )
                self.record_capability(
                    scope,
                    "members_api",
                    supported=True,
                    probe="/orgs/current/members/active",
                )
            except Exception as e:
                self.record_capability(
                    scope,
                    "members_api",
                    supported=False,
                    detail=str(e),
                    probe="/orgs/current/members/active",
                )
