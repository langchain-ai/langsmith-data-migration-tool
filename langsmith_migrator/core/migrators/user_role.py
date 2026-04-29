"""User and role migration logic."""

import hashlib
import json
from typing import Any, Dict, List, Optional, Tuple

from .base import BaseMigrator
from ...utils.retry import APIError, AuthenticationError, ConflictError


WORKSPACE_ROLE_UNION_PREFIX = "union::workspace::"
WORKSPACE_ROLE_UNION_DISPLAY_PREFIX = "LangSmith Migrator Union"
WORKSPACE_ROLE_PRECEDENCE = {
    "WORKSPACE_VIEWER": 10,
    "WORKSPACE_USER": 20,
    "WORKSPACE_EDITOR": 20,
    "WORKSPACE_ADMIN": 30,
}
ORG_MEMBER_MANAGEMENT_CAPABILITY = "org_member_management"
ORG_ADMIN_PAT_REQUIRED_CODE = "dest_org_admin_pat_required"
ORG_ADMIN_PAT_NEXT_ACTION = (
    "Retry with an Organization Admin PAT for the destination organization, "
    "or perform the org member or pending invite change manually and re-run "
    "`langsmith-migrator users`."
)


def make_workspace_role_union_id(role_ids: set[str]) -> str:
    """Return a stable synthetic role ID for a set of source workspace role IDs."""
    normalized = sorted({role_id.strip() for role_id in role_ids if role_id.strip()})
    return WORKSPACE_ROLE_UNION_PREFIX + ",".join(normalized)


def is_workspace_role_union_id(role_id: str | None) -> bool:
    """Return whether *role_id* is a synthetic workspace role union ID."""
    return bool(role_id and role_id.startswith(WORKSPACE_ROLE_UNION_PREFIX))


def parse_workspace_role_union_id(role_id: str) -> set[str]:
    """Return source role IDs embedded in a synthetic workspace role union ID."""
    if not is_workspace_role_union_id(role_id):
        return set()
    encoded = role_id[len(WORKSPACE_ROLE_UNION_PREFIX):]
    return {part for part in encoded.split(",") if part}


def is_org_member_management_permission_error(error: BaseException) -> bool:
    """Return whether an API error looks like missing org-member management access."""
    status_code = getattr(error, "status_code", None)
    return isinstance(error, AuthenticationError) or status_code in {401, 403}


def is_member_absent_error(error: BaseException) -> bool:
    """Return whether a member mutation error means the member is already absent."""
    status_code = getattr(error, "status_code", None)
    message = str(error).lower()
    return status_code == 404 or "not found" in message


def org_admin_pat_required_message(operation: str, error: BaseException) -> str:
    """Build a consistent org-admin PAT requirement message."""
    return (
        f"{operation} requires an Organization Admin PAT on the destination "
        "organization. The current destination API key does not appear to be "
        f"allowed to manage organization members or pending invites. Original "
        f"error: {error}"
    )


def org_admin_pat_required_evidence(
    operation: str,
    error: BaseException,
) -> Dict[str, Any]:
    """Build evidence payload for org-member permission failures."""
    return {
        "operation": operation,
        "status_code": getattr(error, "status_code", None),
        "error": str(error),
        "requires_org_admin_pat": True,
    }


def select_effective_workspace_role_id(
    rows: List[Dict[str, Any]],
    *,
    email: str,
    workspace_id: str,
) -> str:
    """Collapse one user's CSV rows for a workspace to one effective role ID."""
    role_rows = [
        row
        for row in rows
        if (row.get("role_id") or "").strip()
    ]
    role_ids = {
        (row.get("role_id") or "").strip()
        for row in role_rows
        if (row.get("role_id") or "").strip()
    }
    if not role_ids:
        return ""
    if len(role_ids) == 1:
        return next(iter(role_ids))

    custom_role_ids = {
        (row.get("role_id") or "").strip()
        for row in role_rows
        if (row.get("role_name") or "").strip().upper() == "CUSTOM"
    }
    builtin_rows = [
        row
        for row in role_rows
        if (row.get("role_name") or "").strip().upper() in WORKSPACE_ROLE_PRECEDENCE
    ]
    unknown_role_labels = sorted(
        {
            row.get("role_name") or row.get("langsmith_role") or row.get("role_id") or "<unknown>"
            for row in role_rows
            if (
                (row.get("role_name") or "").strip().upper() not in WORKSPACE_ROLE_PRECEDENCE
                and (row.get("role_name") or "").strip().upper() != "CUSTOM"
            )
        },
        key=str.lower,
    )
    if unknown_role_labels:
        raise ValueError(
            "Members CSV has unsupported workspace role type(s) for "
            f"{email} in workspace {workspace_id}: "
            + ", ".join(unknown_role_labels)
        )

    if custom_role_ids:
        union_role_ids = set(custom_role_ids)
        union_role_ids.update(
            (row.get("role_id") or "").strip()
            for row in builtin_rows
            if (row.get("role_id") or "").strip()
        )
        if len(union_role_ids) == 1:
            return next(iter(union_role_ids))
        return make_workspace_role_union_id(union_role_ids)

    admin_role_ids = {
        (row.get("role_id") or "").strip()
        for row in role_rows
        if (row.get("role_name") or "").strip().upper() == "WORKSPACE_ADMIN"
    }
    if admin_role_ids:
        return sorted(admin_role_ids)[0]

    best_row = max(
        builtin_rows,
        key=lambda row: WORKSPACE_ROLE_PRECEDENCE[
            (row.get("role_name") or "").strip().upper()
        ],
    )
    return (best_row.get("role_id") or "").strip()


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
        self._pending_org_email_to_identity: Dict[str, Dict[str, Any]] = {}
        self._pending_workspace_invites: set[tuple[str, str]] = set()
        self._pending_org_blockers: Dict[str, Dict[str, Any]] = {}
        self._pending_org_invite_wait_reported: set[str] = set()
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

    def require_destination_org_admin_for_authoritative_sync(self) -> None:
        """Fail fast when authoritative sync cannot read org-member state.

        ``--csv-source-of-truth`` can remove active members and cancel pending
        invites, so a non-admin PAT cannot safely provide the requested
        semantics. There is no safe read-only delete probe; this verifies the
        read side up front and write failures still report the same PAT hint.
        """
        probes = [
            (
                "Listing active organization members for authoritative users sync",
                "/orgs/current/members/active",
            ),
            (
                "Listing pending organization invites for authoritative users sync",
                "/orgs/current/members/pending",
            ),
        ]
        for operation, endpoint in probes:
            try:
                self.dest.get(endpoint, params={"limit": 1})
            except (AuthenticationError, APIError) as e:
                if is_org_member_management_permission_error(e):
                    message = org_admin_pat_required_message(operation, e)
                    evidence = {
                        "endpoint": endpoint,
                        **org_admin_pat_required_evidence(operation, e),
                    }
                    self.record_capability(
                        "dest",
                        ORG_MEMBER_MANAGEMENT_CAPABILITY,
                        supported=False,
                        detail=message,
                        evidence=evidence,
                        probe=f"GET {endpoint}?limit=1",
                    )
                    self.record_issue(
                        "capability",
                        ORG_ADMIN_PAT_REQUIRED_CODE,
                        message,
                        next_action=ORG_ADMIN_PAT_NEXT_ACTION,
                        evidence=evidence,
                    )
                    raise APIError(
                        message,
                        status_code=getattr(e, "status_code", None),
                        request_info=getattr(e, "request_info", None),
                    ) from e
                raise

        self.record_capability(
            "dest",
            ORG_MEMBER_MANAGEMENT_CAPABILITY,
            supported=True,
            detail="Active and pending org-member list probes succeeded",
            evidence={"endpoints": [endpoint for _, endpoint in probes]},
            probe="GET /orgs/current/members/{active,pending}?limit=1",
        )

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
            active_by_email = {
                (member.get("email") or "").lower(): member
                for member in dest_members
                if member.get("email")
            }
            for email, member in self._pending_org_email_to_identity.items():
                active_by_email.setdefault(email, member)
            self._dest_email_to_identity = active_by_email
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

    def list_source_access_policies(self) -> List[Dict[str, Any]]:
        """Fetch ABAC access policies from the source organisation when supported."""
        return self._list_access_policies(self.source)

    def list_dest_access_policies(self) -> List[Dict[str, Any]]:
        """Fetch ABAC access policies from the destination organisation when supported."""
        return self._list_access_policies(self.dest)

    def materialize_workspace_role_unions(
        self,
        union_role_ids: set[str],
    ) -> Dict[str, str]:
        """Create/reuse managed destination roles for synthetic workspace role unions."""
        requested_union_ids = {
            role_id
            for role_id in union_role_ids
            if is_workspace_role_union_id(role_id)
        }
        if not requested_union_ids:
            return {}

        source_roles = self.list_source_roles()
        dest_roles = self.list_dest_roles()
        source_role_by_id = {
            role["id"]: role
            for role in source_roles
            if role.get("id")
        }
        dest_by_display = {
            role.get("display_name", ""): role
            for role in dest_roles
            if role.get("name") == "CUSTOM"
        }
        source_policies = self.list_source_access_policies()
        dest_policies = self.list_dest_access_policies()

        mapping: Dict[str, str] = {}
        for union_role_id in sorted(requested_union_ids):
            source_role_ids = parse_workspace_role_union_id(union_role_id)
            source_union_roles = [
                source_role_by_id[role_id]
                for role_id in sorted(source_role_ids)
                if role_id in source_role_by_id
            ]
            missing_role_ids = source_role_ids - {
                role["id"] for role in source_union_roles
            }
            if missing_role_ids:
                raise APIError(
                    "Cannot create workspace role union because source role "
                    f"metadata is missing for: {', '.join(sorted(missing_role_ids))}",
                    request_info={},
                )

            union_role = self._build_workspace_union_role(
                union_role_id,
                source_union_roles,
            )
            display_name = union_role["display_name"]
            existing = dest_by_display.get(display_name)
            if existing:
                dest_role_id = existing["id"]
                self.log(f"Reused managed workspace role union {display_name}")
                if self.config.migration.skip_existing:
                    self.log(f"Workspace role union '{display_name}' exists, skipping")
                    mapping[union_role_id] = dest_role_id
                    continue

                if set(existing.get("permissions", [])) != set(union_role["permissions"]):
                    if not self._update_custom_role(dest_role_id, union_role):
                        self.log(
                            f"Workspace role union '{display_name}' mapped but "
                            "permissions may be stale",
                            "warning",
                        )
                else:
                    self.log(f"Workspace role union '{display_name}' already up to date")
            else:
                dest_role_id = self._create_custom_role(union_role)
                if not dest_role_id:
                    raise APIError(
                        f"Failed to create workspace role union '{display_name}'",
                        request_info={},
                    )
                self.log(
                    f"Created managed workspace role union {display_name}",
                    "success",
                )
                dest_by_display[display_name] = {
                    **union_role,
                    "id": dest_role_id,
                    "name": "CUSTOM",
                }

            self._attach_union_access_policies(
                source_role_ids,
                dest_role_id,
                source_policies,
                dest_policies,
                union_role_hash=self._workspace_union_hash(source_role_ids),
            )
            mapping[union_role_id] = dest_role_id
            self.log(
                f"Resolved workspace role union {union_role_id} -> {dest_role_id}"
            )

        self._role_id_map.update(mapping)
        if self.state:
            for src_id, dst_id in mapping.items():
                self.state.set_mapped_id("roles", src_id, dst_id)
            self.persist_state()
        return mapping

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

    def _platform_url(self, client, endpoint: str) -> str:
        """Return an absolute platform API URL for clients based at /api/v1."""
        base_url = getattr(client, "base_url", "").rstrip("/")
        for suffix in ("/api/v1", "/api/v2"):
            if base_url.endswith(suffix):
                base_url = base_url[: -len(suffix)]
                break
        return f"{base_url}{endpoint}"

    def _list_access_policies(self, client) -> List[Dict[str, Any]]:
        """List ABAC access policies, returning [] when the endpoint is unsupported."""
        endpoint = self._platform_url(
            client,
            "/v1/platform/orgs/current/access-policies",
        )
        try:
            response = client.get(endpoint)
        except APIError as e:
            if getattr(e, "status_code", None) == 404:
                self.log("ABAC access policy API is not available", "info")
                return []
            raise
        if isinstance(response, dict):
            policies = response.get("access_policies", [])
            return policies if isinstance(policies, list) else []
        if isinstance(response, list):
            return response
        return []

    def _workspace_union_hash(self, source_role_ids: set[str]) -> str:
        """Return a short stable hash for a synthetic workspace role union."""
        encoded = json.dumps(sorted(source_role_ids), separators=(",", ":"))
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()[:12]

    def _build_workspace_union_role(
        self,
        union_role_id: str,
        source_roles: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Build the managed custom role payload for a workspace role union."""
        source_role_ids = parse_workspace_role_union_id(union_role_id)
        union_hash = self._workspace_union_hash(source_role_ids)
        has_custom_role = any(role.get("name") == "CUSTOM" for role in source_roles)
        builtin_roles_without_permissions = [
            role
            for role in source_roles
            if (
                has_custom_role
                and role.get("name") in WORKSPACE_ROLE_PRECEDENCE
                and not role.get("permissions")
            )
        ]
        if builtin_roles_without_permissions:
            labels = [
                role.get("display_name") or role.get("name") or role["id"]
                for role in builtin_roles_without_permissions
            ]
            raise APIError(
                "Cannot create workspace role union because built-in role "
                "permissions were not exposed by the roles API for: "
                + ", ".join(sorted(labels)),
                request_info={},
            )
        source_labels = [
            f"{role.get('display_name') or role.get('name') or role['id']} ({role['id']})"
            for role in source_roles
        ]
        permissions = sorted(
            {
                permission
                for role in source_roles
                for permission in (role.get("permissions") or [])
                if permission
            }
        )
        return {
            "id": union_role_id,
            "name": "CUSTOM",
            "display_name": f"{WORKSPACE_ROLE_UNION_DISPLAY_PREFIX} {union_hash}",
            "description": (
                "Managed by langsmith-migrator. Union of source workspace roles: "
                + "; ".join(source_labels)
            ),
            "permissions": permissions,
        }

    def _access_policy_signature(self, policy: Dict[str, Any]) -> str:
        """Return a stable signature for policy equivalence across instances."""
        payload = {
            "name": policy.get("name") or "",
            "description": policy.get("description") or "",
            "effect": policy.get("effect") or "",
            "condition_groups": policy.get("condition_groups") or [],
        }
        return json.dumps(payload, sort_keys=True, separators=(",", ":"))

    def _access_policy_payload(
        self,
        policy: Dict[str, Any],
        *,
        role_ids: List[str],
        fallback_name: str | None = None,
    ) -> Dict[str, Any]:
        """Build the create payload for an ABAC access policy clone."""
        return {
            "name": fallback_name or policy.get("name") or "Migrated access policy",
            "description": policy.get("description") or "",
            "effect": policy.get("effect") or "",
            "condition_groups": policy.get("condition_groups") or [],
            "role_ids": role_ids,
        }

    def _managed_union_policy_name(
        self,
        policy: Dict[str, Any],
        *,
        union_role_hash: str,
    ) -> str:
        """Return the deterministic managed fallback name for a cloned policy."""
        return (
            f"{WORKSPACE_ROLE_UNION_DISPLAY_PREFIX} {union_role_hash}: "
            f"{policy.get('name') or policy.get('id') or 'policy'}"
        )

    def _attach_union_access_policies(
        self,
        source_role_ids: set[str],
        dest_role_id: str,
        source_policies: List[Dict[str, Any]],
        dest_policies: List[Dict[str, Any]],
        *,
        union_role_hash: str,
    ) -> None:
        """Clone or attach ABAC policies that apply to any source role in a union."""
        relevant_policies = [
            policy
            for policy in source_policies
            if source_role_ids.intersection(set(policy.get("role_ids") or []))
        ]
        if not relevant_policies:
            return

        dest_by_id = {
            policy.get("id"): policy
            for policy in dest_policies
            if policy.get("id")
        }
        dest_by_signature = {
            self._access_policy_signature(policy): policy
            for policy in dest_policies
        }
        dest_by_name = {
            policy.get("name"): policy
            for policy in dest_policies
            if policy.get("name")
        }

        for source_policy in relevant_policies:
            dest_policy = dest_by_id.get(source_policy.get("id"))
            if not dest_policy:
                dest_policy = dest_by_signature.get(
                    self._access_policy_signature(source_policy)
                )
            if not dest_policy:
                dest_policy = dest_by_name.get(
                    self._managed_union_policy_name(
                        source_policy,
                        union_role_hash=union_role_hash,
                    )
                )

            if not dest_policy:
                dest_policy = self._create_access_policy_clone(
                    source_policy,
                    dest_role_id,
                    union_role_hash=union_role_hash,
                )
                if not dest_policy:
                    raise APIError(
                        "Failed to create destination access policy for "
                        f"workspace role union {union_role_hash}",
                        request_info={},
                    )
                dest_policies.append(dest_policy)
                dest_by_id[dest_policy["id"]] = dest_policy
                dest_by_signature[
                    self._access_policy_signature(dest_policy)
                ] = dest_policy
                dest_by_name[dest_policy["name"]] = dest_policy
                continue

            attached_role_ids = {
                role_id for role_id in (dest_policy.get("role_ids") or []) if role_id
            }
            if dest_role_id in attached_role_ids:
                continue
            self._attach_access_policy_to_role(dest_policy["id"], dest_role_id)
            dest_policy["role_ids"] = sorted(attached_role_ids | {dest_role_id})

    def _create_access_policy_clone(
        self,
        source_policy: Dict[str, Any],
        dest_role_id: str,
        *,
        union_role_hash: str,
    ) -> Dict[str, Any]:
        """Create a destination ABAC access policy equivalent to a source policy."""
        endpoint = self._platform_url(
            self.dest,
            "/v1/platform/orgs/current/access-policies",
        )
        payload = self._access_policy_payload(
            source_policy,
            role_ids=[dest_role_id],
        )
        try:
            response = self.dest.post(endpoint, payload)
        except ConflictError:
            managed_name = self._managed_union_policy_name(
                source_policy,
                union_role_hash=union_role_hash,
            )
            payload = self._access_policy_payload(
                source_policy,
                role_ids=[dest_role_id],
                fallback_name=managed_name,
            )
            try:
                response = self.dest.post(endpoint, payload)
            except ConflictError as e:
                self.log(
                    f"Access policy '{managed_name}' already exists but could not "
                    "be matched for attachment",
                    "error",
                )
                self.record_issue(
                    "capability",
                    "access_policy_create_conflict",
                    (
                        f"Access policy '{managed_name}' already exists but could "
                        "not be matched for attachment"
                    ),
                    evidence={"policy": source_policy, "error": str(e)},
                )
                raise
        except APIError as e:
            self.log(
                f"Failed to create access policy '{source_policy.get('name')}': {e}",
                "error",
            )
            self.record_issue(
                "capability",
                "access_policy_create_failed",
                f"Failed to create access policy '{source_policy.get('name')}': {e}",
                evidence={"policy": source_policy, "error": str(e)},
            )
            raise

        policy_id = response.get("id") if isinstance(response, dict) else None
        if not policy_id:
            self.record_issue(
                "data_integrity",
                "access_policy_create_missing_id",
                "Access policy create response did not include an id",
                evidence={"policy": source_policy, "response": response},
            )
            raise APIError(
                "Access policy create response did not include an id",
                request_info={"response": response},
            )
        self.log(f"Created access policy: {payload['name']}", "success")
        return {
            **payload,
            "id": policy_id,
        }

    def _attach_access_policy_to_role(
        self,
        access_policy_id: str,
        dest_role_id: str,
    ) -> None:
        """Attach an existing destination ABAC access policy to a destination role."""
        endpoint = self._platform_url(
            self.dest,
            f"/v1/platform/orgs/current/access-policies/roles/{dest_role_id}/access-policies",
        )
        try:
            self.dest.post(
                endpoint,
                {"access_policy_ids": [access_policy_id]},
            )
            self.log(
                f"Attached access policy {access_policy_id} to role {dest_role_id}",
                "success",
            )
        except APIError as e:
            self.log(
                f"Failed to attach access policy {access_policy_id}: {e}",
                "error",
            )
            self.record_issue(
                "capability",
                "access_policy_attach_failed",
                f"Failed to attach access policy {access_policy_id}: {e}",
                evidence={
                    "access_policy_id": access_policy_id,
                    "dest_role_id": dest_role_id,
                    "error": str(e),
                },
            )
            raise

    # ------------------------------------------------------------------
    # Phase 2: organisation member migration
    # ------------------------------------------------------------------

    def _org_member_failure_context(
        self,
        *,
        email: str,
        error: BaseException,
        operation: str,
        fallback_next_action: str,
    ) -> tuple[str, Dict[str, Any]]:
        """Return remediation text/evidence for org-member mutation failures."""
        evidence: Dict[str, Any] = {"email": email, "error": str(error)}
        if is_org_member_management_permission_error(error):
            evidence.update(org_admin_pat_required_evidence(operation, error))
            return ORG_ADMIN_PAT_NEXT_ACTION, evidence
        return fallback_next_action, evidence

    def _pending_invite_evidence(
        self,
        *,
        email: str,
        pending_member: Dict[str, Any],
        desired_org_role_id: Optional[str],
        desired_workspace_ids: List[str],
        desired_workspace_role_id: Optional[str],
        desired_workspace_access_representable: bool = True,
        error: Optional[BaseException] = None,
    ) -> Dict[str, Any]:
        """Return structured evidence for pending invite reconciliation."""
        evidence: Dict[str, Any] = {
            "email": email,
            "existing_pending_invite_id": pending_member.get("id"),
            "existing_org_role_id": pending_member.get("role_id"),
            "existing_workspace_ids": sorted(
                workspace_id
                for workspace_id in (pending_member.get("workspace_ids") or [])
                if workspace_id
            ),
            "existing_workspace_role_id": (
                (pending_member.get("workspace_role_id") or "").strip() or None
            ),
            "desired_org_role_id": desired_org_role_id,
            "desired_workspace_ids": sorted(
                workspace_id for workspace_id in desired_workspace_ids if workspace_id
            ),
            "desired_workspace_role_id": desired_workspace_role_id,
            "desired_workspace_access_representable": (
                desired_workspace_access_representable
            ),
        }
        if error is not None:
            evidence.update(
                {
                    "error": str(error),
                    "status_code": getattr(error, "status_code", None),
                    "request_info": getattr(error, "request_info", None),
                }
            )
        return evidence

    def _mark_pending_org_blocked(
        self,
        *,
        email: str,
        item_id: str,
        code: str,
        next_action: str,
        evidence: Dict[str, Any],
    ) -> None:
        """Mark an org pending-invite blocker and expose it to workspace phase."""
        self._pending_org_blockers[email] = {
            "item_id": item_id,
            "code": code,
            "next_action": next_action,
            "evidence": evidence,
        }
        self.mark_blocked(
            item_id,
            code,
            next_action=next_action,
            evidence=evidence,
        )

    def _log_pending_invite_workspace_payload(
        self,
        *,
        email: str,
        workspace_ids: List[str],
        workspace_role_id: Optional[str],
    ) -> None:
        """Log the workspace access that can be embedded in an org invite."""
        if not workspace_ids or not workspace_role_id:
            return
        self.log(
            "Pending invite for "
            f"{email} will include workspace access: {len(workspace_ids)} "
            f"workspaces with role {workspace_role_id}"
        )

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
            self.list_dest_pending_org_members()
            if (
                remove_pending
                or any(member.get("workspace_ids") for member in selected_members)
            )
            else []
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
            source_workspace_role_id = (
                (member.get("workspace_role_id") or "").strip() or None
            )
            mapped_workspace_role_id = (
                self._role_id_map.get(source_workspace_role_id)
                if source_workspace_role_id
                else None
            )

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
            if source_workspace_role_id and not mapped_workspace_role_id:
                self.log(
                    f"No workspace role mapping for {email} (role_id={source_workspace_role_id})",
                    "warning",
                )
                self.mark_blocked(
                    item_id,
                    "unmapped_workspace_role",
                    next_action="Run phase 1 (role sync) and retry.",
                    evidence={
                        "email": email,
                        "source_workspace_role_id": source_workspace_role_id,
                    },
                )
                failed += 1
                continue

            dest_member = dest_by_email.get(email)
            pending_member = pending_by_email.get(email)
            workspace_ids = [
                workspace_id
                for workspace_id in (member.get("workspace_ids") or [])
                if workspace_id
            ]
            workspace_role_id = mapped_workspace_role_id

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
                        next_action, evidence = self._org_member_failure_context(
                            email=email,
                            error=e,
                            operation="Updating an organization member role",
                            fallback_next_action=(
                                "Review the org role update error in the remediation "
                                "bundle, then re-run `langsmith-migrator users`."
                            ),
                        )
                        self.mark_blocked(
                            item_id, "org_member_role_update_failed",
                            next_action=next_action,
                            evidence=evidence,
                        )
                        failed += 1
                else:
                    self.log(f"Org member '{email}' already has correct role")
                    self.mark_migrated(item_id, outcome_code="org_member_skipped_existing")
                    skipped += 1
            elif pending_member:
                dest_role_id = pending_member.get("role_id")
                needs_workspace_access = bool(workspace_ids and workspace_role_id)
                desired_workspace_ids = {
                    workspace_id
                    for workspace_id in workspace_ids
                    if workspace_id
                }
                pending_workspace_ids = {
                    workspace_id
                    for workspace_id in (pending_member.get("workspace_ids") or [])
                    if workspace_id
                }
                pending_workspace_role_id = (
                    (pending_member.get("workspace_role_id") or "").strip() or None
                )
                pending_covers_workspace_access = (
                    needs_workspace_access
                    and desired_workspace_ids.issubset(pending_workspace_ids)
                    and pending_workspace_role_id == workspace_role_id
                )
                pending_workspace_access_matches = (
                    (
                        pending_workspace_ids == desired_workspace_ids
                        and pending_workspace_role_id == workspace_role_id
                    )
                    if remove_missing
                    else (not needs_workspace_access or pending_covers_workspace_access)
                )
                needs_reinvite = bool(
                    (mapped_role_id and dest_role_id != mapped_role_id)
                    or not pending_workspace_access_matches
                )
                if needs_reinvite:
                    if self.config.migration.skip_existing:
                        self.log(
                            f"Org invite for '{email}' exists, skipping update",
                            "warning",
                        )
                        self.mark_migrated(
                            item_id,
                            outcome_code="org_member_skipped_existing",
                        )
                        skipped += 1
                        continue

                    try:
                        reasons: list[str] = []
                        if mapped_role_id and dest_role_id != mapped_role_id:
                            reasons.append("org role differs")
                        if not pending_workspace_access_matches:
                            reasons.append("workspace access differs")
                        self.log(
                            f"Replacing pending invite for {email}: "
                            + ", ".join(reasons)
                        )
                        pending_removed = self._remove_org_member(
                            pending_member["id"],
                            pending=True,
                            tolerate_missing=True,
                        )
                    except (AuthenticationError, APIError) as e:
                        self.log(
                            f"Failed to cancel pending invite for '{email}': {e}",
                            "error",
                        )
                        next_action, evidence = self._org_member_failure_context(
                            email=email,
                            error=e,
                            operation="Replacing a pending organization invite",
                            fallback_next_action=(
                                "Cancel or replace the pending org invite on the "
                                "target, then re-run `langsmith-migrator users`."
                            ),
                        )
                        evidence.update(
                            self._pending_invite_evidence(
                                email=email,
                                pending_member=pending_member,
                                desired_org_role_id=mapped_role_id,
                                desired_workspace_ids=workspace_ids,
                                desired_workspace_role_id=workspace_role_id,
                                error=e,
                            )
                        )
                        self._mark_pending_org_blocked(
                            email=email,
                            item_id=item_id,
                            code="org_member_pending_invite_replace_failed",
                            next_action=next_action,
                            evidence=evidence,
                        )
                        failed += 1
                        continue

                    if not pending_removed:
                        next_action = (
                            "Cancel or replace the pending org invite on the "
                            "target, then re-run `langsmith-migrator users`."
                        )
                        evidence = self._pending_invite_evidence(
                            email=email,
                            pending_member=pending_member,
                            desired_org_role_id=mapped_role_id,
                            desired_workspace_ids=workspace_ids,
                            desired_workspace_role_id=workspace_role_id,
                        )
                        self._mark_pending_org_blocked(
                            email=email,
                            item_id=item_id,
                            code="org_member_pending_invite_cancel_unsupported",
                            next_action=next_action,
                            evidence=evidence,
                        )
                        failed += 1
                        continue

                    try:
                        self._invite_org_member(
                            email,
                            mapped_role_id,
                            workspace_ids=workspace_ids,
                            workspace_role_id=workspace_role_id,
                        )
                        self.mark_migrated(
                            item_id,
                            outcome_code="org_member_migrated",
                        )
                        migrated += 1
                    except ConflictError as e:
                        self.log(
                            f"Replacement invite for '{email}' conflicted: {e}",
                            "error",
                        )
                        next_action = (
                            "Cancel or replace the pending org invite on the "
                            "target, then re-run `langsmith-migrator users`."
                        )
                        evidence = self._pending_invite_evidence(
                            email=email,
                            pending_member=pending_member,
                            desired_org_role_id=mapped_role_id,
                            desired_workspace_ids=workspace_ids,
                            desired_workspace_role_id=workspace_role_id,
                            error=e,
                        )
                        self._mark_pending_org_blocked(
                            email=email,
                            item_id=item_id,
                            code="org_member_pending_invite_replace_conflict",
                            next_action=next_action,
                            evidence=evidence,
                        )
                        failed += 1
                    except (AuthenticationError, APIError) as e:
                        self.log(
                            f"Failed to replace pending invite for '{email}': {e}",
                            "error",
                        )
                        next_action, evidence = self._org_member_failure_context(
                            email=email,
                            error=e,
                            operation="Replacing a pending organization invite",
                            fallback_next_action=(
                                "Cancel or replace the pending org invite on the "
                                "target, then re-run `langsmith-migrator users`."
                            ),
                        )
                        evidence.update(
                            self._pending_invite_evidence(
                                email=email,
                                pending_member=pending_member,
                                desired_org_role_id=mapped_role_id,
                                desired_workspace_ids=workspace_ids,
                                desired_workspace_role_id=workspace_role_id,
                                error=e,
                            )
                        )
                        self._mark_pending_org_blocked(
                            email=email,
                            item_id=item_id,
                            code="org_member_pending_invite_replace_failed",
                            next_action=next_action,
                            evidence=evidence,
                        )
                        failed += 1
                else:
                    pending_identity = {**pending_member, "email": email}
                    self._pending_org_email_to_identity[email] = pending_identity
                    if self._dest_email_to_identity is not None:
                        self._dest_email_to_identity[email] = pending_identity
                    if needs_workspace_access:
                        for workspace_id in workspace_ids:
                            self._pending_workspace_invites.add((email, workspace_id))
                        self.log(
                            f"Keeping pending invite for {email}: existing invite satisfies desired access",
                            "info",
                        )
                    else:
                        self.log(
                            f"Keeping pending invite for {email}: existing invite satisfies desired access",
                            "warning",
                        )
                    self.mark_migrated(
                        item_id,
                        outcome_code="org_member_skipped_existing",
                    )
                    skipped += 1
            else:
                try:
                    self._invite_org_member(
                        email,
                        mapped_role_id,
                        workspace_ids=workspace_ids,
                        workspace_role_id=workspace_role_id,
                    )
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
                    next_action, evidence = self._org_member_failure_context(
                        email=email,
                        error=e,
                        operation="Inviting an organization member",
                        fallback_next_action=(
                            "Review the org invite error in the remediation "
                            "bundle, then re-run `langsmith-migrator users`."
                        ),
                    )
                    self.mark_blocked(
                        item_id, "org_member_invite_failed",
                        next_action=next_action,
                        evidence=evidence,
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
                        tolerate_missing=True,
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
                    next_action, evidence = self._org_member_failure_context(
                        email=email,
                        error=e,
                        operation="Removing an organization member or pending invite",
                        fallback_next_action=(
                            "Remove the extra org user or pending invite manually "
                            "if needed, then re-run `langsmith-migrator users --sync`."
                        ),
                    )
                    self.mark_blocked(
                        item_id,
                        "org_member_remove_failed",
                        next_action=next_action,
                        evidence=evidence,
                    )
                    failed += 1

        self._last_org_member_removals = removed
        return migrated, skipped, failed

    def _invite_org_member(
        self,
        email: str,
        role_id: Optional[str] = None,
        *,
        workspace_ids: Optional[List[str]] = None,
        workspace_role_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Invite a new member to the destination org."""
        self._log_pending_invite_workspace_payload(
            email=email,
            workspace_ids=workspace_ids or [],
            workspace_role_id=workspace_role_id,
        )
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would invite {email}")
            response = {"id": f"dry-run-{email}", "email": email}
            self._pending_org_email_to_identity[email] = response
            if self._dest_email_to_identity is not None:
                self._dest_email_to_identity[email] = response
            if workspace_ids and workspace_role_id:
                for workspace_id in workspace_ids:
                    self._pending_workspace_invites.add((email, workspace_id))
            return response

        payload: Dict[str, Any] = {"email": email}
        if role_id:
            payload["role_id"] = role_id
        if workspace_ids:
            payload["workspace_ids"] = workspace_ids
        if workspace_role_id:
            payload["workspace_role_id"] = workspace_role_id

        response = self.dest.post("/orgs/current/members", payload)
        if isinstance(response, dict) and response.get("id"):
            invited_identity = {**response, "email": email}
            self._pending_org_email_to_identity[email] = invited_identity
            if self._dest_email_to_identity is not None:
                self._dest_email_to_identity[email] = invited_identity
        if workspace_ids and workspace_role_id:
            for workspace_id in workspace_ids:
                self._pending_workspace_invites.add((email, workspace_id))
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
        tolerate_missing: bool = False,
    ) -> bool:
        """Remove an active org member or cancel a pending invite."""
        if self.config.migration.dry_run:
            action = "cancel org invite" if pending else "remove org member"
            self.log(f"[DRY RUN] Would {action}: {identity_id}")
            return True

        if pending:
            pending_endpoint = f"/orgs/current/members/pending/{identity_id}"
            legacy_endpoint = f"/orgs/current/members/{identity_id}"
            try:
                self.dest.delete(pending_endpoint)
            except APIError as e:
                if is_org_member_management_permission_error(e):
                    raise
                status_code = getattr(e, "status_code", None)
                message = str(e).lower()
                if (
                    status_code not in {404, 405}
                    and "not found" not in message
                    and "not allowed" not in message
                ):
                    raise
                try:
                    self.dest.delete(legacy_endpoint)
                except APIError as legacy_error:
                    if is_org_member_management_permission_error(legacy_error):
                        raise
                    legacy_status_code = getattr(legacy_error, "status_code", None)
                    legacy_message = str(legacy_error).lower()
                    if is_member_absent_error(legacy_error):
                        if not tolerate_missing:
                            raise
                        self.log(
                            f"Pending org invite already absent: {identity_id}",
                            "warning",
                        )
                        return True
                    legacy_unsupported = (
                        legacy_status_code == 405 or "not allowed" in legacy_message
                    )
                    if not (tolerate_missing and legacy_unsupported):
                        raise
                    self.log(
                        "Pending org invite is not cancellable via known endpoints: "
                        f"{identity_id}",
                        "warning",
                    )
                    return False
        else:
            endpoint = f"/orgs/current/members/{identity_id}"
            try:
                self.dest.delete(endpoint)
            except APIError as e:
                if not (tolerate_missing and is_member_absent_error(e)):
                    raise
                self.log(
                    f"Org member already absent during source-of-truth cleanup: {identity_id}",
                    "warning",
                )
                return False
        action = "Cancelled org invite" if pending else "Removed org member"
        self.log(f"{action}: {identity_id}", "success")
        return True

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

        rows_by_email: Dict[str, List[Dict[str, Any]]] = {}
        for row in csv_rows:
            if row.get("workspace_id") != source_workspace_id:
                continue

            email = (row.get("email") or "").strip().lower()
            if not email:
                continue
            rows_by_email.setdefault(email, []).append(row)

        selected_by_email: Dict[str, Dict[str, Any]] = {}
        for email, rows in rows_by_email.items():
            try:
                role_id = select_effective_workspace_role_id(
                    rows,
                    email=email,
                    workspace_id=source_workspace_id,
                )
            except ValueError as e:
                raise APIError(
                    str(e),
                    request_info={},
                ) from e
            first_row = rows[0]
            selected_by_email[email] = {
                "id": first_row.get("id") or f"{source_workspace_id}:{email}",
                "email": email,
                "role_id": role_id,
                "full_name": first_row.get("full_name", ""),
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
        dst_ws = ws_pair.get("dest") or src_ws
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

            pending_org_blocker = self._pending_org_blockers.get(email)
            if pending_org_blocker:
                self.log(
                    "Skipping workspace membership for "
                    f"'{email}' because org pending invite reconciliation is blocked",
                    "warning",
                )
                self.mark_migrated(
                    item_id,
                    outcome_code="ws_member_skipped_pending_org_blocker",
                    evidence={
                        "email": email,
                        "org_item_id": pending_org_blocker.get("item_id"),
                        "org_blocker": pending_org_blocker.get("code"),
                    },
                )
                skipped += 1
                continue

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
            if dst_ws and (email, dst_ws) in self._pending_workspace_invites:
                self.log(
                    f"Workspace access for '{email}' is already included in the pending org invite",
                    "info",
                )
                self.mark_migrated(
                    item_id,
                    outcome_code="ws_member_included_in_org_invite",
                )
                skipped += 1
                continue

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
                            next_action=(
                                "Review the workspace role update error in the "
                                "remediation bundle, then re-run "
                                "`langsmith-migrator users`."
                            ),
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
                pending_org_member = self._pending_org_email_to_identity.get(email)
                pending_user_id = (
                    pending_org_member
                    and (
                        pending_org_member.get("user_id")
                        or (pending_org_member.get("user") or {}).get("id")
                    )
                )
                if pending_org_member and not pending_user_id:
                    if email in self._pending_org_invite_wait_reported:
                        self.log(
                            f"Workspace membership for '{email}' is already waiting on pending org invite acceptance",
                            "info",
                        )
                        self.mark_migrated(
                            item_id,
                            outcome_code="ws_member_skipped_pending_org_invite_waiting",
                            evidence={"email": email},
                        )
                        skipped += 1
                        continue
                    self._pending_org_invite_wait_reported.add(email)
                    self.log(
                        f"Cannot add '{email}' to workspace until the pending org invite is accepted",
                        "warning",
                    )
                    self.mark_blocked(
                        item_id,
                        "ws_member_pending_org_invite",
                        next_action=(
                            "Wait for the pending org invite to be accepted, "
                            "then re-run `langsmith-migrator users`."
                        ),
                        evidence={"email": email},
                    )
                    failed += 1
                    continue

                try:
                    self._add_workspace_member(
                        dest_org_member,
                        mapped_role_id,
                        workspace_id=dst_ws,
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
                        next_action=(
                            "Review the workspace membership create error in the "
                            "remediation bundle, then re-run "
                            "`langsmith-migrator users`."
                        ),
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
                        next_action=(
                            "Remove the extra workspace membership manually if "
                            "needed, then re-run `langsmith-migrator users --sync`."
                        ),
                        evidence={"email": email, "error": str(e)},
                    )
                    failed += 1

        self._last_workspace_member_removals = removed
        return migrated, skipped, failed

    def _add_workspace_member(
        self,
        org_member: Dict[str, Any],
        role_id: Optional[str] = None,
        *,
        workspace_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Add an org member to the current workspace."""
        org_identity_id = org_member.get("id", "")
        user_id = (
            org_member.get("user_id")
            or (org_member.get("user") or {}).get("id")
        )

        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would add workspace member: {org_identity_id}")
            return {"id": f"dry-run-{org_identity_id}"}

        if user_id and workspace_id:
            payload: Dict[str, Any] = {
                "user_id": user_id,
                "workspace_ids": [workspace_id],
            }
            if role_id:
                payload["workspace_role_id"] = role_id
            try:
                response = self.dest.post("/workspaces/current/members", payload)
                self.log(f"Added workspace member: {org_identity_id}", "success")
                return response
            except APIError as e:
                status_code = getattr(e, "status_code", None)
                message = str(e).lower()
                if (
                    status_code not in {404, 405, 422}
                    and "not found" not in message
                    and "not allowed" not in message
                    and "user_id" not in message
                    and "validation" not in message
                ):
                    raise

        payload = {"org_identity_id": org_identity_id}
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

        try:
            self.dest.patch(
                f"/workspaces/current/members/{identity_id}", {"role_id": role_id}
            )
        except APIError as e:
            status_code = getattr(e, "status_code", None)
            message = str(e).lower()
            if (
                status_code not in {404, 405}
                and "not found" not in message
                and "not allowed" not in message
            ):
                raise
            self.dest.patch(
                f"/tenants/current/members/{identity_id}", {"role_id": role_id}
            )
        self.log(f"Updated workspace member role: {identity_id}", "success")

    def _remove_workspace_member(self, identity_id: str) -> None:
        """Remove a workspace member from the current workspace."""
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would remove workspace member: {identity_id}")
            return

        try:
            self.dest.delete(f"/workspaces/current/members/{identity_id}")
        except APIError as e:
            status_code = getattr(e, "status_code", None)
            message = str(e).lower()
            if (
                status_code not in {404, 405}
                and "not found" not in message
                and "not allowed" not in message
            ):
                raise
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
