"""Unit tests for UserRoleMigrator."""

import pytest
from unittest.mock import Mock, call
from langsmith_migrator.core.migrators import UserRoleMigrator
from langsmith_migrator.core.migrators.user_role import make_workspace_role_union_id
from langsmith_migrator.utils.retry import APIError, AuthenticationError, ConflictError


class TestUserRoleMigrator:
    """Test cases for UserRoleMigrator."""

    @pytest.fixture
    def migrator(self, sample_config, migration_state):
        """Create a UserRoleMigrator with separate source/dest clients."""
        from langsmith_migrator.core.api_client import EnhancedAPIClient

        source = Mock(spec=EnhancedAPIClient)
        source.base_url = "https://source.api.test.com"
        source.session = Mock()
        source.session.headers = {"X-Tenant-Id": "ws-default-src"}

        dest = Mock(spec=EnhancedAPIClient)
        dest.base_url = "https://dest.api.test.com"
        dest.session = Mock()
        dest.session.headers = {"X-Tenant-Id": "ws-default-dst"}

        return UserRoleMigrator(source, dest, migration_state, sample_config)

    @pytest.fixture
    def source_roles(self):
        return [
            {"id": "src-admin", "name": "ORGANIZATION_ADMIN", "display_name": "Admin", "permissions": []},
            {"id": "src-user", "name": "ORGANIZATION_USER", "display_name": "User", "permissions": []},
            {"id": "src-ws-admin", "name": "WORKSPACE_ADMIN", "display_name": "Workspace Admin", "permissions": []},
            {"id": "src-custom-1", "name": "CUSTOM", "display_name": "Data Scientist",
             "description": "Custom DS role", "permissions": ["datasets:read", "datasets:create", "runs:read"]},
        ]

    @pytest.fixture
    def dest_roles(self):
        return [
            {"id": "dst-admin", "name": "ORGANIZATION_ADMIN", "display_name": "Admin", "permissions": []},
            {"id": "dst-user", "name": "ORGANIZATION_USER", "display_name": "User", "permissions": []},
            {"id": "dst-ws-admin", "name": "WORKSPACE_ADMIN", "display_name": "Workspace Admin", "permissions": []},
        ]

    # ── Phase 1: Role mapping ──

    def test_match_builtin_roles(self, migrator, source_roles, dest_roles):
        """Built-in roles are matched by name."""
        mapping = migrator._match_builtin_roles(source_roles, dest_roles)

        assert mapping["src-admin"] == "dst-admin"
        assert mapping["src-user"] == "dst-user"
        assert mapping["src-ws-admin"] == "dst-ws-admin"
        assert "src-custom-1" not in mapping

    def test_sync_custom_roles_create(self, migrator, source_roles, dest_roles):
        """Custom roles missing on destination are created."""
        migrator.dest.post.return_value = {"id": "dst-custom-1"}

        mapping = migrator._sync_custom_roles(source_roles, dest_roles)

        assert mapping["src-custom-1"] == "dst-custom-1"
        migrator.dest.post.assert_called_once_with(
            "/orgs/current/roles",
            {
                "display_name": "Data Scientist",
                "description": "Custom DS role",
                "permissions": ["datasets:read", "datasets:create", "runs:read"],
            },
        )

    def test_sync_custom_roles_skip_existing(self, migrator, source_roles, sample_config):
        """When skip_existing, existing custom roles are mapped but not updated."""
        sample_config.migration.skip_existing = True

        dest_roles_with_custom = [
            {"id": "dst-custom-1", "name": "CUSTOM", "display_name": "Data Scientist",
             "permissions": ["datasets:read"]},  # Fewer permissions
        ]

        mapping = migrator._sync_custom_roles(source_roles, dest_roles_with_custom)

        assert mapping["src-custom-1"] == "dst-custom-1"
        migrator.dest.patch.assert_not_called()
        migrator.dest.post.assert_not_called()

    def test_sync_custom_roles_update_permissions(self, migrator, source_roles, sample_config):
        """Existing custom roles with different permissions are updated."""
        sample_config.migration.skip_existing = False

        dest_roles_with_custom = [
            {"id": "dst-custom-1", "name": "CUSTOM", "display_name": "Data Scientist",
             "description": "Old desc", "permissions": ["datasets:read"]},
        ]

        mapping = migrator._sync_custom_roles(source_roles, dest_roles_with_custom)

        assert mapping["src-custom-1"] == "dst-custom-1"
        migrator.dest.patch.assert_called_once_with(
            "/orgs/current/roles/dst-custom-1",
            {
                "display_name": "Data Scientist",
                "description": "Custom DS role",
                "permissions": ["datasets:read", "datasets:create", "runs:read"],
            },
        )

    def test_sync_custom_roles_no_update_when_permissions_match(self, migrator, source_roles, sample_config):
        """No PATCH when permissions already match."""
        sample_config.migration.skip_existing = False

        dest_roles_with_custom = [
            {"id": "dst-custom-1", "name": "CUSTOM", "display_name": "Data Scientist",
             "description": "Custom DS role",
             "permissions": ["datasets:read", "datasets:create", "runs:read"]},
        ]

        migrator._sync_custom_roles(source_roles, dest_roles_with_custom)

        migrator.dest.patch.assert_not_called()

    def test_build_role_mapping_persists_to_state(self, migrator, source_roles, dest_roles, migration_state):
        """Role mapping is persisted in state.id_mappings['roles']."""
        migrator.source.get.return_value = source_roles
        migrator.dest.get.return_value = dest_roles
        migrator.dest.post.return_value = {"id": "dst-custom-1"}

        migrator.build_role_mapping()

        assert migration_state.id_mappings.get("roles", {}).get("src-admin") == "dst-admin"
        assert migration_state.id_mappings.get("roles", {}).get("src-custom-1") == "dst-custom-1"

    def test_build_role_mapping_can_skip_custom_roles(self, migrator, source_roles, dest_roles):
        """Selective sync can leave custom roles untouched until they are needed."""
        migrator.source.get.return_value = source_roles
        migrator.dest.get.return_value = dest_roles

        mapping = migrator.build_role_mapping(custom_role_ids=set())

        assert mapping["src-admin"] == "dst-admin"
        assert "src-custom-1" not in mapping
        migrator.dest.post.assert_not_called()

    def test_build_role_mapping_accumulates_requested_custom_roles(self, migrator, dest_roles):
        """Repeated selective sync calls preserve custom roles already mapped earlier."""
        source_roles = [
            {"id": "src-admin", "name": "ORGANIZATION_ADMIN", "display_name": "Admin", "permissions": []},
            {
                "id": "src-custom-1",
                "name": "CUSTOM",
                "display_name": "Data Scientist",
                "description": "Custom DS role",
                "permissions": ["datasets:read"],
            },
            {
                "id": "src-custom-2",
                "name": "CUSTOM",
                "display_name": "Reviewer",
                "description": "Custom reviewer role",
                "permissions": ["runs:read"],
            },
        ]
        migrator.source.get.return_value = source_roles
        migrator.dest.get.return_value = dest_roles
        migrator.dest.post.side_effect = [
            {"id": "dst-custom-1"},
            {"id": "dst-custom-2"},
        ]

        first = migrator.build_role_mapping(custom_role_ids={"src-custom-1"})
        second = migrator.build_role_mapping(custom_role_ids={"src-custom-2"})

        assert first["src-custom-1"] == "dst-custom-1"
        assert "src-custom-2" not in first
        assert second["src-custom-1"] == "dst-custom-1"
        assert second["src-custom-2"] == "dst-custom-2"

    def test_materialize_workspace_role_union_creates_role_and_clones_access_policy(
        self,
        migrator,
        migration_state,
    ):
        """Union roles combine permissions and carry over ABAC policy attachments."""
        migrator.log = Mock()
        union_role_id = make_workspace_role_union_id({"src-custom-1", "src-custom-2"})
        source_roles = [
            {
                "id": "src-custom-1",
                "name": "CUSTOM",
                "display_name": "Assistant A",
                "description": "Assistant A role",
                "permissions": ["projects:read"],
            },
            {
                "id": "src-custom-2",
                "name": "CUSTOM",
                "display_name": "Assistant B",
                "description": "Assistant B role",
                "permissions": ["datasets:read", "projects:read"],
            },
        ]
        source_policy = {
            "id": "src-policy-1",
            "name": "Assistant project access",
            "description": "Restrict projects by assistant",
            "effect": "allow",
            "condition_groups": [
                {
                    "permission": "projects:read",
                    "resource_type": "project",
                    "conditions": [
                        {
                            "attribute_name": "resource_tag_key",
                            "attribute_key": "Assistant",
                            "operator": "equals",
                            "attribute_value": "A",
                        }
                    ],
                }
            ],
            "role_ids": ["src-custom-1"],
        }

        def source_get(endpoint, params=None):
            if endpoint == "/orgs/current/roles":
                return source_roles
            if endpoint.endswith("/v1/platform/orgs/current/access-policies"):
                return {"access_policies": [source_policy]}
            raise AssertionError(f"unexpected source GET {endpoint}")

        def dest_get(endpoint, params=None):
            if endpoint == "/orgs/current/roles":
                return []
            if endpoint.endswith("/v1/platform/orgs/current/access-policies"):
                return {"access_policies": []}
            raise AssertionError(f"unexpected dest GET {endpoint}")

        def dest_post(endpoint, data):
            if endpoint == "/orgs/current/roles":
                return {"id": "dst-union-role"}
            if endpoint.endswith("/v1/platform/orgs/current/access-policies"):
                return {"id": "dst-policy-1"}
            raise AssertionError(f"unexpected dest POST {endpoint}")

        migrator.source.get.side_effect = source_get
        migrator.dest.get.side_effect = dest_get
        migrator.dest.post.side_effect = dest_post

        mapping = migrator.materialize_workspace_role_unions({union_role_id})

        assert mapping[union_role_id] == "dst-union-role"
        assert migration_state.id_mappings["roles"][union_role_id] == "dst-union-role"
        assert any(
            "Created managed workspace role union" in call_args.args[0]
            for call_args in migrator.log.call_args_list
        )
        assert any(
            "Resolved workspace role union" in call_args.args[0]
            for call_args in migrator.log.call_args_list
        )
        migrator.dest.post.assert_any_call(
            "/orgs/current/roles",
            {
                "display_name": migrator._build_workspace_union_role(
                    union_role_id,
                    source_roles,
                )["display_name"],
                "description": (
                    "Managed by langsmith-migrator. Union of source workspace roles: "
                    "Assistant A (src-custom-1); Assistant B (src-custom-2)"
                ),
                "permissions": ["datasets:read", "projects:read"],
            },
        )
        migrator.dest.post.assert_any_call(
            "https://dest.api.test.com/v1/platform/orgs/current/access-policies",
            {
                "name": "Assistant project access",
                "description": "Restrict projects by assistant",
                "effect": "allow",
                "condition_groups": source_policy["condition_groups"],
                "role_ids": ["dst-union-role"],
            },
        )

    def test_materialize_workspace_role_union_reuses_policy_and_attaches_role(
        self,
        migrator,
    ):
        """Equivalent destination ABAC policies are attached instead of duplicated."""
        migrator.log = Mock()
        union_role_id = make_workspace_role_union_id({"src-custom-1", "src-custom-2"})
        source_roles = [
            {
                "id": "src-custom-1",
                "name": "CUSTOM",
                "display_name": "Assistant A",
                "description": "Assistant A role",
                "permissions": ["projects:read"],
            },
            {
                "id": "src-custom-2",
                "name": "CUSTOM",
                "display_name": "Assistant B",
                "description": "Assistant B role",
                "permissions": ["datasets:read"],
            },
        ]
        union_role = migrator._build_workspace_union_role(union_role_id, source_roles)
        source_policy = {
            "id": "src-policy-1",
            "name": "Assistant project access",
            "description": "Restrict projects by assistant",
            "effect": "allow",
            "condition_groups": [{"permission": "projects:read", "resource_type": "project", "conditions": []}],
            "role_ids": ["src-custom-1"],
        }
        dest_policy = {
            **source_policy,
            "id": "dst-policy-1",
            "role_ids": [],
        }

        migrator.source.get.side_effect = lambda endpoint, params=None: (
            source_roles
            if endpoint == "/orgs/current/roles"
            else {"access_policies": [source_policy]}
        )
        migrator.dest.get.side_effect = lambda endpoint, params=None: (
            [{**union_role, "id": "dst-union-role"}]
            if endpoint == "/orgs/current/roles"
            else {"access_policies": [dest_policy]}
        )

        mapping = migrator.materialize_workspace_role_unions({union_role_id})

        assert mapping[union_role_id] == "dst-union-role"
        assert any(
            "Reused managed workspace role union" in call_args.args[0]
            for call_args in migrator.log.call_args_list
        )
        assert any(
            "Resolved workspace role union" in call_args.args[0]
            for call_args in migrator.log.call_args_list
        )
        migrator.dest.patch.assert_not_called()
        migrator.dest.post.assert_called_once_with(
            "https://dest.api.test.com/v1/platform/orgs/current/access-policies/roles/"
            "dst-union-role/access-policies",
            {"access_policy_ids": ["dst-policy-1"]},
        )

    def test_materialize_workspace_role_union_fails_closed_when_policy_clone_fails(
        self,
        migrator,
        migration_state,
    ):
        """A union role is not mapped when an ABAC policy clone cannot be created."""
        union_role_id = make_workspace_role_union_id({"src-custom-1", "src-custom-2"})
        source_roles = [
            {
                "id": "src-custom-1",
                "name": "CUSTOM",
                "display_name": "Assistant A",
                "description": "Assistant A role",
                "permissions": ["projects:read"],
            },
            {
                "id": "src-custom-2",
                "name": "CUSTOM",
                "display_name": "Assistant B",
                "description": "Assistant B role",
                "permissions": ["datasets:read"],
            },
        ]
        source_policy = {
            "id": "src-policy-1",
            "name": "Assistant project access",
            "description": "Restrict projects by assistant",
            "effect": "allow",
            "condition_groups": [{"permission": "projects:read", "resource_type": "project", "conditions": []}],
            "role_ids": ["src-custom-1"],
        }

        migrator.source.get.side_effect = lambda endpoint, params=None: (
            source_roles
            if endpoint == "/orgs/current/roles"
            else {"access_policies": [source_policy]}
        )
        migrator.dest.get.side_effect = lambda endpoint, params=None: (
            []
            if endpoint == "/orgs/current/roles"
            else {"access_policies": []}
        )
        migrator.dest.post.side_effect = [
            {"id": "dst-union-role"},
            {},
        ]

        with pytest.raises(APIError, match="Access policy create response did not include an id"):
            migrator.materialize_workspace_role_unions({union_role_id})

        assert union_role_id not in migrator._role_id_map
        assert union_role_id not in migration_state.id_mappings.get("roles", {})

    def test_materialize_workspace_role_union_fails_closed_when_policy_attach_fails(
        self,
        migrator,
        migration_state,
    ):
        """A union role is not mapped when an existing ABAC policy cannot be attached."""
        union_role_id = make_workspace_role_union_id({"src-custom-1", "src-custom-2"})
        source_roles = [
            {
                "id": "src-custom-1",
                "name": "CUSTOM",
                "display_name": "Assistant A",
                "description": "Assistant A role",
                "permissions": ["projects:read"],
            },
            {
                "id": "src-custom-2",
                "name": "CUSTOM",
                "display_name": "Assistant B",
                "description": "Assistant B role",
                "permissions": ["datasets:read"],
            },
        ]
        union_role = migrator._build_workspace_union_role(union_role_id, source_roles)
        source_policy = {
            "id": "src-policy-1",
            "name": "Assistant project access",
            "description": "Restrict projects by assistant",
            "effect": "allow",
            "condition_groups": [{"permission": "projects:read", "resource_type": "project", "conditions": []}],
            "role_ids": ["src-custom-1"],
        }
        dest_policy = {
            **source_policy,
            "id": "dst-policy-1",
            "role_ids": [],
        }

        migrator.source.get.side_effect = lambda endpoint, params=None: (
            source_roles
            if endpoint == "/orgs/current/roles"
            else {"access_policies": [source_policy]}
        )
        migrator.dest.get.side_effect = lambda endpoint, params=None: (
            [{**union_role, "id": "dst-union-role"}]
            if endpoint == "/orgs/current/roles"
            else {"access_policies": [dest_policy]}
        )
        migrator.dest.post.side_effect = APIError(
            "attach failed",
            request_info={},
        )

        with pytest.raises(APIError, match="attach failed"):
            migrator.materialize_workspace_role_unions({union_role_id})

        assert union_role_id not in migrator._role_id_map
        assert union_role_id not in migration_state.id_mappings.get("roles", {})

    def test_materialize_workspace_role_union_skip_existing_leaves_role_and_policies(
        self,
        migrator,
        sample_config,
    ):
        """skip_existing maps existing managed unions without mutating them."""
        sample_config.migration.skip_existing = True
        union_role_id = make_workspace_role_union_id({"src-custom-1", "src-custom-2"})
        source_roles = [
            {"id": "src-custom-1", "name": "CUSTOM", "display_name": "A", "permissions": ["projects:read"]},
            {"id": "src-custom-2", "name": "CUSTOM", "display_name": "B", "permissions": ["datasets:read"]},
        ]
        union_role = migrator._build_workspace_union_role(union_role_id, source_roles)

        migrator.source.get.side_effect = lambda endpoint, params=None: (
            source_roles
            if endpoint == "/orgs/current/roles"
            else {"access_policies": [{"id": "src-policy", "role_ids": ["src-custom-1"]}]}
        )
        migrator.dest.get.side_effect = lambda endpoint, params=None: (
            [{**union_role, "id": "dst-union-role", "permissions": []}]
            if endpoint == "/orgs/current/roles"
            else {"access_policies": []}
        )

        mapping = migrator.materialize_workspace_role_unions({union_role_id})

        assert mapping[union_role_id] == "dst-union-role"
        migrator.dest.patch.assert_not_called()
        migrator.dest.post.assert_not_called()

    def test_materialize_workspace_role_union_rejects_builtin_without_permissions(
        self,
        migrator,
    ):
        """Mixed built-in/custom unions fail closed when built-in permissions are absent."""
        union_role_id = make_workspace_role_union_id({"src-custom-1", "src-ws-user"})
        source_roles = [
            {
                "id": "src-custom-1",
                "name": "CUSTOM",
                "display_name": "Assistant A",
                "permissions": ["projects:read"],
            },
            {
                "id": "src-ws-user",
                "name": "WORKSPACE_USER",
                "display_name": "Workspace User",
                "permissions": [],
            },
        ]
        migrator.source.get.side_effect = lambda endpoint, params=None: (
            source_roles
            if endpoint == "/orgs/current/roles"
            else {"access_policies": []}
        )
        migrator.dest.get.side_effect = lambda endpoint, params=None: (
            []
            if endpoint == "/orgs/current/roles"
            else {"access_policies": []}
        )

        with pytest.raises(APIError, match="built-in role permissions were not exposed"):
            migrator.materialize_workspace_role_unions({union_role_id})

        migrator.dest.post.assert_not_called()

    def test_ensure_dest_email_index_caches_until_forced(self, migrator):
        """Destination org identities are cached unless a refresh is requested."""
        migrator.dest.get_paginated.side_effect = [
            iter([{"id": "dst-1", "email": "alice@example.com"}]),
            iter([{"id": "dst-2", "email": "bob@example.com"}]),
        ]

        first = migrator.ensure_dest_email_index()
        second = migrator.ensure_dest_email_index()
        refreshed = migrator.ensure_dest_email_index(force=True)

        assert first is second
        assert first["alice@example.com"]["id"] == "dst-1"
        assert refreshed["bob@example.com"]["id"] == "dst-2"
        assert migrator.dest.get_paginated.call_count == 2

    # ── Phase 2: Org member migration ──

    def test_migrate_org_members_invite_new(self, migrator):
        """New users are invited to the destination org."""
        migrator._role_id_map = {"src-role": "dst-role"}
        migrator.dest.get_paginated.return_value = iter([])  # No dest members
        migrator.dest.post.return_value = {"id": "new-identity-1"}

        members = [
            {"id": "src-m1", "email": "alice@example.com", "role_id": "src-role", "full_name": "Alice"},
        ]

        m, s, f = migrator.migrate_org_members(members)

        assert m == 1
        assert s == 0
        assert f == 0
        migrator.dest.post.assert_called_once_with(
            "/orgs/current/members",
            {"email": "alice@example.com", "role_id": "dst-role"},
        )

    def test_migrate_org_members_invite_new_with_workspace_access(self, migrator):
        """Single-instance workspace-only users can be invited to the org and workspace together."""
        migrator._role_id_map = {
            "src-org-role": "dst-org-role",
            "src-ws-role": "dst-ws-role",
        }
        migrator.dest.get_paginated.side_effect = [iter([]), iter([])]
        migrator.dest.post.return_value = {"id": "new-identity-1"}

        members = [
            {
                "id": "src-m1",
                "email": "alice@example.com",
                "role_id": "src-org-role",
                "workspace_ids": ["ws-1"],
                "workspace_role_id": "src-ws-role",
            },
        ]

        m, s, f = migrator.migrate_org_members(members)

        assert (m, s, f) == (1, 0, 0)
        migrator.dest.post.assert_called_once_with(
            "/orgs/current/members",
            {
                "email": "alice@example.com",
                "role_id": "dst-org-role",
                "workspace_ids": ["ws-1"],
                "workspace_role_id": "dst-ws-role",
            },
        )
        assert migrator._pending_workspace_invites == {("alice@example.com", "ws-1")}
        assert migrator._pending_org_email_to_identity["alice@example.com"]["id"] == "new-identity-1"

    def test_ensure_dest_email_index_preserves_newly_invited_pending_identities(self, migrator):
        """A refresh keeps newly invited identities available for the workspace phase."""
        migrator._pending_org_email_to_identity = {
            "alice@example.com": {"id": "pending-1", "email": "alice@example.com"}
        }
        migrator.dest.get_paginated.return_value = iter([])

        refreshed = migrator.ensure_dest_email_index(force=True)

        assert refreshed["alice@example.com"]["id"] == "pending-1"

    def test_migrate_org_members_update_role(self, migrator):
        """Existing members with different roles get updated."""
        migrator._role_id_map = {"src-role": "dst-role-new"}
        migrator.dest.get_paginated.return_value = iter([
            {"id": "dst-m1", "email": "alice@example.com", "role_id": "dst-role-old"},
        ])

        members = [
            {"id": "src-m1", "email": "alice@example.com", "role_id": "src-role"},
        ]

        m, s, f = migrator.migrate_org_members(members)

        assert m == 1
        migrator.dest.patch.assert_called_once_with(
            "/orgs/current/members/dst-m1",
            {"role_id": "dst-role-new"},
        )

    def test_migrate_org_members_skip_existing(self, migrator, sample_config):
        """With skip_existing, existing members are skipped."""
        sample_config.migration.skip_existing = True
        migrator._role_id_map = {"src-role": "dst-role"}
        migrator.dest.get_paginated.return_value = iter([
            {"id": "dst-m1", "email": "alice@example.com", "role_id": "dst-role-old"},
        ])

        members = [
            {"id": "src-m1", "email": "alice@example.com", "role_id": "src-role"},
        ]

        m, s, f = migrator.migrate_org_members(members)

        assert s == 1
        assert m == 0
        migrator.dest.patch.assert_not_called()

    def test_migrate_org_members_conflict_treated_as_skip(self, migrator):
        """409 Conflict on invite is treated as skip."""
        migrator._role_id_map = {"src-role": "dst-role"}
        migrator.dest.get_paginated.return_value = iter([])
        migrator.dest.post.side_effect = ConflictError(
            "Already pending", request_info={}
        )

        members = [
            {"id": "src-m1", "email": "alice@example.com", "role_id": "src-role"},
        ]

        m, s, f = migrator.migrate_org_members(members)

        assert s == 1
        assert m == 0
        assert f == 0

    def test_migrate_org_members_unmapped_role(self, migrator):
        """Members with unmapped role_id are counted as failed."""
        migrator._role_id_map = {}  # No mappings
        migrator.dest.get_paginated.return_value = iter([])

        members = [
            {"id": "src-m1", "email": "alice@example.com", "role_id": "unknown-role"},
        ]

        m, s, f = migrator.migrate_org_members(members)

        assert f == 1
        assert m == 0

    def test_migrate_org_members_reinvites_pending_when_role_differs(self, migrator):
        """Pending invites with the wrong role are replaced in authoritative mode."""
        migrator.log = Mock()
        migrator._role_id_map = {"src-role": "dst-role-new"}
        migrator.dest.get_paginated.side_effect = [
            iter([]),
            iter([
                {"id": "pending-1", "email": "alice@example.com", "role_id": "dst-role-old"},
            ]),
        ]
        migrator.dest.post.return_value = {"id": "new-identity-1"}

        members = [
            {"id": "src-m1", "email": "alice@example.com", "role_id": "src-role"},
        ]

        m, s, f = migrator.migrate_org_members(
            members,
            remove_pending=True,
        )

        assert (m, s, f) == (1, 0, 0)
        assert any(
            "Replacing pending invite for alice@example.com" in call_args.args[0]
            for call_args in migrator.log.call_args_list
        )
        migrator.dest.delete.assert_called_once_with(
            "/orgs/current/members/pending/pending-1"
        )
        migrator.dest.post.assert_called_once_with(
            "/orgs/current/members",
            {"email": "alice@example.com", "role_id": "dst-role-new"},
        )

    def test_migrate_org_members_reinvites_pending_falls_back_to_legacy_delete_path(
        self, migrator
    ):
        """Pending invite replacement should tolerate instances that only support the legacy delete path."""
        migrator._role_id_map = {"src-role": "dst-role-new"}
        migrator.dest.get_paginated.side_effect = [
            iter([]),
            iter([
                {"id": "pending-1", "email": "alice@example.com", "role_id": "dst-role-old"},
            ]),
        ]
        migrator.dest.delete.side_effect = [
            APIError("Not found", status_code=404, request_info={}),
            {},
        ]
        migrator.dest.post.return_value = {"id": "new-identity-1"}

        members = [
            {"id": "src-m1", "email": "alice@example.com", "role_id": "src-role"},
        ]

        m, s, f = migrator.migrate_org_members(
            members,
            remove_pending=True,
        )

        assert (m, s, f) == (1, 0, 0)
        assert migrator.dest.delete.call_args_list == [
            call("/orgs/current/members/pending/pending-1"),
            call("/orgs/current/members/pending-1"),
        ]
        migrator.dest.post.assert_called_once_with(
            "/orgs/current/members",
            {"email": "alice@example.com", "role_id": "dst-role-new"},
        )

    def test_migrate_org_members_reinvites_pending_when_delete_paths_return_not_found(
        self, migrator
    ):
        """Pending invite refresh should still re-invite when cancel endpoints report not found."""
        migrator._role_id_map = {"src-role": "dst-role-new"}
        migrator.dest.get_paginated.side_effect = [
            iter([]),
            iter([
                {"id": "pending-1", "email": "alice@example.com", "role_id": "dst-role-old"},
            ]),
        ]
        migrator.dest.delete.side_effect = [
            APIError("Not found", status_code=404, request_info={}),
            APIError("User not found", status_code=404, request_info={}),
        ]
        migrator.dest.post.return_value = {"id": "new-identity-1"}

        members = [
            {"id": "src-m1", "email": "alice@example.com", "role_id": "src-role"},
        ]

        m, s, f = migrator.migrate_org_members(
            members,
            remove_pending=True,
        )

        assert (m, s, f) == (1, 0, 0)
        assert migrator.dest.delete.call_args_list == [
            call("/orgs/current/members/pending/pending-1"),
            call("/orgs/current/members/pending-1"),
        ]
        migrator.dest.post.assert_called_once_with(
            "/orgs/current/members",
            {"email": "alice@example.com", "role_id": "dst-role-new"},
        )

    def test_migrate_org_members_blocks_when_pending_cancel_is_unsupported(
        self, migrator, migration_state
    ):
        """Unsupported pending invite cancel APIs block before replacement invite creation."""
        migrator.state = migration_state
        migrator._role_id_map = {"src-role": "dst-role-new"}
        migrator.dest.get_paginated.side_effect = [
            iter([]),
            iter([
                {"id": "pending-1", "email": "alice@example.com", "role_id": "dst-role-old"},
            ]),
        ]
        migrator.dest.delete.side_effect = [
            APIError("Method not allowed", status_code=405, request_info={}),
            APIError("Method not allowed", status_code=405, request_info={}),
        ]

        members = [
            {"id": "src-m1", "email": "alice@example.com", "role_id": "src-role"},
        ]

        m, s, f = migrator.migrate_org_members(
            members,
            remove_pending=True,
        )

        assert (m, s, f) == (0, 0, 1)
        item = migration_state.get_item("org_member_alice@example.com")
        assert item is not None
        assert item.outcome_code == "org_member_pending_invite_cancel_unsupported"
        assert item.evidence["existing_pending_invite_id"] == "pending-1"
        assert item.evidence["desired_org_role_id"] == "dst-role-new"
        assert migrator._pending_org_blockers["alice@example.com"]["item_id"] == item.id
        migrator.dest.post.assert_not_called()

    def test_migrate_org_members_reinvites_pending_when_workspace_access_is_needed(
        self, migrator
    ):
        """Existing pending invites should be replaced when workspace access still needs to be attached."""
        migrator.log = Mock()
        migrator._role_id_map = {
            "src-role": "dst-role",
            "src-ws-role": "dst-ws-role",
        }
        migrator.dest.get_paginated.side_effect = [
            iter([]),
            iter([
                {"id": "pending-1", "email": "alice@example.com", "role_id": "dst-role"},
            ]),
        ]
        migrator.dest.post.return_value = {"id": "new-identity-1"}

        members = [
            {
                "id": "src-m1",
                "email": "alice@example.com",
                "role_id": "src-role",
                "workspace_ids": ["ws-1"],
                "workspace_role_id": "src-ws-role",
            },
        ]

        m, s, f = migrator.migrate_org_members(members)

        assert (m, s, f) == (1, 0, 0)
        assert any(
            "Pending invite for alice@example.com will include workspace access" in call_args.args[0]
            for call_args in migrator.log.call_args_list
        )
        migrator.dest.delete.assert_called_once_with(
            "/orgs/current/members/pending/pending-1"
        )
        migrator.dest.post.assert_called_once_with(
            "/orgs/current/members",
            {
                "email": "alice@example.com",
                "role_id": "dst-role",
                "workspace_ids": ["ws-1"],
                "workspace_role_id": "dst-ws-role",
            },
        )
        assert migrator._pending_workspace_invites == {("alice@example.com", "ws-1")}
        assert migrator._pending_org_email_to_identity["alice@example.com"]["id"] == "new-identity-1"

    def test_migrate_org_members_keeps_pending_invite_when_role_and_workspace_access_match(
        self, migrator
    ):
        """Existing pending invites are reused when they already include the requested workspace access."""
        migrator.log = Mock()
        migrator._role_id_map = {
            "src-role": "dst-role",
            "src-ws-role": "dst-ws-role",
        }
        migrator.dest.get_paginated.side_effect = [
            iter([]),
            iter([
                {
                    "id": "pending-1",
                    "email": "alice@example.com",
                    "role_id": "dst-role",
                    "workspace_ids": ["ws-1"],
                    "workspace_role_id": "dst-ws-role",
                },
            ]),
        ]

        members = [
            {
                "id": "src-m1",
                "email": "alice@example.com",
                "role_id": "src-role",
                "workspace_ids": ["ws-1"],
                "workspace_role_id": "src-ws-role",
            },
        ]

        m, s, f = migrator.migrate_org_members(members)

        assert (m, s, f) == (0, 1, 0)
        assert any(
            "Keeping pending invite for alice@example.com" in call_args.args[0]
            for call_args in migrator.log.call_args_list
        )
        assert migrator._pending_org_email_to_identity["alice@example.com"]["id"] == "pending-1"
        assert migrator._pending_workspace_invites == {("alice@example.com", "ws-1")}
        migrator.dest.delete.assert_not_called()
        migrator.dest.post.assert_not_called()

    def test_migrate_org_members_source_of_truth_replaces_pending_invite_with_extra_workspace_access(
        self, migrator
    ):
        """Authoritative sync should remove extra workspace grants embedded in pending invites."""
        migrator._role_id_map = {
            "src-role": "dst-role",
            "src-ws-role": "dst-ws-role",
        }
        migrator.dest.get_paginated.side_effect = [
            iter([]),
            iter([
                {
                    "id": "pending-1",
                    "email": "alice@example.com",
                    "role_id": "dst-role",
                    "workspace_ids": ["ws-1", "ws-2"],
                    "workspace_role_id": "dst-ws-role",
                },
            ]),
        ]
        migrator.dest.post.return_value = {"id": "new-identity-1"}

        members = [
            {
                "id": "src-m1",
                "email": "alice@example.com",
                "role_id": "src-role",
                "workspace_ids": ["ws-1"],
                "workspace_role_id": "src-ws-role",
            },
        ]

        m, s, f = migrator.migrate_org_members(
            members,
            remove_missing=True,
            remove_pending=True,
        )

        assert (m, s, f) == (1, 0, 0)
        migrator.dest.delete.assert_called_once_with(
            "/orgs/current/members/pending/pending-1"
        )
        migrator.dest.post.assert_called_once_with(
            "/orgs/current/members",
            {
                "email": "alice@example.com",
                "role_id": "dst-role",
                "workspace_ids": ["ws-1"],
                "workspace_role_id": "dst-ws-role",
            },
        )

    def test_migrate_org_members_pending_replace_permission_failure_calls_out_org_admin_pat(
        self, migrator, migration_state
    ):
        """Pending invite replacement failures should tell operators to use an org admin PAT."""
        migrator.state = migration_state
        migrator._role_id_map = {"src-role": "dst-role-new"}
        migrator.dest.get_paginated.side_effect = [
            iter([]),
            iter([
                {"id": "pending-1", "email": "alice@example.com", "role_id": "dst-role-old"},
            ]),
        ]
        migrator.dest.delete.side_effect = AuthenticationError(
            "Access denied for /orgs/current/members/pending/pending-1",
            status_code=403,
            request_info={"endpoint": "/orgs/current/members/pending/pending-1"},
        )

        members = [
            {"id": "src-m1", "email": "alice@example.com", "role_id": "src-role"},
        ]

        m, s, f = migrator.migrate_org_members(
            members,
            remove_pending=True,
        )

        assert (m, s, f) == (0, 0, 1)
        item = migration_state.get_item("org_member_alice@example.com")
        assert item is not None
        assert item.outcome_code == "org_member_pending_invite_replace_failed"
        assert "Organization Admin PAT" in item.next_action
        assert item.evidence["requires_org_admin_pat"] is True
        migrator.dest.post.assert_not_called()

    def test_migrate_org_members_pending_cancel_api_error_permission_failure_calls_out_org_admin_pat(
        self, migrator, migration_state
    ):
        """403 not-allowed pending invite deletes should be treated as permission failures."""
        migrator.state = migration_state
        migrator._role_id_map = {"src-role": "dst-role-new"}
        migrator.dest.get_paginated.side_effect = [
            iter([]),
            iter([
                {"id": "pending-1", "email": "alice@example.com", "role_id": "dst-role-old"},
            ]),
        ]
        migrator.dest.delete.side_effect = APIError(
            "not allowed to delete pending invite",
            status_code=403,
            request_info={"endpoint": "/orgs/current/members/pending/pending-1"},
        )

        members = [
            {"id": "src-m1", "email": "alice@example.com", "role_id": "src-role"},
        ]

        m, s, f = migrator.migrate_org_members(
            members,
            remove_pending=True,
        )

        assert (m, s, f) == (0, 0, 1)
        item = migration_state.get_item("org_member_alice@example.com")
        assert item is not None
        assert item.outcome_code == "org_member_pending_invite_replace_failed"
        assert "Organization Admin PAT" in item.next_action
        assert item.evidence["requires_org_admin_pat"] is True
        assert migrator.dest.delete.call_args_list == [
            call("/orgs/current/members/pending/pending-1")
        ]
        migrator.dest.post.assert_not_called()

    def test_migrate_org_members_pending_replace_conflict_is_specific(
        self, migrator, migration_state
    ):
        """A replacement conflict after cancel is reported as a pending invite conflict."""
        migrator.state = migration_state
        migrator._role_id_map = {
            "src-role": "dst-role-new",
            "src-ws-role": "dst-ws-role",
        }
        migrator.dest.get_paginated.side_effect = [
            iter([]),
            iter([
                {
                    "id": "pending-1",
                    "email": "alice@example.com",
                    "role_id": "dst-role-old",
                    "workspace_ids": ["ws-old"],
                    "workspace_role_id": "dst-ws-old",
                },
            ]),
        ]
        migrator.dest.post.side_effect = ConflictError(
            "pending invite already exists",
            request_info={"endpoint": "/orgs/current/members"},
        )

        members = [
            {
                "id": "src-m1",
                "email": "alice@example.com",
                "role_id": "src-role",
                "workspace_ids": ["ws-new"],
                "workspace_role_id": "src-ws-role",
            },
        ]

        m, s, f = migrator.migrate_org_members(
            members,
            remove_pending=True,
        )

        assert (m, s, f) == (0, 0, 1)
        item = migration_state.get_item("org_member_alice@example.com")
        assert item is not None
        assert item.outcome_code == "org_member_pending_invite_replace_conflict"
        assert item.evidence["existing_workspace_ids"] == ["ws-old"]
        assert item.evidence["desired_workspace_ids"] == ["ws-new"]
        assert item.evidence["existing_workspace_role_id"] == "dst-ws-old"
        assert item.evidence["desired_workspace_role_id"] == "dst-ws-role"
        assert migrator._pending_org_blockers["alice@example.com"]["code"] == (
            "org_member_pending_invite_replace_conflict"
        )

    def test_migrate_org_members_source_of_truth_replaces_org_only_pending_invite_with_workspace_access(
        self, migrator
    ):
        """Authoritative sync should remove pending workspace access when the CSV only wants org access."""
        migrator._role_id_map = {"src-role": "dst-role"}
        migrator.dest.get_paginated.side_effect = [
            iter([]),
            iter([
                {
                    "id": "pending-1",
                    "email": "alice@example.com",
                    "role_id": "dst-role",
                    "workspace_ids": ["ws-1"],
                    "workspace_role_id": "dst-ws-role",
                },
            ]),
        ]
        migrator.dest.post.return_value = {"id": "new-identity-1"}

        members = [
            {"id": "src-m1", "email": "alice@example.com", "role_id": "src-role"},
        ]

        m, s, f = migrator.migrate_org_members(
            members,
            remove_missing=True,
            remove_pending=True,
        )

        assert (m, s, f) == (1, 0, 0)
        migrator.dest.delete.assert_called_once_with(
            "/orgs/current/members/pending/pending-1"
        )
        migrator.dest.post.assert_called_once_with(
            "/orgs/current/members",
            {"email": "alice@example.com", "role_id": "dst-role"},
        )

    def test_migrate_org_members_remove_missing_active_and_pending(self, migrator):
        """Authoritative mode removes extra active members and pending invites."""
        migrator._role_id_map = {"src-role": "dst-role"}
        migrator.dest.get_paginated.side_effect = [
            iter([
                {"id": "dst-keep", "email": "keep@example.com", "role_id": "dst-role"},
                {"id": "dst-remove", "email": "remove@example.com", "role_id": "dst-role"},
            ]),
            iter([
                {"id": "pending-remove", "email": "pending@example.com", "role_id": "dst-role"},
            ]),
        ]

        members = [
            {"id": "src-keep", "email": "keep@example.com", "role_id": "src-role"},
        ]

        m, s, f = migrator.migrate_org_members(
            members,
            remove_missing=True,
            remove_pending=True,
        )

        assert (m, s, f) == (2, 1, 0)
        assert migrator._last_org_member_removals == 2
        migrator.dest.delete.assert_any_call("/orgs/current/members/dst-remove")
        migrator.dest.delete.assert_any_call(
            "/orgs/current/members/pending/pending-remove"
        )

    def test_migrate_org_members_remove_missing_treats_active_not_found_as_reconciled(
        self, migrator
    ):
        """Authoritative org member removal is idempotent when DELETE reports already absent."""
        migrator._role_id_map = {"src-role": "dst-role"}
        migrator.dest.get_paginated.side_effect = [
            iter([
                {"id": "dst-keep", "email": "keep@example.com", "role_id": "dst-role"},
                {"id": "dst-remove", "email": "remove@example.com", "role_id": "dst-role"},
            ]),
            iter([]),
        ]
        migrator.dest.delete.side_effect = APIError(
            "User not found",
            status_code=404,
            request_info={"endpoint": "/orgs/current/members/dst-remove"},
        )

        members = [
            {"id": "src-keep", "email": "keep@example.com", "role_id": "src-role"},
        ]

        m, s, f = migrator.migrate_org_members(
            members,
            remove_missing=True,
            remove_pending=True,
        )

        assert (m, s, f) == (1, 1, 0)
        assert migrator._last_org_member_removals == 1
        migrator.dest.delete.assert_called_once_with("/orgs/current/members/dst-remove")

    # ── Phase 3: Workspace member migration ──

    def test_migrate_workspace_members_add(self, migrator):
        """Users not in workspace are added."""
        migrator._role_id_map = {"src-ws-role": "dst-ws-role"}
        migrator._dest_email_to_identity = {
            "alice@example.com": {"id": "dst-org-identity-1", "user_id": "dst-user-1"},
        }
        migrator.source.get_paginated.return_value = iter([
            {"id": "src-ws-m1", "email": "alice@example.com", "role_id": "src-ws-role"},
        ])
        migrator.dest.get_paginated.return_value = iter([])
        migrator.dest.post.return_value = {"id": "dst-ws-identity-1"}

        m, s, f = migrator.migrate_workspace_members()

        assert m == 1
        migrator.dest.post.assert_called_once_with(
            "/workspaces/current/members",
            {
                "user_id": "dst-user-1",
                "workspace_ids": ["ws-default-dst"],
                "workspace_role_id": "dst-ws-role",
            },
        )

    def test_migrate_workspace_members_update_role(self, migrator):
        """Workspace members with wrong role get updated."""
        migrator._role_id_map = {"src-ws-role": "dst-ws-role-new"}
        migrator.source.get_paginated.return_value = iter([
            {"id": "src-ws-m1", "email": "alice@example.com", "role_id": "src-ws-role"},
        ])
        migrator.dest.get_paginated.return_value = iter([
            {"id": "dst-ws-m1", "email": "alice@example.com", "role_id": "dst-ws-role-old"},
        ])

        m, s, f = migrator.migrate_workspace_members()

        assert m == 1
        migrator.dest.patch.assert_called_once_with(
            "/workspaces/current/members/dst-ws-m1",
            {"role_id": "dst-ws-role-new"},
        )

    def test_migrate_workspace_members_add_falls_back_to_legacy_tenant_endpoint(self, migrator):
        """Workspace member creation should tolerate instances that only support the legacy tenant write path."""
        migrator._role_id_map = {"src-ws-role": "dst-ws-role"}
        migrator._dest_email_to_identity = {
            "alice@example.com": {"id": "dst-org-identity-1", "user_id": "dst-user-1"},
        }
        migrator.source.get_paginated.return_value = iter([
            {"id": "src-ws-m1", "email": "alice@example.com", "role_id": "src-ws-role"},
        ])
        migrator.dest.get_paginated.return_value = iter([])
        migrator.dest.post.side_effect = [
            APIError("Not found", status_code=404, request_info={}),
            {"id": "dst-ws-identity-1"},
        ]

        m, s, f = migrator.migrate_workspace_members()

        assert (m, s, f) == (1, 0, 0)
        assert migrator.dest.post.call_args_list == [
            call(
                "/workspaces/current/members",
                {
                    "user_id": "dst-user-1",
                    "workspace_ids": ["ws-default-dst"],
                    "workspace_role_id": "dst-ws-role",
                },
            ),
            call(
                "/tenants/current/members",
                {"org_identity_id": "dst-org-identity-1", "role_id": "dst-ws-role"},
            ),
        ]

    def test_migrate_workspace_members_update_role_falls_back_to_legacy_tenant_endpoint(
        self, migrator
    ):
        """Workspace role updates should tolerate instances that only support the legacy tenant write path."""
        migrator._role_id_map = {"src-ws-role": "dst-ws-role-new"}
        migrator.source.get_paginated.return_value = iter([
            {"id": "src-ws-m1", "email": "alice@example.com", "role_id": "src-ws-role"},
        ])
        migrator.dest.get_paginated.return_value = iter([
            {"id": "dst-ws-m1", "email": "alice@example.com", "role_id": "dst-ws-role-old"},
        ])
        migrator.dest.patch.side_effect = [
            APIError("Not found", status_code=404, request_info={}),
            {},
        ]

        m, s, f = migrator.migrate_workspace_members()

        assert (m, s, f) == (1, 0, 0)
        assert migrator.dest.patch.call_args_list == [
            call("/workspaces/current/members/dst-ws-m1", {"role_id": "dst-ws-role-new"}),
            call("/tenants/current/members/dst-ws-m1", {"role_id": "dst-ws-role-new"}),
        ]

    def test_migrate_workspace_members_not_in_org(self, migrator):
        """User not an org member on dest is recorded as failed."""
        migrator._role_id_map = {"src-ws-role": "dst-ws-role"}
        migrator._dest_email_to_identity = {}  # Not in org
        migrator.source.get_paginated.return_value = iter([
            {"id": "src-ws-m1", "email": "bob@example.com", "role_id": "src-ws-role"},
        ])
        migrator.dest.get_paginated.return_value = iter([])

        m, s, f = migrator.migrate_workspace_members()

        assert f == 1
        assert m == 0

    def test_migrate_workspace_members_skips_when_included_in_pending_org_invite(self, migrator):
        """Workspace phase should not re-add membership already included in a pending org invite."""
        migrator._role_id_map = {"src-ws-role": "dst-ws-role"}
        migrator._dest_email_to_identity = {}
        migrator._pending_workspace_invites = {("alice@example.com", "ws-default-dst")}
        migrator.source.get_paginated.return_value = iter([
            {"id": "src-ws-m1", "email": "alice@example.com", "role_id": "src-ws-role"},
        ])
        migrator.dest.get_paginated.return_value = iter([])

        m, s, f = migrator.migrate_workspace_members()

        assert (m, s, f) == (0, 1, 0)
        migrator.dest.post.assert_not_called()

    def test_migrate_workspace_members_blocks_pending_org_invite_without_user_id(self, migrator):
        """Pending org invites without user_id need acceptance before workspace add."""
        migrator._role_id_map = {"src-ws-role": "dst-ws-role"}
        pending_identity = {"id": "pending-org-identity-1", "email": "alice@example.com"}
        migrator._pending_org_email_to_identity = {"alice@example.com": pending_identity}
        migrator._dest_email_to_identity = {"alice@example.com": pending_identity}
        migrator.source.get_paginated.return_value = iter([
            {"id": "src-ws-m1", "email": "alice@example.com", "role_id": "src-ws-role"},
        ])
        migrator.dest.get_paginated.return_value = iter([])

        m, s, f = migrator.migrate_workspace_members()

        assert (m, s, f) == (0, 0, 1)
        migrator.dest.post.assert_not_called()
        item = migrator.state.items["ws_member_ws-default-src_alice@example.com"]
        assert item.outcome_code == "ws_member_pending_org_invite"

    def test_migrate_workspace_members_skips_when_org_pending_reconciliation_blocked(
        self, migrator
    ):
        """Workspace phase should not cascade a blocked org pending invite into add failures."""
        migrator._role_id_map = {"src-ws-role": "dst-ws-role"}
        migrator._dest_email_to_identity = {}
        migrator._pending_org_blockers = {
            "alice@example.com": {
                "item_id": "org_member_alice@example.com",
                "code": "org_member_pending_invite_cancel_unsupported",
                "next_action": "Cancel or replace the pending org invite manually.",
            }
        }
        migrator.source.get_paginated.return_value = iter([
            {"id": "src-ws-m1", "email": "alice@example.com", "role_id": "src-ws-role"},
        ])
        migrator.dest.get_paginated.return_value = iter([])

        m, s, f = migrator.migrate_workspace_members()

        assert (m, s, f) == (0, 1, 0)
        migrator.dest.post.assert_not_called()
        item = migrator.state.items["ws_member_ws-default-src_alice@example.com"]
        assert item.outcome_code == "ws_member_skipped_pending_org_blocker"
        assert item.evidence["org_item_id"] == "org_member_alice@example.com"
        assert item.evidence["org_blocker"] == "org_member_pending_invite_cancel_unsupported"

    def test_migrate_workspace_members_remove_missing(self, migrator):
        """Authoritative workspace sync removes extra memberships."""
        migrator._role_id_map = {"src-ws-role": "dst-ws-role"}
        migrator._dest_email_to_identity = {
            "keep@example.com": {"id": "dst-org-identity-1"},
        }
        migrator.dest.get_paginated.return_value = iter([
            {"id": "dst-ws-keep", "email": "keep@example.com", "role_id": "dst-ws-role"},
            {"id": "dst-ws-remove", "email": "remove@example.com", "role_id": "dst-ws-role"},
        ])

        selected = [
            {"id": "src-ws-m1", "email": "keep@example.com", "role_id": "src-ws-role"},
        ]

        m, s, f = migrator.migrate_workspace_members(
            selected_members=selected,
            remove_missing=True,
        )

        assert (m, s, f) == (1, 1, 0)
        assert migrator._last_workspace_member_removals == 1
        migrator.dest.delete.assert_called_once_with(
            "/workspaces/current/members/dst-ws-remove"
        )

    def test_migrate_workspace_members_remove_missing_with_empty_selection(self, migrator):
        """Authoritative workspace sync can clear a workspace omitted from the CSV."""
        migrator._role_id_map = {}
        migrator._dest_email_to_identity = {}
        migrator.dest.get_paginated.return_value = iter([
            {"id": "dst-ws-remove", "email": "remove@example.com", "role_id": "dst-ws-role"},
        ])

        m, s, f = migrator.migrate_workspace_members(
            selected_members=[],
            remove_missing=True,
        )

        assert (m, s, f) == (1, 0, 0)
        assert migrator._last_workspace_member_removals == 1
        migrator.dest.delete.assert_called_once_with(
            "/workspaces/current/members/dst-ws-remove"
        )

    def test_migrate_workspace_members_remove_missing_falls_back_to_legacy_tenant_endpoint(
        self, migrator
    ):
        """Workspace member removals should tolerate instances that only support the legacy tenant delete path."""
        migrator._role_id_map = {}
        migrator._dest_email_to_identity = {}
        migrator.dest.get_paginated.return_value = iter([
            {"id": "dst-ws-remove", "email": "remove@example.com", "role_id": "dst-ws-role"},
        ])
        migrator.dest.delete.side_effect = [
            APIError("Not found", status_code=404, request_info={}),
            {},
        ]

        m, s, f = migrator.migrate_workspace_members(
            selected_members=[],
            remove_missing=True,
        )

        assert (m, s, f) == (1, 0, 0)
        assert migrator.dest.delete.call_args_list == [
            call("/workspaces/current/members/dst-ws-remove"),
            call("/tenants/current/members/dst-ws-remove"),
        ]

    # ── Dry run ──

    def test_dry_run_no_mutations(self, migrator, sample_config, source_roles, dest_roles):
        """In dry run mode, no POST/PATCH calls are made."""
        sample_config.migration.dry_run = True
        migrator.source.get.return_value = source_roles
        migrator.dest.get.return_value = dest_roles

        migrator.build_role_mapping()

        # Custom role should NOT be created via POST
        migrator.dest.post.assert_not_called()
        migrator.dest.patch.assert_not_called()

    def test_dry_run_org_member_invite(self, migrator, sample_config):
        """Dry run skips actual invites."""
        sample_config.migration.dry_run = True
        migrator._role_id_map = {"src-role": "dst-role"}
        migrator.dest.get_paginated.return_value = iter([])

        members = [
            {"id": "src-m1", "email": "alice@example.com", "role_id": "src-role"},
        ]

        m, s, f = migrator.migrate_org_members(members)

        assert m == 1
        migrator.dest.post.assert_not_called()

    def test_dry_run_org_member_removals(self, migrator, sample_config):
        """Dry run records authoritative org removals without issuing DELETEs."""
        sample_config.migration.dry_run = True
        migrator._role_id_map = {"src-role": "dst-role"}
        migrator.dest.get_paginated.side_effect = [
            iter([
                {"id": "dst-keep", "email": "keep@example.com", "role_id": "dst-role"},
                {"id": "dst-remove", "email": "remove@example.com", "role_id": "dst-role"},
            ]),
            iter([
                {"id": "pending-remove", "email": "pending@example.com", "role_id": "dst-role"},
            ]),
        ]

        members = [
            {"id": "src-keep", "email": "keep@example.com", "role_id": "src-role"},
        ]

        m, s, f = migrator.migrate_org_members(
            members,
            remove_missing=True,
            remove_pending=True,
        )

        assert (m, s, f) == (2, 1, 0)
        assert migrator._last_org_member_removals == 2
        migrator.dest.delete.assert_not_called()

    def test_dry_run_workspace_member_update(self, migrator, sample_config):
        """Dry run skips workspace role PATCH requests."""
        sample_config.migration.dry_run = True
        migrator._role_id_map = {"src-ws-role": "dst-ws-role-new"}
        migrator.dest.get_paginated.return_value = iter([
            {"id": "dst-ws-m1", "email": "alice@example.com", "role_id": "dst-ws-role-old"},
        ])

        members = [
            {"id": "src-ws-m1", "email": "alice@example.com", "role_id": "src-ws-role"},
        ]

        m, s, f = migrator.migrate_workspace_members(selected_members=members)

        assert (m, s, f) == (1, 0, 0)
        migrator.dest.patch.assert_not_called()

    def test_dry_run_workspace_member_removals(self, migrator, sample_config):
        """Dry run records authoritative workspace removals without issuing DELETEs."""
        sample_config.migration.dry_run = True
        migrator._role_id_map = {"src-ws-role": "dst-ws-role"}
        migrator._dest_email_to_identity = {
            "keep@example.com": {"id": "dst-org-identity-1"},
        }
        migrator.dest.get_paginated.return_value = iter([
            {"id": "dst-ws-keep", "email": "keep@example.com", "role_id": "dst-ws-role"},
            {"id": "dst-ws-remove", "email": "remove@example.com", "role_id": "dst-ws-role"},
        ])

        selected = [
            {"id": "src-ws-m1", "email": "keep@example.com", "role_id": "src-ws-role"},
        ]

        m, s, f = migrator.migrate_workspace_members(
            selected_members=selected,
            remove_missing=True,
        )

        assert (m, s, f) == (1, 1, 0)
        assert migrator._last_workspace_member_removals == 1
        migrator.dest.delete.assert_not_called()

    # ── Role update failure ──

    def test_sync_custom_roles_update_failure_records_issue(self, migrator, source_roles, sample_config):
        """When _update_custom_role fails, role is still mapped but issue is recorded."""
        sample_config.migration.skip_existing = False

        dest_roles_with_custom = [
            {"id": "dst-custom-1", "name": "CUSTOM", "display_name": "Data Scientist",
             "permissions": ["datasets:read"]},  # Different permissions
        ]

        migrator.dest.patch.side_effect = APIError("Permission denied", request_info={})

        mapping = migrator._sync_custom_roles(source_roles, dest_roles_with_custom)

        # Role should still be mapped (it exists on destination)
        assert mapping["src-custom-1"] == "dst-custom-1"

    def test_update_custom_role_returns_bool(self, migrator):
        """_update_custom_role returns True on success, False on failure."""
        role = {"display_name": "Test", "description": "", "permissions": ["read"]}

        # Success case
        result = migrator._update_custom_role("role-1", role)
        assert result is True

        # Failure case
        migrator.dest.patch.side_effect = APIError("fail", request_info={})
        result = migrator._update_custom_role("role-1", role)
        assert result is False

    # ── Pending member logging ──

    def test_pending_member_logged(self, migrator):
        """Pending members are logged distinctly."""
        migrator._role_id_map = {"src-role": "dst-role"}
        migrator.dest.get_paginated.return_value = iter([])
        migrator.dest.post.return_value = {"id": "new-1"}
        migrator.config.migration.verbose = True

        members = [
            {"id": "src-m1", "email": "alice@example.com", "role_id": "src-role", "_pending": True},
        ]

        m, s, f = migrator.migrate_org_members(members)
        assert m == 1

    # ── Workspace member selection ──

    def test_migrate_workspace_members_with_selected(self, migrator):
        """When selected_members is provided, only those are processed."""
        migrator._role_id_map = {"src-ws-role": "dst-ws-role"}
        migrator._dest_email_to_identity = {
            "alice@example.com": {"id": "dst-org-identity-1"},
        }
        # dest has no existing workspace members
        migrator.dest.get_paginated.return_value = iter([])
        migrator.dest.post.return_value = {"id": "dst-ws-identity-1"}

        selected = [
            {"id": "src-ws-m1", "email": "alice@example.com", "role_id": "src-ws-role"},
        ]

        m, s, f = migrator.migrate_workspace_members(selected_members=selected)

        assert m == 1
        # source.get_paginated should NOT be called since we provided selected_members
        migrator.source.get_paginated.assert_not_called()

    def test_migrate_workspace_members_none_fetches_all(self, migrator):
        """When selected_members is None, all source members are fetched."""
        migrator._role_id_map = {}
        migrator.source.get_paginated.return_value = iter([])
        migrator.dest.get_paginated.return_value = iter([])

        m, s, f = migrator.migrate_workspace_members(selected_members=None)

        assert m == 0
        migrator.source.get_paginated.assert_called_once()

    def test_migrate_workspace_members_from_csv_rows_filters_by_workspace(self, migrator):
        """CSV workspace migration only processes rows for active source workspace."""
        migrator.source.session.headers["X-Tenant-Id"] = "ws-src-1"
        migrator._role_id_map = {"src-ws-role": "dst-ws-role"}
        migrator._dest_email_to_identity = {
            "alice@example.com": {"id": "dst-org-identity-1", "user_id": "dst-user-1"},
        }
        migrator.dest.get_paginated.return_value = iter([])
        migrator.dest.post.return_value = {"id": "dst-ws-identity-1"}

        rows = [
            {
                "email": "alice@example.com",
                "role_id": "src-ws-role",
                "workspace_id": "ws-src-1",
            },
            {
                "email": "bob@example.com",
                "role_id": "src-ws-role",
                "workspace_id": "ws-src-2",
            },
        ]

        m, s, f = migrator.migrate_workspace_members_from_csv_rows(rows)

        assert (m, s, f) == (1, 0, 0)
        migrator.dest.post.assert_called_once_with(
            "/workspaces/current/members",
            {
                "user_id": "dst-user-1",
                "workspace_ids": ["ws-default-dst"],
                "workspace_role_id": "dst-ws-role",
            },
        )

    def test_migrate_workspace_members_from_csv_rows_requires_workspace_context(self, migrator):
        """CSV workspace migration requires an active source workspace."""
        migrator.source.session.headers = {}

        with pytest.raises(APIError):
            migrator.migrate_workspace_members_from_csv_rows(
                [{"email": "alice@example.com", "role_id": "src-role", "workspace_id": "ws-1"}]
            )

    # ── Per-member state tracking ──

    def test_org_member_ensure_item_called(self, migrator, migration_state):
        """ensure_item is called for each org member."""
        migrator.state = migration_state
        migrator._role_id_map = {"src-role": "dst-role"}
        migrator.dest.get_paginated.return_value = iter([])
        migrator.dest.post.return_value = {"id": "new-1"}

        members = [
            {"id": "src-m1", "email": "alice@example.com", "role_id": "src-role"},
        ]

        migrator.migrate_org_members(members)

        item = migration_state.get_item("org_member_alice@example.com")
        assert item is not None
        assert item.type == "org_member"

    def test_ws_member_ensure_item_called(self, migrator, migration_state):
        """ensure_item is called for each workspace member."""
        migrator.state = migration_state
        migrator._role_id_map = {"src-ws-role": "dst-ws-role"}
        migrator._dest_email_to_identity = {
            "alice@example.com": {"id": "dst-org-1"},
        }
        migrator.source.get_paginated.return_value = iter([
            {"id": "src-ws-m1", "email": "alice@example.com", "role_id": "src-ws-role"},
        ])
        migrator.dest.get_paginated.return_value = iter([])
        migrator.dest.post.return_value = {"id": "dst-ws-1"}

        migrator.migrate_workspace_members()

        item = migration_state.get_item("ws_member_ws-default-src_alice@example.com")
        assert item is not None
        assert item.type == "ws_member"

    def test_migrate_workspace_members_requires_workspace_context(self, migrator):
        """Workspace migration requires active workspace context."""
        migrator.source.session.headers = {}
        migrator._role_id_map = {"src-ws-role": "dst-ws-role"}
        migrator._dest_email_to_identity = {
            "alice@example.com": {"id": "dst-org-identity-1"},
        }
        migrator.dest.get_paginated.return_value = iter([])

        with pytest.raises(APIError, match="Active source workspace"):
            migrator.migrate_workspace_members(
                selected_members=[
                    {"id": "src-ws-m1", "email": "alice@example.com", "role_id": "src-ws-role"}
                ]
            )

    def test_workspace_member_item_ids_do_not_collide_across_workspaces(self, migrator, migration_state):
        """Same email in different source workspaces creates distinct state items."""
        migrator.state = migration_state
        migrator._role_id_map = {"src-ws-role": "dst-ws-role"}
        migrator._dest_email_to_identity = {
            "alice@example.com": {"id": "dst-org-identity-1"},
        }
        migrator.dest.get_paginated.return_value = iter([])
        migrator.dest.post.return_value = {"id": "dst-ws-identity-1"}

        migrator.source.session.headers["X-Tenant-Id"] = "ws-src-1"
        migrator.migrate_workspace_members(
            selected_members=[
                {"id": "src-ws-a", "email": "alice@example.com", "role_id": "src-ws-role"}
            ]
        )
        migrator.source.session.headers["X-Tenant-Id"] = "ws-src-2"
        migrator.migrate_workspace_members(
            selected_members=[
                {"id": "src-ws-b", "email": "alice@example.com", "role_id": "src-ws-role"}
            ]
        )

        assert migration_state.get_item("ws_member_ws-src-1_alice@example.com") is not None
        assert migration_state.get_item("ws_member_ws-src-2_alice@example.com") is not None

    def test_dest_email_index_none_sentinel(self, migrator):
        """Destination email index starts as None (not-yet-fetched sentinel)."""
        assert migrator._dest_email_to_identity is None
        # Can be set to a dict for resume
        migrator._dest_email_to_identity = {"alice@example.com": {"id": "dst-1"}}
        assert migrator._dest_email_to_identity["alice@example.com"]["id"] == "dst-1"

    def test_migrate_workspace_members_from_csv_rows_combines_custom_roles(
        self, migrator
    ):
        """CSV workspace rows with multiple custom roles collapse to a union role."""
        migrator.source.session.headers["X-Tenant-Id"] = "ws-src-1"
        migrator.migrate_workspace_members = Mock(return_value=(1, 0, 0))
        rows = [
            {
                "email": "alice@example.com",
                "role_id": "src-custom-1",
                "role_name": "CUSTOM",
                "workspace_id": "ws-src-1",
            },
            {
                "email": "alice@example.com",
                "role_id": "src-custom-2",
                "role_name": "CUSTOM",
                "workspace_id": "ws-src-1",
            },
        ]

        assert migrator.migrate_workspace_members_from_csv_rows(rows) == (1, 0, 0)
        migrator.migrate_workspace_members.assert_called_once_with(
            selected_members=[
                {
                    "id": "ws-src-1:alice@example.com",
                    "email": "alice@example.com",
                    "role_id": make_workspace_role_union_id(
                        {"src-custom-1", "src-custom-2"}
                    ),
                    "full_name": "",
                }
            ]
        )

    def test_ws_member_item_id_includes_workspace(self, migrator, migration_state):
        """ws_member item_id includes source workspace to avoid cross-workspace collision."""
        migrator.state = migration_state
        migrator.source.session.headers["X-Tenant-Id"] = "ws-abc-123"
        migrator._role_id_map = {"src-ws-role": "dst-ws-role"}
        migrator._dest_email_to_identity = {
            "alice@example.com": {"id": "dst-org-1"},
        }
        migrator.source.get_paginated.return_value = iter([
            {"id": "src-ws-m1", "email": "alice@example.com", "role_id": "src-ws-role"},
        ])
        migrator.dest.get_paginated.return_value = iter([])
        migrator.dest.post.return_value = {"id": "dst-ws-1"}

        migrator.migrate_workspace_members()

        item = migration_state.get_item("ws_member_ws-abc-123_alice@example.com")
        assert item is not None
        assert item.type == "ws_member"

    def test_empty_role_id_treated_as_none(self, migrator):
        """Empty-string role_id does not trigger unmapped_role failure."""
        migrator._role_id_map = {}
        migrator.dest.get_paginated.return_value = iter([])
        migrator.dest.post.return_value = {"id": "new-1"}

        members = [
            {"id": "src-m1", "email": "alice@example.com", "role_id": ""},
        ]

        m, s, f = migrator.migrate_org_members(members)

        # Empty role_id → None → no role mapping needed → invited without role
        assert m == 1
        assert f == 0

    def test_org_member_failed_marked_blocked(self, migrator, migration_state):
        """Failed org members are marked blocked for remediation."""
        migrator.state = migration_state
        migrator._role_id_map = {}  # No mappings — will cause failure
        migrator.dest.get_paginated.return_value = iter([])

        members = [
            {"id": "src-m1", "email": "alice@example.com", "role_id": "unknown-role"},
        ]

        m, s, f = migrator.migrate_org_members(members)

        assert f == 1
        item = migration_state.get_item("org_member_alice@example.com")
        assert item is not None
        assert item.terminal_state == "blocked_with_checkpoint"
        assert item.outcome_code == "unmapped_role"

    # ── Capability probing ──

    def test_require_destination_org_admin_for_authoritative_sync_fails_on_pending_permission_error(
        self, migrator, migration_state
    ):
        """Authoritative sync should fail fast when pending invite lookup lacks org admin access."""
        migrator.state = migration_state
        migrator.dest.get.side_effect = [
            [],
            AuthenticationError(
                "Access denied for /orgs/current/members/pending",
                status_code=403,
                request_info={"endpoint": "/orgs/current/members/pending"},
            ),
        ]

        with pytest.raises(APIError, match="Organization Admin PAT"):
            migrator.require_destination_org_admin_for_authoritative_sync()

        assert migrator.dest.get.call_args_list == [
            call("/orgs/current/members/active", params={"limit": 1}),
            call("/orgs/current/members/pending", params={"limit": 1}),
        ]
        assert migration_state.capability_matrix["dest"]["org_member_management"][
            "supported"
        ] is False
        assert migration_state.issue_log[-1].code == "dest_org_admin_pat_required"

    def test_probe_capabilities_success(self, migrator):
        """Probe records capabilities when endpoints succeed."""
        migrator.source.get.return_value = []
        migrator.dest.get.return_value = []

        migrator.probe_capabilities()

        # Should not raise
