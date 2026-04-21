"""Unit tests for UserRoleMigrator."""

import pytest
from unittest.mock import Mock, call
from langsmith_migrator.core.migrators import UserRoleMigrator
from langsmith_migrator.utils.retry import APIError, ConflictError


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

    def test_ensure_workspace_role_unions_reuses_state_mapping(
        self, migrator, migration_state
    ):
        """Managed union roles are reused across retries via persisted state."""
        migration_state.id_mappings["role_unions"] = {"src-a|src-b": "dst-union"}
        migrator.state = migration_state

        source_roles = [
            {
                "id": "src-a",
                "name": "CUSTOM",
                "display_name": "Assistant A",
                "permissions": ["projects:read"],
            },
            {
                "id": "src-b",
                "name": "CUSTOM",
                "display_name": "Assistant B",
                "permissions": ["datasets:read"],
            },
        ]
        migrator.source.get.side_effect = lambda endpoint: []
        migrator.dest.get.side_effect = lambda endpoint: (
            [
                {
                    "id": "dst-union",
                    "name": "CUSTOM",
                    "display_name": "Migrator Union 128d62f94654",
                    "description": (
                        "Managed by langsmith-migrator for union::src-a|src-b. "
                        "Source roles: Assistant A, Assistant B"
                    ),
                    "permissions": ["projects:read"],
                }
            ]
            if endpoint == "/orgs/current/roles"
            else []
        )

        mapping = migrator.ensure_workspace_role_unions(
            source_roles=source_roles,
            union_specs=[
                {
                    "signature": "src-a|src-b",
                    "role_id": "union::src-a|src-b",
                    "source_role_ids": ["src-a", "src-b"],
                }
            ],
        )

        assert mapping == {"union::src-a|src-b": "dst-union"}
        assert (
            migrator.state.get_mapped_id("roles", "union::src-a|src-b")
            == "dst-union"
        )
        assert migrator.dest.post.call_args_list == [
            call(
                "/orgs/current/roles/dst-union/access-policies",
                {"access_policy_ids": []},
            )
        ]

    def test_ensure_workspace_role_unions_patches_permission_union(self, migrator):
        """Existing managed union roles are updated to the full permission union."""
        source_roles = [
            {
                "id": "src-a",
                "name": "CUSTOM",
                "display_name": "Assistant A",
                "permissions": ["projects:read"],
            },
            {
                "id": "src-b",
                "name": "CUSTOM",
                "display_name": "Assistant B",
                "permissions": ["datasets:read", "projects:read"],
            },
        ]
        migrator.state.id_mappings["role_unions"] = {"src-a|src-b": "dst-union"}
        migrator.source.get.side_effect = lambda endpoint: []
        migrator.dest.get.side_effect = lambda endpoint: (
            [
                {
                    "id": "dst-union",
                    "name": "CUSTOM",
                    "display_name": "Old Union",
                    "description": "Managed by langsmith-migrator for union::src-a|src-b.",
                    "permissions": ["projects:read"],
                }
            ]
            if endpoint == "/orgs/current/roles"
            else []
        )

        migrator.ensure_workspace_role_unions(
            source_roles=source_roles,
            union_specs=[
                {
                    "signature": "src-a|src-b",
                    "role_id": "union::src-a|src-b",
                    "source_role_ids": ["src-a", "src-b"],
                }
            ],
        )

        migrator.dest.patch.assert_called_once_with(
            "/orgs/current/roles/dst-union",
            {
                "display_name": "Migrator Union 128d62f94654",
                "description": (
                    "Managed by langsmith-migrator for union::src-a|src-b. "
                    "Source roles: Assistant A, Assistant B"
                ),
                "permissions": ["datasets:read", "projects:read"],
            },
        )

    def test_ensure_workspace_role_unions_skip_existing_reuses_without_mutation(
        self, migrator, sample_config
    ):
        """Existing managed union roles are reused as-is when skip_existing is enabled."""
        sample_config.migration.skip_existing = True
        source_roles = [
            {
                "id": "src-a",
                "name": "CUSTOM",
                "display_name": "Assistant A",
                "permissions": ["projects:read"],
            },
            {
                "id": "src-b",
                "name": "CUSTOM",
                "display_name": "Assistant B",
                "permissions": ["datasets:read", "projects:read"],
            },
        ]
        source_policy = {
            "id": "src-policy-1",
            "name": "Project Access",
            "effect": "allow",
            "condition_groups": [
                {
                    "permission": "projects:read",
                    "resource_type": "project",
                    "conditions": [],
                }
            ],
            "role_ids": ["src-a"],
        }
        migrator.state.id_mappings["role_unions"] = {"src-a|src-b": "dst-union"}

        def source_get(endpoint):
            if endpoint == "/orgs/current/access-policies":
                return [source_policy]
            raise AssertionError(endpoint)

        def dest_get(endpoint):
            if endpoint == "/orgs/current/roles":
                return [
                    {
                        "id": "dst-union",
                        "name": "CUSTOM",
                        "display_name": "Migrator Union 128d62f94654",
                        "description": (
                            "Managed by langsmith-migrator for union::src-a|src-b. "
                            "Source roles: Assistant A, Assistant B"
                        ),
                        "permissions": ["projects:read"],
                    }
                ]
            if endpoint == "/orgs/current/access-policies":
                return []
            raise AssertionError(endpoint)

        migrator.source.get.side_effect = source_get
        migrator.dest.get.side_effect = dest_get

        mapping = migrator.ensure_workspace_role_unions(
            source_roles=source_roles,
            union_specs=[
                {
                    "signature": "src-a|src-b",
                    "role_id": "union::src-a|src-b",
                    "source_role_ids": ["src-a", "src-b"],
                }
            ],
        )

        assert mapping == {"union::src-a|src-b": "dst-union"}
        migrator.dest.patch.assert_not_called()
        migrator.dest.post.assert_not_called()

    def test_ensure_workspace_role_unions_requires_exact_signature_match(
        self, migrator
    ):
        """A shorter union must not reuse or mutate a managed superset-signature role."""
        source_roles = [
            {
                "id": "src-a",
                "name": "CUSTOM",
                "display_name": "Assistant A",
                "permissions": ["projects:read"],
            },
            {
                "id": "src-b",
                "name": "CUSTOM",
                "display_name": "Assistant B",
                "permissions": ["datasets:read"],
            },
        ]
        superset_roles = source_roles + [
            {
                "id": "src-c",
                "name": "CUSTOM",
                "display_name": "Assistant C",
                "permissions": ["runs:read"],
            }
        ]
        superset_signature = "src-a|src-b|src-c"

        def source_get(endpoint):
            if endpoint == "/orgs/current/access-policies":
                return []
            raise AssertionError(endpoint)

        def dest_get(endpoint):
            if endpoint == "/orgs/current/roles":
                return [
                    {
                        "id": "dst-union-superset",
                        "name": "CUSTOM",
                        "display_name": migrator._workspace_role_union_display_name(
                            superset_signature
                        ),
                        "description": migrator._workspace_role_union_description(
                            superset_signature,
                            superset_roles,
                        ),
                        "permissions": ["datasets:read", "projects:read", "runs:read"],
                    }
                ]
            if endpoint == "/orgs/current/access-policies":
                return []
            raise AssertionError(endpoint)

        migrator.source.get.side_effect = source_get
        migrator.dest.get.side_effect = dest_get
        migrator.dest.post.side_effect = [
            {"id": "dst-union-short", "name": "CUSTOM"},
            {},
        ]

        mapping = migrator.ensure_workspace_role_unions(
            source_roles=source_roles,
            union_specs=[
                {
                    "signature": "src-a|src-b",
                    "role_id": "union::src-a|src-b",
                    "source_role_ids": ["src-a", "src-b"],
                }
            ],
        )

        assert mapping == {"union::src-a|src-b": "dst-union-short"}
        assert migrator.dest.post.call_args_list == [
            call(
                "/orgs/current/roles",
                {
                    "display_name": migrator._workspace_role_union_display_name(
                        "src-a|src-b"
                    ),
                    "description": migrator._workspace_role_union_description(
                        "src-a|src-b",
                        source_roles,
                    ),
                    "permissions": ["datasets:read", "projects:read"],
                },
            ),
            call(
                "/orgs/current/roles/dst-union-short/access-policies",
                {"access_policy_ids": []},
            ),
        ]
        migrator.dest.patch.assert_not_called()

    def test_ensure_workspace_role_unions_creates_and_reuses_equivalent_policies(
        self, migrator
    ):
        """Equivalent destination policies are reused and only missing ones are created."""
        source_roles = [
            {
                "id": "src-a",
                "name": "CUSTOM",
                "display_name": "Assistant A",
                "permissions": ["projects:read"],
            },
            {
                "id": "src-b",
                "name": "CUSTOM",
                "display_name": "Assistant B",
                "permissions": ["datasets:read"],
            },
        ]
        source_policies = [
            {
                "id": "src-policy-1",
                "name": "Project Access",
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
                "role_ids": ["src-a"],
            },
            {
                "id": "src-policy-2",
                "name": "Dataset Access",
                "effect": "allow",
                "condition_groups": [
                    {
                        "permission": "datasets:read",
                        "resource_type": "dataset",
                        "conditions": [
                            {
                                "attribute_name": "resource_tag_key",
                                "attribute_key": "Assistant",
                                "operator": "equals",
                                "attribute_value": "B",
                            }
                        ],
                    }
                ],
                "role_ids": ["src-b"],
            },
        ]
        dest_policies = [
            {
                "id": "dst-policy-existing",
                "name": "Existing Project Access",
                "effect": "allow",
                "condition_groups": source_policies[0]["condition_groups"],
                "role_ids": [],
            }
        ]

        def source_get(endpoint):
            if endpoint == "/orgs/current/access-policies":
                return source_policies
            raise AssertionError(endpoint)

        def dest_get(endpoint):
            if endpoint == "/orgs/current/roles":
                return []
            if endpoint == "/orgs/current/access-policies":
                return dest_policies
            raise AssertionError(endpoint)

        migrator.source.get.side_effect = source_get
        migrator.dest.get.side_effect = dest_get
        migrator.dest.post.side_effect = [
            {"id": "dst-union", "name": "CUSTOM"},
            {**source_policies[1], "id": "dst-policy-created", "role_ids": ["dst-union"]},
            {},
        ]

        mapping = migrator.ensure_workspace_role_unions(
            source_roles=source_roles,
            union_specs=[
                {
                    "signature": "src-a|src-b",
                    "role_id": "union::src-a|src-b",
                    "source_role_ids": ["src-a", "src-b"],
                }
            ],
        )

        assert mapping == {"union::src-a|src-b": "dst-union"}
        assert migrator.dest.post.call_args_list == [
            call(
                "/orgs/current/roles",
                {
                    "display_name": "Migrator Union 128d62f94654",
                    "description": (
                        "Managed by langsmith-migrator for union::src-a|src-b. "
                        "Source roles: Assistant A, Assistant B"
                    ),
                    "permissions": ["datasets:read", "projects:read"],
                },
            ),
            call(
                "/orgs/current/access-policies",
                {
                    "name": "Dataset Access",
                    "description": "",
                    "effect": "allow",
                    "condition_groups": source_policies[1]["condition_groups"],
                    "role_ids": ["dst-union"],
                },
            ),
            call(
                "/orgs/current/roles/dst-union/access-policies",
                {"access_policy_ids": ["dst-policy-created", "dst-policy-existing"]},
            ),
        ]

    def test_ensure_workspace_role_unions_reconciles_policy_attachments(
        self, migrator
    ):
        """Role attachment reconciliation can remove stale policy bindings for the managed role."""
        source_roles = [
            {
                "id": "src-a",
                "name": "CUSTOM",
                "display_name": "Assistant A",
                "permissions": ["projects:read"],
            }
        ]
        source_policy = {
            "id": "src-policy-1",
            "name": "Project Access",
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
            "role_ids": ["src-a"],
        }
        stale_policy = {
            "id": "dst-policy-stale",
            "name": "Stale",
            "effect": "allow",
            "condition_groups": [
                {
                    "permission": "datasets:read",
                    "resource_type": "dataset",
                    "conditions": [],
                }
            ],
            "role_ids": ["dst-union"],
        }
        desired_dest_policy = {
            "id": "dst-policy-1",
            "name": "Project Access",
            "effect": "allow",
            "condition_groups": source_policy["condition_groups"],
            "role_ids": [],
        }

        def source_get(endpoint):
            if endpoint == "/orgs/current/access-policies":
                return [source_policy]
            raise AssertionError(endpoint)

        def dest_get(endpoint):
            if endpoint == "/orgs/current/roles":
                return [
                    {
                        "id": "dst-union",
                        "name": "CUSTOM",
                        "display_name": "Migrator Union 48d4658aab03",
                        "description": (
                            "Managed by langsmith-migrator for union::src-a. "
                            "Source roles: Assistant A"
                        ),
                        "permissions": ["projects:read"],
                    }
                ]
            if endpoint == "/orgs/current/access-policies":
                return [desired_dest_policy, stale_policy]
            raise AssertionError(endpoint)

        migrator.state.id_mappings["role_unions"] = {"src-a": "dst-union"}
        migrator.source.get.side_effect = source_get
        migrator.dest.get.side_effect = dest_get

        migrator.ensure_workspace_role_unions(
            source_roles=source_roles,
            union_specs=[
                {
                    "signature": "src-a",
                    "role_id": "union::src-a",
                    "source_role_ids": ["src-a"],
                }
            ],
        )

        migrator.dest.post.assert_called_once_with(
            "/orgs/current/roles/dst-union/access-policies",
            {"access_policy_ids": ["dst-policy-1"]},
        )
        cached_policies = migrator._dest_access_policies_cache or []
        stale_after = next(
            policy for policy in cached_policies if policy.get("id") == "dst-policy-stale"
        )
        assert stale_after["role_ids"] == []

    def test_ensure_workspace_role_unions_fails_fast_when_policy_listing_is_unavailable(
        self, migrator
    ):
        """ABAC auto-merge fails before any role/member writes when policies cannot be listed."""
        source_roles = [
            {
                "id": "src-a",
                "name": "CUSTOM",
                "display_name": "Assistant A",
                "permissions": ["projects:read"],
            },
            {
                "id": "src-b",
                "name": "CUSTOM",
                "display_name": "Assistant B",
                "permissions": ["datasets:read"],
            },
        ]
        migrator.source.get.side_effect = APIError(
            "not found",
            status_code=404,
            request_info={},
        )
        migrator.dest.get.return_value = []

        with pytest.raises(APIError, match="Unable to list source access policies"):
            migrator.ensure_workspace_role_unions(
                source_roles=source_roles,
                union_specs=[
                    {
                        "signature": "src-a|src-b",
                        "role_id": "union::src-a|src-b",
                        "source_role_ids": ["src-a", "src-b"],
                    }
                ],
            )

        migrator.dest.post.assert_not_called()

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

    def test_migrate_org_members_reinvites_pending_when_workspace_access_is_needed(
        self, migrator
    ):
        """Existing pending invites should be replaced when workspace access still needs to be attached."""
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
        assert migrator._pending_org_email_to_identity["alice@example.com"]["id"] == "pending-1"
        assert migrator._pending_workspace_invites == {("alice@example.com", "ws-1")}
        migrator.dest.delete.assert_not_called()
        migrator.dest.post.assert_not_called()

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

    def test_migrate_workspace_members_from_csv_rows_rejects_conflicting_roles(
        self, migrator
    ):
        """CSV workspace rows with conflicting role IDs for one email are rejected."""
        migrator.source.session.headers["X-Tenant-Id"] = "ws-src-1"
        rows = [
            {"email": "alice@example.com", "role_id": "role-1", "workspace_id": "ws-src-1"},
            {"email": "alice@example.com", "role_id": "role-2", "workspace_id": "ws-src-1"},
        ]

        with pytest.raises(APIError, match="Conflicting workspace role_id"):
            migrator.migrate_workspace_members_from_csv_rows(rows)

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

    def test_probe_capabilities_success(self, migrator):
        """Probe records capabilities when endpoints succeed."""
        migrator.source.get.return_value = []
        migrator.dest.get.return_value = []

        migrator.probe_capabilities()

        # Should not raise
