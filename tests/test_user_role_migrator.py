"""Unit tests for UserRoleMigrator."""

import pytest
from unittest.mock import Mock
from langsmith_migrator.core.migrators import UserRoleMigrator
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
        source.session.headers = {}

        dest = Mock(spec=EnhancedAPIClient)
        dest.base_url = "https://dest.api.test.com"
        dest.session = Mock()
        dest.session.headers = {}

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

    # ── Phase 3: Workspace member migration ──

    def test_migrate_workspace_members_add(self, migrator):
        """Users not in workspace are added."""
        migrator._role_id_map = {"src-ws-role": "dst-ws-role"}
        migrator._dest_email_to_identity = {
            "alice@example.com": {"id": "dst-org-identity-1"},
        }
        migrator._dest_members_loaded = True
        migrator.source.get_paginated.return_value = iter([
            {"id": "src-ws-m1", "email": "alice@example.com", "role_id": "src-ws-role"},
        ])
        migrator.dest.get_paginated.return_value = iter([])
        migrator.dest.post.return_value = {"id": "dst-ws-identity-1"}

        m, s, f = migrator.migrate_workspace_members()

        assert m == 1
        migrator.dest.post.assert_called_once_with(
            "/tenants/current/members",
            {"org_identity_id": "dst-org-identity-1", "role_id": "dst-ws-role"},
        )

    def test_migrate_workspace_members_update_role(self, migrator):
        """Workspace members with wrong role get updated."""
        migrator._role_id_map = {"src-ws-role": "dst-ws-role-new"}
        migrator._dest_members_loaded = True
        migrator.source.get_paginated.return_value = iter([
            {"id": "src-ws-m1", "email": "alice@example.com", "role_id": "src-ws-role"},
        ])
        migrator.dest.get_paginated.return_value = iter([
            {"id": "dst-ws-m1", "email": "alice@example.com", "role_id": "dst-ws-role-old"},
        ])

        m, s, f = migrator.migrate_workspace_members()

        assert m == 1
        migrator.dest.patch.assert_called_once_with(
            "/tenants/current/members/dst-ws-m1",
            {"role_id": "dst-ws-role-new"},
        )

    def test_migrate_workspace_members_not_in_org(self, migrator):
        """User not an org member on dest is recorded as failed."""
        migrator._role_id_map = {"src-ws-role": "dst-ws-role"}
        migrator._dest_email_to_identity = {}
        migrator._dest_members_loaded = True
        migrator.source.get_paginated.return_value = iter([
            {"id": "src-ws-m1", "email": "bob@example.com", "role_id": "src-ws-role"},
        ])
        migrator.dest.get_paginated.return_value = iter([])

        m, s, f = migrator.migrate_workspace_members()

        assert f == 1
        assert m == 0

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

    # ── Capability probing ──

    def test_probe_capabilities_success(self, migrator):
        """Probe records capabilities when endpoints succeed."""
        migrator.source.get.return_value = []
        migrator.dest.get.return_value = []

        migrator.probe_capabilities()

        # Should not raise
