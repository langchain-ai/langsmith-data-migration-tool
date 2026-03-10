"""CLI tests for roles and users commands."""

import pytest
from click.testing import CliRunner
from unittest.mock import patch, Mock, MagicMock
from langsmith_migrator.cli.main import cli


# ── Shared helpers ──

CLI_BASE_ARGS = [
    '--source-key', 'sk',
    '--dest-key', 'dk',
    '--source-url', 'http://source',
    '--dest-url', 'http://dest',
    '--no-ssl',
]

SAMPLE_CUSTOM_ROLES = [
    {"id": "r1", "name": "Annotator", "is_system": False, "description": "Can annotate", "permissions": ["read", "annotate"]},
    {"id": "r2", "name": "Reviewer", "is_system": False, "description": "Can review", "permissions": ["read", "review"]},
]

SAMPLE_ORG_MEMBERS = [
    {"id": "u1", "email": "alice@example.com", "full_name": "Alice", "role_id": "src-admin", "status": "active"},
    {"id": "u2", "email": "bob@example.com", "full_name": "Bob", "role_id": "src-editor", "status": "active"},
]


def _mock_orchestrator():
    """Create a mock orchestrator with passing connections."""
    orch = Mock()
    orch.test_connections_detailed.return_value = (True, True, None, None)
    orch.test_connections.return_value = True
    orch.source_client = Mock()
    orch.dest_client = Mock()
    orch.config = Mock()
    orch.config.migration.dry_run = False
    return orch


# ═══════════════════════════════════════════════════════════════════════
# ROLES COMMAND
# ═══════════════════════════════════════════════════════════════════════

class TestRolesCommand:

    @patch('langsmith_migrator.cli.main.select_items')
    @patch('langsmith_migrator.cli.main.RoleMigrator')
    @patch('langsmith_migrator.cli.main.MigrationOrchestrator')
    def test_roles_migrate_success(self, MockOrch, MockRoleMigrator, mock_select):
        """Happy path: select and migrate custom roles."""
        orch = _mock_orchestrator()
        MockOrch.return_value = orch

        migrator = MockRoleMigrator.return_value
        migrator.list_custom_roles.return_value = SAMPLE_CUSTOM_ROLES
        migrator.get_dest_custom_roles_by_name.return_value = {}
        migrator.create_custom_role.return_value = "new-id"

        mock_select.return_value = SAMPLE_CUSTOM_ROLES

        runner = CliRunner()
        result = runner.invoke(cli, CLI_BASE_ARGS + ['roles'])

        assert result.exit_code == 0
        assert "Fetching custom roles from source" in result.output
        assert "Migrating 2 custom role(s)" in result.output
        assert "2 migrated, 0 failed" in result.output
        assert migrator.create_custom_role.call_count == 2
        orch.cleanup.assert_called_once()

    @patch('langsmith_migrator.cli.main.RoleMigrator')
    @patch('langsmith_migrator.cli.main.MigrationOrchestrator')
    def test_roles_no_custom_roles(self, MockOrch, MockRoleMigrator):
        """No custom roles found on source."""
        orch = _mock_orchestrator()
        MockOrch.return_value = orch

        migrator = MockRoleMigrator.return_value
        migrator.list_custom_roles.return_value = []

        runner = CliRunner()
        result = runner.invoke(cli, CLI_BASE_ARGS + ['roles'])

        assert result.exit_code == 0
        assert "No custom roles found" in result.output
        orch.cleanup.assert_called_once()

    @patch('langsmith_migrator.cli.main.select_items')
    @patch('langsmith_migrator.cli.main.RoleMigrator')
    @patch('langsmith_migrator.cli.main.MigrationOrchestrator')
    def test_roles_none_selected(self, MockOrch, MockRoleMigrator, mock_select):
        """User selects no roles in TUI."""
        orch = _mock_orchestrator()
        MockOrch.return_value = orch

        migrator = MockRoleMigrator.return_value
        migrator.list_custom_roles.return_value = SAMPLE_CUSTOM_ROLES

        mock_select.return_value = []

        runner = CliRunner()
        result = runner.invoke(cli, CLI_BASE_ARGS + ['roles'])

        assert result.exit_code == 0
        assert "No roles selected" in result.output
        migrator.create_custom_role.assert_not_called()

    @patch('langsmith_migrator.cli.main.select_items')
    @patch('langsmith_migrator.cli.main.RoleMigrator')
    @patch('langsmith_migrator.cli.main.MigrationOrchestrator')
    def test_roles_partial_failure(self, MockOrch, MockRoleMigrator, mock_select):
        """One role succeeds, one fails."""
        orch = _mock_orchestrator()
        MockOrch.return_value = orch

        migrator = MockRoleMigrator.return_value
        migrator.list_custom_roles.return_value = SAMPLE_CUSTOM_ROLES
        migrator.get_dest_custom_roles_by_name.return_value = {}
        migrator.create_custom_role.side_effect = ["new-id", Exception("API error")]

        mock_select.return_value = SAMPLE_CUSTOM_ROLES

        runner = CliRunner()
        result = runner.invoke(cli, CLI_BASE_ARGS + ['roles'])

        assert result.exit_code == 0
        assert "1 migrated, 1 failed" in result.output

    @patch('langsmith_migrator.cli.main.RoleMigrator')
    @patch('langsmith_migrator.cli.main.MigrationOrchestrator')
    def test_roles_connection_failure(self, MockOrch, MockRoleMigrator):
        """Source connection fails."""
        orch = Mock()
        orch.test_connections_detailed.return_value = (False, True, "timeout", None)
        MockOrch.return_value = orch

        runner = CliRunner()
        result = runner.invoke(cli, CLI_BASE_ARGS + ['roles'])

        assert result.exit_code == 0
        assert "Source connection failed" in result.output
        MockRoleMigrator.assert_not_called()

    @patch('langsmith_migrator.cli.main.select_items')
    @patch('langsmith_migrator.cli.main.RoleMigrator')
    @patch('langsmith_migrator.cli.main.MigrationOrchestrator')
    def test_roles_dest_roles_cache_used(self, MockOrch, MockRoleMigrator, mock_select):
        """Verify get_dest_custom_roles_by_name is called once and passed to create_custom_role."""
        orch = _mock_orchestrator()
        MockOrch.return_value = orch

        migrator = MockRoleMigrator.return_value
        migrator.list_custom_roles.return_value = [SAMPLE_CUSTOM_ROLES[0]]
        dest_cache = {"ExistingRole": {"id": "x", "name": "ExistingRole"}}
        migrator.get_dest_custom_roles_by_name.return_value = dest_cache
        migrator.create_custom_role.return_value = "new-id"

        mock_select.return_value = [SAMPLE_CUSTOM_ROLES[0]]

        runner = CliRunner()
        result = runner.invoke(cli, CLI_BASE_ARGS + ['roles'])

        assert result.exit_code == 0
        migrator.get_dest_custom_roles_by_name.assert_called_once()
        migrator.create_custom_role.assert_called_once_with(
            SAMPLE_CUSTOM_ROLES[0], dest_cache
        )


# ═══════════════════════════════════════════════════════════════════════
# USERS COMMAND
# ═══════════════════════════════════════════════════════════════════════

class TestUsersCommand:

    @patch('langsmith_migrator.cli.main.Confirm')
    @patch('langsmith_migrator.cli.main.select_items')
    @patch('langsmith_migrator.cli.main.UserMigrator')
    @patch('langsmith_migrator.cli.main.RoleMigrator')
    @patch('langsmith_migrator.cli.main.MigrationOrchestrator')
    def test_users_org_members_migrate(self, MockOrch, MockRoleMigrator,
                                        MockUserMigrator, mock_select, mock_confirm):
        """Happy path: build role map, select members, invite."""
        orch = _mock_orchestrator()
        MockOrch.return_value = orch

        role_migrator = MockRoleMigrator.return_value
        role_migrator.list_roles.return_value = [
            {"id": "src-admin", "name": "Admin", "is_system": True},
        ]
        role_migrator.build_role_id_map.return_value = {"src-admin": "dst-admin"}

        user_migrator = MockUserMigrator.return_value
        user_migrator.list_org_members.return_value = SAMPLE_ORG_MEMBERS
        user_migrator.migrate_org_members.return_value = {"u1": "d1", "u2": "d2"}

        mock_select.return_value = SAMPLE_ORG_MEMBERS
        mock_confirm.ask.return_value = True

        runner = CliRunner()
        result = runner.invoke(cli, CLI_BASE_ARGS + ['users', '--skip-workspace-members'])

        assert result.exit_code == 0
        assert "Building role mapping" in result.output
        assert "Mapped 1 role(s)" in result.output
        assert "2 migrated, 0 failed" in result.output
        user_migrator.migrate_org_members.assert_called_once_with(SAMPLE_ORG_MEMBERS)
        orch.cleanup.assert_called_once()

    @patch('langsmith_migrator.cli.main.UserMigrator')
    @patch('langsmith_migrator.cli.main.RoleMigrator')
    @patch('langsmith_migrator.cli.main.MigrationOrchestrator')
    def test_users_no_org_members(self, MockOrch, MockRoleMigrator, MockUserMigrator):
        """No org members found on source."""
        orch = _mock_orchestrator()
        MockOrch.return_value = orch

        role_migrator = MockRoleMigrator.return_value
        role_migrator.list_roles.return_value = []
        role_migrator.build_role_id_map.return_value = {}

        user_migrator = MockUserMigrator.return_value
        user_migrator.list_org_members.return_value = []

        runner = CliRunner()
        result = runner.invoke(cli, CLI_BASE_ARGS + ['users', '--skip-workspace-members'])

        assert result.exit_code == 0
        assert "none found" in result.output
        user_migrator.migrate_org_members.assert_not_called()

    @patch('langsmith_migrator.cli.main.Confirm')
    @patch('langsmith_migrator.cli.main.select_items')
    @patch('langsmith_migrator.cli.main.UserMigrator')
    @patch('langsmith_migrator.cli.main.RoleMigrator')
    @patch('langsmith_migrator.cli.main.MigrationOrchestrator')
    def test_users_confirm_declined(self, MockOrch, MockRoleMigrator,
                                     MockUserMigrator, mock_select, mock_confirm):
        """User declines the invite confirmation prompt."""
        orch = _mock_orchestrator()
        MockOrch.return_value = orch

        role_migrator = MockRoleMigrator.return_value
        role_migrator.list_roles.return_value = []
        role_migrator.build_role_id_map.return_value = {}

        user_migrator = MockUserMigrator.return_value
        user_migrator.list_org_members.return_value = SAMPLE_ORG_MEMBERS

        mock_select.return_value = SAMPLE_ORG_MEMBERS
        mock_confirm.ask.return_value = False

        runner = CliRunner()
        result = runner.invoke(cli, CLI_BASE_ARGS + ['users', '--skip-workspace-members'])

        assert result.exit_code == 0
        assert "Cancelled" in result.output
        user_migrator.migrate_org_members.assert_not_called()

    @patch('langsmith_migrator.cli.main.select_items')
    @patch('langsmith_migrator.cli.main.UserMigrator')
    @patch('langsmith_migrator.cli.main.RoleMigrator')
    @patch('langsmith_migrator.cli.main.MigrationOrchestrator')
    def test_users_none_selected(self, MockOrch, MockRoleMigrator,
                                  MockUserMigrator, mock_select):
        """User selects no members in TUI."""
        orch = _mock_orchestrator()
        MockOrch.return_value = orch

        role_migrator = MockRoleMigrator.return_value
        role_migrator.list_roles.return_value = []
        role_migrator.build_role_id_map.return_value = {}

        user_migrator = MockUserMigrator.return_value
        user_migrator.list_org_members.return_value = SAMPLE_ORG_MEMBERS

        mock_select.return_value = []

        runner = CliRunner()
        result = runner.invoke(cli, CLI_BASE_ARGS + ['users', '--skip-workspace-members'])

        assert result.exit_code == 0
        assert "No members selected" in result.output
        user_migrator.migrate_org_members.assert_not_called()

    @patch('langsmith_migrator.cli.main.RoleMigrator')
    @patch('langsmith_migrator.cli.main.MigrationOrchestrator')
    def test_users_unmapped_roles_warning(self, MockOrch, MockRoleMigrator):
        """Warns about unmapped custom roles."""
        orch = _mock_orchestrator()
        MockOrch.return_value = orch

        role_migrator = MockRoleMigrator.return_value
        # Source has a custom role not on dest
        role_migrator.list_roles.return_value = [
            {"id": "src-admin", "name": "Admin", "is_system": True},
            {"id": "src-custom", "name": "Annotator", "is_system": False},
        ]
        role_migrator.build_role_id_map.return_value = {"src-admin": "dst-admin"}
        # No mapping for src-custom

        # Patch UserMigrator to return empty members so we exit early
        with patch('langsmith_migrator.cli.main.UserMigrator') as MockUserMigrator:
            user_migrator = MockUserMigrator.return_value
            user_migrator.list_org_members.return_value = []

            runner = CliRunner()
            result = runner.invoke(cli, CLI_BASE_ARGS + ['users', '--skip-workspace-members'])

        assert result.exit_code == 0
        assert "1 custom role(s) unmapped: Annotator" in result.output
        assert "Run 'roles' command first" in result.output

    @patch('langsmith_migrator.cli.main._resolve_workspaces')
    @patch('langsmith_migrator.cli.main.Confirm')
    @patch('langsmith_migrator.cli.main.select_items')
    @patch('langsmith_migrator.cli.main.UserMigrator')
    @patch('langsmith_migrator.cli.main.RoleMigrator')
    @patch('langsmith_migrator.cli.main.MigrationOrchestrator')
    def test_users_with_workspace_members(self, MockOrch, MockRoleMigrator,
                                           MockUserMigrator, mock_select,
                                           mock_confirm, mock_resolve_ws):
        """Full flow with workspace membership migration."""
        orch = _mock_orchestrator()
        MockOrch.return_value = orch

        role_migrator = MockRoleMigrator.return_value
        role_migrator.list_roles.return_value = []
        role_migrator.build_role_id_map.return_value = {"src-admin": "dst-admin"}

        user_migrator = MockUserMigrator.return_value
        user_migrator.list_org_members.return_value = SAMPLE_ORG_MEMBERS
        user_migrator.migrate_org_members.return_value = {"u1": "d1", "u2": "d2"}
        user_migrator.migrate_workspace_members.return_value = 2

        mock_select.return_value = SAMPLE_ORG_MEMBERS
        mock_confirm.ask.return_value = True

        # Simulate workspace resolution returning one pair
        ws_result = Mock()
        ws_result.workspace_mapping = {"ws-src": "ws-dst"}
        mock_resolve_ws.return_value = ws_result

        runner = CliRunner()
        result = runner.invoke(cli, CLI_BASE_ARGS + ['users'])

        assert result.exit_code == 0
        assert "2 migrated, 0 failed" in result.output
        assert "Workspace memberships (1 pair(s))" in result.output
        assert "Workspace members: 2 migrated" in result.output
        user_migrator.migrate_workspace_members.assert_called_once_with({"u1": "d1", "u2": "d2"})

    @patch('langsmith_migrator.cli.main.Confirm')
    @patch('langsmith_migrator.cli.main.select_items')
    @patch('langsmith_migrator.cli.main.UserMigrator')
    @patch('langsmith_migrator.cli.main.RoleMigrator')
    @patch('langsmith_migrator.cli.main.MigrationOrchestrator')
    def test_users_dry_run_skips_confirm(self, MockOrch, MockRoleMigrator,
                                          MockUserMigrator, mock_select, mock_confirm):
        """In dry-run mode, the invite confirmation prompt is skipped."""
        orch = _mock_orchestrator()
        orch.config.migration.dry_run = True
        MockOrch.return_value = orch

        role_migrator = MockRoleMigrator.return_value
        role_migrator.list_roles.return_value = []
        role_migrator.build_role_id_map.return_value = {}

        user_migrator = MockUserMigrator.return_value
        user_migrator.list_org_members.return_value = SAMPLE_ORG_MEMBERS
        user_migrator.migrate_org_members.return_value = {"u1": "d1"}

        mock_select.return_value = SAMPLE_ORG_MEMBERS

        runner = CliRunner()
        result = runner.invoke(cli, CLI_BASE_ARGS + ['--dry-run', 'users', '--skip-workspace-members'])

        assert result.exit_code == 0
        # Confirm.ask should NOT have been called (dry-run bypasses it)
        mock_confirm.ask.assert_not_called()
        user_migrator.migrate_org_members.assert_called_once()

    @patch('langsmith_migrator.cli.main.UserMigrator')
    @patch('langsmith_migrator.cli.main.RoleMigrator')
    @patch('langsmith_migrator.cli.main.MigrationOrchestrator')
    def test_users_connection_failure(self, MockOrch, MockRoleMigrator, MockUserMigrator):
        """Dest connection fails."""
        orch = Mock()
        orch.test_connections_detailed.return_value = (True, False, None, "refused")
        MockOrch.return_value = orch

        runner = CliRunner()
        result = runner.invoke(cli, CLI_BASE_ARGS + ['users', '--skip-workspace-members'])

        assert result.exit_code == 0
        assert "Destination connection failed" in result.output
        MockRoleMigrator.assert_not_called()


# ═══════════════════════════════════════════════════════════════════════
# MIGRATE_ALL — roles/users integration
# ═══════════════════════════════════════════════════════════════════════

class TestMigrateAllRolesUsers:

    def _base_patches(self):
        """Return a dict of patch targets for migrate_all."""
        return {
            'orch': patch('langsmith_migrator.cli.main.MigrationOrchestrator'),
            'role_migrator': patch('langsmith_migrator.cli.main.RoleMigrator'),
            'user_migrator': patch('langsmith_migrator.cli.main.UserMigrator'),
            'confirm': patch('langsmith_migrator.cli.main.Confirm'),
            'resolve_ws': patch('langsmith_migrator.cli.main._resolve_workspaces'),
            'migrate_ws': patch('langsmith_migrator.cli.main._migrate_all_for_workspace'),
        }

    def test_migrate_all_skip_roles_and_users(self):
        """--skip-custom-roles --skip-users skips both org-level steps."""
        patches = self._base_patches()
        with patches['orch'] as MockOrch, \
             patches['role_migrator'] as MockRoleMigrator, \
             patches['user_migrator'] as MockUserMigrator, \
             patches['confirm'] as _, \
             patches['resolve_ws'] as mock_resolve_ws, \
             patches['migrate_ws'] as mock_migrate_ws:

            orch = _mock_orchestrator()
            MockOrch.return_value = orch
            mock_resolve_ws.return_value = None  # No workspace scoping

            # RoleMigrator is still instantiated for role_id_map, but no custom roles migrated
            role_migrator = MockRoleMigrator.return_value
            role_migrator.build_role_id_map.return_value = {}

            runner = CliRunner()
            result = runner.invoke(cli, CLI_BASE_ARGS + [
                'migrate-all',
                '--skip-custom-roles', '--skip-users',
                '--skip-datasets', '--skip-experiments',
                '--skip-prompts', '--skip-queues', '--skip-rules',
            ])

            assert result.exit_code == 0
            assert "Skipping custom roles" in result.output
            assert "Skipping users" in result.output
            role_migrator.list_custom_roles.assert_not_called()
            MockUserMigrator.assert_not_called()

    def test_migrate_all_roles_and_users_flow(self):
        """migrate_all runs roles then users as org-level steps."""
        patches = self._base_patches()
        with patches['orch'] as MockOrch, \
             patches['role_migrator'] as MockRoleMigrator, \
             patches['user_migrator'] as MockUserMigrator, \
             patches['confirm'] as mock_confirm, \
             patches['resolve_ws'] as mock_resolve_ws, \
             patches['migrate_ws'] as mock_migrate_ws:

            orch = _mock_orchestrator()
            MockOrch.return_value = orch
            mock_resolve_ws.return_value = None
            mock_confirm.ask.return_value = True

            role_migrator = MockRoleMigrator.return_value
            role_migrator.list_custom_roles.return_value = SAMPLE_CUSTOM_ROLES
            role_migrator.get_dest_custom_roles_by_name.return_value = {}
            role_migrator.create_custom_role.return_value = "new-id"
            role_migrator.build_role_id_map.return_value = {"src-admin": "dst-admin"}

            user_migrator = MockUserMigrator.return_value
            user_migrator.list_org_members.return_value = SAMPLE_ORG_MEMBERS
            user_migrator.migrate_org_members.return_value = {"u1": "d1"}

            runner = CliRunner()
            result = runner.invoke(cli, CLI_BASE_ARGS + [
                'migrate-all',
                '--skip-datasets', '--skip-experiments',
                '--skip-prompts', '--skip-queues', '--skip-rules',
            ])

            assert result.exit_code == 0
            assert "Org Step: Custom Roles" in result.output
            assert "Org Step: Members" in result.output
            # Roles migrated
            assert role_migrator.create_custom_role.call_count == 2
            # Users migrated
            user_migrator.migrate_org_members.assert_called_once()

    def test_migrate_all_single_role_migrator_instance(self):
        """Verify only one RoleMigrator is instantiated."""
        patches = self._base_patches()
        with patches['orch'] as MockOrch, \
             patches['role_migrator'] as MockRoleMigrator, \
             patches['user_migrator'] as MockUserMigrator, \
             patches['confirm'] as mock_confirm, \
             patches['resolve_ws'] as mock_resolve_ws, \
             patches['migrate_ws'] as mock_migrate_ws:

            orch = _mock_orchestrator()
            MockOrch.return_value = orch
            mock_resolve_ws.return_value = None
            mock_confirm.ask.return_value = False  # Decline all confirmations

            role_migrator = MockRoleMigrator.return_value
            role_migrator.list_custom_roles.return_value = []
            role_migrator.build_role_id_map.return_value = {}

            user_migrator = MockUserMigrator.return_value
            user_migrator.list_org_members.return_value = []

            runner = CliRunner()
            result = runner.invoke(cli, CLI_BASE_ARGS + [
                'migrate-all',
                '--skip-datasets', '--skip-experiments',
                '--skip-prompts', '--skip-queues', '--skip-rules',
            ])

            assert result.exit_code == 0
            # RoleMigrator should be constructed exactly once
            assert MockRoleMigrator.call_count == 1

    def test_migrate_all_workspace_member_migration(self):
        """Workspace members are migrated inside the workspace loop."""
        patches = self._base_patches()
        with patches['orch'] as MockOrch, \
             patches['role_migrator'] as MockRoleMigrator, \
             patches['user_migrator'] as MockUserMigrator, \
             patches['confirm'] as mock_confirm, \
             patches['resolve_ws'] as mock_resolve_ws, \
             patches['migrate_ws'] as mock_migrate_ws:

            orch = _mock_orchestrator()
            MockOrch.return_value = orch
            mock_confirm.ask.return_value = True

            # Simulate workspace result with one pair
            ws_result = Mock()
            ws_result.workspace_mapping = {"ws-src": "ws-dst"}
            ws_result.project_mappings = {}
            mock_resolve_ws.return_value = ws_result

            role_migrator = MockRoleMigrator.return_value
            role_migrator.list_custom_roles.return_value = []
            role_migrator.build_role_id_map.return_value = {"src-admin": "dst-admin"}

            user_migrator = MockUserMigrator.return_value
            user_migrator.list_org_members.return_value = SAMPLE_ORG_MEMBERS
            user_migrator.migrate_org_members.return_value = {"u1": "d1", "u2": "d2"}
            user_migrator.migrate_workspace_members.return_value = 2

            # Need to also patch _enter_workspace_pair
            with patch('langsmith_migrator.cli.main._enter_workspace_pair') as mock_enter:
                mock_enter.return_value = ({}, [], [])

                runner = CliRunner()
                result = runner.invoke(cli, CLI_BASE_ARGS + [
                    'migrate-all',
                    '--skip-datasets', '--skip-experiments',
                    '--skip-prompts', '--skip-queues', '--skip-rules',
                ])

            assert result.exit_code == 0
            assert "Workspace members: 2 migrated" in result.output
            # Same user_migrator instance reused for workspace loop
            user_migrator.migrate_workspace_members.assert_called_once_with(
                {"u1": "d1", "u2": "d2"}
            )

    def test_migrate_all_no_ws_members_when_no_org_map(self):
        """Workspace member migration is skipped when org_identity_map is empty."""
        patches = self._base_patches()
        with patches['orch'] as MockOrch, \
             patches['role_migrator'] as MockRoleMigrator, \
             patches['user_migrator'] as MockUserMigrator, \
             patches['confirm'] as mock_confirm, \
             patches['resolve_ws'] as mock_resolve_ws, \
             patches['migrate_ws'] as mock_migrate_ws:

            orch = _mock_orchestrator()
            MockOrch.return_value = orch
            mock_confirm.ask.return_value = False  # Decline member migration

            ws_result = Mock()
            ws_result.workspace_mapping = {"ws-src": "ws-dst"}
            ws_result.project_mappings = {}
            mock_resolve_ws.return_value = ws_result

            role_migrator = MockRoleMigrator.return_value
            role_migrator.list_custom_roles.return_value = []
            role_migrator.build_role_id_map.return_value = {}

            user_migrator = MockUserMigrator.return_value
            user_migrator.list_org_members.return_value = SAMPLE_ORG_MEMBERS
            # User declined, so migrate_org_members won't be called
            # org_identity_map stays {}

            with patch('langsmith_migrator.cli.main._enter_workspace_pair') as mock_enter:
                mock_enter.return_value = ({}, [], [])

                runner = CliRunner()
                result = runner.invoke(cli, CLI_BASE_ARGS + [
                    'migrate-all',
                    '--skip-datasets', '--skip-experiments',
                    '--skip-prompts', '--skip-queues', '--skip-rules',
                ])

            assert result.exit_code == 0
            # workspace members should NOT be migrated since org_identity_map is empty
            user_migrator.migrate_workspace_members.assert_not_called()
