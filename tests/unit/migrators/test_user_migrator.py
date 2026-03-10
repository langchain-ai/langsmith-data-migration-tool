import pytest
from unittest.mock import Mock
from langsmith_migrator.core.migrators.user import UserMigrator


class TestUserMigrator:
    @pytest.fixture
    def role_id_map(self):
        return {
            "src-admin": "dst-admin",
            "src-editor": "dst-editor",
            "src-annotator": "dst-annotator",
        }

    @pytest.fixture
    def migrator(self, role_id_map):
        source_client = Mock()
        dest_client = Mock()
        state = Mock()
        config = Mock()
        config.migration.dry_run = False
        config.migration.skip_existing = False
        config.migration.verbose = False
        return UserMigrator(source_client, dest_client, state, config, role_id_map=role_id_map)

    @pytest.fixture
    def sample_members(self):
        return [
            {
                "id": "u1", "email": "alice@example.com", "full_name": "Alice",
                "role_id": "src-admin", "status": "active",
            },
            {
                "id": "u2", "email": "bob@example.com", "full_name": "Bob",
                "role_id": "src-editor", "status": "active",
            },
        ]

    # ── list_org_members ──

    def test_list_org_members(self, migrator):
        migrator.source.get.side_effect = [
            {"members": [{"id": "u1", "email": "a@b.com"}]},
            {"members": [{"id": "u2", "email": "c@d.com", "status": "pending"}]},
        ]
        result = migrator.list_org_members()
        assert len(result) == 2
        assert result[0]["status"] == "active"
        assert result[1]["status"] == "pending"

    def test_list_org_members_handles_list_response(self, migrator):
        migrator.source.get.side_effect = [
            [{"id": "u1", "email": "a@b.com"}],
            [],
        ]
        result = migrator.list_org_members()
        assert len(result) == 1

    def test_list_org_members_handles_error(self, migrator):
        migrator.source.get.side_effect = Exception("fail")
        result = migrator.list_org_members()
        assert result == []

    # ── list_dest_org_members ──

    def test_list_dest_org_members(self, migrator):
        migrator.dest.get.side_effect = [
            {"members": [{"id": "d1", "email": "a@b.com"}]},
            {"members": []},
        ]
        result = migrator.list_dest_org_members()
        assert "a@b.com" in result
        assert result["a@b.com"]["id"] == "d1"

    # ── invite_or_update_org_member ──

    def test_invite_new_member(self, migrator):
        migrator.dest.post.return_value = {"members": [{"id": "new-1"}]}

        result = migrator.invite_or_update_org_member(
            {"id": "u1", "email": "alice@example.com", "role_id": "src-admin"},
            {},  # no existing members
        )
        assert result == "new-1"
        migrator.dest.post.assert_called_once()
        call_args = migrator.dest.post.call_args
        assert call_args[0][0] == "/orgs/current/members/batch"
        payload = call_args[0][1]
        assert payload["members"][0]["email"] == "alice@example.com"
        assert payload["members"][0]["role_id"] == "dst-admin"

    def test_update_existing_member_role(self, migrator):
        migrator.dest.patch.return_value = {}

        dest_members = {
            "alice@example.com": {"id": "d1", "email": "alice@example.com", "role_id": "dst-editor"},
        }
        result = migrator.invite_or_update_org_member(
            {"id": "u1", "email": "alice@example.com", "role_id": "src-admin"},
            dest_members,
        )
        assert result == "d1"
        migrator.dest.patch.assert_called_once_with(
            "/orgs/current/members/d1",
            {"role_id": "dst-admin"},
        )

    def test_skip_existing_member(self, migrator):
        migrator.config.migration.skip_existing = True
        dest_members = {
            "alice@example.com": {"id": "d1", "email": "alice@example.com", "role_id": "dst-editor"},
        }
        result = migrator.invite_or_update_org_member(
            {"id": "u1", "email": "alice@example.com", "role_id": "src-admin"},
            dest_members,
        )
        assert result == "d1"
        migrator.dest.patch.assert_not_called()

    def test_skip_unmapped_role(self, migrator):
        result = migrator.invite_or_update_org_member(
            {"id": "u1", "email": "alice@example.com", "role_id": "unmapped-role"},
            {},
        )
        assert result is None
        migrator.dest.post.assert_not_called()

    def test_invite_dry_run(self, migrator):
        migrator.config.migration.dry_run = True
        result = migrator.invite_or_update_org_member(
            {"id": "u1", "email": "alice@example.com", "role_id": "src-admin"},
            {},
        )
        assert result.startswith("dry-run-")
        migrator.dest.post.assert_not_called()

    def test_invite_no_email_skipped(self, migrator):
        result = migrator.invite_or_update_org_member(
            {"id": "u1"},
            {},
        )
        assert result is None

    def test_existing_member_already_correct_role(self, migrator):
        dest_members = {
            "alice@example.com": {"id": "d1", "email": "alice@example.com", "role_id": "dst-admin"},
        }
        result = migrator.invite_or_update_org_member(
            {"id": "u1", "email": "alice@example.com", "role_id": "src-admin"},
            dest_members,
        )
        assert result == "d1"
        migrator.dest.patch.assert_not_called()

    # ── migrate_org_members ──

    def test_migrate_org_members(self, migrator, sample_members):
        migrator.dest.get.side_effect = [
            {"members": []},  # active dest
            {"members": []},  # pending dest
        ]
        migrator.dest.post.side_effect = [
            {"members": [{"id": "new-1"}]},
            {"members": [{"id": "new-2"}]},
        ]
        result = migrator.migrate_org_members(sample_members)
        assert len(result) == 2
        assert result["u1"] == "new-1"
        assert result["u2"] == "new-2"

    # ── Workspace member operations ──

    def test_list_workspace_members(self, migrator):
        migrator.source.get.return_value = {"members": [
            {"user_id": "wu1", "role_id": "src-editor"},
        ]}
        result = migrator.list_workspace_members()
        assert len(result) == 1

    def test_add_new_workspace_member(self, migrator):
        migrator.dest.post.return_value = {}
        result = migrator.add_or_update_workspace_member(
            {"user_id": "wu1", "role_id": "src-editor"},
            {},  # no existing ws members
            {"wu1": "dwu1"},  # org identity map
        )
        assert result is True
        migrator.dest.post.assert_called_once()
        call_args = migrator.dest.post.call_args
        assert call_args[0][0] == "/workspaces/current/members"
        assert call_args[0][1]["user_id"] == "dwu1"
        assert call_args[0][1]["role_id"] == "dst-editor"

    def test_update_existing_workspace_member_role(self, migrator):
        migrator.dest.patch.return_value = {}
        result = migrator.add_or_update_workspace_member(
            {"user_id": "wu1", "role_id": "src-admin"},
            {"dwu1": {"user_id": "dwu1", "role_id": "dst-editor"}},
            {"wu1": "dwu1"},
        )
        assert result is True
        migrator.dest.patch.assert_called_once()

    def test_workspace_member_dry_run_add(self, migrator):
        migrator.config.migration.dry_run = True
        result = migrator.add_or_update_workspace_member(
            {"user_id": "wu1", "role_id": "src-editor"},
            {},
            {"wu1": "dwu1"},
        )
        assert result is True
        migrator.dest.post.assert_not_called()

    def test_workspace_member_skip_existing(self, migrator):
        migrator.config.migration.skip_existing = True
        result = migrator.add_or_update_workspace_member(
            {"user_id": "wu1", "role_id": "src-admin"},
            {"dwu1": {"user_id": "dwu1", "role_id": "dst-editor"}},
            {"wu1": "dwu1"},
        )
        assert result is True
        migrator.dest.patch.assert_not_called()

    def test_migrate_workspace_members(self, migrator):
        migrator.source.get.return_value = {"members": [
            {"user_id": "wu1", "role_id": "src-editor"},
            {"user_id": "wu2", "role_id": "src-admin"},
        ]}
        migrator.dest.get.return_value = {"members": []}
        migrator.dest.post.return_value = {}

        count = migrator.migrate_workspace_members({"wu1": "dwu1", "wu2": "dwu2"})
        assert count == 2
