import pytest
from unittest.mock import Mock
from langsmith_migrator.core.migrators.role import RoleMigrator, SYSTEM_ROLE_NAMES, _is_custom_role


class TestRoleMigrator:
    @pytest.fixture
    def migrator(self):
        source_client = Mock()
        dest_client = Mock()
        state = Mock()
        config = Mock()
        config.migration.dry_run = False
        config.migration.skip_existing = False
        config.migration.verbose = False
        return RoleMigrator(source_client, dest_client, state, config)

    @pytest.fixture
    def sample_roles(self):
        return [
            {"id": "r-admin", "name": "Admin", "is_system": True, "permissions": ["all"]},
            {"id": "r-viewer", "name": "Viewer", "is_system": True, "permissions": ["read"]},
            {"id": "r-editor", "name": "Editor", "is_system": True, "permissions": ["read", "write"]},
            {"id": "r-annotator", "name": "Annotator", "is_system": False, "permissions": ["read", "annotate"]},
            {"id": "r-reviewer", "name": "Reviewer", "is_system": False, "permissions": ["read", "review"]},
        ]

    # ── _is_custom_role helper ──

    def test_is_custom_role(self):
        assert _is_custom_role({"name": "Annotator", "is_system": False}) is True
        assert _is_custom_role({"name": "Admin", "is_system": True}) is False
        assert _is_custom_role({"name": "Editor", "is_system": False}) is False  # in SYSTEM_ROLE_NAMES

    # ── list_roles / list_custom_roles ──

    def test_list_roles_returns_all(self, migrator, sample_roles):
        migrator.source.get.return_value = {"roles": sample_roles}
        result = migrator.list_roles()
        assert len(result) == 5

    def test_list_roles_handles_list_response(self, migrator, sample_roles):
        migrator.source.get.return_value = sample_roles
        result = migrator.list_roles()
        assert len(result) == 5

    def test_list_roles_handles_error(self, migrator):
        migrator.source.get.side_effect = Exception("connection error")
        result = migrator.list_roles()
        assert result == []

    def test_list_custom_roles_filters_system(self, migrator, sample_roles):
        migrator.source.get.return_value = {"roles": sample_roles}
        result = migrator.list_custom_roles()
        assert len(result) == 2
        names = {r["name"] for r in result}
        assert names == {"Annotator", "Reviewer"}
        for name in SYSTEM_ROLE_NAMES:
            assert name not in names

    def test_list_custom_roles_uses_specified_client(self, migrator, sample_roles):
        migrator.dest.get.return_value = {"roles": sample_roles}
        result = migrator.list_custom_roles(migrator.dest)
        migrator.dest.get.assert_called_once_with("/orgs/current/roles")
        assert len(result) == 2

    # ── build_role_id_map ──

    def test_build_role_id_map(self, migrator):
        migrator.source.get.return_value = {"roles": [
            {"id": "src-admin", "name": "Admin"},
            {"id": "src-custom", "name": "Annotator"},
        ]}
        migrator.dest.get.return_value = {"roles": [
            {"id": "dst-admin", "name": "Admin"},
            {"id": "dst-custom", "name": "Annotator"},
        ]}
        result = migrator.build_role_id_map()
        assert result == {"src-admin": "dst-admin", "src-custom": "dst-custom"}

    def test_build_role_id_map_unmatched_roles(self, migrator):
        migrator.source.get.return_value = {"roles": [
            {"id": "src-x", "name": "OnlyOnSource"},
        ]}
        migrator.dest.get.return_value = {"roles": [
            {"id": "dst-y", "name": "OnlyOnDest"},
        ]}
        result = migrator.build_role_id_map()
        assert result == {}

    # ── get_dest_custom_roles_by_name / _find_existing_custom_role ──

    def test_get_dest_custom_roles_by_name(self, migrator):
        migrator.dest.get.return_value = {"roles": [
            {"id": "d1", "name": "Annotator", "is_system": False},
            {"id": "d2", "name": "Admin", "is_system": True},
        ]}
        result = migrator.get_dest_custom_roles_by_name()
        assert "Annotator" in result
        assert "Admin" not in result  # system role filtered out

    def test_find_existing_custom_role_found(self, migrator):
        migrator.dest.get.return_value = {"roles": [
            {"id": "d1", "name": "Annotator", "is_system": False},
        ]}
        result = migrator._find_existing_custom_role("Annotator")
        assert result["id"] == "d1"

    def test_find_existing_custom_role_not_found(self, migrator):
        migrator.dest.get.return_value = {"roles": []}
        result = migrator._find_existing_custom_role("Annotator")
        assert result is None

    # ── create_custom_role ──

    def test_create_custom_role_new_with_cache(self, migrator):
        """Using pre-fetched dest_roles_by_name avoids API calls."""
        migrator.dest.post.return_value = {"id": "new-role-1"}

        result = migrator.create_custom_role(
            {"id": "src-1", "name": "Annotator", "permissions": ["read"]},
            dest_roles_by_name={},  # empty cache = no existing roles
        )
        assert result == "new-role-1"
        migrator.dest.post.assert_called_once()
        # Should NOT call dest.get (cache was provided)
        migrator.dest.get.assert_not_called()

    def test_create_custom_role_new_without_cache(self, migrator):
        """Without cache, falls back to API call."""
        migrator.dest.get.return_value = {"roles": []}
        migrator.dest.post.return_value = {"id": "new-role-1"}

        result = migrator.create_custom_role(
            {"id": "src-1", "name": "Annotator", "permissions": ["read"]},
        )
        assert result == "new-role-1"

    def test_create_custom_role_exists_skip(self, migrator):
        migrator.config.migration.skip_existing = True
        result = migrator.create_custom_role(
            {"id": "src-1", "name": "Annotator"},
            dest_roles_by_name={"Annotator": {"id": "existing-1", "name": "Annotator"}},
        )
        assert result == "existing-1"
        migrator.dest.post.assert_not_called()
        migrator.dest.patch.assert_not_called()

    def test_create_custom_role_exists_update(self, migrator):
        migrator.dest.patch.return_value = {}

        result = migrator.create_custom_role(
            {"id": "src-1", "name": "Annotator", "permissions": ["read", "annotate"]},
            dest_roles_by_name={"Annotator": {"id": "existing-1", "name": "Annotator"}},
        )
        assert result == "existing-1"
        migrator.dest.patch.assert_called_once()

    def test_create_custom_role_dry_run(self, migrator):
        migrator.config.migration.dry_run = True

        result = migrator.create_custom_role(
            {"id": "src-1", "name": "Annotator"},
            dest_roles_by_name={},
        )
        assert result.startswith("dry-run-")
        migrator.dest.post.assert_not_called()

    # ── _update_custom_role ──

    def test_update_custom_role_dry_run(self, migrator):
        migrator.config.migration.dry_run = True
        result = migrator._update_custom_role("r1", {"name": "Test"})
        assert result == "r1"
        migrator.dest.patch.assert_not_called()

    def test_update_custom_role_error(self, migrator):
        migrator.dest.patch.side_effect = Exception("update failed")
        result = migrator._update_custom_role("r1", {"name": "Test"})
        assert result is None
