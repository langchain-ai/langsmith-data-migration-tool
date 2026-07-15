"""Unit tests for FleetUsageLimitMigrator."""

from unittest.mock import Mock

import pytest

from langsmith_migrator.core.api_client import EnhancedAPIClient
from langsmith_migrator.core.migrators import FleetUsageLimitMigrator


def _mock_client() -> Mock:
    client = Mock(spec=EnhancedAPIClient)
    client.session = Mock()
    client.session.headers = {}
    return client


class TestFleetUsageLimitMigrator:
    """Test cases for FleetUsageLimitMigrator."""

    @pytest.fixture
    def limit_migrator(self, sample_config, migration_state):
        source = _mock_client()
        dest = _mock_client()
        return FleetUsageLimitMigrator(source, dest, migration_state, sample_config)

    @pytest.fixture
    def sample_agent_limit(self):
        return {
            "id": "limit-1",
            "tenant_id": "ws-1",
            "subject_type": "agent",
            "subject_id": "agent-123",
            "limit_usd": 50.0,
        }

    @pytest.fixture
    def sample_global_limit(self):
        return {
            "id": "limit-2",
            "tenant_id": "ws-1",
            "subject_type": "agent",
            "subject_id": None,
            "limit_usd": 100.0,
        }

    def test_list_limits(self, limit_migrator, sample_agent_limit):
        """Test listing usage limits."""
        limit_migrator.source.get_cursor_paginated.return_value = [sample_agent_limit]

        result = limit_migrator.list_limits()

        assert len(result) == 1
        limit_migrator.source.get_cursor_paginated.assert_called_once_with(
            "/v1/platform/fleet/usage/limits"
        )

    def test_list_limits_not_found(self, limit_migrator):
        """Test listing when endpoint not found."""
        from langsmith_migrator.core.api_client import NotFoundError

        limit_migrator.source.get_cursor_paginated.side_effect = NotFoundError(
            "Not found", status_code=404, request_info={}
        )

        result = limit_migrator.list_limits()

        assert len(result) == 0

    def test_create_agent_limit_with_remap(self, limit_migrator, sample_agent_limit):
        """Test creating an agent spend limit with remapped subject_id."""
        limit_migrator.dest.post.return_value = {"id": "new-limit-id"}

        agent_map = {"agent-123": "dest-agent-id"}
        result = limit_migrator.create_limit(sample_agent_limit, agent_map)

        assert result == "new-limit-id"
        call_args = limit_migrator.dest.post.call_args
        assert call_args[0][0] == "/v1/platform/fleet/usage/limits"
        payload = call_args[0][1]
        assert payload["subject_type"] == "agent"
        assert payload["subject_id"] == "dest-agent-id"
        assert payload["limit_usd"] == 50.0

    def test_create_global_limit(self, limit_migrator, sample_global_limit):
        """Global default limits (nil subject_id) should pass through without remapping."""
        limit_migrator.dest.post.return_value = {"id": "new-limit-id"}

        result = limit_migrator.create_limit(sample_global_limit, {})

        assert result == "new-limit-id"
        payload = limit_migrator.dest.post.call_args[0][1]
        assert payload["subject_type"] == "agent"
        assert "subject_id" not in payload
        assert payload["limit_usd"] == 100.0

    def test_create_agent_limit_agent_not_in_map(self, limit_migrator, sample_agent_limit):
        """Limit should be skipped when agent ID is not in the mapping."""
        result = limit_migrator.create_limit(sample_agent_limit, {})

        assert result is None
        limit_migrator.dest.post.assert_not_called()

    def test_create_limit_dry_run(self, limit_migrator, sample_agent_limit):
        """Test dry run mode."""
        limit_migrator.config.migration.dry_run = True

        agent_map = {"agent-123": "dest-agent-id"}
        result = limit_migrator.create_limit(sample_agent_limit, agent_map)

        assert result.startswith("dry-run-")
        limit_migrator.dest.post.assert_not_called()

    def test_create_user_limit_with_user_map(self, limit_migrator):
        """User-type limits should remap using the user_id_map."""
        user_limit = {
            "id": "limit-3",
            "subject_type": "user",
            "subject_id": "user-1",
            "limit_usd": 25.0,
        }
        limit_migrator.dest.post.return_value = {"id": "new-limit-id"}

        result = limit_migrator.create_limit(
            user_limit, agent_id_map={}, user_id_map={"user-1": "dest-user-1"}
        )

        assert result == "new-limit-id"
        payload = limit_migrator.dest.post.call_args[0][1]
        assert payload["subject_id"] == "dest-user-1"

    def test_create_user_limit_no_user_map_preserves_id(self, limit_migrator):
        """User-type limits without a user_id_map should preserve the original subject_id."""
        user_limit = {
            "id": "limit-4",
            "subject_type": "user",
            "subject_id": "user-1",
            "limit_usd": 25.0,
        }
        limit_migrator.dest.post.return_value = {"id": "new-limit-id"}

        result = limit_migrator.create_limit(user_limit, agent_id_map={})

        assert result == "new-limit-id"
        payload = limit_migrator.dest.post.call_args[0][1]
        assert payload["subject_id"] == "user-1"

    def test_create_limit_failure(self, limit_migrator, sample_agent_limit):
        """Test failure handling."""
        limit_migrator.dest.post.side_effect = Exception("API error")

        agent_map = {"agent-123": "dest-agent-id"}
        result = limit_migrator.create_limit(sample_agent_limit, agent_map)

        assert result is None
