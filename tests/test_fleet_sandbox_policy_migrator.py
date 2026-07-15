"""Unit tests for FleetSandboxPolicyMigrator."""

from unittest.mock import Mock

import pytest

from langsmith_migrator.core.api_client import EnhancedAPIClient
from langsmith_migrator.core.migrators import FleetSandboxPolicyMigrator


def _mock_client() -> Mock:
    client = Mock(spec=EnhancedAPIClient)
    client.session = Mock()
    client.session.headers = {}
    return client


class TestFleetSandboxPolicyMigrator:
    """Test cases for FleetSandboxPolicyMigrator."""

    @pytest.fixture
    def policy_migrator(self, sample_config, migration_state):
        source = _mock_client()
        dest = _mock_client()
        return FleetSandboxPolicyMigrator(source, dest, migration_state, sample_config)

    @pytest.fixture
    def sample_policy(self):
        return {
            "id": "policy-1",
            "tenant_id": "ws-1",
            "name": "Default Sandbox",
            "max_cpu": "2",
            "max_memory": "4Gi",
            "network_allowed": False,
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-01T00:00:00Z",
        }

    def test_list_policies(self, policy_migrator, sample_policy):
        """Test listing sandbox policies."""
        policy_migrator.source.get_cursor_paginated.return_value = [sample_policy]

        result = policy_migrator.list_policies()

        assert len(result) == 1
        policy_migrator.source.get_cursor_paginated.assert_called_once_with(
            "/v1/platform/fleet/sandboxes/policies"
        )

    def test_list_policies_not_found(self, policy_migrator):
        """Test listing when endpoint not found."""
        from langsmith_migrator.core.api_client import NotFoundError

        policy_migrator.source.get_cursor_paginated.side_effect = NotFoundError(
            "Not found", status_code=404, request_info={}
        )

        result = policy_migrator.list_policies()

        assert len(result) == 0

    def test_create_policy(self, policy_migrator, sample_policy):
        """Test creating a sandbox policy."""
        policy_migrator.dest.get_cursor_paginated.return_value = []
        policy_migrator.dest.post.return_value = {"id": "new-policy-id"}

        result = policy_migrator.create_policy(sample_policy)

        assert result == "new-policy-id"
        call_args = policy_migrator.dest.post.call_args
        assert call_args[0][0] == "/v1/platform/fleet/sandboxes/policies"
        payload = call_args[0][1]
        assert payload["name"] == "Default Sandbox"
        assert payload["max_cpu"] == "2"
        # Internal fields should be stripped
        assert "id" not in payload
        assert "tenant_id" not in payload
        assert "created_at" not in payload
        assert "updated_at" not in payload

    def test_create_policy_existing_skip(self, policy_migrator, sample_policy):
        """Test skipping an existing sandbox policy (always skips)."""
        policy_migrator.dest.get_cursor_paginated.return_value = [
            {"id": "existing", "name": "Default Sandbox"},
        ]

        result = policy_migrator.create_policy(sample_policy)

        assert result == "existing"
        policy_migrator.dest.post.assert_not_called()

    def test_create_policy_dry_run(self, policy_migrator, sample_policy):
        """Test dry run mode."""
        policy_migrator.dest.get_cursor_paginated.return_value = []
        policy_migrator.config.migration.dry_run = True

        result = policy_migrator.create_policy(sample_policy)

        assert result.startswith("dry-run-")
        policy_migrator.dest.post.assert_not_called()

    def test_create_policy_failure(self, policy_migrator, sample_policy):
        """Test failure handling."""
        policy_migrator.dest.get_cursor_paginated.return_value = []
        policy_migrator.dest.post.side_effect = Exception("API error")

        result = policy_migrator.create_policy(sample_policy)

        assert result is None
