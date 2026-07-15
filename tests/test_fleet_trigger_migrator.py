"""Unit tests for FleetTriggerMigrator."""

from unittest.mock import Mock

import pytest

from langsmith_migrator.core.api_client import EnhancedAPIClient
from langsmith_migrator.core.migrators import FleetTriggerMigrator


def _mock_client() -> Mock:
    client = Mock(spec=EnhancedAPIClient)
    client.session = Mock()
    client.session.headers = {}
    return client


class TestFleetTriggerMigrator:
    """Test cases for FleetTriggerMigrator."""

    @pytest.fixture
    def trigger_migrator(self, sample_config, migration_state):
        source = _mock_client()
        dest = _mock_client()
        return FleetTriggerMigrator(source, dest, migration_state, sample_config)

    @pytest.fixture
    def sample_trigger(self):
        return {
            "id": "trigger-1",
            "agent_id": "agent-123",
            "template_id": "template-456",
            "name": "Slack trigger",
            "config": {"channel_id": "C123", "channel_name": "general"},
            "status": "active",
        }

    def test_list_triggers(self, trigger_migrator, sample_trigger):
        """Test listing triggers."""
        trigger_migrator.source.get_cursor_paginated.return_value = [sample_trigger]

        result = trigger_migrator.list_triggers()

        assert len(result) == 1
        trigger_migrator.source.get_cursor_paginated.assert_called_once_with("/v1/fleet/triggers")

    def test_list_triggers_not_found(self, trigger_migrator):
        """Test listing when endpoint not found."""
        from langsmith_migrator.core.api_client import NotFoundError

        trigger_migrator.source.get_cursor_paginated.side_effect = NotFoundError(
            "Not found", status_code=404, request_info={}
        )

        result = trigger_migrator.list_triggers()

        assert len(result) == 0

    def test_create_trigger(self, trigger_migrator, sample_trigger):
        """Test creating a trigger with remapped agent ID."""
        trigger_migrator.dest.get_cursor_paginated.return_value = []
        trigger_migrator.dest.post.return_value = {"id": "new-trigger-id"}

        agent_map = {"agent-123": "dest-agent-id"}
        result = trigger_migrator.create_trigger(sample_trigger, agent_map)

        assert result == "new-trigger-id"
        call_args = trigger_migrator.dest.post.call_args
        assert call_args[0][0] == "/v1/fleet/triggers"
        payload = call_args[0][1]
        assert payload["agent_id"] == "dest-agent-id"
        assert payload["template_id"] == "template-456"
        assert payload["config"] == {"channel_id": "C123", "channel_name": "general"}

    def test_create_trigger_existing_skip(self, trigger_migrator, sample_trigger):
        """An existing trigger for the same agent, template, and config is reused."""
        trigger_migrator.dest.get_cursor_paginated.return_value = [
            {
                "id": "existing-trigger-id",
                "agent_id": "dest-agent-id",
                "template_id": "template-456",
                "config": {"channel_id": "C123", "channel_name": "general"},
                "status": "paused",
            }
        ]

        result = trigger_migrator.create_trigger(
            sample_trigger, {"agent-123": "dest-agent-id"}
        )

        assert result == "existing-trigger-id"
        trigger_migrator.dest.post.assert_not_called()
        trigger_migrator.dest.get_cursor_paginated.assert_called_once_with(
            "/v1/fleet/triggers", params=None
        )

    def test_create_trigger_agent_not_in_map(self, trigger_migrator, sample_trigger):
        """Trigger should be skipped when agent ID is not in the mapping."""
        result = trigger_migrator.create_trigger(sample_trigger, {})

        assert result is None
        trigger_migrator.dest.post.assert_not_called()

    def test_create_trigger_dry_run(self, trigger_migrator, sample_trigger):
        """Test dry run mode."""
        trigger_migrator.config.migration.dry_run = True

        agent_map = {"agent-123": "dest-agent-id"}
        result = trigger_migrator.create_trigger(sample_trigger, agent_map)

        assert result.startswith("dry-run-")
        trigger_migrator.dest.post.assert_not_called()

    def test_create_trigger_failure(self, trigger_migrator, sample_trigger):
        """Test failure handling."""
        trigger_migrator.dest.post.side_effect = Exception("API error")

        agent_map = {"agent-123": "dest-agent-id"}
        result = trigger_migrator.create_trigger(sample_trigger, agent_map)

        assert result is None
