"""Unit tests for FleetScheduleMigrator."""

from unittest.mock import Mock

import pytest

from langsmith_migrator.core.api_client import EnhancedAPIClient
from langsmith_migrator.core.migrators import FleetScheduleMigrator


def _mock_client() -> Mock:
    client = Mock(spec=EnhancedAPIClient)
    client.session = Mock()
    client.session.headers = {}
    return client


class TestFleetScheduleMigrator:
    """Test cases for FleetScheduleMigrator."""

    @pytest.fixture
    def schedule_migrator(self, sample_config, migration_state):
        source = _mock_client()
        dest = _mock_client()
        return FleetScheduleMigrator(source, dest, migration_state, sample_config)

    @pytest.fixture
    def sample_schedule(self):
        return {
            "id": "sched-1",
            "agent_id": "agent-123",
            "cron": "0 9 * * *",
            "input_message": "Summarize my unread emails.",
            "display_name": "Daily Email Briefing",
            "enabled": True,
        }

    def test_list_schedules(self, schedule_migrator, sample_schedule):
        """Test listing schedules for an agent."""
        schedule_migrator.source.get_cursor_paginated.return_value = [sample_schedule]

        result = schedule_migrator.list_schedules("agent-123")

        assert len(result) == 1
        schedule_migrator.source.get_cursor_paginated.assert_called_once_with(
            "/v1/fleet/agents/agent-123/schedules"
        )

    def test_create_schedule(self, schedule_migrator, sample_schedule):
        """Test creating a schedule on destination agent."""
        schedule_migrator.dest.get_cursor_paginated.return_value = []
        schedule_migrator.dest.post.return_value = {"id": "new-sched-id"}

        result = schedule_migrator.create_schedule("dest-agent-id", sample_schedule)

        assert result == "new-sched-id"
        call_args = schedule_migrator.dest.post.call_args
        assert call_args[0][0] == "/v1/fleet/agents/dest-agent-id/schedules"
        assert call_args[0][1]["cron"] == "0 9 * * *"

    def test_create_schedule_skips_duplicate_cron(self, schedule_migrator, sample_schedule):
        """Should skip if a schedule with the same cron already exists."""
        schedule_migrator.dest.get_cursor_paginated.return_value = [
            {"id": "existing-sched", "cron": "0 9 * * *"},
        ]

        result = schedule_migrator.create_schedule("dest-agent-id", sample_schedule)

        assert result == "existing-sched"
        schedule_migrator.dest.post.assert_not_called()

    def test_create_schedule_dry_run(self, schedule_migrator, sample_schedule):
        """Test dry run mode."""
        schedule_migrator.dest.get_cursor_paginated.return_value = []
        schedule_migrator.config.migration.dry_run = True

        result = schedule_migrator.create_schedule("dest-agent-id", sample_schedule)

        assert result.startswith("dry-run-")
        schedule_migrator.dest.post.assert_not_called()
