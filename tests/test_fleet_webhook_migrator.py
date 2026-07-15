"""Unit tests for FleetWebhookMigrator."""

from unittest.mock import Mock

import pytest

from langsmith_migrator.core.api_client import EnhancedAPIClient
from langsmith_migrator.core.migrators import FleetWebhookMigrator


def _mock_client() -> Mock:
    client = Mock(spec=EnhancedAPIClient)
    client.session = Mock()
    client.session.headers = {}
    return client


class TestFleetWebhookMigrator:
    """Test cases for FleetWebhookMigrator."""

    @pytest.fixture
    def webhook_migrator(self, sample_config, migration_state):
        source = _mock_client()
        dest = _mock_client()
        return FleetWebhookMigrator(source, dest, migration_state, sample_config)

    @pytest.fixture
    def sample_webhook(self):
        return {
            "id": "webhook-1",
            "name": "Data Pipeline Trigger",
            "url": "https://external.example.com/webhook",
            "form_schema": {"fields": [{"name": "message", "type": "text"}]},
            "headers": {"Authorization": "Bearer token"},
        }

    def test_list_webhooks(self, webhook_migrator, sample_webhook):
        """Test listing webhooks."""
        webhook_migrator.source.get_cursor_paginated.return_value = [sample_webhook]

        result = webhook_migrator.list_webhooks()

        assert len(result) == 1
        webhook_migrator.source.get_cursor_paginated.assert_called_once_with(
            "/v1/platform/fleet-webhooks"
        )

    def test_list_webhooks_not_found(self, webhook_migrator):
        """Test listing when endpoint not found."""
        from langsmith_migrator.core.api_client import NotFoundError

        webhook_migrator.source.get_cursor_paginated.side_effect = NotFoundError(
            "Not found", status_code=404, request_info={}
        )

        result = webhook_migrator.list_webhooks()

        assert len(result) == 0

    def test_create_webhook(self, webhook_migrator, sample_webhook):
        """Test creating a webhook."""
        webhook_migrator.dest.get_cursor_paginated.return_value = []
        webhook_migrator.dest.post.return_value = {"id": "new-webhook-id"}

        result = webhook_migrator.create_webhook(sample_webhook)

        assert result == "new-webhook-id"
        call_args = webhook_migrator.dest.post.call_args
        assert call_args[0][0] == "/v1/platform/fleet-webhooks"
        payload = call_args[0][1]
        assert payload["name"] == "Data Pipeline Trigger"
        assert payload["url"] == "https://external.example.com/webhook"
        assert "form_schema" in payload

    def test_create_webhook_existing_skip(self, webhook_migrator, sample_webhook):
        """Test skipping an existing webhook (always skips)."""
        webhook_migrator.dest.get_cursor_paginated.return_value = [
            {"id": "existing", "name": "Data Pipeline Trigger"},
        ]

        result = webhook_migrator.create_webhook(sample_webhook)

        assert result == "existing"
        webhook_migrator.dest.post.assert_not_called()

    def test_create_webhook_dry_run(self, webhook_migrator, sample_webhook):
        """Test dry run mode."""
        webhook_migrator.dest.get_cursor_paginated.return_value = []
        webhook_migrator.config.migration.dry_run = True

        result = webhook_migrator.create_webhook(sample_webhook)

        assert result.startswith("dry-run-")
        webhook_migrator.dest.post.assert_not_called()

    def test_create_webhook_failure(self, webhook_migrator, sample_webhook):
        """Test failure handling."""
        webhook_migrator.dest.get_cursor_paginated.return_value = []
        webhook_migrator.dest.post.side_effect = Exception("API error")

        result = webhook_migrator.create_webhook(sample_webhook)

        assert result is None
