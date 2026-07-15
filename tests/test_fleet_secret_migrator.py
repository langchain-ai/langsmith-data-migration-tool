"""Unit tests for FleetSecretMigrator."""

from unittest.mock import Mock

import pytest

from langsmith_migrator.core.api_client import EnhancedAPIClient
from langsmith_migrator.core.migrators import FleetSecretMigrator


def _mock_client() -> Mock:
    client = Mock(spec=EnhancedAPIClient)
    client.session = Mock()
    client.session.headers = {}
    return client


class TestFleetSecretMigrator:
    """Test cases for FleetSecretMigrator."""

    @pytest.fixture
    def secret_migrator(self, sample_config, migration_state):
        source = _mock_client()
        dest = _mock_client()
        return FleetSecretMigrator(source, dest, migration_state, sample_config)

    def test_list_secrets(self, secret_migrator):
        """Test listing secrets returns names only (write-only)."""
        secret_migrator.source.get_cursor_paginated.return_value = [
            {"name": "OPENAI_API_KEY", "set": True},
            {"name": "ANTHROPIC_API_KEY", "set": True},
        ]

        result = secret_migrator.list_secrets()

        assert len(result) == 2
        assert result[0]["name"] == "OPENAI_API_KEY"
        assert "value" not in result[0]

    def test_create_secret_placeholder(self, secret_migrator):
        """Test creating a placeholder secret."""
        result = secret_migrator.create_secret_placeholder("MY_SECRET")

        assert result is True
        secret_migrator.dest.put.assert_called_once()
        call_args = secret_migrator.dest.put.call_args
        assert call_args[0][0] == "/v1/fleet/secrets/MY_SECRET"
        assert call_args[0][1] == {"value": ""}

    def test_create_secret_placeholder_dry_run(self, secret_migrator):
        """Test dry run mode."""
        secret_migrator.config.migration.dry_run = True

        result = secret_migrator.create_secret_placeholder("MY_SECRET")

        assert result is True
        secret_migrator.dest.put.assert_not_called()

    def test_create_secret_placeholder_failure(self, secret_migrator):
        """Test failure handling."""
        secret_migrator.dest.put.side_effect = Exception("API error")

        result = secret_migrator.create_secret_placeholder("MY_SECRET")

        assert result is False
