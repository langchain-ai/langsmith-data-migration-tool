"""Unit tests for ChartMigrator."""

from unittest.mock import Mock

from langsmith_migrator.core.api_client import EnhancedAPIClient
from langsmith_migrator.core.migrators import ChartMigrator


def _mock_client() -> Mock:
    client = Mock(spec=EnhancedAPIClient)
    client.session = Mock()
    client.session.headers = {}
    return client


def test_find_existing_chart_checks_destination_not_source(sample_config, migration_state):
    """Chart dedupe should look at destination charts before deciding to create."""

    source_client = _mock_client()
    dest_client = _mock_client()

    source_client.post.return_value = [
        {"id": "source-chart", "title": "Latency"},
    ]
    dest_client.post.return_value = [
        {"id": "dest-chart", "title": "Latency"},
    ]

    migrator = ChartMigrator(
        source_client,
        dest_client,
        migration_state,
        sample_config,
    )

    assert migrator.find_existing_chart("Latency") == "dest-chart"
    dest_client.post.assert_called_once()
    source_client.post.assert_not_called()
