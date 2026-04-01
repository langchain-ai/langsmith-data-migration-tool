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


def test_create_chart_enriches_missing_series_before_create(sample_config, migration_state):
    """Chart creation should attempt a richer fetch before exporting missing-series charts."""

    source_client = _mock_client()
    dest_client = _mock_client()

    source_client.post.return_value = [
        {
            "id": "source-chart",
            "title": "Latency",
            "chart_type": "line",
            "series": [{"filters": {"project_id": "project-1"}}],
        }
    ]
    dest_client.post.return_value = {"id": "dest-chart"}

    migrator = ChartMigrator(
        source_client,
        dest_client,
        migration_state,
        sample_config,
    )
    migrator.find_existing_chart = Mock(return_value=None)
    migrator._verify_chart = Mock(return_value=(True, {}))

    chart_id = migrator.create_chart({"id": "source-chart", "title": "Latency"})

    assert chart_id == "dest-chart"
    dest_client.post.assert_called_with(
        "/charts/create",
        {
            "title": "Latency",
            "chart_type": "line",
            "series": [{"filters": {"project_id": "project-1"}}],
        },
    )


def test_build_project_mapping_skips_duplicate_names(sample_config, migration_state):
    """Duplicate project names should disable exact-name chart mapping."""

    source_client = _mock_client()
    dest_client = _mock_client()
    source_client.get_paginated.return_value = [
        {"id": "source-a", "name": "Duplicate"},
        {"id": "source-b", "name": "Duplicate"},
    ]
    dest_client.get_paginated.return_value = [
        {"id": "dest-a", "name": "Duplicate"},
    ]

    migrator = ChartMigrator(
        source_client,
        dest_client,
        migration_state,
        sample_config,
    )

    assert migrator._build_project_mapping() == {}
