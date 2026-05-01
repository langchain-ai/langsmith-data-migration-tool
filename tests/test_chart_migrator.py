"""Unit tests for ChartMigrator."""

from unittest.mock import Mock

import pytest

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


def test_resolve_destination_session_id_falls_back_to_exact_name_matching(
    sample_config,
    migration_state,
):
    """Explicit project mappings should not block exact-name fallback for other sessions."""

    source_client = _mock_client()
    dest_client = _mock_client()

    def source_get_paginated(endpoint, page_size=100):
        if endpoint == "/sessions":
            return [
                {"id": "source-explicit", "name": "Explicit Source"},
                {"id": "source-fallback", "name": "Fallback Project"},
            ]
        return []

    def dest_get_paginated(endpoint, page_size=100):
        if endpoint == "/sessions":
            return [{"id": "dest-fallback", "name": "Fallback Project"}]
        return []

    source_client.get_paginated.side_effect = source_get_paginated
    dest_client.get_paginated.side_effect = dest_get_paginated

    migrator = ChartMigrator(
        source_client,
        dest_client,
        migration_state,
        sample_config,
    )
    migrator._project_id_map = {"source-explicit": "dest-explicit"}

    assert (
        migrator.resolve_destination_session_id("source-explicit", same_instance=False)
        == "dest-explicit"
    )
    assert (
        migrator.resolve_destination_session_id("source-fallback", same_instance=False)
        == "dest-fallback"
    )


def test_resolve_destination_session_id_uses_saved_project_mapping_before_listing(
    sample_config,
    migration_state,
):
    """Saved TUI/state project mappings should directly resolve chart destination sessions."""

    source_session_id = "a3cee8e2-40bb-472e-8456-c660b5ea1f3d"
    dest_session_id = "cc3ac580-destination-project"
    migration_state.set_mapped_id("project", source_session_id, dest_session_id)
    source_client = _mock_client()
    dest_client = _mock_client()

    migrator = ChartMigrator(
        source_client,
        dest_client,
        migration_state,
        sample_config,
    )

    assert (
        migrator.resolve_destination_session_id(source_session_id, same_instance=False)
        == dest_session_id
    )
    assert migrator._project_id_map == {source_session_id: dest_session_id}
    source_client.get_paginated.assert_not_called()
    dest_client.get_paginated.assert_not_called()


def test_extract_session_id_finds_nested_session_filter(
    sample_config,
    migration_state,
):
    """Chart pre-resolution should find session filters that migration remaps."""

    source_client = _mock_client()
    dest_client = _mock_client()
    migrator = ChartMigrator(
        source_client,
        dest_client,
        migration_state,
        sample_config,
    )

    assert (
        migrator._extract_session_id(
            {
                "id": "chart-1",
                "title": "Chart One",
                "series": [
                    {
                        "filters": {
                            "operator": "and",
                            "children": [{"session": ["a3cee8e2-40bb-472e-8456-c660b5ea1f3d"]}],
                        }
                    }
                ],
            }
        )
        == "a3cee8e2-40bb-472e-8456-c660b5ea1f3d"
    )


@pytest.mark.parametrize(
    "chart",
    [
        {"id": "chart-1", "project_id": "source-project"},
        {"id": "chart-1", "session_id": "source-project"},
        {"id": "chart-1", "series": [{"filters": {"project_id": "source-project"}}]},
        {"id": "chart-1", "series": [{"filters": {"session_id": "source-project"}}]},
        {"id": "chart-1", "common_filters": {"session": ["source-project"]}},
        {
            "id": "chart-1",
            "series": [
                {
                    "filters": {
                        "children": [
                            {"session": ["source-project"]},
                        ]
                    }
                }
            ],
        },
        {
            "id": "chart-1",
            "series": [
                {
                    "filters": {
                        "children": [
                            {"nested": {"project_id": "source-project"}},
                        ]
                    }
                }
            ],
        },
    ],
)
def test_extract_session_id_finds_all_project_dependency_shapes(
    sample_config,
    migration_state,
    chart,
):
    """Pre-resolution should find every project/session shape remapping supports."""

    migrator = ChartMigrator(
        _mock_client(),
        _mock_client(),
        migration_state,
        sample_config,
    )

    assert migrator._extract_session_id(chart) == "source-project"


def test_migrate_all_charts_resolves_nested_session_filter(
    sample_config,
    migration_state,
):
    """All-chart migration should pass mapped destination IDs for nested filters."""

    source_session_id = "a3cee8e2-40bb-472e-8456-c660b5ea1f3d"
    dest_session_id = "cc3ac580-destination-project"
    source_client = _mock_client()
    dest_client = _mock_client()
    migrator = ChartMigrator(
        source_client,
        dest_client,
        migration_state,
        sample_config,
    )
    chart = {
        "id": "chart-1",
        "title": "Chart One",
        "series": [
            {
                "filters": {
                    "operator": "and",
                    "children": [{"session": [source_session_id]}],
                }
            }
        ],
    }
    migrator._project_id_map = {source_session_id: dest_session_id}
    migrator.list_charts = Mock(return_value=[chart])
    migrator.migrate_chart = Mock(return_value="dest-chart-1")

    mappings = migrator.migrate_all_charts(same_instance=False)

    assert mappings == {source_session_id: {"chart-1": "dest-chart-1"}}
    migrator.migrate_chart.assert_called_once_with(
        chart,
        dest_session_id,
        same_instance=False,
    )


def test_migrate_all_charts_preserves_multiple_remapped_project_dependencies(
    sample_config,
    migration_state,
):
    """All-chart remap should keep every mapped project dependency distinct."""

    source_client = _mock_client()
    dest_client = _mock_client()
    migrator = ChartMigrator(
        source_client,
        dest_client,
        migration_state,
        sample_config,
    )
    migrator._project_id_map = {
        "source-project-a": "dest-project-a",
        "source-project-b": "dest-project-b",
    }
    migrator._project_mapping_complete = True
    migrator._dataset_id_map = {}
    migrator.list_charts = Mock(
        return_value=[
            {
                "id": "chart-1",
                "title": "Chart One",
                "project_id": "source-project-a",
                "series": [
                    {
                        "filters": {
                            "session": [
                                "source-project-a",
                                "source-project-b",
                            ]
                        }
                    }
                ],
            }
        ]
    )
    migrator.create_chart = Mock(return_value="dest-chart-1")
    migrator._export_chart_manual_apply = Mock(return_value="/tmp/chart.json")

    mappings = migrator.migrate_all_charts(same_instance=False)

    assert mappings == {"source-project-a": {"chart-1": "dest-chart-1"}}
    payload = migrator.create_chart.call_args.args[0]
    assert payload["project_id"] == "dest-project-a"
    assert payload["series"][0]["filters"]["session"] == [
        "dest-project-a",
        "dest-project-b",
    ]
    migrator._export_chart_manual_apply.assert_not_called()


def test_migrate_chart_exports_when_dependencies_are_unresolved(
    sample_config,
    migration_state,
):
    """Charts with unmapped project/session filters should export remediation instead of creating."""

    source_client = _mock_client()
    dest_client = _mock_client()

    migrator = ChartMigrator(
        source_client,
        dest_client,
        migration_state,
        sample_config,
    )
    migrator._project_id_map = {}
    migrator._project_mapping_complete = True
    migrator._dataset_id_map = {}
    migrator.create_chart = Mock()
    migrator._export_chart_manual_apply = Mock(return_value="/tmp/chart.json")

    chart_id = migrator.migrate_chart(
        {
            "id": "source-chart",
            "title": "Latency",
            "series": [{"filters": {"project_id": "source-project"}}],
        }
    )

    assert chart_id is None
    migrator.create_chart.assert_not_called()
    migrator._export_chart_manual_apply.assert_called_once()
    analysis = migrator._export_chart_manual_apply.call_args.kwargs["analysis"]
    assert analysis["unresolved_dependencies"] == {"project_id": ["source-project"]}


def test_migrate_chart_preserves_dataset_ids_in_same_instance_mode(
    sample_config,
    migration_state,
):
    """Same-instance chart runs should keep raw dataset IDs instead of exporting them."""

    source_client = _mock_client()
    dest_client = _mock_client()

    migrator = ChartMigrator(
        source_client,
        dest_client,
        migration_state,
        sample_config,
    )
    migrator._project_id_map = {}
    migrator._project_mapping_complete = True
    migrator._dataset_id_map = {}
    migrator.create_chart = Mock(return_value="dest-chart")
    migrator._export_chart_manual_apply = Mock(return_value="/tmp/chart.json")

    chart_id = migrator.migrate_chart(
        {
            "id": "source-chart",
            "title": "Latency",
            "series": [{"filters": {"dataset_id": "source-dataset"}}],
        },
        dest_session_id="source-project",
        same_instance=True,
    )

    assert chart_id == "dest-chart"
    migrator.create_chart.assert_called_once()
    payload = migrator.create_chart.call_args.args[0]
    assert payload["series"][0]["filters"]["dataset_id"] == "source-dataset"
    migrator._export_chart_manual_apply.assert_not_called()


def test_migrate_chart_exports_unresolved_dataset_filter_in_remap_mode(
    sample_config,
    migration_state,
):
    """Remapped chart runs should still export unresolved dataset filters."""

    source_client = _mock_client()
    dest_client = _mock_client()

    migrator = ChartMigrator(
        source_client,
        dest_client,
        migration_state,
        sample_config,
    )
    migrator._project_id_map = {}
    migrator._project_mapping_complete = True
    migrator._dataset_id_map = {}
    migrator.create_chart = Mock()
    migrator._export_chart_manual_apply = Mock(return_value="/tmp/chart.json")

    chart_id = migrator.migrate_chart(
        {
            "id": "source-chart",
            "title": "Latency",
            "series": [{"filters": {"dataset_id": "source-dataset"}}],
        },
        same_instance=False,
    )

    assert chart_id is None
    migrator.create_chart.assert_not_called()
    analysis = migrator._export_chart_manual_apply.call_args.kwargs["analysis"]
    assert analysis["unresolved_dependencies"] == {"dataset_id": ["source-dataset"]}


def test_migrate_chart_maps_each_project_dependency_when_dest_session_is_known(
    sample_config,
    migration_state,
):
    """Cross-workspace chart remap should not collapse multiple project filters."""

    source_client = _mock_client()
    dest_client = _mock_client()

    migrator = ChartMigrator(
        source_client,
        dest_client,
        migration_state,
        sample_config,
    )
    migrator._project_id_map = {
        "source-project-a": "dest-project-a",
        "source-project-b": "dest-project-b",
    }
    migrator._project_mapping_complete = True
    migrator._dataset_id_map = {}
    migrator.create_chart = Mock(return_value="dest-chart")
    migrator._export_chart_manual_apply = Mock(return_value="/tmp/chart.json")

    chart_id = migrator.migrate_chart(
        {
            "id": "source-chart",
            "title": "Latency",
            "project_id": "source-project-a",
            "series": [
                {
                    "filters": {
                        "session_id": "source-project-b",
                        "nested": {
                            "session": [
                                "source-project-a",
                                "source-project-b",
                            ]
                        },
                    }
                }
            ],
        },
        dest_session_id="dest-project-a",
        same_instance=False,
    )

    assert chart_id == "dest-chart"
    payload = migrator.create_chart.call_args.args[0]
    assert payload["project_id"] == "dest-project-a"
    assert payload["series"][0]["filters"]["session_id"] == "dest-project-b"
    assert payload["series"][0]["filters"]["nested"]["session"] == [
        "dest-project-a",
        "dest-project-b",
    ]
    migrator._export_chart_manual_apply.assert_not_called()


def test_migrate_chart_rewrites_project_ids_embedded_in_string_filters(
    sample_config,
    migration_state,
):
    """Project mappings should also apply to serialized chart filter expressions."""

    source_client = _mock_client()
    dest_client = _mock_client()

    migrator = ChartMigrator(
        source_client,
        dest_client,
        migration_state,
        sample_config,
    )
    migrator._project_id_map = {
        "source-project-a": "dest-project-a",
        "source-project-b": "dest-project-b",
    }
    migrator._project_mapping_complete = True
    migrator._dataset_id_map = {}
    migrator.create_chart = Mock(return_value="dest-chart")
    migrator._export_chart_manual_apply = Mock(return_value="/tmp/chart.json")

    chart_id = migrator.migrate_chart(
        {
            "id": "source-chart",
            "title": "Latency",
            "project_id": "source-project-a",
            "series": [
                {
                    "filters": {
                        "filter": (
                            'and(eq(session_id, "source-project-a"), '
                            'eq(project_id, "source-project-b"))'
                        )
                    }
                }
            ],
        },
        dest_session_id="dest-project-a",
        same_instance=False,
    )

    assert chart_id == "dest-chart"
    payload = migrator.create_chart.call_args.args[0]
    filter_expr = payload["series"][0]["filters"]["filter"]
    assert "dest-project-a" in filter_expr
    assert "dest-project-b" in filter_expr
    assert "source-project" not in filter_expr
    migrator._export_chart_manual_apply.assert_not_called()


def test_migrate_chart_exports_unmapped_project_ids_embedded_only_in_string_filters(
    sample_config,
    migration_state,
):
    """String-only chart project dependencies must not survive cross-workspace remap."""

    mapped_source_id = "11111111-1111-1111-1111-111111111111"
    mapped_dest_id = "22222222-2222-2222-2222-222222222222"
    unmapped_source_id = "33333333-3333-3333-3333-333333333333"
    source_client = _mock_client()
    dest_client = _mock_client()

    def source_get_paginated(endpoint, page_size=100):
        if endpoint == "/sessions":
            return [
                {"id": mapped_source_id, "name": "Mapped Project"},
                {"id": unmapped_source_id, "name": "Unmapped Project"},
            ]
        return []

    def dest_get_paginated(endpoint, page_size=100):
        if endpoint == "/sessions":
            return [{"id": mapped_dest_id, "name": "Mapped Project"}]
        return []

    source_client.get_paginated.side_effect = source_get_paginated
    dest_client.get_paginated.side_effect = dest_get_paginated

    migrator = ChartMigrator(
        source_client,
        dest_client,
        migration_state,
        sample_config,
    )
    migrator.create_chart = Mock()
    migrator._export_chart_manual_apply = Mock(return_value="/tmp/chart.json")

    chart_id = migrator.migrate_chart(
        {
            "id": "source-chart",
            "title": "Latency",
            "series": [
                {
                    "filters": {
                        "filter": f'eq(session_id, "{unmapped_source_id}")',
                    }
                }
            ],
        },
        same_instance=False,
    )

    assert chart_id is None
    migrator.create_chart.assert_not_called()
    analysis = migrator._export_chart_manual_apply.call_args.kwargs["analysis"]
    assert analysis["unresolved_dependencies"] == {"session_id": [unmapped_source_id]}


def test_migrate_chart_same_instance_preserves_multiple_project_filters(
    sample_config,
    migration_state,
):
    """Same-instance chart mode should preserve all source project IDs."""

    source_client = _mock_client()
    dest_client = _mock_client()

    migrator = ChartMigrator(
        source_client,
        dest_client,
        migration_state,
        sample_config,
    )
    migrator._project_id_map = {}
    migrator._project_mapping_complete = True
    migrator._dataset_id_map = {}
    migrator.create_chart = Mock(return_value="dest-chart")
    migrator._export_chart_manual_apply = Mock(return_value="/tmp/chart.json")

    chart_id = migrator.migrate_chart(
        {
            "id": "source-chart",
            "title": "Latency",
            "project_id": "source-project-a",
            "series": [
                {
                    "filters": {
                        "session_id": "source-project-b",
                        "nested": {
                            "session": [
                                "source-project-a",
                                "source-project-b",
                            ]
                        },
                    }
                }
            ],
        },
        dest_session_id="source-project-a",
        same_instance=True,
    )

    assert chart_id == "dest-chart"
    payload = migrator.create_chart.call_args.args[0]
    assert payload["project_id"] == "source-project-a"
    assert payload["series"][0]["filters"]["session_id"] == "source-project-b"
    assert payload["series"][0]["filters"]["nested"]["session"] == [
        "source-project-a",
        "source-project-b",
    ]
    migrator._export_chart_manual_apply.assert_not_called()


def test_migrate_chart_exports_unmapped_secondary_project_dependency(
    sample_config,
    migration_state,
):
    """Cross-workspace remap should not hide missing secondary project mappings."""

    source_client = _mock_client()
    dest_client = _mock_client()

    migrator = ChartMigrator(
        source_client,
        dest_client,
        migration_state,
        sample_config,
    )
    migrator._project_id_map = {"source-project-a": "dest-project-a"}
    migrator._project_mapping_complete = True
    migrator._dataset_id_map = {}
    migrator.create_chart = Mock()
    migrator._export_chart_manual_apply = Mock(return_value="/tmp/chart.json")

    chart_id = migrator.migrate_chart(
        {
            "id": "source-chart",
            "title": "Latency",
            "project_id": "source-project-a",
            "series": [
                {
                    "filters": {
                        "session": [
                            "source-project-a",
                            "source-project-b",
                        ]
                    }
                }
            ],
        },
        dest_session_id="dest-project-a",
        same_instance=False,
    )

    assert chart_id is None
    migrator.create_chart.assert_not_called()
    analysis = migrator._export_chart_manual_apply.call_args.kwargs["analysis"]
    assert analysis["dest_session_id"] == "dest-project-a"
    assert analysis["unresolved_dependencies"] == {"session_id": ["source-project-b"]}


def test_migrate_session_charts_forwards_same_instance(
    sample_config,
    migration_state,
):
    """Session-scoped chart migration should forward same-instance mode to each chart."""

    source_client = _mock_client()
    dest_client = _mock_client()

    migrator = ChartMigrator(
        source_client,
        dest_client,
        migration_state,
        sample_config,
    )
    migrator.list_charts = Mock(return_value=[{"id": "chart-1", "title": "Latency"}])
    migrator.migrate_chart = Mock(return_value="dest-chart-1")

    mappings = migrator.migrate_session_charts(
        "source-session",
        "dest-session",
        same_instance=True,
    )

    assert mappings == {"chart-1": "dest-chart-1"}
    migrator.migrate_chart.assert_called_once_with(
        {"id": "chart-1", "title": "Latency"},
        "dest-session",
        same_instance=True,
    )
