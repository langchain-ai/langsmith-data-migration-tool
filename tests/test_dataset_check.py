"""Tests for comparing migrated datasets with their source."""

from __future__ import annotations

from unittest.mock import Mock

from langsmith_migrator.core.api_client import APIError
from langsmith_migrator.core.dataset_check import compare_datasets


def _migrator(source_datasets, dest_datasets, source_experiments=None, dest_experiments=None):
    """A DatasetMigrator stand-in serving dataset reads and experiment listings."""
    migrator = Mock()

    def reader(datasets):
        def get(endpoint, params=None):
            dataset_id = endpoint.rsplit("/", 1)[-1]
            if dataset_id not in datasets:
                raise APIError("not found", status_code=404)
            return datasets[dataset_id]
        return get

    def sessions(experiments):
        return lambda endpoint, params=None, **kw: iter(
            [{}] * (experiments or {}).get(params["reference_dataset"], 0)
        )

    migrator.source.get.side_effect = reader(source_datasets)
    migrator.dest.get.side_effect = reader(dest_datasets)
    migrator.source.get_paginated.side_effect = sessions(source_experiments)
    migrator.dest.get_paginated.side_effect = sessions(dest_experiments)
    migrator.find_existing_dataset.side_effect = (
        lambda name, dataset_id: dataset_id if dataset_id in dest_datasets else None
    )
    return migrator


def test_matching_copy_passes():
    migrator = _migrator({"d1": {"example_count": 5}}, {"d1": {"example_count": 5}}, {"d1": 2}, {"d1": 2})

    [row] = compare_datasets(migrator, [{"id": "d1", "name": "eval"}], include_experiments=True)

    assert row.matches


def test_empty_copy_and_missing_experiments_are_reported():
    migrator = _migrator(
        {"d1": {"example_count": 2104}}, {"d1": {"example_count": 0}}, {"d1": 6}, {"d1": 0}
    )

    [row] = compare_datasets(migrator, [{"id": "d1", "name": "eval"}], include_experiments=True)

    assert not row.matches
    assert (row.source_examples, row.dest_examples) == (2104, 0)
    assert (row.source_experiments, row.dest_experiments) == (6, 0)


def test_dataset_missing_on_destination_is_reported():
    migrator = _migrator({"d1": {"example_count": 3}}, {})

    [row] = compare_datasets(migrator, [{"id": "d1", "name": "eval"}])

    assert not row.matches
    assert row.dest_id is None
    assert row.source_experiments is None


def test_lookup_error_is_reported_not_raised():
    migrator = _migrator({"d1": {"example_count": 3}}, {"d1": {"example_count": 3}})
    migrator.find_existing_dataset.side_effect = APIError("name conflict", status_code=409)

    [row] = compare_datasets(migrator, [{"id": "d1", "name": "eval", "example_count": 3}])

    assert not row.matches
    assert "name conflict" in row.error


def test_report_returns_the_number_of_differing_datasets():
    from langsmith_migrator.cli.main import _report_dataset_comparison

    migrator = _migrator(
        {"d1": {"example_count": 1}, "d2": {"example_count": 4}},
        {"d1": {"example_count": 1}, "d2": {"example_count": 0}},
    )

    differing = _report_dataset_comparison(
        migrator, [{"id": "d1", "name": "a"}, {"id": "d2", "name": "b"}], False
    )

    assert differing == 1
