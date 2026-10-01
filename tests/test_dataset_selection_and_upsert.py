"""--dataset selection on the datasets command and the cheap re-run path for unchanged examples."""

from __future__ import annotations

from unittest.mock import Mock

from langsmith_migrator.cli.main import _filter_datasets
from langsmith_migrator.core.api_client import EnhancedAPIClient
from langsmith_migrator.core.migrators import DatasetMigrator
from langsmith_migrator.core.migrators.dataset import _example_unchanged


def test_filter_datasets_matches_id_or_name_and_reports_missing():
    datasets = [
        {"id": "d1", "name": "Backtest A"},
        {"id": "d2", "name": "Backtest B"},
        {"id": "d3", "name": "Other"},
    ]
    chosen, missing = _filter_datasets(datasets, ["d1", "Backtest B", "nope"])
    assert [d["id"] for d in chosen] == ["d1", "d2"]
    assert missing == ["nope"]


def test_example_unchanged_compares_outputs_and_metadata_only():
    existing = {"id": "x", "inputs": {"q": 1}, "outputs": {"a": 2}, "metadata": {"m": 3}}
    assert _example_unchanged(existing, {"id": "s", "inputs": {"q": 1}, "outputs": {"a": 2}, "metadata": {"m": 3}})
    assert not _example_unchanged(existing, {"outputs": {"a": 99}, "metadata": {"m": 3}})
    assert _example_unchanged({"outputs": None, "metadata": None}, {"outputs": {}, "metadata": {}})


def test_rerun_skips_identical_examples_and_updates_changed_ones(sample_config, migration_state):
    """A re-run used to PATCH every existing example (1.2M requests, 20 h on a real workspace)."""
    source = Mock(spec=EnhancedAPIClient); source.base_url = "https://s/api/v1"; source.session = Mock(); source.session.headers = {}
    dest = Mock(spec=EnhancedAPIClient); dest.base_url = "https://d/api/v1"; dest.session = Mock(); dest.session.headers = {}
    migrator = DatasetMigrator(source, dest, migration_state, sample_config)

    same = {"id": "src-1", "inputs": {"q": "a"}, "outputs": {"a": 1}, "metadata": {}}
    changed = {"id": "src-2", "inputs": {"q": "b"}, "outputs": {"a": 2}, "metadata": {}}
    migrator.stream_examples = lambda dataset_id: iter([same, changed])
    migrator.get_existing_examples = lambda dataset_id: {
        migrator._hash_inputs({"q": "a"}): {"id": "dst-1", "outputs": {"a": 1}, "metadata": {}},
        migrator._hash_inputs({"q": "b"}): {"id": "dst-2", "outputs": {"a": 0}, "metadata": {}},
    }
    migrator.update_example = Mock()

    mapping = migrator.migrate_examples_streaming("src-ds", "dst-ds", upsert=True)

    assert mapping == {"src-1": "dst-1", "src-2": "dst-2"}
    migrator.update_example.assert_called_once()
    assert migrator.update_example.call_args.args[0] == "dst-2"
