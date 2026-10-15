"""--preserve-ids creates resources under their source IDs and matches re-runs by ID."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from langsmith_migrator.core.api_client import EnhancedAPIClient, NotFoundError
from langsmith_migrator.core.migrators import DatasetMigrator, ExperimentMigrator, FeedbackMigrator
from langsmith_migrator.utils.retry import APIError


def _client(url: str) -> Mock:
    client = Mock(spec=EnhancedAPIClient)
    client.base_url = url
    client.session = Mock()
    client.session.headers = {}
    return client


@pytest.fixture
def preserving_config(sample_config):
    sample_config.migration.preserve_ids = True
    sample_config.migration.skip_existing = False
    sample_config.migration.dry_run = False
    return sample_config


def test_dataset_is_created_with_its_source_id(preserving_config, migration_state):
    source, dest = _client("https://s/api/v1"), _client("https://d/api/v1")
    dest.get = Mock(side_effect=NotFoundError("missing"))
    dest.post = Mock(return_value={"id": "ds-1"})
    migrator = DatasetMigrator(source, dest, migration_state, preserving_config)

    created = migrator.create_dataset({"id": "ds-1", "name": "golden"})

    assert created == "ds-1"
    assert dest.post.call_args.args[1]["id"] == "ds-1"


def test_existing_dataset_is_matched_by_id_not_name(preserving_config, migration_state):
    source, dest = _client("https://s/api/v1"), _client("https://d/api/v1")
    dest.get = Mock(return_value={"id": "ds-1", "name": "renamed on destination"})
    migrator = DatasetMigrator(source, dest, migration_state, preserving_config)

    assert migrator.find_existing_dataset("golden", "ds-1") == "ds-1"
    dest.get.assert_called_once_with("/datasets/ds-1")


def test_same_name_under_another_id_is_a_conflict(preserving_config, migration_state):
    source, dest = _client("https://s/api/v1"), _client("https://d/api/v1")

    def get(endpoint, params=None):
        if endpoint == "/datasets/ds-1":
            raise NotFoundError("missing")
        return [{"id": "ds-other", "name": "golden"}]

    dest.get = Mock(side_effect=get)
    migrator = DatasetMigrator(source, dest, migration_state, preserving_config)

    with pytest.raises(APIError, match="ds-other"):
        migrator.find_existing_dataset("golden", "ds-1")


def test_examples_keep_their_ids_and_rerun_matches_by_id(preserving_config, migration_state):
    source, dest = _client("https://s/api/v1"), _client("https://d/api/v1")
    migrator = DatasetMigrator(source, dest, migration_state, preserving_config)
    existing = {"id": "ex-1", "inputs": {"q": "a"}, "outputs": {"a": 1}, "metadata": {}}
    new = {"id": "ex-2", "inputs": {"q": "a"}, "outputs": {"a": 2}, "metadata": {}}
    migrator.stream_examples = lambda dataset_id: iter([existing, new])
    dest.get_paginated = Mock(return_value=iter([existing]))
    dest.post_batch = Mock(return_value=[{"id": "ex-2"}])

    mapping = migrator.migrate_examples_streaming("ds-1", "ds-1", upsert=True)

    assert mapping == {"ex-1": "ex-1", "ex-2": "ex-2"}
    posted = dest.post_batch.call_args.args[1]
    assert [example["id"] for example in posted] == ["ex-2"]


def test_experiment_and_runs_keep_source_ids_and_dotted_order(preserving_config, migration_state):
    source, dest = _client("https://s/api/v1"), _client("https://d/api/v1")
    migrator = ExperimentMigrator(source, dest, None, preserving_config)
    dest.get = Mock(side_effect=NotFoundError("missing"))
    dest.get_paginated = Mock(return_value=iter([]))
    dest.post = Mock(return_value={"id": "exp-1"})

    created = migrator.create_experiment(
        {"id": "exp-1", "name": "exp", "start_time": "2026-01-01T00:00:00+00:00"}, "ds-1"
    )
    assert created == "exp-1"
    assert dest.post.call_args.args[1]["id"] == "exp-1"

    dotted = "20260101T000000000000Zrun-root.20260101T000001000000Zrun-child"
    runs = [
        {"id": "run-root", "name": "root", "run_type": "chain", "session_id": "exp-1",
         "trace_id": "run-root", "dotted_order": dotted.split(".")[0],
         "reference_example_id": "ex-1"},
        {"id": "run-child", "name": "child", "run_type": "llm", "session_id": "exp-1",
         "trace_id": "run-root", "parent_run_id": "run-root", "dotted_order": dotted},
    ]
    source.post = Mock(return_value={"runs": runs, "cursors": {}})
    dest.post = Mock(return_value={})

    total, mapping, failed = migrator.migrate_runs_streaming(
        ["exp-1"], {"experiments": {"exp-1": "exp-1"}, "examples": {"ex-1": "ex-1"}}
    )

    assert (total, failed) == (2, 0)
    assert mapping == {"run-root": "run-root", "run-child": "run-child"}
    posted = dest.post.call_args.args[1]["post"]
    assert [run["id"] for run in posted] == ["run-root", "run-child"]
    assert posted[1]["parent_run_id"] == "run-root"
    assert posted[1]["dotted_order"] == dotted
    assert posted[0]["reference_example_id"] == "ex-1"


def test_feedback_keeps_source_id_and_trace(preserving_config, migration_state):
    source, dest = _client("https://s/api/v1"), _client("https://d/api/v1")
    migrator = FeedbackMigrator(source, dest, migration_state, preserving_config)
    feedback = {"id": "fb-1", "run_id": "run-1", "trace_id": "run-1", "key": "correctness", "score": 1}
    source.get = Mock(side_effect=lambda ep, params: [feedback] if params.get("offset", 0) == 0 else [])
    dest.post = Mock(return_value={})

    found, migrated = migrator.migrate_feedback_for_experiments({"exp-1": "exp-1"}, {"run-1": "run-1"})

    assert (found, migrated) == (1, 1)
    payload = dest.post.call_args.args[1]
    assert payload["id"] == "fb-1"
    assert payload["run_id"] == "run-1"
