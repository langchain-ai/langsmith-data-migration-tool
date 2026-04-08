"""Experiment and feedback resume helper tests."""

from __future__ import annotations

from unittest.mock import Mock

from langsmith_migrator.core.api_client import EnhancedAPIClient
from langsmith_migrator.core.migrators import ExperimentMigrator, FeedbackMigrator


def test_deterministic_run_id_is_stable(mock_api_client, sample_config, migration_state):
    """Run replay IDs should be stable across retries."""

    migrator = ExperimentMigrator(
        mock_api_client,
        mock_api_client,
        migration_state,
        sample_config,
    )

    first = migrator._deterministic_run_id("run-123")
    second = migrator._deterministic_run_id("run-123")

    assert first == second
    assert first != "run-123"


def test_feedback_fingerprint_is_stable_for_equivalent_payloads(mock_api_client, sample_config, migration_state):
    """Feedback dedupe fingerprints should ignore dictionary key ordering."""

    migrator = FeedbackMigrator(
        mock_api_client,
        mock_api_client,
        migration_state,
        sample_config,
    )

    first = migrator._feedback_fingerprint(
        "experiment-1",
        {"key": "score", "extra": {"b": 2, "a": 1}},
    )
    second = migrator._feedback_fingerprint(
        "experiment-1",
        {"extra": {"a": 1, "b": 2}, "key": "score"},
    )

    assert first == second


def test_migrate_runs_streaming_drops_unmapped_reference_example_id(
    sample_config,
    migration_state,
):
    """Runs should still replay when their example link cannot be preserved."""

    source = Mock(spec=EnhancedAPIClient)
    source.base_url = "https://source.example/api/v1"
    source.session = Mock()
    source.session.headers = {}
    source.post.return_value = {
        "runs": [
            {
                "id": "run-1",
                "name": "Example Run",
                "run_type": "chain",
                "session_id": "exp-src",
                "reference_example_id": "example-src",
            }
        ],
        "cursors": {"next": None},
    }

    captured_payloads: list[dict] = []
    dest = Mock(spec=EnhancedAPIClient)
    dest.base_url = "https://dest.example/api/v1"
    dest.session = Mock()
    dest.session.headers = {}

    def _dest_post(endpoint: str, payload: dict):
        if endpoint == "/runs/batch":
            captured_payloads.append(payload)
            return {}
        raise AssertionError(f"Unexpected destination endpoint: {endpoint}")

    dest.post.side_effect = _dest_post

    migrator = ExperimentMigrator(source, dest, migration_state, sample_config)

    total_runs, run_mapping, failed_runs = migrator.migrate_runs_streaming(
        ["exp-src"],
        {
            "experiments": {"exp-src": "exp-dst"},
            "examples": {},
        },
    )

    assert total_runs == 1
    assert failed_runs == 0
    assert run_mapping["run-1"] == captured_payloads[0]["post"][0]["id"]
    assert "reference_example_id" not in captured_payloads[0]["post"][0]
