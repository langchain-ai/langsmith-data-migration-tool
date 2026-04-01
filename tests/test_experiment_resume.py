"""Experiment and feedback resume helper tests."""

from __future__ import annotations

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
