"""Feedback replay accounting across repeated passes."""

from __future__ import annotations

from unittest.mock import Mock

from langsmith_migrator.core.api_client import EnhancedAPIClient
from langsmith_migrator.core.migrators import FeedbackMigrator


def _clients(feedbacks: list[dict]):
    source = Mock(spec=EnhancedAPIClient)
    source.base_url = "https://source.example/api/v1"
    source.session = Mock()
    source.session.headers = {}

    def _source_get(endpoint: str, params: dict | None = None):
        if endpoint == "/feedback":
            # One page, then an empty page to end pagination.
            return feedbacks if (params or {}).get("offset", 0) == 0 else []
        raise AssertionError(f"Unexpected source endpoint: {endpoint}")

    source.get.side_effect = _source_get

    dest = Mock(spec=EnhancedAPIClient)
    dest.base_url = "https://dest.example/api/v1"
    dest.session = Mock()
    dest.session.headers = {}
    dest.post.return_value = {}
    return source, dest


def _feedback(idx: int) -> dict:
    return {
        "id": f"fb-{idx}",
        "run_id": f"run-{idx}",
        "key": "correctness",
        "score": 1,
    }


def _migrator(source, dest, sample_config, migration_state):
    return FeedbackMigrator(source, dest, migration_state, sample_config)


def test_first_pass_reports_everything_migrated(sample_config, migration_state):
    feedbacks = [_feedback(1), _feedback(2), _feedback(3)]
    source, dest = _clients(feedbacks)
    migration_state.ensure_item(
        "experiment_exp-src", "experiment", "exp-1", "exp-src", stage="migrate_feedback"
    )
    run_mapping = {f"run-{i}": f"dest-run-{i}" for i in (1, 2, 3)}

    found, accounted = _migrator(source, dest, sample_config, migration_state).\
        migrate_feedback_for_experiments({"exp-src": "exp-dst"}, run_mapping)

    assert (found, accounted) == (3, 3)
    item = migration_state.get_item("experiment_exp-src")
    assert item.metadata["feedback_verified"] is True


def test_second_pass_counts_already_replayed_records(sample_config, migration_state):
    """The false-failure bug: a fully migrated experiment reported as incomplete.

    On a re-run every record is fingerprint-skipped, so counting only records
    created on this pass yielded 0/N and the caller marked the experiment failed
    forever, with no way to recover.
    """
    feedbacks = [_feedback(1), _feedback(2), _feedback(3)]
    run_mapping = {f"run-{i}": f"dest-run-{i}" for i in (1, 2, 3)}
    migration_state.ensure_item(
        "experiment_exp-src", "experiment", "exp-1", "exp-src", stage="migrate_feedback"
    )

    source, dest = _clients(feedbacks)
    _migrator(source, dest, sample_config, migration_state).migrate_feedback_for_experiments(
        {"exp-src": "exp-dst"}, run_mapping
    )
    first_pass_posts = dest.post.call_count

    # Second pass over the same state: nothing new to create.
    source, dest = _clients(feedbacks)
    found, accounted = _migrator(source, dest, sample_config, migration_state).\
        migrate_feedback_for_experiments({"exp-src": "exp-dst"}, run_mapping)

    assert first_pass_posts == 3
    assert dest.post.call_count == 0, "already-replayed feedback must not be re-posted"
    assert (found, accounted) == (3, 3), "an already-complete experiment is not a failure"
    assert migration_state.get_item("experiment_exp-src").metadata["feedback_verified"] is True


def test_partial_pass_still_reports_a_shortfall(sample_config, migration_state):
    """A genuine gap must keep reporting, so the fix cannot mask real failures."""
    feedbacks = [_feedback(1), _feedback(2), _feedback(3)]
    source, dest = _clients(feedbacks)
    migration_state.ensure_item(
        "experiment_exp-src", "experiment", "exp-1", "exp-src", stage="migrate_feedback"
    )
    # Only run-1 made it over, so the other two records have nothing to attach to.
    run_mapping = {"run-1": "dest-run-1"}

    found, accounted = _migrator(source, dest, sample_config, migration_state).\
        migrate_feedback_for_experiments({"exp-src": "exp-dst"}, run_mapping)

    assert found == 3
    assert accounted == 1
    item = migration_state.get_item("experiment_exp-src")
    assert item.metadata.get("feedback_verified") is not True
    issue = next(i for i in migration_state.issue_log if i.code == "feedback_partial_replay")
    assert issue.evidence["unmapped_runs"] == 2


def test_create_failures_are_reported_and_retried_next_pass(sample_config, migration_state):
    """A record whose POST fails stays unfingerprinted, so a later pass retries it."""
    feedbacks = [_feedback(1), _feedback(2)]
    run_mapping = {"run-1": "dest-run-1", "run-2": "dest-run-2"}
    migration_state.ensure_item(
        "experiment_exp-src", "experiment", "exp-1", "exp-src", stage="migrate_feedback"
    )

    source, dest = _clients(feedbacks)
    calls = {"count": 0}

    def _flaky_post(endpoint: str, payload: dict):
        calls["count"] += 1
        if calls["count"] == 1:
            raise RuntimeError("403 from a proxy")
        return {}

    dest.post.side_effect = _flaky_post

    found, accounted = _migrator(source, dest, sample_config, migration_state).\
        migrate_feedback_for_experiments({"exp-src": "exp-dst"}, run_mapping)

    assert (found, accounted) == (2, 1)
    issue = next(i for i in migration_state.issue_log if i.code == "feedback_partial_replay")
    assert issue.evidence["create_failures"] == 1

    # Next pass: the one that succeeded is skipped, the failed one is retried.
    source, dest = _clients(feedbacks)
    found, accounted = _migrator(source, dest, sample_config, migration_state).\
        migrate_feedback_for_experiments({"exp-src": "exp-dst"}, run_mapping)

    assert dest.post.call_count == 1
    assert (found, accounted) == (2, 2)
    assert migration_state.get_item("experiment_exp-src").metadata["feedback_verified"] is True
