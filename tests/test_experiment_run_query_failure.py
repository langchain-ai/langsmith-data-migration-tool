"""A failed source run query must not look like a successful run stage."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from langsmith_migrator.core.api_client import EnhancedAPIClient
from langsmith_migrator.core.migrators import ExperimentMigrator, FeedbackMigrator
from langsmith_migrator.utils.retry import UpstreamRejectionError


def _clients():
    source = Mock(spec=EnhancedAPIClient)
    source.base_url = "https://source.example/api/v1"
    source.session = Mock()
    source.session.headers = {}

    dest = Mock(spec=EnhancedAPIClient)
    dest.base_url = "https://dest.example/api/v1"
    dest.session = Mock()
    dest.session.headers = {}
    return source, dest


def test_failed_run_query_raises_instead_of_reporting_zero_runs(
    sample_config,
    migration_state,
):
    """Silently returning zero runs left empty experiments on the destination."""
    source, dest = _clients()
    source.post.side_effect = UpstreamRejectionError(
        "403 for /runs/query came from an intermediary", status_code=403
    )
    migration_state.ensure_item(
        "experiment_exp-src", "experiment", "exp-1", "exp-src", stage="migrate_runs"
    )

    migrator = ExperimentMigrator(source, dest, migration_state, sample_config)

    with pytest.raises(UpstreamRejectionError):
        migrator.migrate_runs_streaming(
            ["exp-src"],
            {"experiments": {"exp-src": "exp-dst"}, "examples": {}},
        )

    # Nothing was written to the destination.
    dest.post.assert_not_called()


def test_failed_run_query_records_a_run_query_issue(sample_config, migration_state):
    """The operator needs the real error, not a downstream feedback symptom."""
    source, dest = _clients()
    source.post.side_effect = RuntimeError("connection reset")
    migration_state.ensure_item(
        "experiment_exp-src", "experiment", "exp-1", "exp-src", stage="migrate_runs"
    )

    migrator = ExperimentMigrator(source, dest, migration_state, sample_config)

    with pytest.raises(RuntimeError):
        migrator.migrate_runs_streaming(
            ["exp-src"],
            {"experiments": {"exp-src": "exp-dst"}, "examples": {}},
        )

    codes = [issue.code for issue in migration_state.issue_log]
    assert "run_query_failed" in codes
    issue = next(i for i in migration_state.issue_log if i.code == "run_query_failed")
    assert "connection reset" in issue.evidence["error"]
    assert issue.item_id == "experiment_exp-src"


def test_feedback_is_not_attempted_after_a_failed_run_query(
    sample_config,
    migration_state,
):
    """Regression guard for the misleading `feedback replay incomplete (0/N)` report.

    Runs never made it over, so feedback has no runs to attach to. Previously the
    run failure was swallowed and this surfaced as a feedback problem instead.
    """
    source, dest = _clients()
    source.post.side_effect = RuntimeError("connection reset")
    migration_state.ensure_item(
        "experiment_exp-src", "experiment", "exp-1", "exp-src", stage="migrate_runs"
    )

    migrator = ExperimentMigrator(source, dest, migration_state, sample_config)
    with pytest.raises(RuntimeError):
        migrator.migrate_runs_streaming(
            ["exp-src"],
            {"experiments": {"exp-src": "exp-dst"}, "examples": {}},
        )

    item = migration_state.get_item("experiment_exp-src")
    assert item.metadata.get("runs_migrated", 0) == 0
    assert item.stage != "migrate_feedback"

    # And the feedback migrator would have had no run mapping to work with.
    feedback = FeedbackMigrator(source, dest, migration_state, sample_config)
    assert feedback.state.id_mappings.get("run", {}) == {}
