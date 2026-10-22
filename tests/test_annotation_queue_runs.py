"""Tests for copying annotation queue runs (queue items) to the destination."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from langsmith_migrator.core.api_client import APIError, EnhancedAPIClient, NotFoundError
from langsmith_migrator.core.migrators import AnnotationQueueMigrator


def _client() -> Mock:
    client = Mock(spec=EnhancedAPIClient)
    client.session = Mock()
    client.session.headers = {}
    return client


def _run(i: int, session: str = "sess-1") -> dict:
    return {"id": f"run-{i}", "session_id": session, "start_time": f"2026-01-0{i}T00:00:00"}


@pytest.fixture
def migrator(sample_config):
    source = _client()
    dest = _client()
    return AnnotationQueueMigrator(source, dest, None, sample_config)


def test_migrate_queue_runs_posts_keys_in_batches(migrator):
    migrator.source.get_paginated.return_value = iter([_run(1), _run(2), _run(3)])

    result = migrator.migrate_queue_runs("q-src", "q-dst", batch_size=2)

    assert result["total"] == 3
    assert result["added"] == 3
    assert result["failed"] == 0
    migrator.source.get_paginated.assert_called_once_with(
        "/annotation-queues/q-src/runs", page_size=100
    )
    calls = migrator.dest.post.call_args_list
    assert [c.args[0] for c in calls] == ["/annotation-queues/q-dst/runs/by-key"] * 2
    assert calls[0].args[1] == [
        {"run_id": "run-1", "session_id": "sess-1", "start_time": "2026-01-01T00:00:00"},
        {"run_id": "run-2", "session_id": "sess-1", "start_time": "2026-01-02T00:00:00"},
    ]
    assert calls[1].args[1] == [
        {"run_id": "run-3", "session_id": "sess-1", "start_time": "2026-01-03T00:00:00"},
    ]


def test_migrate_queue_runs_remaps_session_ids(migrator):
    migrator.source.get_paginated.return_value = iter([_run(1, "old-proj")])

    migrator.migrate_queue_runs("q-src", "q-dst", session_id_map={"old-proj": "new-proj"})

    body = migrator.dest.post.call_args.args[1]
    assert body[0]["session_id"] == "new-proj"
    assert body[0]["run_id"] == "run-1"


def test_migrate_queue_runs_falls_back_to_run_id_list_when_by_key_missing(migrator):
    migrator.source.get_paginated.return_value = iter([_run(1), _run(2)])
    migrator.dest.post.side_effect = [
        NotFoundError("Not found: /annotation-queues/q-dst/runs/by-key", status_code=404),
        {},
    ]

    result = migrator.migrate_queue_runs("q-src", "q-dst")

    assert result["added"] == 2
    endpoints = [c.args[0] for c in migrator.dest.post.call_args_list]
    assert endpoints == [
        "/annotation-queues/q-dst/runs/by-key",
        "/annotation-queues/q-dst/runs",
    ]
    assert migrator.dest.post.call_args_list[1].args[1] == ["run-1", "run-2"]


def test_migrate_queue_runs_replays_rejected_batch_per_run(migrator):
    migrator.source.get_paginated.return_value = iter([_run(1), _run(2)])
    migrator.dest.post.side_effect = [
        APIError("batch rejected", status_code=422),
        {},
        APIError("run not found on destination", status_code=404),
    ]

    result = migrator.migrate_queue_runs("q-src", "q-dst")

    assert result == {
        "total": 2,
        "added": 1,
        "skipped": 0,
        "failed": 1,
        "errors": [("run-2", "run not found on destination")],
    }
    per_run = [c.args[1] for c in migrator.dest.post.call_args_list[1:]]
    assert [body[0]["run_id"] for body in per_run] == ["run-1", "run-2"]


def test_migrate_queue_runs_skips_runs_without_key_fields(migrator):
    migrator.source.get_paginated.return_value = iter(
        [{"id": "run-x", "session_id": "sess-1"}, _run(1)]
    )

    result = migrator.migrate_queue_runs("q-src", "q-dst")

    assert result["skipped"] == 1
    assert result["added"] == 1
    assert result["errors"] == [("run-x", "missing run key fields")]
    assert migrator.dest.post.call_count == 1


def test_migrate_queue_runs_dry_run_does_not_post(migrator):
    migrator.config.migration.dry_run = True
    migrator.source.get_paginated.return_value = iter([_run(1), _run(2)])

    result = migrator.migrate_queue_runs("q-src", "q-dst")

    assert result["added"] == 2
    migrator.dest.post.assert_not_called()


def test_migrate_queue_runs_empty_queue(migrator):
    migrator.source.get_paginated.return_value = iter([])

    result = migrator.migrate_queue_runs("q-src", "q-dst")

    assert result["total"] == 0
    migrator.dest.post.assert_not_called()
