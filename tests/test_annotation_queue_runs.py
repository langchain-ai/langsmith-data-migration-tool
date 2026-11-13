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


def _pages(*pages):
    """Serve successive run pages, then an empty page, from a server without the items API."""
    remaining = list(pages) + [[]]

    def get(endpoint, params=None):
        if "/platform/" in endpoint:
            raise NotFoundError(f"Not found: {endpoint}", status_code=404)
        if not endpoint.endswith("/runs"):
            return {"id": endpoint.rsplit("/", 1)[-1]}  # the queue itself exists
        return remaining.pop(0)

    return get


def _run_list_calls(migrator):
    """The source calls that list queue runs, without the items API probe."""
    return [c for c in migrator.source.get.call_args_list if c.args[0].endswith("/runs")]


def _items_page(runs, next_cursor=None):
    return {
        "items": [
            {
                "id": f"item-{r['id']}",
                "item_type": "RUN",
                "run_id": r["id"],
                "project_id": r["session_id"],
                "start_time": r["start_time"],
            }
            for r in runs
        ],
        "next_cursor": next_cursor,
    }


@pytest.fixture
def migrator(sample_config):
    source = _client()
    dest = _client()
    m = AnnotationQueueMigrator(source, dest, None, sample_config)
    # default: both instances predate the items API, and each test sets the
    # single page of runs the source serves
    m.source.get.side_effect = _pages([])
    m.dest.get.side_effect = _pages()
    return m


def _serve(migrator, runs):
    migrator.source.get.side_effect = _pages(runs)


def test_iter_queue_runs_pages_with_offset_and_stops_on_short_page(migrator):
    first = [_run(i) for i in range(1, 4)]
    migrator.source.get.side_effect = _pages(first, [_run(9)])
    import langsmith_migrator.core.migrators.annotation_queue as aq

    original = aq.QUEUE_RUNS_BATCH_SIZE
    aq.QUEUE_RUNS_BATCH_SIZE = 3
    try:
        runs = list(migrator.iter_queue_runs("q-src"))
    finally:
        aq.QUEUE_RUNS_BATCH_SIZE = original

    assert [r["id"] for r in runs] == ["run-1", "run-2", "run-3", "run-9"]
    calls = _run_list_calls(migrator)
    assert calls[0].args == ("/annotation-queues/q-src/runs", {"limit": 3, "offset": 0})
    assert calls[1].args == ("/annotation-queues/q-src/runs", {"limit": 3, "offset": 3})
    assert len(calls) == 2


def test_iter_queue_runs_propagates_listing_errors(migrator):
    migrator.source.get.side_effect = APIError("forbidden", status_code=403)

    with pytest.raises(APIError):
        list(migrator.iter_queue_runs("q-src"))
    migrator.dest.post.assert_not_called()


def test_migrate_queue_runs_raises_when_listing_fails(migrator):
    migrator.source.get.side_effect = APIError("forbidden", status_code=403)

    with pytest.raises(APIError):
        migrator.migrate_queue_runs("q-src", "q-dst")


def test_migrate_queue_runs_posts_keys_in_batches(migrator):
    _serve(migrator, [_run(1), _run(2), _run(3)])

    result = migrator.migrate_queue_runs("q-src", "q-dst", batch_size=2)

    assert result["total"] == 3
    assert result["added"] == 3
    assert result["failed"] == 0
    assert _run_list_calls(migrator)[0].args == (
        "/annotation-queues/q-src/runs",
        {"limit": 100, "offset": 0},
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
    _serve(migrator, [_run(1, "old-proj")])

    migrator.migrate_queue_runs("q-src", "q-dst", session_id_map={"old-proj": "new-proj"})

    body = migrator.dest.post.call_args.args[1]
    assert body[0]["session_id"] == "new-proj"
    assert body[0]["run_id"] == "run-1"


def test_migrate_queue_runs_falls_back_to_run_id_list_when_by_key_missing(migrator):
    _serve(migrator, [_run(1), _run(2)])
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
    _serve(migrator, [_run(1), _run(2)])
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
    _serve(migrator, [{"id": "run-x", "session_id": "sess-1"}, _run(1)])

    result = migrator.migrate_queue_runs("q-src", "q-dst")

    assert result["skipped"] == 1
    assert result["added"] == 1
    assert result["errors"] == [("run-x", "missing run key fields")]
    assert migrator.dest.post.call_count == 1


def test_migrate_queue_runs_dry_run_does_not_post(migrator):
    migrator.config.migration.dry_run = True
    _serve(migrator, [_run(1), _run(2)])

    result = migrator.migrate_queue_runs("q-src", "q-dst")

    assert result["added"] == 2
    migrator.dest.post.assert_not_called()


def test_migrate_queue_runs_empty_queue(migrator):
    _serve(migrator, [])

    result = migrator.migrate_queue_runs("q-src", "q-dst")

    assert result["total"] == 0
    migrator.dest.post.assert_not_called()


# --- queue items API (replaces the deprecated /runs endpoints) ---------------


def test_items_api_is_used_for_listing_and_adding_when_both_sides_support_it(migrator):
    pages = [_items_page([]), _items_page([_run(1), _run(2)], "cur-1"), _items_page([_run(3)])]
    migrator.source.get.side_effect = lambda endpoint, params=None: pages.pop(0)
    migrator.dest.get.side_effect = None
    migrator.dest.get.return_value = _items_page([])

    result = migrator.migrate_queue_runs("q-src", "q-dst")

    assert result["total"] == 3
    assert result["added"] == 3
    listing = migrator.source.get.call_args_list
    assert {c.args[0] for c in listing} == {"/platform/annotation-queues/q-src/items"}
    assert listing[1].args[1] == {"status": "needs_my_review", "item_type": "RUN", "page_size": 100}
    assert listing[2].args[1]["cursor"] == "cur-1"
    endpoint, body = migrator.dest.post.call_args.args
    assert endpoint == "/platform/annotation-queues/q-dst/items"
    assert body == {
        "items": [
            {
                "item_type": "RUN",
                "run_id": f"run-{i}",
                "session_id": "sess-1",
                "start_time": f"2026-01-0{i}T00:00:00",
            }
            for i in (1, 2, 3)
        ]
    }


def test_each_side_is_probed_once_and_falls_back_independently(migrator):
    """A new source and an old destination in the same run."""
    pages = [_items_page([]), _items_page([_run(1)]), _items_page([_run(2)])]
    migrator.source.get.side_effect = lambda endpoint, params=None: pages.pop(0)

    migrator.migrate_queue_runs("q-src", "q-dst")
    migrator.migrate_queue_runs("q-src-2", "q-dst-2")

    assert [c.args[0] for c in migrator.dest.post.call_args_list] == [
        "/annotation-queues/q-dst/runs/by-key",
        "/annotation-queues/q-dst-2/runs/by-key",
    ]
    probes = [c for c in migrator.dest.get.call_args_list if "/platform/" in c.args[0]]
    assert len(probes) == 1


def test_missing_queue_is_not_mistaken_for_a_missing_items_api(migrator):
    migrator.source.get.side_effect = NotFoundError("queue not found", status_code=404)

    with pytest.raises(NotFoundError):
        list(migrator.iter_queue_runs("q-gone"))
    assert migrator.__dict__.get("_items_support", {}) == {}


def test_no_deprecated_fallback_refuses_the_old_endpoints(migrator):
    migrator.config.migration.no_deprecated_fallback = True
    _serve(migrator, [_run(1)])

    with pytest.raises(APIError, match="MIGRATION_NO_DEPRECATED_FALLBACK"):
        migrator.migrate_queue_runs("q-src", "q-dst")
    assert _run_list_calls(migrator) == []
    migrator.dest.post.assert_not_called()


def test_no_deprecated_fallback_stops_before_listing_when_destination_is_old(migrator):
    migrator.config.migration.no_deprecated_fallback = True
    migrator.source.get.side_effect = lambda endpoint, params=None: _items_page([_run(1)])

    with pytest.raises(APIError, match="dest instance"):
        migrator.migrate_queue_runs("q-src", "q-dst")
    migrator.source.get.assert_not_called()
    migrator.dest.post.assert_not_called()
