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


def _items_server(pages_by_status):
    """Serve the items API: each status has a list of pages, chained by cursor."""

    def get(endpoint, params=None):
        params = params or {}
        pages = pages_by_status.get(params.get("status"), [[]])
        index = int(params.get("cursor") or 0)
        next_cursor = str(index + 1) if index + 1 < len(pages) else None
        return _items_page(pages[index], next_cursor)

    return get


@pytest.fixture
def migrator(sample_config):
    source = _client()
    dest = _client()
    # One post worker keeps batch order deterministic; concurrency has its own tests.
    sample_config.migration.feedback_workers = 1
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
        "assumed_missing": 0,
        "errors": [("run-2", "run not found on destination")],
        "errors_truncated": 0,
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


def test_items_api_lists_every_active_item_and_adds_by_key(migrator):
    """Both active statuses are listed, so the caller's own reviews hide nothing."""
    migrator.source.get.side_effect = _items_server(
        {
            # the empty page still has a next cursor, so listing goes on past it
            "needs_my_review": [[_run(1)], [], [_run(2)]],
            # run-1 was reviewed mid-listing, so it is listed twice but copied once
            "needs_others_review": [[_run(3), _run(1)]],
            "archived": [[_run(4)]],  # review complete: not copied
        }
    )
    migrator.dest.get.side_effect = _items_server({})

    result = migrator.migrate_queue_runs("q-src", "q-dst")

    assert result["total"] == 3
    assert result["added"] == 3
    listing = migrator.source.get.call_args_list[1:]  # after the support probe
    assert {c.args[0] for c in listing} == {"/platform/annotation-queues/q-src/items"}
    assert listing[0].args[1] == {"status": "needs_my_review", "item_type": "RUN", "page_size": 100}
    assert [(c.args[1]["status"], c.args[1].get("cursor")) for c in listing] == [
        ("needs_my_review", None),
        ("needs_my_review", "1"),
        ("needs_my_review", "2"),
        ("needs_others_review", None),
    ]
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
    migrator.source.get.side_effect = _items_server({"needs_my_review": [[_run(1)]]})

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
    assert migrator._items_support == {}


def test_no_deprecated_fallback_refuses_the_old_listing_on_the_source(migrator):
    migrator.config.migration.no_deprecated_fallback = True
    _serve(migrator, [_run(1)])  # the source predates the items API
    migrator.dest.get.side_effect = _items_server({})  # the destination has it

    with pytest.raises(APIError, match="source instance.*MIGRATION_NO_DEPRECATED_FALLBACK"):
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


# ---------------------------------------------------------------------------
# Parallel posting and the cached destination index
# ---------------------------------------------------------------------------


def test_batches_post_concurrently_and_all_runs_are_counted(migrator):
    import threading
    import time

    migrator.config.migration.feedback_workers = 4
    _serve(migrator, [_run(i % 9 + 1) | {"id": f"run-{i}"} for i in range(40)])
    active = {"now": 0, "peak": 0}
    guard = threading.Lock()

    def slow_post(endpoint, body):
        with guard:
            active["now"] += 1
            active["peak"] = max(active["peak"], active["now"])
        time.sleep(0.02)
        with guard:
            active["now"] -= 1
        return {}

    migrator.dest.post.side_effect = slow_post

    result = migrator.migrate_queue_runs("q-src", "q-dst", batch_size=4)

    assert result["total"] == 40
    assert result["added"] == 40
    assert result["failed"] == 0
    assert migrator.dest.post.call_count == 10
    assert 1 < active["peak"] <= 4


def test_concurrent_per_run_replay_counts_failures(migrator):
    migrator.config.migration.feedback_workers = 4
    _serve(migrator, [_run(i % 9 + 1) | {"id": f"run-{i}"} for i in range(8)])

    def post(endpoint, body):
        ids = [k["run_id"] for k in body]
        if len(ids) > 1:
            raise APIError("batch rejected", status_code=422)
        if ids[0] in {"run-1", "run-6"}:
            raise APIError("run not found", status_code=404)
        return {}

    migrator.dest.post.side_effect = post

    result = migrator.migrate_queue_runs("q-src", "q-dst", batch_size=2)

    assert result["added"] == 6
    assert result["failed"] == 2
    assert sorted(run_id for run_id, _ in result["errors"]) == ["run-1", "run-6"]


def test_unexpected_post_error_stops_the_copy(migrator):
    migrator.config.migration.feedback_workers = 4
    _serve(migrator, [_run(i % 9 + 1) | {"id": f"run-{i}"} for i in range(8)])
    migrator.dest.post.side_effect = RuntimeError("connection pool broke")

    with pytest.raises(RuntimeError):
        migrator.migrate_queue_runs("q-src", "q-dst", batch_size=2)


def test_destination_queues_are_listed_once_per_workspace(migrator):
    migrator.dest.get_paginated.return_value = []
    migrator.dest.get_paginated.side_effect = lambda *a, **k: iter(
        [{"id": "dst-1", "name": "One"}, {"id": "dst-2", "name": "Two"}]
    )

    migrator.dest.session.headers = {"X-Tenant-Id": "ws-a"}
    assert migrator.find_existing_queue("One") == "dst-1"
    assert migrator.find_existing_queue("Two") == "dst-2"
    assert migrator.find_existing_queue("Missing") is None
    assert migrator.dest.get_paginated.call_count == 1

    migrator.dest.session.headers = {"X-Tenant-Id": "ws-b"}
    migrator.find_existing_queue("One")
    assert migrator.dest.get_paginated.call_count == 2


def test_created_queue_is_found_without_relisting(migrator):
    migrator.dest.get_paginated.side_effect = lambda *a, **k: iter([])
    migrator.dest.post.return_value = {"id": "dst-new"}

    new_id = migrator.create_queue({"id": "src-1", "name": "Fresh"})

    assert new_id == "dst-new"
    assert migrator.find_existing_queue("Fresh") == "dst-new"
    assert migrator.dest.get_paginated.call_count == 1


def test_failed_destination_page_is_not_cached(migrator):
    """A failed page through the real paginator must raise, not cache a partial index."""
    from langsmith_migrator.core.api_client import EnhancedAPIClient

    migrator.dest.get_paginated.side_effect = (
        lambda endpoint, **kw: EnhancedAPIClient.get_paginated(migrator.dest, endpoint, **kw)
    )
    pages = {"n": 0}

    def get(endpoint, params=None):
        pages["n"] += 1
        if pages["n"] == 1:
            raise APIError("503 upstream", status_code=503)
        return [{"id": "dst-1", "name": "One"}]

    migrator.dest.get.side_effect = get

    with pytest.raises(APIError):
        migrator.find_existing_queue("One")
    assert migrator.find_existing_queue("One") == "dst-1"
    assert migrator.dest.get_paginated.call_args.kwargs.get("raise_errors") is True


def test_failed_listing_fails_the_queue_instead_of_creating_a_duplicate(migrator):
    migrator.dest.get_paginated.side_effect = APIError("503 upstream", status_code=503)

    with pytest.raises(APIError):
        migrator.create_queue({"id": "src-1", "name": "Existing"})
    migrator.dest.post.assert_not_called()


def test_same_named_queues_create_one_destination_queue(migrator):
    """Concurrent workers must not both create a queue for the same name."""
    import threading
    import time
    from concurrent.futures import ThreadPoolExecutor

    migrator.dest.get_paginated.side_effect = lambda *a, **k: iter([])
    created = []
    guard = threading.Lock()

    def post(endpoint, body):
        time.sleep(0.02)  # widen the window between lookup and create
        with guard:
            created.append(body["name"])
            return {"id": f"dst-{len(created)}"}

    migrator.dest.post.side_effect = post

    with ThreadPoolExecutor(max_workers=4) as pool:
        ids = list(pool.map(
            migrator.create_queue,
            [{"id": f"src-{i}", "name": "Same"} for i in range(4)],
        ))

    assert created == ["Same"]
    assert set(ids) == {"dst-1"}


# ---------------------------------------------------------------------------
# Rejected batches: halving, the missing-run streak limit, the error cap
# ---------------------------------------------------------------------------


def _many(n):
    return [{"id": f"run-{i}", "session_id": "sess-1", "start_time": "2026-01-01T00:00:00"}
            for i in range(n)]


def _reject(missing_ids, error_cls=NotFoundError, status=404):
    """dest.post that rejects any batch containing a missing run."""
    def post(endpoint, body):
        if any(k["run_id"] in missing_ids for k in body):
            raise error_cls("run not found", status_code=status)
        return {}
    return post


def test_one_missing_run_is_found_by_halving(migrator):
    _serve(migrator, _many(100))
    migrator.dest.post.side_effect = _reject({"run-37"})

    result = migrator.migrate_queue_runs("q-src", "q-dst")

    assert result["added"] == 99
    assert result["failed"] == 1
    assert result["errors"] == [("run-37", "run not found")]
    # One batch, then two halves per level down to the missing run: far below
    # the 101 requests of a one-by-one retry.
    assert migrator.dest.post.call_count <= 15


def test_missing_streak_records_rejected_batches_whole(migrator):
    migrator.config.migration.queue_missing_streak = 4
    _serve(migrator, _many(40))
    migrator.dest.post.side_effect = _reject({f"run-{i}" for i in range(40)})

    result = migrator.migrate_queue_runs("q-src", "q-dst", batch_size=8)

    assert result["added"] == 0
    assert result["failed"] == 40
    assert result["assumed_missing"] > 0
    # Splitting every batch of 8 would cost 15 requests each (75 in total).
    assert migrator.dest.post.call_count < 40


def test_accepted_batch_ends_the_missing_streak(migrator):
    migrator.config.migration.queue_missing_streak = 2
    runs = _many(12)
    _serve(migrator, runs)
    # runs 0-3 missing (trips the streak), 4-7 fine (resets it), 8 missing alone.
    migrator.dest.post.side_effect = _reject({"run-0", "run-1", "run-2", "run-3", "run-8"})

    result = migrator.migrate_queue_runs("q-src", "q-dst", batch_size=4)

    assert result["added"] == 7
    assert result["failed"] == 5
    # run-0 and run-1 are confirmed one by one, then run-2/run-3 are assumed as a
    # pair. The accepted batch resets the streak, so run-8 is found by splitting
    # and run-9 is still linked.
    assert result["assumed_missing"] == 2
    assert ("run-8", "run not found") in result["errors"]


def test_streak_only_counts_not_found(migrator):
    migrator.config.migration.queue_missing_streak = 1
    _serve(migrator, _many(8))
    migrator.dest.post.side_effect = _reject(
        {f"run-{i}" for i in range(8)}, error_cls=APIError, status=422
    )

    result = migrator.migrate_queue_runs("q-src", "q-dst", batch_size=4)

    assert result["failed"] == 8
    assert result["assumed_missing"] == 0


def test_streak_limit_zero_always_splits(migrator):
    migrator.config.migration.queue_missing_streak = 0
    _serve(migrator, _many(16))
    migrator.dest.post.side_effect = _reject({f"run-{i}" for i in range(16)})

    result = migrator.migrate_queue_runs("q-src", "q-dst", batch_size=8)

    assert result["failed"] == 16
    assert result["assumed_missing"] == 0
    assert len(result["errors"]) == 16


def test_stored_errors_are_capped(migrator):
    from langsmith_migrator.core.migrators.annotation_queue import MAX_STORED_RUN_ERRORS

    migrator.config.migration.queue_missing_streak = 0
    _serve(migrator, _many(250))
    migrator.dest.post.side_effect = _reject({f"run-{i}" for i in range(250)})

    result = migrator.migrate_queue_runs("q-src", "q-dst")

    assert result["failed"] == 250
    assert len(result["errors"]) == MAX_STORED_RUN_ERRORS
    assert result["errors_truncated"] == 250 - MAX_STORED_RUN_ERRORS

