"""Parallel dataset/experiment migration: throttled state saves, cached lookups, worker pools."""

from __future__ import annotations

import json
import threading
import time
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from langsmith_migrator.core.api_client import APIError
from langsmith_migrator.core.migrators.dataset import DatasetMigrator
from langsmith_migrator.core.migrators.experiment import ExperimentMigrator
from langsmith_migrator.core.migrators.orchestrator import MigrationOrchestrator
from langsmith_migrator.utils.state import MigrationState, StateManager


def _client():
    client = Mock()
    client.session = SimpleNamespace(headers={})
    return client


# --- State saves -------------------------------------------------------------------------


def _manager_with_session(tmp_path) -> StateManager:
    manager = StateManager(tmp_path / "state")
    manager.create_session("https://s", "https://d")
    return manager


def test_throttled_saves_skip_writes_inside_interval_and_flush_on_exit(tmp_path, monkeypatch):
    manager = _manager_with_session(tmp_path)
    writes = []
    real_write = StateManager._write_atomic
    monkeypatch.setattr(
        StateManager,
        "_write_atomic",
        staticmethod(lambda path, payload: (writes.append(payload), real_write(path, payload))),
    )

    with manager.throttled_saves(3600):
        # create_session() just wrote the file, so none of these saves is due.
        for i in range(5):
            manager.current_state.set_mapped_id("examples", f"src-{i}", f"dst-{i}")
            manager.save()
        assert writes == []
        manager.save(force=True)
        assert len(writes) == 1
        manager.current_state.set_mapped_id("examples", "src-4", "dst-4")
        manager.save()

    assert len(writes) == 2  # the flush on exit
    on_disk = json.loads(manager.state_file.read_text())
    assert on_disk["id_mappings"]["examples"]["src-4"] == "dst-4"
    assert manager.min_save_interval == 0.0


def test_throttled_saves_flush_when_block_raises(tmp_path):
    manager = _manager_with_session(tmp_path)

    with pytest.raises(RuntimeError):
        with manager.throttled_saves(3600):
            manager.save()
            manager.current_state.set_mapped_id("dataset", "a", "b")
            manager.save()
            raise RuntimeError("interrupted")

    assert json.loads(manager.state_file.read_text())["id_mappings"]["dataset"] == {"a": "b"}


def test_save_without_throttle_writes_every_time_and_leaves_no_temp_files(tmp_path):
    manager = _manager_with_session(tmp_path)

    manager.current_state.set_mapped_id("dataset", "a", "b")
    manager.save()

    assert json.loads(manager.state_file.read_text())["id_mappings"]["dataset"] == {"a": "b"}
    assert [p.name for p in manager.state_dir.iterdir()] == [manager.state_file.name]
    assert manager.list_sessions()[0]["session_id"] == manager.current_state.session_id


def test_state_can_be_saved_while_workers_add_mappings(tmp_path):
    """Serializing must not see a dict another thread is resizing."""
    manager = _manager_with_session(tmp_path)
    state = manager.current_state
    errors = []

    def writer(prefix):
        for i in range(3000):
            state.set_mapped_id("run", f"{prefix}-{i}", "x")
            if i % 100 == 0:
                state.ensure_item(f"item-{prefix}-{i}", "experiment", "n", f"{prefix}-{i}")

    threads = [threading.Thread(target=writer, args=(n,)) for n in range(4)]
    for t in threads:
        t.start()
    while any(t.is_alive() for t in threads):
        try:
            manager.save()
            state.mapping_snapshot("run")
        except RuntimeError as e:  # "dictionary changed size during iteration"
            errors.append(e)
            break
    for t in threads:
        t.join()

    assert errors == []
    manager.save()
    assert len(json.loads(manager.state_file.read_text())["id_mappings"]["run"]) == 12000


def test_merge_mappings_adds_to_existing_type(migration_state):
    migration_state.set_mapped_id("examples", "a", "1")
    migration_state.merge_mappings("examples", {"b": "2"})
    migration_state.merge_mappings("examples", {})

    assert migration_state.mapping_snapshot("examples") == {"a": "1", "b": "2"}


# --- Experiment lookups ----------------------------------------------------------------


def _experiment_migrator(config, dest_experiments):
    source, dest = _client(), _client()
    dest.get_paginated = Mock(side_effect=lambda *a, **k: iter(list(dest_experiments)))
    return ExperimentMigrator(source, dest, None, config)


def test_existing_experiments_are_listed_once_per_dataset(sample_config):
    migrator = _experiment_migrator(sample_config, [{"id": "e1", "name": "one"}])

    assert migrator.find_existing_experiment("one", "ds-1") == "e1"
    assert migrator.find_existing_experiment("two", "ds-1") is None
    assert migrator.find_existing_experiment("one", "ds-1") == "e1"
    migrator.find_existing_experiment("one", "ds-2")

    assert migrator.dest.get_paginated.call_count == 2


def test_preserved_ids_use_the_cached_listing_and_still_detect_name_conflicts(sample_config):
    sample_config.migration.preserve_ids = True
    migrator = _experiment_migrator(
        sample_config, [{"id": "e1", "name": "one"}, {"id": "other", "name": "taken"}]
    )

    assert migrator.find_existing_experiment("one", "ds-1", "e1") == "e1"
    assert migrator.find_existing_experiment("new", "ds-1", "e2") is None
    with pytest.raises(APIError, match="exists on the destination as other"):
        migrator.find_existing_experiment("taken", "ds-1", "e3")

    migrator.dest.get.assert_not_called()
    assert migrator.dest.get_paginated.call_count == 1


def test_created_experiment_is_added_to_the_cache(sample_config):
    migrator = _experiment_migrator(sample_config, [])
    migrator.dest.post = Mock(return_value={"id": "new-id"})

    migrator.create_experiment({"id": "src", "name": "exp"}, "ds-1")

    assert migrator.find_existing_experiment("exp", "ds-1") == "new-id"
    assert migrator.dest.get_paginated.call_count == 1


def test_listing_failure_falls_back_to_per_experiment_lookups(sample_config):
    sample_config.migration.preserve_ids = True
    migrator = _experiment_migrator(sample_config, [])
    migrator.dest.get_paginated = Mock(side_effect=APIError("boom"))
    migrator.dest.get = Mock(return_value={"id": "e1", "reference_dataset_id": "ds-1"})

    assert migrator.find_existing_experiment("one", "ds-1", "e1") == "e1"
    migrator.dest.get.assert_called_once_with("/sessions/e1")


def test_concurrent_lookups_on_one_dataset_share_one_listing(sample_config):
    migrator = _experiment_migrator(sample_config, [])
    calls = []

    def slow_listing(*args, **kwargs):
        calls.append(1)
        time.sleep(0.05)
        return iter([])

    migrator.dest.get_paginated = Mock(side_effect=slow_listing)
    threads = [
        threading.Thread(target=migrator.find_existing_experiment, args=(f"n{i}", "ds-1"))
        for i in range(6)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert len(calls) == 1


def test_list_experiments_keeps_listing_order(sample_config):
    sample_config.migration.concurrent_workers = 4
    migrator = _experiment_migrator(sample_config, [])
    summaries = [{"id": f"e{i}"} for i in range(8)]
    migrator.source.get_paginated = Mock(return_value=iter(summaries))

    def detail(endpoint):
        exp_id = endpoint.rsplit("/", 1)[1]
        time.sleep(0.01 * (8 - int(exp_id[1:])))  # later IDs finish first
        return {"id": exp_id, "name": exp_id}

    migrator.source.get = Mock(side_effect=detail)

    assert [e["id"] for e in migrator.list_experiments("ds-1")] == [s["id"] for s in summaries]


# --- Orchestrator ----------------------------------------------------------------------


def _orchestrator(config, tmp_path, monkeypatch):
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.orchestrator.EnhancedAPIClient",
        lambda **kwargs: _client(),
    )
    orchestrator = MigrationOrchestrator(config, StateManager(tmp_path / "state"))
    orchestrator.ensure_state()
    return orchestrator


def test_experiments_migrate_concurrently(sample_config, tmp_path, monkeypatch):
    sample_config.migration.concurrent_workers = 2
    orchestrator = _orchestrator(sample_config, tmp_path, monkeypatch)
    experiments = {"ds-1": [{"id": "e1", "name": "one"}], "ds-2": [{"id": "e2", "name": "two"}]}
    monkeypatch.setattr(
        ExperimentMigrator, "list_experiment_summaries", lambda self, ds: experiments[ds]
    )
    monkeypatch.setattr(ExperimentMigrator, "fetch_experiment_details", lambda self, e: e)
    # Both experiments must be in flight at once, or the barrier times out.
    barrier = threading.Barrier(2, timeout=5)
    seen = []

    def resolve(experiment, source_dataset_id, dest_dataset_id, *_):
        barrier.wait()
        seen.append((experiment["id"], source_dataset_id, dest_dataset_id))
        return True, "migrated"

    monkeypatch.setattr(orchestrator, "_resolve_experiment_item", resolve)

    orchestrator._migrate_experiments_for_datasets(
        ["ds-1", "ds-2"], {"ds-1": "dst-1", "ds-2": "dst-2"}
    )

    assert sorted(seen) == [("e1", "ds-1", "dst-1"), ("e2", "ds-2", "dst-2")]


def test_experiment_without_migrated_dataset_is_blocked(sample_config, tmp_path, monkeypatch):
    orchestrator = _orchestrator(sample_config, tmp_path, monkeypatch)
    monkeypatch.setattr(
        ExperimentMigrator,
        "list_experiment_summaries",
        lambda self, ds: [{"id": "e1", "name": "one"}],
    )
    monkeypatch.setattr(ExperimentMigrator, "fetch_experiment_details", lambda self, e: e)
    resolve = Mock()
    monkeypatch.setattr(orchestrator, "_resolve_experiment_item", resolve)

    orchestrator._migrate_experiments_for_datasets(["ds-1"], {})

    resolve.assert_not_called()
    assert orchestrator.state.get_item("experiment_e1").outcome_code == "missing_dataset_dependency"


def test_dataset_run_throttles_saves_and_flushes(sample_config, tmp_path, monkeypatch):
    sample_config.migration.state_save_interval = 3600
    orchestrator = _orchestrator(sample_config, tmp_path, monkeypatch)
    monkeypatch.setattr(
        DatasetMigrator, "get_dataset", lambda self, ds: {"id": ds, "name": ds}
    )
    monkeypatch.setattr(
        DatasetMigrator,
        "migrate_dataset",
        lambda self, ds, include_examples: (f"dst-{ds}", {f"{ds}-ex": f"{ds}-ex-dst"}),
    )
    writes = []
    real_write = StateManager._write_atomic
    monkeypatch.setattr(
        StateManager,
        "_write_atomic",
        staticmethod(lambda path, payload: (writes.append(1), real_write(path, payload))),
    )

    mapping = orchestrator.migrate_datasets_parallel(["a", "b", "c"], include_experiments=False)

    assert mapping == {"a": "dst-a", "b": "dst-b", "c": "dst-c"}
    assert len(writes) <= 2  # one due save plus the flush, not one per dataset
    on_disk = json.loads(orchestrator.state_manager.state_file.read_text())
    assert on_disk["id_mappings"]["examples"]["c-ex"] == "c-ex-dst"
    assert on_disk["items"]["dataset_c"]["status"] == "completed"


# --- Example batches with attachments --------------------------------------------------


def _dataset_migrator(config, state=None):
    dest = _client()
    dest.post_batch = Mock(side_effect=lambda endpoint, payloads, batch_size: [
        {"id": f"new-{p['inputs']['n']}"} for p in payloads
    ])
    return DatasetMigrator(_client(), dest, state, config)


def test_examples_without_attachments_use_the_bulk_endpoint(sample_config, monkeypatch):
    sample_config.migration.concurrent_workers = 2
    migrator = _dataset_migrator(sample_config)
    barrier = threading.Barrier(2, timeout=5)
    uploaded = []

    def upload(dest_dataset_id, entry):
        barrier.wait()  # both attachment examples upload at once
        uploaded.append(entry[0])
        return {entry[0]: f"new-{entry[0]}"}

    monkeypatch.setattr(migrator, "_migrate_attachment_example", upload)
    batch = [
        ("p1", {"inputs": {"n": "p1"}}, {}),
        ("a1", {"inputs": {"n": "a1"}}, {"attachment.x": {"presigned_url": "/x"}}),
        ("p2", {"inputs": {"n": "p2"}}, {}),
        ("a2", {"inputs": {"n": "a2"}}, {"attachment.y": {"presigned_url": "/y"}}),
    ]
    id_mapping = {}

    try:
        created = migrator._process_example_batch("dst-ds", batch, id_mapping)
    finally:
        migrator.close()

    assert created == 4
    assert id_mapping == {"p1": "new-p1", "p2": "new-p2", "a1": "new-a1", "a2": "new-a2"}
    sent = migrator.dest.post_batch.call_args.args[1]
    assert [p["inputs"]["n"] for p in sent] == ["p1", "p2"]
    assert sorted(uploaded) == ["a1", "a2"]


def test_attachment_temp_files_are_removed_when_upload_fails(sample_config, monkeypatch, tmp_path):
    migrator = _dataset_migrator(sample_config)
    temp_file = tmp_path / "att.bin"
    temp_file.write_bytes(b"x")
    monkeypatch.setattr(
        migrator,
        "download_attachments",
        lambda urls: {"attachment.x": ("text/plain", str(temp_file), "x.txt")},
    )
    monkeypatch.setattr(
        migrator, "_sdk_client", Mock(side_effect=RuntimeError("no sdk"))
    )

    created = migrator._process_example_batch(
        "dst-ds", [("a1", {"inputs": {}}, {"attachment.x": {"presigned_url": "/x"}})], {}
    )
    migrator.close()

    assert created == 0
    assert not temp_file.exists()


def test_sdk_client_is_built_once_per_destination_workspace(sample_config, monkeypatch):
    built = []
    monkeypatch.setattr("langsmith.Client", lambda **kwargs: built.append(kwargs) or Mock())
    migrator = _dataset_migrator(sample_config)
    migrator.dest.base_url = "https://d/api/v1"
    migrator.dest.headers = {"X-API-Key": "k"}
    migrator.dest.get = Mock(return_value={})

    migrator.dest.session.headers["X-Tenant-Id"] = "ws-1"
    first = migrator._sdk_client()
    assert migrator._sdk_client() is first
    migrator.dest.session.headers["X-Tenant-Id"] = "ws-2"
    migrator._sdk_client()

    assert [kwargs["workspace_id"] for kwargs in built] == ["ws-1", "ws-2"]


# --- BYOC profile ----------------------------------------------------------------------


def test_byoc_profile_removes_the_per_request_delay(sample_config, monkeypatch):
    from langsmith_migrator.cli.main import _apply_byoc_migration_profile

    monkeypatch.delenv("MIGRATION_RATE_LIMIT_DELAY", raising=False)
    _apply_byoc_migration_profile(sample_config)

    assert sample_config.migration.rate_limit_delay == 0.0


def test_byoc_profile_keeps_an_explicit_delay(sample_config, monkeypatch):
    from langsmith_migrator.cli.main import _apply_byoc_migration_profile

    monkeypatch.setenv("MIGRATION_RATE_LIMIT_DELAY", "0.5")
    sample_config.migration.rate_limit_delay = 0.5
    _apply_byoc_migration_profile(sample_config)

    assert sample_config.migration.rate_limit_delay == 0.5


def test_state_save_interval_env(monkeypatch):
    from langsmith_migrator.utils.config import Config

    monkeypatch.setenv("MIGRATION_STATE_SAVE_INTERVAL", "not-a-number")
    assert Config().migration.state_save_interval == 5.0
    monkeypatch.setenv("MIGRATION_STATE_SAVE_INTERVAL", "0")
    assert Config().migration.state_save_interval == 0.0


def test_migration_state_lock_is_not_serialized(migration_state):
    assert "_lock" not in migration_state.to_dict()
    assert isinstance(MigrationState.from_dict(migration_state.to_dict()).lock, type(threading.RLock()))


# --- Resume safety under throttled saves ------------------------------------------------


def _resumable_on_disk(orchestrator):
    manager = StateManager(orchestrator.state_manager.state_dir)
    state = manager.load_session(orchestrator.state.session_id)
    return {item.id for item in state.get_resume_items()}


def test_dataset_inventory_is_on_disk_before_workers_run(sample_config, tmp_path, monkeypatch):
    sample_config.migration.state_save_interval = 3600
    orchestrator = _orchestrator(sample_config, tmp_path, monkeypatch)
    monkeypatch.setattr(DatasetMigrator, "get_dataset", lambda self, ds: {"id": ds, "name": ds})
    seen_on_disk = []

    def migrate_dataset(self, ds, include_examples):
        seen_on_disk.append(_resumable_on_disk(orchestrator))
        return f"dst-{ds}", {}

    monkeypatch.setattr(DatasetMigrator, "migrate_dataset", migrate_dataset)

    orchestrator.migrate_datasets_parallel(["a", "b"], include_experiments=False)

    assert seen_on_disk[0] == {"dataset_a", "dataset_b"}


def test_experiment_inventory_is_on_disk_before_workers_run(sample_config, tmp_path, monkeypatch):
    sample_config.migration.state_save_interval = 3600
    orchestrator = _orchestrator(sample_config, tmp_path, monkeypatch)
    experiments = [{"id": "e1", "name": "one"}, {"id": "e2", "name": "two"}]
    monkeypatch.setattr(ExperimentMigrator, "list_experiment_summaries", lambda self, ds: experiments)
    monkeypatch.setattr(ExperimentMigrator, "fetch_experiment_details", lambda self, e: e)
    seen_on_disk = []

    def resolve(*args):
        seen_on_disk.append(_resumable_on_disk(orchestrator))
        return True, "migrated"

    monkeypatch.setattr(orchestrator, "_resolve_experiment_item", resolve)

    with orchestrator.state_manager.throttled_saves(3600):
        orchestrator._migrate_experiments_for_datasets(["ds-1"], {"ds-1": "dst-1"})

    assert seen_on_disk[0] >= {"experiment_e1", "experiment_e2"}


def _feedback_migrator(config):
    from langsmith_migrator.core.migrators.feedback import FeedbackMigrator

    dest = _client()
    dest.post = Mock(return_value={})
    return FeedbackMigrator(_client(), dest, None, config)


def _feedback_record():
    return {"run_id": "r1", "key": "correct", "score": 1, "_fingerprint": "fp-1", "_session_id": "s1"}


def test_individual_feedback_uploads_send_a_stable_id(sample_config):
    migrator = _feedback_migrator(sample_config)

    migrator.create_feedback_batch([_feedback_record()])
    migrator.create_feedback_batch([_feedback_record()])

    first, second = (c.args[1] for c in migrator.dest.post.call_args_list)
    assert first["id"] and first["id"] == second["id"]


def test_feedback_conflict_on_a_stable_id_counts_as_created(sample_config):
    from langsmith_migrator.utils.retry import ConflictError

    migrator = _feedback_migrator(sample_config)
    migrator.dest.post.side_effect = ConflictError("exists")

    created, records = migrator.create_feedback_batch([_feedback_record()])

    assert created == 1 and records[0]["_fingerprint"] == "fp-1"


def test_feedback_conflict_without_an_id_is_a_failure(sample_config):
    from langsmith_migrator.utils.retry import ConflictError

    migrator = _feedback_migrator(sample_config)
    migrator.dest.post.side_effect = ConflictError("exists")

    assert migrator.create_feedback({"key": "k", "run_id": "r1"}) is False


def test_pagination_raises_failed_pages_only_when_asked():
    from langsmith_migrator.utils.pagination import PaginationHelper

    def fetch(endpoint, params):
        if params["offset"]:
            raise APIError("page 2 failed")
        return [{"id": str(i)} for i in range(2)]

    assert len(list(PaginationHelper.paginate(fetch, "/x", page_size=2))) == 2
    with pytest.raises(APIError):
        list(PaginationHelper.paginate(fetch, "/x", page_size=2, raise_errors=True))


def test_partial_experiment_listing_is_not_cached(sample_config):
    from langsmith_migrator.utils.pagination import PaginationHelper

    sample_config.migration.preserve_ids = True
    migrator = _experiment_migrator(sample_config, [])
    pages = {"fail": True}

    def fetch(endpoint, params=None):
        if endpoint.startswith("/sessions/"):
            return {"id": "e2", "reference_dataset_id": "ds-1"}
        if params["offset"] and pages["fail"]:
            raise APIError("page 2 failed")
        return [{"id": "e1", "name": "one"}] if not params["offset"] else [{"id": "e2", "name": "two"}]

    migrator.dest.get = Mock(side_effect=fetch)
    migrator.dest.get_paginated = lambda endpoint, params=None, raise_errors=False: (
        PaginationHelper.paginate(fetch, endpoint, params, 1, raise_errors=raise_errors)
    )

    assert migrator.find_existing_experiment("two", "ds-1", "e2") == "e2"
    assert migrator._dest_experiment_index == {}

    pages["fail"] = False
    assert migrator.find_existing_experiment("two", "ds-1", "e2") == "e2"
    assert migrator._dest_experiment_index["ds-1"]["ids"] == {"e1", "e2"}


def test_time_shift_is_on_disk_before_runs_are_sent(sample_config, tmp_path, monkeypatch):
    orchestrator = _orchestrator(sample_config, tmp_path, monkeypatch)
    item = orchestrator.state.ensure_item("experiment_e1", "experiment", "one", "e1")
    experiment = {"id": "e1", "name": "one", "start_time": "2025-01-01T00:00:00+00:00"}

    with orchestrator.state_manager.throttled_saves(3600):
        delta = orchestrator._resolve_experiment_delta(experiment, item)
        on_disk = json.loads(orchestrator.state_manager.state_file.read_text())

    assert on_disk["items"]["experiment_e1"]["metadata"]["time_shift_seconds"] == delta.total_seconds()


def test_interrupt_drops_queued_experiments_and_cancels_clients(sample_config, tmp_path, monkeypatch):
    sample_config.migration.concurrent_workers = 1
    orchestrator = _orchestrator(sample_config, tmp_path, monkeypatch)
    experiments = [{"id": f"e{i}", "name": f"n{i}"} for i in range(6)]
    monkeypatch.setattr(ExperimentMigrator, "list_experiment_summaries", lambda self, ds: experiments)
    monkeypatch.setattr(ExperimentMigrator, "fetch_experiment_details", lambda self, e: e)
    ran = []

    def resolve(experiment, *args):
        ran.append(experiment["id"])
        time.sleep(0.05)
        return True, "migrated"

    monkeypatch.setattr(orchestrator, "_resolve_experiment_item", resolve)

    def interrupted(futures):
        raise KeyboardInterrupt
        yield

    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.orchestrator.as_completed", interrupted
    )

    with pytest.raises(KeyboardInterrupt):
        orchestrator._migrate_experiments_for_datasets(["ds-1"], {"ds-1": "dst-1"})

    assert len(ran) < len(experiments)
    orchestrator.source_client.cancel.assert_called_once()
    orchestrator.dest_client.cancel.assert_called_once()


def test_cancelled_client_stops_requests_and_listings():
    from langsmith_migrator.core.api_client import EnhancedAPIClient, MigrationCancelled

    client = EnhancedAPIClient("https://d/api/v1", {"X-API-Key": "k"}, rate_limit_delay=0)
    client.session.get = Mock()
    client.cancel()

    with pytest.raises(MigrationCancelled):
        client.get("/sessions")
    with pytest.raises(MigrationCancelled):
        list(client.get_paginated("/sessions"))
    client.session.get.assert_not_called()


def test_client_connection_pool_matches_worker_concurrency(sample_config):
    from langsmith_migrator.core.api_client import EnhancedAPIClient

    client = EnhancedAPIClient("https://d/api/v1", {}, pool_maxsize=48)
    assert client.session.get_adapter("https://d/api/v1")._pool_maxsize == 48

    sample_config.migration.concurrent_workers = 4
    sample_config.migration.feedback_workers = 0
    assert MigrationOrchestrator._pool_maxsize(sample_config) == 20
    sample_config.migration.concurrent_workers = 2
    assert MigrationOrchestrator._pool_maxsize(sample_config) == 10


def test_skipped_save_is_written_by_timer_without_another_save(tmp_path):
    manager = _manager_with_session(tmp_path)

    with manager.throttled_saves(0.2):
        manager.current_state.set_mapped_id("dataset", "a", "b")
        manager.save()
        assert "dataset" not in json.loads(manager.state_file.read_text())["id_mappings"]
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline:
            on_disk = json.loads(manager.state_file.read_text())["id_mappings"]
            if on_disk.get("dataset") == {"a": "b"}:
                break
            time.sleep(0.02)

    assert on_disk.get("dataset") == {"a": "b"}
    assert manager._flush_timer is None
