"""--dataset selection on the datasets command and the cheap re-run path for unchanged examples."""

from __future__ import annotations

from unittest.mock import Mock

import pytest

from langsmith_migrator.cli.main import _filter_datasets
from langsmith_migrator.core.api_client import EnhancedAPIClient
from langsmith_migrator.core.migrators import DatasetMigrator
from langsmith_migrator.core.migrators.dataset import _example_unchanged
from langsmith_migrator.utils.retry import APIError


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


def _feedback_migrator(sample_config, migration_state, workers=3):
    from langsmith_migrator.core.migrators.feedback import FeedbackMigrator

    sample_config.migration.concurrent_workers = workers
    source = Mock(spec=EnhancedAPIClient); source.base_url = "https://s/api/v1"; source.session = Mock(); source.session.headers = {}
    dest = Mock(spec=EnhancedAPIClient); dest.base_url = "https://d/api/v1"; dest.session = Mock(); dest.session.headers = {}
    return FeedbackMigrator(source, dest, migration_state, sample_config), source, dest


def test_feedback_listing_fetches_pages_concurrently_and_keeps_order(sample_config, migration_state):
    migrator, source, _ = _feedback_migrator(sample_config, migration_state, workers=3)
    total, limit = 250, 100
    records = [{"id": str(i)} for i in range(total)]
    source.get = Mock(side_effect=lambda ep, params: records[params["offset"]:params["offset"] + params["limit"]])

    result = migrator.list_feedback_for_session("exp", limit=limit)

    assert [r["id"] for r in result] == [str(i) for i in range(total)]


def test_feedback_creation_is_concurrent_and_reports_failures_in_order(sample_config, migration_state):
    migrator, _, dest = _feedback_migrator(sample_config, migration_state, workers=4)
    sample_config.migration.dry_run = False
    feedbacks = [{"run_id": "r", "key": f"k{i}"} for i in range(10)]

    def post(endpoint, payload):
        if payload["key"] == "k3":
            raise RuntimeError("boom")

    dest.post = Mock(side_effect=post)

    created, created_feedbacks = migrator.create_feedback_batch(feedbacks)

    assert created == 9
    assert [f["key"] for f in created_feedbacks] == [f"k{i}" for i in range(10) if i != 3]


def test_feedback_fingerprints_are_checkpointed_per_chunk(sample_config, migration_state, monkeypatch):
    """An interruption mid-experiment must not forget feedback already created."""
    from langsmith_migrator.core.migrators import feedback as fb_mod

    monkeypatch.setattr(fb_mod, "FEEDBACK_CHECKPOINT_SIZE", 2)
    migrator, source, dest = _feedback_migrator(sample_config, migration_state, workers=1)
    sample_config.migration.dry_run = False
    source.get = Mock(side_effect=lambda ep, params: [
        {"id": f"f{i}", "run_id": "r1", "key": "k", "score": i} for i in range(5)
    ][params["offset"]:params["offset"] + params["limit"]])
    posts = []

    def post(endpoint, payload):
        posts.append(payload)
        if len(posts) == 4:
            raise KeyboardInterrupt  # simulated interruption during the third chunk

    dest.post = Mock(side_effect=post)
    migrator.persist_state = Mock()

    try:
        migrator.migrate_feedback_for_experiments({"exp": "dexp"}, {"r1": "dr1"})
    except BaseException:
        pass

    # Chunks of 2: the first two complete chunks (4 records, the 4th POST raised) leave
    # only the first chunk checkpointed; the interrupted chunk is not remembered.
    remembered = migration_state.id_mappings.get("feedback_fingerprint", {})
    assert len(remembered) == 2


def test_feedback_workers_override_falls_back_to_migration_workers(sample_config, migration_state):
    migrator, _, _ = _feedback_migrator(sample_config, migration_state, workers=4)
    assert migrator._feedback_workers() == 4
    sample_config.migration.feedback_workers = 16
    assert migrator._feedback_workers() == 16


def _mp_feedback(i, **extra):
    return {"run_id": f"r{i}", "key": f"k{i}", "score": 1, "_fingerprint": f"fp{i}",
            "_trace_id": f"t{i}", "_session_id": "sess", **extra}


def test_multipart_batches_feedback_into_single_requests(sample_config, migration_state):
    import json

    migrator, _, dest = _feedback_migrator(sample_config, migration_state, workers=2)
    sample_config.migration.dry_run = False
    sample_config.migration.feedback_multipart = True
    sample_config.migration.feedback_batch_size = 3
    dest.post_multipart = Mock()
    dest.post = Mock()

    created, done = migrator.create_feedback_batch([_mp_feedback(i) for i in range(7)])

    assert created == 7 and len(done) == 7
    assert dest.post_multipart.call_count == 3  # 3 + 3 + 1
    dest.post.assert_not_called()
    parts = dest.post_multipart.call_args_list[0].args[1]
    names = [n for n, _ in parts]
    assert len(set(names)) == len(names) and all(n.startswith("feedback.") for n in names)
    body = json.loads(parts[0][1])
    assert body["trace_id"] == "t0" and body["session_id"] == "sess" and body["run_id"] == "r0"
    assert "_fingerprint" not in body and body["id"] == names[0].split(".", 1)[1]


def test_multipart_failure_falls_back_to_individual_posts(sample_config, migration_state):
    migrator, _, dest = _feedback_migrator(sample_config, migration_state, workers=1)
    sample_config.migration.dry_run = False
    sample_config.migration.feedback_multipart = True
    dest.post_multipart = Mock(side_effect=APIError("422 bad part", status_code=422))
    dest.post = Mock(side_effect=lambda ep, payload: (_ for _ in ()).throw(RuntimeError("x")) if payload["key"] == "k1" else None)

    created, done = migrator.create_feedback_batch([_mp_feedback(i) for i in range(3)])

    assert created == 2 and [f["key"] for f in done] == ["k0", "k2"]
    assert dest.post.call_count == 3
    # The replay reuses each multipart part's id, so a batch that did land is upserted.
    part_ids = [name.split(".", 1)[1] for name, _ in dest.post_multipart.call_args.args[1]]
    assert [c.args[1]["id"] for c in dest.post.call_args_list] == part_ids


def test_multipart_ids_are_stable_across_resends(sample_config, migration_state):
    migrator, _, dest = _feedback_migrator(sample_config, migration_state, workers=1)
    sample_config.migration.dry_run = False
    sample_config.migration.feedback_multipart = True
    dest.post_multipart = Mock()

    migrator.create_feedback_batch([_mp_feedback(0)])
    migrator.create_feedback_batch([_mp_feedback(0)])

    first, second = (c.args[1][0][0] for c in dest.post_multipart.call_args_list)
    assert first == second


def test_multipart_rate_limit_fails_batch_without_fanning_out(sample_config, migration_state):
    from langsmith_migrator.utils.retry import RateLimitError

    migrator, _, dest = _feedback_migrator(sample_config, migration_state, workers=1)
    sample_config.migration.dry_run = False
    sample_config.migration.feedback_multipart = True
    dest.post_multipart = Mock(side_effect=RateLimitError("429"))
    dest.post = Mock()

    created, done = migrator.create_feedback_batch([_mp_feedback(i) for i in range(3)])

    assert created == 0 and done == []
    dest.post.assert_not_called()


def test_multipart_server_error_fails_batch_without_fanning_out(sample_config, migration_state):
    migrator, _, dest = _feedback_migrator(sample_config, migration_state, workers=1)
    sample_config.migration.dry_run = False
    sample_config.migration.feedback_multipart = True
    dest.post_multipart = Mock(side_effect=APIError("503", status_code=503))
    dest.post = Mock()

    created, _ = migrator.create_feedback_batch([_mp_feedback(i) for i in range(3)])

    assert created == 0
    dest.post.assert_not_called()


def test_multipart_auth_error_raises(sample_config, migration_state):
    from langsmith_migrator.utils.retry import AuthenticationError

    migrator, _, dest = _feedback_migrator(sample_config, migration_state, workers=1)
    sample_config.migration.dry_run = False
    sample_config.migration.feedback_multipart = True
    dest.post_multipart = Mock(side_effect=AuthenticationError("Access denied.", status_code=403))
    dest.post = Mock()

    with pytest.raises(AuthenticationError, match="MIGRATION_FEEDBACK_MULTIPART"):
        migrator.create_feedback_batch([_mp_feedback(0)])
    dest.post.assert_not_called()


def test_ineligible_feedback_skips_multipart(sample_config, migration_state):
    migrator, _, dest = _feedback_migrator(sample_config, migration_state, workers=1)
    sample_config.migration.dry_run = False
    sample_config.migration.feedback_multipart = True
    dest.post_multipart = Mock()
    dest.post = Mock()

    no_trace = {"run_id": "r", "key": "k", "_session_id": "sess"}
    created, _ = migrator.create_feedback_batch([no_trace])

    assert created == 1
    dest.post_multipart.assert_not_called()
    dest.post.assert_called_once()


def test_multipart_off_by_default_and_batch_size_bounded(monkeypatch):
    from langsmith_migrator.utils.config import Config

    monkeypatch.delenv("MIGRATION_FEEDBACK_MULTIPART", raising=False)
    assert Config(source_api_key="a", dest_api_key="b").migration.feedback_multipart is False
    monkeypatch.setenv("MIGRATION_FEEDBACK_MULTIPART", "true")
    monkeypatch.setenv("MIGRATION_FEEDBACK_BATCH_SIZE", "100000")
    cfg = Config(source_api_key="a", dest_api_key="b").migration
    assert cfg.feedback_multipart is True and cfg.feedback_batch_size == 100


def test_post_multipart_sends_json_parts_with_length():
    import requests

    client = EnhancedAPIClient("https://dest.test/api/v1", {"X-API-Key": "k"}, rate_limit_delay=0)
    response = requests.Response()
    response.status_code = 202
    response._content = b"{}"
    response.request = requests.Request("POST", "https://dest.test/api/v1/runs/multipart").prepare()
    client.session.post = Mock(return_value=response)

    client.post_multipart("/runs/multipart", [("feedback.abc", b'{"key":"k"}')])

    url = client.session.post.call_args.args[0]
    files = client.session.post.call_args.kwargs["files"]
    wire = requests.Request("POST", url, files=files).prepare()
    assert wire.headers["Content-Type"].startswith("multipart/form-data; boundary=")
    body = wire.body.decode()
    assert url == "https://dest.test/api/v1/runs/multipart"
    assert 'name="feedback.abc"' in body
    assert "Content-Type: application/json; length=11" in body
    assert '{"key":"k"}' in body
