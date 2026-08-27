"""Migrator-shell tests for long-lived trace migration.

Only the parts that genuinely need a client live here - pagination, the
diff/ingest/confirm round trip, blob handling and failure isolation. Everything
expressible as a function of its arguments is in ``tests/unit/test_trace_domain.py``.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import pytest

from langsmith_migrator.core.api_client import EnhancedAPIClient
from langsmith_migrator.core.migrators.trace import TraceMigrator, TracePreflightError
from langsmith_migrator.core.trace_domain import Window

NOW = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
WINDOW = Window(NOW - timedelta(days=1), NOW)
DEST = "dest-session"


def _client() -> Mock:
    client = Mock(spec=EnhancedAPIClient)
    client.session = Mock()
    client.session.headers = {}
    return client


def _migrator(sample_config, state=None, **kwargs):
    with patch("langsmith_migrator.core.migrators.trace.Client"):
        migrator = TraceMigrator(_client(), _client(), state, sample_config, **kwargs)
    migrator.dest_ls_client = Mock()
    migrator.dest_ls_client.info.batch_ingest_config = {"size_limit": 100, "size_limit_bytes": 20_000_000}
    return migrator


def _run(rid, trace=None, order=None, **kw):
    run = {
        "id": rid,
        "trace_id": trace or rid,
        "name": "n",
        "run_type": "chain",
        "start_time": "2026-08-25T10:00:00",
        "dotted_order": order or f"20260825T100000000000Z{rid}",
        "session_id": "src-session",
        "trace_tier": "longlived",
        "inputs": {"i": rid},
    }
    run.update(kw)
    return run


def _pages(*pages):
    """A ``/runs/query`` side effect that walks cursors then stops."""
    responses = []
    for index, runs in enumerate(pages):
        last = index == len(pages) - 1
        responses.append({"runs": runs, "cursors": {} if last else {"next": f"c{index}"}})
    return responses


# --------------------------------------------------------------------------
# Query shape
# --------------------------------------------------------------------------
def test_query_paginates_across_cursors(sample_config):
    m = _migrator(sample_config)
    m.source.post.side_effect = _pages([_run("a")], [_run("b")], [_run("c")])
    assert m.long_lived_run_ids(m.source, "src", WINDOW) == {"a", "b", "c"}
    assert m.source.post.call_args_list[1][0][1]["cursor"] == "c0"


def test_query_is_scoped_to_one_session_with_an_explicit_lower_bound(sample_config):
    m = _migrator(sample_config)
    m.source.post.side_effect = _pages([])
    m.long_lived_run_ids(m.source, "only-this-one", WINDOW)
    body = m.source.post.call_args[0][1]
    assert body["session"] == ["only-this-one"]
    assert body["start_time"] < WINDOW.start.isoformat()  # explicit, with skew buffer
    assert "lt(start_time" in body["trace_filter"]


def test_short_lived_runs_are_filtered_client_side(sample_config):
    # The V1 endpoint rejects trace_tier inside trace_filter but returns it on
    # every run, so the predicate has to live here.
    m = _migrator(sample_config)
    m.source.post.side_effect = _pages([_run("keep"), _run("drop", trace_tier="shortlived")])
    assert m.long_lived_run_ids(m.source, "src", WINDOW) == {"keep"}


def test_payload_fetch_is_chunked_without_changing_the_result(sample_config):
    m = _migrator(sample_config)
    with patch("langsmith_migrator.core.migrators.trace._ID_CHUNK", 2):
        m.source.post.side_effect = _pages([_run("a"), _run("b")]) + _pages([_run("c")])
        fetched = [r["id"] for r in m.fetch_runs(m.source, "src", WINDOW, ["a", "b", "c"])]
    assert sorted(fetched) == ["a", "b", "c"]
    assert [len(call[0][1]["id"]) for call in m.source.post.call_args_list] == [2, 1]


# --------------------------------------------------------------------------
# Diff, ingest, confirm
# --------------------------------------------------------------------------
def _wire_slice(m, source_runs, dest_ids, *, confirmed=None):
    """Source population -> dest diff -> payload fetch -> confirm.

    The payload fetch honours the request's ``id`` filter, as the real endpoint
    does, so a test can tell "fetched" from "ingested".
    """
    def source_query(_endpoint, body):
        wanted = set(body.get("id") or [])
        runs = [r for r in source_runs if not wanted or r["id"] in wanted]
        return {"runs": runs, "cursors": {}}

    m.source.post.side_effect = source_query
    confirmed = source_runs if confirmed is None else confirmed
    m.dest.post.side_effect = [
        {"runs": [_run(i) for i in dest_ids], "cursors": {}},
        *[{"runs": confirmed, "cursors": {}} for _ in range(6)],
    ]


def test_only_the_difference_is_ingested(sample_config):
    m = _migrator(sample_config, verify=False)
    _wire_slice(m, [_run("a"), _run("b"), _run("c")], ["b"])
    sent = []
    m.ingest = lambda batch: sent.extend(batch) or []

    report = m.migrate_slice({"id": "src"}, {"id": DEST}, WINDOW)
    assert {p["id"] for p in sent} == {"a", "c"}
    assert (report.source_total, report.ingested, report.already_present) == (3, 2, 1)


def test_runs_the_destination_already_holds_are_not_re_sent(sample_config):
    m = _migrator(sample_config, verify=False)
    _wire_slice(m, [_run("a")], ["a"])
    m.ingest = Mock(return_value=[])
    report = m.migrate_slice({"id": "src"}, {"id": DEST}, WINDOW)
    m.ingest.assert_not_called()
    assert (report.ingested, report.already_present) == (0, 1)


def test_extra_runs_on_the_destination_are_informational(sample_config):
    m = _migrator(sample_config, verify=False)
    _wire_slice(m, [_run("a")], ["a", "stranger"])
    m.ingest = Mock(return_value=[])
    report = m.migrate_slice({"id": "src"}, {"id": DEST}, WINDOW)
    assert report.extra_on_dest == 1 and report.blocked == 0


def test_a_run_that_never_appears_is_blocked(sample_config, migration_state):
    m = _migrator(sample_config, migration_state)
    _wire_slice(m, [_run("a")], [], confirmed=[])
    m.ingest = Mock(return_value=[])
    with patch("langsmith_migrator.core.migrators.trace.time.sleep"):
        report = m.migrate_slice({"id": "src"}, {"id": DEST}, WINDOW)
    assert (report.blocked, report.ingested) == (1, 0)
    assert any(i.code == "run_not_ingested" for i in migration_state.issue_log)


def test_ingest_queue_lag_is_tolerated(sample_config):
    m = _migrator(sample_config)
    source_page = {"runs": [_run("a")], "cursors": {}}
    m.source.post.side_effect = [source_page, source_page]
    m.dest.post.side_effect = [
        {"runs": [], "cursors": {}},          # pre-diff: missing
        {"runs": [], "cursors": {}},          # confirm 1: still not readable
        {"runs": [_run("a")], "cursors": {}},  # confirm 2: readable
        {"runs": [_run("a")], "cursors": {}},  # content sample
    ]
    m.ingest = Mock(return_value=[])
    with patch("langsmith_migrator.core.migrators.trace.time.sleep"):
        report = m.migrate_slice({"id": "src"}, {"id": DEST}, WINDOW)
    assert (report.ingested, report.blocked) == (1, 0)


def test_no_verify_still_computes_the_pre_diff(sample_config):
    m = _migrator(sample_config, verify=False)
    _wire_slice(m, [_run("a"), _run("b")], ["a"])
    sent = []
    m.ingest = lambda batch: sent.extend(batch) or []
    m.migrate_slice({"id": "src"}, {"id": DEST}, WINDOW)
    assert [p["id"] for p in sent] == ["b"]  # the diff is the work-list either way


def test_sets_from_two_sessions_are_never_merged(sample_config):
    m = _migrator(sample_config, verify=False)
    m.source.post.side_effect = [
        {"runs": [_run("a")], "cursors": {}}, {"runs": [_run("a")], "cursors": {}},
        {"runs": [_run("b")], "cursors": {}}, {"runs": [_run("b")], "cursors": {}},
    ]
    m.dest.post.side_effect = [{"runs": [], "cursors": {}}, {"runs": [], "cursors": {}}]
    m.ingest = Mock(return_value=[])
    first = m.migrate_slice({"id": "s1"}, {"id": "d1"}, WINDOW)
    second = m.migrate_slice({"id": "s2"}, {"id": "d2"}, WINDOW)
    assert (first.source_total, second.source_total) == (1, 1)
    assert (first.dest_session_id, second.dest_session_id) == ("d1", "d2")


# --------------------------------------------------------------------------
# Failure isolation
# --------------------------------------------------------------------------
def test_a_bad_run_is_isolated_by_binary_split(sample_config):
    m = _migrator(sample_config)
    bad = "b"

    def ingest(create):
        if any(p["id"] == bad for p in create):
            m._ingest_errors.append(RuntimeError("nope"))

    m.dest_ls_client.multipart_ingest.side_effect = lambda create: ingest(create)
    failures = m.ingest([{"id": c} for c in "abcd"])
    assert failures == [("b", "nope")]


def test_a_single_run_conflict_never_reaches_us_as_an_error(sample_config):
    # The SDK breaks out of its retry loop on 409 without invoking the error
    # callback, so a replay reads as success.
    m = _migrator(sample_config)
    m.dest_ls_client.multipart_ingest.return_value = None
    assert m.ingest([{"id": "a"}]) == []


def test_dry_run_sends_nothing(sample_config):
    sample_config.migration.dry_run = True
    m = _migrator(sample_config)
    assert m.ingest([{"id": "a"}]) == []
    m.dest_ls_client.multipart_ingest.assert_not_called()


# --------------------------------------------------------------------------
# Blobs
# --------------------------------------------------------------------------
def _blob_response(body=b"bytes", content_type="text/plain"):
    response = Mock()
    response.__enter__ = Mock(return_value=response)
    response.__exit__ = Mock(return_value=False)
    response.headers = {"Content-Type": content_type}
    response.iter_content = Mock(return_value=[body])
    response.raise_for_status = Mock()
    return response


def test_attachments_are_fetched_through_the_configured_session(sample_config):
    m = _migrator(sample_config)
    m._source_blob_host = "source.api.test.com"
    m.source.session.get.return_value = _blob_response(b"attachment-bytes")

    run = _run("a", s3_urls={"attachment.note": {"presigned_url": "https://source.api.test.com/public/download?jwt=x"}})
    overrides, issues = m.materialise(run)

    assert overrides["attachments"] == {"note": ("text/plain", b"attachment-bytes")}
    assert issues == []
    # not a bare requests.get: the tool's own session, and no redirect chasing
    assert m.source.session.get.call_args.kwargs["allow_redirects"] is False


def test_an_off_host_blob_url_is_refused(sample_config):
    m = _migrator(sample_config)
    m._source_blob_host = "source.api.test.com"
    run = _run("a", s3_urls={"attachment.note": {"presigned_url": "https://evil.example.com/blob"}})
    _, issues = m.materialise(run)
    assert issues == ["attachment_host_rejected"]
    m.source.session.get.assert_not_called()


def test_a_failed_blob_fetch_is_explicit_not_a_placeholder(sample_config):
    m = _migrator(sample_config)
    m._source_blob_host = "source.api.test.com"
    m.source.session.get.side_effect = RuntimeError("network down")
    run = _run("a", s3_urls={"attachment.note": {"presigned_url": "https://source.api.test.com/x"}})
    overrides, issues = m.materialise(run)
    assert issues == ["attachment_fetch_failed"]
    assert "attachments" not in overrides


def test_skip_attachments_degrades_rather_than_pretending(sample_config):
    m = _migrator(sample_config, skip_attachments=True)
    m._source_blob_host = "source.api.test.com"
    run = _run("a", s3_urls={"attachment.note": {"presigned_url": "https://source.api.test.com/x"}})
    overrides, issues = m.materialise(run)
    assert issues == ["attachments_skipped"]
    assert "attachments" not in overrides
    # the repair is named, and it is never "pass --skip-attachments again"
    assert m.fidelity_reduced == {"without --skip-attachments"}


def test_offloaded_inputs_are_re_inlined_only_when_absent(sample_config):
    m = _migrator(sample_config)
    m._source_blob_host = "source.api.test.com"
    m.source.session.get.return_value = _blob_response(b'{"q": "from-blob"}', "application/json")

    offloaded = _run("a", inputs=None, inputs_s3_urls={"ROOT": {"presigned_url": "https://source.api.test.com/b"}})
    assert m.materialise(offloaded)[0]["inputs"] == {"q": "from-blob"}

    # the query API usually resolves the payload itself; do not re-fetch it
    m.source.session.get.reset_mock()
    inline = _run("a", inputs={"q": "inline"}, inputs_s3_urls={"ROOT": {"presigned_url": "https://source.api.test.com/b"}})
    assert m.materialise(inline)[0] == {}
    m.source.session.get.assert_not_called()


def test_a_blob_larger_than_the_budget_is_refused(sample_config):
    m = _migrator(sample_config, max_field_bytes=4)
    m._source_blob_host = "source.api.test.com"
    m.source.session.get.return_value = _blob_response(b"far too many bytes")
    run = _run("a", inputs=None, inputs_s3_urls={"ROOT": {"presigned_url": "https://source.api.test.com/b"}})
    assert m.materialise(run)[1] == ["attachment_fetch_failed"]


# --------------------------------------------------------------------------
# Oversized payloads
# --------------------------------------------------------------------------
def test_a_payload_over_the_field_limit_is_degraded_before_sending(sample_config, migration_state):
    m = _migrator(sample_config, migration_state, max_field_bytes=50, verify=False)
    _wire_slice(m, [_run("a", inputs={"big": "x" * 500})], [])
    m.ingest = Mock(return_value=[])

    report = m.migrate_slice({"id": "src"}, {"id": DEST}, WINDOW)
    # The backend would accept it and store a placeholder, so it must not be
    # reported as migrated.
    assert (report.degraded, report.ingested) == (1, 0)
    m.ingest.assert_not_called()
    assert any(i.code == "payload_oversized_for_destination" for i in migration_state.issue_log)


# --------------------------------------------------------------------------
# Session mapping and tier
# --------------------------------------------------------------------------
def test_the_source_session_id_is_attempted_on_create(sample_config):
    m = _migrator(sample_config)
    m.dest.get_paginated.return_value = []
    m.dest.post.return_value = {"id": "src-1", "trace_tier": "longlived"}
    resolved = m.resolve_dest_session({"id": "src-1", "name": "proj"})
    assert m.dest.post.call_args[0][1]["id"] == "src-1"
    assert m.dest.post.call_args[0][1]["trace_tier"] == "longlived"
    assert resolved["id"] == "src-1"


def test_a_rejected_source_id_is_retried_without_one(sample_config):
    m = _migrator(sample_config)
    m.dest.get_paginated.return_value = []
    m.dest.post.side_effect = [RuntimeError("id taken"), {"id": "fresh", "trace_tier": "longlived"}]
    resolved = m.resolve_dest_session({"id": "src-1", "name": "proj"})
    assert resolved["id"] == "fresh"
    assert "id" not in m.dest.post.call_args[0][1]


def test_an_existing_destination_session_is_matched_by_name(sample_config):
    m = _migrator(sample_config)
    m.dest.get_paginated.return_value = [{"id": "other-id", "name": "proj", "trace_tier": "longlived"}]
    resolved = m.resolve_dest_session({"id": "src-1", "name": "proj"})
    assert resolved["id"] == "other-id"  # identity is a coincidence, not a requirement
    m.dest.post.assert_not_called()


def test_an_operator_mapping_wins_over_name_matching(sample_config):
    m = _migrator(sample_config, project_id_map={"src-1": "chosen"})
    m.dest.get.return_value = {"id": "chosen", "trace_tier": "longlived"}
    assert m.resolve_dest_session({"id": "src-1", "name": "proj"})["id"] == "chosen"


def test_an_ambiguous_destination_name_is_skipped(sample_config):
    m = _migrator(sample_config)
    m.dest.get_paginated.return_value = [
        {"id": "a", "name": "proj"}, {"id": "b", "name": "proj"},
    ]
    assert m.resolve_dest_session({"id": "src-1", "name": "proj"}) is None


def test_into_session_suffix_targets_a_separate_session(sample_config):
    m = _migrator(sample_config, into_session_suffix="-migrated")
    m.dest.get_paginated.return_value = [{"id": "live", "name": "proj", "trace_tier": "shortlived"}]
    m.dest.post.return_value = {"id": "copy", "trace_tier": "longlived"}
    resolved = m.resolve_dest_session({"id": "src-1", "name": "proj"})
    assert m.dest.post.call_args[0][1]["name"] == "proj-migrated"
    assert resolved["id"] == "copy"  # the live session's tier is untouched


def test_a_short_lived_session_is_raised_and_re_read_before_ingesting(sample_config):
    m = _migrator(sample_config)
    session = {"id": "d1", "trace_tier": "shortlived"}
    # a stale cached lookup would write blobs under the short-lived prefix
    m.dest.get.side_effect = [{"trace_tier": "shortlived"}, {"trace_tier": "longlived"}]
    with patch("langsmith_migrator.core.migrators.trace.time.sleep"):
        m.ensure_long_lived(session)
    assert session["trace_tier"] == "longlived"
    m.dest.patch.assert_called_once_with("/sessions/d1", {"trace_tier": "longlived"})


def test_a_tier_permission_failure_blocks_the_session(sample_config):
    m = _migrator(sample_config)
    m.dest.patch.side_effect = RuntimeError("403 Forbidden")
    with pytest.raises(TracePreflightError, match="trace_tier_permission_denied"):
        m.ensure_long_lived({"id": "d1", "trace_tier": "shortlived"})


def test_the_tier_is_not_restored_while_a_session_is_incomplete(sample_config):
    m = _migrator(sample_config, verify=False)
    m.resolve_dest_session = Mock(return_value={"id": DEST, "trace_tier": "longlived"})
    m.restore_tier = Mock()
    m.migrate_slice = Mock(side_effect=lambda s, d, w: __import__(
        "langsmith_migrator.core.trace_domain", fromlist=["Reconciliation"]
    ).Reconciliation("src", DEST, w.label(), 1, 0, 0, 0, 1))
    m.migrate_session({"id": "src", "name": "p", "trace_tier": "shortlived"}, now=NOW)
    m.restore_tier.assert_not_called()


# --------------------------------------------------------------------------
# Pre-flight canary
# --------------------------------------------------------------------------
def test_the_canary_blocks_when_the_timestamp_is_rewritten(sample_config, migration_state):
    m = _migrator(sample_config, migration_state, max_age_days=180)
    m.dest.get_paginated.return_value = [{"id": "scratch", "name": "langsmith-migrator-canary"}]
    m.ingest = Mock(return_value=[])
    m.dest.get.return_value = {"start_time": datetime.now(timezone.utc).isoformat()}
    with patch("langsmith_migrator.core.migrators.trace.time.sleep"):
        with pytest.raises(TracePreflightError, match="historical_ingest_rejected"):
            m.canary()
    assert any(i.code == "historical_ingest_rejected" for i in migration_state.issue_log)


def test_the_canary_blocks_when_the_run_never_appears(sample_config, migration_state):
    m = _migrator(sample_config, migration_state, max_age_days=180)
    m.dest.get_paginated.return_value = [{"id": "scratch", "name": "langsmith-migrator-canary"}]
    m.ingest = Mock(return_value=[])
    m.dest.get.return_value = None
    with patch("langsmith_migrator.core.migrators.trace.time.sleep"):
        with pytest.raises(TracePreflightError):
            m.canary()


def test_repeated_preflights_reuse_one_canary_identity(sample_config):
    m = _migrator(sample_config, max_age_days=180)
    m.dest.get_paginated.return_value = [{"id": "scratch", "name": "langsmith-migrator-canary"}]
    sent = []
    m.ingest = lambda batch: sent.extend(batch) or []
    m.dest.get.side_effect = lambda path: {"start_time": sent[-1]["start_time"]}
    with patch("langsmith_migrator.core.migrators.trace.time.sleep"):
        m.canary()
        m.canary()
    # same id AND same start_time: start_time is part of the dedup key, so a
    # drifted stamp would add a row instead of replacing one
    assert sent[0]["id"] == sent[1]["id"]
    assert sent[0]["start_time"] == sent[1]["start_time"]
    assert sent[0]["session_id"] == "scratch"  # never a migration target


def test_dry_run_skips_the_canary_entirely(sample_config):
    sample_config.migration.dry_run = True
    m = _migrator(sample_config)
    m.canary()
    m.dest.post.assert_not_called()


# --------------------------------------------------------------------------
# Timestamps
# --------------------------------------------------------------------------
def test_no_time_shifting_code_is_reachable_from_this_module():
    from pathlib import Path

    import langsmith_migrator.core.migrators.trace as module

    assert "time_shift" not in Path(module.__file__).read_text()


# --------------------------------------------------------------------------
# Deferred operator upgrade
# --------------------------------------------------------------------------
def test_the_upgrade_list_is_emitted_even_when_the_diff_is_empty(sample_config, migration_state):
    # Built from the diff instead, a re-run of a finished migration would emit
    # nothing at all.
    m = _migrator(sample_config, migration_state, emit_upgrade_list="/tmp/x.csv", verify=False)
    root, child = _run("t1"), _run("c1", trace="t1", order="A.B")
    _wire_slice(m, [root, child], ["t1", "c1"])
    m.ingest = Mock(return_value=[])

    report = m.migrate_slice({"id": "src"}, {"id": DEST}, WINDOW)
    assert m.ingest.call_count == 0 and report.already_present == 2
    assert m.upgrade_rows == [(DEST, "t1", root["start_time"])]  # roots only
    assert any(i.code == "longlived_pending_operator_upgrade" for i in migration_state.issue_log)


def test_the_destination_query_drops_the_tier_filter_for_a_deferred_upgrade(sample_config):
    # The projects were left short-lived, so filtering on tier would read every
    # already-migrated run as missing.
    m = _migrator(sample_config, emit_upgrade_list="/tmp/x.csv", verify=False)
    source_page = {"runs": [_run("a")], "cursors": {}}
    m.source.post.side_effect = [source_page, source_page]
    m.dest.post.side_effect = [{"runs": [_run("a", trace_tier="shortlived")], "cursors": {}}]
    report = m.migrate_slice({"id": "src"}, {"id": DEST}, WINDOW)
    assert report.already_present == 1


def test_a_deferred_upgrade_leaves_the_session_tier_alone(sample_config):
    m = _migrator(sample_config, emit_upgrade_list="/tmp/x.csv")
    m.ensure_long_lived({"id": "d1", "trace_tier": "shortlived"})
    m.dest.patch.assert_not_called()


# --------------------------------------------------------------------------
# Source and destination capability fallbacks
# --------------------------------------------------------------------------
def test_a_query_failure_is_never_swallowed(sample_config):
    # There is no select-rejection fallback: the derived select is built from
    # the write contract, every name of which the endpoint accepts.
    m = _migrator(sample_config)
    m.source.post.side_effect = RuntimeError("503 Service unavailable")
    with pytest.raises(RuntimeError, match="503"):
        m.long_lived_run_ids(m.source, "src", WINDOW)


def test_a_forbidden_session_create_is_a_tier_permission_blocker(sample_config):
    # PROJECTS_INCREASE_TRACE_TIER is checked on create too, not only on update.
    m = _migrator(sample_config)
    m.dest.get_paginated.return_value = []
    m.dest.post.side_effect = RuntimeError("403 Forbidden")
    with pytest.raises(TracePreflightError, match="trace_tier_permission_denied"):
        m.resolve_dest_session({"id": "src-1", "name": "proj"})


# --------------------------------------------------------------------------
# The write contract is pinned deliberately
# --------------------------------------------------------------------------
def test_the_ingest_field_set_matches_the_backend_contract():
    """Golden fixture for ``smith-go/runs/runs.go`` ``type Run struct``.

    The backend drops anything it does not declare, silently. If an SDK or
    backend bump changes that struct, this test is the place that notices -
    update it against the Go source, not against whatever the SDK happens to
    accept.
    """
    from dataclasses import fields

    from langsmith_migrator.core.trace_domain import RunIngestPayload

    assert {f.name for f in fields(RunIngestPayload)} == {
        "id", "trace_id", "parent_run_id", "dotted_order", "session_id",
        "name", "run_type", "start_time", "end_time", "status",
        "inputs", "outputs", "extra", "error", "serialized", "events", "tags",
        "attachments",
    }


# --------------------------------------------------------------------------
# A transient source failure must not look like "no such project"
# --------------------------------------------------------------------------
def test_a_missing_source_session_resolves_to_none(sample_config):
    from langsmith_migrator.core.api_client import NotFoundError

    m = _migrator(sample_config)
    m.source.get.side_effect = NotFoundError("404 Not Found")
    assert m.get_source_session("no-such-id") is None


def test_a_transient_source_failure_is_raised_not_swallowed(sample_config):
    # Swallowing it made a 429 indistinguishable from a mistyped name, and the
    # command exited zero having migrated nothing.
    from langsmith_migrator.core.api_client import APIError

    m = _migrator(sample_config)
    m.source.get.side_effect = APIError("429 Rate limit exceeded")
    with pytest.raises(APIError, match="429"):
        m.get_source_session("11111111-1111-1111-1111-111111111111")


def test_an_experiment_session_is_never_a_trace_target(sample_config):
    m = _migrator(sample_config)
    m.source.get.return_value = {"id": "x", "reference_dataset_id": "ds-1"}
    assert m.get_source_session("x") is None


# --------------------------------------------------------------------------
# --project accepts a name as well as an ID
# --------------------------------------------------------------------------
def test_a_project_name_is_resolved_through_the_endpoint_not_a_full_walk(sample_config):
    m = _migrator(sample_config)
    m.source.get.return_value = [{"id": "p1", "name": "evaluators"}]
    session, reason = m.find_source_session("evaluators")
    assert (session["id"], reason) == ("p1", "name")
    # the endpoint filters; enumerating tens of thousands of sessions would not scale
    assert m.source.get.call_args.kwargs["params"] == {"name": "evaluators", "reference_free": "true"}
    m.source.get_paginated.assert_not_called()


def test_an_id_wins_over_a_name_lookup(sample_config):
    the_id = "0fffee47-dd97-46f6-ae24-8866e2b8b9d9"
    m = _migrator(sample_config)
    m.source.get.return_value = {"id": the_id, "name": "whatever"}
    assert m.find_source_session(the_id) == ({"id": the_id, "name": "whatever"}, "id")
    assert m.source.get.call_args[0][0] == f"/sessions/{the_id}"


def test_an_unknown_uuid_falls_through_to_the_name_lookup(sample_config):
    from langsmith_migrator.core.api_client import NotFoundError

    m = _migrator(sample_config)
    m.source.get.side_effect = [NotFoundError("404"), [{"id": "p1", "name": "named-like-a-uuid"}]]
    assert m.find_source_session("00000000-0000-0000-0000-000000000000")[1] == "name"


def test_a_duplicated_project_name_is_reported_as_ambiguous(sample_config):
    m = _migrator(sample_config)
    m.source.get.return_value = [{"id": "a", "name": "dup"}, {"id": "b", "name": "dup"}]
    assert m.find_source_session("dup") == (None, "ambiguous")


def test_an_experiment_session_is_not_matched_by_name(sample_config):
    m = _migrator(sample_config)
    m.source.get.return_value = [{"id": "x", "name": "n", "reference_dataset_id": "ds"}]
    assert m.find_source_session("n") == (None, "missing")


def test_a_non_uuid_project_value_skips_the_id_endpoint_entirely(sample_config):
    # /sessions/<non-uuid> answers 422, not 404, so attempting it and then
    # sniffing the error to recover would be both slower and more fragile.
    m = _migrator(sample_config)
    m.source.get.return_value = [{"id": "p1", "name": "evaluators"}]
    session, reason = m.find_source_session("evaluators")
    assert (session["id"], reason) == ("p1", "name")
    assert m.source.get.call_count == 1
    assert m.source.get.call_args[0][0] == "/sessions"


# --------------------------------------------------------------------------
# Progress reporting
# --------------------------------------------------------------------------
def test_each_query_page_reports_runs_traces_and_size(sample_config, capsys):
    sample_config.migration.verbose = True
    m = _migrator(sample_config)
    m.source.post.side_effect = _pages(
        [_run("a", trace="t1"), _run("b", trace="t1"), _run("c", trace="t2")], [_run("d", trace="t3")]
    )
    list(m._query_runs(m.source, "src", WINDOW, select=["id"]))
    lines = [ln for ln in capsys.readouterr().out.splitlines() if "query source" in ln]
    assert len(lines) == 2
    assert "runs" in lines[0] and "traces" in lines[0]
    assert "3" in lines[0] and "2" in lines[0]  # 3 runs across 2 traces
    assert "total" in lines[1]
    # a wrapped progress line is worse than a terse one
    assert all(len(ln) <= 80 for ln in lines), lines


def test_the_multipart_write_is_reported_prominently(sample_config, capsys):
    m = _migrator(sample_config)
    m.dest_ls_client.multipart_ingest.return_value = None
    m.ingest([{"id": "a", "trace_id": "t1"}, {"id": "b", "trace_id": "t1"}, {"id": "c", "trace_id": "t2"}])
    out = capsys.readouterr().out
    # the one step that writes must not read like another query log line
    assert "MULTIPART INGEST" in out
    assert "runs 3" in out and "traces 2" in out


def test_query_pages_are_silent_without_verbose(sample_config, capsys):
    m = _migrator(sample_config)
    m.source.post.side_effect = _pages([_run("a")])
    list(m._query_runs(m.source, "src", WINDOW, select=["id"]))
    assert "query source" not in capsys.readouterr().out


def test_reported_size_counts_attachment_bytes(sample_config):
    small = TraceMigrator._shape([{"id": "a", "trace_id": "t"}])[2]
    big = TraceMigrator._shape([{"id": "a", "trace_id": "t", "attachments": {"n": ("text/plain", b"x" * 5000)}}])[2]
    assert big - small >= 5000


def test_human_sizes_are_readable():
    assert TraceMigrator._human(512) == "512 B"
    assert TraceMigrator._human(1536) == "1.5 KB"
    assert TraceMigrator._human(5 * 1024 * 1024) == "5.0 MB"


# --------------------------------------------------------------------------
# Blob hosts are per side
# --------------------------------------------------------------------------
def test_the_destination_read_back_uses_the_destination_blob_host(sample_config):
    """A destination presigned URL lives on the destination host.

    Validating it against the *source* host refused every read-back, so the
    fidelity digest compared N attachments against 0 and reported a mismatch
    that was not real.
    """
    m = _migrator(sample_config)
    m._source_blob_host, m._dest_blob_host = "source.api.test.com", "dest.api.test.com"
    m.dest.session.get.return_value = _blob_response(b"attachment-bytes")

    run = _run("a", s3_urls={"attachment.note": {"presigned_url": "https://dest.api.test.com/public/download?jwt=x"}})
    overrides, issues = m.materialise(run, side="dest")

    assert issues == []
    assert overrides["attachments"]["note"] == ("text/plain", b"attachment-bytes")
    m.source.session.get.assert_not_called()


def test_a_source_url_is_still_refused_on_the_destination_side(sample_config):
    m = _migrator(sample_config)
    m._source_blob_host, m._dest_blob_host = "source.api.test.com", "dest.api.test.com"
    run = _run("a", s3_urls={"attachment.note": {"presigned_url": "https://source.api.test.com/x"}})
    assert m.materialise(run, side="dest")[1] == ["attachment_host_rejected"]


def test_an_unreadable_read_back_is_not_reported_as_a_content_mismatch(sample_config, migration_state):
    m = _migrator(sample_config, migration_state)
    m._source_blob_host, m._dest_blob_host = "source.api.test.com", "dest.api.test.com"
    m.dest.session.get.side_effect = RuntimeError("blob store down")
    m.dest.post.side_effect = _pages([_run("a", s3_urls={"attachment.n": {"presigned_url": "https://dest.api.test.com/x"}})])

    degraded = {}
    m._check_content(DEST, WINDOW, {"a": {"attachments": "deadbeef"}}, degraded)
    assert degraded == {}
    assert not any(i.code == "run_fidelity_mismatch" for i in migration_state.issue_log)
