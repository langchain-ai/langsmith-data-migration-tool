"""Migrator-shell tests for long-lived trace migration.

Only the parts that genuinely need a client live here - pagination, the
diff/ingest/confirm round trip, blob handling and failure isolation. Everything
expressible as a function of its arguments is in ``tests/unit/test_trace_domain.py``.
"""

import io
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch

import pytest
import requests

from langsmith_migrator.core.api_client import APIError, EnhancedAPIClient
from langsmith_migrator.core.migrators.trace import (
    _CANARY_SESSION,
    _ID_CHUNK_MIN,
    _PAGE_LIMIT,
    TraceMigrator,
    TracePreflightError,
)
from langsmith_migrator.core.trace_domain import Reconciliation, Window
from langsmith_migrator.core.trace_frames import DEFAULT_COMPRESS_LEVEL, CompiledFrame

NOW = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)
WINDOW = Window(NOW - timedelta(days=1), NOW)
DEST = "dest-session"


def _client() -> Mock:
    client = Mock(spec=EnhancedAPIClient)
    client.session = Mock()
    client.session.headers = {}
    return client


def _migrator(sample_config, state=None, **kwargs):
    kwargs.setdefault("range_start", NOW - timedelta(days=180))
    kwargs.setdefault("range_end", NOW)
    with patch("langsmith_migrator.core.migrators.trace.Client"):
        migrator = TraceMigrator(_client(), _client(), state, sample_config, **kwargs)
    migrator.dest_ls_client = Mock()
    migrator.dest_ls_client.info.batch_ingest_config = {
        "size_limit": 100,
        "size_limit_bytes": 20_000_000,
    }
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


def test_a_deployment_that_pages_smaller_is_obeyed_and_remembered(sample_config):
    m = _migrator(sample_config)
    reject = APIError(
        "API request failed: 400 - Limit exceeds maximum allowed value of 250", status_code=400
    )
    m.source.post.side_effect = [reject] + _pages([_run("a")])
    assert m.long_lived_run_ids(m.source, "src", WINDOW) == {"a"}
    assert m.source.post.call_args[0][1]["limit"] == 250
    # Adopted for the rest of the run, so the rejection is paid once.
    assert m._page_limit["source"] == 250
    assert m._page_limit["dest"] == _PAGE_LIMIT


def test_payload_fetch_is_chunked_without_changing_the_result(sample_config):
    m = _migrator(sample_config)
    m._id_chunk = 2
    m.source.post.side_effect = _pages([_run("a"), _run("b")]) + _pages([_run("c")])
    fetched = [r["id"] for r in m.fetch_runs(m.source, "src", WINDOW, ["a", "b", "c"])]
    assert sorted(fetched) == ["a", "b", "c"]


def test_an_oversized_response_halves_the_chunk_and_retries(sample_config):
    """A heavy project's payloads make 500 IDs a ~65 MB response the gateway
    refuses with a 502; the size is the problem, so shrink rather than fail."""
    m = _migrator(sample_config)
    m._id_chunk = 100
    ids = [str(i) for i in range(100)]
    too_big = APIError("API request failed: 502 - <html>...", status_code=502)
    m.source.post.side_effect = (
        [too_big] + _pages([_run(i) for i in ids[:50]]) + _pages([_run(i) for i in ids[50:]])
    )
    fetched = [r["id"] for r in m.fetch_runs(m.source, "src", WINDOW, ids)]
    assert fetched == ids
    assert m._id_chunk == 50  # remembered for the rest of the run
    for call in m.source.post.call_args_list[1:]:
        assert len(call[0][1]["id"]) <= 50


def test_a_chunk_already_at_the_floor_gives_up(sample_config):
    m = _migrator(sample_config)
    m._id_chunk = _ID_CHUNK_MIN
    m.source.post.side_effect = APIError("API request failed: 502 - <html>", status_code=502)
    with pytest.raises(APIError):
        list(m.fetch_runs(m.source, "src", WINDOW, ["a"]))


def test_a_failed_chunk_does_not_double_yield_what_it_had_produced(sample_config):
    """The retry re-requests the whole chunk, so nothing may have escaped yet."""
    m = _migrator(sample_config)
    m._id_chunk = 100
    ids = [str(i) for i in range(100)]
    # first attempt yields a page, then dies on the second page of the chunk
    m.source.post.side_effect = [
        {"runs": [_run(ids[0])], "cursors": {"next": "c0"}},
        APIError("API request failed: 502 - <html>", status_code=502),
        *_pages([_run(i) for i in ids[:50]]),
        *_pages([_run(i) for i in ids[50:]]),
    ]
    fetched = [r["id"] for r in m.fetch_runs(m.source, "src", WINDOW, ids)]
    assert fetched == ids, "the abandoned page leaked into the result"
    # the whole chunk is re-requested at half size, not resumed mid-chunk
    assert [len(c[0][1]["id"]) for c in m.source.post.call_args_list] == [100, 100, 50, 50]


# --------------------------------------------------------------------------
# Diff, ingest, confirm
# --------------------------------------------------------------------------
def _wire_slice(m, source_runs, dest_ids, *, confirmed=None):
    """Source population -> dest diff -> payload fetch -> confirm.

    The payload fetch honours the request's ``id`` filter, as the real endpoint
    does, so a test can tell "fetched" from "ingested".

    Compression is off here: these tests are about the diff/verify round trip,
    and frame compilation needs a real SDK client. It has its own tests.
    """
    m.compress_level = None

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
    m.ingest = lambda batch, frame=None: sent.extend(batch) or []

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


def test_a_run_that_never_appears_is_blocked(sample_config, migration_state):
    m = _migrator(sample_config, migration_state)
    _wire_slice(m, [_run("a")], [], confirmed=[])
    m.ingest = Mock(return_value=[])
    m.dest.get.return_value = None  # nowhere else either
    with patch("langsmith_migrator.core.migrators.trace.time.sleep"):
        report = m.migrate_slice({"id": "src"}, {"id": DEST}, WINDOW)
    assert (report.blocked, report.ingested) == (1, 0)
    assert any(i.code == "run_not_ingested" for i in migration_state.issue_log)
    assert any("still missing" in r for r in m.blocked_reasons)


# --------------------------------------------------------------------------
# Failure isolation
# --------------------------------------------------------------------------
def test_a_bad_run_is_isolated_by_binary_split(sample_config):
    m = _migrator(sample_config, compress_level=None)
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
    m = _migrator(sample_config, compress_level=None)
    m.dest_ls_client.multipart_ingest.return_value = None
    assert m.ingest([{"id": "a"}]) == []


def test_dry_run_sends_nothing(sample_config):
    sample_config.migration.dry_run = True
    m = _migrator(sample_config)
    assert m.ingest([{"id": "a"}]) == []
    m.dest_ls_client.multipart_ingest.assert_not_called()


# --------------------------------------------------------------------------
# Ingest compression
# --------------------------------------------------------------------------
def _frame(run_ids, raw=1000, comp=100):
    return CompiledFrame(run_ids=tuple(run_ids), stream=io.BytesIO(b"z"), sizes=(raw, comp))


def test_the_body_is_compressed_by_default(sample_config, capsys):
    m = _migrator(sample_config)
    assert m.compress_level == DEFAULT_COMPRESS_LEVEL
    with patch("langsmith_migrator.core.migrators.trace.compile_frame") as compile_:
        compile_.side_effect = lambda client, payloads, level: _frame(
            [str(p["id"]) for p in payloads]
        )
        m._ingest_responses.append((202, ""))
        assert m.ingest([{"id": "a", "trace_id": "t", "dotted_order": "o"}]) == []
    m.dest_ls_client._send_compressed_multipart_req.assert_called_once()
    m.dest_ls_client.multipart_ingest.assert_not_called()
    # the ratio is worth seeing: it is the whole point of the option
    assert "10.0x" in capsys.readouterr().out


def test_no_compress_upload_uses_the_sdk_path(sample_config):
    m = _migrator(sample_config, compress_level=None)
    m.dest_ls_client.multipart_ingest.side_effect = lambda create: m._ingest_responses.append(
        (202, "")
    )
    assert m.ingest([{"id": "a", "trace_id": "t"}]) == []
    m.dest_ls_client._send_compressed_multipart_req.assert_not_called()


def test_a_missing_sdk_internal_degrades_to_uncompressed(sample_config):
    with patch("langsmith_migrator.core.migrators.trace.unavailable_reason", return_value="moved"):
        m = _migrator(sample_config)
    assert m.compress_level is None
    assert m.compress_unavailable == "moved"


def test_a_split_recompiles_rather_than_reusing_the_batch_frame(sample_config):
    """The pre-built frame covers the whole batch, so each half needs its own."""
    m = _migrator(sample_config)
    payloads = [{"id": c, "trace_id": "t", "dotted_order": f"o{c}"} for c in "abcd"]
    compiled = []

    def fake_compile(client, batch, level):
        compiled.append(tuple(str(p["id"]) for p in batch))
        return _frame([str(p["id"]) for p in batch])

    def send(stream, sizes, attempts=1):
        # only the batch still containing "b" fails
        if "b" in compiled[-1]:
            m._ingest_errors.append(RuntimeError("nope"))

    with patch("langsmith_migrator.core.migrators.trace.compile_frame", side_effect=fake_compile):
        m.dest_ls_client._send_compressed_multipart_req.side_effect = send
        failures = m.ingest(payloads)
    assert failures == [("b", "nope")]
    # a frame per attempted batch, never the parent's frame re-sent
    assert compiled[0] == ("a", "b", "c", "d")
    assert ("b",) in compiled


def test_a_matching_prebuilt_frame_is_sent_as_is(sample_config):
    m = _migrator(sample_config)
    ready = _frame(["a"])
    with patch("langsmith_migrator.core.migrators.trace.compile_frame") as compile_:
        m._send([{"id": "a", "trace_id": "t", "dotted_order": "o"}], frame=ready)
    compile_.assert_not_called()
    assert m.dest_ls_client._send_compressed_multipart_req.call_args[0][0] is ready.stream


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

    run = _run(
        "a",
        s3_urls={
            "attachment.note": {
                "presigned_url": "https://source.api.test.com/public/download?jwt=x"
            }
        },
    )
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


def test_offloaded_inputs_are_re_inlined_only_when_absent(sample_config):
    m = _migrator(sample_config)
    m._source_blob_host = "source.api.test.com"
    m.source.session.get.return_value = _blob_response(b'{"q": "from-blob"}', "application/json")

    offloaded = _run(
        "a",
        inputs=None,
        inputs_s3_urls={"ROOT": {"presigned_url": "https://source.api.test.com/b"}},
    )
    assert m.materialise(offloaded)[0]["inputs"] == {"q": "from-blob"}

    # the query API usually resolves the payload itself; do not re-fetch it
    m.source.session.get.reset_mock()
    inline = _run(
        "a",
        inputs={"q": "inline"},
        inputs_s3_urls={"ROOT": {"presigned_url": "https://source.api.test.com/b"}},
    )
    assert m.materialise(inline)[0] == {}
    m.source.session.get.assert_not_called()


def test_a_blob_larger_than_the_budget_is_refused(sample_config):
    m = _migrator(sample_config, max_field_bytes=4)
    m._source_blob_host = "source.api.test.com"
    m.source.session.get.return_value = _blob_response(b"far too many bytes")
    run = _run(
        "a",
        inputs=None,
        inputs_s3_urls={"ROOT": {"presigned_url": "https://source.api.test.com/b"}},
    )
    # A payload field, so a payload code - an archive uses these to decide
    # whether the run it holds is whole.
    assert m.materialise(run)[1] == ["payload_fetch_failed"]


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
    m.dest.get.return_value = []
    m.dest.post.return_value = {"id": "src-1", "trace_tier": "longlived"}
    resolved = m.resolve_dest_session({"id": "src-1", "name": "proj"})
    assert m.dest.post.call_args[0][1]["id"] == "src-1"
    assert m.dest.post.call_args[0][1]["trace_tier"] == "longlived"
    assert resolved["id"] == "src-1"


def test_a_rejected_source_id_is_retried_without_one(sample_config):
    m = _migrator(sample_config)
    m.dest.get.return_value = []
    m.dest.post.side_effect = [RuntimeError("id taken"), {"id": "fresh", "trace_tier": "longlived"}]
    resolved = m.resolve_dest_session({"id": "src-1", "name": "proj"})
    assert resolved["id"] == "fresh"
    assert "id" not in m.dest.post.call_args[0][1]


def test_an_operator_mapping_wins_over_name_matching(sample_config):
    m = _migrator(sample_config, project_id_map={"src-1": "chosen"})
    m.dest.get.return_value = {"id": "chosen", "trace_tier": "longlived"}
    assert m.resolve_dest_session({"id": "src-1", "name": "proj"})["id"] == "chosen"


def test_an_ambiguous_destination_name_is_skipped(sample_config):
    m = _migrator(sample_config)
    m.dest.get.return_value = [{"id": "a", "name": "proj"}, {"id": "b", "name": "proj"}]
    assert m.resolve_dest_session({"id": "src-1", "name": "proj"}) is None


def test_a_short_lived_session_is_raised_and_re_read_before_ingesting(sample_config):
    m = _migrator(sample_config)
    session = {"id": "d1", "trace_tier": "shortlived"}
    # a stale cached lookup would write blobs under the short-lived prefix
    m.dest.get.side_effect = [{"trace_tier": "shortlived"}, {"trace_tier": "longlived"}]
    with patch("langsmith_migrator.core.migrators.trace.time.sleep"):
        m.ensure_long_lived(session)
    assert session["trace_tier"] == "longlived"
    m.dest.patch.assert_called_once_with("/sessions/d1", {"trace_tier": "longlived"})


def test_the_tier_is_not_restored_while_a_session_is_incomplete(sample_config):
    m = _migrator(sample_config, verify=False, prefetch_windows=1)
    m.resolve_dest_session = Mock(return_value={"id": DEST, "trace_tier": "longlived"})
    m.restore_tier = Mock()
    m.migrate_slice = Mock(
        side_effect=lambda s, d, w: __import__(
            "langsmith_migrator.core.trace_domain", fromlist=["Reconciliation"]
        ).Reconciliation("src", DEST, w.label(), 1, 0, 0, 0, 1)
    )
    m.migrate_session({"id": "src", "name": "p", "trace_tier": "shortlived"})
    m.restore_tier.assert_not_called()


# --------------------------------------------------------------------------
# Pre-flight canary
# --------------------------------------------------------------------------
def _dest_get(run_payload):
    """Route dest.get: /sessions is the scratch lookup, /runs/<id> the read-back."""

    def get(path, params=None):
        if path == "/sessions":
            return [{"id": "scratch", "name": _CANARY_SESSION}]
        return run_payload() if callable(run_payload) else run_payload

    return get


def test_the_canary_blocks_when_the_timestamp_is_rewritten(sample_config, migration_state):
    m = _migrator(sample_config, migration_state)
    m.ingest = Mock(return_value=[])
    m.dest.get.side_effect = _dest_get({"start_time": datetime.now(timezone.utc).isoformat()})
    with patch("langsmith_migrator.core.migrators.trace.time.sleep"):
        with pytest.raises(TracePreflightError, match="historical_ingest_rejected"):
            m.canary()
    assert any(i.code == "historical_ingest_rejected" for i in migration_state.issue_log)


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


# --------------------------------------------------------------------------
# Source and destination capability fallbacks
# --------------------------------------------------------------------------


def test_a_forbidden_session_create_is_a_tier_permission_blocker(sample_config):
    # PROJECTS_INCREASE_TRACE_TIER is checked on create too, not only on update.
    m = _migrator(sample_config)
    m.dest.get.return_value = []
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
        "id",
        "trace_id",
        "parent_run_id",
        "dotted_order",
        "session_id",
        "name",
        "run_type",
        "start_time",
        "end_time",
        "status",
        "inputs",
        "outputs",
        "extra",
        "error",
        "serialized",
        "events",
        "tags",
        "attachments",
    }


# --------------------------------------------------------------------------
# A transient source failure must not look like "no such project"
# --------------------------------------------------------------------------


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
    assert m.source.get.call_args.kwargs["params"] == {
        "name": "evaluators",
        "reference_free": "true",
    }
    m.source.get_paginated.assert_not_called()


def test_a_duplicated_project_name_is_reported_as_ambiguous(sample_config):
    m = _migrator(sample_config)
    m.source.get.return_value = [{"id": "a", "name": "dup"}, {"id": "b", "name": "dup"}]
    assert m.find_source_session("dup") == (None, "ambiguous")


# --------------------------------------------------------------------------
# Progress reporting
# --------------------------------------------------------------------------
def test_each_query_page_reports_runs_traces_and_size(sample_config, capsys):
    sample_config.migration.verbose = True
    m = _migrator(sample_config)
    m.source.post.side_effect = _pages(
        [_run("a", trace="t1"), _run("b", trace="t1"), _run("c", trace="t2")],
        [_run("d", trace="t3")],
    )
    list(m._query_runs(m.source, "src", WINDOW, select=["id"]))
    lines = [ln for ln in capsys.readouterr().out.splitlines() if "scan " in ln]
    assert len(lines) == 2
    assert "runs" in lines[0] and "traces" in lines[0]
    assert "3" in lines[0] and "2" in lines[0]  # 3 runs across 2 traces
    assert "p1" in lines[0] and "p2" in lines[1] and "total" in lines[1]
    # a wrapped progress line is worse than a terse one
    assert all(len(ln) <= 80 for ln in lines), lines


def test_reported_size_counts_attachment_bytes(sample_config):
    small = TraceMigrator._shape([{"id": "a", "trace_id": "t"}])[2]
    big = TraceMigrator._shape(
        [{"id": "a", "trace_id": "t", "attachments": {"n": ("text/plain", b"x" * 5000)}}]
    )[2]
    assert big - small >= 5000


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

    run = _run(
        "a",
        s3_urls={
            "attachment.note": {"presigned_url": "https://dest.api.test.com/public/download?jwt=x"}
        },
    )
    overrides, issues = m.materialise(run, side="dest")

    assert issues == []
    assert overrides["attachments"]["note"] == ("text/plain", b"attachment-bytes")
    m.source.session.get.assert_not_called()


def test_an_unreadable_read_back_is_not_reported_as_a_content_mismatch(
    sample_config, migration_state
):
    m = _migrator(sample_config, migration_state)
    m._source_blob_host, m._dest_blob_host = "source.api.test.com", "dest.api.test.com"
    m.dest.session.get.side_effect = RuntimeError("blob store down")
    m.dest.post.side_effect = _pages(
        [_run("a", s3_urls={"attachment.n": {"presigned_url": "https://dest.api.test.com/x"}})]
    )

    degraded = {}
    m._check_content(DEST, WINDOW, {"a": {"attachments": "deadbeef"}}, degraded)
    assert degraded == {}
    assert not any(i.code == "run_fidelity_mismatch" for i in migration_state.issue_log)


# --------------------------------------------------------------------------
# A raised destination tier is always settled
# --------------------------------------------------------------------------
def _raisable(m, source_tier="shortlived"):
    """A destination project that ensure_long_lived() has to raise."""
    dest = {"id": DEST, "trace_tier": "shortlived"}
    m.resolve_dest_session = Mock(return_value=dest)
    m.dest.get.return_value = {"trace_tier": "longlived"}
    m.restore_tier = Mock()
    return {"id": "src", "name": "p", "trace_tier": source_tier}, dest


def test_a_raised_tier_is_restored_after_a_clean_migration(sample_config):
    m = _migrator(sample_config, prefetch_windows=1)
    src, dest = _raisable(m)
    m.migrate_slice = Mock(
        side_effect=lambda s, d, w: Reconciliation("src", DEST, w.label(), 0, 0, 0, 0, 0)
    )
    with patch("langsmith_migrator.core.migrators.trace.time.sleep"):
        m.migrate_session(src)
    m.restore_tier.assert_called_once_with(dest, "shortlived")


def test_a_raised_tier_is_settled_even_when_the_migration_raises(sample_config, migration_state):
    # Walking away here would leave the project long-lived indefinitely,
    # changing retention for traffic unrelated to this migration.
    m = _migrator(sample_config, migration_state, prefetch_windows=1)
    src, dest = _raisable(m)
    m.migrate_slice = Mock(side_effect=RuntimeError("source query blew up"))
    with patch("langsmith_migrator.core.migrators.trace.time.sleep"):
        with pytest.raises(RuntimeError, match="blew up"):
            m.migrate_session(src)
    # deliberately left raised so a re-run still ingests long-lived...
    m.restore_tier.assert_not_called()
    # ...but never silently
    issue = next(i for i in migration_state.issue_log if i.code == "dest_tier_left_raised")
    assert "did not finish" in issue.summary
    assert issue.evidence["prior_tier"] == "shortlived"


def test_an_incomplete_migration_leaves_the_tier_raised_and_says_so(sample_config, migration_state):
    m = _migrator(sample_config, migration_state, prefetch_windows=1)
    src, dest = _raisable(m)
    m.migrate_slice = Mock(
        side_effect=lambda s, d, w: Reconciliation("src", DEST, w.label(), 1, 0, 0, 0, 1)
    )
    with patch("langsmith_migrator.core.migrators.trace.time.sleep"):
        m.migrate_session(src)
    m.restore_tier.assert_not_called()
    assert any(
        i.code == "dest_tier_left_raised" and "blocked runs" in i.summary
        for i in migration_state.issue_log
    )


# --------------------------------------------------------------------------
# The watermark is a completeness claim, so it needs real verification
# --------------------------------------------------------------------------
def _one_run_slice(m):
    m.compress_level = None  # ingest is mocked; see _wire_slice
    page = {"runs": [_run("a")], "cursors": {}}
    m.source.post.side_effect = [page, page]
    m.dest.post.side_effect = [{"runs": [], "cursors": {}}] * 6
    m.ingest = Mock(return_value=[])
    with patch("langsmith_migrator.core.migrators.trace.time.sleep"):
        return m.migrate_slice({"id": "src"}, {"id": DEST}, WINDOW)


# --------------------------------------------------------------------------
# A conflict is a rejection, not a replay
# --------------------------------------------------------------------------


def test_a_409_defers_to_verification_when_it_will_run(sample_config, capsys):
    """A 409 cannot distinguish "already in another project" from "our own
    earlier attempt landed"; only the destination can, so do not pre-judge."""
    m = _migrator(sample_config, compress_level=None)
    original = [(409, '{"error":"Run create payload already received."}')]
    m.dest_ls_client.multipart_ingest.side_effect = lambda create: m._ingest_responses.extend(
        original
    )
    assert m.ingest([{"id": "a", "trace_id": "t"}, {"id": "b", "trace_id": "t"}]) == []
    assert "leaving the verdict to verification" in capsys.readouterr().out
    # not split into halves either: the rejection applies to the whole request
    assert m.dest_ls_client.multipart_ingest.call_count == 1


def test_the_compressed_send_never_blind_retries(sample_config):
    """Ingest is not idempotent, so an unknown outcome must not be re-sent."""
    m = _migrator(sample_config)
    with patch("langsmith_migrator.core.migrators.trace.compile_frame") as compile_:
        compile_.side_effect = lambda client, payloads, level: _frame(
            [str(p["id"]) for p in payloads]
        )
        m._ingest_responses.append((202, ""))
        m.ingest([{"id": "a", "trace_id": "t", "dotted_order": "o"}])
    assert m.dest_ls_client._send_compressed_multipart_req.call_args.kwargs["attempts"] == 1


def test_a_swallowed_non_2xx_is_still_a_failure(sample_config):
    m = _migrator(sample_config, compress_level=None)
    original = [(503, "Service unavailable")]
    m.dest_ls_client.multipart_ingest.side_effect = lambda create: m._ingest_responses.extend(
        original
    )
    failures = m.ingest([{"id": "solo", "trace_id": "t"}])
    assert failures and "503" in failures[0][1]


def test_a_clean_202_is_success(sample_config):
    m = _migrator(sample_config, compress_level=None)
    m.dest_ls_client.multipart_ingest.side_effect = lambda create: m._ingest_responses.append(
        (202, "")
    )
    assert m.ingest([{"id": "a", "trace_id": "t"}]) == []


# --------------------------------------------------------------------------
# A blocked count always has a matching reason, from either path
# --------------------------------------------------------------------------
def test_an_ingest_rejection_is_reported_once_per_cause(sample_config, migration_state):
    """Twenty runs refused for one reason is one fact, not twenty log lines.

    The confirm-miss path recorded a reason but the ingest-rejection path did
    not, and because a fully-blocked slice leaves nothing for the confirm to
    check, a blocked count could appear with no reason anywhere.
    """
    m = _migrator(sample_config, migration_state)
    runs = [_run(c, trace="t1", order=f"A.{c}") for c in "abcde"]
    _wire_slice(m, runs, [])
    m.ingest = lambda batch, frame=None: [
        (str(p["id"]), 'HTTP 409: {"error":"already received"}') for p in batch
    ]

    with patch("langsmith_migrator.core.migrators.trace.time.sleep"):
        report = m.migrate_slice({"id": "src"}, {"id": DEST}, WINDOW)

    assert report.blocked == 5
    reasons = [r for r in m.blocked_reasons if "refused by the destination" in r]
    assert len(reasons) == 1, f"expected one grouped reason, got {m.blocked_reasons}"
    assert "5 run(s)" in reasons[0] and "409" in reasons[0]
    issue = next(i for i in migration_state.issue_log if i.code == "run_ingest_rejected")
    assert issue.evidence["count"] == 5


def test_a_read_timeout_halves_the_id_chunk_like_a_502_does(sample_config):
    """The server took the request and never finished the body: same problem as
    a 502 on an oversized response, and requests reports it under two classes.
    """
    from langsmith_migrator.core.migrators.trace import _response_too_large

    assert _response_too_large(requests.exceptions.ReadTimeout("read timed out"))
    assert _response_too_large(
        requests.exceptions.ConnectionError(
            "HTTPSConnectionPool(...): Read timed out. (read timeout=30)"
        )
    )
    assert not _response_too_large(requests.exceptions.ConnectionError("dns failure"))

    m = _migrator(sample_config)
    calls = {"n": 0}

    def post(_endpoint, body):
        calls["n"] += 1
        if len(body.get("id") or []) > 250:
            raise requests.exceptions.ConnectionError("HTTPSConnectionPool: Read timed out.")
        return {"runs": [{"id": i} for i in body["id"]], "cursors": {}}

    m.source.post.side_effect = post
    fetched = list(m.fetch_runs(m.source, "src", WINDOW, [str(i) for i in range(500)]))
    assert len(fetched) == 500 and m._id_chunk == 250


def test_a_destination_project_is_created_without_fields_the_source_lacks(sample_config):
    """An archive knows a project's name and ID and nothing else, and the
    endpoint answers 422 to "start_time": null."""
    m = _migrator(sample_config)
    m._dest_sessions_named = Mock(return_value=[])
    sent = []
    m.dest.post = Mock(side_effect=lambda _e, body: sent.append(body) or {"id": "new"})

    m.resolve_dest_session({"id": "src-id", "name": "from-archive"})
    assert "start_time" not in sent[0] and "description" not in sent[0]
    assert sent[0]["trace_tier"] == "longlived" and sent[0]["name"] == "from-archive"


def test_an_offloaded_field_with_no_url_is_a_loss_not_a_no_op(sample_config):
    """The field is offloaded, so returning quietly loses it with nothing said."""
    from langsmith_migrator.core.trace_domain import LOST_CONTENT_CODES

    m = _migrator(sample_config)
    run = _run("a", inputs=None, inputs_s3_urls={"ROOT": {}})
    overrides, issues = m.materialise(run)
    assert issues == ["payload_field_unavailable"]
    assert "inputs" not in overrides
    assert set(issues) <= LOST_CONTENT_CODES, "an archive must refuse to publish this"
