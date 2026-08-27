"""Pure-core tests for long-lived trace migration.

No HTTP and no implicit clock: everything here is a function of its arguments,
which is the point of keeping the domain separate from the migrator shell.
"""

from datetime import datetime, timedelta, timezone

import pytest

from langsmith_migrator.core.trace_domain import (
    RUN_QUERY_SELECT,
    SKEW_BUFFER,
    RunIngestPayload,
    SessionReconciliation,
    SliceReconciliation,
    Window,
    batch_traces,
    digest_mismatches,
    group_into_traces,
    is_long_lived,
    iter_windows,
    payload_digest,
    plan_slice,
    resolve_window_bounds,
    to_ingest_payload,
)

NOW = datetime(2026, 8, 26, 12, 0, tzinfo=timezone.utc)


def _run(**kw):
    base = {
        "id": "11111111-1111-1111-1111-111111111111",
        "trace_id": "11111111-1111-1111-1111-111111111111",
        "name": "root",
        "run_type": "chain",
        "start_time": "2026-08-20T10:00:00",
        "end_time": "2026-08-20T10:00:05",
        "dotted_order": "20260820T100000000000Z11111111-1111-1111-1111-111111111111",
        "status": "success",
        "session_id": "SOURCE-SESSION",
        "inputs": {"q": 1},
        "outputs": {"a": 2},
        "trace_tier": "longlived",
    }
    base.update(kw)
    return base


# --------------------------------------------------------------------------
# Windows
# --------------------------------------------------------------------------
def test_windows_are_half_open_and_oldest_first():
    windows = list(iter_windows(NOW, max_age_days=3, window_days=1))
    assert [w.start for w in windows] == sorted(w.start for w in windows)
    assert windows[0].start == NOW - timedelta(days=3)
    assert windows[-1].end == NOW
    # abutting, so no gap and no overlap
    for earlier, later in zip(windows, windows[1:]):
        assert earlier.end == later.start


def test_final_window_is_clipped_to_now():
    windows = list(iter_windows(NOW, max_age_days=2.5, window_days=1))
    assert windows[-1].end == NOW
    assert sum((w.end - w.start).total_seconds() for w in windows) == pytest.approx(2.5 * 86400)


def test_window_size_does_not_change_which_instants_are_covered():
    coarse = list(iter_windows(NOW, 4, 4))
    fine = list(iter_windows(NOW, 4, 0.5))
    assert (coarse[0].start, coarse[-1].end) == (fine[0].start, fine[-1].end)


@pytest.mark.parametrize("bad", [(0, 1), (1, 0), (-1, 1)])
def test_nonpositive_range_or_window_is_rejected(bad):
    with pytest.raises(ValueError):
        list(iter_windows(NOW, *bad))


def test_bounds_carry_no_run_level_upper_bound_and_a_skew_buffer():
    window = Window(NOW - timedelta(days=1), NOW)
    trace_filter, run_start = resolve_window_bounds(window)

    # The upper bound lives only on the trace root: a run-level one would
    # truncate children that start after their trace's window.
    assert "lt(start_time" in trace_filter
    assert trace_filter.count("start_time") == 2
    assert run_start == (window.start - SKEW_BUFFER).isoformat()
    assert run_start < window.start.isoformat()


# --------------------------------------------------------------------------
# Read/write contract
# --------------------------------------------------------------------------
def test_select_is_derived_from_the_write_contract():
    for name in ("id", "trace_id", "dotted_order", "status", "serialized", "session_id"):
        assert name in RUN_QUERY_SELECT
    # blob references and the tier are needed to build a payload, never to send one
    for name in ("inputs_s3_urls", "outputs_s3_urls", "s3_urls", "trace_tier"):
        assert name in RUN_QUERY_SELECT
    # attachments are not projectable by the query API
    assert "attachments" not in RUN_QUERY_SELECT


def test_adding_a_write_contract_field_selects_it_with_no_second_edit():
    from dataclasses import fields

    writable = {f.name for f in fields(RunIngestPayload)} - {"attachments"}
    assert writable <= set(RUN_QUERY_SELECT)


def test_identity_and_timestamps_are_preserved_verbatim():
    run = _run(parent_run_id="22222222-2222-2222-2222-222222222222")
    payload, _ = to_ingest_payload(run, "DEST-SESSION")
    for field in ("id", "trace_id", "parent_run_id", "dotted_order", "start_time", "end_time"):
        assert getattr(payload, field) == run[field]


def test_session_is_rewritten_to_the_mapped_target():
    payload, _ = to_ingest_payload(_run(), "DEST-SESSION")
    assert payload.session_id == "DEST-SESSION"


def test_read_only_fields_are_structurally_unsendable():
    run = _run(
        manifest_id="m",
        app_path="/x",
        feedback_stats={"a": 1},
        total_tokens=5,
        inputs_s3_urls={"ROOT": {"presigned_url": "https://x/y"}},
        outputs_s3_urls={"ROOT": {"presigned_url": "https://x/y"}},
        s3_urls={"extra": {"presigned_url": "https://x/y"}},
    )
    sent = to_ingest_payload(run, "DEST")[0].as_dict()
    for leaked in ("manifest_id", "app_path", "feedback_stats", "total_tokens",
                   "inputs_s3_urls", "outputs_s3_urls", "s3_urls", "trace_tier"):
        assert leaked not in sent
    assert sent["status"] == "success"  # the write contract does accept this one


def test_reference_example_id_is_dropped_and_reported():
    payload, dropped = to_ingest_payload(_run(reference_example_id="ex-1"), "DEST")
    assert dropped == ("reference_example_id",)
    assert "reference_example_id" not in payload.as_dict()


def test_serialized_is_preserved_for_an_llm_run():
    manifest = {"lc": 1, "name": "chat"}
    payload, _ = to_ingest_payload(_run(run_type="llm", serialized=manifest), "DEST")
    assert payload.serialized == manifest


def test_offloaded_inputs_are_inlined_and_the_url_never_forwarded():
    run = _run(inputs=None, inputs_s3_urls={"ROOT": {"presigned_url": "https://src/blob"}})
    sent = to_ingest_payload(run, "DEST", {"inputs": {"q": "recovered"}})[0].as_dict()
    assert sent["inputs"] == {"q": "recovered"}
    assert "https://src/blob" not in str(sent)


def test_unset_optionals_are_omitted_so_the_destination_keeps_its_defaults():
    sent = to_ingest_payload(_run(end_time=None, tags=None), "DEST")[0].as_dict()
    assert "end_time" not in sent and "tags" not in sent


def test_the_dedup_key_reproduces_across_two_builds_of_the_same_run():
    run = _run()
    first = to_ingest_payload(run, "DEST")[0]
    second = to_ingest_payload(run, "DEST")[0]
    key = lambda p: (p.session_id, p.start_time, p.id)  # noqa: E731
    assert key(first) == key(second)


def test_tier_predicate_is_applied_to_the_run_body():
    assert is_long_lived(_run())
    assert not is_long_lived(_run(trace_tier="shortlived"))
    assert not is_long_lived(_run(trace_tier=None))


# --------------------------------------------------------------------------
# Digests
# --------------------------------------------------------------------------
def test_digest_is_stable_under_key_reordering_and_changes_with_a_value():
    a = payload_digest(_run(inputs={"x": 1, "y": 2}))
    b = payload_digest(_run(inputs={"y": 2, "x": 1}))
    c = payload_digest(_run(inputs={"x": 1, "y": 3}))
    assert a == b
    assert digest_mismatches(a, c) == ("inputs",)


def test_digest_covers_the_manifest_only_for_llm_and_prompt_runs():
    assert "serialized" in payload_digest(_run(run_type="llm", serialized={"a": 1}))
    assert "serialized" not in payload_digest(_run(run_type="chain", serialized={"a": 1}))


def test_digest_covers_attachment_names_and_sizes():
    left = payload_digest(_run(), {"note": 21})
    right = payload_digest(_run(), {"note": 22})
    assert digest_mismatches(left, right) == ("attachments",)


def test_digest_mismatch_names_fields_never_values():
    fields = digest_mismatches(payload_digest(_run(inputs={"secret": "s3cret"})), payload_digest(_run()))
    assert fields == ("inputs",)
    assert "s3cret" not in str(fields)


# --------------------------------------------------------------------------
# Grouping and batching
# --------------------------------------------------------------------------
def _child(trace, suffix, parent_order):
    return _run(
        id=suffix, trace_id=trace, parent_run_id=trace, dotted_order=f"{parent_order}.{suffix}"
    )


def test_traces_group_with_parents_before_children():
    root = _run(id="T1", trace_id="T1", dotted_order="A")
    kids = [_child("T1", "c2", "A"), _child("T1", "c1", "A")]
    (group,) = group_into_traces(kids + [root])
    assert [r["id"] for r in group] == ["T1", "c1", "c2"]


def test_a_trace_is_never_split_across_batches():
    traces = [
        [_run(id=f"t{t}r{r}", trace_id=f"t{t}", dotted_order=f"{t}{r}") for r in range(3)]
        for t in range(4)
    ]
    batches = list(batch_traces(traces, max_runs=4, max_bytes=10**9, size_of=lambda r: 1))
    assert [len(b) for b in batches] == [3, 3, 3, 3]
    for batch in batches:
        assert len({r["trace_id"] for r in batch}) == 1


def test_an_oversized_single_trace_still_ships_whole():
    traces = [[_run(id=f"r{i}", dotted_order=str(i)) for i in range(9)]]
    (only,) = list(batch_traces(traces, max_runs=2, max_bytes=1, size_of=lambda r: 100))
    assert len(only) == 9


def test_batches_respect_the_advertised_byte_limit():
    traces = [[_run(id=f"t{t}", trace_id=f"t{t}", dotted_order=str(t))] for t in range(4)]
    batches = list(batch_traces(traces, max_runs=100, max_bytes=250, size_of=lambda r: 100))
    assert [len(b) for b in batches] == [2, 2]


# --------------------------------------------------------------------------
# Diff and reconciliation
# --------------------------------------------------------------------------
def test_the_diff_is_the_work_list_and_the_skip_list():
    plan = plan_slice({"a", "b", "c"}, {"b", "z"})
    assert plan.to_ingest == {"a", "c"}
    assert plan.already_present == {"b"}
    assert plan.extra_on_dest == {"z"}  # informational only


def test_a_run_already_on_the_destination_is_never_re_sent():
    # A fully ingested run is immutable (its end_time is persisted), so
    # re-sending it could not repair it even if we tried.
    plan = plan_slice({"a", "b"}, {"a", "b"})
    assert plan.to_ingest == set()
    assert plan.already_present == {"a", "b"}


def test_reconciliation_parts_must_account_for_the_source_total():
    ok = SliceReconciliation("S", "D", "w", source_total=10, ingested=6, already_present=2, degraded=1, blocked=1)
    assert ok.source_total == 10
    with pytest.raises(ValueError):
        SliceReconciliation("S", "D", "w", source_total=10, ingested=6, already_present=2, degraded=1, blocked=0)


def test_session_reconciliation_sums_its_slices_and_reports_both_ids():
    slices = [
        SliceReconciliation("S", "D", "w1", 4, 4, 0, 0, 0),
        SliceReconciliation("S", "D", "w2", 6, 3, 2, 1, 0),
    ]
    total = SessionReconciliation.of("S", "D", slices)
    assert (total.source_total, total.ingested, total.already_present, total.degraded) == (10, 7, 2, 1)
    assert (total.session_id, total.dest_session_id) == ("S", "D")
    assert total.complete


def test_a_session_with_blocked_runs_is_not_complete():
    blocked = SessionReconciliation.of("S", "D", [SliceReconciliation("S", "D", "w", 2, 1, 0, 0, 1)])
    assert not blocked.complete
