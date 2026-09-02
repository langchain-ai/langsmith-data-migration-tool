"""Export drives the migrator's existing walk, so its guarantees must survive.

The two that matter and are easy to lose: the tar+zstd encode has to happen in
the prefetch workers (or a heavy export gains hours of serial compression), and
the rename has to happen on the main thread in window order (or an interrupted
export leaves an interior hole that nothing re-derives).
"""

import threading
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import Mock, patch

import pytest

from langsmith_migrator.core.api_client import EnhancedAPIClient
from langsmith_migrator.core.migrators.trace import TraceMigrator
from langsmith_migrator.core.trace_archive import (
    ArchiveSink,
    ArchiveSource,
    read_window,
    window_label,
)
from langsmith_migrator.core.trace_domain import iter_windows

NOW = datetime(2026, 9, 1, 12, 0, tzinfo=timezone.utc)
PROJECT = {"id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "name": "gtm-agent", "trace_tier": "longlived"}


def _client():
    client = Mock(spec=EnhancedAPIClient)
    client.session = Mock()
    client.session.headers = {}
    return client


def _run(day, n):
    run_id = f"{day:04d}{n:04d}-1111-1111-1111-111111111111"
    stamp = (NOW - timedelta(days=day)).isoformat()
    return {
        "id": run_id, "trace_id": run_id, "trace_tier": "longlived",
        "name": "n", "run_type": "chain", "start_time": stamp,
        "dotted_order": f"20260901T120000000000Z{run_id}", "inputs": {"n": n},
    }


def _exporter(tmp_path, sample_config, *, days=4, prefetch=4, runs_per_window=2, delay=None):
    """A migrator whose sink is an archive and whose source returns fixed runs."""
    sink = ArchiveSink(tmp_path, compress_level=1)
    sink.intent = {"range_start": "s", "range_end": "e", "window_hours": 24.0, "projects": ["gtm-agent"]}
    with patch("langsmith_migrator.core.migrators.trace.Client"):
        m = TraceMigrator(
            _client(), _client(), None, sample_config,
            range_start=NOW - timedelta(days=days), range_end=NOW,
            window_hours=24.0, prefetch_windows=prefetch, verify=False,
            verify_content_sample=0, run_sink=sink,
        )

    order = [w.start for w in iter_windows(m.resolved_range_start(), NOW, 24.0)]

    def slice_runs(_client_, _session, window, **_kw):
        if delay:
            # Earliest windows made slowest, so completion order fights
            # commit order.
            time.sleep(delay * (len(order) - order.index(window.start)))
        day = (NOW - window.start).days
        return [_run(day, n) for n in range(runs_per_window)]

    m.slice_runs = Mock(side_effect=slice_runs)
    m.fetch_runs = Mock(side_effect=lambda _c, _s, _w, ids: [
        r for r in slice_runs(None, None, _w) if str(r["id"]) in set(ids)
    ])
    m.materialise = Mock(return_value=({}, []))
    return m, sink


def _labels(directory: Path):
    return sorted(p.name for p in Path(directory).glob("*.tar.zst"))


# --------------------------------------------------------------------------
def test_an_export_writes_one_complete_file_per_window(tmp_path, sample_config):
    m, sink = _exporter(tmp_path, sample_config, days=4)
    report = m.migrate_session(PROJECT)

    assert report.source_total == 8 and report.ingested == 8 and report.blocked == 0
    target = Path(sink.written[0]).parent
    assert len(_labels(target)) == 4
    for path in Path(target).glob("*.tar.zst"):
        assert len(read_window(path).payloads) == 2


def test_the_encode_runs_in_the_prefetch_workers_not_on_the_commit_path(tmp_path, sample_config):
    m, sink = _exporter(tmp_path, sample_config, prefetch=4)
    threads = set()
    original = sink.stage
    sink.stage = lambda *a: (threads.add(threading.current_thread().name), original(*a))[1]

    m.migrate_session(PROJECT)
    assert threads and all(name.startswith("trace-prepare") for name in threads), threads


def test_renames_happen_on_the_main_thread_in_window_order(tmp_path, sample_config):
    m, sink = _exporter(tmp_path, sample_config, days=6, prefetch=4, delay=0.03)
    seen, threads = [], set()
    original = sink.commit

    def commit(target, window, prepared, staged):
        seen.append(window.start)
        threads.add(threading.current_thread().name)
        return original(target, window, prepared, staged)

    sink.commit = commit
    m.migrate_session(PROJECT)

    assert seen == sorted(seen), seen
    assert threads == {threading.current_thread().name}


def test_an_interruption_mid_walk_leaves_a_contiguous_prefix(tmp_path, sample_config):
    """Ordering makes an interior hole unrepresentable, not merely recoverable."""
    m, sink = _exporter(tmp_path, sample_config, days=6, prefetch=3)
    original = sink.commit
    calls = {"n": 0}

    def commit(target, window, prepared, staged):
        calls["n"] += 1
        if calls["n"] == 4:
            raise RuntimeError("boom")
        return original(target, window, prepared, staged)

    sink.commit = commit
    with pytest.raises(RuntimeError):
        m.migrate_session(PROJECT)

    target = Path(sink.written[0]).parent
    expected = [w.start for w in iter_windows(m._resolved_start, NOW, 24.0)]
    assert [read_window(p).window.start for p in sorted(Path(target).glob("*.tar.zst"))] == expected[:3]
    # every .partial sits strictly beyond the prefix
    for partial in Path(target).glob("*.partial"):
        assert read_window(partial, allow_incomplete=True).window.start > expected[2]


def test_a_graceful_stop_drains_what_is_in_flight_and_loses_nothing(tmp_path, sample_config):
    """First Ctrl-C: stop scheduling, finish the rest. Loss is zero."""
    m, sink = _exporter(tmp_path, sample_config, days=8, prefetch=2)
    original = sink.commit

    def commit(target, window, prepared, staged):
        if len(sink.written) == 1:
            m.stop_requested = True
        return original(target, window, prepared, staged)

    sink.commit = commit
    m.migrate_session(PROJECT)

    target = Path(sink.written[0]).parent
    # what was scheduled before the stop is complete, and nothing is left over
    assert len(_labels(target)) == len(sink.written) < 8
    assert list(Path(target).glob("*.partial")) == []
    assert [read_window(p).window.start for p in sorted(Path(target).glob("*.tar.zst"))] == sorted(
        read_window(p).window.start for p in Path(target).glob("*.tar.zst")
    )


def test_a_re_run_skips_captured_windows_without_reading_the_source(tmp_path, sample_config):
    m, sink = _exporter(tmp_path, sample_config, days=3)
    m.migrate_session(PROJECT)
    first = m.slice_runs.call_count

    again, _ = _exporter(tmp_path, sample_config, days=3)
    report = again.migrate_session(PROJECT)
    assert again.slice_runs.call_count == 0
    assert again.skipped_windows == 3
    assert report is None or report.source_total == 0
    assert first == 3


def test_deleting_one_window_file_re_captures_exactly_that_window(tmp_path, sample_config):
    m, sink = _exporter(tmp_path, sample_config, days=4)
    m.migrate_session(PROJECT)
    target = Path(sink.written[0]).parent
    removed = sorted(Path(target).glob("*.tar.zst"))[1]
    removed.unlink()

    again, _ = _exporter(tmp_path, sample_config, days=4)
    again.migrate_session(PROJECT)
    assert again.slice_runs.call_count == 1
    assert again.skipped_windows == 3
    assert removed.exists()


def test_every_manifest_from_one_run_records_the_same_range_end(tmp_path, sample_config):
    m, sink = _exporter(tmp_path, sample_config, days=4)
    m.migrate_session(PROJECT)
    ends = {read_window(p).manifest["intent"]["range_end"] for p in sink.written}
    assert len(ends) == 1


def test_the_same_bounds_always_give_the_same_windows(tmp_path, sample_config):
    """Both bounds are absolute stamps, so window derivation reads no clock.

    Relative ages were removed for this: "now - N days" denoted a different
    instant on every evaluation, which slid the grid, gave an archive's files a
    new name each run so nothing was ever skipped, and - when the two bounds
    were read a moment apart - pushed a sub-second window past the last real one.
    """
    start = datetime(2026, 8, 30, 3, 17, tzinfo=timezone.utc)
    end = datetime(2026, 9, 1, 3, 17, tzinfo=timezone.utc)
    grids = []
    for _ in range(2):
        with patch("langsmith_migrator.core.migrators.trace.Client"):
            m = TraceMigrator(
                _client(), _client(), None, sample_config, range_start=start, range_end=end,
                window_hours=12.0, run_sink=ArchiveSink(tmp_path, compress_level=1),
            )
        grids.append([window_label(w) for w in m.windows(PROJECT)])
    assert grids[0] == grids[1]
    windows = list(m.windows(PROJECT))
    assert len(windows) == 4
    assert (windows[0].start, windows[-1].end) == (start, end)
    assert all(w.end - w.start == timedelta(days=0.5) for w in windows)


def test_a_naive_bound_is_read_as_utc(sample_config):
    with patch("langsmith_migrator.core.migrators.trace.Client"):
        m = TraceMigrator(
            _client(), _client(), None, sample_config,
            range_start=datetime(2026, 8, 30, 3, 17), range_end=datetime(2026, 8, 31, 3, 17),
        )
    assert m.resolved_range_start().tzinfo == timezone.utc
    assert m.walk_end() == datetime(2026, 8, 31, 3, 17, tzinfo=timezone.utc)


def test_a_run_the_source_lists_but_never_returns_is_reported_not_counted(tmp_path, sample_config):
    """An export has no confirming re-query, so this is the only place it shows."""
    m, sink = _exporter(tmp_path, sample_config, days=1, runs_per_window=3)
    m.fetch_runs = Mock(side_effect=lambda _c, _s, w, ids: [_run(1, 0)])

    report = m.migrate_session(PROJECT)
    assert (report.source_total, report.ingested, report.degraded) == (3, 1, 2)
    assert "the source did not return every run it listed" in m.fidelity_reduced
    written = read_window(sink.written[0]).manifest
    assert written["run_count"] == 1 and written["runs_scanned"] == 3


def test_an_export_needs_no_destination_client(tmp_path, sample_config):
    m, _ = _exporter(tmp_path, sample_config)
    assert m.dest_ls_client is None and m.compress_level is None


def test_a_replay_reads_back_what_an_export_wrote(tmp_path, sample_config):
    """The round trip, through both adapters and the same walk."""
    m, sink = _exporter(tmp_path, sample_config, days=3)
    m.migrate_session(PROJECT)

    source = ArchiveSource(tmp_path)
    session = source.sessions()[0]
    sent = []
    with patch("langsmith_migrator.core.migrators.trace.Client"):
        replay = TraceMigrator(
            _client(), _client(), None, sample_config,
            range_start=NOW - timedelta(days=3), range_end=NOW,
            window_hours=24.0, prefetch_windows=2, verify=False, verify_content_sample=0,
            compress_level=None, run_source=source,
        )
    source.bind(replay)
    replay.resolve_dest_session = Mock(return_value={"id": "NEW", "trace_tier": "longlived"})
    replay.ensure_long_lived = Mock()
    replay.existing_ids = Mock(return_value=set())
    replay.ingest = Mock(side_effect=lambda batch, frame=None: sent.extend(batch) or [])

    report = replay.migrate_session(session)
    assert report.source_total == 6 and report.ingested == 6
    assert {p["session_id"] for p in sent} == {"NEW"}
    assert len(sent) == 6


def test_an_end_before_the_start_is_refused(sample_config):
    with pytest.raises(ValueError, match="must precede"):
        with patch("langsmith_migrator.core.migrators.trace.Client"):
            TraceMigrator(
                _client(), _client(), None, sample_config,
                range_start=NOW, range_end=NOW - timedelta(days=1),
            )


def test_a_dry_run_previews_an_export_without_writing_anything(tmp_path, sample_config):
    """Otherwise the preview *is* the export."""
    sample_config.migration.dry_run = True
    m, sink = _exporter(tmp_path, sample_config, days=3)
    sink.dry_run = True

    report = m.migrate_session(PROJECT)
    assert report.source_total == 6
    assert list(tmp_path.rglob("*")) == []
    assert sink.written == [] and sink.staged == []
