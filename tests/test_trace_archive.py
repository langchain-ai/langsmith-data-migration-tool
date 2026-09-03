"""The archive's contracts, all of which are cheap to get wrong.

Completeness here is a *file-level* property - a window is renamed into place
only after its manifest is appended - so these pin the file states, the
contiguity that ordered renames buy, and the fact that every member name coming
out of a tarball is treated as untrusted.
"""

import io
import json
import os
import tarfile
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import zstandard as zstd

from langsmith_migrator.core.trace_archive import (
    FORMAT_VERSION,
    ArchiveError,
    ArchiveSink,
    ArchiveSource,
    read_window,
    window_label,
)
from langsmith_migrator.core.trace_domain import Window, plan_slice
from langsmith_migrator.core.trace_ports import SlicePrepared

_MANIFEST_NAME = "MANIFEST.json"

NOW = datetime(2026, 9, 1, tzinfo=timezone.utc)
WINDOW = Window(NOW - timedelta(days=1), NOW)
PROJECT = {"id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "name": "gtm-agent"}


def _run(n: int, **extra):
    run_id = f"{n:08d}-1111-1111-1111-111111111111"
    return {
        "id": run_id,
        "trace_id": run_id,
        "name": "n",
        "run_type": "chain",
        "start_time": "2026-08-31T10:00:00+00:00",
        "dotted_order": f"20260831T100000000000Z{run_id}",
        "session_id": "source-session",
        "inputs": {"i": n},
        **extra,
    }


def _prepared(window, runs, **kw):
    ids = {str(r["id"]) for r in runs}
    prepared = SlicePrepared(window, "src", PROJECT["id"], plan_slice(ids, set()), list(runs), **kw)
    prepared.batches = [(list(runs), None)] if runs else []
    return prepared


def _export(root, runs, window=WINDOW, *, level=3, commit=True):
    sink = ArchiveSink(root, compress_level=level)
    sink.intent = {
        "range_start": "s",
        "range_end": "e",
        "window_hours": 24.0,
        "projects": ["gtm-agent"],
    }
    target = sink.open_target(PROJECT)
    prepared = _prepared(window, runs)
    staged = sink.stage(target, window, prepared)
    if commit:
        sink.commit(target, window, prepared, staged)
    return sink, target, prepared, staged


# --------------------------------------------------------------------------
# Round trip
# --------------------------------------------------------------------------
def test_a_window_round_trips_payloads_and_attachments_byte_for_byte(tmp_path):
    blob = os.urandom(3 * 1024 * 1024)
    runs = [_run(1), _run(2, attachments={"a/../weird name.bin": ("image/png", blob)})]
    _, target, _, _ = _export(tmp_path, runs)

    decoded = read_window(next(Path(target["dir"]).glob("*.tar.zst")))
    by_id = {p["id"]: p for p in decoded.payloads}
    assert set(by_id) == {r["id"] for r in runs}
    assert by_id[runs[1]["id"]]["attachments"] == {"a/../weird name.bin": ("image/png", blob)}
    assert by_id[runs[0]["id"]]["inputs"] == {"i": 1}
    assert decoded.window == WINDOW


def test_an_attachment_name_never_becomes_a_path_component(tmp_path):
    """Names ride inside the JSON; blob members are numbered by position."""
    _, target, _, _ = _export(
        tmp_path, [_run(1, attachments={"../../etc/passwd": ("text/plain", b"x")})]
    )
    with open(next(Path(target["dir"]).glob("*.tar.zst")), "rb") as raw:
        with zstd.ZstdDecompressor().stream_reader(raw) as stream:
            names = tarfile.open(fileobj=stream, mode="r|").getnames()
    assert all(".." not in name for name in names), names
    assert f"{_run(1)['id']}/0" in names


def test_replay_rewrites_the_session_and_rebatches_to_the_live_caps(tmp_path):
    _export(tmp_path, [_run(i) for i in range(1, 6)])
    source = ArchiveSource(tmp_path)
    session = source.sessions()[0]
    source._batch_limits = lambda: (2, 10**9)

    prepared = source.prepare(session, {"id": "NEW"}, WINDOW, lambda: set())
    assert [len(batch) for batch, _ in prepared.batches] == [2, 2, 1]
    assert {p["session_id"] for batch, _ in prepared.batches for p in batch} == {"NEW"}


# --------------------------------------------------------------------------
# Completeness, and what an interruption leaves
# --------------------------------------------------------------------------
def test_a_staged_window_is_partial_and_invisible_until_it_is_renamed(tmp_path):
    sink, target, prepared, staged = _export(tmp_path, [_run(1)], commit=False)
    assert staged.partial.name.endswith(".tar.zst.partial")
    assert sink.has_window(target, WINDOW) is False

    sink.commit(target, WINDOW, prepared, staged)
    assert sink.has_window(target, WINDOW) is True
    assert not staged.partial.exists()


def test_an_empty_window_still_produces_a_file_so_contiguity_holds(tmp_path):
    """ "We looked here and found nothing" is not "we never looked"."""
    sink = ArchiveSink(tmp_path, compress_level=3)
    sink.intent = {}
    target = sink.open_target(PROJECT)
    prepared = SlicePrepared(WINDOW, "src", PROJECT["id"], plan_slice(set(), set()), [])
    sink.commit(target, WINDOW, prepared, sink.stage(target, WINDOW, prepared))

    assert sink.has_window(target, WINDOW) is True
    decoded = read_window(next(Path(target["dir"]).glob("*.tar.zst")))
    assert decoded.payloads == [] and decoded.manifest["run_count"] == 0


def test_the_intent_lets_a_short_archive_be_told_from_a_complete_small_one(tmp_path):
    sink, target, _, _ = _export(tmp_path, [_run(1)])
    intent = read_window(next(Path(target["dir"]).glob("*.tar.zst"))).manifest["intent"]
    assert set(intent) == {"range_start", "range_end", "window_hours", "projects"}


# --------------------------------------------------------------------------
# Hostile and awkward input
# --------------------------------------------------------------------------
@pytest.mark.parametrize("name", ["../../escape", "..", "/absolute", "-leading", "a" * 400, ""])
def test_a_hostile_project_name_cannot_escape_the_archive_directory(tmp_path, name):
    sink = ArchiveSink(tmp_path, compress_level=3)
    target = sink.open_target({"id": PROJECT["id"], "name": name})
    resolved = Path(target["dir"]).resolve()
    assert tmp_path.resolve() in resolved.parents
    assert len(resolved.name) < 160


def _handmade(path: Path, members, *, level=3):
    """``members`` is a dict, or pairs when a name has to repeat."""
    pairs = members.items() if isinstance(members, dict) else members
    with open(path, "wb") as raw:
        with zstd.ZstdCompressor(level=level).stream_writer(raw, closefd=False) as stream:
            with tarfile.open(fileobj=stream, mode="w|") as tar:
                for name, data in pairs:
                    info = tarfile.TarInfo(name)
                    info.size = len(data)
                    tar.addfile(info, io.BytesIO(data))


def _manifest(**over):
    base = {
        "format_version": FORMAT_VERSION,
        "project": PROJECT,
        "window": {"start": WINDOW.start.isoformat(), "end": WINDOW.end.isoformat()},
        "run_count": 0,
        "runs_scanned": 0,
        "run_ids": [],
        "bytes": {},
        "degraded": {},
        "intent": {},
    }
    base.update(over)
    return json.dumps(base).encode()


def test_an_unexpected_member_name_is_refused_rather_than_read(tmp_path):
    path = tmp_path / f"{window_label(WINDOW)}.tar.zst"
    _handmade(path, {"../../etc/passwd": b"x", "MANIFEST.json": _manifest()})
    with pytest.raises(ArchiveError, match="unexpected member"):
        read_window(path)


def test_an_unknown_format_version_is_refused_not_partially_read(tmp_path):
    path = tmp_path / f"{window_label(WINDOW)}.tar.zst"
    _handmade(path, {"MANIFEST.json": _manifest(format_version=99)})
    with pytest.raises(ArchiveError, match="format_version"):
        read_window(path)


def test_a_manifestless_file_is_reported_as_truncated(tmp_path):
    path = tmp_path / f"{window_label(WINDOW)}.tar.zst"
    _handmade(path, {f"{_run(1)['id']}.json": json.dumps(_run(1)).encode()})
    with pytest.raises(ArchiveError, match="truncated"):
        read_window(path)


def test_a_run_missing_its_attachment_members_is_never_presented_as_whole(tmp_path):
    path = tmp_path / f"{window_label(WINDOW)}.tar.zst"
    body = json.dumps({**_run(1), "attachments": [["a.bin", "text/plain"]]}).encode()
    _handmade(
        path,
        {
            f"{_run(1)['id']}.json": body,
            "MANIFEST.json": _manifest(run_count=1, run_ids=[_run(1)["id"]]),
        },
    )
    with pytest.raises(ArchiveError, match="missing attachment"):
        read_window(path)


def test_a_decompression_bomb_aborts_instead_of_filling_memory(tmp_path, monkeypatch):
    monkeypatch.setattr("langsmith_migrator.core.trace_archive.MAX_WINDOW_BYTES", 4096)
    path = tmp_path / f"{window_label(WINDOW)}.tar.zst"
    _handmade(path, {f"{_run(1)['id']}.json": b"0" * 65536, "MANIFEST.json": _manifest()})
    with pytest.raises(ArchiveError, match="decompresses past"):
        read_window(path)


def test_a_full_disk_fails_before_a_window_file_is_opened(tmp_path, monkeypatch):
    import shutil as _shutil

    monkeypatch.setattr(
        "langsmith_migrator.core.trace_blobstore.shutil.disk_usage",
        lambda _p: _shutil._ntuple_diskusage(0, 0, 1024),
    )
    with pytest.raises(ArchiveError, match="refusing to start a window"):
        _export(tmp_path, [_run(1)])
    assert not list(tmp_path.rglob("*.tar.zst*"))


# --------------------------------------------------------------------------
# Enumeration
# --------------------------------------------------------------------------
def test_only_complete_windows_are_offered_for_replay_unless_asked(tmp_path):
    sink, target, prepared, staged = _export(tmp_path, [_run(1)], commit=False)
    assert ArchiveSource(tmp_path).sessions() == []
    assert len(ArchiveSource(tmp_path, allow_incomplete=True).sessions()) == 1

    sink.commit(target, WINDOW, prepared, staged)
    assert len(ArchiveSource(tmp_path).sessions()) == 1


def test_a_replay_range_confines_which_windows_are_read(tmp_path):
    """The lever for a destination that only accepts a recent ingest window."""
    older = Window(NOW - timedelta(days=3), NOW - timedelta(days=2))
    _export(tmp_path, [_run(1)], window=older)
    _export(tmp_path, [_run(2)], window=WINDOW)
    session = ArchiveSource(tmp_path).sessions()[0]

    assert len(list(ArchiveSource(tmp_path).windows(session))) == 2
    recent = ArchiveSource(tmp_path, range_start=NOW - timedelta(days=1))
    assert [w.start for w in recent.windows(session)] == [WINDOW.start]


# --------------------------------------------------------------------------
# Workspace level
# --------------------------------------------------------------------------
WORKSPACE = {"id": "11112222-3333-4444-5555-666677778888", "name": "LangChain Team"}


def test_two_workspaces_sharing_a_display_name_get_separate_directories(tmp_path):
    """The ID suffix is what keeps them apart; the manifest records it too, so
    the pairing survives even if someone renames a directory."""
    twin = {"id": "99998888-7777-6666-5555-444433332222", "name": "LangChain Team"}
    for ws in (WORKSPACE, twin):
        sink = ArchiveSink(tmp_path, compress_level=3, workspace=ws)
        sink.intent = {}
        target = sink.open_target({"id": ws["id"], "name": "shared"})
        prepared = _prepared(WINDOW, [_run(1)])
        sink.commit(target, WINDOW, prepared, sink.stage(target, WINDOW, prepared))

    assert sorted(p.name for p in tmp_path.iterdir() if p.is_dir()) == [
        f"LangChain_Team-{WORKSPACE['id']}",
        f"LangChain_Team-{twin['id']}",
    ]
    ids = {read_window(f).manifest["workspace"]["id"] for f in tmp_path.rglob("*.tar.zst")}
    assert ids == {WORKSPACE["id"], twin["id"]}


# --------------------------------------------------------------------------
# The field cap belongs to the sink, not to the fetch
# --------------------------------------------------------------------------
def test_an_archive_keeps_a_field_a_deployment_would_stub(tmp_path):
    """A file has no field limit, so an export must not drop the run."""
    sink = ArchiveSink(tmp_path, compress_level=1)
    assert sink.oversized_fields({"inputs": {"q": "x" * 50_000_000}}) == []


def test_replay_holds_back_a_run_the_destination_would_stub(tmp_path):
    """Archived with a bigger limit, or by another tool: still not "ingested"."""
    big = _run(1)
    big["inputs"] = {"q": "x" * 4096}
    _export(tmp_path, [big])

    source = ArchiveSource(tmp_path)
    source._oversized_fields = lambda payload: ["inputs"]
    session = source.sessions()[0]
    prepared = source.prepare(session, {"id": "dst"}, WINDOW, lambda: set())

    assert prepared.batches == [], "an oversized run must not be sent"
    assert prepared.degraded["payload_oversized_for_destination"] == {str(big["id"])}
    assert "payloads exceeded --max-field-bytes" in prepared.fidelity_notes


# --------------------------------------------------------------------------
# The manifest has to match what the tar actually holds
# --------------------------------------------------------------------------
def _one_run_manifest(**over):
    return _manifest(run_count=1, run_ids=[_run(1)["id"]], **over)


def test_a_duplicate_run_member_is_refused_not_silently_overwritten(tmp_path):
    """The second copy used to win, so the file replayed fewer runs than claimed."""
    path = tmp_path / f"{window_label(WINDOW)}.tar.zst"
    body = json.dumps(_run(1)).encode()
    _handmade(
        path,
        [
            (f"{_run(1)['id']}.json", body),
            (f"{_run(1)['id']}.json", body),
            ("MANIFEST.json", _one_run_manifest()),
        ],
    )
    with pytest.raises(ArchiveError, match="appears twice"):
        read_window(path)


def test_a_manifest_that_lists_other_runs_than_the_file_holds_is_refused(tmp_path):
    path = tmp_path / f"{window_label(WINDOW)}.tar.zst"
    _handmade(
        path,
        {
            f"{_run(1)['id']}.json": json.dumps(_run(1)).encode(),
            "MANIFEST.json": _manifest(run_count=1, run_ids=[_run(9)["id"]]),
        },
    )
    with pytest.raises(ArchiveError, match="different ids"):
        read_window(path)


def test_a_gap_in_the_attachment_indices_is_refused(tmp_path):
    """A length check alone passes {0, 5} for two names, then reads a missing part."""
    path = tmp_path / f"{window_label(WINDOW)}.tar.zst"
    body = json.dumps(
        {**_run(1), "attachments": [["a", "text/plain"], ["b", "text/plain"]]}
    ).encode()
    _handmade(
        path,
        {
            f"{_run(1)['id']}.json": body,
            f"{_run(1)['id']}/0": b"x",
            f"{_run(1)['id']}/5": b"y",
            "MANIFEST.json": _one_run_manifest(),
        },
    )
    with pytest.raises(ArchiveError, match="missing attachment"):
        read_window(path)


# --------------------------------------------------------------------------
# A replay across workspace pairs must not offer the same project twice
# --------------------------------------------------------------------------
WS_A = {"id": "aaaa1111-0000-0000-0000-000000000000", "name": "Team A"}
WS_B = {"id": "bbbb2222-0000-0000-0000-000000000000", "name": "Team B"}


def _export_into(root, workspace, session):
    sink = ArchiveSink(root, compress_level=1, workspace=workspace)
    sink.intent = {}
    target = sink.open_target(session)
    prepared = _prepared(WINDOW, [_run(1)])
    sink.commit(target, WINDOW, prepared, sink.stage(target, WINDOW, prepared))


def test_a_replay_only_offers_projects_from_the_workspace_being_replayed(tmp_path):
    """Unscoped, every pair replayed every project into its own destination."""
    _export_into(tmp_path, WS_A, {"id": PROJECT["id"], "name": "shared-name"})
    _export_into(tmp_path, WS_B, {"id": PROJECT["id"], "name": "shared-name"})

    assert len(ArchiveSource(tmp_path).sessions()) == 2, "both are in the archive"
    for workspace in (WS_A, WS_B):
        scoped = ArchiveSource(tmp_path, workspace=workspace["id"]).sessions()
        assert len(scoped) == 1
        assert workspace["id"] in scoped[0]["dir"]


def test_an_unknown_workspace_replays_nothing_rather_than_everything(tmp_path):
    _export_into(tmp_path, WS_A, PROJECT)
    assert ArchiveSource(tmp_path, workspace=WS_B["id"]).sessions() == []


# --------------------------------------------------------------------------
# What may be published: complete, unique, and whole
# --------------------------------------------------------------------------


def test_a_window_holding_a_run_twice_is_never_published(tmp_path):
    """A duplicate passes a set check but writes a tar the decoder rejects."""
    sink = ArchiveSink(tmp_path, compress_level=1)
    sink.intent = {}
    target = sink.open_target(PROJECT)
    prepared = _prepared(WINDOW, [_run(1)])
    prepared.batches = [([_run(1), _run(1)], None)]
    with pytest.raises(ArchiveError, match="more than once"):
        sink.stage(target, WINDOW, prepared)
    assert list(tmp_path.rglob("*.tar.zst*")) == []


def test_a_window_whose_offloaded_content_was_lost_is_never_published(tmp_path):
    """Published, its name would tell a re-run the run was captured whole."""
    sink = ArchiveSink(tmp_path, compress_level=1)
    sink.intent = {}
    target = sink.open_target(PROJECT)
    prepared = _prepared(WINDOW, [_run(1)])
    prepared.degraded = {"attachment_fetch_failed": {str(_run(1)["id"])}}
    with pytest.raises(ArchiveError, match="could not be"):
        sink.stage(target, WINDOW, prepared)
    assert list(tmp_path.rglob("*.tar.zst*")) == []


def test_a_run_outside_the_plan_is_never_published(tmp_path):
    """The manifest's run_ids are what a replay diffs against."""
    sink = ArchiveSink(tmp_path, compress_level=1)
    sink.intent = {}
    target = sink.open_target(PROJECT)
    prepared = _prepared(WINDOW, [_run(1)])
    prepared.batches = [([_run(1), _run(2)], None)]
    with pytest.raises(ArchiveError, match="outside this window's plan"):
        sink.stage(target, WINDOW, prepared)
    assert list(tmp_path.rglob("*.tar.zst*")) == []


def test_the_manifest_counts_against_the_window_ceiling(tmp_path, monkeypatch):
    """The decoder counts every member, so a manifest that tips it over must
    not be written - the file would publish under a name meaning "complete"."""
    monkeypatch.setattr("langsmith_migrator.core.trace_archive.MAX_WINDOW_BYTES", 400)
    sink = ArchiveSink(tmp_path, compress_level=1)
    sink.intent = {}
    target = sink.open_target(PROJECT)
    prepared = _prepared(WINDOW, [_run(1)])
    with pytest.raises(ArchiveError, match="window ceiling"):
        sink.stage(target, WINDOW, prepared)
    assert list(tmp_path.rglob("*.tar.zst*")) == []
