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
    parse_window_label,
    project_dir_name,
    projected_file_count,
    read_window,
    window_label,
    workspace_dir_name,
)
from langsmith_migrator.core.trace_domain import Window, plan_slice
from langsmith_migrator.core.trace_ports import SlicePrepared

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
    sink.intent = {"range_start": "s", "range_end": "e", "window_hours": 24.0, "projects": ["gtm-agent"]}
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
    _, target, _, _ = _export(tmp_path, [_run(1, attachments={"../../etc/passwd": ("text/plain", b"x")})])
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


def test_replay_sends_only_what_the_destination_lacks(tmp_path):
    _export(tmp_path, [_run(1), _run(2)])
    source = ArchiveSource(tmp_path)
    session = source.sessions()[0]

    prepared = source.prepare(session, {"id": "NEW"}, WINDOW, lambda: {_run(1)["id"]})
    assert [p["id"] for batch, _ in prepared.batches for p in batch] == [_run(2)["id"]]
    assert prepared.plan.already_present == {_run(1)["id"]}


def test_replaying_a_fully_present_window_asks_the_destination_once_and_sends_nothing(tmp_path):
    _export(tmp_path, [_run(1)])
    source = ArchiveSource(tmp_path)
    session = source.sessions()[0]
    calls = []

    prepared = source.prepare(
        session, {"id": "NEW"}, WINDOW, lambda: calls.append(1) or {_run(1)["id"]}
    )
    assert prepared.batches == []
    assert len(calls) == 1


# --------------------------------------------------------------------------
# Completeness, and what an interruption leaves
# --------------------------------------------------------------------------
def test_a_staged_window_is_partial_and_invisible_until_it_is_renamed(tmp_path):
    sink, target, prepared, staged = _export(tmp_path, [_run(1)], commit=False)
    assert staged.name.endswith(".tar.zst.partial")
    assert sink.has_window(target, WINDOW) is False

    sink.commit(target, WINDOW, prepared, staged)
    assert sink.has_window(target, WINDOW) is True
    assert not staged.exists()


def test_a_complete_tarball_left_partial_is_re_captured_not_trusted(tmp_path):
    """The crash between stream close and rename. Wasteful once, never wrong."""
    sink, target, _, staged = _export(tmp_path, [_run(1)], commit=False)
    assert staged.exists() and sink.has_window(target, WINDOW) is False
    with pytest.raises(ArchiveError, match="incomplete"):
        read_window(staged)
    assert read_window(staged, allow_incomplete=True).payloads


def test_discarding_staged_windows_names_every_file_it_removed(tmp_path):
    sink, target, _, staged = _export(tmp_path, [_run(1)], commit=False)
    assert sink.discard_staged() == [staged]
    assert not staged.exists()
    assert sink.discard_staged() == []


def test_an_empty_window_still_produces_a_file_so_contiguity_holds(tmp_path):
    """"We looked here and found nothing" is not "we never looked"."""
    sink = ArchiveSink(tmp_path, compress_level=3)
    sink.intent = {}
    target = sink.open_target(PROJECT)
    prepared = SlicePrepared(WINDOW, "src", PROJECT["id"], plan_slice(set(), set()), [])
    sink.commit(target, WINDOW, prepared, sink.stage(target, WINDOW, prepared))

    assert sink.has_window(target, WINDOW) is True
    decoded = read_window(next(Path(target["dir"]).glob("*.tar.zst")))
    assert decoded.payloads == [] and decoded.manifest["run_count"] == 0


def test_runs_scanned_separates_a_quiet_window_from_a_wrong_tier_one(tmp_path):
    """A window of 10,000 short-lived runs is empty too, but differently."""
    sink = ArchiveSink(tmp_path, compress_level=3)
    sink.intent = {}
    target = sink.open_target(PROJECT)
    scanned = SlicePrepared(WINDOW, "src", PROJECT["id"], plan_slice(set(), set()), [_run(1)])
    sink.commit(target, WINDOW, scanned, sink.stage(target, WINDOW, scanned))

    manifest = read_window(next(Path(target["dir"]).glob("*.tar.zst"))).manifest
    assert (manifest["run_count"], manifest["runs_scanned"]) == (0, 1)


def test_window_labels_sort_in_time_order_and_carry_both_bounds(tmp_path):
    labels = [window_label(Window(NOW - timedelta(hours=h + 1), NOW - timedelta(hours=h))) for h in (0, 5, 30)]
    assert sorted(labels) == labels[::-1]
    assert parse_window_label(labels[0]) == Window(NOW - timedelta(hours=1), NOW)
    assert parse_window_label("nonsense") is None
    assert parse_window_label(window_label(Window(NOW, NOW))) is None  # not half-open


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


def test_project_directories_are_owner_only(tmp_path):
    _, target, _, _ = _export(tmp_path, [_run(1)])
    assert oct(Path(target["dir"]).stat().st_mode)[-3:] == "700"
    for path in Path(target["dir"]).iterdir():
        assert oct(path.stat().st_mode)[-3:] == "600", path


def _handmade(path: Path, members: dict, *, level=3):
    with open(path, "wb") as raw:
        with zstd.ZstdCompressor(level=level).stream_writer(raw, closefd=False) as stream:
            with tarfile.open(fileobj=stream, mode="w|") as tar:
                for name, data in members.items():
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
    _handmade(path, {f"{_run(1)['id']}.json": b"{}"})
    with pytest.raises(ArchiveError, match="truncated"):
        read_window(path)


def test_a_run_missing_its_attachment_members_is_never_presented_as_whole(tmp_path):
    path = tmp_path / f"{window_label(WINDOW)}.tar.zst"
    body = json.dumps({**_run(1), "attachments": [["a.bin", "text/plain"]]}).encode()
    _handmade(path, {f"{_run(1)['id']}.json": body, "MANIFEST.json": _manifest()})
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
        "langsmith_migrator.core.trace_archive.shutil.disk_usage",
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


def test_project_dir_names_are_unique_per_session_even_when_slugs_collide():
    a = project_dir_name({"id": "1111", "name": "a/b"})
    b = project_dir_name({"id": "2222", "name": "a:b"})
    assert a != b and a.startswith("a_b-") and b.startswith("a_b-")


def test_the_projected_file_count_is_range_over_window_times_projects():
    assert projected_file_count(180 * 24, 24.0, 50) == 9_000
    assert projected_file_count(180 * 24, 2.4, 50) == 90_000
    assert projected_file_count(180 * 24, 0, 50) == 0


# --------------------------------------------------------------------------
# Workspace level
# --------------------------------------------------------------------------
WORKSPACE = {"id": "11112222-3333-4444-5555-666677778888", "name": "LangChain Team"}


def test_window_files_are_filed_under_the_source_workspace_name(tmp_path):
    sink = ArchiveSink(tmp_path, compress_level=3, workspace=WORKSPACE)
    sink.intent = {}
    target = sink.open_target(PROJECT)
    prepared = _prepared(WINDOW, [_run(1)])
    sink.commit(target, WINDOW, prepared, sink.stage(target, WINDOW, prepared))

    written = sink.written[0].relative_to(tmp_path).parts
    assert written[0] == f"LangChain_Team-{WORKSPACE['id']}"
    assert written[1].startswith("gtm-agent-")
    assert len(written) == 3


@pytest.mark.parametrize("name", ["../../escape", "..", "/absolute", "-x", "a" * 300, ""])
def test_a_hostile_workspace_name_cannot_escape_the_archive_directory(tmp_path, name):
    sink = ArchiveSink(tmp_path, compress_level=3, workspace={"id": "ws-1", "name": name})
    resolved = Path(sink.open_target(PROJECT)["dir"]).resolve()
    assert tmp_path.resolve() in resolved.parents


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
        f"LangChain_Team-{WORKSPACE['id']}", f"LangChain_Team-{twin['id']}"
    ]
    ids = {read_window(f).manifest["workspace"]["id"] for f in tmp_path.rglob("*.tar.zst")}
    assert ids == {WORKSPACE["id"], twin["id"]}


def test_a_workspace_without_a_name_or_id_still_yields_one_safe_component(tmp_path):
    assert workspace_dir_name({"id": "abc", "name": None}) == "workspace-abc"
    assert workspace_dir_name({"id": None, "name": "Team A"}) == "Team_A"
    assert workspace_dir_name(None) == "workspace"


def test_replay_finds_projects_under_the_workspace_level(tmp_path):
    sink = ArchiveSink(tmp_path, compress_level=3, workspace=WORKSPACE)
    sink.intent = {}
    target = sink.open_target(PROJECT)
    prepared = _prepared(WINDOW, [_run(1)])
    sink.commit(target, WINDOW, prepared, sink.stage(target, WINDOW, prepared))

    # from the archive root...
    assert [s["name"] for s in ArchiveSource(tmp_path).sessions()] == ["gtm-agent"]
    # ...and from one workspace directory
    one = tmp_path / f"LangChain_Team-{WORKSPACE['id']}"
    assert [s["name"] for s in ArchiveSource(one).sessions()] == ["gtm-agent"]
