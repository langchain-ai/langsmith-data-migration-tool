"""Long-lived traces on disk: one solid ``.tar.zst`` per ``(project, window)``.

Why payloads and not compiled ingest frames: the ``session_id`` lives inside a
compiled multipart body once per run, so remapping the destination project would
mean decompress -> parse multipart -> patch -> re-encode. Payloads make it one
field assignment, and keep the archive readable without the SDK internals the
ingest path borrows. Re-compiling on replay costs ~1500 MB/s against a network
three orders of magnitude slower.

Why one solid stream and not per-record compression: measured on real trace
bodies, compressing per run costs **17.4x more bytes** than compressing the
whole file at once (4.0x against 69.7x). That single number rules out any
container that compresses row-by-row, and is why tar members - which are pure
framing, uncompressed by themselves - are the record format.

Completeness is a file-level property, twice over: a window is written as
``.partial`` and renamed only after its ``MANIFEST.json`` is appended as the
final member. So ``ls`` answers "is this window done" without decompressing, and
the manifest answers it again for anything that reads the file.

An archive is **plaintext production trace data**. Files are 0o600 and
directories 0o700; there is no encryption at rest, and the CLI says so.
"""

from __future__ import annotations

import io
import json
import math
import os
import re
import shutil
import tarfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterator, List, Optional, Set, Tuple

import zstandard as zstd

from .trace_domain import (
    Window,
    batch_traces,
    group_into_traces,
    payload_bytes,
    plan_slice,
)

# 2: intent.window_days became intent.window_hours.
# 3: window files gained a source-workspace directory above the project one.
FORMAT_VERSION = 3

# The archive compresses a whole window file rather than a ~20 MB ingest frame,
# so it can afford a larger match window than the ingest path's 24. Measured
# 69.7x -> 70.8x on a 45 MB sample and flat beyond; a multi-GB window has more
# cross-run redundancy to reach for, so re-measure before treating 25 as final.
WINDOW_LOG = 25

# A window is held whole in memory on both sides, so this is simultaneously the
# memory ceiling and the decompression-bomb guard. A window larger than this
# cannot be replayed in one piece anyway - shrink --window instead.
MAX_WINDOW_BYTES = 8 * 1024**3

# Refuse to start a window with less than this free. A floor, not a quota: it
# stops "disk filled at hour six" from truncating a file, nothing more.
FREE_SPACE_FLOOR = 1024**3

_MANIFEST = "MANIFEST.json"
_PROJECT = "PROJECT.json"
_STAMP = "%Y%m%dT%H%M%SZ"
_PARTIAL = ".partial"
_SUFFIX = ".tar.zst"

# Tar member names are untrusted: an archive is a file some other process wrote.
# Nothing derived from one ever reaches the filesystem - members are read into
# memory via extractfile() - and these are what a name must match to be read at
# all. Attachment members are numbered rather than named so that an attachment
# called "../x" cannot exist in the first place.
_RUN_JSON = re.compile(r"^([0-9a-fA-F-]{36})\.json$")
_RUN_BLOB = re.compile(r"^([0-9a-fA-F-]{36})/(\d{1,4})$")

_WINDOW_FILE = re.compile(r"^(\d{8}T\d{6}Z)__(\d{8}T\d{6}Z)$")


class ArchiveError(RuntimeError):
    """An archive could not be written, or could not be trusted to be read."""


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime(_STAMP)


def window_label(window: Window) -> str:
    """``20260801T000000Z__20260802T000000Z``.

    Both bounds, because sorting by the first gives the contiguity proof and
    the second lets ``end_k == start_k+1`` be checked straight off a directory
    listing - no manifest, no ``--window`` to remember.
    """
    return f"{_stamp(window.start)}__{_stamp(window.end)}"


def parse_window_label(label: str) -> Optional[Window]:
    match = _WINDOW_FILE.match(label)
    if not match:
        return None
    try:
        bounds = [
            datetime.strptime(part, _STAMP).replace(tzinfo=timezone.utc)
            for part in match.groups()
        ]
    except ValueError:
        return None
    return Window(bounds[0], bounds[1]) if bounds[0] < bounds[1] else None


def _slug(value: str, limit: int, fallback: str = "project") -> str:
    """A single path component that cannot traverse, whatever went in."""
    cleaned = re.sub(r"[^A-Za-z0-9._-]", "_", value or "")[:limit].strip("._-")
    return cleaned or fallback


def project_dir_name(session: Dict[str, Any]) -> str:
    """``<slug>-<session id>``. The ID keeps it unique when slugs collide."""
    return f"{_slug(str(session.get('name') or ''), 80)}-{_slug(str(session.get('id')), 40)}"


def workspace_dir_name(workspace: Optional[Dict[str, Any]]) -> str:
    """``<slug>-<workspace id>``, the same shape as the project level below it.

    The name because an archive is read by people and a bare UUID tells them
    nothing; the ID because two workspaces can share a display name and must not
    then share a directory. The ID is in every manifest as well, so the pairing
    is recoverable from the data and not only from the path.
    """
    ws = workspace or {}
    name = _slug(str(ws.get("name") or ""), 80, "workspace")
    return f"{name}-{_slug(str(ws['id']), 40, 'id')}" if ws.get("id") else name


def _mkdir(path: Path) -> None:
    path.mkdir(mode=0o700, parents=True, exist_ok=True)


def _open_private(path: Path):
    return os.fdopen(os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "wb")


def _contained(root: Path, child: Path) -> Path:
    """Reject any path that resolves outside the archive directory."""
    resolved, base = child.resolve(), root.resolve()
    if resolved != base and base not in resolved.parents:
        raise ArchiveError(f"refusing a path outside the archive directory: {child}")
    return resolved


# ----------------------------------------------------------------------
# Writing
# ----------------------------------------------------------------------
def _tar_bytes(tar: tarfile.TarFile, name: str, data: bytes) -> int:
    info = tarfile.TarInfo(name)
    info.size = len(data)
    info.mode = 0o600
    tar.addfile(info, io.BytesIO(data))
    return len(data)


def _encode_payload(payload: Dict[str, Any]) -> Tuple[bytes, List[bytes]]:
    """Split one payload into its JSON member and its attachment members.

    Attachment names are arbitrary source strings, so they stay *inside* the
    JSON - as an ordered ``[[name, content_type], ...]`` list - and the blob
    members are numbered by position. A name is therefore never a path
    component, on write or on read.
    """
    attachments = payload.get("attachments") or {}
    ordered = sorted(attachments.items())
    body = {k: v for k, v in payload.items() if k != "attachments"}
    if ordered:
        body["attachments"] = [[name, content_type] for name, (content_type, _) in ordered]
    return (
        json.dumps(body, default=str, separators=(",", ":")).encode(),
        [data for _, (_, data) in ordered],
    )


class ArchiveSink:
    """``RunSink`` writing one ``.tar.zst`` per window. Makes no destination claim.

    A complete window file asserts that the source's window was read and
    written - which is what ``has_window`` reports, and what makes a re-run skip
    it. It asserts nothing about any deployment: verification belongs to replay,
    where a real destination can answer.
    """

    def __init__(
        self,
        root: str | Path,
        *,
        compress_level: int,
        dry_run: bool = False,
        workspace: Optional[Dict[str, Any]] = None,
    ):
        self.root = Path(root)
        # {"id", "name"} of the *source* workspace, or None when the deployment
        # has no workspace concept to report.
        self.workspace = workspace
        self.compress_level = compress_level
        # A dry run scans and fetches exactly as a real one does, but must not
        # leave files behind - otherwise the preview *is* the export.
        self.dry_run = dry_run
        # Filled by the caller once project selection resolves: the *intended*
        # extent of this export, so the full expected window list is
        # reconstructible from any single file and a short archive cannot pass
        # for a complete small one.
        self.intent: Dict[str, Any] = {}
        self.written: List[Path] = []
        self.staged: List[Path] = []

    # -- RunSink ----------------------------------------------------------
    def open_target(self, session: Dict[str, Any]) -> Dict[str, Any]:
        directory = _contained(
            self.root, self.root / workspace_dir_name(self.workspace) / project_dir_name(session)
        )
        record = {"id": str(session["id"]), "name": session.get("name"), "dir": str(directory)}
        if self.dry_run:
            return record
        _mkdir(directory)
        marker = directory / _PROJECT
        if not marker.exists():
            with _open_private(marker) as handle:
                handle.write(json.dumps({k: record[k] for k in ("id", "name")}, indent=1).encode())
        return record

    def batch_limits(self) -> Tuple[int, int]:
        """No destination caps apply; the replay side re-batches to its own."""
        return 10**9, MAX_WINDOW_BYTES

    def has_window(self, target: Dict[str, Any], window: Window) -> bool:
        return self._final(target, window).exists()

    def existing_ids(self, target: Dict[str, Any], window: Window) -> Set[str]:
        """Always empty: a window file holds all of a window or none of it."""
        return set()

    def stage(self, target: Dict[str, Any], window: Window, prepared) -> Optional[Path]:
        """Worker thread: write the whole file, manifest last, leave it ``.partial``.

        The encode runs here rather than on the commit path deliberately - a
        heavy window is tens of GB and ~18 s of compression, which on the serial
        path would add hours to a multi-thousand-window export.
        """
        if self.dry_run:
            return None
        partial = Path(str(self._final(target, window)) + _PARTIAL)
        free = shutil.disk_usage(partial.parent).free
        if free < FREE_SPACE_FLOOR:
            raise ArchiveError(
                f"only {free:,} bytes free under {partial.parent}; refusing to start a window"
            )
        payloads = [p for batch, _ in prepared.batches for p in batch]
        run_ids = [str(p["id"]) for p in payloads]
        counts = {"payloads": 0, "attachments": 0}
        # write_checksum belongs in the params, not the compressor, and it is
        # why the manifest carries no digest of its own: zstd verifies the frame.
        params = zstd.ZstdCompressionParameters.from_level(
            self.compress_level, enable_ldm=True, window_log=WINDOW_LOG, write_checksum=1
        )
        with _open_private(partial) as raw:
            # closefd=False: the frame is flushed when this context exits, but
            # the fd has to outlive it so the fsync below can reach it.
            with zstd.ZstdCompressor(compression_params=params).stream_writer(raw, closefd=False) as stream:
                with tarfile.open(fileobj=stream, mode="w|") as tar:
                    for payload in payloads:
                        body, blobs = _encode_payload(payload)
                        counts["payloads"] += _tar_bytes(tar, f"{payload['id']}.json", body)
                        for index, blob in enumerate(blobs):
                            counts["attachments"] += _tar_bytes(tar, f"{payload['id']}/{index}", blob)
                    _tar_bytes(tar, _MANIFEST, self._manifest(target, window, prepared, run_ids, counts))
            raw.flush()
            os.fsync(raw.fileno())
        self.staged.append(partial)
        return partial

    def commit(
        self, target: Dict[str, Any], window: Window, prepared, staged: Optional[Path]
    ) -> List[Tuple[str, str]]:
        """Main thread, in window order: publish the window by renaming it.

        Ordering is what makes an interrupted export leave a contiguous prefix.
        Unordered renames could publish a later window while an earlier one is
        missing, and an interior hole in an archive nobody re-runs is invisible.
        """
        if staged is None:  # dry run
            return []
        final = self._final(target, window)
        os.replace(staged, final)
        if staged in self.staged:
            self.staged.remove(staged)
        self.written.append(final)
        return []

    def close_target(self, target: Dict[str, Any], recon: Any = None) -> None:
        return None

    # -- internals --------------------------------------------------------
    def _final(self, target: Dict[str, Any], window: Window) -> Path:
        return Path(target["dir"]) / f"{window_label(window)}{_SUFFIX}"

    def _manifest(self, target, window, prepared, run_ids, counts) -> bytes:
        written = set(run_ids)
        return json.dumps(
            {
                "format_version": FORMAT_VERSION,
                "workspace": self.workspace or {},
                "project": {"id": target["id"], "name": target.get("name")},
                "window": {"start": _iso(window.start), "end": _iso(window.end)},
                # Both counts, because a window with 10,000 short-lived runs and
                # no long-lived ones is also empty - and an archive that cannot
                # distinguish that from "we never looked" is not a coverage proof.
                "run_count": len(run_ids),
                "runs_scanned": len(prepared.population),
                "run_ids": sorted(run_ids),
                "bytes": counts,
                # Only codes for runs actually in the file: a run dropped at
                # export is absent, and the export's own reconciliation already
                # counted it.
                "degraded": {
                    code: sorted(ids & written)
                    for code, ids in (prepared.degraded or {}).items()
                    if ids & written
                },
                "intent": self.intent,
            },
            indent=1,
        ).encode()

    def discard_staged(self) -> List[Path]:
        """Remove any ``.partial`` this process left behind, and name them."""
        removed = []
        for path in list(self.staged):
            try:
                path.unlink()
                removed.append(path)
            except OSError:
                pass
            self.staged.remove(path)
        return removed


# ----------------------------------------------------------------------
# Reading
# ----------------------------------------------------------------------
@dataclass(frozen=True)
class ArchiveWindow:
    """One decoded window file."""

    path: Path
    window: Window
    manifest: Dict[str, Any]
    payloads: List[Dict[str, Any]]
    dropped: Tuple[str, ...] = ()


def read_window(path: Path, *, allow_incomplete: bool = False) -> ArchiveWindow:
    """Decode one window file, treating every member name as untrusted.

    Members are read into memory with ``extractfile``; ``extract``/``extractall``
    are never used, so no name from the tar reaches the filesystem. Names must
    match one of the three known shapes, sizes are capped in aggregate, and an
    unknown ``format_version`` is refused rather than partially read.
    """
    incomplete = str(path).endswith(_PARTIAL)
    if incomplete and not allow_incomplete:
        raise ArchiveError(f"{path.name} is incomplete (no manifest); pass --allow-incomplete to read it")

    bodies: Dict[str, Dict[str, Any]] = {}
    blobs: Dict[str, Dict[int, bytes]] = {}
    manifest: Optional[Dict[str, Any]] = None
    total = 0
    with open(path, "rb") as raw:
        with zstd.ZstdDecompressor().stream_reader(raw) as stream:
            with tarfile.open(fileobj=stream, mode="r|") as tar:
                for member in tar:
                    if not member.isfile():
                        raise ArchiveError(f"{path.name}: unexpected member type {member.name!r}")
                    total += member.size
                    if total > MAX_WINDOW_BYTES:
                        raise ArchiveError(
                            f"{path.name} decompresses past {MAX_WINDOW_BYTES:,} bytes; refusing to read it"
                        )
                    handle = tar.extractfile(member)
                    data = handle.read() if handle else b""
                    if member.name == _MANIFEST:
                        manifest = json.loads(data)
                        continue
                    if member.name == _PROJECT:
                        continue
                    run_json = _RUN_JSON.match(member.name)
                    if run_json:
                        bodies[run_json.group(1)] = json.loads(data)
                        continue
                    run_blob = _RUN_BLOB.match(member.name)
                    if run_blob:
                        blobs.setdefault(run_blob.group(1), {})[int(run_blob.group(2))] = data
                        continue
                    raise ArchiveError(f"{path.name}: unexpected member {member.name!r}")

    if manifest is None:
        if not incomplete:
            raise ArchiveError(f"{path.name} has no {_MANIFEST}; it was truncated")
        manifest = {}
    version = manifest.get("format_version", FORMAT_VERSION if incomplete else None)
    if version != FORMAT_VERSION:
        raise ArchiveError(f"{path.name}: unsupported format_version {version!r} (this build reads {FORMAT_VERSION})")

    window = parse_window_label(path.name.split(_SUFFIX)[0])
    bounds = manifest.get("window") or {}
    if bounds.get("start") and bounds.get("end"):
        window = Window(
            datetime.fromisoformat(bounds["start"]), datetime.fromisoformat(bounds["end"])
        )
    if window is None:
        raise ArchiveError(f"{path.name}: no window bounds in the name or the manifest")

    payloads, dropped = [], []
    for run_id, body in bodies.items():
        names = body.pop("attachments", None) or []
        if names:
            parts = blobs.get(run_id) or {}
            if len(parts) != len(names):
                # A run without all its attachments must not be presented as
                # whole. A complete file missing one is a defect, not a truncation.
                if not incomplete:
                    raise ArchiveError(f"{path.name}: run {run_id} is missing attachment members")
                dropped.append(run_id)
                continue
            body["attachments"] = {
                name: (content_type, parts[index])
                for index, (name, content_type) in enumerate(names)
            }
        payloads.append(body)
    return ArchiveWindow(path, window, manifest, payloads, tuple(sorted(dropped)))


class ArchiveSource:
    """``RunSource`` reading window files back out of an archive directory.

    Both sides of the port enumerate ``(project, window)``: the API source
    derives windows from the walk plan, this one from directory entries. Same
    shape, same driver - which is what makes ``RunSource`` a port rather than
    two unrelated readers.
    """

    def __init__(
        self,
        root: str | Path,
        *,
        allow_incomplete: bool = False,
        range_start: Optional[datetime] = None,
        range_end: Optional[datetime] = None,
    ):
        self.root = Path(root)
        self.allow_incomplete = allow_incomplete
        # Windows wholly outside these bounds are not replayed. Lets a replay be
        # confined to part of an archive - e.g. only what a destination that
        # enforces a +/-24h ingest window will still accept.
        self.range_start = range_start
        self.range_end = range_end
        # A replayed window is re-batched to *this* destination's caps, which
        # need not match the ones in force when it was captured.
        self._batch_limits: Callable[[], Tuple[int, int]] = lambda: (100, 20_971_520)
        self._compile_batch: Callable[[List[Dict[str, Any]]], Any] = lambda batch: None

    def bind(self, sink) -> None:
        """Take the live destination's batch caps and frame compiler.

        Deferred rather than passed in: the sink is the migrator, which needs
        this source at construction time.
        """
        self._batch_limits = sink.batch_limits
        self._compile_batch = sink.compile_batch

    # -- RunSource --------------------------------------------------------
    def sessions(self) -> List[Dict[str, Any]]:
        """Every project in the archive, found by its ``PROJECT.json`` marker.

        Located by search rather than by walking a fixed depth, so pointing
        ``--from-archive`` at one workspace directory works as well as at the
        archive root.
        """
        found = []
        for marker in sorted(self.root.rglob(_PROJECT)):
            directory = marker.parent
            if not self._window_paths(directory):
                continue
            record = json.loads(marker.read_text())
            found.append({"id": record["id"], "name": record.get("name"), "dir": str(directory)})
        return found

    def windows(self, session: Dict[str, Any]) -> Iterator[Window]:
        for path in self._window_paths(Path(session["dir"])):
            window = self._require_window(path)
            if self.range_start and window.end <= self.range_start:
                continue
            if self.range_end and window.start >= self.range_end:
                continue
            yield window

    def prepare(self, session, target, window, existing_ids: Callable[[], Set[str]]):
        from .trace_ports import SlicePrepared

        decoded = read_window(self._path_for(session, window), allow_incomplete=self.allow_incomplete)
        ids = {str(p["id"]) for p in decoded.payloads}
        plan = plan_slice(ids, existing_ids() if ids else set())
        # Stands in for the API source's identity scan, off the same runs.
        population = [
            {"id": str(p["id"]), "trace_id": str(p.get("trace_id")), "start_time": p.get("start_time")}
            for p in decoded.payloads
        ]
        prepared = SlicePrepared(window, str(session["id"]), str(target["id"]), plan, population)
        if decoded.dropped:
            prepared.issues.append((
                "degraded",
                "archived_run_incomplete",
                f"{len(decoded.dropped)} run(s) in {decoded.path.name} were truncated in the "
                "archive and are missing attachment members; they were not replayed",
                {"window": window.label(), "run_ids": list(decoded.dropped[:20])},
            ))
        # Restricted to what is actually being ingested, so the reconciliation
        # parts stay a partition of the source total.
        prepared.degraded = {
            code: set(run_ids) & plan.to_ingest
            for code, run_ids in (decoded.manifest.get("degraded") or {}).items()
            if set(run_ids) & plan.to_ingest
        }
        if prepared.degraded:
            prepared.fidelity_notes.add("some archived runs were captured with reduced fidelity")

        outgoing = []
        for payload in decoded.payloads:
            if str(payload["id"]) not in plan.to_ingest:
                continue
            # The whole of "optionally change the destination project".
            payload["session_id"] = str(target["id"])
            outgoing.append(payload)
        max_runs, max_bytes = self._batch_limits()
        for batch in batch_traces(
            group_into_traces(outgoing), max_runs=max_runs, max_bytes=max_bytes, size_of=payload_bytes
        ):
            prepared.batches.append((batch, self._compile_batch(batch)))
        return prepared

    # -- internals --------------------------------------------------------
    def _window_paths(self, directory: Path) -> List[Path]:
        suffixes = (_SUFFIX, _SUFFIX + _PARTIAL) if self.allow_incomplete else (_SUFFIX,)
        return sorted(
            path
            for path in directory.iterdir()
            if path.is_file()
            and path.name.endswith(suffixes)
            and _WINDOW_FILE.match(path.name.split(_SUFFIX)[0])
        )

    @staticmethod
    def _require_window(path: Path) -> Window:
        window = parse_window_label(path.name.split(_SUFFIX)[0])
        if window is None:
            raise ArchiveError(f"{path.name} is not a readable window label")
        return window

    def _path_for(self, session: Dict[str, Any], window: Window) -> Path:
        base = Path(session["dir"]) / f"{window_label(window)}{_SUFFIX}"
        if base.exists():
            return base
        partial = Path(str(base) + _PARTIAL)
        if self.allow_incomplete and partial.exists():
            return partial
        raise ArchiveError(f"no window file for {window_label(window)} under {session['dir']}")


def projected_file_count(range_hours: float, window_hours: float, projects: int) -> int:
    """Window files an export of this shape would create."""
    if window_hours <= 0:
        return 0
    return max(0, math.ceil(range_hours / window_hours)) * max(1, projects)
