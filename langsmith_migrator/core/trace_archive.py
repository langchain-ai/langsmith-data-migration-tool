"""Long-lived traces on disk: one solid ``.tar.zst`` per ``(project, window)``."""

from __future__ import annotations

import io
import json
import math
import re
import tarfile
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, BinaryIO, Callable, Dict, Iterator, List, Optional, Set, Tuple

import zstandard as zstd

from .trace_blobstore import (
    MARKER,
    PARTIAL,
    ArchiveError,
    BlobStore,
    CountingWriter,
    open_store,
)
from .trace_domain import (
    LOST_CONTENT_CODES,
    Window,
    batch_traces,
    group_into_traces,
    payload_bytes,
    plan_slice,
)

FORMAT_VERSION = 3

WINDOW_LOG = 25

MAX_WINDOW_BYTES = 8 * 1024**3

_MANIFEST = "MANIFEST.json"
_STAMP = "%Y%m%dT%H%M%SZ"
_SUFFIX = ".tar.zst"

_RUN_JSON = re.compile(r"^([0-9a-fA-F-]{36})\.json$")
_RUN_BLOB = re.compile(r"^([0-9a-fA-F-]{36})/(\d{1,4})$")

_WINDOW_FILE = re.compile(r"^(\d{8}T\d{6}Z)__(\d{8}T\d{6}Z)$")

_LOST_CONTENT = LOST_CONTENT_CODES


def _sample(ids: Set[str], limit: int = 3) -> str:
    """A few IDs for an error message, without printing thousands."""
    shown = sorted(ids)[:limit]
    return ", ".join(shown) + (", ..." if len(ids) > limit else "")


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


def _stamp(value: datetime) -> str:
    return value.astimezone(timezone.utc).strftime(_STAMP)


def window_label(window: Window) -> str:
    """``20260801T000000Z__20260802T000000Z``."""
    return f"{_stamp(window.start)}__{_stamp(window.end)}"


def parse_window_label(label: str) -> Optional[Window]:
    match = _WINDOW_FILE.match(label)
    if not match:
        return None
    try:
        bounds = [
            datetime.strptime(part, _STAMP).replace(tzinfo=timezone.utc) for part in match.groups()
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
    """``<slug>-<workspace id>``, the same shape as the project level below it."""
    ws = workspace or {}
    name = _slug(str(ws.get("name") or ""), 80, "workspace")
    return f"{name}-{_slug(str(ws['id']), 40, 'id')}" if ws.get("id") else name


def _tar_bytes(tar: tarfile.TarFile, name: str, data: bytes) -> int:
    info = tarfile.TarInfo(name)
    info.size = len(data)
    info.mode = 0o600
    tar.addfile(info, io.BytesIO(data))
    return len(data)


def _encode_payload(payload: Dict[str, Any]) -> Tuple[bytes, List[bytes]]:
    """Split one payload into its JSON member and its attachment members."""
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
    """``RunSink`` writing one ``.tar.zst`` per window. Makes no destination claim."""

    def __init__(
        self,
        root: str | Path,
        *,
        compress_level: int,
        dry_run: bool = False,
        workspace: Optional[Dict[str, Any]] = None,
        store: Optional[BlobStore] = None,
        **store_kwargs: Any,
    ):
        self.root = root
        self.store = store or open_store(root, **store_kwargs)
        self.workspace = workspace
        self.compress_level = compress_level
        self.dry_run = dry_run
        self.projected_bytes = 0
        self.source_bytes = 0
        self.intent: Dict[str, Any] = {}
        self.written: List[str] = []
        self.staged: List[Any] = []

    def open_target(self, session: Dict[str, Any]) -> Dict[str, Any]:
        container = self.store.container(
            workspace_dir_name(self.workspace), project_dir_name(session)
        )
        workspace = (self.workspace or {}).get("id")
        record = {"id": str(session["id"]), "name": session.get("name"), "dir": container}
        record["names"] = self.store.names(container)
        if self.dry_run:
            return record
        marker = {k: record[k] for k in ("id", "name")}
        if workspace:
            marker["workspace_id"] = str(workspace)
        self.store.ensure_container(container, MARKER, json.dumps(marker, indent=1).encode())
        return record

    def batch_limits(self) -> Tuple[int, int]:
        """No destination caps apply; the replay side re-batches to its own."""
        return 10**9, MAX_WINDOW_BYTES

    def field_ceiling(self) -> int:
        """The window ceiling, not a destination's: a file stores the run whole."""
        return MAX_WINDOW_BYTES

    def oversized_fields(self, payload: Dict[str, Any]) -> List[str]:
        """None. A file has no field limit, so an export keeps the whole run."""
        return []

    def has_window(self, target: Dict[str, Any], window: Window) -> bool:
        return self._name(window) in target.get("names", ())

    def existing_ids(self, target: Dict[str, Any], window: Window) -> Set[str]:
        """Always empty: a window file holds all of a window or none of it."""
        return set()

    def stage(self, target: Dict[str, Any], window: Window, prepared) -> Any:
        """Worker thread: encode the whole window, manifest last, publish nothing."""
        payloads = [p for batch, _ in prepared.batches for p in batch]
        run_ids = [str(p["id"]) for p in payloads]
        self._refuse_if_incomplete(window, prepared, run_ids)
        writer = (
            CountingWriter()
            if self.dry_run
            else self.store.begin(target["dir"], self._name(window))
        )
        counts = {"payloads": 0, "attachments": 0}
        try:
            params = zstd.ZstdCompressionParameters.from_level(
                self.compress_level, enable_ldm=True, window_log=WINDOW_LOG, write_checksum=1
            )
            with zstd.ZstdCompressor(compression_params=params).stream_writer(
                writer, closefd=False
            ) as stream:
                with tarfile.open(fileobj=stream, mode="w|") as tar:
                    for payload in payloads:
                        body, blobs = _encode_payload(payload)
                        counts["payloads"] += _tar_bytes(tar, f"{payload['id']}.json", body)
                        for index, blob in enumerate(blobs):
                            counts["attachments"] += _tar_bytes(
                                tar, f"{payload['id']}/{index}", blob
                            )
                        self._check_ceiling(window, counts)
                    manifest = self._manifest(target, window, prepared, run_ids, counts)
                    self._check_ceiling(window, counts, len(manifest))
                    _tar_bytes(tar, _MANIFEST, manifest)
            writer.flush_tail()
            writer.uncompressed = counts["payloads"] + counts["attachments"]
        except BaseException:
            writer.abort()
            raise
        self.staged.append(writer)
        return writer

    def commit(
        self, target: Dict[str, Any], window: Window, prepared, staged: Any
    ) -> List[Tuple[str, str]]:
        """Main thread, in window order: publish the window."""
        if staged is None:
            return []
        staged.finish()
        if staged in self.staged:
            self.staged.remove(staged)
        name = self._name(window)
        self.source_bytes += getattr(staged, "uncompressed", 0)
        if isinstance(staged, CountingWriter):
            self.projected_bytes += staged.written
        target.setdefault("names", set()).add(name)
        self.written.append(self.store.describe(target["dir"], name))
        return []

    def close_target(self, target: Dict[str, Any], recon: Any = None) -> None:
        return None

    def _refuse_if_incomplete(self, window: Window, prepared, run_ids: List[str]) -> None:
        name = self._name(window)
        missing = prepared.plan.to_ingest - set(run_ids)
        if missing:
            raise ArchiveError(
                f"{name} would be short {len(missing)} run(s) the source listed but did not "
                f"return ({_sample(missing)}). Not writing an incomplete window; re-run to "
                "retry, or move --since/--until past this window to skip it"
            )
        unplanned = set(run_ids) - prepared.plan.to_ingest
        if unplanned:
            raise ArchiveError(
                f"{name} would hold {len(unplanned)} run(s) outside this window's plan "
                f"({_sample(unplanned)})"
            )
        if len(run_ids) != len(set(run_ids)):
            seen: Set[str] = set()
            twice = {r for r in run_ids if r in seen or seen.add(r)}
            raise ArchiveError(f"{name} would hold {_sample(twice)} more than once")
        lost = {
            run_id
            for code, ids in (prepared.degraded or {}).items()
            if code in _LOST_CONTENT
            for run_id in ids & set(run_ids)
        }
        if lost:
            raise ArchiveError(
                f"{name} would hold {len(lost)} run(s) whose offloaded content could not be "
                f"fetched ({_sample(lost)}); not writing a window that is missing their fields"
            )

    def _check_ceiling(self, window: Window, counts: Dict[str, int], extra: int = 0) -> None:
        """Stop before writing past the limit a replay will refuse to read."""
        if counts["payloads"] + counts["attachments"] + extra > MAX_WINDOW_BYTES:
            raise ArchiveError(
                f"{self._name(window)} exceeds the {MAX_WINDOW_BYTES:,}-byte window ceiling, "
                "which is also the limit a replay will read; shrink --window"
            )

    @staticmethod
    def _name(window: Window) -> str:
        return f"{window_label(window)}{_SUFFIX}"

    def _manifest(self, target, window, prepared, run_ids, counts) -> bytes:
        written = set(run_ids)
        return json.dumps(
            {
                "format_version": FORMAT_VERSION,
                "workspace": self.workspace or {},
                "project": {"id": target["id"], "name": target.get("name")},
                "window": {"start": _iso(window.start), "end": _iso(window.end)},
                "run_count": len(run_ids),
                "runs_scanned": len(prepared.population),
                "run_ids": sorted(run_ids),
                "bytes": counts,
                "degraded": {
                    code: sorted(ids & written)
                    for code, ids in (prepared.degraded or {}).items()
                    if ids & written
                },
                "intent": self.intent,
            },
            indent=1,
        ).encode()

    def discard_staged(self) -> List[str]:
        """Drop anything this process started but never published, and name it."""
        removed = []
        for writer in list(self.staged):
            try:
                writer.abort()
                removed.append(getattr(writer, "partial", writer))
            except OSError:
                pass
            self.staged.remove(writer)
        return removed


@dataclass(frozen=True)
class ArchiveWindow:
    """One decoded window file."""

    name: str
    window: Window
    manifest: Dict[str, Any]
    payloads: List[Dict[str, Any]]
    dropped: Tuple[str, ...] = ()


def read_window(path: Path, *, allow_incomplete: bool = False) -> ArchiveWindow:
    """Decode one window file from the filesystem. See ``decode_window``."""
    with open(path, "rb") as raw:
        return decode_window(raw, Path(path).name, allow_incomplete=allow_incomplete)


def read_blob(store, container: str, name: str, *, allow_incomplete: bool = False) -> ArchiveWindow:
    """Decode one window out of any store."""
    with store.read(container, name) as raw:
        return decode_window(raw, name, allow_incomplete=allow_incomplete)


def decode_window(raw: BinaryIO, name: str, *, allow_incomplete: bool = False) -> ArchiveWindow:
    """Decode one window, treating every member name as untrusted."""
    incomplete = name.endswith(PARTIAL)
    if incomplete and not allow_incomplete:
        raise ArchiveError(
            f"{name} is incomplete (no manifest); pass --allow-incomplete to read it"
        )

    bodies: Dict[str, Dict[str, Any]] = {}
    blobs: Dict[str, Dict[int, bytes]] = {}
    manifest: Optional[Dict[str, Any]] = None
    total = 0
    with zstd.ZstdDecompressor().stream_reader(raw) as stream:
        with tarfile.open(fileobj=stream, mode="r|") as tar:
            for member in tar:
                if not member.isfile():
                    raise ArchiveError(f"{name}: unexpected member type {member.name!r}")
                total += member.size
                if total > MAX_WINDOW_BYTES:
                    raise ArchiveError(
                        f"{name} decompresses past {MAX_WINDOW_BYTES:,} bytes; refusing to read it"
                    )
                handle = tar.extractfile(member)
                data = handle.read() if handle else b""
                if member.name == _MANIFEST:
                    if manifest is not None:
                        raise ArchiveError(f"{name}: two {_MANIFEST} members")
                    manifest = json.loads(data)
                    continue
                if manifest is not None:
                    raise ArchiveError(f"{name}: {member.name!r} follows {_MANIFEST}")
                if member.name == MARKER:
                    continue
                run_json = _RUN_JSON.match(member.name)
                if run_json:
                    run_id = run_json.group(1)
                    if run_id in bodies:
                        raise ArchiveError(f"{name}: run {run_id} appears twice")
                    body = json.loads(data)
                    if str(body.get("id")) != run_id:
                        raise ArchiveError(
                            f"{name}: member {member.name} holds run {body.get('id')!r}"
                        )
                    bodies[run_id] = body
                    continue
                run_blob = _RUN_BLOB.match(member.name)
                if run_blob:
                    run_id, index = run_blob.group(1), int(run_blob.group(2))
                    parts = blobs.setdefault(run_id, {})
                    if index in parts:
                        raise ArchiveError(
                            f"{name}: attachment {index} of run {run_id} appears twice"
                        )
                    parts[index] = data
                    continue
                raise ArchiveError(f"{name}: unexpected member {member.name!r}")

    if manifest is None:
        if not incomplete:
            raise ArchiveError(f"{name} has no {_MANIFEST}; it was truncated")
        manifest = {}
    version = manifest.get("format_version", FORMAT_VERSION if incomplete else None)
    if version != FORMAT_VERSION:
        raise ArchiveError(
            f"{name}: unsupported format_version {version!r} (this build reads {FORMAT_VERSION})"
        )

    window = parse_window_label(name.split(_SUFFIX)[0])
    bounds = manifest.get("window") or {}
    if bounds.get("start") and bounds.get("end"):
        window = Window(
            datetime.fromisoformat(bounds["start"]), datetime.fromisoformat(bounds["end"])
        )
    if window is None:
        raise ArchiveError(f"{name}: no window bounds in the name or the manifest")

    if not incomplete:
        orphans = set(blobs) - set(bodies)
        if orphans:
            raise ArchiveError(f"{name}: attachments for {len(orphans)} run(s) with no run body")
        claimed = manifest.get("run_ids")
        if claimed is not None and sorted(bodies) != sorted(claimed):
            same_size = " with different ids" if len(claimed) == len(bodies) else ""
            raise ArchiveError(
                f"{name}: manifest lists {len(claimed)} run(s), "
                f"the file holds {len(bodies)}{same_size}"
            )
        if manifest.get("run_count") not in (None, len(bodies)):
            raise ArchiveError(
                f"{name}: manifest says run_count {manifest['run_count']}, "
                f"the file holds {len(bodies)}"
            )

    payloads, dropped = [], []
    for run_id, body in bodies.items():
        names = body.pop("attachments", None) or []
        if names:
            parts = blobs.get(run_id) or {}
            if set(parts) != set(range(len(names))):
                if not incomplete:
                    raise ArchiveError(f"{name}: run {run_id} is missing attachment members")
                dropped.append(run_id)
                continue
            body["attachments"] = {
                name: (content_type, parts[index])
                for index, (name, content_type) in enumerate(names)
            }
        payloads.append(body)
    return ArchiveWindow(name, window, manifest, payloads, tuple(sorted(dropped)))


class ArchiveSource:
    """``RunSource`` reading window files back out of an archive directory."""

    def __init__(
        self,
        root: str | Path,
        *,
        allow_incomplete: bool = False,
        range_start: Optional[datetime] = None,
        range_end: Optional[datetime] = None,
        workspace: Optional[str] = None,
        store: Optional[BlobStore] = None,
        **store_kwargs: Any,
    ):
        self.root = root
        self.store = store or open_store(root, **store_kwargs)
        self.allow_incomplete = allow_incomplete
        self.workspace = workspace
        self.range_start = range_start
        self.range_end = range_end
        self._batch_limits: Callable[[], Tuple[int, int]] = lambda: (100, 20_971_520)
        self._compile_batch: Callable[[List[Dict[str, Any]]], Any] = lambda batch: None
        self._oversized_fields: Callable[[Dict[str, Any]], List[str]] = lambda payload: []

    def bind(self, sink) -> None:
        """Take the live destination's batch caps and frame compiler."""
        self._batch_limits = sink.batch_limits
        self._compile_batch = sink.compile_batch
        self._oversized_fields = sink.oversized_fields

    def sessions(self) -> List[Dict[str, Any]]:
        """Every project in the archive, found by its ``PROJECT.json`` marker."""
        found = []
        for container, body in self.store.find_containers():
            record = json.loads(body)
            if not self._in_workspace(container, record):
                continue
            names = self._window_names(container)
            if not names:
                continue
            found.append(
                {"id": record["id"], "name": record.get("name"), "dir": container, "names": names}
            )
        return found

    def workspaces(self) -> Set[str]:
        """Every source workspace the archive holds, from its own metadata."""
        found = set()
        for container, body in self.store.find_containers():
            if not self._window_names(container):
                continue
            recorded = json.loads(body).get("workspace_id") or self._workspace_from_manifest(
                container
            )
            if recorded:
                found.add(str(recorded))
        return found

    def windows(self, session: Dict[str, Any]) -> Iterator[Window]:
        for name in sorted(session.get("names") or self._window_names(session["dir"])):
            window = self._require_window(name)
            if self.range_start and window.end <= self.range_start:
                continue
            if self.range_end and window.start >= self.range_end:
                continue
            yield window

    def prepare(self, session, target, window, existing_ids: Callable[[], Set[str]]):
        from .trace_ports import SlicePrepared

        decoded = read_blob(
            self.store,
            session["dir"],
            self._name_for(session, window),
            allow_incomplete=self.allow_incomplete,
        )
        ids = {str(p["id"]) for p in decoded.payloads}
        plan = plan_slice(ids, existing_ids() if ids else set())
        population = [
            {
                "id": str(p["id"]),
                "trace_id": str(p.get("trace_id")),
                "start_time": p.get("start_time"),
            }
            for p in decoded.payloads
        ]
        prepared = SlicePrepared(window, str(session["id"]), str(target["id"]), plan, population)
        if decoded.dropped:
            prepared.issues.append(
                (
                    "degraded",
                    "archived_run_incomplete",
                    f"{len(decoded.dropped)} run(s) in {decoded.name} were truncated in the "
                    "archive and are missing attachment members; they were not replayed",
                    {"window": window.label(), "run_ids": list(decoded.dropped[:20])},
                )
            )
        prepared.degraded = {
            code: set(run_ids) & plan.to_ingest
            for code, run_ids in (decoded.manifest.get("degraded") or {}).items()
            if set(run_ids) & plan.to_ingest
        }
        if prepared.degraded:
            prepared.fidelity_notes.add("some archived runs were captured with reduced fidelity")

        outgoing = []
        for payload in decoded.payloads:
            run_id = str(payload["id"])
            if run_id not in plan.to_ingest:
                continue
            if self._oversized_fields(payload):
                prepared.degraded.setdefault("payload_oversized_for_destination", set()).add(run_id)
                prepared.fidelity_notes.add("payloads exceeded --max-field-bytes")
                continue
            payload["session_id"] = str(target["id"])
            outgoing.append(payload)
        max_runs, max_bytes = self._batch_limits()
        for batch in batch_traces(
            group_into_traces(outgoing),
            max_runs=max_runs,
            max_bytes=max_bytes,
            size_of=payload_bytes,
        ):
            prepared.batches.append((batch, self._compile_batch(batch)))
        return prepared

    def _in_workspace(self, container: str, record: Dict[str, Any]) -> bool:
        """Whether this project belongs to the workspace being replayed."""
        if not self.workspace:
            return True
        recorded = record.get("workspace_id") or self._workspace_from_manifest(container)
        if not recorded:
            raise ArchiveError(
                f"{self.store.describe(container)} records no workspace in its marker or its "
                "windows, so it cannot be matched against --source-workspace; drop the flag "
                "to replay the whole archive"
            )
        return str(recorded) == str(self.workspace)

    def _workspace_from_manifest(self, container: str) -> Optional[str]:
        """The workspace from any one window, for markers written before it."""
        for name in sorted(self._window_names(container)):
            try:
                manifest = read_blob(
                    self.store, container, name, allow_incomplete=self.allow_incomplete
                ).manifest
            except ArchiveError:
                continue
            found = (manifest.get("workspace") or {}).get("id")
            if found:
                return str(found)
        return None

    def _window_names(self, container: str) -> Set[str]:
        suffixes = (_SUFFIX, _SUFFIX + PARTIAL) if self.allow_incomplete else (_SUFFIX,)
        return {
            name
            for name in self.store.names(container)
            if name.endswith(suffixes) and _WINDOW_FILE.match(name.split(_SUFFIX)[0])
        }

    @staticmethod
    def _require_window(name: str) -> Window:
        window = parse_window_label(name.split(_SUFFIX)[0])
        if window is None:
            raise ArchiveError(f"{name} is not a readable window label")
        return window

    def _name_for(self, session: Dict[str, Any], window: Window) -> str:
        names = session.get("names") or self._window_names(session["dir"])
        base = f"{window_label(window)}{_SUFFIX}"
        if base in names:
            return base
        if self.allow_incomplete and base + PARTIAL in names:
            return base + PARTIAL
        raise ArchiveError(
            f"no window file for {window_label(window)} under {self.store.describe(session['dir'])}"
        )


def projected_file_count(range_hours: float, window_hours: float, projects: int) -> int:
    """Window files an export of this shape would create."""
    if window_hours <= 0:
        return 0
    return max(0, math.ceil(range_hours / window_hours)) * max(1, projects)
