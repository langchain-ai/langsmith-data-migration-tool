"""Pure core for long-lived trace migration."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, fields
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

LOST_CONTENT_CODES = frozenset(
    {
        "attachment_fetch_failed",
        "attachment_host_rejected",
        "payload_fetch_failed",
        "payload_host_rejected",
        "payload_field_unavailable",
    }
)

LONGLIVED = "longlived"

SKEW_BUFFER = timedelta(seconds=60)

READ_ONLY_SELECT = (
    "inputs_s3_urls",
    "outputs_s3_urls",
    "s3_urls",
    "trace_tier",
    "reference_example_id",
)

WRITE_ONLY_FIELDS = frozenset({"attachments"})

S3_URL_PAYLOAD_FIELDS = ("extra", "events", "error", "serialized", "inputs", "outputs")
ATTACHMENT_PREFIX = "attachment."


@dataclass(frozen=True)
class RunIngestPayload:
    """The ingest write contract (``smith-go/runs/runs.go`` ``type Run struct``)."""

    id: str
    trace_id: str
    name: str
    run_type: str
    start_time: str
    dotted_order: str
    session_id: str
    end_time: Optional[str] = None
    parent_run_id: Optional[str] = None
    status: Optional[str] = None
    inputs: Optional[Any] = None
    outputs: Optional[Any] = None
    extra: Optional[Any] = None
    error: Optional[Any] = None
    serialized: Optional[Any] = None
    events: Optional[Any] = None
    tags: Optional[Any] = None
    attachments: Optional[Dict[str, Tuple[str, bytes]]] = None

    def as_dict(self) -> Dict[str, Any]:
        """Drop unset optionals so the destination keeps its own defaults."""
        return {
            f.name: getattr(self, f.name) for f in fields(self) if getattr(self, f.name) is not None
        }


RUN_QUERY_SELECT: Tuple[str, ...] = tuple(
    [f.name for f in fields(RunIngestPayload) if f.name not in WRITE_ONLY_FIELDS]
    + list(READ_ONLY_SELECT)
)


def _iso(value: datetime) -> str:
    return _as_utc(value).isoformat()


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)


@dataclass(frozen=True)
class Window:
    """A half-open ``[start, end)`` slice of the walked range."""

    start: datetime
    end: datetime

    def label(self) -> str:
        """Human-readable bounds in UTC, e.g. ``2026-09-01T10:00..11:42Z``."""
        start, end = _as_utc(self.start), _as_utc(self.end)
        fmt = "%Y-%m-%dT%H:%M:%S" if (start.second or end.second) else "%Y-%m-%dT%H:%M"
        tail = end.strftime(fmt.split("T")[1] if start.date() == end.date() else fmt)
        return f"{start.strftime(fmt)}..{tail}Z"


def iter_windows(start: datetime, end: datetime, window_hours: float) -> Iterator[Window]:
    """Yield half-open windows covering ``[start, end)``, oldest first."""
    if window_hours <= 0:
        raise ValueError("window_hours must be positive")
    if start >= end:
        raise ValueError("range start must precede its end")
    step = timedelta(hours=window_hours)
    while start < end:
        boundary = min(start + step, end)
        yield Window(start, boundary)
        start = boundary


def as_utc(stamp: datetime) -> datetime:
    """Normalise a bound to aware UTC, reading a naive value as UTC."""
    return stamp.astimezone(timezone.utc) if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)


def resolve_window_bounds(window: Window) -> Tuple[str, str]:
    """Return ``(trace_filter, run_level_start_time)`` for one window."""
    trace_filter = (
        f'and(gte(start_time,"{_iso(window.start)}"),lt(start_time,"{_iso(window.end)}"))'
    )
    return trace_filter, _iso(window.start - SKEW_BUFFER)


def is_long_lived(run: Dict[str, Any]) -> bool:
    """Tier predicate applied client-side."""
    return run.get("trace_tier") == LONGLIVED


def to_ingest_payload(
    source_run: Dict[str, Any],
    dest_session_id: str,
    materialised: Optional[Dict[str, Any]] = None,
) -> Tuple[RunIngestPayload, Tuple[str, ...]]:
    """Adapt one queried run to the write contract."""
    merged = dict(source_run)
    merged.update(materialised or {})

    dropped: List[str] = []
    if source_run.get("reference_example_id"):
        dropped.append("reference_example_id")

    payload = RunIngestPayload(
        id=str(merged["id"]),
        trace_id=str(merged["trace_id"]),
        name=merged.get("name") or "run",
        run_type=merged.get("run_type") or "chain",
        start_time=merged["start_time"],
        dotted_order=merged["dotted_order"],
        session_id=str(dest_session_id),
        end_time=merged.get("end_time"),
        parent_run_id=(str(merged["parent_run_id"]) if merged.get("parent_run_id") else None),
        status=merged.get("status"),
        inputs=merged.get("inputs"),
        outputs=merged.get("outputs"),
        extra=merged.get("extra"),
        error=merged.get("error"),
        serialized=merged.get("serialized"),
        events=merged.get("events"),
        tags=merged.get("tags"),
        attachments=merged.get("attachments") or None,
    )
    return payload, tuple(dropped)


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), default=str)


def payload_digest(
    run: Dict[str, Any], attachments: Optional[Dict[str, int]] = None
) -> Dict[str, str]:
    """Per-field digests over a materialised run, for the fidelity comparison."""
    parts = {
        "inputs": run.get("inputs"),
        "outputs": run.get("outputs"),
        "error": run.get("error"),
        "events": run.get("events"),
    }
    if run.get("run_type") in ("llm", "prompt"):
        parts["serialized"] = run.get("serialized")
    if attachments is not None:
        parts["attachments"] = sorted(attachments.items())
    return {
        key: hashlib.sha256(_canonical(value).encode()).hexdigest()[:16]
        for key, value in parts.items()
    }


def digest_mismatches(left: Dict[str, str], right: Dict[str, str]) -> Tuple[str, ...]:
    """Field names whose digests differ. Never returns values."""
    return tuple(sorted(k for k in set(left) | set(right) if left.get(k) != right.get(k)))


def verified_bounds(
    runs: Iterable[Dict[str, Any]], complete_ids: Set[str]
) -> Tuple[Optional[str], Optional[str]]:
    """Earliest and latest ``start_time`` among runs confirmed complete."""
    stamps = []
    for run in runs:
        if str(run.get("id")) not in complete_ids or not run.get("start_time"):
            continue
        try:
            stamp = datetime.fromisoformat(str(run["start_time"]).replace("Z", "+00:00"))
        except ValueError:
            continue
        stamps.append(stamp if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc))
    if not stamps:
        return None, None
    return min(stamps).isoformat(), max(stamps).isoformat()


def group_into_traces(runs: Iterable[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """Group runs by ``trace_id``, parents before children within each trace."""
    traces: Dict[str, List[Dict[str, Any]]] = {}
    for run in runs:
        traces.setdefault(str(run.get("trace_id")), []).append(run)
    for group in traces.values():
        group.sort(key=lambda r: r.get("dotted_order") or "")
    return [
        traces[key] for key in sorted(traces, key=lambda t: traces[t][0].get("dotted_order") or "")
    ]


def payload_bytes(run: Dict[str, Any]) -> int:
    """Serialized size of one run, attachments counted as their raw bytes."""
    return len(json.dumps({k: v for k, v in run.items() if k != "attachments"}, default=str)) + sum(
        len(data) for _, data in (run.get("attachments") or {}).values()
    )


def batch_traces(
    traces: Sequence[Sequence[Dict[str, Any]]],
    *,
    max_runs: int,
    max_bytes: int,
    size_of,
) -> Iterator[List[Dict[str, Any]]]:
    """Flush at trace boundaries, so a trace never splits across requests."""
    batch: List[Dict[str, Any]] = []
    batch_bytes = 0
    for trace in traces:
        trace_bytes = sum(size_of(run) for run in trace)
        if batch and (len(batch) + len(trace) > max_runs or batch_bytes + trace_bytes > max_bytes):
            yield batch
            batch, batch_bytes = [], 0
        batch.extend(trace)
        batch_bytes += trace_bytes
    if batch:
        yield batch


@dataclass(frozen=True)
class SlicePlan:
    """What one ``(session, window)`` diff says to do."""

    to_ingest: Set[str]
    already_present: Set[str]
    extra_on_dest: Set[str]


def plan_slice(source_ids: Set[str], dest_ids: Set[str]) -> SlicePlan:
    """The diff is the work-list, the skip-list and the completeness proof."""
    return SlicePlan(
        to_ingest=source_ids - dest_ids,
        already_present=source_ids & dest_ids,
        extra_on_dest=dest_ids - source_ids,
    )


@dataclass(frozen=True)
class Reconciliation:
    """Counts for one window, or a session's total. The parts must account for"""

    session_id: str
    dest_session_id: str
    window: str  # "" for a session-level total
    source_total: int
    ingested: int
    already_present: int
    degraded: int
    blocked: int
    extra_on_dest: int = 0
    earliest: Optional[str] = None
    latest: Optional[str] = None
    verified_runs: int = 0

    def __post_init__(self) -> None:
        total = self.ingested + self.already_present + self.degraded + self.blocked
        if total != self.source_total:
            scope = f"window {self.window}" if self.window else f"session {self.session_id}"
            raise ValueError(
                f"reconciliation does not add up for {scope}: "
                f"source_total={self.source_total} but parts sum to {total}"
            )

    @classmethod
    def of(
        cls, session_id: str, dest_session_id: str, slices: Sequence["Reconciliation"]
    ) -> "Reconciliation":
        def s(attr: str) -> int:
            return sum(getattr(x, attr) for x in slices)

        return cls(
            session_id=session_id,
            dest_session_id=dest_session_id,
            window="",
            source_total=s("source_total"),
            ingested=s("ingested"),
            already_present=s("already_present"),
            degraded=s("degraded"),
            blocked=s("blocked"),
            extra_on_dest=s("extra_on_dest"),
            earliest=min([x.earliest for x in slices if x.earliest], default=None),
            latest=max([x.latest for x in slices if x.latest], default=None),
            verified_runs=s("verified_runs"),
        )

    @property
    def complete(self) -> bool:
        return self.blocked == 0
