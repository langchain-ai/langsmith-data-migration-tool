"""Pure core for long-lived trace migration.

No HTTP client, no implicit clock: every function here takes what it needs and
returns a value, so the requirements it encodes (windowing, the read/write
contract split, digests, reconciliation arithmetic) are unit-testable without
mocking a backend. The imperative shell lives in ``core/migrators/trace.py``.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, fields
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, Iterator, List, Optional, Sequence, Set, Tuple

LONGLIVED = "longlived"

# Root and child runs of one trace can be stamped microseconds apart in either
# direction (the backend buffers for the same skew). The run-level lower bound
# is relaxed by this much so a child stamped just before its own root is still
# returned. It never widens which *traces* are selected - that is decided by
# the trace-level predicate on the root.
SKEW_BUFFER = timedelta(seconds=60)

# Read-only fields the query API returns that we need to materialise a payload
# but must never forward: blob references, and the source-only example pointer.
BLOB_REF_FIELDS = ("inputs_s3_urls", "outputs_s3_urls", "s3_urls")
READ_ONLY_SELECT = BLOB_REF_FIELDS + ("trace_tier", "reference_example_id")

# Fields of the write contract that the query API cannot project. Attachments
# come back as ``s3_urls["attachment.<name>"]`` entries instead.
WRITE_ONLY_FIELDS = frozenset({"attachments"})

# Offloadable payload fields carried in the generic ``s3_urls`` map.
S3_URL_PAYLOAD_FIELDS = ("extra", "events", "error", "serialized", "inputs", "outputs")
ATTACHMENT_PREFIX = "attachment."


@dataclass(frozen=True)
class RunIngestPayload:
    """The ingest write contract (``smith-go/runs/runs.go`` ``type Run struct``).

    Constructing one *is* the allow-list: ``manifest_id``, ``*_s3_urls``,
    ``reference_example_id`` and the token/cost rollups are structurally
    unsendable because there is no field to put them in. An SDK or backend bump
    means re-checking this list against that Go struct.
    """

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
        return {f.name: getattr(self, f.name) for f in fields(self) if getattr(self, f.name) is not None}


# Derived, not hand-maintained: adding a field to the write contract selects it
# with no second edit. Order is stable for test comparison.
RUN_QUERY_SELECT: Tuple[str, ...] = tuple(
    [f.name for f in fields(RunIngestPayload) if f.name not in WRITE_ONLY_FIELDS]
    + list(READ_ONLY_SELECT)
)


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()


@dataclass(frozen=True)
class Window:
    """A half-open ``[start, end)`` slice of the walked range."""

    start: datetime
    end: datetime

    def label(self) -> str:
        return f"{self.start.date()}..{self.end.date()}"


def iter_windows(now: datetime, max_age_days: float, window_days: float) -> Iterator[Window]:
    """Yield half-open windows covering ``[now - max_age_days, now)``, oldest first.

    Windows abut exactly, so every instant in the range belongs to exactly one.
    """
    if max_age_days <= 0 or window_days <= 0:
        raise ValueError("max_age_days and window_days must be positive")
    start = now - timedelta(days=max_age_days)
    step = timedelta(days=window_days)
    while start < now:
        end = min(start + step, now)
        yield Window(start, end)
        start = end


def resolve_window_bounds(window: Window) -> Tuple[str, str]:
    """Return ``(trace_filter, run_level_start_time)`` for one window.

    The window is expressed **only** as a predicate on the trace root, so a
    trace is selected whole and belongs to exactly one window. There is
    deliberately no run-level upper bound: children start after their root, so
    one would truncate late children out of in-window traces. The run-level
    lower bound is always sent explicitly (the endpoint otherwise defaults to
    ~1 day ago) and carries the skew buffer.
    """
    trace_filter = (
        f'and(gte(start_time,"{_iso(window.start)}"),lt(start_time,"{_iso(window.end)}"))'
    )
    return trace_filter, _iso(window.start - SKEW_BUFFER)


def is_long_lived(run: Dict[str, Any]) -> bool:
    """Tier predicate applied client-side.

    ``trace_tier`` is not accepted inside ``trace_filter`` on the V1 endpoint
    ("Attribute trace_tier not accepted"), but it *is* returned on every run,
    so the filter moves here.
    """
    return run.get("trace_tier") == LONGLIVED


def to_ingest_payload(
    source_run: Dict[str, Any],
    dest_session_id: str,
    materialised: Optional[Dict[str, Any]] = None,
) -> Tuple[RunIngestPayload, Tuple[str, ...]]:
    """Adapt one queried run to the write contract.

    ``materialised`` supplies re-inlined values for fields the source offloaded
    to blob storage (and the fetched ``attachments``); it overrides the run's
    own value for those keys. Returns the payload plus the names of source
    fields that could not be carried, for ``degraded`` reporting.
    """
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


def payload_digest(run: Dict[str, Any], attachments: Optional[Dict[str, int]] = None) -> Dict[str, str]:
    """Per-field digests over a materialised run, for the fidelity comparison.

    Field *names* and digests are safe to report; values are not. ``serialized``
    is only compared for llm/prompt runs, because the SDK drops it for every
    other run type on the way in.
    """
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


def group_into_traces(runs: Iterable[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
    """Group runs by ``trace_id``, parents before children within each trace.

    Sorting on ``dotted_order`` puts a parent before its children because a
    child's dotted order is its parent's plus a suffix.
    """
    traces: Dict[str, List[Dict[str, Any]]] = {}
    for run in runs:
        traces.setdefault(str(run.get("trace_id")), []).append(run)
    for group in traces.values():
        group.sort(key=lambda r: r.get("dotted_order") or "")
    return [traces[key] for key in sorted(traces, key=lambda t: traces[t][0].get("dotted_order") or "")]


def batch_traces(
    traces: Sequence[Sequence[Dict[str, Any]]],
    *,
    max_runs: int,
    max_bytes: int,
    size_of,
) -> Iterator[List[Dict[str, Any]]]:
    """Flush at trace boundaries, so a trace never splits across requests.

    A single trace larger than a limit still ships alone rather than being cut.
    """
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

    source_ids: Set[str]
    dest_ids: Set[str]
    to_ingest: Set[str]
    already_present: Set[str]
    extra_on_dest: Set[str]


def plan_slice(source_ids: Set[str], dest_ids: Set[str]) -> SlicePlan:
    """The diff is the work-list, the skip-list and the completeness proof.

    There is deliberately no "re-send anyway" mode: a run is immutable once
    fully ingested (its ``end_time`` is persisted), so re-sending one already
    on the destination cannot repair it. Repair means writing into a *different*
    destination session, because run identity includes ``session_id``.
    """
    present = source_ids & dest_ids
    return SlicePlan(
        source_ids=source_ids,
        dest_ids=dest_ids,
        to_ingest=source_ids - dest_ids,
        already_present=present,
        extra_on_dest=dest_ids - source_ids,
    )


@dataclass(frozen=True)
class SliceReconciliation:
    """Counts for one window. The parts must account for the source total."""

    session_id: str
    dest_session_id: str
    window: str
    source_total: int
    ingested: int
    already_present: int
    degraded: int
    blocked: int
    extra_on_dest: int = 0

    def __post_init__(self) -> None:
        total = self.ingested + self.already_present + self.degraded + self.blocked
        if total != self.source_total:
            raise ValueError(
                f"reconciliation does not add up for {self.window}: "
                f"source_total={self.source_total} but parts sum to {total}"
            )


@dataclass(frozen=True)
class SessionReconciliation:
    """Sum of a session's slices, with the same invariant."""

    session_id: str
    dest_session_id: str
    source_total: int
    ingested: int
    already_present: int
    degraded: int
    blocked: int
    extra_on_dest: int = 0

    def __post_init__(self) -> None:
        total = self.ingested + self.already_present + self.degraded + self.blocked
        if total != self.source_total:
            raise ValueError(
                f"reconciliation does not add up for session {self.session_id}: "
                f"source_total={self.source_total} but parts sum to {total}"
            )

    @classmethod
    def of(cls, session_id: str, dest_session_id: str, slices: Sequence[SliceReconciliation]):
        def s(attr: str) -> int:
            return sum(getattr(x, attr) for x in slices)

        return cls(
            session_id=session_id,
            dest_session_id=dest_session_id,
            source_total=s("source_total"),
            ingested=s("ingested"),
            already_present=s("already_present"),
            degraded=s("degraded"),
            blocked=s("blocked"),
            extra_on_dest=s("extra_on_dest"),
        )

    @property
    def complete(self) -> bool:
        return self.blocked == 0
