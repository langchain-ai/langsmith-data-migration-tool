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
# but must never forward: blob references, the tier, and the source-only
# example pointer.
READ_ONLY_SELECT = (
    "inputs_s3_urls", "outputs_s3_urls", "s3_urls", "trace_tier", "reference_example_id",
)

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
    return _as_utc(value).isoformat()


def _as_utc(value: datetime) -> datetime:
    return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)


@dataclass(frozen=True)
class Window:
    """A half-open ``[start, end)`` slice of the walked range."""

    start: datetime
    end: datetime

    def label(self) -> str:
        """Human-readable bounds in UTC, e.g. ``2026-09-01T10:00..11:42Z``.

        Carries the time, not just the date: ``--window`` is in hours, so
        sub-day windows are the norm and a date-only label made every window of
        one day print identically. The end's date is omitted when it matches the
        start's - the common case, and it keeps the label inside a column.
        Seconds appear only when a bound actually has them, so a window narrower
        than a minute still labels distinctly without widening every other line.
        """
        start, end = _as_utc(self.start), _as_utc(self.end)
        fmt = "%Y-%m-%dT%H:%M:%S" if (start.second or end.second) else "%Y-%m-%dT%H:%M"
        tail = end.strftime(fmt.split("T")[1] if start.date() == end.date() else fmt)
        return f"{start.strftime(fmt)}..{tail}Z"


def iter_windows(start: datetime, end: datetime, window_hours: float) -> Iterator[Window]:
    """Yield half-open windows covering ``[start, end)``, oldest first.

    Hours rather than days because the useful range is sub-day: a heavy project
    needs roughly 1.7 h to keep one prepare slice in memory, which as a fraction
    of a day (0.07) is a number nobody can read.

    Bounds are absolute on purpose. Deriving them from ``now`` inside here
    meant the same relative age denoted a different instant every time it was
    evaluated, so a watermark expressed in days silently drifted while a long
    migration was still running.
    """
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
    """Normalise a bound to aware UTC, reading a naive value as UTC.

    Both ends of the walk are absolute stamps, so there is no clock reading in
    window derivation at all: the same command covers the same span whenever it
    runs. Relative ages (``--max-age-days`` / ``--min-age-days``) were removed
    for exactly that reason - each evaluation of "now - N days" denoted a
    different instant, which slid the whole window grid and gave an archive's
    files a new name on every run.
    """
    return stamp.astimezone(timezone.utc) if stamp.tzinfo else stamp.replace(tzinfo=timezone.utc)


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


def verified_bounds(
    runs: Iterable[Dict[str, Any]], complete_ids: Set[str]
) -> Tuple[Optional[str], Optional[str]]:
    """Earliest and latest ``start_time`` among runs confirmed complete.

    Timestamps are parsed rather than string-compared: the API is consistent
    today, but a mix of offset-bearing and naive ISO strings would order
    wrongly, and this value is meant to be trusted as a watermark. Naive
    values are read as UTC, which is what the endpoint returns - without that,
    a mixed set raises rather than ordering, and comparing them is the whole
    point of this function.
    """
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


def payload_bytes(run: Dict[str, Any]) -> int:
    """Serialized size of one run, attachments counted as their raw bytes.

    The batching limits and the archive's byte accounting are both expressed in
    these units, so they have to agree on what a run costs.
    """
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
    return SlicePlan(
        to_ingest=source_ids - dest_ids,
        already_present=source_ids & dest_ids,
        extra_on_dest=dest_ids - source_ids,
    )


@dataclass(frozen=True)
class Reconciliation:
    """Counts for one window, or a session's total. The parts must account for
    the source total, so the "counts add up" rule holds on every real run
    rather than only in a test.
    """

    session_id: str
    dest_session_id: str
    window: str  # "" for a session-level total
    source_total: int
    ingested: int
    already_present: int
    degraded: int
    blocked: int
    extra_on_dest: int = 0
    # Bounds of the runs confirmed complete on the destination. A slice that
    # degraded or blocked anything contributes nothing, so the pair always
    # describes runs that are wholly there.
    earliest: Optional[str] = None
    latest: Optional[str] = None
    # How many runs the span above covers, so a resume overlap can be sized in
    # runs (one ingest batch) rather than guessed in time.
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
    def of(cls, session_id: str, dest_session_id: str, slices: Sequence["Reconciliation"]) -> "Reconciliation":
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
