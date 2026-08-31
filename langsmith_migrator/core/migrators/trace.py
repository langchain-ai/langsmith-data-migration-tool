"""Long-lived trace migration.

Copies runs whose trace tier is ``longlived`` between two LangSmith
deployments, preserving ``id`` / ``trace_id`` / ``parent_run_id`` /
``dotted_order`` and every timestamp verbatim. Contrast with
``ExperimentMigrator``, which remaps run IDs, time-shifts into the ingest
window, and checkpoints a cursor: none of that happens here.

The unit of work is a ``(session, time window)`` slice. For each one we ask the
source for its long-lived run IDs, ask the mapped destination session for the
same, ingest the difference, and re-query to confirm the difference is now
empty. That diff is simultaneously the work-list, the skip-list and the
completeness proof, which is why nothing is checkpointed: interrupt and re-run,
and finished windows produce an empty diff. ``resume`` does not apply.

Two backend facts drive most of the shape here:

* A run id is write-once per tenant: re-sending one is refused with ``409 Run
  create payload already received``, not upserted. Idempotency comes from the ID
  diff never re-sending a run the destination already holds - which is also why
  migrating into the *same* tenant is refused outright.
* There is no per-run trace-tier field on the ingest contract. The destination
  *session's* tier is the only lever, and it must be correct at ingest time
  because the row TTL and the blob key prefix are both baked in at insert.
"""

from __future__ import annotations

import json
import re
import threading
import time
import uuid
from dataclasses import dataclass
from dataclasses import field as _field
from datetime import datetime, timezone
from collections import deque
from concurrent.futures import Future, ThreadPoolExecutor
from tempfile import SpooledTemporaryFile
from typing import Any, Deque, Dict, Iterator, List, Optional, Sequence, Set, Tuple
from urllib.parse import urlparse

import requests
from langsmith import Client

from ..api_client import APIError, NotFoundError
from ..trace_frames import (
    DEFAULT_COMPRESS_LEVEL,
    CompiledFrame,
    compile_frame,
    unavailable_reason,
)
from ..trace_domain import (
    ATTACHMENT_PREFIX,
    LONGLIVED,
    RUN_QUERY_SELECT,
    S3_URL_PAYLOAD_FIELDS,
    Reconciliation,
    SlicePlan,
    Window,
    batch_traces,
    digest_mismatches,
    group_into_traces,
    is_long_lived,
    iter_windows,
    payload_digest,
    plan_slice,
    resolve_range_start,
    resolve_window_bounds,
    to_ingest_payload,
    verified_bounds,
)
from .base import BaseMigrator

# The binding constraint is the *response* size, not the ID count: 500 IDs of a
# heavy project's full payloads is ~65 MB and a gateway rejects it with a 502 in
# under ten seconds, while 500 IDs of a light one is fine. So this is a starting
# point that halves itself on rejection, down to _ID_CHUNK_MIN.
_ID_CHUNK = 500
_ID_CHUNK_MIN = 25

# Both real deployments cap /runs/query at 1000. A deployment that caps lower
# says so in the 400, so the limit self-corrects rather than being probed.
# Measured: an ``id``-filtered query ignores the limit entirely (2000 IDs at
# limit=10 returned all 2000 in one page). Not relied on - ID chunks are held
# at or under the limit so a request never depends on that leniency.
_PAGE_LIMIT = 1000
_PAGE_CAP_RE = re.compile(r"maximum allowed value of (\d+)")

# Not advertised by /info (only the batch limits are), so it matches the
# backend's own MAX_ATTACHMENT_SIZE_BYTES default and is a guard, not a truth.
_MAX_ATTACHMENT_BYTES = 200 * 1024 * 1024

_VERIFY_ATTEMPTS = 4
_VERIFY_BACKOFF = 3.0

_CANARY_SESSION = "langsmith-migrator-canary"


def _size_connection_pools(*clients_and_size) -> None:
    """Give each client a connection pool that fits the reader count.

    urllib3 defaults to 10, and a full pool silently drops connections rather
    than queueing, which shows up as ChunkedEncodingError under load.

    The sessions themselves stay shared across the prepare workers. urllib3's
    pools are thread-safe; what is not is mutating session state, and the only
    mutable piece in play is the cookie jar - measured empty, as neither
    deployment sends Set-Cookie on the endpoints this migrator uses. Headers
    are mutated only on the destination write session, which is main-thread
    only.
    """
    *clients, workers = clients_and_size
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=max(10, workers * 2), pool_maxsize=max(10, workers * 2)
    )
    for client in clients:
        session = getattr(client, "session", None)
        if session is not None and hasattr(session, "mount"):
            session.mount("https://", adapter)
            session.mount("http://", adapter)


def _response_too_large(error: Exception) -> bool:
    """Whether a failure looks like "that response was too big to serve".

    The gateway in front of LangSmith answers 502 (or 413) on an oversized
    response body rather than saying so, and it does it fast - there is nothing
    to distinguish it from a genuine outage except that retrying at the same
    size keeps failing, which the retry layer has already established.
    """
    status = getattr(error, "status_code", None)
    return status in (413, 502, 503)


def _page_limit_cap(error: Exception) -> Optional[int]:
    """The page cap a deployment advertises when it rejects our limit."""
    match = _PAGE_CAP_RE.search(str(error))
    return int(match.group(1)) if match else None


def _raise_if_tier_denied(error: Exception) -> None:
    """Turn a 403 on a tier-carrying write into an explicit pre-flight blocker.

    ``PROJECTS_INCREASE_TRACE_TIER`` is checked on *create* too, not only on
    update, when the destination tenant's default tier is short-lived.
    """
    text = str(error)
    if "403" in text or "Forbidden" in text:
        raise TracePreflightError(
            "trace_tier_permission_denied: the destination key needs PROJECTS_INCREASE_TRACE_TIER "
            f"(and PROJECTS_DECREASE_TRACE_TIER to restore). {text[:200]}"
        )




@dataclass
class SlicePrepared:
    """One slice's work, built off the main thread and committed on it.

    Findings accumulate here rather than on the migrator so a worker never
    writes shared state; ``commit_slice`` merges them in window order.
    """

    window: Window
    src_id: str
    dst_id: str
    plan: SlicePlan
    population: List[Dict[str, Any]]
    # (payloads, pre-compiled frame). The payloads are kept so a rejected batch
    # can still be split to isolate one bad run; that is what bounds peak
    # memory to prefetch x window, and why --window is the knob for it.
    batches: List[Tuple[List[Dict[str, Any]], Optional[CompiledFrame]]] = _field(default_factory=list)
    digests: Dict[str, Dict[str, str]] = _field(default_factory=dict)
    degraded: Dict[str, Set[str]] = _field(default_factory=dict)
    fidelity_notes: Set[str] = _field(default_factory=set)
    upgrade_rows: List[Tuple[str, str, str]] = _field(default_factory=list)
    # (issue_class, code, summary, evidence), replayed through record_issue.
    issues: List[Tuple[str, str, str, Dict[str, Any]]] = _field(default_factory=list)


class TracePreflightError(RuntimeError):
    """A destination precondition failed; nothing may be migrated."""


class TraceMigrator(BaseMigrator):
    """Migrates long-lived traces, statelessly, one (session, window) at a time."""

    def __init__(
        self,
        source_client,
        dest_client,
        state,
        config,
        *,
        max_age_days: Optional[float] = 180.0,
        range_start: Optional[datetime] = None,
        window_days: float = 1.0,
        max_field_bytes: int = 25 * 1024 * 1024,
        verify: bool = True,
        verify_content_sample: int = 100,
        skip_attachments: bool = False,
        restore_session_tier: Optional[bool] = None,
        into_session_suffix: Optional[str] = None,
        emit_upgrade_list: Optional[str] = None,
        project_id_map: Optional[Dict[str, str]] = None,
        compress_level: Optional[int] = DEFAULT_COMPRESS_LEVEL,
        prefetch_windows: int = 4,
    ):
        super().__init__(source_client, dest_client, state, config)
        self.max_age_days = max_age_days
        self.range_start = range_start
        # Resolved exactly once. Recomputing "now - max_age_days" per call gave
        # a slightly different instant each time, which is the same drift an
        # absolute --max-age-stamp exists to avoid.
        self._resolved_start = resolve_range_start(
            datetime.now(timezone.utc), max_age_days, range_start
        )
        self.window_days = window_days
        self.max_field_bytes = max_field_bytes
        self.verify = verify
        self.verify_content_sample = verify_content_sample
        self.skip_attachments = skip_attachments
        self.restore_session_tier = restore_session_tier
        self.into_session_suffix = into_session_suffix
        self.emit_upgrade_list = emit_upgrade_list
        self.project_id_map = dict(project_id_map or {})

        # What was reduced, stated as fact. A run that landed incomplete is
        # invisible to the ordinary ID diff, since its ID is present.
        self.fidelity_reduced: Set[str] = set()
        if skip_attachments:
            self.fidelity_reduced.add("attachments were skipped (--skip-attachments)")
        self.upgrade_rows: List[Tuple[str, str, str]] = []
        # Human-readable reasons for blocked runs, so the CLI can explain a
        # blocked count instead of only recording it into state.
        self.blocked_reasons: List[str] = []

        # Per side: the two deployments need not cap /runs/query alike.
        self._page_limit = {"source": _PAGE_LIMIT, "dest": _PAGE_LIMIT}
        # Shrinks itself when a project's payloads make the response too big.
        self._id_chunk = _ID_CHUNK

        # None = send uncompressed through the SDK's public path. Resolved
        # once, after the client exists, so a missing SDK internal or a
        # destination without the feature degrades instead of failing per batch.
        self.compress_level = compress_level
        self.compress_unavailable: Optional[str] = None

        # How many windows may be retrieved concurrently. Ingest stays serial
        # whatever this is; only the reading ahead is parallel. Peak memory is
        # roughly this many windows of payloads, so --window trades it back.
        self.prefetch_windows = max(1, prefetch_windows)
        self._main_thread = threading.get_ident()
        # Sized to the readers: the default pool of 10 would churn connections
        # once several windows are in flight.
        _size_connection_pools(source_client, dest_client, self.prefetch_windows)

        # Blobs are proxied by whichever deployment stores them, so the
        # allow-list is per side: the destination's read-back URLs live on the
        # destination host, not the source's.
        self._source_blob_host = urlparse(self._host(config.source.base_url)).netloc
        self._dest_blob_host = urlparse(self._host(config.destination.base_url)).netloc

        self._dest_session_http = requests.Session()
        if not config.destination.verify_ssl:
            self._dest_session_http.verify = False
        self._ingest_errors: List[Exception] = []
        # The SDK reports nothing on a 2xx, and a backend that accepts the
        # request then drops the runs is indistinguishable from success. We own
        # this session, so record what the multipart POST actually answered.
        self._ingest_responses: List[Tuple[int, str]] = []
        _session_request = self._dest_session_http.request

        def _record_ingest(method, url, **kwargs):
            response = _session_request(method, url, **kwargs)
            if "/runs/multipart" in str(url):
                try:
                    body = (response.text or "").strip()[:600]
                except Exception:
                    body = "<unreadable>"
                self._ingest_responses.append((response.status_code, body))
            return response

        self._dest_session_http.request = _record_ingest
        # The SDK logs multipart failures and returns normally, so an exception
        # never reaches us. The callback is the only way to notice.
        self.dest_ls_client = Client(
            api_key=config.destination.api_key,
            api_url=self._host(config.destination.base_url),
            session=self._dest_session_http,
            omit_traced_runtime_info=True,
            tracing_error_callback=self._ingest_errors.append,
        )
        if self.compress_level is not None:
            self.compress_unavailable = unavailable_reason(self.dest_ls_client)
            if self.compress_unavailable:
                self.compress_level = None

    def resolved_range_start(self) -> datetime:
        """Absolute lower bound of this run's walk, fixed for the whole run."""
        return self._resolved_start

    # ------------------------------------------------------------------
    # Plumbing
    # ------------------------------------------------------------------
    @staticmethod
    def _human(size: float) -> str:
        for unit in ("B", "KB", "MB", "GB"):
            if size < 1024 or unit == "GB":
                return f"{size:,.0f} {unit}" if unit == "B" else f"{size:,.1f} {unit}"
            size /= 1024
        return f"{size:,.1f} GB"

    @staticmethod
    def _shape(runs: Sequence[Dict[str, Any]]) -> Tuple[int, int, int]:
        """``(runs, distinct traces, serialized bytes)`` for a page or a batch."""
        traces = {str(r.get("trace_id")) for r in runs}
        size = sum(
            len(json.dumps({k: v for k, v in r.items() if k != "attachments"}, default=str))
            + sum(len(data) for _, data in (r.get("attachments") or {}).values())
            for r in runs
        )
        return len(runs), len(traces), size

    @staticmethod
    def _host(base_url: str) -> str:
        clean = (base_url or "").rstrip("/")
        return clean[: -len("/api/v1")] if clean.endswith("/api/v1") else clean

    def _sync_dest_headers(self) -> None:
        ws = self.dest.session.headers.get("X-Tenant-Id")
        if ws:
            self._dest_session_http.headers["X-Tenant-Id"] = ws
        else:
            self._dest_session_http.headers.pop("X-Tenant-Id", None)

    def batch_limits(self) -> Tuple[int, int]:
        """Runs-per-request and bytes-per-request the destination advertises."""
        try:
            cfg = dict(self.dest_ls_client.info.batch_ingest_config or {})
        except Exception:
            cfg = {}
        return int(cfg.get("size_limit") or 100), int(cfg.get("size_limit_bytes") or 20_971_520)

    # ------------------------------------------------------------------
    # Read path
    # ------------------------------------------------------------------
    def _query_runs(
        self, client, session_id: str, window: Window, *, select: Sequence[str], ids=None
    ) -> Iterator[Dict[str, Any]]:
        """Paginate ``POST /runs/query`` for one session and window."""
        trace_filter, run_start = resolve_window_bounds(window)
        body: Dict[str, Any] = {
            "session": [str(session_id)],
            "trace_filter": trace_filter,
            "start_time": run_start,
            "select": list(select),
        }
        if ids is not None:
            body["id"] = [str(i) for i in ids]
        side = "source" if client is self.source else "dest"
        body["limit"] = self._page_limit[side]
        page_num, seen = 0, 0
        while True:
            try:
                response = client.post("/runs/query", body) or {}
            except APIError as exc:
                cap = _page_limit_cap(exc)
                if cap is None or cap >= body["limit"]:
                    raise
                # This deployment pages smaller than we asked. Adopt its
                # number for the rest of the run and re-send.
                self._page_limit[side] = cap
                body = {**body, "limit": cap}
                continue
            runs = response.get("runs") or []
            page_num += 1
            seen += len(runs)
            if self.config.migration.verbose and runs:
                n, traces, size = self._shape(runs)
                # Kept under 80 columns: a wrapped progress line is worse
                # than a terse one.
                self.console.print(
                    f"[dim]  query {side:<6} p{page_num:<4}"
                    f"runs {n:>4} | traces {traces:>4} | {self._human(size):>9} | "
                    f"total {seen:>6}[/dim]"
                )
            yield from runs
            cursor = (response.get("cursors") or {}).get("next")
            if not cursor or not runs:
                return
            body = {**body, "cursor": cursor}

    # Enough to identify a run and place its trace, so one pass over a slice
    # serves both the diff and the deferred-upgrade list.
    _ID_SELECT = ("id", "trace_tier", "trace_id", "start_time")

    def slice_runs(self, client, session_id: str, window: Window, *, tier_filter: bool = True) -> List[Dict[str, Any]]:
        """The long-lived runs of one slice, projected to identity fields only.

        ``trace_tier`` is filtered client-side: the V1 endpoint rejects it
        inside ``trace_filter`` ("Attribute trace_tier not accepted") but
        returns it on every run.
        """
        return [
            run
            for run in self._query_runs(client, session_id, window, select=self._ID_SELECT)
            if run.get("id") and (not tier_filter or is_long_lived(run))
        ]

    def long_lived_run_ids(self, client, session_id: str, window: Window, *, tier_filter: bool = True) -> Set[str]:
        return {str(r["id"]) for r in self.slice_runs(client, session_id, window, tier_filter=tier_filter)}

    def fetch_runs(
        self, client, session_id: str, window: Window, run_ids: Sequence[str]
    ) -> Iterator[Dict[str, Any]]:
        """Full payloads for a set of IDs, chunked so no response is oversized.

        A chunk is materialised before being yielded so an oversized response
        can be retried at half the size without double-yielding what the failed
        attempt had already produced.
        """
        side = "source" if client is self.source else "dest"
        ids = list(run_ids)
        start = 0
        while start < len(ids):
            chunk = min(self._id_chunk, self._page_limit[side])
            try:
                page = list(
                    self._query_runs(
                        client, session_id, window, select=RUN_QUERY_SELECT, ids=ids[start : start + chunk]
                    )
                )
            except APIError as exc:
                if not _response_too_large(exc) or chunk <= _ID_CHUNK_MIN:
                    raise
                # Retries at this size are already exhausted, so the size is
                # the problem. Halve it and keep it for the rest of the run.
                self._id_chunk = max(_ID_CHUNK_MIN, chunk // 2)
                self.log(
                    f"  {chunk} IDs per request was refused ({exc.status_code}); "
                    f"continuing at {self._id_chunk}",
                    "warning",
                )
                continue
            yield from page
            start += chunk

    # ------------------------------------------------------------------
    # Blobs
    # ------------------------------------------------------------------
    def _fetch_blob(self, url: str, limit: int, host: str, sess) -> Tuple[str, bytes]:
        """Download one presigned blob through the tool's configured session.

        Never the SDK's own conversion, which uses a bare ``requests.get`` that
        ignores SSL config, CA bundles and proxies, and substitutes a
        placeholder on failure. Redirects are not followed: the deployment
        proxies blob downloads on its own host, so a redirect would be the
        source steering us somewhere else.
        """
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.netloc != host:
            raise PermissionError("attachment_host_rejected")
        with sess.get(url, stream=True, timeout=120, allow_redirects=False) as resp:
            resp.raise_for_status()
            content_type = (resp.headers.get("Content-Type") or "application/octet-stream").split(";")[0].strip()
            with SpooledTemporaryFile(max_size=8 * 1024 * 1024) as buf:
                size = 0
                for chunk in resp.iter_content(65536):
                    size += len(chunk)
                    if size > limit:
                        raise ValueError(f"blob exceeds {limit} bytes")
                    buf.write(chunk)
                buf.seek(0)
                return content_type, buf.read()

    def materialise(self, run: Dict[str, Any], *, side: str = "source") -> Tuple[Dict[str, Any], List[str]]:
        """Re-inline anything the source left in blob storage, and fetch attachments.

        The query API usually resolves offloaded ``inputs`` / ``outputs`` /
        ``extra`` for us; the blob reference is only followed when the inline
        value is genuinely absent. Attachments are never inlined and always
        need fetching. Returns ``(overrides, degraded_codes)`` - a failure is
        explicit, never a placeholder.
        """
        overrides: Dict[str, Any] = {}
        issues: List[str] = []
        src = side == "source"
        host = self._source_blob_host if src else self._dest_blob_host
        sess = (self.source if src else self.dest).session

        def pull(field: str, ref: Dict[str, Any], limit: int) -> None:
            url = (ref or {}).get("presigned_url")
            if not url:
                return
            try:
                _, raw = self._fetch_blob(url, limit, host, sess)
                overrides[field] = json.loads(raw)
            except PermissionError:
                issues.append("attachment_host_rejected")
            except Exception:
                issues.append("attachment_fetch_failed")

        for field, key in (("inputs", "inputs_s3_urls"), ("outputs", "outputs_s3_urls")):
            refs = run.get(key) or {}
            if run.get(field) is None and refs.get("ROOT"):
                pull(field, refs["ROOT"], self.max_field_bytes)

        attachments: Dict[str, Tuple[str, bytes]] = {}
        for key, ref in (run.get("s3_urls") or {}).items():
            if key.startswith(ATTACHMENT_PREFIX):
                name = key[len(ATTACHMENT_PREFIX) :]
                if self.skip_attachments:
                    issues.append("attachments_skipped")
                    continue
                try:
                    attachments[name] = self._fetch_blob(
                        ref.get("presigned_url", ""), _MAX_ATTACHMENT_BYTES, host, sess
                    )
                except PermissionError:
                    issues.append("attachment_host_rejected")
                except Exception:
                    issues.append("attachment_fetch_failed")
            elif key in S3_URL_PAYLOAD_FIELDS and run.get(key) is None:
                pull(key, ref, self.max_field_bytes)
        if attachments:
            overrides["attachments"] = attachments
        return overrides, issues

    # ------------------------------------------------------------------
    # Write path
    # ------------------------------------------------------------------
    @classmethod
    def _size_of(cls, payload: Dict[str, Any]) -> int:
        return cls._shape([payload])[2]

    def _oversized_fields(self, payload: Dict[str, Any]) -> List[str]:
        """Fields the destination would silently replace with a placeholder.

        The backend stubs an oversized ``inputs`` / ``outputs`` rather than
        rejecting it, and does not advertise the limit, so this is checked
        against the operator-supplied value before sending.
        """
        return [
            field
            for field in ("inputs", "outputs")
            if payload.get(field) is not None
            and len(json.dumps(payload[field], default=str)) > self.max_field_bytes
        ]

    def ingest(
        self, payloads: List[Dict[str, Any]], frame: Optional[CompiledFrame] = None
    ) -> List[Tuple[str, str]]:
        """Send one batch, isolating a bad run by binary split.

        ``frame`` is a body compiled earlier, off this thread. A split has to
        compile its halves here, since the parent's frame covers the whole batch.

        Returns ``[(run_id, error)]`` for runs that could not be ingested.

        The SDK reports neither a 409 (it breaks out of its retry loop
        silently) nor any other non-2xx it has already logged, so failures are
        read off the recorded response rather than from an exception.
        """
        if not payloads or self.config.migration.dry_run:
            return []
        # Ordered, serial, single-threaded is the whole basis of "a failure
        # leaves no gap". Fail loudly rather than quietly losing that.
        assert threading.get_ident() == self._main_thread, "ingest must run on the main thread"
        n, traces, size = self._shape(payloads)
        # Deliberately louder than the query lines around it: this is the only
        # step that writes, and it is otherwise invisible - multipart_ingest
        # goes through the SDK, which does not emit the client's request logs.
        self.console.print(
            f"[bold cyan]  ==> MULTIPART INGEST -> destination[/bold cyan] "
            f"[cyan]runs {n} | traces {traces} | {self._human(size)}[/cyan]"
        )
        self._sync_dest_headers()
        del self._ingest_errors[:]
        del self._ingest_responses[:]
        try:
            self._send(payloads, frame)
            error = self._ingest_errors[0] if self._ingest_errors else None
        except Exception as exc:  # pragma: no cover - defensive
            error = exc
        self._report_ingest_responses()
        conflict = next((body for status, body in self._ingest_responses if status == 409), None)
        if conflict is not None:
            # A 409 rejects the whole request, so splitting would only repeat
            # it per run. What it does NOT say is where those runs are: the
            # duplicate may be a copy in another project (a real rejection) or
            # this very request's own earlier attempt, which landed. Only the
            # destination can tell the two apart, so when verification is going
            # to ask anyway, defer to it rather than guessing "blocked".
            if self.verify:
                self.console.print(
                    "[yellow]  ==> 409 on ingest; leaving the verdict to verification[/yellow]"
                )
                return []
            return [(str(p["id"]), f"HTTP 409: {conflict}") for p in payloads]
        if error is None:
            swallowed = next(((st, bd) for st, bd in self._ingest_responses if st >= 300), None)
            if swallowed:
                error = RuntimeError(f"HTTP {swallowed[0]}: {swallowed[1]}")
        if error is None:
            return []
        if len(payloads) == 1:
            return [(str(payloads[0]["id"]), str(error))]
        mid = len(payloads) // 2
        self.console.print(
            f"[yellow]  ==> batch of {len(payloads)} rejected; splitting to isolate the bad run[/yellow]"
        )
        return self.ingest(payloads[:mid]) + self.ingest(payloads[mid:])

    def _send(self, payloads: List[Dict[str, Any]], frame: Optional[CompiledFrame] = None) -> None:
        """Post one batch, compressing unless that is unavailable or refused.

        ``frame`` is a body already compiled elsewhere; without one it is
        compiled here. A split re-compiles, which is why this takes both.
        """
        if self.compress_level is None:
            self.dest_ls_client.multipart_ingest(create=payloads)
            return
        if frame is None or set(frame.run_ids) != {str(p["id"]) for p in payloads}:
            frame = compile_frame(self.dest_ls_client, payloads, self.compress_level)
        self.console.print(
            f"[dim]      zstd L{self.compress_level}: {self._human(frame.sizes[0])}"
            f" -> {self._human(frame.sizes[1])}"
            f" ({frame.sizes[0] / max(frame.sizes[1], 1):.1f}x)[/dim]"
        )
        # attempts=1 deliberately. Ingest is not idempotent - a run id is
        # write-once per tenant - so a blind retry of a request whose outcome is
        # unknown (a slow, large batch that the server did accept) earns a 409
        # and makes a landed run look rejected. Verification establishes the
        # truth instead; a genuinely lost batch shows up as missing there.
        self.dest_ls_client._send_compressed_multipart_req(frame.stream, frame.sizes, attempts=1)

    def _report_ingest_responses(self) -> None:
        """Show what the multipart endpoint answered.

        A non-2xx is printed unconditionally - it is the direct explanation for
        runs that never arrive, and the SDK only logs it. A 2xx with a body is
        printed too, because per-run rejections are reported that way.
        """
        for status, body in self._ingest_responses:
            interesting = body and body not in ("{}", "null", '""')
            if status >= 300:
                self.console.print(f"[red]      ingest POST -> HTTP {status}: {body or '<empty body>'}[/red]")
            elif interesting:
                self.console.print(f"[yellow]      ingest POST -> HTTP {status}: {body}[/yellow]")
            else:
                self.log(f"      ingest POST -> HTTP {status} (empty body)", "info")

    def last_ingest_summary(self) -> str:
        """Last multipart status codes, for blocked-run evidence."""
        return ", ".join(f"HTTP {st}{': ' + bd if bd else ''}" for st, bd in self._ingest_responses) or "no response recorded"

    # ------------------------------------------------------------------
    # Sessions
    # ------------------------------------------------------------------
    def list_source_sessions(self) -> List[Dict[str, Any]]:
        """Real tracing sessions only - never experiment/test-run sessions."""
        return [
            s
            for s in self.source.get_paginated("/sessions", params={"reference_free": "true"}, page_size=100)
            if isinstance(s, dict) and not s.get("reference_dataset_id")
        ]

    def get_source_session(self, value: str) -> Optional[Dict[str, Any]]:
        """Fetch one source tracing session by ID. ``None`` when it is not one.

        Only a 404 means "not a session ID". Anything else - a rate limit, a
        gateway blip - is re-raised: swallowing it would make a transient
        failure indistinguishable from a name the operator mistyped, and the
        command would report success having migrated nothing.
        """
        try:
            session = self.source.get(f"/sessions/{value}")
        except NotFoundError:
            return None
        except APIError as exc:
            if "404" in str(exc):
                return None
            raise
        if not isinstance(session, dict) or session.get("reference_dataset_id"):
            return None
        return session

    def find_source_session(self, value: str) -> Tuple[Optional[Dict[str, Any]], str]:
        """Resolve one ``--project`` value, by ID or by exact name.

        Returns ``(session, reason)``. The name lookup uses the endpoint's own
        ``name`` filter rather than enumerating sessions: a busy deployment has
        tens of thousands of them, so a full walk would take minutes to answer
        a question the backend answers in one request.
        """
        # Only try the ID endpoint when the value actually is one: it rejects a
        # non-UUID path segment with a 422, and sniffing that out of an error
        # string to decide "this was a name all along" is the wrong shape.
        try:
            uuid.UUID(str(value))
        except (ValueError, AttributeError, TypeError):
            pass
        else:
            by_id = self.get_source_session(value)
            if by_id:
                return by_id, "id"
        matches = [
            s
            for s in (self.source.get("/sessions", params={"name": value, "reference_free": "true"}) or [])
            if isinstance(s, dict) and not s.get("reference_dataset_id")
        ]
        if len(matches) > 1:
            return None, "ambiguous"
        return (matches[0], "name") if matches else (None, "missing")

    def _dest_sessions_named(self, name: str) -> List[Dict[str, Any]]:
        """Destination tracing projects with exactly this name.

        Uses the endpoint's ``name`` filter rather than enumerating every
        project: the previous full walk cost one request per 100 projects on
        the destination, repeated for the canary and again for each project
        being migrated, which dominated the request count on a busy tenant.
        Returning every match keeps the ambiguity check intact.
        """
        found = self.dest.get("/sessions", params={"name": name, "reference_free": "true"}) or []
        return [
            s for s in found
            if isinstance(s, dict) and s.get("name") == name and not s.get("reference_dataset_id")
        ]

    def resolve_dest_session(self, source_session: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Operator mapping -> destination name match -> create.

        Re-derived every invocation and never read back from state. Nothing
        downstream may assume the two IDs are equal: reusing the source ID on
        create is a convenience, and a rejection is normal progress.
        """
        source_id = str(source_session["id"])
        name = source_session.get("name")
        target_name = f"{name}{self.into_session_suffix}" if self.into_session_suffix else name

        mapped = self.project_id_map.get(source_id)
        if mapped:
            try:
                existing = self.dest.get(f"/sessions/{mapped}")
            except Exception as exc:
                self.log(f"Mapped destination project {mapped} is not reachable: {exc}", "warning")
                existing = None
            if existing:
                return existing

        matches = self._dest_sessions_named(target_name)
        if len(matches) > 1:
            self.log(f"Ambiguous destination session name '{target_name}'; supply --project-mapping", "warning")
            return None
        if matches:
            return matches[0]

        if self.config.migration.dry_run:
            return {"id": f"dry-run-{source_id}", "name": target_name, "trace_tier": LONGLIVED}

        # Explicit tier, never omitted: a nil tier resolves to the destination
        # tenant's default, and the tier cannot be repaired after ingest.
        payload = {
            "name": target_name,
            "description": source_session.get("description"),
            "metadata": source_session.get("metadata"),
            "extra": source_session.get("extra"),
            "start_time": source_session.get("start_time"),
            "end_time": source_session.get("end_time"),
            "trace_tier": source_session.get("trace_tier") if self.emit_upgrade_list else LONGLIVED,
        }
        # Reusing the source ID is convenient, not load-bearing: a rejection
        # just means the session gets mapped instead.
        with_id = payload if self.into_session_suffix else {**payload, "id": source_id}
        try:
            created = self.dest.post("/sessions", with_id)
        except Exception as exc:
            _raise_if_tier_denied(exc)
            created = None
        if not created or not created.get("id"):
            try:
                created = self.dest.post("/sessions", payload)
            except Exception as exc:
                _raise_if_tier_denied(exc)
                raise
            self.log(f"Destination session for '{name}' created with a new ID {created.get('id')}", "info")
        return created

    def ensure_long_lived(self, dest_session: Dict[str, Any]) -> None:
        """Raise an existing session's tier and wait until the raise is observable.

        The ingest path caches session lookups, and a stale short-lived entry
        writes blobs under the short-lived prefix - which cannot be repaired
        afterwards. So we re-read until the new tier comes back before any run
        is ingested into it.
        """
        if self.emit_upgrade_list or self.config.migration.dry_run:
            return
        if dest_session.get("trace_tier") == LONGLIVED:
            return
        session_id = dest_session["id"]
        try:
            self.dest.patch(f"/sessions/{session_id}", {"trace_tier": LONGLIVED})
        except Exception as exc:
            raise TracePreflightError(f"trace_tier_permission_denied: {exc}") from exc
        for attempt in range(_VERIFY_ATTEMPTS):
            current = self.dest.get(f"/sessions/{session_id}") or {}
            if current.get("trace_tier") == LONGLIVED:
                dest_session["trace_tier"] = LONGLIVED
                return
            time.sleep(_VERIFY_BACKOFF * (attempt + 1))
        raise TracePreflightError(f"trace_tier_not_observed for session {session_id}")

    def restore_tier(self, dest_session: Dict[str, Any], tier: Optional[str]) -> None:
        """Put a raised session back to the tier it had before we touched it."""
        if self.config.migration.dry_run or not tier or tier == LONGLIVED:
            return
        try:
            self.dest.patch(f"/sessions/{dest_session['id']}", {"trace_tier": tier})
        except Exception as exc:
            self.log(f"Could not restore trace tier on {dest_session['id']}: {exc}", "warning")

    # ------------------------------------------------------------------
    # Pre-flight
    # ------------------------------------------------------------------
    def canary(self) -> None:
        """Prove the destination still accepts a timestamp at the far end of the range.

        Historical ingest is refused outright by a deployment that enforces the
        ±24h window, and the whole multipart request fails - so this is checked
        once, before anything is migrated, rather than discovered mid-transfer.
        The ID is fresh on every invocation. It used to be derived from the
        scratch project and an hour-rounded stamp, so repeated runs would
        "upsert one canary" - but the destination does not upsert a re-sent
        run, it answers ``409 Run create payload already received``. The write
        was therefore rejected and the read-back found the *previous* run,
        so the check passed while proving nothing about this invocation. A
        unique ID means the read-back can only succeed if our own write landed.

        The cost is one tiny run per invocation in the scratch project, which
        cannot be cleaned up because run deletion is not exposed. That is worth
        paying for a gate that actually gates.
        """
        if self.config.migration.dry_run:
            return
        session = self._scratch_session()
        # The exact range start, not an hour-rounded one: with a unique ID
        # there is no dedup pair to keep stable, so the canary can test the
        # precise oldest instant this run will send.
        stamp = self.resolved_range_start()
        run_id = str(uuid.uuid4())
        payload = {
            "id": run_id,
            "trace_id": run_id,
            "session_id": str(session["id"]),
            "name": "langsmith-migrator historical ingest canary",
            "run_type": "chain",
            "start_time": stamp.isoformat(),
            "end_time": stamp.isoformat(),
            "dotted_order": f"{stamp.strftime('%Y%m%dT%H%M%S%fZ')}{run_id}",
            "status": "success",
            "inputs": {"canary": True},
        }
        failures = self.ingest([payload])
        if failures:
            self._block_historical(f"ingest rejected: {failures[0][1]}")
        for attempt in range(_VERIFY_ATTEMPTS):
            time.sleep(_VERIFY_BACKOFF * (attempt + 1))
            try:
                run = self.dest.get(f"/runs/{run_id}")
            except Exception:
                run = None
            if run and run.get("start_time"):
                observed = str(run["start_time"])[:19]
                if observed != stamp.isoformat()[:19]:
                    self._block_historical(f"start_time rewritten to {observed}")
                return
        self._block_historical("canary run absent after ingest")

    def _block_historical(self, detail: str) -> None:
        issue = self.record_issue(
            "blocked",
            "historical_ingest_rejected",
            f"Destination refuses historical run timestamps "
            f"(from {self.resolved_range_start().isoformat()}): {detail}",
            evidence={"range_start": self.resolved_range_start().isoformat(), "detail": detail},
        )
        del issue
        raise TracePreflightError(f"historical_ingest_rejected: {detail}")

    def _scratch_session(self) -> Dict[str, Any]:
        existing = next(iter(self._dest_sessions_named(_CANARY_SESSION)), None)
        return existing or self.dest.post(
            "/sessions", {"name": _CANARY_SESSION, "description": "langsmith-migrator pre-flight canary"}
        )

    # ------------------------------------------------------------------
    # The unit of work
    # ------------------------------------------------------------------
    def prepare_slice(
        self, source_session: Dict[str, Any], dest_session: Dict[str, Any], window: Window
    ) -> SlicePrepared:
        """Everything up to the write: diff, fetch, adapt, compile.

        Runs off the main thread, so it touches no shared state - findings
        accumulate into the returned value and are merged by ``commit_slice``.
        The exceptions are ``self._page_limit`` and ``self._id_chunk``: both are
        only ever lowered, toward a bound the server dictated, so concurrent
        writes converge on the same value and a lost update just means one more
        rejection before it sticks.
        """
        src_id, dst_id = str(source_session["id"]), str(dest_session["id"])
        population = self.slice_runs(self.source, src_id, window)
        if not population:
            return SlicePrepared(window, src_id, dst_id, plan_slice(set(), set()), [])
        source_ids = {str(r["id"]) for r in population}

        # With --emit-upgrade-list the destination sessions stay at their
        # existing tier, so filtering on tier there would read every migrated
        # run as missing.
        dest_ids = (
            set()
            if self.config.migration.dry_run
            else self.long_lived_run_ids(self.dest, dst_id, window, tier_filter=not self.emit_upgrade_list)
        )
        prepared = SlicePrepared(window, src_id, dst_id, plan_slice(source_ids, dest_ids), population)

        if self.emit_upgrade_list:
            # Derived from the window's source population, not the diff: built
            # from the diff, a re-run whose differences are all empty would
            # emit an empty list.
            prepared.upgrade_rows.extend(
                (dst_id, str(r["trace_id"]), str(r["start_time"]))
                for r in population
                if str(r.get("trace_id")) == str(r.get("id"))
            )
            # Reported against the whole population for the same reason, and
            # kept out of the reconciliation buckets so the parts still
            # partition the source total exactly once.
            prepared.issues.append(
                (
                    "degraded",
                    "longlived_pending_operator_upgrade",
                    f"{len(source_ids)} run(s) in {window.label()} are not yet long-lived: "
                    "the destination project was left at its existing tier",
                    {"window": window.label(), "count": len(source_ids), "dest_session_id": dst_id},
                )
            )

        self._build_batches(prepared)
        return prepared

    def _build_batches(self, prepared: SlicePrepared) -> None:
        """Fetch and adapt the diff into ready-to-send batches."""
        if not prepared.plan.to_ingest:
            return
        payloads: List[Dict[str, Any]] = []
        for run in self.fetch_runs(
            self.source, prepared.src_id, prepared.window, sorted(prepared.plan.to_ingest)
        ):
            run_id = str(run["id"])
            overrides, issues = self.materialise(run)
            payload_obj, dropped = to_ingest_payload(run, prepared.dst_id, overrides)
            payload = payload_obj.as_dict()

            oversized = self._oversized_fields(payload)
            if oversized:
                # The backend stubs an oversized field rather than rejecting
                # it, so this run must not be reported as fully migrated.
                prepared.degraded.setdefault("payload_oversized_for_destination", set()).add(run_id)
                prepared.fidelity_notes.add("payloads exceeded --max-field-bytes")
                continue

            codes = set(issues)
            if dropped:
                codes.add("run_example_link_dropped")
            for code in codes:
                prepared.degraded.setdefault(code, set()).add(run_id)
                if code in ("attachment_fetch_failed", "attachment_host_rejected"):
                    prepared.fidelity_notes.add("some source blobs could not be fetched")

            # Before compiling: _run_transform mutates the payload in place.
            if len(prepared.digests) < self.verify_content_sample:
                prepared.digests[run_id] = self._digest_of(run, overrides)
            payloads.append(payload)

        max_runs, max_bytes = self.batch_limits()
        for batch in batch_traces(
            group_into_traces(payloads), max_runs=max_runs, max_bytes=max_bytes, size_of=self._size_of
        ):
            frame = (
                None
                if self.compress_level is None or self.config.migration.dry_run
                else compile_frame(self.dest_ls_client, batch, self.compress_level)
            )
            prepared.batches.append((batch, frame))

    def commit_slice(self, prepared: SlicePrepared) -> Reconciliation:
        """Write one prepared slice, confirm it, and account for it.

        Main thread only, and called in window order, so a failure leaves every
        earlier window complete rather than a gap.
        """
        src_id, dst_id, window, plan = prepared.src_id, prepared.dst_id, prepared.window, prepared.plan
        if not prepared.population:
            return Reconciliation(src_id, dst_id, window.label(), 0, 0, 0, 0, 0)

        self.upgrade_rows.extend(prepared.upgrade_rows)
        self.fidelity_reduced |= prepared.fidelity_notes
        for issue_class, code, summary, evidence in prepared.issues:
            self.record_issue(issue_class, code, summary, evidence=evidence)

        degraded = prepared.degraded
        blocked: Set[str] = set()
        rejected: Dict[str, Set[str]] = {}
        for batch, frame in prepared.batches:
            for run_id, error in self.ingest(batch, frame=frame):
                blocked.add(run_id)
                rejected.setdefault(str(error), set()).add(run_id)
        # Grouped by cause: twenty runs refused for one reason is one fact, not
        # twenty truncated lines.
        for error, ids in rejected.items():
            self._record_blocked(
                window, ids, "run_ingest_rejected",
                f"{len(ids)} run(s) refused by the destination on ingest: {error}",
            )

        if self.verify and plan.to_ingest and not self.config.migration.dry_run:
            still_missing = self._confirm(dst_id, window, plan.to_ingest - blocked)
            if still_missing:
                blocked |= still_missing
                self._block_runs(window, still_missing, dst_id)
            if prepared.digests:
                self._check_content(dst_id, window, prepared.digests, degraded)

        self._report_degraded(window, degraded)

        # A run can be both degraded and blocked; blocked wins, so the parts
        # stay a partition of the source total.
        degraded_ids = set().union(*degraded.values()) if degraded else set()
        degraded_only = degraded_ids - blocked
        # Only a wholly clean slice yields a watermark: a slice that degraded
        # or blocked anything cannot claim "everything up to here is complete".
        # A watermark is a claim that everything up to it is confirmed on the
        # destination, so it may only be made when a confirming query actually
        # ran: --no-verify skips it, and a dry run wrote nothing to confirm.
        complete = (plan.to_ingest | plan.already_present) - blocked - degraded_ids
        clean = not (blocked or degraded_ids) and self.verify and not self.config.migration.dry_run
        earliest, latest = verified_bounds(prepared.population, complete) if clean else (None, None)
        return Reconciliation(
            session_id=src_id,
            dest_session_id=dst_id,
            window=window.label(),
            source_total=len(prepared.population),
            ingested=len(plan.to_ingest) - len(degraded_only) - len(blocked),
            already_present=len(plan.already_present),
            degraded=len(degraded_only),
            blocked=len(blocked),
            extra_on_dest=len(plan.extra_on_dest),
            earliest=earliest,
            latest=latest,
            verified_runs=len(complete) if clean else 0,
        )

    def migrate_slice(
        self, source_session: Dict[str, Any], dest_session: Dict[str, Any], window: Window
    ) -> Reconciliation:
        """Prepare and commit one slice, without look-ahead."""
        return self.commit_slice(self.prepare_slice(source_session, dest_session, window))

    @staticmethod
    def _digest_of(run: Dict[str, Any], overrides: Dict[str, Any]) -> Dict[str, str]:
        merged = {**run, **overrides}
        sizes = {name: len(data) for name, (_, data) in (overrides.get("attachments") or {}).items()}
        return payload_digest(merged, sizes)

    def _confirm(self, dest_session_id: str, window: Window, expected: Set[str]) -> Set[str]:
        """Re-query with backoff until the difference is empty or attempts run out."""
        missing = set(expected)
        for attempt in range(_VERIFY_ATTEMPTS):
            time.sleep(_VERIFY_BACKOFF * (attempt + 1))
            missing -= self.long_lived_run_ids(
                self.dest, dest_session_id, window, tier_filter=not self.emit_upgrade_list
            )
            if not missing:
                return set()
        return missing

    def _check_content(
        self, dest_session_id: str, window: Window, digests: Dict[str, Dict[str, str]], degraded: Dict[str, Set[str]]
    ) -> None:
        """Compare sampled per-field digests. Reports field names, never values."""
        mismatched: Dict[str, List[str]] = {}
        for run in self.fetch_runs(self.dest, dest_session_id, window, sorted(digests)):
            run_id = str(run["id"])
            if run_id not in digests:
                continue
            overrides, issues = self.materialise(run, side="dest")
            if issues:
                # Otherwise a read-back failure is indistinguishable from the
                # destination genuinely holding different content.
                self.log(f"Could not read back run {run_id} for comparison: {sorted(set(issues))}", "warning")
                continue
            fields = digest_mismatches(digests[run_id], self._digest_of(run, overrides))
            if fields:
                mismatched[run_id] = list(fields)
                degraded.setdefault("run_fidelity_mismatch", set()).add(run_id)
        if mismatched:
            self.record_issue(
                "degraded",
                "run_fidelity_mismatch",
                f"{len(mismatched)} sampled run(s) differ from the source ({window.label()})",
                # Field names and counts only - never values.
                evidence={"window": window.label(), "runs": dict(list(mismatched.items())[:20])},
            )

    def _report_degraded(self, window: Window, degraded: Dict[str, Set[str]]) -> None:
        """One issue per (window, code), not per run.

        ``record_issue`` rewrites the whole state file, so per-run issues would
        make a large window quadratic in I/O for no extra information.
        """
        for code, run_ids in sorted(degraded.items()):
            if code == "run_fidelity_mismatch":
                continue  # already reported with its field names
            self.record_issue(
                "degraded",
                code,
                f"{len(run_ids)} run(s) migrated with reduced fidelity ({code}) in {window.label()}",
                evidence={"window": window.label(), "count": len(run_ids), "run_ids": sorted(run_ids)[:20]},
            )

    def _record_blocked(self, window: Window, run_ids: Set[str], code: str, summary: str) -> None:
        """One record per (window, cause), whichever path blocked the runs.

        Both the ingest rejection and the confirm miss land here, so a blocked
        count always has a matching reason in the output and in state - rather
        than a reason only for one of the two ways runs get blocked.
        """
        self.blocked_reasons.append(f"{window.label()}: {summary}")
        self.record_issue(
            "blocked",
            code,
            f"{summary} ({window.label()})",
            evidence={"window": window.label(), "count": len(run_ids), "run_ids": sorted(run_ids)[:20]},
        )

    def _diagnose_missing(self, dest_session_id: str, run_ids: Set[str]) -> Tuple[str, str]:
        """Explain why runs are absent, by asking where they actually are.

        The common cause is not a lost write: the destination keeps one copy of
        a run id per tenant, so a run that already landed in another project
        cannot be ingested into a second one - the write is accepted and then
        dropped. Saying "still missing" for that is true but useless.
        """
        for run_id in sorted(run_ids)[:3]:
            try:
                run = self.dest.get(f"/runs/{run_id}")
            except Exception:
                continue
            other = str((run or {}).get("session_id") or "")
            if other and other != dest_session_id:
                return (
                    "run_exists_in_other_project",
                    f"{len(run_ids)} run(s) already exist on this destination under a different "
                    f"project ({other}); the destination keeps one copy per run id per tenant, so "
                    f"they were not written into {dest_session_id}",
                )
        return (
            "run_not_ingested",
            f"{len(run_ids)} run(s) still missing from the destination after ingest "
            f"(last ingest response: {self.last_ingest_summary()})",
        )

    def _block_runs(self, window: Window, run_ids: Set[str], dest_session_id: str) -> None:
        code, summary = self._diagnose_missing(dest_session_id, run_ids)
        self._record_blocked(window, run_ids, code, summary)

    # ------------------------------------------------------------------
    # Per-session driver
    # ------------------------------------------------------------------
    def migrate_session(self, source_session: Dict[str, Any], now: Optional[datetime] = None) -> Optional[Reconciliation]:
        """Raise tier -> migrate windows oldest-first -> verify -> restore.

        The tier is settled in a ``finally``: once an existing project has been
        raised, walking away without deciding what to do about it would leave a
        project long-lived indefinitely on any mid-run failure, silently
        changing retention for traffic that has nothing to do with this
        migration.
        """
        dest_session = self.resolve_dest_session(source_session)
        if not dest_session:
            return None
        prior_tier = dest_session.get("trace_tier")
        self.ensure_long_lived(dest_session)

        recon = None
        try:
            slices = []
            end = now or datetime.now(timezone.utc)
            windows = iter_windows(self.resolved_range_start(), end, self.window_days)
            for sliced in self._walk_windows(source_session, dest_session, windows):
                slices.append(sliced)
                if sliced.source_total:
                    # Per-slice detail is verbose-only: the range can be 180
                    # windows wide, and the per-session total is always printed.
                    self.log(
                        f"  {sliced.window}: source {sliced.source_total} = ingested {sliced.ingested}"
                        f" + already present {sliced.already_present}"
                        f" + degraded {sliced.degraded} + blocked {sliced.blocked}"
                        + (f" | verified {sliced.earliest} .. {sliced.latest}" if sliced.earliest else ""),
                        "info",
                    )
            recon = Reconciliation.of(str(source_session["id"]), str(dest_session["id"]), slices)
            return recon
        finally:
            self._settle_tier(dest_session, prior_tier, source_session.get("trace_tier"), recon)

    def _walk_windows(
        self, source_session: Dict[str, Any], dest_session: Dict[str, Any], windows
    ) -> Iterator[Reconciliation]:
        """Prepare up to ``prefetch_windows`` slices at once; commit in order.

        Retrieval is the slow half and parallelises cleanly. Ingest does not:
        committing out of order would let a later window land while an earlier
        one is missing, so a failure would leave a hole rather than a clean
        prefix. Windows are therefore committed strictly oldest-first, and the
        first failure stops the walk with every earlier window complete - which
        is what makes the watermark a claim worth printing.

        A failure still waits for the preparations already in flight, since a
        thread mid-request cannot be interrupted; nothing further is committed.
        """
        if self.prefetch_windows <= 1:
            for window in windows:
                yield self.migrate_slice(source_session, dest_session, window)
            return

        pending: Deque[Future] = deque()
        with ThreadPoolExecutor(
            max_workers=self.prefetch_windows, thread_name_prefix="trace-prepare"
        ) as pool:
            def submit_next() -> None:
                # Main thread only: ``windows`` is a generator, and advancing
                # one from several threads is not safe.
                window = next(windows, None)
                if window is not None:
                    pending.append(pool.submit(self.prepare_slice, source_session, dest_session, window))

            for _ in range(self.prefetch_windows):
                submit_next()
            while pending:
                prepared = pending.popleft().result()  # in-order: raises here on failure
                # Refilled before committing, not after: the commit is the slow
                # serial POST, and that is exactly when readers should be busy.
                # Peak residency is therefore prefetch_windows in flight plus
                # the one being written.
                submit_next()
                yield self.commit_slice(prepared)

    def _settle_tier(
        self,
        dest_session: Dict[str, Any],
        prior_tier: Optional[str],
        source_tier: Optional[str],
        recon: Optional[Reconciliation],
    ) -> None:
        """Restore a raised destination tier, or say why it was left raised.

        ``ensure_long_lived`` only mutates the session dict on a real raise, so
        a changed tier is exactly "we raised this one".
        """
        if dest_session.get("trace_tier") == prior_tier:
            return
        restore = self.restore_session_tier
        if restore is None:
            restore = source_tier != LONGLIVED
        if not restore:
            return
        if recon is not None and recon.complete:
            self.restore_tier(dest_session, prior_tier)
            return
        # Left raised on purpose - a re-run must still ingest long-lived - but
        # recorded, because an operator otherwise has no way to know this
        # project's retention was changed and not put back.
        reason = "the migration did not finish" if recon is None else "the project has blocked runs"
        self.record_issue(
            "degraded",
            "dest_tier_left_raised",
            f"Destination project {dest_session['id']} was raised to {LONGLIVED} and left raised "
            f"because {reason}; its previous tier was '{prior_tier}'",
            evidence={"dest_session_id": str(dest_session["id"]), "prior_tier": prior_tier, "reason": reason},
        )
