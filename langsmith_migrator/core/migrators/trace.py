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

* Run identity on the destination is ``(tenant_id, session_id, start_time,
  id)``, so preserving IDs and timestamps is what makes replay an upsert - and
  why migrating into the *same* tenant is refused outright.
* There is no per-run trace-tier field on the ingest contract. The destination
  *session's* tier is the only lever, and it must be correct at ingest time
  because the row TTL and the blob key prefix are both baked in at insert.
"""

from __future__ import annotations

import time
import uuid
from datetime import datetime, timedelta, timezone
from tempfile import SpooledTemporaryFile
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set, Tuple
from urllib.parse import urlparse

import requests
from langsmith import Client

from ..api_client import APIError, NotFoundError
from ..trace_domain import (
    ATTACHMENT_PREFIX,
    LONGLIVED,
    RUN_QUERY_SELECT,
    S3_URL_PAYLOAD_FIELDS,
    SessionReconciliation,
    SlicePlan,
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
from .base import BaseMigrator

# Measured on a real deployment: 2000 IDs per /runs/query is fine, 5000 returns
# 503. Half the safe value, so a slow shard has headroom.
_ID_CHUNK = 500

# Not advertised by /info (only the batch limits are), so it matches the
# backend's own MAX_ATTACHMENT_SIZE_BYTES default and is a guard, not a truth.
_MAX_ATTACHMENT_BYTES = 200 * 1024 * 1024

_VERIFY_ATTEMPTS = 4
_VERIFY_BACKOFF = 3.0

_DEGRADED_ACTIONS = {
    "attachment_fetch_failed": "Once the source blob store is reachable, re-migrate this window into a fresh destination project.",
    "attachment_host_rejected": "The source returned a blob URL off its own host; investigate, then re-migrate into a fresh destination project.",
    "attachments_skipped": "Re-migrate this window into a fresh destination project without --skip-attachments.",
    "payload_oversized_for_destination": "Raise MAX_FIELD_SIZE_BYTES on the destination, set a matching --max-field-bytes, and re-migrate into a fresh destination project.",
    "run_example_link_dropped": "The run pointed at a source dataset example; re-point it after migrating datasets if needed.",
    "longlived_pending_operator_upgrade": "Feed the emitted upgrade list to POST /internal/runs/upgrade-trace-tier; long-lived retention is not yet in effect.",
}

_CANARY_SESSION = "langsmith-migrator-canary"


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


def _is_select_rejection(error: Exception) -> bool:
    """True when the source refused a name in ``select`` rather than the query."""
    text = str(error)
    return "select" in text and ("422" in text or "Input should be" in text)
_CANARY_NS = uuid.UUID("6f9619ff-8b86-d011-b42d-00c04fc964ff")


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
        max_age_days: float = 180.0,
        window_days: float = 1.0,
        max_field_bytes: int = 25 * 1024 * 1024,
        verify: bool = True,
        verify_content_sample: int = 100,
        skip_attachments: bool = False,
        restore_session_tier: Optional[bool] = None,
        into_session_suffix: Optional[str] = None,
        emit_upgrade_list: Optional[str] = None,
        project_id_map: Optional[Dict[str, str]] = None,
    ):
        super().__init__(source_client, dest_client, state, config)
        self.max_age_days = max_age_days
        self.window_days = window_days
        self.max_field_bytes = max_field_bytes
        self.verify = verify
        self.verify_content_sample = verify_content_sample
        self.skip_attachments = skip_attachments
        self.restore_session_tier = restore_session_tier
        self.into_session_suffix = into_session_suffix
        self.emit_upgrade_list = emit_upgrade_list
        self.project_id_map = dict(project_id_map or {})

        # Anything that reduces fidelity names its repair. A run that landed
        # incomplete is invisible to the ordinary ID diff - its ID is present -
        # and cannot be fixed by re-sending, because a fully ingested run is
        # immutable. The only repair is a different destination session.
        self.fidelity_reduced: Set[str] = set()
        if skip_attachments:
            self.fidelity_reduced.add("without --skip-attachments")
        self.upgrade_rows: List[Tuple[str, str, str]] = []

        self._dest_sessions: Dict[str, Dict[str, Any]] = {}
        self._select_unsupported = False
        self._source_blob_host = urlparse(self._host(config.source.base_url)).netloc

        self._dest_session_http = requests.Session()
        if not config.destination.verify_ssl:
            self._dest_session_http.verify = False
        self._ingest_errors: List[Exception] = []
        # The SDK logs multipart failures and returns normally, so an exception
        # never reaches us. The callback is the only way to notice.
        self.dest_ls_client = Client(
            api_key=config.destination.api_key,
            api_url=self._host(config.destination.base_url),
            session=self._dest_session_http,
            omit_traced_runtime_info=True,
            tracing_error_callback=self._ingest_errors.append,
        )

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
        import json as _json

        traces = {str(r.get("trace_id")) for r in runs}
        size = sum(
            len(_json.dumps({k: v for k, v in r.items() if k != "attachments"}, default=str))
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
            "limit": 100,
        }
        if ids is not None:
            body["id"] = [str(i) for i in ids]
        if self._select_unsupported:
            body.pop("select")
        side = "source" if client is self.source else "dest  "
        page_num, seen = 0, 0
        while True:
            try:
                response = client.post("/runs/query", body) or {}
            except Exception as exc:
                # One fallback, no cascade: a source that rejects a name in
                # `select` gets the query again with no projection at all. The
                # endpoint then returns every column, which is a superset of
                # what we asked for, so nothing is lost but the narrowing.
                if self._select_unsupported or "select" not in body or not _is_select_rejection(exc):
                    raise
                self._select_unsupported = True
                self.record_issue(
                    "degraded",
                    "run_query_select_unsupported",
                    "The source rejected the requested `select`; querying without a projection instead",
                    evidence={"select": list(select), "error": str(exc)[:300]},
                )
                body.pop("select")
                continue
            runs = response.get("runs") or []
            page_num += 1
            seen += len(runs)
            if self.config.migration.verbose and runs:
                n, traces, size = self._shape(runs)
                # Kept under 80 columns: a wrapped progress line is worse
                # than a terse one.
                self.console.print(
                    f"[dim]  query {side} p{page_num:<4}"
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

    def fetch_runs(self, session_id: str, window: Window, run_ids: Sequence[str]) -> Iterator[Dict[str, Any]]:
        """Full payloads for a set of IDs, chunked so no request is oversized."""
        ids = list(run_ids)
        for start in range(0, len(ids), _ID_CHUNK):
            yield from self._query_runs(
                self.source, session_id, window, select=RUN_QUERY_SELECT, ids=ids[start : start + _ID_CHUNK]
            )

    # ------------------------------------------------------------------
    # Blobs
    # ------------------------------------------------------------------
    def _fetch_blob(self, url: str, limit: int) -> Tuple[str, bytes]:
        """Download one presigned blob through the tool's configured session.

        Never the SDK's own conversion, which uses a bare ``requests.get`` that
        ignores SSL config, CA bundles and proxies, and substitutes a
        placeholder on failure. Redirects are not followed: the deployment
        proxies blob downloads on its own host, so a redirect would be the
        source steering us somewhere else.
        """
        parsed = urlparse(url)
        if parsed.scheme != "https" or parsed.netloc != self._source_blob_host:
            raise PermissionError("attachment_host_rejected")
        with self.source.session.get(url, stream=True, timeout=120, allow_redirects=False) as resp:
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

    def materialise(self, run: Dict[str, Any]) -> Tuple[Dict[str, Any], List[str]]:
        """Re-inline anything the source left in blob storage, and fetch attachments.

        The query API usually resolves offloaded ``inputs`` / ``outputs`` /
        ``extra`` for us; the blob reference is only followed when the inline
        value is genuinely absent. Attachments are never inlined and always
        need fetching. Returns ``(overrides, degraded_codes)`` - a failure is
        explicit, never a placeholder.
        """
        overrides: Dict[str, Any] = {}
        issues: List[str] = []
        import json as _json

        def pull(field: str, ref: Dict[str, Any], limit: int) -> None:
            url = (ref or {}).get("presigned_url")
            if not url:
                return
            try:
                _, raw = self._fetch_blob(url, limit)
                overrides[field] = _json.loads(raw)
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
                    attachments[name] = self._fetch_blob(ref.get("presigned_url", ""), _MAX_ATTACHMENT_BYTES)
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
    @staticmethod
    def _size_of(payload: Dict[str, Any]) -> int:
        import json as _json

        attachments = payload.get("attachments") or {}
        blob_bytes = sum(len(data) for _, data in attachments.values())
        body = {k: v for k, v in payload.items() if k != "attachments"}
        return len(_json.dumps(body, default=str)) + blob_bytes

    def _oversized_fields(self, payload: Dict[str, Any]) -> List[str]:
        """Fields the destination would silently replace with a placeholder.

        The backend stubs an oversized ``inputs`` / ``outputs`` rather than
        rejecting it, and does not advertise the limit, so this is checked
        against the operator-supplied value before sending.
        """
        import json as _json

        return [
            field
            for field in ("inputs", "outputs")
            if payload.get(field) is not None
            and len(_json.dumps(payload[field], default=str)) > self.max_field_bytes
        ]

    def ingest(self, payloads: List[Dict[str, Any]]) -> List[Tuple[str, str]]:
        """Send one batch, isolating a bad run by binary split.

        Returns ``[(run_id, error)]`` for runs that could not be ingested. A
        single-run conflict is replay success: the SDK breaks out of its retry
        loop without reporting it, so it never reaches us as an error.
        """
        if not payloads or self.config.migration.dry_run:
            return []
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
        try:
            self.dest_ls_client.multipart_ingest(create=payloads)
            error = self._ingest_errors[0] if self._ingest_errors else None
        except Exception as exc:  # pragma: no cover - defensive
            error = exc
        if error is None:
            return []
        if len(payloads) == 1:
            return [(str(payloads[0]["id"]), str(error))]
        mid = len(payloads) // 2
        self.console.print(
            f"[yellow]  ==> batch of {len(payloads)} rejected; splitting to isolate the bad run[/yellow]"
        )
        return self.ingest(payloads[:mid]) + self.ingest(payloads[mid:])

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

    def _dest_sessions_by_name(self) -> Dict[str, List[Dict[str, Any]]]:
        index: Dict[str, List[Dict[str, Any]]] = {}
        for s in self.dest.get_paginated("/sessions", params={"reference_free": "true"}, page_size=100):
            if isinstance(s, dict) and not s.get("reference_dataset_id"):
                index.setdefault(s.get("name"), []).append(s)
        return index

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

        by_name = self._dest_sessions_by_name()
        matches = by_name.get(target_name) or []
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

    def restore_tier(self, dest_session: Dict[str, Any], source_tier: Optional[str]) -> None:
        """Put a raised session back, so only the session being migrated is exposed."""
        if self.config.migration.dry_run or not source_tier or source_tier == LONGLIVED:
            return
        try:
            self.dest.patch(f"/sessions/{dest_session['id']}", {"trace_tier": source_tier})
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
        The run ID is deterministic and the session is scratch, so repeated
        stateless invocations upsert one canary instead of accumulating them.
        """
        if self.config.migration.dry_run:
            return
        session = self._scratch_session()
        # Quantised to the hour, and the ID derived from it: ``start_time`` is
        # part of the destination's dedup key, so a canary re-sent under the
        # same ID with a drifted timestamp lands as a *second* row and the
        # read-back then compares against the wrong one. Same hour, same pair,
        # real upsert. Run deletion is not exposed by the API, so the scratch
        # project keeps at most one canary per hour the tool is used.
        stamp = (datetime.now(timezone.utc) - timedelta(days=self.max_age_days)).replace(
            minute=0, second=0, microsecond=0
        )
        run_id = str(uuid.uuid5(_CANARY_NS, f"canary:{session['id']}:{stamp.isoformat()}"))
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
        next_action = (
            "Raise the destination's ingest time window "
            "(V1_INGEST_ENFORCE_TIME_WINDOW_EXCLUDED_ORGS) and re-run `langsmith-migrator traces`."
        )
        issue = self.record_issue(
            "blocked",
            "historical_ingest_rejected",
            f"Destination refuses historical run timestamps ({self.max_age_days:g}d): {detail}",
            next_action=next_action,
            evidence={"max_age_days": self.max_age_days, "detail": detail},
        )
        if issue:
            self.queue_remediation(issue_id=issue.id, next_action=next_action, command="langsmith-migrator traces")
        raise TracePreflightError(f"historical_ingest_rejected: {detail}")

    def _scratch_session(self) -> Dict[str, Any]:
        for existing in self._dest_sessions_by_name().get(_CANARY_SESSION, []):
            return existing
        return self.dest.post("/sessions", {"name": _CANARY_SESSION, "description": "langsmith-migrator pre-flight canary"})

    # ------------------------------------------------------------------
    # The unit of work
    # ------------------------------------------------------------------
    def migrate_slice(
        self, source_session: Dict[str, Any], dest_session: Dict[str, Any], window: Window
    ) -> SliceReconciliation:
        """Diff, ingest the difference, confirm it is empty, sample content."""
        src_id, dst_id = str(source_session["id"]), str(dest_session["id"])
        population = self.slice_runs(self.source, src_id, window)
        if not population:
            return SliceReconciliation(src_id, dst_id, window.label(), 0, 0, 0, 0, 0)
        source_ids = {str(r["id"]) for r in population}

        # With --emit-upgrade-list the destination sessions stay at their
        # existing tier, so filtering on tier there would read every migrated
        # run as missing.
        dest_ids = (
            set()
            if self.config.migration.dry_run
            else self.long_lived_run_ids(self.dest, dst_id, window, tier_filter=not self.emit_upgrade_list)
        )
        plan = plan_slice(source_ids, dest_ids)

        if self.emit_upgrade_list:
            # Derived from the window's source population, not the diff: built
            # from the diff, a re-run whose differences are all empty would
            # emit an empty list.
            self.upgrade_rows.extend(
                (dst_id, str(r["trace_id"]), str(r["start_time"]))
                for r in population
                if str(r.get("trace_id")) == str(r.get("id"))
            )
            # Reported against the whole population for the same reason, and
            # kept out of the reconciliation buckets so the parts still
            # partition the source total exactly once.
            self.record_issue(
                "degraded",
                "longlived_pending_operator_upgrade",
                f"{len(source_ids)} run(s) in {window.label()} are not yet long-lived: "
                "the destination project was left at its existing tier",
                next_action=_DEGRADED_ACTIONS["longlived_pending_operator_upgrade"],
                evidence={"window": window.label(), "count": len(source_ids), "dest_session_id": dst_id},
            )

        degraded: Dict[str, Set[str]] = {}
        blocked: Set[str] = set()
        digests = self._ingest_plan(src_id, dst_id, window, plan, degraded, blocked)

        if self.verify and plan.to_ingest and not self.config.migration.dry_run:
            still_missing = self._confirm(dst_id, window, plan.to_ingest - blocked)
            if still_missing:
                blocked |= still_missing
                self._block_runs(window, still_missing)
            if digests:
                self._check_content(dst_id, window, digests, degraded)

        self._report_degraded(window, degraded)

        # A run can be both degraded and blocked; blocked wins, so the parts
        # stay a partition of the source total.
        degraded_ids = set().union(*degraded.values()) if degraded else set()
        degraded_only = degraded_ids - blocked
        return SliceReconciliation(
            session_id=src_id,
            dest_session_id=dst_id,
            window=window.label(),
            source_total=len(plan.source_ids),
            ingested=len(plan.to_ingest) - len(degraded_only) - len(blocked),
            already_present=len(plan.already_present),
            degraded=len(degraded_only),
            blocked=len(blocked),
            extra_on_dest=len(plan.extra_on_dest),
        )

    def _ingest_plan(
        self,
        src_id: str,
        dst_id: str,
        window: Window,
        plan: SlicePlan,
        degraded: Dict[str, Set[str]],
        blocked: Set[str],
    ) -> Dict[str, Dict[str, str]]:
        """Fetch, adapt and send the diff. Returns digests for the sampled runs."""
        digests: Dict[str, Dict[str, str]] = {}
        if not plan.to_ingest:
            return digests

        prepared: List[Dict[str, Any]] = []
        for run in self.fetch_runs(src_id, window, sorted(plan.to_ingest)):
            run_id = str(run["id"])
            overrides, issues = self.materialise(run)
            payload_obj, dropped = to_ingest_payload(run, dst_id, overrides)
            payload = payload_obj.as_dict()

            oversized = self._oversized_fields(payload)
            if oversized:
                # The backend stubs an oversized field rather than rejecting
                # it, so this run must not be reported as fully migrated.
                degraded.setdefault("payload_oversized_for_destination", set()).add(run_id)
                self.fidelity_reduced.add("after raising --max-field-bytes")
                continue

            codes = set(issues)
            if dropped:
                codes.add("run_example_link_dropped")
            for code in codes:
                degraded.setdefault(code, set()).add(run_id)
                if code in ("attachment_fetch_failed", "attachment_host_rejected"):
                    self.fidelity_reduced.add("once the source blob store is reachable")

            if len(digests) < self.verify_content_sample:
                digests[run_id] = self._digest_of(run, overrides)
            prepared.append(payload)

        max_runs, max_bytes = self.batch_limits()
        for batch in batch_traces(
            group_into_traces(prepared), max_runs=max_runs, max_bytes=max_bytes, size_of=self._size_of
        ):
            for run_id, error in self.ingest(batch):
                blocked.add(run_id)
                self.log(f"Run {run_id} rejected: {str(error)[:200]}", "error")
        return digests

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
        for run in self.fetch_runs_dest(dest_session_id, window, sorted(digests)):
            run_id = str(run["id"])
            if run_id not in digests:
                continue
            overrides, _ = self.materialise(run)
            fields = digest_mismatches(digests[run_id], self._digest_of(run, overrides))
            if fields:
                mismatched[run_id] = list(fields)
                degraded.setdefault("run_fidelity_mismatch", set()).add(run_id)
        if mismatched:
            self.record_issue(
                "degraded",
                "run_fidelity_mismatch",
                f"{len(mismatched)} sampled run(s) differ from the source ({window.label()})",
                next_action="Re-migrate this window into a fresh destination project.",
                # Field names and counts only - never values.
                evidence={"window": window.label(), "runs": dict(list(mismatched.items())[:20])},
            )

    def fetch_runs_dest(self, session_id: str, window: Window, run_ids: Sequence[str]) -> Iterator[Dict[str, Any]]:
        ids = list(run_ids)
        for start in range(0, len(ids), _ID_CHUNK):
            yield from self._query_runs(
                self.dest, session_id, window, select=RUN_QUERY_SELECT, ids=ids[start : start + _ID_CHUNK]
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
                next_action=_DEGRADED_ACTIONS.get(code),
                evidence={"window": window.label(), "count": len(run_ids), "run_ids": sorted(run_ids)[:20]},
            )

    def _block_runs(self, window: Window, run_ids: Set[str]) -> None:
        next_action = "Re-run `langsmith-migrator traces` for this window; the diff will re-send them."
        issue = self.record_issue(
            "blocked",
            "run_not_ingested",
            f"{len(run_ids)} run(s) still missing from the destination after ingest ({window.label()})",
            next_action=next_action,
            evidence={"window": window.label(), "run_ids": sorted(run_ids)[:20], "count": len(run_ids)},
        )
        if issue:
            self.queue_remediation(issue_id=issue.id, next_action=next_action, command="langsmith-migrator traces")

    # ------------------------------------------------------------------
    # Per-session driver
    # ------------------------------------------------------------------
    def migrate_session(self, source_session: Dict[str, Any], now: Optional[datetime] = None) -> Optional[SessionReconciliation]:
        """Raise tier -> migrate windows oldest-first -> verify -> restore."""
        dest_session = self.resolve_dest_session(source_session)
        if not dest_session:
            return None
        self.ensure_long_lived(dest_session)

        slices = []
        for window in iter_windows(now or datetime.now(timezone.utc), self.max_age_days, self.window_days):
            recon = self.migrate_slice(source_session, dest_session, window)
            slices.append(recon)
            if recon.source_total:
                # Per-slice detail is verbose-only: the range can be 180
                # windows wide, and the per-session total is always printed.
                self.log(
                    f"  {recon.window}: source {recon.source_total} = ingested {recon.ingested}"
                    f" + already present {recon.already_present}"
                    f" + degraded {recon.degraded} + blocked {recon.blocked}",
                    "info",
                )
        recon = SessionReconciliation.of(str(source_session["id"]), str(dest_session["id"]), slices)

        source_tier = source_session.get("trace_tier")
        restore = self.restore_session_tier
        if restore is None:
            restore = source_tier != LONGLIVED
        # Never restore an incomplete session: a re-run must still ingest long-lived.
        if restore and recon.complete:
            self.restore_tier(dest_session, source_tier)
        return recon
