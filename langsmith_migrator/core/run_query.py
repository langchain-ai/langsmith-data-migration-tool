"""Query runs through the current (v2) endpoint, falling back to v1 on older servers.

`POST /api/v1/runs/query` is deprecated in favour of `POST /api/v2/runs/query`.
Callers keep building the v1 request body and reading v1 field names; this
module translates both ways, so a migrator does not care which one answered.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from .api_client import EnhancedAPIClient, NotFoundError

V2_RUNS_QUERY = "/api/v2/runs/query"
V1_RUNS_QUERY = "/runs/query"

# The largest page the v2 endpoint accepts.
V2_MAX_PAGE_SIZE = 1000

# How far back to look when a project has no usable start time: the longest
# retention a run can have.
_MAX_LOOKBACK = timedelta(days=400)

# A project's runs can start slightly before the project row was written.
_START_SKEW = timedelta(days=1)

# v1 select name -> v2 select token. v1 names with no v2 equivalent are dropped.
_SELECTS = {
    "id": "ID",
    "name": "NAME",
    "run_type": "RUN_TYPE",
    "status": "STATUS",
    "start_time": "START_TIME",
    "end_time": "END_TIME",
    "inputs": "INPUTS",
    "outputs": "OUTPUTS",
    "extra": "EXTRA",
    "error": "ERROR",
    "events": "EVENTS",
    "tags": "TAGS",
    "serialized": "MANIFEST",
    "trace_id": "TRACE_ID",
    "dotted_order": "DOTTED_ORDER",
    "session_id": "PROJECT_ID",
    "parent_run_id": "PARENT_RUN_IDS",
    "reference_example_id": "REFERENCE_EXAMPLE_ID",
    "s3_urls": "ATTACHMENTS",
}

# v1 lists an attachment in `s3_urls` under this key prefix.
_ATTACHMENT_PREFIX = "attachment."

# What a run replay needs. v2 returns only the ID unless fields are selected.
REPLAY_SELECT = tuple(name for name in _SELECTS if name != "s3_urls")

# v1 request fields that pass through unchanged.
_PASSTHROUGH = ("filter", "trace_filter", "tree_filter", "is_root", "cursor")


def to_v1_run(item: Dict[str, Any]) -> Dict[str, Any]:
    """Give a v2 run the v1 field names the migrators read."""
    run = dict(item)
    if "project_id" in item:
        run["session_id"] = item["project_id"]
    # dotted_order lists the run's ancestors root-first, itself last, so the
    # direct parent is the second-to-last part.
    parts = str(item.get("dotted_order") or "").split(".")
    if len(parts) > 1:
        run["parent_run_id"] = parts[-2][-36:]
    elif item.get("parent_run_ids"):
        run["parent_run_id"] = item["parent_run_ids"][-1]
    if "manifest" in item:
        # v2 returns {} where v1 returns null for a run with no manifest.
        run["serialized"] = item["manifest"] or None
    # v2 returns attachments as name -> presigned URL. v1 lists them in s3_urls,
    # and `attachments` on a run payload means the downloaded bytes.
    urls = run.pop("attachments", None)
    if isinstance(urls, dict) and urls:
        run["s3_urls"] = {
            f"{_ATTACHMENT_PREFIX}{name}": {"presigned_url": url} for name, url in urls.items()
        }
    return run


class RunQuery:
    """Run queries for one migrator, against whichever endpoint each instance has."""

    def __init__(self, migrator: Any):
        self.migrator = migrator

    def _client(self, side: str) -> EnhancedAPIClient:
        return self.migrator.source if side == "source" else self.migrator.dest

    def uses_v2(self, side: str, session_id: str) -> bool:
        """Whether one instance serves the v2 run query, checked once per client.

        Servers that predate it answer 404. So does a missing project, so a 404
        only counts as "no v2" once the project is confirmed to exist.
        """
        client = self._client(side)
        known = client.__dict__.get("runs_v2_supported")
        if known is not None:
            return known
        probe = {
            "project_ids": [str(session_id)],
            # Must be earlier than the server's "now", which is its default upper bound.
            "min_start_time": _iso(datetime.now(timezone.utc) - timedelta(hours=1)),
            "page_size": 1,
            "selects": ["ID"],
        }
        try:
            response = client.post(V2_RUNS_QUERY, probe)
            supported = isinstance(response, dict) and "items" in response
        except NotFoundError:
            client.get(f"/sessions/{session_id}")  # raises if the project is missing
            supported = False
        if not supported:
            self.migrator.use_deprecated_endpoint(side, "run query")
        client.__dict__["runs_v2_supported"] = supported
        return supported

    def page(
        self, side: str, body: Dict[str, Any]
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Fetch one page for a v1-shaped query body.

        Returns the runs, with v1 field names, and the cursor for the next page.
        """
        client = self._client(side)
        session_id = str(body["session"][0])
        if not self.uses_v2(side, session_id):
            response = client.post(V1_RUNS_QUERY, body) or {}
            return response.get("runs") or [], (response.get("cursors") or {}).get("next")
        response = client.post(V2_RUNS_QUERY, self._v2_body(side, session_id, body)) or {}
        runs = [to_v1_run(item) for item in response.get("items") or []]
        if "events" in (body.get("select") or REPLAY_SELECT):
            # v2 omits a field that is empty; v1 returns [] for a run with no events.
            for run in runs:
                run.setdefault("events", [])
        return runs, response.get("next_cursor")

    def _v2_body(self, side: str, session_id: str, body: Dict[str, Any]) -> Dict[str, Any]:
        selects = [_SELECTS[name] for name in body.get("select") or REPLAY_SELECT if name in _SELECTS]
        v2: Dict[str, Any] = {
            "project_ids": [str(s) for s in body["session"]],
            "selects": list(dict.fromkeys(selects)),
            # v2 looks back one day unless told otherwise, even for a query by ID.
            "min_start_time": body.get("start_time") or self._project_min_start(side, session_id),
            "page_size": min(int(body.get("limit") or 100), V2_MAX_PAGE_SIZE),
        }
        if body.get("id"):
            v2["ids"] = [str(i) for i in body["id"]]
        for key in _PASSTHROUGH:
            if body.get(key) is not None:
                v2[key] = body[key]
        return v2

    def _project_min_start(self, side: str, session_id: str) -> str:
        """The earliest a run of this project can have started, cached per project."""
        client = self._client(side)
        cache = client.__dict__.setdefault("_project_min_start", {})
        if session_id not in cache:
            start = None
            try:
                project = client.get(f"/sessions/{session_id}") or {}
                raw = str(project.get("start_time") or "")
                if raw:
                    start = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                    if start.tzinfo is None:
                        start = start.replace(tzinfo=timezone.utc)
                    start -= _START_SKEW
            except (ValueError, TypeError):
                start = None
            floor = datetime.now(timezone.utc) - _MAX_LOOKBACK
            cache[session_id] = _iso(max(start, floor) if start else floor)
        return cache[session_id]


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()
