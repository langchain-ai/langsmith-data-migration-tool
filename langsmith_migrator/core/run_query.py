"""Query runs through v1, or through v2 on instances where v1 no longer answers.

`POST /api/v1/runs/query` is deprecated in favour of `POST /api/v2/runs/query`,
and returns 501 on SmithDB-only instances. Callers keep building the v1 request
body and reading v1 field names; this module translates both ways, so a migrator
does not care which one answered.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from .api_client import APIError, EnhancedAPIClient, NotFoundError

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
    # Servers send these in lower case, but the v2 docs give them in upper case (LLM,
    # SUCCESS), and the write endpoints only accept lower case, so normalise them.
    for key in ("run_type", "status"):
        if isinstance(run.get(key), str):
            run[key] = run[key].lower()
    # parent_run_ids lists the ancestors from the trace root down to the direct parent.
    if item.get("parent_run_ids"):
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
    """Run queries for one migrator, against whichever endpoint each instance needs."""

    def __init__(self, migrator: Any):
        self.migrator = migrator
        self._uses_v2: Dict[str, bool] = {}  # per side
        self._min_start: Dict[Tuple[str, str], str] = {}  # per (side, project)

    def _client(self, side: str) -> EnhancedAPIClient:
        return self.migrator.source if side == "source" else self.migrator.dest

    def uses_v2(self, side: str, session_id: str) -> bool:
        """Whether to query one instance through v2, decided once per side (see also
        ``v1_not_served``).

        As in the LangSmith SDK, v1 stays in use wherever it still answers: it reports
        each run's tier and the URLs of offloaded payloads, and v2 does not. v2 is used
        where v1 cannot answer (SmithDB-only instances return 501 for it), and wherever
        v2 exists when MIGRATION_NO_DEPRECATED_FALLBACK is set.
        """
        if side not in self._uses_v2:
            if self._smithdb_only(side):
                v2 = True
            elif self.migrator.config.migration.no_deprecated_fallback:
                v2 = self._has_v2(side, session_id)
                if not v2:
                    self.migrator.use_deprecated_endpoint(side, "run query")  # raises
            else:
                v2 = False
            self._uses_v2[side] = v2
        return self._uses_v2[side]

    def _smithdb_only(self, side: str) -> bool:
        """Whether ``/info`` says the instance serves run queries from SmithDB only.

        Mirrors the SDK's ``get_query_backend``: an older server that omits the flags
        has ClickHouse queries on and SmithDB queries off.
        """
        try:
            info = self._client(side).get("/info")
        except Exception as exc:
            self.migrator.log(
                f"Could not read /info from the {side} instance ({exc}); "
                "using the v1 run query",
                "warning",
            )
            return False
        flags = info.get("instance_flags") if isinstance(info, dict) else None
        if not isinstance(flags, dict):
            return False
        return not flags.get("ch_query_enabled", True) and bool(flags.get("sdb_query_enabled"))

    def _has_v2(self, side: str, session_id: str) -> bool:
        """Probe for the v2 endpoint.

        Servers that predate it answer 404. So does a missing project, so a 404
        only counts as "no v2" once the project is confirmed to exist.
        """
        client = self._client(side)
        probe = {
            "project_ids": [str(session_id)],
            # Must be earlier than the server's "now", which is its default upper bound.
            "min_start_time": _iso(datetime.now(timezone.utc) - timedelta(hours=1)),
            "page_size": 1,
            "selects": ["ID"],
        }
        try:
            response = client.post(V2_RUNS_QUERY, probe)
        except NotFoundError:
            client.get(f"/sessions/{session_id}")  # raises if the project is missing
            return False
        return isinstance(response, dict) and "items" in response

    def page(
        self, side: str, body: Dict[str, Any]
    ) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        """Fetch one page for a v1-shaped query body.

        Returns the runs, with v1 field names, and the cursor for the next page.
        """
        client = self._client(side)
        session_id = str(body["session"][0])
        if not self.uses_v2(side, session_id):
            try:
                response = client.post(V1_RUNS_QUERY, body) or {}
                return response.get("runs") or [], (response.get("cursors") or {}).get("next")
            except APIError as exc:
                if exc.status_code != 501:
                    raise
                self.v1_not_served(side)
        response = client.post(V2_RUNS_QUERY, self._v2_body(side, session_id, body)) or {}
        runs = [to_v1_run(item) for item in response.get("items") or []]
        # v2 omits a field that is empty; v1 returns [] for no events and {} for no inputs.
        selected = body.get("select") or REPLAY_SELECT
        for run in runs:
            if "events" in selected:
                run.setdefault("events", [])
            if "inputs" in selected and run.get("inputs") is None:
                run["inputs"] = {}
        return runs, response.get("next_cursor")

    def v1_not_served(self, side: str) -> None:
        """Switch a side to v2 after v1 answered 501, whatever /info said."""
        self.migrator.log(
            f"The {side} instance does not serve the v1 run query (501); using v2",
            "warning",
        )
        self._uses_v2[side] = True

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
        key = (side, session_id)
        if key not in self._min_start:
            start = None
            try:
                project = self._client(side).get(f"/sessions/{session_id}") or {}
                raw = str(project.get("start_time") or "")
                if raw:
                    start = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                    if start.tzinfo is None:
                        start = start.replace(tzinfo=timezone.utc)
                    start -= _START_SKEW
            except (ValueError, TypeError):
                start = None
            floor = datetime.now(timezone.utc) - _MAX_LOOKBACK
            self._min_start[key] = _iso(max(start, floor) if start else floor)
        return self._min_start[key]


def _iso(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat()
