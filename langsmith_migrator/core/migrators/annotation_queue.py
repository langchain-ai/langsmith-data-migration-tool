"""Annotation queue migration logic."""

import threading
from datetime import datetime, timedelta, timezone
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from typing import Any, Dict, Iterable, Iterator, List, Optional, Set

from ..api_client import APIError, NotFoundError
from ..run_query import RunQuery
from ...utils.pagination import PaginationHelper
from .base import BaseMigrator

QUEUE_RUNS_BATCH_SIZE = 100
# Per-run errors kept in a result (and so in the state file); counts stay exact.
MAX_STORED_RUN_ERRORS = 100
MISSING_RUN_ERROR = "run not found on destination"
# Rubric item fields that hold source IDs or read-only data. The destination
# resolves each item's config from its feedback key.
_RUBRIC_SOURCE_FIELDS = ("feedback_config", "feedback_config_id")


class AnnotationQueueMigrator(BaseMigrator):
    """Handles annotation queue migration."""

    def list_queues(self) -> List[Dict[str, Any]]:
        """List all annotation queues."""
        queues = []
        for queue in self.source.get_paginated("/annotation-queues"):
            if isinstance(queue, dict):
                queues.append(queue)
        return queues

    def get_queue(self, queue_id: str) -> Dict[str, Any]:
        """Get a specific annotation queue."""
        return self.source.get(f"/annotation-queues/{queue_id}")

    def with_details(self, queue: Dict[str, Any]) -> Dict[str, Any]:
        """The queue with its rubric and reviewers, which only the single-queue read returns."""
        if not queue.get("id"):
            return queue
        try:
            detail = self.get_queue(str(queue["id"]))
        except APIError as e:
            self.log(
                f"Could not read annotation queue '{queue.get('name')}' ({e}); "
                "its rubric and reviewers are not copied",
                "warning",
            )
            return queue
        return {**queue, **detail} if isinstance(detail, dict) else queue

    def dest_rubric_items(self, items: Any) -> Optional[List[Dict[str, Any]]]:
        """Rubric items for the destination, after creating their missing feedback configs."""
        if not isinstance(items, list):
            return None
        items = [i for i in items if isinstance(i, dict) and i.get("feedback_key")]
        self._ensure_feedback_configs(items)
        return [
            {k: v for k, v in item.items() if k not in _RUBRIC_SOURCE_FIELDS}
            for item in items
        ]

    def _ensure_feedback_configs(self, items: List[Dict[str, Any]]) -> None:
        """Create a destination feedback config for each rubric key that has none.

        Without one, a rubric item has no label type and reviewers have nowhere
        to answer. The config comes from the source rubric item.
        """
        wanted = {
            item["feedback_key"]: item["feedback_config"]
            for item in items
            if isinstance(item.get("feedback_config"), dict)
            and item["feedback_config"].get("feedback_config")
        }
        if not wanted or self.config.migration.dry_run:
            return
        scope = self._dest_scope()
        with self._feedback_config_lock:
            known = self._dest_feedback_keys.setdefault(scope, set())
            keys = [k for k in wanted if k not in known]
            if not keys:
                return
            try:
                existing = self.dest.get("/feedback-configs", {"key": keys}) or []
                have = {c.get("feedback_key") for c in existing if isinstance(c, dict)}
                for key in keys:
                    if key not in have:
                        config = wanted[key]
                        self.dest.post(
                            "/feedback-configs",
                            {
                                "feedback_key": key,
                                "feedback_config": config["feedback_config"],
                                "is_lower_score_better": config.get("is_lower_score_better"),
                            },
                        )
                        self.log(f"Created feedback config '{key}'", "success")
                    known.add(key)
            except APIError as e:
                self.log(f"Could not copy feedback configs {keys} ({e})", "warning")

    def assign_reviewers(self, dest_queue_id: str, queue: Dict[str, Any]) -> None:
        """Assign the source queue's reviewers, matched by email to destination members."""
        reviewers = [r for r in queue.get("assigned_reviewers") or [] if isinstance(r, dict)]
        if not reviewers or self.config.migration.dry_run:
            return
        try:
            members = self._dest_members_by_email()
        except APIError as e:
            self.log(
                f"Could not list destination workspace members ({e}); reviewers of "
                f"'{queue.get('name')}' are not assigned",
                "warning",
            )
            return
        for reviewer in reviewers:
            email = str(reviewer.get("email") or "").lower()
            identity_id = members.get(email) if email else None
            if not identity_id:
                self.log(
                    f"Reviewer {email or reviewer.get('id')} of queue '{queue.get('name')}' "
                    "is not a member of the destination workspace; not assigned",
                    "warning",
                )
                continue
            try:
                self.dest.post(
                    f"/annotation-queues/{dest_queue_id}/reviewers",
                    {"identity_id": identity_id},
                )
            except APIError as e:
                self.log(
                    f"Could not assign reviewer {email} to queue '{queue.get('name')}' ({e})",
                    "warning",
                )

    def _dest_members_by_email(self) -> Dict[str, str]:
        scope = self._dest_scope()
        with self._members_lock:
            if scope not in self._dest_members:
                members: Dict[str, str] = {}
                for member in self.dest.get_paginated("/workspaces/current/members/active"):
                    if isinstance(member, dict) and member.get("email") and member.get("id"):
                        members[str(member["email"]).lower()] = str(member["id"])
                self._dest_members[scope] = members
            return self._dest_members[scope]

    def _dest_scope(self) -> Optional[str]:
        """The destination workspace the client currently targets."""
        headers = getattr(getattr(self.dest, "session", None), "headers", None) or {}
        return headers.get("X-Tenant-Id")

    def find_existing_queue(self, name: str) -> Optional[str]:
        """Check if queue already exists in destination.

        The API has no name filter, so the destination queues are listed once
        per workspace and cached; listing them for every queue made a
        migration quadratic in the queue count. A failed listing raises and is
        not cached: treating it as "not found" would create a duplicate queue.
        """
        scope = self._dest_scope()
        with self._dest_index_lock:
            index = self._dest_queue_index.get(scope)
            if index is None:
                index = {}
                # raise_errors: a failed page must raise, not end the listing early.
                for queue in self.dest.get_paginated("/annotation-queues", raise_errors=True):
                    if isinstance(queue, dict) and queue.get("name"):
                        index.setdefault(queue["name"], queue.get("id"))
                self._dest_queue_index[scope] = index
            return index.get(name)

    def _name_lock(self, name: str) -> threading.Lock:
        """One lock per destination workspace and queue name."""
        key = (self._dest_scope(), name)
        with self._dest_index_lock:
            return self._name_locks.setdefault(key, threading.Lock())

    def _remember_dest_queue(self, name: str, queue_id: str) -> None:
        with self._dest_index_lock:
            index = self._dest_queue_index.get(self._dest_scope())
            if index is not None:
                index.setdefault(name, queue_id)

    def update_queue(self, queue_id: str, queue: Dict[str, Any]) -> None:
        """Update existing queue in destination."""
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would update annotation queue: {queue['name']} ({queue_id})")
            return

        payload = {
            "name": queue["name"],
            "description": queue.get("description"),
            "num_reviewers_per_item": queue.get("num_reviewers_per_item"),
            "enable_reservations": queue.get("enable_reservations"),
            "reservation_minutes": queue.get("reservation_minutes"),
            "rubric_items": self.dest_rubric_items(queue.get("rubric_items")),
            "rubric_instructions": queue.get("rubric_instructions"),
            "reviewer_access_mode": queue.get("reviewer_access_mode"),
        }

        # Remove None values
        payload = {k: v for k, v in payload.items() if v is not None}

        self.dest.patch(f"/annotation-queues/{queue_id}", payload)
        self.log(f"Updated annotation queue: {queue['name']} ({queue_id})", "success")

    def create_queue(
        self,
        queue: Dict[str, Any],
        default_dataset_id: Optional[str] = None
    ) -> str:
        """Create or update annotation queue in destination.

        Serialized per queue name, so two source queues with one name are
        handled as in a sequential run: the second finds the first.
        """
        queue = self.with_details(queue)
        with self._name_lock(queue["name"]):
            return self._create_queue_unlocked(queue, default_dataset_id)

    def _create_queue_unlocked(
        self,
        queue: Dict[str, Any],
        default_dataset_id: Optional[str] = None,
    ) -> str:
        # Check if exists
        existing_id = self.find_existing_queue(queue["name"])

        if existing_id:
            if self.config.migration.skip_existing:
                self.log(f"Annotation queue '{queue['name']}' already exists, skipping", "warning")
                return existing_id
            else:
                self.log(f"Annotation queue '{queue['name']}' exists, updating...", "info")
                self.update_queue(existing_id, queue)
                self.assign_reviewers(existing_id, queue)
                return existing_id

        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create annotation queue: {queue['name']}")
            return f"dry-run-{queue['id']}"

        payload = {
            "name": queue["name"],
            "description": queue.get("description") or None,
            "created_at": queue.get("created_at"),
            "updated_at": queue.get("updated_at"),
            "default_dataset": default_dataset_id,
            "num_reviewers_per_item": queue.get("num_reviewers_per_item", 1),
            "enable_reservations": queue.get("enable_reservations", False),
            "reservation_minutes": queue.get("reservation_minutes", 60),
            "rubric_items": self.dest_rubric_items(queue.get("rubric_items")) or [],
            "rubric_instructions": queue.get("rubric_instructions") or None,
            "reviewer_access_mode": queue.get("reviewer_access_mode") or "any",
            "session_ids": []
        }

        response = self.dest.post("/annotation-queues", payload)

        # Validate response has expected fields
        if not isinstance(response, dict):
            from ..api_client import APIError
            raise APIError(f"Invalid response creating queue: expected dict, got {type(response)}")
        if "id" not in response:
            from ..api_client import APIError
            raise APIError(f"Invalid response creating queue: missing 'id' field. Response: {response}")

        self._remember_dest_queue(queue["name"], response["id"])
        self.assign_reviewers(response["id"], queue)
        return response["id"]

    # ------------------------------------------------------------------
    # Queue items (runs)
    # ------------------------------------------------------------------

    # The items API requires a status. These two cover every item still in the queue:
    # which one an item is in depends on whether the caller has reviewed it, so both
    # are listed to copy the same runs whoever's key runs the migration. Archived
    # items are done and are left out, as the deprecated endpoint does by default.
    _ITEM_STATUSES = ("needs_my_review", "needs_others_review")

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        # Whether each side ("source"/"dest") serves the queue items API.
        self._items_support: Dict[str, bool] = {}
        self._items_support_lock = threading.Lock()
        # Destination queue name -> id, per destination workspace (X-Tenant-Id).
        self._dest_queue_index: Dict[Optional[str], Dict[str, str]] = {}
        self._dest_index_lock = threading.Lock()
        self._name_locks: Dict[tuple, threading.Lock] = {}
        self._dest_run_query = RunQuery(self)
        # Per destination workspace: feedback keys known to have a config, and
        # active members' identity IDs by lower-case email.
        self._dest_feedback_keys: Dict[Optional[str], Set[str]] = {}
        self._feedback_config_lock = threading.Lock()
        self._dest_members: Dict[Optional[str], Dict[str, str]] = {}
        self._members_lock = threading.Lock()

    def _post_workers(self) -> int:
        """Threads posting run batches for one queue, like feedback per experiment."""
        migration = self.config.migration
        return max(1, int(migration.feedback_workers or migration.concurrent_workers or 1))

    @staticmethod
    def _items_endpoint(queue_id: str) -> str:
        return f"/platform/annotation-queues/{queue_id}/items"

    def _items_supported(self, side: str, queue_id: str) -> bool:
        """Whether one instance serves the queue items API, checked once per side.

        The items API replaces the deprecated ``/annotation-queues/{id}/runs``
        endpoints. Servers that predate it answer 404; so does a missing queue,
        so a 404 is only read as "unsupported" once the queue is confirmed to exist.
        """
        with self._items_support_lock:
            return self._probe_items_support(side, queue_id)

    def _probe_items_support(self, side: str, queue_id: str) -> bool:
        if side not in self._items_support:
            client = self.source if side == "source" else self.dest
            try:
                client.get(
                    self._items_endpoint(queue_id),
                    {"status": self._ITEM_STATUSES[0], "item_type": "RUN", "page_size": 1},
                )
                self._items_support[side] = True
            except NotFoundError:
                client.get(f"/annotation-queues/{queue_id}")  # raises if the queue is missing
                self.use_deprecated_endpoint(side, "annotation queue items")
                self._items_support[side] = False
        return self._items_support[side]

    def iter_queue_runs(self, queue_id: str) -> Iterator[Dict[str, Any]]:
        """Yield the runs currently in a source annotation queue, one page at a time.

        Each run has at least ``id``, ``session_id`` and ``start_time``. A failed
        page (permission error, exhausted retries) raises rather than looking
        like the end of the queue.
        """
        if self._items_supported("source", queue_id):
            yield from self._iter_queue_items(queue_id)
        else:
            yield from self._iter_queue_runs_legacy(queue_id)

    def _iter_queue_items(self, queue_id: str) -> Iterator[Dict[str, Any]]:
        """List source run items through the queue items API."""
        for item in self._iter_items("source", queue_id):
            yield {
                "id": item.get("run_id"),
                "session_id": item.get("project_id") or item.get("session_id"),
                "start_time": item.get("start_time"),
                "added_at": item.get("added_at"),
            }

    def _iter_items(self, side: str, queue_id: str) -> Iterator[Dict[str, Any]]:
        """List one instance's run items through the queue items API, one status at a time."""
        client = self.source if side == "source" else self.dest
        endpoint = self._items_endpoint(queue_id)
        seen: set = set()
        for status in self._ITEM_STATUSES:
            params: Dict[str, Any] = {
                "status": status, "item_type": "RUN", "page_size": QUEUE_RUNS_BATCH_SIZE,
            }
            while True:
                response = client.get(endpoint, dict(params))
                page = response if isinstance(response, dict) else {}
                items = page.get("items") or []
                for item in items:
                    if not isinstance(item, dict):
                        continue
                    run_id = item.get("run_id")
                    if run_id and run_id in seen:
                        continue
                    if run_id:
                        seen.add(run_id)
                    yield item
                # A page can be empty and still have a next cursor (the server skips
                # unusable rows), so follow the cursor rather than stopping early.
                cursor = page.get("next_cursor")
                if not cursor or cursor == params.get("cursor"):
                    break
                params["cursor"] = cursor

    def restore_item_order(
        self, dest_queue_id: str, added_at: Dict[str, str], workers: int = 1
    ) -> Dict[str, int]:
        """Give each destination item its source ``added_at``, which sets the review order.

        Linking stamps every item with the time it was linked, and parallel
        batches shuffle that. Only items whose time differs are patched.
        """
        counts = {"reordered": 0, "order_failed": 0}
        if not added_at or not self._items_supported("dest", dest_queue_id):
            return counts
        patches = []
        try:
            for item in self._iter_items("dest", dest_queue_id):
                wanted = added_at.get(str(item.get("run_id")))
                if wanted and item.get("id") and _parse_time(item.get("added_at")) != _parse_time(wanted):
                    patches.append((str(item["id"]), wanted))
        except APIError as e:
            self.log(f"Could not list destination queue items to restore their order ({e})", "warning")
            counts["order_failed"] = len(added_at)
            return counts
        lock = threading.Lock()

        def patch(entry: tuple) -> None:
            item_id, wanted = entry
            try:
                self.dest.patch(
                    f"{self._items_endpoint(dest_queue_id)}/{item_id}", {"added_at": wanted}
                )
            except APIError as e:
                with lock:
                    counts["order_failed"] += 1
                self.log(f"Could not set the position of queue item {item_id} ({e})", "warning")
                return
            with lock:
                counts["reordered"] += 1

        if workers > 1 and len(patches) > 1:
            with ThreadPoolExecutor(max_workers=workers) as pool:
                list(pool.map(patch, patches))
        else:
            for entry in patches:
                patch(entry)
        return counts

    def _iter_queue_runs_legacy(self, queue_id: str) -> Iterator[Dict[str, Any]]:
        """List runs through the deprecated ``/runs`` endpoint (offset pagination).

        Pages with offset/limit directly instead of ``get_paginated`` so a
        failed page raises rather than looking like the end of the queue.
        """
        endpoint = f"/annotation-queues/{queue_id}/runs"
        page_size = QUEUE_RUNS_BATCH_SIZE
        offset = 0
        seen: set = set()
        while True:
            response = self.source.get(endpoint, {"limit": page_size, "offset": offset})
            items = PaginationHelper._extract_items(response)
            if not items:
                return
            new_items = 0
            for run in items:
                if not isinstance(run, dict):
                    continue
                run_id = run.get("id") or run.get("run_id")
                if run_id and run_id in seen:
                    continue
                if run_id:
                    seen.add(run_id)
                new_items += 1
                yield run
            if new_items == 0 or len(items) < page_size:
                return
            offset += len(items)

    def list_queue_runs(self, queue_id: str) -> List[Dict[str, Any]]:
        """List the runs currently in a source annotation queue."""
        return list(self.iter_queue_runs(queue_id))

    @staticmethod
    def _run_key(
        run: Dict[str, Any], session_id_map: Optional[Dict[str, str]]
    ) -> Optional[Dict[str, str]]:
        """Build the by-key payload for one queue run, or None when a field is missing."""
        run_id = run.get("id") or run.get("run_id")
        session_id = run.get("session_id")
        start_time = run.get("start_time")
        if not (run_id and session_id and start_time):
            return None
        if session_id_map:
            session_id = session_id_map.get(str(session_id), session_id)
        if hasattr(start_time, "isoformat"):
            start_time = start_time.isoformat()
        return {
            "run_id": str(run_id),
            "session_id": str(session_id),
            "start_time": str(start_time),
        }

    def _add_runs_to_queue(self, dest_queue_id: str, keys: List[Dict[str, str]]) -> None:
        """Add runs to a destination queue.

        Uses the queue items API where the destination has it. Older servers get
        the deprecated endpoints: `/runs/by-key`, which locates runs by their
        partition key, then the plain run-id list for servers without that either.
        """
        if self._items_supported("dest", dest_queue_id):
            self.dest.post(
                self._items_endpoint(dest_queue_id),
                {"items": [{"item_type": "RUN", **key} for key in keys]},
            )
            return
        try:
            self.dest.post(f"/annotation-queues/{dest_queue_id}/runs/by-key", keys)
        except NotFoundError as e:
            if "by-key" not in str(e):
                raise
            self.dest.post(
                f"/annotation-queues/{dest_queue_id}/runs", [k["run_id"] for k in keys]
            )

    def _existing_dest_runs(self, keys: List[Dict[str, str]]) -> Optional[Set[str]]:
        """The run IDs among ``keys`` that exist on the destination, or None if unknown.

        One run query per project, filtered by ID and bounded below by the
        earliest start time in the batch. A failed lookup returns None so the
        caller links the batch as it would without the check.
        """
        by_session: Dict[str, List[Dict[str, str]]] = {}
        for key in keys:
            by_session.setdefault(key["session_id"], []).append(key)
        found: Set[str] = set()
        try:
            for session_id, group in by_session.items():
                body: Dict[str, Any] = {
                    "session": [session_id],
                    "id": [k["run_id"] for k in group],
                    "select": ["id"],
                    "limit": len(group),
                }
                start = _lower_bound(k["start_time"] for k in group)
                if start:
                    body["start_time"] = start
                while True:
                    runs, cursor = self._dest_run_query.page("dest", body)
                    found.update(str(r.get("id")) for r in runs if r.get("id"))
                    if not cursor or cursor == body.get("cursor"):
                        break
                    body["cursor"] = cursor
        except APIError as error:
            self.log(
                f"Could not look up {len(keys)} queue run(s) on the destination "
                f"({error}); linking them without the check",
                "warning",
            )
            return None
        return found

    def migrate_queue_runs(
        self,
        source_queue_id: str,
        dest_queue_id: str,
        session_id_map: Optional[Dict[str, str]] = None,
        batch_size: int = QUEUE_RUNS_BATCH_SIZE,
    ) -> Dict[str, Any]:
        """Copy the runs queued in a source queue into the destination queue.

        Runs are addressed by ID, so this only works when the runs already exist
        on the destination with their source IDs (for example after a trace
        storage migration). Review progress (reviewed / done / reviewer cursors)
        is not exposed by the API and is not copied.

        With ``check_queue_runs_exist`` (set for BYOC migrations), each batch is
        first looked up on the destination by run ID, and only the runs found
        are linked: queues whose runs expired before the trace migration cost
        two requests per batch, and every missing run is confirmed, not guessed.
        A rejected batch is split in halves until the rejected runs are found,
        so a few missing runs cost a few requests rather than one per run.

        Returns counts: ``total`` runs listed, ``added``, ``skipped`` (missing key
        fields), ``failed`` (of which ``missing`` were not found on the
        destination), and ``errors`` as up to ``MAX_STORED_RUN_ERRORS``
        ``(run_id, message)`` pairs, with ``errors_truncated`` counting the rest.
        """
        batch_size = max(1, min(batch_size, QUEUE_RUNS_BATCH_SIZE))
        result: Dict[str, Any] = {
            "total": 0, "added": 0, "skipped": 0, "failed": 0,
            "missing": 0, "errors": [], "errors_truncated": 0,
            "reordered": 0, "order_failed": 0,
        }
        check_exists = bool(getattr(self.config.migration, "check_queue_runs_exist", False))
        dry_run = self.config.migration.dry_run
        if not dry_run:
            # Settle which endpoints the destination has before any run is sent, so
            # MIGRATION_NO_DEPRECATED_FALLBACK stops the copy instead of failing each run.
            self._items_supported("dest", dest_queue_id)

        batch: List[Dict[str, str]] = []
        lock = threading.Lock()
        workers = self._post_workers()
        # Listing stays sequential (cursor pages); posting overlaps with it. At
        # most 2x workers batches are in flight, which bounds memory.
        max_in_flight = workers * 2
        in_flight: Set[Future] = set()
        executor = ThreadPoolExecutor(max_workers=workers) if workers > 1 else None

        def record_error(run_id: str, message: str) -> None:
            # Caller holds ``lock``.
            if len(result["errors"]) < MAX_STORED_RUN_ERRORS:
                result["errors"].append((run_id, message))
            else:
                result["errors_truncated"] += 1

        def post(keys: List[Dict[str, str]]) -> None:
            if check_exists:
                found = self._existing_dest_runs(keys)
                if found is not None:
                    absent = [k for k in keys if k["run_id"] not in found]
                    if absent:
                        with lock:
                            result["failed"] += len(absent)
                            result["missing"] += len(absent)
                            for key in absent:
                                record_error(key["run_id"], MISSING_RUN_ERROR)
                    keys = [k for k in keys if k["run_id"] in found]
            link(keys)

        def link(keys: List[Dict[str, str]]) -> None:
            if not keys:
                return
            try:
                self._add_runs_to_queue(dest_queue_id, keys)
            except APIError as error:
                if len(keys) == 1:
                    with lock:
                        result["failed"] += 1
                        record_error(keys[0]["run_id"], str(error))
                    return
                self.log(
                    f"Batch of {len(keys)} queue run(s) rejected ({error}); splitting",
                    "warning",
                )
                middle = len(keys) // 2
                link(keys[:middle])
                link(keys[middle:])
                return
            with lock:
                result["added"] += len(keys)

        def drain(limit: int) -> None:
            while len(in_flight) > limit:
                done, _ = wait(in_flight, return_when=FIRST_COMPLETED)
                for future in done:
                    in_flight.discard(future)
                    future.result()  # a non-API error stops the copy

        def flush() -> None:
            if not batch:
                return
            keys = list(batch)
            batch.clear()
            if dry_run:
                result["added"] += len(keys)
                return
            if executor is None:
                post(keys)
                return
            drain(max_in_flight - 1)
            in_flight.add(executor.submit(post, keys))

        # Source added_at per run ID, restored on the destination after linking.
        added_at: Dict[str, str] = {}
        try:
            for run in self.iter_queue_runs(source_queue_id):
                result["total"] += 1
                if run.get("added_at") and (run.get("id") or run.get("run_id")):
                    added_at[str(run.get("id") or run.get("run_id"))] = str(run["added_at"])
                key = self._run_key(run, session_id_map)
                if key is None:
                    with lock:
                        result["skipped"] += 1
                        record_error(
                            str(run.get("id") or run.get("run_id") or "?"),
                            "missing run key fields",
                        )
                    continue
                batch.append(key)
                if len(batch) >= batch_size:
                    flush()
            flush()
            drain(0)
            if not dry_run:
                result.update(self.restore_item_order(dest_queue_id, added_at, workers))
        finally:
            if executor is not None:
                # On error, drop queued batches; running ones finish.
                executor.shutdown(wait=True, cancel_futures=True)

        if dry_run:
            self.log(
                f"[DRY RUN] Would add {result['added']} run(s) to queue {dest_queue_id}"
            )
        else:
            self.log(
                f"Queue {dest_queue_id}: {result['added']} run(s) added, "
                f"{result['failed']} failed, {result['skipped']} skipped",
                "success" if not result["failed"] else "warning",
            )
        return result


def _lower_bound(start_times: Iterable[str]) -> Optional[str]:
    """The earliest start time as RFC 3339 in UTC, one second early, or None."""
    earliest = None
    for raw in start_times:
        try:
            value = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        except ValueError:
            return None
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        if earliest is None or value < earliest:
            earliest = value
    if earliest is None:
        return None
    return (earliest - timedelta(seconds=1)).astimezone(timezone.utc).isoformat()


def _parse_time(value: Any) -> Optional[datetime]:
    """A timestamp as an aware UTC datetime, or None when it cannot be read."""
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
