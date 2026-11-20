"""Annotation queue migration logic."""

import threading
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from typing import Any, Dict, Iterator, List, Optional, Set

from ..api_client import APIError, NotFoundError
from ...utils.pagination import PaginationHelper
from .base import BaseMigrator

QUEUE_RUNS_BATCH_SIZE = 100


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

    def _dest_scope(self) -> Optional[str]:
        """The destination workspace the client currently targets."""
        headers = getattr(getattr(self.dest, "session", None), "headers", None) or {}
        return headers.get("X-Tenant-Id")

    def find_existing_queue(self, name: str) -> Optional[str]:
        """Check if queue already exists in destination.

        The API has no name filter, so the destination queues are listed once
        per workspace and cached; listing them for every queue made a
        migration quadratic in the queue count. A failed listing is not cached.
        """
        scope = self._dest_scope()
        with self._dest_index_lock:
            index = self._dest_queue_index.get(scope)
            if index is None:
                try:
                    index = {}
                    # raise_errors: a failed page must not leave a partial index cached.
                    for queue in self.dest.get_paginated(
                        "/annotation-queues", raise_errors=True
                    ):
                        if isinstance(queue, dict) and queue.get("name"):
                            index.setdefault(queue["name"], queue.get("id"))
                except Exception as e:
                    self.log(f"Failed to check for existing queue: {e}", "warning")
                    return None
                self._dest_queue_index[scope] = index
            return index.get(name)

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
            "rubric_items": queue.get("rubric_items"),
            "rubric_instructions": queue.get("rubric_instructions"),
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
        """Create or update annotation queue in destination."""

        # Check if exists
        existing_id = self.find_existing_queue(queue["name"])

        if existing_id:
            if self.config.migration.skip_existing:
                self.log(f"Annotation queue '{queue['name']}' already exists, skipping", "warning")
                return existing_id
            else:
                self.log(f"Annotation queue '{queue['name']}' exists, updating...", "info")
                self.update_queue(existing_id, queue)
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
            "rubric_items": queue.get("rubric_items", []),
            "rubric_instructions": queue.get("rubric_instructions") or None,
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
        """List run items through the queue items API, one status at a time."""
        endpoint = self._items_endpoint(queue_id)
        seen: set = set()
        for status in self._ITEM_STATUSES:
            params: Dict[str, Any] = {
                "status": status, "item_type": "RUN", "page_size": QUEUE_RUNS_BATCH_SIZE,
            }
            while True:
                response = self.source.get(endpoint, dict(params))
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
                    yield {
                        "id": run_id,
                        "session_id": item.get("project_id") or item.get("session_id"),
                        "start_time": item.get("start_time"),
                    }
                # A page can be empty and still have a next cursor (the server skips
                # unusable rows), so follow the cursor rather than stopping early.
                cursor = page.get("next_cursor")
                if not cursor or cursor == params.get("cursor"):
                    break
                params["cursor"] = cursor

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

        Returns counts: ``total`` runs listed, ``added``, ``skipped`` (missing key
        fields), ``failed``, and ``errors`` as ``(run_id, message)`` pairs.
        """
        batch_size = max(1, min(batch_size, QUEUE_RUNS_BATCH_SIZE))
        result: Dict[str, Any] = {
            "total": 0, "added": 0, "skipped": 0, "failed": 0, "errors": [],
        }
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

        def post(keys: List[Dict[str, str]]) -> None:
            try:
                self._add_runs_to_queue(dest_queue_id, keys)
                with lock:
                    result["added"] += len(keys)
                return
            except APIError as batch_error:
                self.log(
                    f"Batch of {len(keys)} queue run(s) rejected ({batch_error}); "
                    "retrying one by one",
                    "warning",
                )
            for key in keys:
                try:
                    self._add_runs_to_queue(dest_queue_id, [key])
                    with lock:
                        result["added"] += 1
                except APIError as run_error:
                    with lock:
                        result["failed"] += 1
                        result["errors"].append((key["run_id"], str(run_error)))

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

        try:
            for run in self.iter_queue_runs(source_queue_id):
                result["total"] += 1
                key = self._run_key(run, session_id_map)
                if key is None:
                    with lock:
                        result["skipped"] += 1
                        result["errors"].append(
                            (str(run.get("id") or run.get("run_id") or "?"),
                             "missing run key fields")
                        )
                    continue
                batch.append(key)
                if len(batch) >= batch_size:
                    flush()
            flush()
            drain(0)
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
