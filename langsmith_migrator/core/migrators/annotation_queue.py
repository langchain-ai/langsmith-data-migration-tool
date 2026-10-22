"""Annotation queue migration logic."""

from typing import Any, Dict, Iterator, List, Optional

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

    def find_existing_queue(self, name: str) -> Optional[str]:
        """Check if queue already exists in destination."""
        try:
            # We have to list all queues because there's no name search param
            queues = self.dest.get_paginated("/annotation-queues")
            for queue in queues:
                if isinstance(queue, dict) and queue.get("name") == name:
                    return queue.get("id")
        except Exception as e:
            self.log(f"Failed to check for existing queue: {e}", "warning")
        return None

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

        return response["id"]

    # ------------------------------------------------------------------
    # Queue items (runs)
    # ------------------------------------------------------------------

    def iter_queue_runs(self, queue_id: str) -> Iterator[Dict[str, Any]]:
        """Yield the runs currently in a source annotation queue, one page at a time.

        Pages with offset/limit directly instead of ``get_paginated`` so a
        failed page (permission error, exhausted retries) raises rather than
        looking like the end of the queue.
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
        """Add runs to a destination queue, preferring the keyed endpoint.

        `/runs/by-key` locates runs by their partition key (required on SmithDB
        workspaces). Older servers answer 404 for it, so fall back to the plain
        run-id list endpoint.
        """
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

        batch: List[Dict[str, str]] = []

        def flush() -> None:
            if not batch:
                return
            keys = list(batch)
            batch.clear()
            if dry_run:
                result["added"] += len(keys)
                return
            try:
                self._add_runs_to_queue(dest_queue_id, keys)
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
                    result["added"] += 1
                except APIError as run_error:
                    result["failed"] += 1
                    result["errors"].append((key["run_id"], str(run_error)))

        for run in self.iter_queue_runs(source_queue_id):
            result["total"] += 1
            key = self._run_key(run, session_id_map)
            if key is None:
                result["skipped"] += 1
                result["errors"].append(
                    (str(run.get("id") or run.get("run_id") or "?"), "missing run key fields")
                )
                continue
            batch.append(key)
            if len(batch) >= batch_size:
                flush()
        flush()

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
