"""Annotation queue migration logic."""

from typing import Dict, List, Any, Optional

from .base import BaseMigrator


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

    def create_queue(
        self,
        queue: Dict[str, Any],
        default_dataset_id: Optional[str] = None
    ) -> str:
        """Create annotation queue in destination."""
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
        return response["id"]
