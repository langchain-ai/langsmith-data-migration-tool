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
