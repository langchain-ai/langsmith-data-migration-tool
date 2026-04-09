"""Feedback migration logic."""

import hashlib
import json
from typing import Dict, List, Any, Tuple

from .base import BaseMigrator


class FeedbackMigrator(BaseMigrator):
    """Handles feedback migration for experiments."""

    def _feedback_fingerprint(
        self,
        source_experiment_id: str,
        feedback: Dict[str, Any],
    ) -> str:
        """Create a stable provenance fingerprint for feedback replay."""
        payload = {
            "source_experiment_id": source_experiment_id,
            "source_feedback_id": feedback.get("id"),
            "run_id": feedback.get("run_id"),
            "key": feedback.get("key"),
            "score": feedback.get("score"),
            "value": feedback.get("value"),
            "comment": feedback.get("comment"),
            "correction": feedback.get("correction"),
        }
        serialized = json.dumps(payload, sort_keys=True, default=str)
        return hashlib.sha256(serialized.encode("utf-8")).hexdigest()

    def list_feedback_for_session(self, session_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch feedback records for an experiment session.

        Args:
            session_id: The experiment session ID to fetch feedback for
            limit: Number of records per page

        Returns:
            List of feedback records
        """
        all_feedback = []
        offset = 0

        while True:
            try:
                response = self.source.get(
                    "/feedback",
                    params={"session": session_id, "limit": limit, "offset": offset}
                )

                # Handle response - could be list directly or dict with items
                if isinstance(response, list):
                    feedback_items = response
                elif isinstance(response, dict):
                    feedback_items = response.get("feedback", response.get("items", []))
                else:
                    break

                if not feedback_items:
                    break

                all_feedback.extend(feedback_items)
                self.log(f"Fetched {len(feedback_items)} feedback records (offset={offset})", "info")

                if len(feedback_items) < limit:
                    break

                offset += limit

            except Exception as e:
                self.log(f"Error fetching feedback for session {session_id}: {e}", "warning")
                break

        return all_feedback

    def list_feedback_for_runs(self, run_ids: List[str], limit: int = 100) -> List[Dict[str, Any]]:
        """
        Fetch feedback records for specific runs.

        Args:
            run_ids: List of run IDs to fetch feedback for
            limit: Number of records per page

        Returns:
            List of feedback records
        """
        all_feedback = []

        # Process in chunks to avoid URL length limits
        chunk_size = 50
        for i in range(0, len(run_ids), chunk_size):
            chunk = run_ids[i:i + chunk_size]
            run_param = ",".join(chunk)

            offset = 0
            while True:
                try:
                    response = self.source.get(
                        "/feedback",
                        params={"run": run_param, "limit": limit, "offset": offset}
                    )

                    if isinstance(response, list):
                        feedback_items = response
                    elif isinstance(response, dict):
                        feedback_items = response.get("feedback", response.get("items", []))
                    else:
                        break

                    if not feedback_items:
                        break

                    all_feedback.extend(feedback_items)

                    if len(feedback_items) < limit:
                        break

                    offset += limit

                except Exception as e:
                    self.log(f"Error fetching feedback for runs: {e}", "warning")
                    break

        return all_feedback

    def create_feedback(self, feedback: Dict[str, Any]) -> bool:
        """
        Create a single feedback record in destination.

        Args:
            feedback: Feedback record to create

        Returns:
            True if successful, False otherwise
        """
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create feedback: {feedback.get('key')}", "info")
            return True

        try:
            payload = {k: v for k, v in feedback.items() if not k.startswith("_")}
            self.dest.post("/feedback", payload)
            return True
        except Exception as e:
            self.log(f"Failed to create feedback '{feedback.get('key')}': {e}", "warning")
            return False

    def create_feedback_batch(self, feedbacks: List[Dict[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
        """
        Create feedback records in destination.

        Note: LangSmith doesn't have a /feedback/batch endpoint,
        so we create them one at a time.

        Args:
            feedbacks: List of feedback records to create

        Returns:
            Tuple of (number_created, created_feedbacks)
        """
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create {len(feedbacks)} feedback records", "info")
            return len(feedbacks), list(feedbacks)

        created = 0
        created_feedbacks: List[Dict[str, Any]] = []
        for feedback in feedbacks:
            if self.create_feedback(feedback):
                created += 1
                created_feedbacks.append(feedback)

        return created, created_feedbacks

    def migrate_feedback_for_experiments(
        self,
        experiment_id_mapping: Dict[str, str],
        run_id_mapping: Dict[str, str]
    ) -> Tuple[int, int]:
        """
        Migrate all feedback for migrated experiments.

        Args:
            experiment_id_mapping: Mapping of source experiment IDs to destination IDs
            run_id_mapping: Mapping of source run IDs to destination IDs

        Returns:
            Tuple of (total_feedback_found, total_feedback_migrated)
        """
        if not experiment_id_mapping:
            self.log("No experiments to migrate feedback for", "info")
            return 0, 0

        total_found = 0
        total_migrated = 0

        for source_exp_id, dest_exp_id in experiment_id_mapping.items():
            self.log(f"Fetching feedback for experiment {source_exp_id}...", "info")
            experiment_item_id = f"experiment_{source_exp_id}"
            if self.state:
                self.checkpoint_item(experiment_item_id, stage="migrate_feedback")

            # Fetch feedback for this experiment session
            feedbacks = self.list_feedback_for_session(source_exp_id)

            if not feedbacks:
                self.log(f"No feedback found for experiment {source_exp_id}", "info")
                continue

            total_found += len(feedbacks)
            self.log(f"Found {len(feedbacks)} feedback records for experiment {source_exp_id}", "info")

            # Transform feedback for destination
            migrated_feedbacks = []
            skipped = 0

            for fb in feedbacks:
                fingerprint = self._feedback_fingerprint(source_exp_id, fb)
                if self.state and self.state.get_mapped_id("feedback_fingerprint", fingerprint):
                    self.log(
                        f"Skipping feedback '{fb.get('key')}' - already replayed in a prior attempt",
                        "warning",
                    )
                    continue

                # Map run_id to destination
                source_run_id = fb.get("run_id")

                if source_run_id:
                    dest_run_id = run_id_mapping.get(source_run_id)
                    if not dest_run_id:
                        # Run wasn't migrated, skip this feedback
                        self.log(
                            f"Skipping feedback '{fb.get('key')}' - run {source_run_id} not in mapping",
                            "warning"
                        )
                        skipped += 1
                        continue
                else:
                    dest_run_id = None

                # Build the feedback record for destination
                migrated_fb = {
                    "run_id": dest_run_id,
                    "key": fb["key"],
                }

                # Add optional fields if present
                if fb.get("score") is not None:
                    migrated_fb["score"] = fb["score"]
                if fb.get("value") is not None:
                    migrated_fb["value"] = fb["value"]
                if fb.get("comment"):
                    migrated_fb["comment"] = fb["comment"]
                if fb.get("correction"):
                    migrated_fb["correction"] = fb["correction"]
                if fb.get("feedback_source"):
                    migrated_fb["feedback_source"] = fb["feedback_source"]

                migrated_fb["_fingerprint"] = fingerprint
                migrated_feedbacks.append(migrated_fb)

            if skipped > 0:
                self.log(f"Skipped {skipped} feedback records due to unmapped runs", "warning")

            # Create feedback in destination
            if migrated_feedbacks:
                created, created_feedbacks = self.create_feedback_batch(migrated_feedbacks)
                total_migrated += created
                self.log(
                    f"Migrated {created}/{len(migrated_feedbacks)} feedback for experiment {source_exp_id}",
                    "success"
                )
                if self.state:
                    for migrated_fb in created_feedbacks:
                        fingerprint = migrated_fb.get("_fingerprint")
                        if fingerprint:
                            self.state.set_mapped_id(
                                "feedback_fingerprint", fingerprint, fingerprint
                            )
                    if created == len(migrated_feedbacks):
                        self.checkpoint_item(
                            experiment_item_id,
                            stage="completed",
                            metadata={
                                "feedback_found": len(feedbacks),
                                "feedback_migrated": created,
                                "feedback_verified": True,
                            },
                        )
                    elif created < len(migrated_feedbacks):
                        issue = self.record_issue(
                            "transient",
                            "feedback_partial_replay",
                            f"Some feedback could not be replayed for experiment {source_exp_id}",
                            item_id=experiment_item_id,
                            next_action="Re-run `langsmith-migrator resume` to retry feedback creation.",
                            evidence={
                                "feedback_found": len(feedbacks),
                                "feedback_migrated": created,
                            },
                        )
                        if issue:
                            self.queue_remediation(
                                issue_id=issue.id,
                                next_action=issue.next_action or "Retry feedback replay.",
                                item_id=experiment_item_id,
                                command="langsmith-migrator resume",
                            )
                    self.persist_state()

        return total_found, total_migrated
