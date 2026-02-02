"""Feedback migration logic."""

from typing import Dict, List, Any, Tuple

from .base import BaseMigrator


class FeedbackMigrator(BaseMigrator):
    """Handles feedback migration for experiments."""

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
            self.dest.post("/feedback", feedback)
            return True
        except Exception as e:
            self.log(f"Failed to create feedback '{feedback.get('key')}': {e}", "warning")
            return False

    def create_feedback_batch(self, feedbacks: List[Dict[str, Any]]) -> int:
        """
        Create feedback records in destination.

        Note: LangSmith doesn't have a /feedback/batch endpoint,
        so we create them one at a time.

        Args:
            feedbacks: List of feedback records to create

        Returns:
            Number of successfully created feedback records
        """
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create {len(feedbacks)} feedback records", "info")
            return len(feedbacks)

        created = 0
        for feedback in feedbacks:
            if self.create_feedback(feedback):
                created += 1

        return created

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

                migrated_feedbacks.append(migrated_fb)

            if skipped > 0:
                self.log(f"Skipped {skipped} feedback records due to unmapped runs", "warning")

            # Create feedback in destination
            if migrated_feedbacks:
                created = self.create_feedback_batch(migrated_feedbacks)
                total_migrated += created
                self.log(
                    f"Migrated {created}/{len(migrated_feedbacks)} feedback for experiment {source_exp_id}",
                    "success"
                )

        return total_found, total_migrated
