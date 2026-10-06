"""Feedback migration logic."""

import hashlib
import json
import uuid
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Any, Optional, Tuple

from .base import BaseMigrator
from ...utils.retry import APIError, AuthenticationError


# Multipart statuses that mean a part was malformed, so the batch is replayed record by
# record to isolate it. Anything else (rate limits, outages) would fail the same way per
# record, so it fails the batch instead.
_MULTIPART_FALLBACK_STATUSES = (400, 413, 422)


def _run_namespace() -> uuid.UUID:
    """Namespace the experiment migrator uses for destination run/trace ids."""
    from .experiment import ExperimentMigrator

    return ExperimentMigrator._RUN_NAMESPACE


# Feedback is created and checkpointed in chunks of this size, so an interruption
# (a crash, or a maintenance window hours into an experiment) loses at most one chunk.
FEEDBACK_CHECKPOINT_SIZE = 1000


class FeedbackMigrator(BaseMigrator):
    """Handles feedback migration for experiments."""

    def _feedback_workers(self) -> int:
        """Threads for feedback paging/creation: MIGRATION_FEEDBACK_WORKERS, else MIGRATION_WORKERS."""
        cfg = self.config.migration
        return max(1, cfg.feedback_workers or cfg.concurrent_workers)

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
        workers = self._feedback_workers()
        all_feedback: List[Dict[str, Any]] = []
        offset = 0

        # Offset pages are independent, so fetch a window of them at once. A 9.9M-record
        # workspace paged one 100-record request at a time spent 7 hours just counting.
        with ThreadPoolExecutor(max_workers=workers) as executor:
            while True:
                offsets = [offset + i * limit for i in range(workers)]
                try:
                    pages = list(
                        executor.map(
                            lambda o: self._fetch_feedback_page(session_id, limit, o), offsets
                        )
                    )
                except Exception as e:
                    self.log(f"Error fetching feedback for session {session_id}: {e}", "error")
                    raise

                last_page = False
                for page_offset, items in zip(offsets, pages):
                    all_feedback.extend(items)
                    if items:
                        self.log(
                            f"Fetched {len(items)} feedback records (offset={page_offset})", "info"
                        )
                    if len(items) < limit:
                        last_page = True
                        break

                if last_page:
                    break
                offset += workers * limit

        return all_feedback

    def _fetch_feedback_page(self, session_id: str, limit: int, offset: int) -> List[Dict[str, Any]]:
        """Fetch one page of feedback for a session; an unrecognised response is an empty page."""
        response = self.source.get(
            "/feedback", params={"session": session_id, "limit": limit, "offset": offset}
        )
        if isinstance(response, list):
            return response
        if isinstance(response, dict):
            return response.get("feedback", response.get("items", []))
        return []

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
                    self.log(f"Error fetching feedback for runs: {e}", "error")
                    raise

        return all_feedback

    def _record_replayed(self, created_feedbacks: List[Dict[str, Any]]) -> None:
        """Persist the fingerprints of feedback just created so resumes skip them."""
        if not self.state or not created_feedbacks:
            return
        for fb in created_feedbacks:
            fingerprint = fb.get("_fingerprint")
            if fingerprint:
                self.state.set_mapped_id("feedback_fingerprint", fingerprint, fingerprint)
        self.persist_state()

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

        By default each record is its own POST /feedback, sent concurrently across the
        configured workers. With MIGRATION_FEEDBACK_MULTIPART enabled, eligible records are
        instead sent in batches via POST /runs/multipart, the same ingest path the LangSmith
        SDK uses, which accepts feedback alongside runs and dedupes by id. A batch rejected
        for a bad part is replayed record by record with the same ids.

        Args:
            feedbacks: List of feedback records to create

        Returns:
            Tuple of (number_created, created_feedbacks)
        """
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create {len(feedbacks)} feedback records", "info")
            return len(feedbacks), list(feedbacks)

        workers = self._feedback_workers()
        results = [False] * len(feedbacks)

        # Each task is (indexes, multipart): a batch of eligible records sent as one
        # multipart request, or a single record sent on its own.
        tasks: List[Tuple[List[int], bool]] = []
        eligible: List[int] = []
        if self.config.migration.feedback_multipart:
            eligible = [i for i, fb in enumerate(feedbacks) if self._multipart_eligible(fb)]
            if len(eligible) < len(feedbacks):
                self.log(
                    f"{len(feedbacks) - len(eligible)} of {len(feedbacks)} feedback record(s) "
                    "lack a trace id and will be sent one POST at a time",
                    "warning",
                )
            size = self.config.migration.feedback_batch_size
            tasks.extend((eligible[i:i + size], True) for i in range(0, len(eligible), size))
        batched = set(eligible)
        tasks.extend(([i], False) for i in range(len(feedbacks)) if i not in batched)

        def run(task: Tuple[List[int], bool]) -> List[Tuple[int, bool]]:
            indexes, multipart = task
            if not multipart:
                return [(indexes[0], self.create_feedback(feedbacks[indexes[0]]))]
            # Fix each record's id up front so a per-record replay upserts the same
            # feedback rather than duplicating it if the batch had in fact landed.
            batch = [self._with_feedback_id(feedbacks[i]) for i in indexes]
            outcome = self._create_feedback_multipart(batch)
            if outcome is None:
                # One bad record rejects a whole multipart request, so replay the batch
                # record by record: the good ones still land and the bad one is isolated.
                return [(i, self.create_feedback(fb)) for i, fb in zip(indexes, batch)]
            return [(i, outcome) for i in indexes]

        with ThreadPoolExecutor(max_workers=workers) as executor:
            for outcome in executor.map(run, tasks):
                for index, ok in outcome:
                    results[index] = ok

        created_feedbacks = [fb for fb, ok in zip(feedbacks, results) if ok]
        return len(created_feedbacks), created_feedbacks

    @staticmethod
    def _multipart_eligible(feedback: Dict[str, Any]) -> bool:
        """Multipart feedback needs the run, its trace and the experiment (session) ids."""
        return bool(
            feedback.get("run_id") and feedback.get("_trace_id") and feedback.get("_session_id")
        )

    @staticmethod
    def _with_feedback_id(feedback: Dict[str, Any]) -> Dict[str, Any]:
        """
        Return a copy of the record with a destination feedback id.

        The id derives from the record's fingerprint, so it is unique per record (the
        multipart endpoint silently drops a repeated part name) and stable across retries
        and resumes, which lets both endpoints dedupe a resend.
        """
        fingerprint = feedback.get("_fingerprint")
        feedback_id = (
            str(uuid.uuid5(uuid.NAMESPACE_URL, f"langsmith-migrator/feedback/{fingerprint}"))
            if fingerprint
            else str(uuid.uuid4())
        )
        return {**feedback, "id": feedback_id}

    def _create_feedback_multipart(self, batch: List[Dict[str, Any]]) -> Optional[bool]:
        """
        Send a batch of feedback in one POST /runs/multipart request.

        Each part is named ``feedback.<id>`` using the id already on the record.

        Returns:
            True if the batch landed, None if it was rejected for a bad part (the caller
            replays it record by record), or False if it failed for any other reason. A
            rate limit or outage that outlasts the client's retries would fail every
            per-record POST the same way, so the batch is left for a resume to resend.

        Raises:
            AuthenticationError: the key cannot use /runs/multipart at all.
        """
        parts: List[Tuple[str, bytes]] = []
        for fb in batch:
            body = {
                k: v
                for k, v in fb.items()
                if not k.startswith("_") and v is not None
            }
            body.update({"trace_id": fb["_trace_id"], "session_id": fb["_session_id"]})
            parts.append((f"feedback.{fb['id']}", json.dumps(body, default=str).encode("utf-8")))

        try:
            self.dest.post_multipart("/runs/multipart", parts)
            return True
        except AuthenticationError as e:
            raise AuthenticationError(
                f"{e.args[0] if e.args else e} Unset MIGRATION_FEEDBACK_MULTIPART to send "
                "feedback one record at a time.",
                status_code=e.status_code,
                request_info=e.request_info,
            ) from e
        except APIError as e:
            if e.status_code in _MULTIPART_FALLBACK_STATUSES:
                self.log(
                    f"Multipart feedback batch of {len(batch)} rejected ({e}); "
                    "retrying individually",
                    "warning",
                )
                return None
            self.log(f"Multipart feedback batch of {len(batch)} failed: {e}", "warning")
            return False
        except Exception as e:
            self.log(f"Multipart feedback batch of {len(batch)} failed: {e}", "warning")
            return False

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
            Tuple of (total_feedback_found, total_feedback_accounted_for).

            The second element counts every record now present on the destination,
            which is the records created on this pass plus those a previous pass
            already replayed. Callers compare the two to decide whether an
            experiment's feedback is complete, so counting only this pass would
            report an already-complete experiment as failed.
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

            try:
                feedbacks = self.list_feedback_for_session(source_exp_id)
            except Exception as e:
                if self.state:
                    issue = self.record_issue(
                        "transient",
                        "feedback_query_failed",
                        f"Could not query source feedback for experiment {source_exp_id}",
                        item_id=experiment_item_id,
                        next_action="Re-run `langsmith-migrator resume` to retry the feedback query.",
                        evidence={"error": str(e)},
                    )
                    if issue:
                        self.queue_remediation(
                            issue_id=issue.id,
                            next_action=issue.next_action or "Retry the source feedback query.",
                            item_id=experiment_item_id,
                            command="langsmith-migrator resume",
                        )
                raise

            if not feedbacks:
                self.log(f"No feedback found for experiment {source_exp_id}", "info")
                continue

            total_found += len(feedbacks)
            self.log(f"Found {len(feedbacks)} feedback records for experiment {source_exp_id}", "info")

            if self.config.migration.dry_run:
                # A dry run creates no runs, so there is no run mapping to remap feedback
                # against; without this every record would count as unmapped and the
                # experiment would be reported as a failed replay.
                self.log(
                    f"[DRY RUN] Would migrate {len(feedbacks)} feedback records for experiment {source_exp_id}",
                    "info",
                )
                total_migrated += len(feedbacks)
                continue

            # Transform feedback for destination
            migrated_feedbacks = []
            unmapped_runs = 0
            already_replayed = 0

            for fb in feedbacks:
                fingerprint = self._feedback_fingerprint(source_exp_id, fb)
                if self.state and self.state.get_mapped_id("feedback_fingerprint", fingerprint):
                    already_replayed += 1
                    self.log(
                        f"Skipping feedback '{fb.get('key')}' - already replayed in a prior attempt",
                        "info",
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
                        unmapped_runs += 1
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
                # Used only by the multipart path (stripped from per-record POSTs).
                source_trace_id = fb.get("trace_id")
                if source_trace_id:
                    migrated_fb["_trace_id"] = str(
                        uuid.uuid5(_run_namespace(), source_trace_id)
                    )
                migrated_fb["_session_id"] = dest_exp_id
                migrated_feedbacks.append(migrated_fb)

            if unmapped_runs > 0:
                self.log(f"Skipped {unmapped_runs} feedback records due to unmapped runs", "warning")
            if already_replayed > 0:
                self.log(
                    f"{already_replayed} feedback record(s) were already replayed on an earlier pass",
                    "info",
                )

            # Create feedback in destination
            created = 0
            created_feedbacks: List[Dict[str, Any]] = []
            if migrated_feedbacks:
                for start in range(0, len(migrated_feedbacks), FEEDBACK_CHECKPOINT_SIZE):
                    chunk = migrated_feedbacks[start:start + FEEDBACK_CHECKPOINT_SIZE]
                    chunk_created, chunk_feedbacks = self.create_feedback_batch(chunk)
                    created += chunk_created
                    created_feedbacks.extend(chunk_feedbacks)
                    # Record replayed fingerprints now, not after the whole experiment, so a
                    # resume does not re-create (and duplicate) feedback already sent.
                    self._record_replayed(chunk_feedbacks)
                self.log(
                    f"Migrated {created}/{len(migrated_feedbacks)} feedback for experiment {source_exp_id}",
                    "success"
                )

            # Records replayed on an earlier pass are already on the destination, so
            # they count toward this experiment being complete. Counting only the
            # records created on *this* pass makes a fully-migrated experiment look
            # like a failure on every subsequent run, and one that can never recover.
            accounted = created + already_replayed
            total_migrated += accounted

            if self.state:
                if accounted == len(feedbacks):
                    self.checkpoint_item(
                        experiment_item_id,
                        stage="migrate_feedback",
                        metadata={
                            "feedback_found": len(feedbacks),
                            "feedback_migrated": accounted,
                        },
                    )
                else:
                    issue = self.record_issue(
                        "transient",
                        "feedback_partial_replay",
                        f"Some feedback could not be replayed for experiment {source_exp_id}",
                        item_id=experiment_item_id,
                        next_action="Re-run `langsmith-migrator resume` to retry feedback creation.",
                        evidence={
                            "feedback_found": len(feedbacks),
                            "feedback_migrated": accounted,
                            "already_replayed": already_replayed,
                            "unmapped_runs": unmapped_runs,
                            "create_failures": len(migrated_feedbacks) - created,
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
