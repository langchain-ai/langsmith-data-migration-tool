"""Experiment migration logic."""

from collections import ChainMap
from typing import Dict, List, Any, Optional, Tuple
import copy
import hashlib
import json
import uuid

from .base import BaseMigrator


class ExperimentMigrator(BaseMigrator):
    """Handles experiment and run migration."""

    _RUN_NAMESPACE = uuid.uuid5(uuid.NAMESPACE_URL, "langsmith-data-migration-tool/runs")

    def _deterministic_run_id(self, source_run_id: str) -> str:
        """Generate a stable destination run ID for idempotent replay."""
        return str(uuid.uuid5(self._RUN_NAMESPACE, source_run_id))

    def _experiment_item_id(self, experiment_id: str) -> str:
        """Return the tracked item id for an experiment."""
        return f"experiment_{experiment_id}"

    def _regenerate_dotted_order(
        self,
        dotted_order: Optional[str],
        id_mapping: Dict[str, str],
        new_run_id: str
    ) -> Optional[str]:
        """
        Regenerate dotted_order by replacing source UUIDs with new mapped UUIDs.

        dotted_order format: {timestamp}Z{uuid}.{timestamp}Z{uuid}...
        - Each part is {timestamp}Z{uuid}
        - Parts are separated by '.'
        - First part's UUID is the trace_id (for root) or parent chain
        - Last part's UUID MUST be the run's own ID

        Args:
            dotted_order: Original dotted_order string from source
            id_mapping: Mapping of source UUIDs to destination UUIDs
            new_run_id: The new run ID - MUST be used for the last part

        Returns:
            Regenerated dotted_order with new UUIDs, or None if input is None
        """
        if not dotted_order:
            return None

        parts = dotted_order.split(".")
        new_parts = []

        for i, part in enumerate(parts):
            # Find the Z separator between timestamp and UUID
            # Format: 20260203T003519695988Zc9ba7a73-985a-4104-aad7-7e3c4fd27a5f
            z_idx = part.rfind("Z")
            if z_idx == -1 or z_idx == len(part) - 1:
                # No Z found or Z is at the end, keep as-is
                new_parts.append(part)
                continue

            timestamp = part[:z_idx + 1]  # Include the Z
            old_uuid = part[z_idx + 1:]

            # For the LAST part, always use the new_run_id
            # This ensures run_id matches the last part of dotted_order (API requirement)
            if i == len(parts) - 1:
                new_parts.append(timestamp + new_run_id)
            else:
                # Map the UUID to its new value for parent chain
                new_uuid = id_mapping.get(old_uuid, self._deterministic_run_id(old_uuid))
                new_parts.append(timestamp + new_uuid)

        return ".".join(new_parts)

    def _ensure_evaluator_types(self, extra: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Ensure evaluator configurations have proper type and feedback_key fields.

        In LangSmith, evaluators need:
        - type: "Code" or "LLM"
        - feedback_key: string identifying the feedback metric

        Args:
            extra: The extra metadata dict from an experiment

        Returns:
            The extra dict with properly typed evaluators, or None if no extra
        """
        if not extra:
            return extra

        # Make a deep copy to avoid modifying the original
        extra_copy = copy.deepcopy(extra)

        # Check common locations where evaluators might be stored
        evaluator_keys = ['evaluators', 'comparative_experiment_evaluators', 'dataset_evaluators']

        total_evaluators_found = 0

        for key in evaluator_keys:
            if key in extra_copy and isinstance(extra_copy[key], list):
                evaluator_count = len(extra_copy[key])
                if evaluator_count > 0:
                    total_evaluators_found += evaluator_count
                    self.log(f"Processing {evaluator_count} evaluator(s) from '{key}'", "info")

                for evaluator in extra_copy[key]:
                    if not isinstance(evaluator, dict):
                        continue

                    # Log full evaluator structure for debugging
                    if self.config.migration.verbose:
                        self.log(f"  Raw evaluator data: {json.dumps(evaluator, indent=2, default=str)}", "info")

                    # Ensure 'type' field exists
                    if 'type' not in evaluator or not evaluator['type']:
                        # Try to infer type from other fields
                        inferred_type = None

                        # Check various field names
                        if 'evaluator_type' in evaluator and evaluator['evaluator_type']:
                            inferred_type = evaluator['evaluator_type']
                        elif 'eval_type' in evaluator and evaluator['eval_type']:
                            inferred_type = evaluator['eval_type']
                        elif '__type__' in evaluator and evaluator['__type__']:
                            inferred_type = evaluator['__type__']
                        # Check for code/function indicators
                        elif any(k in evaluator for k in ['code', 'function', 'func', 'source_code', 'python_code']):
                            inferred_type = 'Code'
                        # Check for LLM indicators
                        elif any(k in evaluator for k in ['llm', 'model', 'model_name', 'llm_config', 'prompt_template']):
                            inferred_type = 'LLM'
                        # Check class/constructor hints
                        elif evaluator.get('__class__'):
                            class_name = str(evaluator['__class__']).lower()
                            if 'llm' in class_name or 'chat' in class_name or 'model' in class_name:
                                inferred_type = 'LLM'
                            else:
                                inferred_type = 'Code'

                        if inferred_type:
                            evaluator['type'] = inferred_type
                            if self.config.migration.verbose:
                                self.log(f"  Inferred type '{inferred_type}' from fields", "info")
                        else:
                            # Default to Code if we can't determine
                            evaluator['type'] = 'Code'
                            self.log(f"Warning: Evaluator missing type, defaulting to 'Code': {evaluator.get('name', 'unknown')}", "warning")
                            self.log(f"  Full evaluator data: {json.dumps(evaluator, indent=2, default=str)}", "warning")

                    # Ensure 'feedback_key' field exists
                    if 'feedback_key' not in evaluator or not evaluator['feedback_key']:
                        # Try to infer from other fields
                        inferred_key = None

                        # Try various field names
                        if 'key' in evaluator and evaluator['key']:
                            inferred_key = evaluator['key']
                        elif 'name' in evaluator and evaluator['name']:
                            inferred_key = evaluator['name']
                        elif 'feedback_name' in evaluator and evaluator['feedback_name']:
                            inferred_key = evaluator['feedback_name']
                        elif 'metric_name' in evaluator and evaluator['metric_name']:
                            inferred_key = evaluator['metric_name']
                        elif 'id' in evaluator and evaluator['id']:
                            # Use ID as last resort if it's a string
                            if isinstance(evaluator['id'], str):
                                inferred_key = evaluator['id']

                        if inferred_key:
                            evaluator['feedback_key'] = inferred_key
                            if self.config.migration.verbose:
                                self.log(f"  Inferred feedback_key '{inferred_key}' from fields", "info")
                        else:
                            # Generate a default feedback key using a more stable identifier
                            if 'name' in evaluator:
                                evaluator['feedback_key'] = f"{evaluator['name']}_key"
                            else:
                                evaluator['feedback_key'] = f"evaluator_{hashlib.sha256(json.dumps(evaluator, sort_keys=True, default=str).encode()).hexdigest()[:12]}"
                            self.log(f"Warning: Evaluator missing feedback_key, generated: {evaluator['feedback_key']}", "warning")
                            self.log(f"  Full evaluator data: {json.dumps(evaluator, indent=2, default=str)}", "warning")

                    # Always log evaluator details (not just in verbose mode) so user can see they're being migrated
                    self.log(f"  ✓ Evaluator: {evaluator.get('name', 'unnamed')} (type={evaluator.get('type')}, feedback_key={evaluator.get('feedback_key')})", "success")

        if total_evaluators_found > 0:
            self.log(f"Total evaluators processed: {total_evaluators_found}", "success")

        return extra_copy

    def list_experiments(self, dataset_id: str) -> List[Dict[str, Any]]:
        """
        List experiments for a dataset.

        Fetches full experiment details including evaluators in the 'extra' field.
        """
        experiments = []
        for experiment in self.source.get_paginated(
            "/sessions",
            params={"reference_dataset": dataset_id}
        ):
            if isinstance(experiment, dict):
                # Get the full experiment details to ensure we have all metadata
                # including evaluators in the 'extra' field
                exp_id = experiment.get('id')
                if exp_id:
                    try:
                        full_experiment = self.source.get(f"/sessions/{exp_id}")
                        # Log if we find evaluators
                        if full_experiment.get('extra'):
                            has_evaluators = any(
                                key in full_experiment['extra']
                                for key in ['evaluators', 'comparative_experiment_evaluators', 'dataset_evaluators']
                            )
                            if has_evaluators:
                                self.log(f"Found evaluators in experiment '{full_experiment.get('name', exp_id)}'", "info")
                        experiments.append(full_experiment)
                    except Exception as e:
                        self.log(f"Failed to fetch full details for experiment {exp_id}: {e}", "warning")
                        # Fall back to the summary data if full fetch fails
                        experiments.append(experiment)
                else:
                    experiments.append(experiment)
        return experiments

    def find_existing_experiment(self, name: str, dataset_id: str) -> Optional[str]:
        """
        Check if an experiment already exists in destination.

        Args:
            name: Experiment name
            dataset_id: Reference dataset ID

        Returns:
            The experiment ID if found, None otherwise
        """
        try:
            # List experiments for the dataset
            experiments = []
            for experiment in self.dest.get_paginated(
                "/sessions",
                params={"reference_dataset": dataset_id}
            ):
                if isinstance(experiment, dict):
                    experiments.append(experiment)

            # Find by name
            for exp in experiments:
                if exp.get("name") == name:
                    return exp.get("id")

            return None
        except Exception as e:
            self.log(f"Failed to check for existing experiment: {e}", "warning")
            return None

    def update_experiment(self, experiment_id: str, experiment: Dict[str, Any]) -> None:
        """Update existing experiment in destination."""
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would update experiment: {experiment['name']} ({experiment_id})")
            return

        # Ensure evaluators in the extra field are properly typed
        extra = self._ensure_evaluator_types(experiment.get("extra"))

        payload = {
            "name": experiment["name"],
            "description": experiment.get("description"),
            "extra": extra,
            "trace_tier": experiment.get("trace_tier")
        }

        # Remove None values
        payload = {k: v for k, v in payload.items() if v is not None}

        self.dest.patch(f"/sessions/{experiment_id}", payload)
        self.log(f"Updated experiment: {experiment['name']} ({experiment_id})", "success")

    def create_experiment(self, experiment: Dict[str, Any], new_dataset_id: str) -> str:
        """Create or update experiment in destination."""
        # Check if experiment already exists
        existing_id = self.find_existing_experiment(experiment["name"], new_dataset_id)

        if existing_id:
            if self.config.migration.skip_existing:
                self.log(f"Experiment '{experiment['name']}' already exists, skipping", "warning")
                return existing_id
            else:
                self.log(f"Experiment '{experiment['name']}' exists, updating...", "info")
                self.update_experiment(existing_id, experiment)
                return existing_id

        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create experiment: {experiment['name']}")
            return f"dry-run-{experiment['id']}"

        # Ensure evaluators in the extra field are properly typed
        extra = self._ensure_evaluator_types(experiment.get("extra"))

        payload = {
            "name": experiment["name"],
            "description": experiment.get("description") or None,
            "reference_dataset_id": new_dataset_id,
            "start_time": experiment.get("start_time"),
            "end_time": experiment.get("end_time"),
            "extra": extra,
            "trace_tier": experiment.get("trace_tier")
        }

        response = self.dest.post("/sessions", payload)

        # Validate response has expected fields
        if not isinstance(response, dict):
            from ..api_client import APIError
            raise APIError(f"Invalid response creating experiment: expected dict, got {type(response)}")
        if "id" not in response:
            from ..api_client import APIError
            raise APIError(f"Invalid response creating experiment: missing 'id' field. Response: {response}")

        return response["id"]

    def migrate_runs_streaming(
        self,
        experiment_ids: List[str],
        id_mappings: Dict[str, Dict[str, str]]
    ) -> Tuple[int, Dict[str, str], int]:
        """
        Migrate runs for experiments using streaming.

        Args:
            experiment_ids: List of source experiment IDs to migrate runs for
            id_mappings: Dict containing "experiments" and "examples" mappings

        Returns:
            Tuple of (total_runs_migrated, run_id_mapping, failed_run_count)
            where run_id_mapping maps source run IDs to destination run IDs
        """
        run_id_mapping: Dict[str, str] = {}
        if self.state:
            run_id_mapping.update(self.state.id_mappings.get("run", {}))

        if self.config.migration.dry_run:
            self.log("[DRY RUN] Would migrate runs")
            return 0, run_id_mapping, 0

        experiment_mapping = id_mappings.get("experiments", {})
        example_mapping = id_mappings.get("examples", {})

        self.log(f"Starting run migration for {len(experiment_ids)} experiment(s)", "info")
        self.log(f"Experiment ID mapping: {experiment_mapping}", "info")

        total_runs = 0
        total_skipped = 0
        total_failed = 0

        # Query runs for EACH experiment separately
        # The LangSmith /runs/query API only processes the first session ID when given a list
        for exp_idx, experiment_id in enumerate(experiment_ids, 1):
            experiment_item_id = self._experiment_item_id(experiment_id)
            experiment_item = self.state.get_item(experiment_item_id) if self.state else None
            start_cursor = experiment_item.metadata.get("run_cursor") if experiment_item else None
            pending_run_mapping: Dict[str, str] = {}
            combined_mapping = ChainMap(pending_run_mapping, run_id_mapping)
            batch: List[Dict[str, Any]] = []
            experiment_runs_created = 0
            experiment_failed_runs = 0

            self.log(f"Fetching runs for experiment {exp_idx}/{len(experiment_ids)}: {experiment_id}", "info")

            payload = {
                "session": [experiment_id],  # Single ID in a list (API requires list format)
                "skip_pagination": False
            }
            if start_cursor:
                payload["cursor"] = start_cursor
                self.log(f"Resuming runs for experiment {experiment_id} from cursor {start_cursor}", "info")
            if experiment_item:
                self.checkpoint_item(
                    experiment_item_id,
                    stage="migrate_runs",
                    metadata={"run_cursor": start_cursor},
                )

            page_num = 0
            while True:
                page_num += 1
                try:
                    response = self.source.post("/runs/query", payload)
                except Exception as e:
                    self.log(f"Error querying runs for experiment {experiment_id}: {e}", "error")
                    break

                runs = response.get("runs", [])

                # Sort runs by dotted_order to ensure parents are processed before children
                # dotted_order format: {timestamp}Z{uuid}.{timestamp}Z{uuid}...
                # Shorter dotted_order = closer to root, so sorting alphabetically works
                # This prevents "dotted_order must contain a single part for root runs" errors
                # when a child run would otherwise be processed before its parent
                runs.sort(key=lambda r: r.get("dotted_order", ""))

                self.log(f"Experiment {experiment_id} page {page_num}: Retrieved {len(runs)} runs", "info")

                if not runs:
                    if page_num == 1:
                        self.log(f"No runs found for experiment {experiment_id}", "info")
                    break

                for run in runs:
                    source_session_id = run.get("session_id")
                    source_run_id = run.get("id")
                    if not source_run_id:
                        continue

                    # Map IDs
                    if source_session_id not in experiment_mapping:
                        self.log(
                            f"Skipping run {source_run_id} - session_id {source_session_id} not in experiment mapping",
                            "warning"
                        )
                        total_skipped += 1
                        continue

                    if source_run_id in run_id_mapping:
                        total_skipped += 1
                        continue

                    dest_session_id = experiment_mapping[source_session_id]

                    # Map parent_run_id if present and already migrated
                    parent_run_id = run.get("parent_run_id")
                    mapped_parent_id = (
                        self._deterministic_run_id(parent_run_id) if parent_run_id else None
                    )

                    # Map reference_example_id if present
                    source_example_id = run.get("reference_example_id")
                    mapped_example_id = None
                    if source_example_id:
                        mapped_example_id = example_mapping.get(source_example_id)
                        if not mapped_example_id:
                            self.log(
                                f"Warning: run references unmapped example {source_example_id}, preserving source ID",
                                "warning",
                            )
                            mapped_example_id = source_example_id

                    # Deterministic IDs make interrupted batches safe to replay.
                    new_run_id = self._deterministic_run_id(source_run_id)
                    source_trace_id = run.get("trace_id")
                    new_trace_id = (
                        self._deterministic_run_id(source_trace_id)
                        if source_trace_id
                        else new_run_id
                    )
                    pending_run_mapping[source_run_id] = new_run_id

                    # Regenerate dotted_order with new IDs
                    new_dotted_order = self._regenerate_dotted_order(
                        run.get("dotted_order"),
                        combined_mapping,
                        new_run_id
                    )

                    migrated_run = {
                        "id": new_run_id,
                        "name": run["name"],
                        "inputs": run.get("inputs"),
                        "outputs": run.get("outputs"),
                        "run_type": run["run_type"],
                        "start_time": run.get("start_time"),
                        "end_time": run.get("end_time"),
                        "extra": run.get("extra"),
                        "error": run.get("error"),
                        "serialized": run.get("serialized", {}),
                        "parent_run_id": mapped_parent_id,
                        "events": run.get("events", []),
                        "tags": run.get("tags", []),
                        "trace_id": new_trace_id,
                        "dotted_order": new_dotted_order,
                        "session_id": dest_session_id,
                        "reference_example_id": mapped_example_id,
                        "_source_run_id": source_run_id,
                    }

                    # Remove None values to avoid API validation errors (422)
                    migrated_run = {k: v for k, v in migrated_run.items() if v is not None}

                    batch.append(migrated_run)

                    # Process batch
                    if len(batch) >= self.config.migration.batch_size:
                        created_mapping, failed_runs = self._create_runs_batch(batch)
                        total_runs += len(created_mapping)
                        total_failed += len(failed_runs)
                        experiment_runs_created += len(created_mapping)
                        experiment_failed_runs += len(failed_runs)
                        if created_mapping:
                            run_id_mapping.update(created_mapping)
                            if self.state:
                                for old_id, new_id in created_mapping.items():
                                    self.state.set_mapped_id("run", old_id, new_id)
                                self.persist_state()
                            self.log(
                                f"Created batch of {len(created_mapping)} runs (total: {total_runs})",
                                "info",
                            )
                        if failed_runs:
                            failed_ids = {entry["source_run_id"] for entry in failed_runs}
                            for failed_id in failed_ids:
                                pending_run_mapping.pop(failed_id, None)
                            self.log(f"Failed to create {len(failed_runs)} run(s) in batch", "error")
                            if experiment_item:
                                issue = self.record_issue(
                                    "transient",
                                    "run_batch_failed",
                                    f"Run batch creation failed for experiment {experiment_id}",
                                    item_id=experiment_item_id,
                                    next_action="Re-run `langsmith-migrator resume` to replay the failed runs.",
                                    evidence={"failed_runs": failed_runs[:10], "failed_count": len(failed_runs)},
                                )
                                if issue:
                                    self.queue_remediation(
                                        issue_id=issue.id,
                                        next_action=issue.next_action or "Resume experiment runs.",
                                        item_id=experiment_item_id,
                                        command="langsmith-migrator resume",
                                    )
                        for created_id in created_mapping:
                            pending_run_mapping.pop(created_id, None)
                        batch.clear()

                # Check for next page
                cursors = response.get("cursors")
                next_cursor = cursors.get("next") if cursors else None

                if experiment_item:
                    self.checkpoint_item(
                        experiment_item_id,
                        stage="migrate_runs",
                        metadata={
                            "run_cursor": next_cursor,
                            "run_failures": experiment_failed_runs,
                            "runs_migrated": experiment_runs_created,
                        },
                    )
                if not next_cursor:
                    break

                payload["cursor"] = next_cursor
                self.log(f"Fetching next page with cursor: {next_cursor}", "info")

            if batch:
                created_mapping, failed_runs = self._create_runs_batch(batch)
                total_runs += len(created_mapping)
                total_failed += len(failed_runs)
                experiment_runs_created += len(created_mapping)
                experiment_failed_runs += len(failed_runs)
                if created_mapping:
                    run_id_mapping.update(created_mapping)
                    if self.state:
                        for old_id, new_id in created_mapping.items():
                            self.state.set_mapped_id("run", old_id, new_id)
                        self.persist_state()
                    self.log(f"Created final batch of {len(created_mapping)} runs", "info")
                if failed_runs:
                    self.log(f"Failed to create {len(failed_runs)} run(s) in final batch", "error")
                    if experiment_item:
                        issue = self.record_issue(
                            "transient",
                            "run_batch_failed",
                            f"Final run batch creation failed for experiment {experiment_id}",
                            item_id=experiment_item_id,
                            next_action="Re-run `langsmith-migrator resume` to replay the failed runs.",
                            evidence={"failed_runs": failed_runs[:10], "failed_count": len(failed_runs)},
                        )
                        if issue:
                            self.queue_remediation(
                                issue_id=issue.id,
                                next_action=issue.next_action or "Resume experiment runs.",
                                item_id=experiment_item_id,
                                command="langsmith-migrator resume",
                            )
                if experiment_item:
                    next_stage = "migrate_feedback" if experiment_failed_runs == 0 else "migrate_runs"
                    self.checkpoint_item(
                        experiment_item_id,
                        stage=next_stage,
                        metadata={
                            "run_cursor": None,
                            "run_failures": experiment_failed_runs,
                            "runs_migrated": experiment_runs_created,
                        },
                    )

        self.log(f"Run migration complete: {total_runs} migrated, {total_skipped} skipped", "success")
        return total_runs, run_id_mapping, total_failed

    def _create_runs_batch(
        self, runs: List[Dict[str, Any]]
    ) -> tuple[Dict[str, str], List[Dict[str, str]]]:
        """
        Create a batch of runs.

        Args:
            runs: List of run dictionaries to create

        Returns:
            Tuple of (created_mapping, failed_runs).
        """
        if not runs:
            return {}, []

        created_mapping: Dict[str, str] = {}
        failed_runs: List[Dict[str, str]] = []

        def strip_internal_fields(batch: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
            stripped = []
            for run in batch:
                stripped.append({k: v for k, v in run.items() if not k.startswith("_")})
            return stripped

        def post_recursive(batch: List[Dict[str, Any]]) -> None:
            payload = {"post": strip_internal_fields(batch)}
            self.log(f"Creating batch of {len(batch)} runs via /runs/batch", "info")
            try:
                response = self.dest.post("/runs/batch", payload)
                if isinstance(response, dict) and response.get("errors"):
                    raise ValueError(f"Batch creation had {len(response['errors'])} error(s)")

                for run in batch:
                    source_run_id = run.get("_source_run_id")
                    if source_run_id:
                        created_mapping[source_run_id] = run["id"]
            except Exception as e:
                if len(batch) == 1:
                    source_run_id = batch[0].get("_source_run_id", "unknown")
                    error_text = str(e)
                    if "409" in error_text or "Conflict" in error_text:
                        created_mapping[source_run_id] = batch[0]["id"]
                        self.log(
                            f"Run {source_run_id} already exists with deterministic ID; treating as replay success",
                            "warning",
                        )
                        return

                    failed_runs.append(
                        {"source_run_id": source_run_id, "error": error_text}
                    )
                    return

                midpoint = len(batch) // 2
                post_recursive(batch[:midpoint])
                post_recursive(batch[midpoint:])

        post_recursive(runs)
        return created_mapping, failed_runs
