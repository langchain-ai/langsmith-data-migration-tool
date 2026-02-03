"""Experiment migration logic."""

from typing import Dict, List, Any, Optional, Tuple
import copy
import uuid

from .base import BaseMigrator


class ExperimentMigrator(BaseMigrator):
    """Handles experiment and run migration."""

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
                new_uuid = id_mapping.get(old_uuid, old_uuid)
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
                        import json
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
                            import json
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
                                evaluator['feedback_key'] = f"evaluator_{hash(str(evaluator))}"
                            self.log(f"Warning: Evaluator missing feedback_key, generated: {evaluator['feedback_key']}", "warning")
                            import json
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
    ) -> Tuple[int, Dict[str, str]]:
        """
        Migrate runs for experiments using streaming.

        Args:
            experiment_ids: List of source experiment IDs to migrate runs for
            id_mappings: Dict containing "experiments" and "examples" mappings

        Returns:
            Tuple of (total_runs_migrated, run_id_mapping)
            where run_id_mapping maps source run IDs to destination run IDs
        """
        run_id_mapping: Dict[str, str] = {}
        # Track trace_id mappings separately (trace_id is the root run's ID)
        # All runs in the same trace share the same trace_id
        trace_id_mapping: Dict[str, str] = {}

        if self.config.migration.dry_run:
            self.log("[DRY RUN] Would migrate runs")
            return 0, run_id_mapping

        experiment_mapping = id_mappings.get("experiments", {})
        example_mapping = id_mappings.get("examples", {})

        self.log(f"Starting run migration for {len(experiment_ids)} experiment(s)", "info")
        self.log(f"Experiment ID mapping: {experiment_mapping}", "info")

        total_runs = 0
        total_skipped = 0
        batch = []

        # Query runs for EACH experiment separately
        # The LangSmith /runs/query API only processes the first session ID when given a list
        for exp_idx, experiment_id in enumerate(experiment_ids, 1):
            self.log(f"Fetching runs for experiment {exp_idx}/{len(experiment_ids)}: {experiment_id}", "info")

            payload = {
                "session": [experiment_id],  # Single ID in a list (API requires list format)
                "skip_pagination": False
            }

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

                    # Map IDs
                    if source_session_id not in experiment_mapping:
                        self.log(
                            f"Skipping run {source_run_id} - session_id {source_session_id} not in experiment mapping",
                            "warning"
                        )
                        total_skipped += 1
                        continue

                    dest_session_id = experiment_mapping[source_session_id]

                    # Map parent_run_id if present and already migrated
                    # If parent not mapped yet, omit the field entirely to avoid 422 errors
                    parent_run_id = run.get("parent_run_id")
                    mapped_parent_id = None
                    if parent_run_id and parent_run_id in run_id_mapping:
                        mapped_parent_id = run_id_mapping[parent_run_id]

                    # Map reference_example_id if present
                    source_example_id = run.get("reference_example_id")
                    mapped_example_id = example_mapping.get(source_example_id) if source_example_id else None

                    # Generate a new UUID for the destination run to avoid conflicts
                    new_run_id = str(uuid.uuid4())

                    # Handle trace_id mapping
                    # trace_id identifies the root run of a trace - all runs in the same trace share it
                    # API requirement: For root runs, trace_id MUST equal run_id
                    # For child runs, trace_id is the root run's ID (shared across the trace)
                    source_trace_id = run.get("trace_id")

                    if not parent_run_id:
                        # Root run: trace_id = run_id (API requirement)
                        # Store mapping so children can look up the new trace_id
                        new_trace_id = new_run_id
                        if source_trace_id:
                            trace_id_mapping[source_trace_id] = new_run_id
                    else:
                        # Child run: use mapped trace_id from root
                        if source_trace_id and source_trace_id in trace_id_mapping:
                            new_trace_id = trace_id_mapping[source_trace_id]
                        else:
                            # Fallback: use run's own ID (shouldn't happen with sorted runs)
                            new_trace_id = new_run_id

                    # Store the run ID mapping before regenerating dotted_order
                    run_id_mapping[source_run_id] = new_run_id

                    # Build combined mapping for dotted_order regeneration
                    # (includes both run IDs and trace IDs)
                    combined_mapping = {**run_id_mapping, **trace_id_mapping}

                    # Regenerate dotted_order with new IDs
                    # Pass new_run_id to ensure the last part always matches the run's ID
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
                    }

                    # Remove None values to avoid API validation errors (422)
                    migrated_run = {k: v for k, v in migrated_run.items() if v is not None}

                    batch.append(migrated_run)

                    # Process batch
                    if len(batch) >= self.config.migration.batch_size:
                        success, created = self._create_runs_batch(batch)
                        total_runs += created
                        if success:
                            self.log(f"Created batch of {created} runs (total: {total_runs})", "info")
                        else:
                            self.log(f"Failed to create batch of {len(batch)} runs", "error")
                        batch.clear()

                # Check for next page
                cursors = response.get("cursors")
                next_cursor = cursors.get("next") if cursors else None

                if not next_cursor:
                    break

                payload["cursor"] = next_cursor
                self.log(f"Fetching next page with cursor: {next_cursor}", "info")

        # Process remaining runs
        if batch:
            success, created = self._create_runs_batch(batch)
            total_runs += created
            if success:
                self.log(f"Created final batch of {created} runs", "info")
            else:
                self.log(f"Failed to create final batch of {len(batch)} runs", "error")

        self.log(f"Run migration complete: {total_runs} migrated, {total_skipped} skipped", "success")
        return total_runs, run_id_mapping

    def _create_runs_batch(self, runs: List[Dict[str, Any]]) -> tuple[bool, int]:
        """
        Create a batch of runs.

        Args:
            runs: List of run dictionaries to create

        Returns:
            Tuple of (success, count_created) - success is True if at least some runs were created
        """
        if not runs:
            return True, 0

        payload = {"post": runs}
        self.log(f"Creating batch of {len(runs)} runs via /runs/batch", "info")

        try:
            response = self.dest.post("/runs/batch", payload)

            # Validate response
            if isinstance(response, dict):
                # The /runs/batch endpoint typically returns summary info
                # Check for any error indicators
                if response.get("errors"):
                    error_count = len(response["errors"])
                    self.log(f"Batch creation had {error_count} error(s)", "warning")
                    for error in response["errors"][:3]:
                        self.log(f"  Error: {error}", "warning")
                    return error_count < len(runs), len(runs) - error_count

                if self.config.migration.verbose:
                    self.log(f"Batch creation response: {response}", "info")
                return True, len(runs)

            elif isinstance(response, list):
                # Some versions return list of created runs
                return True, len(response)
            else:
                self.log(f"Unexpected response type from /runs/batch: {type(response)}", "warning")
                return True, len(runs)  # Assume success if no error raised

        except Exception as e:
            self.log(f"Error creating runs batch: {e}", "error")
            # Log the first run for debugging
            if runs and self.config.migration.verbose:
                import json
                self.log(f"First run in failed batch: {json.dumps(runs[0], default=str)[:500]}", "error")
            return False, 0
