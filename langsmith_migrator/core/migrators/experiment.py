"""Experiment migration logic."""

from typing import Dict, List, Any, Optional
import copy

from .base import BaseMigrator


class ExperimentMigrator(BaseMigrator):
    """Handles experiment and run migration."""

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
        return response["id"]

    def migrate_runs_streaming(
        self,
        experiment_ids: List[str],
        id_mappings: Dict[str, Dict[str, str]]
    ) -> int:
        """Migrate runs for experiments using streaming."""
        if self.config.migration.dry_run:
            self.log("[DRY RUN] Would migrate runs")
            return 0

        experiment_mapping = id_mappings.get("experiments", {})
        example_mapping = id_mappings.get("examples", {})

        total_runs = 0
        batch = []

        # Query runs for all experiments
        payload = {
            "session": experiment_ids,
            "skip_pagination": False
        }

        while True:
            response = self.source.post("/runs/query", payload)
            runs = response.get("runs", [])

            for run in runs:
                # Map IDs
                if run.get("session_id") not in experiment_mapping:
                    continue

                migrated_run = {
                    "name": run["name"],
                    "inputs": run.get("inputs"),
                    "outputs": run.get("outputs"),
                    "run_type": run["run_type"],
                    "start_time": run.get("start_time"),
                    "end_time": run.get("end_time"),
                    "extra": run.get("extra"),
                    "error": run.get("error"),
                    "serialized": run.get("serialized", {}),
                    "parent_run_id": run.get("parent_run_id"),
                    "events": run.get("events", []),
                    "tags": run.get("tags", []),
                    "trace_id": run["trace_id"],
                    "id": run["id"],
                    "dotted_order": run.get("dotted_order"),
                    "session_id": experiment_mapping[run["session_id"]],
                    "reference_example_id": example_mapping.get(run.get("reference_example_id"))
                }

                batch.append(migrated_run)

                # Process batch
                if len(batch) >= self.config.migration.batch_size:
                    self._create_runs_batch(batch)
                    total_runs += len(batch)
                    batch.clear()

            # Check for next page
            cursors = response.get("cursors")
            next_cursor = cursors.get("next") if cursors else None
            if not next_cursor:
                break

            payload["cursor"] = next_cursor

        # Process remaining runs
        if batch:
            self._create_runs_batch(batch)
            total_runs += len(batch)

        self.log(f"Migrated {total_runs} runs", "success")
        return total_runs

    def _create_runs_batch(self, runs: List[Dict[str, Any]]):
        """Create a batch of runs."""
        if not runs:
            return

        payload = {"post": runs}
        self.dest.post("/runs/batch", payload)
