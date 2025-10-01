"""Experiment migration logic."""

from typing import Dict, List, Any

from .base import BaseMigrator


class ExperimentMigrator(BaseMigrator):
    """Handles experiment and run migration."""

    def list_experiments(self, dataset_id: str) -> List[Dict[str, Any]]:
        """List experiments for a dataset."""
        experiments = []
        for experiment in self.source.get_paginated(
            "/sessions",
            params={"reference_dataset": dataset_id}
        ):
            if isinstance(experiment, dict):
                experiments.append(experiment)
        return experiments

    def create_experiment(self, experiment: Dict[str, Any], new_dataset_id: str) -> str:
        """Create experiment in destination."""
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create experiment: {experiment['name']}")
            return f"dry-run-{experiment['id']}"

        payload = {
            "name": experiment["name"],
            "description": experiment.get("description") or None,
            "reference_dataset_id": new_dataset_id,
            "start_time": experiment.get("start_time"),
            "end_time": experiment.get("end_time"),
            "extra": experiment.get("extra"),
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
