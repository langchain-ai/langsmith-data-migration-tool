"""Core migration logic with improved architecture and performance."""

from typing import Dict, List, Any, Optional, Generator, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.console import Console
from rich.progress import Progress, TaskID
import time

from ..core.api_client import EnhancedAPIClient, APIError, NotFoundError
from ..utils.state import MigrationState, MigrationItem, MigrationStatus
from ..cli.interactive_selector import ProgressTracker


class BaseMigrator:
    """Base class for all migrators."""
    
    def __init__(self, source_client: EnhancedAPIClient, dest_client: EnhancedAPIClient,
                 state: MigrationState, config: Any):
        """Initialize base migrator."""
        self.source = source_client
        self.dest = dest_client
        self.state = state
        self.config = config
        self.console = Console()
        self.progress_tracker = ProgressTracker(self.console)
    
    def log(self, message: str, level: str = "info"):
        """Log a message if verbose mode is enabled."""
        if self.config.migration.verbose:
            style = {
                "info": "dim",
                "success": "green",
                "warning": "yellow",
                "error": "red"
            }.get(level, "")
            
            self.console.print(f"[{style}]{message}[/{style}]")


class DatasetMigrator(BaseMigrator):
    """Handles dataset migration with streaming and batching."""
    
    def list_datasets(self) -> List[Dict[str, Any]]:
        """List all datasets from source."""
        datasets = []
        for dataset in self.source.get_paginated("/datasets"):
            datasets.append(dataset)
        return datasets
    
    def get_dataset(self, dataset_id: str) -> Dict[str, Any]:
        """Get a specific dataset."""
        return self.source.get(f"/datasets/{dataset_id}")
    
    def find_existing_dataset(self, name: str) -> Optional[str]:
        """Check if dataset already exists in destination."""
        try:
            response = self.dest.get("/datasets", params={"name": name})
            datasets = response if isinstance(response, list) else []
            
            if len(datasets) == 1:
                return datasets[0]["id"]
            elif len(datasets) > 1:
                self.log(f"Multiple datasets found with name '{name}'", "warning")
        except NotFoundError:
            pass
        
        return None
    
    def create_dataset(self, dataset: Dict[str, Any]) -> str:
        """Create dataset in destination."""
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create dataset: {dataset['name']}")
            return f"dry-run-{dataset['id']}"
        
        payload = {
            "name": dataset["name"],
            "description": dataset.get("description", ""),
            "created_at": dataset.get("created_at"),
            "inputs_schema_definition": dataset.get("inputs_schema_definition"),
            "outputs_schema_definition": dataset.get("outputs_schema_definition"),
            "externally_managed": dataset.get("externally_managed", False),
            "transformations": dataset.get("transformations") or [],
            "data_type": dataset.get("data_type", "kv")
        }
        
        response = self.dest.post("/datasets", payload)
        return response["id"]
    
    def stream_examples(self, dataset_id: str) -> Generator[Dict[str, Any], None, None]:
        """Stream examples from a dataset without loading all into memory."""
        for example in self.source.get_paginated("/examples", params={"dataset": dataset_id}):
            yield example
    
    def migrate_examples_streaming(self, source_dataset_id: str, dest_dataset_id: str,
                                  progress_callback=None) -> Dict[str, str]:
        """Migrate examples using streaming to avoid memory issues."""
        if self.config.migration.dry_run:
            self.log("[DRY RUN] Would migrate examples")
            return {}
        
        id_mapping = {}
        batch = []
        batch_count = 0
        total_migrated = 0
        
        for example in self.stream_examples(source_dataset_id):
            # Prepare example for destination
            migrated_example = {
                "dataset_id": dest_dataset_id,
                "inputs": example.get("inputs", {}),
                "outputs": example.get("outputs", {}),
                "metadata": example.get("metadata", {}),
                "created_at": example.get("created_at"),
                "split": (example.get("metadata") or {}).get("dataset_split", "base")
            }
            
            batch.append((example["id"], migrated_example))
            
            # Process batch when it reaches configured size
            if len(batch) >= self.config.migration.batch_size:
                batch_count += 1
                self.log(f"Processing batch {batch_count} ({len(batch)} examples)")
                
                # Create examples in batch
                payloads = [ex[1] for ex in batch]
                responses = self.dest.post_batch("/examples/bulk", payloads, 
                                                batch_size=self.config.migration.batch_size)
                
                # Update ID mappings
                for i, (original_id, _) in enumerate(batch):
                    if responses[i]:
                        id_mapping[original_id] = responses[i].get("id")
                        total_migrated += 1
                
                if progress_callback:
                    progress_callback(total_migrated)
                
                batch.clear()
        
        # Process remaining examples
        if batch:
            payloads = [ex[1] for ex in batch]
            responses = self.dest.post_batch("/examples/bulk", payloads,
                                            batch_size=self.config.migration.batch_size)
            
            for i, (original_id, _) in enumerate(batch):
                if responses[i]:
                    id_mapping[original_id] = responses[i].get("id")
                    total_migrated += 1
            
            if progress_callback:
                progress_callback(total_migrated)
        
        self.log(f"Migrated {total_migrated} examples", "success")
        return id_mapping
    
    def migrate_dataset(self, dataset_id: str, include_examples: bool = True) -> Tuple[str, Dict[str, str]]:
        """
        Migrate a single dataset.
        
        Returns:
            Tuple of (new_dataset_id, example_id_mapping)
        """
        # Get dataset details
        dataset = self.get_dataset(dataset_id)
        
        # Check if already exists
        if self.config.migration.skip_existing:
            existing_id = self.find_existing_dataset(dataset["name"])
            if existing_id:
                self.log(f"Dataset '{dataset['name']}' already exists, skipping", "warning")
                return existing_id, {}
        
        # Create dataset
        new_dataset_id = self.create_dataset(dataset)
        self.log(f"Created dataset: {dataset['name']} -> {new_dataset_id}", "success")
        
        # Migrate examples if requested
        example_mapping = {}
        if include_examples:
            example_mapping = self.migrate_examples_streaming(dataset_id, new_dataset_id)
        
        return new_dataset_id, example_mapping


class ExperimentMigrator(BaseMigrator):
    """Handles experiment and run migration."""
    
    def list_experiments(self, dataset_id: str) -> List[Dict[str, Any]]:
        """List experiments for a dataset."""
        experiments = []
        for experiment in self.source.get_paginated("/sessions", 
                                                   params={"reference_dataset": dataset_id}):
            experiments.append(experiment)
        return experiments
    
    def create_experiment(self, experiment: Dict[str, Any], new_dataset_id: str) -> str:
        """Create experiment in destination."""
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create experiment: {experiment['name']}")
            return f"dry-run-{experiment['id']}"
        
        payload = {
            "name": experiment["name"],
            "description": experiment.get("description"),
            "reference_dataset_id": new_dataset_id,
            "start_time": experiment.get("start_time"),
            "end_time": experiment.get("end_time"),
            "extra": experiment.get("extra"),
            "trace_tier": experiment.get("trace_tier")
        }
        
        response = self.dest.post("/sessions", payload)
        return response["id"]
    
    def migrate_runs_streaming(self, experiment_ids: List[str], 
                              id_mappings: Dict[str, Dict[str, str]]) -> int:
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
            next_cursor = response.get("cursors", {}).get("next")
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


class AnnotationQueueMigrator(BaseMigrator):
    """Handles annotation queue migration."""
    
    def list_queues(self) -> List[Dict[str, Any]]:
        """List all annotation queues."""
        queues = []
        for queue in self.source.get_paginated("/annotation-queues"):
            queues.append(queue)
        return queues
    
    def get_queue(self, queue_id: str) -> Dict[str, Any]:
        """Get a specific annotation queue."""
        return self.source.get(f"/annotation-queues/{queue_id}")
    
    def create_queue(self, queue: Dict[str, Any], default_dataset_id: Optional[str] = None) -> str:
        """Create annotation queue in destination."""
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create annotation queue: {queue['name']}")
            return f"dry-run-{queue['id']}"
        
        payload = {
            "name": queue["name"],
            "description": queue.get("description"),
            "created_at": queue.get("created_at"),
            "updated_at": queue.get("updated_at"),
            "default_dataset": default_dataset_id,
            "num_reviewers_per_item": queue.get("num_reviewers_per_item", 1),
            "enable_reservations": queue.get("enable_reservations", False),
            "reservation_minutes": queue.get("reservation_minutes", 60),
            "rubric_items": queue.get("rubric_items", []),
            "rubric_instructions": queue.get("rubric_instructions"),
            "session_ids": []
        }
        
        response = self.dest.post("/annotation-queues", payload)
        return response["id"]


class PromptMigrator(BaseMigrator):
    """Handles prompt migration."""
    
    def list_prompts(self) -> List[Dict[str, Any]]:
        """List all prompts."""
        try:
            response = self.source.get("/prompts")
            return response if isinstance(response, list) else []
        except NotFoundError:
            return []
    
    def migrate_prompt(self, prompt_id: str):
        """Migrate a single prompt."""
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would migrate prompt: {prompt_id}")
            return
        
        # Note: This would need the LangSmith client for prompt operations
        # For now, we'll use a placeholder
        self.log(f"Prompt migration requires LangSmith client: {prompt_id}", "warning")


class MigrationOrchestrator:
    """Orchestrates the entire migration process."""
    
    def __init__(self, config, state_manager):
        """Initialize the orchestrator."""
        self.config = config
        self.state_manager = state_manager
        self.console = Console()
        
        # Initialize API clients
        self.source_client = EnhancedAPIClient(
            base_url=self._prepare_base_url(config.source.base_url),
            headers={"X-API-Key": config.source.api_key},
            verify_ssl=config.source.verify_ssl,
            timeout=config.source.timeout,
            max_retries=config.source.max_retries,
            rate_limit_delay=config.migration.rate_limit_delay,
            verbose=config.migration.verbose
        )
        
        self.dest_client = EnhancedAPIClient(
            base_url=self._prepare_base_url(config.destination.base_url),
            headers={"X-API-Key": config.destination.api_key},
            verify_ssl=config.destination.verify_ssl,
            timeout=config.destination.timeout,
            max_retries=config.destination.max_retries,
            rate_limit_delay=config.migration.rate_limit_delay,
            verbose=config.migration.verbose
        )
        
        # Initialize state
        self.state = None
    
    def _prepare_base_url(self, base_url: str) -> str:
        """Prepare base URL for API client."""
        clean_url = base_url.rstrip('/')
        if not clean_url.endswith('/api/v1'):
            clean_url = f"{clean_url}/api/v1"
        return clean_url
    
    def test_connections(self) -> bool:
        """Test connections to both source and destination."""
        source_ok = self.source_client.test_connection()
        dest_ok = self.dest_client.test_connection()
        
        if not source_ok and self.config.migration.verbose:
            self.console.print("[dim]Source connection failed[/dim]")
        
        if not dest_ok and self.config.migration.verbose:
            self.console.print("[dim]Destination connection failed[/dim]")
        
        return source_ok and dest_ok
    
    def migrate_datasets_parallel(self, dataset_ids: List[str], include_examples: bool = True) -> Dict[str, str]:
        """Migrate multiple datasets in parallel."""
        # Create or load state
        if not self.state:
            self.state = self.state_manager.create_session(
                self.config.source.base_url,
                self.config.destination.base_url
            )
        
        # Add items to state
        dataset_migrator = DatasetMigrator(
            self.source_client, self.dest_client, 
            self.state, self.config
        )
        
        for dataset_id in dataset_ids:
            dataset = dataset_migrator.get_dataset(dataset_id)
            item = MigrationItem(
                id=f"dataset_{dataset_id}",
                type="dataset",
                name=dataset["name"],
                source_id=dataset_id
            )
            self.state.add_item(item)
        
        # Migrate with concurrency
        id_mapping = {}
        
        with ThreadPoolExecutor(max_workers=self.config.migration.concurrent_workers) as executor:
            futures = {}
            
            for dataset_id in dataset_ids:
                future = executor.submit(
                    dataset_migrator.migrate_dataset,
                    dataset_id,
                    include_examples
                )
                futures[future] = dataset_id
            
            # Process completed migrations
            with Progress(console=self.console) as progress:
                task = progress.add_task("Migrating datasets...", total=len(dataset_ids))
                
                for future in as_completed(futures):
                    dataset_id = futures[future]
                    item_id = f"dataset_{dataset_id}"
                    
                    try:
                        new_id, example_mapping = future.result()
                        id_mapping[dataset_id] = new_id
                        
                        # Update state
                        self.state.update_item_status(
                            item_id, 
                            MigrationStatus.COMPLETED,
                            destination_id=new_id
                        )
                        
                        # Store example mappings
                        if example_mapping:
                            if "examples" not in self.state.id_mappings:
                                self.state.id_mappings["examples"] = {}
                            self.state.id_mappings["examples"].update(example_mapping)
                        
                    except Exception as e:
                        self.console.print(f"[red]Failed to migrate dataset {dataset_id}: {e}[/red]")
                        self.state.update_item_status(
                            item_id,
                            MigrationStatus.FAILED,
                            error=str(e)
                        )
                    
                    progress.advance(task)
                    self.state_manager.save()
        
        return id_mapping
    
    def cleanup(self):
        """Clean up resources."""
        self.source_client.close()
        self.dest_client.close()