"""Migration orchestrator for coordinating migration operations."""

import threading
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.console import Console
from rich.progress import Progress

from ..api_client import EnhancedAPIClient
from ...utils.state import MigrationItem, MigrationStatus, StateManager
from .dataset import DatasetMigrator
from .experiment import ExperimentMigrator
from .feedback import FeedbackMigrator


class MigrationOrchestrator:
    """Orchestrates the entire migration process."""

    def __init__(self, config, state_manager: StateManager):
        """Initialize the orchestrator."""
        self.config = config
        self.state_manager = state_manager
        self.console = Console()

        # Thread lock for protecting shared state during parallel migrations
        self._state_lock = threading.Lock()

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
        source_ok, source_error = self.source_client.test_connection()
        dest_ok, dest_error = self.dest_client.test_connection()

        if not source_ok and self.config.migration.verbose:
            self.console.print(f"[dim]Source connection failed: {source_error}[/dim]")

        if not dest_ok and self.config.migration.verbose:
            self.console.print(f"[dim]Destination connection failed: {dest_error}[/dim]")

        return source_ok and dest_ok

    def test_connections_detailed(self) -> tuple[bool, bool, Optional[str], Optional[str]]:
        """
        Test connections and return detailed results.

        Returns:
            Tuple of (source_ok, dest_ok, source_error, dest_error)
        """
        source_ok, source_error = self.source_client.test_connection()
        dest_ok, dest_error = self.dest_client.test_connection()
        return source_ok, dest_ok, source_error, dest_error

    def migrate_datasets_parallel(
        self,
        dataset_ids: List[str],
        include_examples: bool = True,
        include_experiments: bool = False
    ) -> Dict[str, str]:
        """Migrate multiple datasets in parallel."""
        # Create or load state
        if not self.state:
            self.state = self.state_manager.create_session(
                self.config.source.base_url,
                self.config.destination.base_url
            )

        # Add items to state
        dataset_migrator = DatasetMigrator(
            self.source_client,
            self.dest_client,
            self.state,
            self.config
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

                        # Thread-safe update of shared state
                        with self._state_lock:
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
                        error_msg = str(e)
                        # Provide helpful hint for SSL errors
                        if "SSL" in error_msg or "certificate verify failed" in error_msg:
                            self.console.print(f"[red]Failed to migrate dataset {dataset_id}:[/red]")
                            self.console.print("[red]SSL certificate verification failed. Use --no-ssl flag to disable SSL verification.[/red]")
                        else:
                            self.console.print(f"[red]Failed to migrate dataset {dataset_id}: {e}[/red]")

                        # Thread-safe state update
                        with self._state_lock:
                            self.state.update_item_status(
                                item_id,
                                MigrationStatus.FAILED,
                                error=str(e)
                            )

                    progress.advance(task)

                    # Thread-safe save
                    with self._state_lock:
                        self.state_manager.save()

        # Migrate experiments if requested
        if include_experiments and id_mapping:
            self.console.print("\n[bold]Migrating experiments...[/bold]")
            self._migrate_experiments_for_datasets(dataset_ids, id_mapping)

        return id_mapping

    def _migrate_experiments_for_datasets(
        self,
        dataset_ids: List[str],
        dataset_id_mapping: Dict[str, str]
    ):
        """Migrate experiments for the given datasets."""
        experiment_migrator = ExperimentMigrator(
            self.source_client,
            self.dest_client,
            self.state,
            self.config
        )

        all_experiments = []
        experiment_to_dataset = {}

        # Collect all experiments for these datasets
        for dataset_id in dataset_ids:
            experiments = experiment_migrator.list_experiments(dataset_id)
            for exp in experiments:
                all_experiments.append(exp)
                experiment_to_dataset[exp['id']] = dataset_id

        if not all_experiments:
            self.console.print("[dim]No experiments found for selected datasets[/dim]")
            return

        self.console.print(f"Found {len(all_experiments)} experiment(s)")

        # Create experiments in destination
        experiment_id_mapping = {}
        for experiment in all_experiments:
            source_dataset_id = experiment_to_dataset[experiment['id']]
            dest_dataset_id = dataset_id_mapping.get(source_dataset_id)

            if not dest_dataset_id:
                self.console.print(f"[yellow]Skipping experiment {experiment['name']} - dataset not migrated[/yellow]")
                continue

            try:
                # Add to state
                item = MigrationItem(
                    id=f"experiment_{experiment['id']}",
                    type="experiment",
                    name=experiment['name'],
                    source_id=experiment['id']
                )
                self.state.add_item(item)
                self.state.update_item_status(item.id, MigrationStatus.IN_PROGRESS)

                new_exp_id = experiment_migrator.create_experiment(experiment, dest_dataset_id)
                experiment_id_mapping[experiment['id']] = new_exp_id

                self.state.update_item_status(
                    item.id,
                    MigrationStatus.COMPLETED,
                    destination_id=new_exp_id
                )
                self.console.print(f"[green]✓[/green] Migrated experiment: {experiment['name']}")

            except Exception as e:
                self.console.print(f"[red]✗[/red] Failed to migrate experiment {experiment['name']}: {e}")
                self.state.update_item_status(
                    item.id,
                    MigrationStatus.FAILED,
                    error=str(e)
                )

        # Migrate runs if experiments were created
        run_id_mapping = {}
        if experiment_id_mapping:
            self.console.print("\n[bold]Migrating experiment runs...[/bold]")
            try:
                total_runs, run_id_mapping = experiment_migrator.migrate_runs_streaming(
                    list(experiment_id_mapping.keys()),
                    {
                        "experiments": experiment_id_mapping,
                        "examples": self.state.id_mappings.get("examples", {})
                    }
                )
                self.console.print(f"[green]✓[/green] Migrated {total_runs} run(s)")
            except Exception as e:
                self.console.print(f"[red]✗[/red] Failed to migrate runs: {e}")

        # Migrate feedback if experiments and runs were created
        if experiment_id_mapping and run_id_mapping:
            self.console.print("\n[bold]Migrating experiment feedback...[/bold]")
            try:
                feedback_migrator = FeedbackMigrator(
                    self.source_client,
                    self.dest_client,
                    self.state,
                    self.config
                )
                total_found, total_migrated = feedback_migrator.migrate_feedback_for_experiments(
                    experiment_id_mapping,
                    run_id_mapping
                )
                if total_found > 0:
                    self.console.print(f"[green]✓[/green] Migrated {total_migrated}/{total_found} feedback record(s)")
                else:
                    self.console.print("[dim]No feedback records found[/dim]")
            except Exception as e:
                self.console.print(f"[red]✗[/red] Failed to migrate feedback: {e}")

    def cleanup(self):
        """Clean up resources."""
        self.source_client.close()
        self.dest_client.close()
