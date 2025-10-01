"""Migration orchestrator for coordinating migration operations."""

from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.console import Console
from rich.progress import Progress

from ..api_client import EnhancedAPIClient
from ...utils.state import MigrationState, MigrationItem, MigrationStatus, StateManager
from .dataset import DatasetMigrator
from .experiment import ExperimentMigrator


class MigrationOrchestrator:
    """Orchestrates the entire migration process."""

    def __init__(self, config, state_manager: StateManager):
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

    def test_connections_detailed(self) -> tuple[bool, bool]:
        """Test connections and return detailed results."""
        source_ok = self.source_client.test_connection()
        dest_ok = self.dest_client.test_connection()
        return source_ok, dest_ok

    def migrate_datasets_parallel(
        self,
        dataset_ids: List[str],
        include_examples: bool = True
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
                            self.console.print(f"[red]SSL certificate verification failed. Use --no-ssl flag to disable SSL verification.[/red]")
                        else:
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
