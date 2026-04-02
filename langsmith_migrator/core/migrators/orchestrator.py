"""Migration orchestrator for coordinating migration operations."""

import threading
from typing import Dict, List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from rich.console import Console
from rich.progress import Progress

from ..api_client import EnhancedAPIClient
from ...utils.state import (
    MigrationItem,
    MigrationStatus,
    ResolutionOutcome,
    StateManager,
    VerificationState,
)
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
        self.config.state_manager = state_manager

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

    def ensure_state(self):
        """Create a session state if one does not already exist."""
        if not self.state:
            self.state = self.state_manager.create_session(
                self.config.source.base_url,
                self.config.destination.base_url,
            )
        elif not self.state.remediation_bundle_path:
            self.state.remediation_bundle_path = str(
                self.state_manager._default_bundle_path(self.state.session_id).resolve()
            )
        self.state_manager.current_state = self.state
        return self.state

    def workspace_pair(self) -> Dict[str, Optional[str]]:
        """Return the active workspace pair for the orchestrator."""
        return {
            "source": self.source_client.session.headers.get("X-Tenant-Id"),
            "dest": self.dest_client.session.headers.get("X-Tenant-Id"),
        }

    def migrate_datasets_parallel(
        self,
        dataset_ids: List[str],
        include_examples: bool = True,
        include_experiments: bool = False
    ) -> Dict[str, str]:
        """Migrate multiple datasets in parallel."""
        self.ensure_state()

        # Add items to state
        dataset_migrator = DatasetMigrator(
            self.source_client,
            self.dest_client,
            self.state,
            self.config
        )

        for dataset_id in dataset_ids:
            dataset = dataset_migrator.get_dataset(dataset_id)
            item = self.state.ensure_item(
                f"dataset_{dataset_id}",
                "dataset",
                dataset["name"],
                dataset_id,
                stage="create_dataset",
                workspace_pair=self.workspace_pair(),
            )
            self.state.update_item_checkpoint(
                item.id,
                metadata={"include_examples": include_examples},
            )
        self.state_manager.save()

        # Migrate with concurrency
        id_mapping = {}

        with ThreadPoolExecutor(max_workers=self.config.migration.concurrent_workers) as executor:
            futures = {}

            for dataset_id in dataset_ids:
                self.state.update_item_status(
                    f"dataset_{dataset_id}",
                    MigrationStatus.IN_PROGRESS,
                    stage="create_dataset",
                )
                future = executor.submit(
                    dataset_migrator.migrate_dataset,
                    dataset_id,
                    include_examples
                )
                futures[future] = dataset_id
            self.state_manager.save()

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
                                destination_id=new_id,
                                stage="completed",
                            )
                            self.state.mark_terminal(
                                item_id,
                                ResolutionOutcome.MIGRATED,
                                "dataset_migrated",
                                verification_state=VerificationState.VERIFIED,
                                evidence={"include_examples": include_examples},
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
                            issue = self.state.add_issue(
                                "transient",
                                "dataset_migration_failed",
                                f"Dataset migration failed for {dataset_id}",
                                item_id=item_id,
                                next_action="Re-run `langsmith-migrator resume` after reviewing the error.",
                                evidence={"error": str(e)},
                                workspace_pair=self.workspace_pair(),
                            )
                            self.state.queue_remediation(
                                issue_id=issue.id,
                                item_id=item_id,
                                next_action=issue.next_action or "Resume dataset migration.",
                                command="langsmith-migrator resume",
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
        self.ensure_state()
        experiment_migrator = ExperimentMigrator(
            self.source_client,
            self.dest_client,
            self.state,
            self.config
        )
        feedback_migrator = FeedbackMigrator(
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
        success_count = 0
        failed_items = []
        skipped_items = []

        with Progress(console=self.console) as progress:
            task = progress.add_task("Migrating experiments...", total=len(all_experiments))

            for experiment in all_experiments:
                source_experiment_id = experiment["id"]
                source_dataset_id = experiment_to_dataset[source_experiment_id]
                dest_dataset_id = dataset_id_mapping.get(source_dataset_id)
                item_id = f"experiment_{source_experiment_id}"
                item = self.state.ensure_item(
                    item_id,
                    "experiment",
                    experiment["name"],
                    source_experiment_id,
                    stage="create_experiment",
                    workspace_pair=self.workspace_pair(),
                    metadata={
                        "source_dataset_id": source_dataset_id,
                        "dest_dataset_id": dest_dataset_id,
                    },
                )

                if item.terminal_state == ResolutionOutcome.MIGRATED.value:
                    success_count += 1
                    progress.advance(task)
                    continue

                if not dest_dataset_id:
                    skipped_items.append((experiment["name"], "dataset not migrated"))
                    self.state.mark_terminal(
                        item_id,
                        ResolutionOutcome.BLOCKED_WITH_CHECKPOINT,
                        "missing_dataset_dependency",
                        verification_state=VerificationState.BLOCKED,
                        next_action="Migrate the referenced dataset, then run `langsmith-migrator resume`.",
                        evidence={"source_dataset_id": source_dataset_id},
                        error="dataset not migrated",
                    )
                    issue = self.state.add_issue(
                        "dependency",
                        "missing_dataset_dependency",
                        f"Experiment {experiment['name']} depends on dataset {source_dataset_id}",
                        item_id=item_id,
                        next_action="Migrate the dataset and re-run resume.",
                        evidence={"source_dataset_id": source_dataset_id},
                        workspace_pair=self.workspace_pair(),
                    )
                    self.state.queue_remediation(
                        issue_id=issue.id,
                        item_id=item_id,
                        next_action=issue.next_action or "Migrate dataset dependency.",
                        command="langsmith-migrator resume",
                    )
                    self.state_manager.save()
                    progress.advance(task)
                    continue

                success, detail = self._resolve_experiment_item(
                    experiment,
                    source_dataset_id,
                    dest_dataset_id,
                    experiment_migrator,
                    feedback_migrator,
                )
                if success:
                    success_count += 1
                else:
                    failed_items.append((experiment["name"], detail))

                progress.advance(task)
                self.state_manager.save()

        # Summary
        self.console.print(f"Experiments: {success_count} migrated, {len(skipped_items)} skipped, {len(failed_items)} failed")
        if (failed_items or skipped_items) and self.config.migration.verbose:
            for name, err in skipped_items:
                self.console.print(f"  [yellow]⊘[/yellow] {name}: {err}")
            for name, err in failed_items:
                self.console.print(f"  [red]✗[/red] {name}: {err}")

    def _resolve_experiment_item(
        self,
        experiment: Dict[str, str],
        source_dataset_id: str,
        dest_dataset_id: str,
        experiment_migrator: ExperimentMigrator,
        feedback_migrator: FeedbackMigrator,
    ) -> tuple[bool, str]:
        """Resolve experiment creation, run replay, and feedback replay."""
        source_experiment_id = experiment["id"]
        item_id = f"experiment_{source_experiment_id}"
        item = self.state.get_item(item_id)
        if item is None:
            return False, "missing experiment state item"

        dest_experiment_id = item.destination_id or item.metadata.get("destination_experiment_id")

        try:
            if not dest_experiment_id:
                self.state.update_item_status(
                    item_id,
                    MigrationStatus.IN_PROGRESS,
                    stage="create_experiment",
                )
                self.state.update_item_checkpoint(
                    item_id,
                    metadata={
                        "source_dataset_id": source_dataset_id,
                        "dest_dataset_id": dest_dataset_id,
                    },
                )
                self.state_manager.save()
                dest_experiment_id = experiment_migrator.create_experiment(
                    experiment,
                    dest_dataset_id,
                )
                self.state.update_item_status(
                    item_id,
                    MigrationStatus.IN_PROGRESS,
                    destination_id=dest_experiment_id,
                    stage="migrate_runs",
                )
                self.state.update_item_checkpoint(
                    item_id,
                    metadata={"destination_experiment_id": dest_experiment_id},
                )
                self.state_manager.save()

            current_item = self.state.get_item(item_id)
            if current_item and current_item.stage in {"pending", "create_experiment", "migrate_runs", "in_progress"}:
                total_runs, _, failed_run_count = experiment_migrator.migrate_runs_streaming(
                    [source_experiment_id],
                    {
                        "experiments": {source_experiment_id: dest_experiment_id},
                        "examples": self.state.id_mappings.get("examples", {}),
                    },
                )
                if failed_run_count > 0:
                    detail = f"{failed_run_count} run(s) failed during replay"
                    self.state.update_item_status(
                        item_id,
                        MigrationStatus.FAILED,
                        error=detail,
                        stage="migrate_runs",
                    )
                    self.state_manager.save()
                    return False, detail

                self.state.update_item_status(
                    item_id,
                    MigrationStatus.IN_PROGRESS,
                    destination_id=dest_experiment_id,
                    stage="migrate_feedback",
                )
                self.state.update_item_checkpoint(
                    item_id,
                    metadata={"runs_migrated": total_runs},
                )
                self.state_manager.save()

            current_item = self.state.get_item(item_id)
            if current_item and (
                current_item.stage in {"migrate_feedback", "completed"}
                and not current_item.metadata.get("feedback_verified")
            ):
                total_found, total_migrated = feedback_migrator.migrate_feedback_for_experiments(
                    {source_experiment_id: dest_experiment_id},
                    self.state.id_mappings.get("run", {}),
                )
                if total_found > total_migrated:
                    detail = (
                        f"feedback replay incomplete ({total_migrated}/{total_found} migrated)"
                    )
                    self.state.update_item_status(
                        item_id,
                        MigrationStatus.FAILED,
                        error=detail,
                        stage="migrate_feedback",
                    )
                    self.state_manager.save()
                    return False, detail

                self.state.update_item_checkpoint(
                    item_id,
                    stage="completed",
                    metadata={
                        "feedback_found": total_found,
                        "feedback_migrated": total_migrated,
                        "feedback_verified": True,
                    },
                )
                self.state.mark_terminal(
                    item_id,
                    ResolutionOutcome.MIGRATED,
                    "experiment_migrated",
                    verification_state=VerificationState.VERIFIED,
                    evidence={
                        "destination_experiment_id": dest_experiment_id,
                        "feedback_found": total_found,
                        "feedback_migrated": total_migrated,
                    },
                )
                self.state_manager.save()

            return True, "migrated"
        except Exception as e:
            self.state.update_item_status(
                item_id,
                MigrationStatus.FAILED,
                error=str(e),
                stage=item.stage or "experiment_resolution",
            )
            issue = self.state.add_issue(
                "transient",
                "experiment_resolution_failed",
                f"Experiment resolution failed for {experiment['name']}",
                item_id=item_id,
                next_action="Re-run `langsmith-migrator resume` after reviewing the error.",
                evidence={"error": str(e)},
                workspace_pair=self.workspace_pair(),
            )
            self.state.queue_remediation(
                issue_id=issue.id,
                item_id=item_id,
                next_action=issue.next_action or "Resume experiment migration.",
                command="langsmith-migrator resume",
            )
            self.state_manager.save()
            return False, str(e)

    def _apply_item_workspace(self, item: MigrationItem) -> None:
        """Restore workspace scope for a tracked item."""
        workspace_pair = getattr(item, "workspace_pair", {}) or {}
        source_ws = workspace_pair.get("source")
        dest_ws = workspace_pair.get("dest")
        if source_ws and dest_ws:
            self.set_workspace_context(source_ws, dest_ws)
        else:
            self.clear_workspace_context()

    def resume_items(self, items_to_process: List[MigrationItem]) -> Dict[str, List[str]]:
        """Resume pending and failed items using typed dispatch."""
        self.ensure_state()
        results = {"resumed": [], "blocked": []}

        from .annotation_queue import AnnotationQueueMigrator
        from .chart import ChartMigrator
        from .prompt import PromptMigrator
        from .rules import RulesMigrator

        prompt_migrator = PromptMigrator(
            self.source_client,
            self.dest_client,
            self.state,
            self.config,
        )
        queue_migrator = AnnotationQueueMigrator(
            self.source_client,
            self.dest_client,
            self.state,
            self.config,
        )
        rules_migrator = RulesMigrator(
            self.source_client,
            self.dest_client,
            self.state,
            self.config,
        )
        chart_migrator = ChartMigrator(
            self.source_client,
            self.dest_client,
            self.state,
            self.config,
        )
        experiment_migrator = ExperimentMigrator(
            self.source_client,
            self.dest_client,
            self.state,
            self.config,
        )
        feedback_migrator = FeedbackMigrator(
            self.source_client,
            self.dest_client,
            self.state,
            self.config,
        )

        # Pre-build user/role migrator if needed
        ur_migrator = None
        has_user_items = any(item.type in ("org_member", "ws_member") for item in items_to_process)
        if has_user_items:
            from .user_role import UserRoleMigrator
            ur_migrator = UserRoleMigrator(
                self.source_client,
                self.dest_client,
                self.state,
                self.config,
            )
            role_mappings = self.state.id_mappings.get("roles", {})
            ur_migrator._role_id_map = dict(role_mappings)

        for item in items_to_process:
            self._apply_item_workspace(item)
            try:
                self.state.update_item_status(
                    item.id,
                    MigrationStatus.IN_PROGRESS,
                    stage=item.stage or "resume",
                )
                self.state_manager.save()

                if item.type == "dataset":
                    self.migrate_datasets_parallel([item.source_id], include_examples=True)
                elif item.type == "experiment":
                    experiment = self.source_client.get(f"/sessions/{item.source_id}")
                    source_dataset_id = item.metadata.get("source_dataset_id")
                    dest_dataset_id = item.metadata.get("dest_dataset_id") or (
                        self.state.get_mapped_id("dataset", source_dataset_id)
                        if source_dataset_id
                        else None
                    )
                    if not source_dataset_id or not dest_dataset_id:
                        self.state.mark_terminal(
                            item.id,
                            ResolutionOutcome.BLOCKED_WITH_CHECKPOINT,
                            "missing_dataset_dependency",
                            verification_state=VerificationState.BLOCKED,
                            next_action="Migrate the referenced dataset, then run `langsmith-migrator resume`.",
                            evidence={"source_dataset_id": source_dataset_id},
                            error="dataset dependency missing",
                        )
                    else:
                        self._resolve_experiment_item(
                            experiment,
                            source_dataset_id,
                            dest_dataset_id,
                            experiment_migrator,
                            feedback_migrator,
                        )
                elif item.type == "prompt":
                    prompt_migrator.migrate_prompt(
                        item.source_id,
                        include_all_commits=item.metadata.get("include_all_commits", False),
                    )
                elif item.type == "queue":
                    queue_payload = item.metadata.get("queue") or queue_migrator.get_queue(item.source_id)
                    destination_id = queue_migrator.create_queue(queue_payload)
                    self.state.update_item_status(
                        item.id,
                        MigrationStatus.COMPLETED,
                        destination_id=destination_id,
                        stage="completed",
                    )
                    self.state.mark_terminal(
                        item.id,
                        ResolutionOutcome.MIGRATED,
                        "queue_migrated",
                        verification_state=VerificationState.VERIFIED,
                        evidence={"destination_id": destination_id},
                    )
                elif item.type == "rule":
                    rules_migrator._project_id_map = dict(item.metadata.get("project_id_map") or {})
                    rule_payload = item.metadata.get("rule") or rules_migrator.get_rule(item.source_id)
                    if rule_payload:
                        destination_id = rules_migrator.create_rule(
                            rule_payload,
                            strip_project_reference=item.metadata.get("strip_projects", False),
                            ensure_project=item.metadata.get("ensure_project", False),
                            create_disabled=item.metadata.get("create_disabled", False),
                        )
                        if destination_id and not self.state.get_item(item.id).terminal_state:
                            self.state.update_item_status(
                                item.id,
                                MigrationStatus.COMPLETED,
                                destination_id=destination_id,
                                stage="completed",
                            )
                            self.state.mark_terminal(
                                item.id,
                                ResolutionOutcome.MIGRATED,
                                "rule_migrated",
                                verification_state=VerificationState.VERIFIED,
                                evidence={"destination_id": destination_id},
                            )
                elif item.type == "chart":
                    chart_payload = item.metadata.get("chart")
                    if chart_payload:
                        destination_id = chart_migrator.migrate_chart(
                            chart_payload,
                            item.metadata.get("dest_session_id"),
                        )
                        if destination_id and not self.state.get_item(item.id).terminal_state:
                            self.state.update_item_status(
                                item.id,
                                MigrationStatus.COMPLETED,
                                destination_id=destination_id,
                                stage="completed",
                            )
                            self.state.mark_terminal(
                                item.id,
                                ResolutionOutcome.MIGRATED,
                                "chart_migrated",
                                verification_state=VerificationState.VERIFIED,
                                evidence={"destination_id": destination_id},
                            )
                elif item.type in ("org_member", "ws_member"):
                    if item.type == "org_member":
                        member_payload = item.metadata.get("member")
                        if member_payload:
                            migrated, _, _ = ur_migrator.migrate_org_members([member_payload])
                            if migrated:
                                results["resumed"].append(f"{item.type}:{item.source_id}")
                    else:  # ws_member
                        try:
                            ur_migrator.ensure_dest_email_index()
                        except Exception as e:  # noqa: BLE001
                            self.state.mark_terminal(
                                item.id,
                                ResolutionOutcome.BLOCKED_WITH_CHECKPOINT,
                                "ws_member_dest_org_lookup_failed",
                                verification_state=VerificationState.BLOCKED,
                                next_action=(
                                    "Verify destination org members API access and run "
                                    "`langsmith-migrator resume` again."
                                ),
                                evidence={"error": str(e)},
                                error=str(e),
                            )
                            results["blocked"].append(f"{item.type}:{item.source_id}")
                            self.state_manager.save()
                            continue
                        member_payload = item.metadata.get("member")
                        if member_payload:
                            migrated, _, _ = ur_migrator.migrate_workspace_members(
                                selected_members=[member_payload]
                            )
                        else:
                            # Backwards compatibility with older state items that
                            # only tracked ws_member type without member payload.
                            migrated, _, _ = ur_migrator.migrate_workspace_members()
                        if migrated:
                            results["resumed"].append(f"{item.type}:{item.source_id}")
                else:
                    issue = self.state.add_issue(
                        "capability",
                        "resume_not_yet_available",
                        f"Automatic resume is not yet implemented for {item.type} items",
                        item_id=item.id,
                        next_action=f"Re-run the `{item.type}` command after reviewing the remediation bundle.",
                        evidence={"item_type": item.type},
                        workspace_pair=self.workspace_pair(),
                    )
                    self.state.queue_remediation(
                        issue_id=issue.id,
                        item_id=item.id,
                        next_action=issue.next_action or "Retry the resource command.",
                        command=f"langsmith-migrator {item.type}s",
                    )

                current_item = self.state.get_item(item.id)
                if current_item and current_item.terminal_state in {
                    ResolutionOutcome.MIGRATED.value,
                    ResolutionOutcome.MIGRATED_WITH_VERIFIED_DOWNGRADE.value,
                }:
                    results["resumed"].append(f"{item.type}:{item.source_id}")
                elif current_item and current_item.terminal_state in {
                    ResolutionOutcome.BLOCKED_WITH_CHECKPOINT.value,
                    ResolutionOutcome.EXPORTED_WITH_MANUAL_APPLY.value,
                }:
                    results["blocked"].append(f"{item.type}:{item.source_id}")
                elif current_item and current_item.status == MigrationStatus.COMPLETED:
                    results["resumed"].append(f"{item.type}:{item.source_id}")
                else:
                    results["blocked"].append(f"{item.type}:{item.source_id}")

                self.state_manager.save()
            except Exception as e:
                self.state.update_item_status(
                    item.id,
                    MigrationStatus.FAILED,
                    error=str(e),
                    stage=item.stage or "resume",
                )
                issue = self.state.add_issue(
                    "transient",
                    "resume_dispatch_failed",
                    f"Resume failed for {item.type} item {item.source_id}",
                    item_id=item.id,
                    next_action="Review the error and run `langsmith-migrator resume` again.",
                    evidence={"error": str(e), "item_type": item.type},
                    workspace_pair=self.workspace_pair(),
                )
                self.state.queue_remediation(
                    issue_id=issue.id,
                    item_id=item.id,
                    next_action=issue.next_action or "Retry resume.",
                    command="langsmith-migrator resume",
                )
                results["blocked"].append(f"{item.type}:{item.source_id}")
                self.state_manager.save()

        self.clear_workspace_context()
        return results

    def set_workspace_context(self, source_ws_id: str, dest_ws_id: str) -> None:
        """Scope both clients to the given workspace pair."""
        self.source_client.set_workspace(source_ws_id)
        self.dest_client.set_workspace(dest_ws_id)

    def clear_workspace_context(self) -> None:
        """Remove workspace scoping from both clients."""
        self.source_client.set_workspace(None)
        self.dest_client.set_workspace(None)

    def cleanup(self):
        """Clean up resources."""
        self.source_client.close()
        self.dest_client.close()
