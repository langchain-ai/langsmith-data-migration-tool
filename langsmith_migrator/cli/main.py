"""Simplified CLI interface with improved architecture."""

import click
import time
from rich.console import Console
from rich.table import Table
from rich.prompt import Confirm

from ..utils.config import Config
from ..utils.state import StateManager, MigrationStatus
from ..core.migrators import (
    MigrationOrchestrator,
    DatasetMigrator,
    AnnotationQueueMigrator,
    PromptMigrator,
    RulesMigrator,
)
from .tui_selector import select_items


console = Console()


def display_banner():
    """Display minimal banner."""
    console.print("\n[bold]LangSmith Migration Tool[/bold]\n")


def ensure_config(config: Config) -> bool:
    """Ensure configuration is valid, prompting for missing values."""
    is_valid, errors = config.validate()

    if not is_valid:
        # Check if we're missing credentials specifically
        missing_creds = any("API key is required" in error for error in errors)

        if missing_creds:
            # Prompt for missing credentials
            config.prompt_for_credentials(console)

            # Re-validate after prompting
            is_valid, errors = config.validate()

            if not is_valid:
                console.print("[red]Configuration still invalid after prompting:[/red]")
                for error in errors:
                    console.print(f"  • {error}")
                return False
        else:
            # Non-credential errors
            for error in errors:
                console.print(f"[red]Error:[/red] {error}")
            return False

    return True


@click.group()
@click.option('--source-key', envvar='LANGSMITH_OLD_API_KEY', help='Source API key')
@click.option('--dest-key', envvar='LANGSMITH_NEW_API_KEY', help='Destination API key')
@click.option('--source-url', envvar='LANGSMITH_OLD_BASE_URL', help='Source base URL')
@click.option('--dest-url', envvar='LANGSMITH_NEW_BASE_URL', help='Destination base URL')
@click.option('--no-ssl', is_flag=True, help='Disable SSL verification')
@click.option('--batch-size', type=int, help='Batch size for operations')
@click.option('--workers', type=int, help='Number of concurrent workers')
@click.option('--dry-run', is_flag=True, help='Run in dry-run mode (no changes)')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose output')
@click.pass_context
def cli(ctx, source_key, dest_key, source_url, dest_url, no_ssl, batch_size, workers, dry_run, verbose):
    """LangSmith Migration Tool - Migrate data between LangSmith instances."""
    ctx.ensure_object(dict)

    # Create configuration
    config = Config(
        source_api_key=source_key,
        dest_api_key=dest_key,
        source_url=source_url,
        dest_url=dest_url,
        verify_ssl=not no_ssl,
        batch_size=batch_size,
        concurrent_workers=workers,
        dry_run=dry_run,
        verbose=verbose
    )

    ctx.obj['config'] = config
    ctx.obj['state_manager'] = StateManager()


@cli.command()
@click.pass_context
def test(ctx):
    """Test connections to source and destination instances."""
    config = ctx.obj['config']

    display_banner()

    if not ensure_config(config):
        return

    config.display_summary(console)

    console.print("Testing connections... ", end="")
    orchestrator = MigrationOrchestrator(config, ctx.obj['state_manager'])

    if orchestrator.test_connections():
        console.print("[green]✓[/green]")
    else:
        console.print("[red]✗[/red]")
        ctx.exit(1)


@cli.command()
@click.option('--include-experiments', is_flag=True, help='Include experiments with datasets')
@click.option('--all', 'select_all', is_flag=True, help='Migrate all datasets')
@click.pass_context
def datasets(ctx, include_experiments, select_all):
    """Migrate datasets with interactive selection."""
    config = ctx.obj['config']
    state_manager = ctx.obj['state_manager']

    display_banner()

    if not ensure_config(config):
        return

    orchestrator = MigrationOrchestrator(config, state_manager)

    # Test connections first
    console.print("Testing connections... ", end="")
    source_ok, dest_ok = orchestrator.test_connections_detailed()
    if not source_ok:
        console.print("[red]✗ Source connection failed[/red]")
        return
    if not dest_ok:
        console.print("[yellow]⚠ Source OK, destination connection failed[/yellow]")
        console.print("Continuing with source-only operations...")
    else:
        console.print("[green]✓[/green]")

    # Get datasets
    console.print("Fetching datasets... ", end="")
    dataset_migrator = DatasetMigrator(
        orchestrator.source_client,
        orchestrator.dest_client,
        None,
        config
    )

    datasets = dataset_migrator.list_datasets()

    if not datasets:
        console.print("[yellow]none found[/yellow]")
        return

    console.print(f"found {len(datasets)}\n")

    # Select datasets
    if select_all:
        selected_datasets = datasets
    else:
        selected_datasets = select_items(
            items=datasets,
            title="Select Datasets to Migrate",
            columns=[
                {"key": "name", "title": "Name", "width": 40},
                {"key": "id", "title": "ID", "width": 36},
                {"key": "description", "title": "Description", "width": 50},
                {"key": "example_count", "title": "Examples", "width": 10}
            ]
        )

    if not selected_datasets:
        console.print("[yellow]No datasets selected[/yellow]")
        return

    # Confirmation
    console.print(f"\nSelected {len(selected_datasets)} dataset(s)")

    if config.migration.dry_run:
        console.print("[dim]Mode: Dry Run (no changes)[/dim]")

    if not Confirm.ask("\nProceed?"):
        console.print("[yellow]Cancelled[/yellow]")
        return

    # Perform migration
    dataset_ids = [d["id"] for d in selected_datasets]

    try:
        id_mapping = orchestrator.migrate_datasets_parallel(
            dataset_ids,
            include_examples=True,
            include_experiments=include_experiments
        )

        # Display results
        console.print("\n[green]✓[/green] Migration completed")

        # Show statistics
        if orchestrator.state:
            stats = orchestrator.state.get_statistics()
            if stats['completed'] > 0:
                console.print(f"  Migrated: {stats['completed']} dataset(s)")
            if stats['failed'] > 0:
                console.print(f"  [red]Failed: {stats['failed']}[/red]")

            # Show experiment stats if included
            if include_experiments and 'by_type' in stats and 'experiment' in stats['by_type']:
                exp_stats = stats['by_type']['experiment']
                console.print(f"  Experiments: {exp_stats['completed']} completed, {exp_stats['failed']} failed")

    except Exception as e:
        console.print(f"\n[red]Migration failed: {e}[/red]")
        ctx.exit(1)
    finally:
        orchestrator.cleanup()


@cli.command()
@click.pass_context
def resume(ctx):
    """Resume a previous migration session."""
    state_manager = ctx.obj['state_manager']

    display_banner()

    # List available sessions
    sessions = state_manager.list_sessions()

    if not sessions:
        console.print("[yellow]No previous migration sessions found[/yellow]")
        return

    # Display sessions
    table = Table(title="Available Migration Sessions", show_header=True)
    table.add_column("#", style="cyan", width=3)
    table.add_column("Session ID", style="dim")
    table.add_column("Started", style="green")
    table.add_column("Status", style="yellow")
    table.add_column("Progress", style="blue")

    for idx, session in enumerate(sessions, 1):
        started = time.strftime("%Y-%m-%d %H:%M", time.localtime(session["started_at"]))
        stats = session.get("statistics", {})
        status = f"{stats.get('completed', 0)}/{stats.get('total', 0)}"
        progress = f"{stats.get('completion_percentage', 0):.1f}%"

        table.add_row(
            str(idx),
            session["session_id"],
            started,
            status,
            progress
        )

    console.print(table)

    # Select session
    choice = console.input("\nEnter session number to resume (or 'q' to quit): ")

    if choice.lower() == 'q':
        return

    try:
        session_idx = int(choice) - 1
        if 0 <= session_idx < len(sessions):
            selected_session = sessions[session_idx]
        else:
            console.print("[red]Invalid selection[/red]")
            return
    except ValueError:
        console.print("[red]Invalid input[/red]")
        return

    # Load session
    state = state_manager.load_session(selected_session["session_id"])

    if not state:
        console.print("[red]Failed to load session[/red]")
        return

    # Display resume info
    resume_info = state_manager.get_resume_info(state)

    console.print("\n[bold]Resume Information:[/bold]")
    console.print(f"  Session: {resume_info['session_id']}")
    console.print(f"  Pending: {resume_info['pending']}")
    console.print(f"  Failed: {resume_info['failed']}")
    console.print(f"  Can resume: {'Yes' if resume_info['can_resume'] else 'No'}")

    if not resume_info['can_resume']:
        console.print("[yellow]Nothing to resume in this session[/yellow]")
        return

    if not Confirm.ask("Resume this migration?"):
        return

    # Resume migration
    config = ctx.obj['config']
    orchestrator = MigrationOrchestrator(config, state_manager)
    orchestrator.state = state

    # Get items that need processing
    pending_items = state.get_pending_items()
    failed_items = state.get_failed_items()
    items_to_process = pending_items + failed_items

    console.print(f"\n[bold]Resuming migration of {len(items_to_process)} items...[/bold]")

    # Process dataset items
    dataset_ids = [
        item.source_id for item in items_to_process
        if item.type == "dataset"
    ]

    # Process experiment items separately
    experiment_ids = [
        item.source_id for item in items_to_process
        if item.type == "experiment"
    ]

    if dataset_ids:
        try:
            orchestrator.migrate_datasets_parallel(dataset_ids, include_examples=True)
            console.print("\n[green]✓ Migration resumed and completed[/green]")
        except Exception as e:
            console.print(f"\n[red]Resume failed: {e}[/red]")
    elif experiment_ids:
        console.print("[yellow]Note: Experiment-only resume not yet supported - please resume full dataset migration[/yellow]")
    else:
        console.print("[yellow]No datasets to process[/yellow]")


@cli.command()
@click.pass_context
def queues(ctx):
    """Migrate annotation queues."""
    config = ctx.obj['config']
    state_manager = ctx.obj['state_manager']

    display_banner()

    if not ensure_config(config):
        return

    orchestrator = MigrationOrchestrator(config, state_manager)

    # Test connections
    if not orchestrator.test_connections():
        console.print("\n[red]Cannot proceed without valid connections[/red]")
        return

    # Get queues
    console.print("\n[bold]Fetching annotation queues from source...[/bold]")
    queue_migrator = AnnotationQueueMigrator(
        orchestrator.source_client,
        orchestrator.dest_client,
        None,
        config
    )

    queues = queue_migrator.list_queues()

    if not queues:
        console.print("[yellow]No annotation queues found[/yellow]")
        return

    selected_queues = select_items(
        items=queues,
        title="Select Annotation Queues to Migrate",
        columns=[
            {"key": "name", "title": "Name", "width": 40},
            {"key": "id", "title": "ID", "width": 36},
            {"key": "description", "title": "Description", "width": 50}
        ]
    )

    if not selected_queues:
        console.print("[yellow]No queues selected[/yellow]")
        return

    console.print(f"\n[bold]Migrating {len(selected_queues)} annotation queue(s)...[/bold]")

    # Perform migration
    for queue in selected_queues:
        try:
            new_id = queue_migrator.create_queue(queue)
            console.print(f"[green]✓[/green] Migrated queue: {queue['name']} -> {new_id}")
        except Exception as e:
            console.print(f"[red]✗[/red] Failed to migrate {queue['name']}: {e}")


@cli.command()
@click.option('--all', 'select_all', is_flag=True, help='Migrate all prompts')
@click.option('--include-all-commits', is_flag=True, help='Include all commit history')
@click.pass_context
def prompts(ctx, select_all, include_all_commits):
    """Migrate prompts with interactive selection."""
    config = ctx.obj['config']
    state_manager = ctx.obj['state_manager']

    display_banner()

    if not ensure_config(config):
        return

    orchestrator = MigrationOrchestrator(config, state_manager)

    console.print("Testing connections... ", end="")
    source_ok, dest_ok = orchestrator.test_connections_detailed()
    if not source_ok:
        console.print("[red]✗ Source connection failed[/red]")
        return
    if not dest_ok:
        console.print("[red]✗ Destination connection failed[/red]")
        return
    console.print("[green]✓[/green]")

    # Create prompt migrator
    prompt_migrator = PromptMigrator(
        orchestrator.source_client,
        orchestrator.dest_client,
        None,
        config
    )

    # Check if prompts API is available on destination
    console.print("Checking prompts API availability... ", end="")
    api_available, error_msg = prompt_migrator.check_prompts_api_available()
    if not api_available:
        console.print("[red]✗[/red]")
        console.print(f"\n[red]Error:[/red] {error_msg}")
        console.print("\n[yellow]Possible reasons:[/yellow]")
        console.print("  • The destination instance may not have the prompts feature enabled")
        console.print("  • The instance may be running an older version of LangSmith")
        console.print("  • The nginx/proxy configuration may not route prompts endpoints")
        console.print("\n[dim]Please check with your LangSmith administrator.[/dim]")
        return
    console.print("[green]✓[/green]")

    console.print("Fetching prompts... ", end="")
    prompts = prompt_migrator.list_prompts()

    if not prompts:
        console.print("[yellow]none found[/yellow]")
        return

    console.print(f"found {len(prompts)}\n")

    # Warning about SDK-based prompt migration
    console.print("[yellow]Note:[/yellow] Prompt migration uses the LangSmith SDK.")
    console.print("[dim]Some prompts (especially those created via API) may not be accessible via the SDK.[/dim]")
    console.print("[dim]If all prompts fail, they may need to be recreated manually in the destination.[/dim]\n")

    if select_all:
        selected_prompts = prompts
    else:
        selected_prompts = select_items(
            items=prompts,
            title="Select Prompts to Migrate",
            columns=[
                {"key": "repo_handle", "title": "Handle", "width": 40},
                {"key": "description", "title": "Description", "width": 50},
                {"key": "num_commits", "title": "Commits", "width": 10},
                {"key": "is_public", "title": "Public", "width": 8}
            ]
        )

    if not selected_prompts:
        console.print("[yellow]No prompts selected[/yellow]")
        return

    console.print(f"\nSelected {len(selected_prompts)} prompt(s)")

    if config.migration.dry_run:
        console.print("[dim]Mode: Dry Run (no changes)[/dim]")

    if include_all_commits:
        console.print("[dim]Including all commit history[/dim]")

    if not Confirm.ask("\nProceed?"):
        console.print("[yellow]Cancelled[/yellow]")
        return

    success_count = 0
    failed_count = 0
    has_405_error = False

    for prompt in selected_prompts:
        try:
            result = prompt_migrator.migrate_prompt(
                prompt['repo_handle'],
                include_all_commits=include_all_commits
            )
            if result:
                console.print(f"[green]✓[/green] Migrated: {prompt['repo_handle']}")
                success_count += 1
            else:
                console.print(f"[red]✗[/red] Failed: {prompt['repo_handle']}")
                failed_count += 1
        except Exception as e:
            error_msg = str(e)
            if "405" in error_msg or "Not Allowed" in error_msg:
                has_405_error = True
            console.print(f"[red]✗[/red] Failed {prompt['repo_handle']}: {e}")
            failed_count += 1

    console.print(f"\n[green]✓[/green] Migration completed")
    console.print(f"  Migrated: {success_count} prompt(s)")
    if failed_count > 0:
        console.print(f"  [red]Failed: {failed_count}[/red]")
        
        if has_405_error:
            console.print("\n[yellow]⚠ All failures were due to 405 Not Allowed errors[/yellow]")
            console.print("[dim]This indicates the destination instance does not support prompt write operations.[/dim]")
            console.print("[dim]Possible solutions:[/dim]")
            console.print("[dim]  • Enable the prompts feature on your LangSmith instance[/dim]")
            console.print("[dim]  • Check nginx/proxy configuration for /api/v1/repos/* endpoints[/dim]")
            console.print("[dim]  • Contact your LangSmith administrator[/dim]")

    orchestrator.cleanup()


@cli.command()
@click.option('--source', is_flag=True, help='List projects from source instance')
@click.option('--dest', is_flag=True, help='List projects from destination instance')
@click.pass_context
def list_projects(ctx, source, dest):
    """List projects with their IDs to help create project mappings."""
    config = ctx.obj['config']
    
    if not source and not dest:
        console.print("[yellow]Specify --source or --dest to list projects[/yellow]")
        return
    
    if not ensure_config(config):
        return
    
    orchestrator = MigrationOrchestrator(config, ctx.obj['state_manager'])
    
    from rich.table import Table
    
    if source:
        console.print("\n[bold]Source Projects:[/bold]")
        try:
            table = Table(show_header=True)
            table.add_column("Name", style="cyan", width=50)
            table.add_column("ID", style="dim", width=36)
            
            for project in orchestrator.source_client.get_paginated("/sessions", page_size=100):
                if isinstance(project, dict):
                    table.add_row(project.get('name', 'unnamed'), project.get('id', ''))
            
            console.print(table)
        except Exception as e:
            console.print(f"[red]Failed to list source projects: {e}[/red]")
    
    if dest:
        console.print("\n[bold]Destination Projects:[/bold]")
        try:
            table = Table(show_header=True)
            table.add_column("Name", style="cyan", width=50)
            table.add_column("ID", style="dim", width=36)
            
            for project in orchestrator.dest_client.get_paginated("/sessions", page_size=100):
                if isinstance(project, dict):
                    table.add_row(project.get('name', 'unnamed'), project.get('id', ''))
            
            console.print(table)
        except Exception as e:
            console.print(f"[red]Failed to list destination projects: {e}[/red]")
    
    orchestrator.cleanup()


@cli.command()
@click.option('--all', 'select_all', is_flag=True, help='Migrate all rules')
@click.option('--strip-projects', is_flag=True, help='Strip project associations and create as global rules')
@click.option('--project-mapping', type=str, help='JSON string or file path with project ID mapping (e.g., \'{"old-id": "new-id"}\')')
@click.pass_context
def rules(ctx, select_all, strip_projects, project_mapping):
    """Migrate project rules (automation rules)."""
    config = ctx.obj['config']
    state_manager = ctx.obj['state_manager']

    display_banner()

    if not ensure_config(config):
        return

    orchestrator = MigrationOrchestrator(config, state_manager)

    console.print("Testing connections... ", end="")
    source_ok, dest_ok = orchestrator.test_connections_detailed()
    if not source_ok:
        console.print("[red]✗ Source connection failed[/red]")
        return
    if not dest_ok:
        console.print("[red]✗ Destination connection failed[/red]")
        return
    console.print("[green]✓[/green]")

    rules_migrator = RulesMigrator(
        orchestrator.source_client,
        orchestrator.dest_client,
        None,
        config
    )
    
    # Parse and apply custom project mapping if provided
    if project_mapping:
        import json
        import os
        
        try:
            # Check if it's a file path
            if os.path.isfile(project_mapping):
                with open(project_mapping, 'r') as f:
                    custom_mapping = json.load(f)
                console.print(f"Loaded project mapping from file: {project_mapping}")
            else:
                # Parse as JSON string
                custom_mapping = json.loads(project_mapping)
            
            # Validate it's a dict
            if not isinstance(custom_mapping, dict):
                console.print("[red]Error: Project mapping must be a JSON object/dict[/red]")
                return
            
            # Apply the custom mapping
            rules_migrator._project_id_map = custom_mapping
            console.print(f"Using custom project mapping with {len(custom_mapping)} project(s)")
            
        except json.JSONDecodeError as e:
            console.print(f"[red]Error parsing project mapping JSON: {e}[/red]")
            return
        except Exception as e:
            console.print(f"[red]Error loading project mapping: {e}[/red]")
            return
    
    console.print("Fetching rules... ", end="")
    rules = rules_migrator.list_rules()

    if not rules:
        console.print("[yellow]none found[/yellow]")
        console.print("\n[yellow]No rules were found on the source instance.[/yellow]")
        console.print("\n[dim]Possible reasons:[/dim]")
        console.print("[dim]  • No rules have been created yet[/dim]")
        console.print("[dim]  • Rules feature is not available on this instance[/dim]")
        console.print("[dim]  • Rules are project-specific (try with a specific project)[/dim]")
        console.print("[dim]  • Rules API uses a different endpoint than expected[/dim]")
        console.print("\n[dim]Tip: Run with -v (verbose) flag to see which endpoints were checked[/dim]")
        return

    console.print(f"found {len(rules)}\n")

    # Analyze rules for project/dataset associations
    project_specific = [r for r in rules if r.get('session_id')]
    dataset_specific = [r for r in rules if r.get('dataset_id')]
    project_only = [r for r in rules if r.get('session_id') and not r.get('dataset_id')]

    if project_specific and not strip_projects:
        console.print(f"[yellow]Warning: {len(project_specific)} rule(s) are project-specific[/yellow]")
        console.print("[dim]These rules reference projects that may not exist in the destination.[/dim]")
        if project_only:
            console.print(f"[dim]Note: {len(project_only)} rule(s) have no dataset_id and cannot be migrated without projects.[/dim]")
        console.print("[dim]Options:[/dim]")
        console.print("[dim]  • Migrate projects first, then migrate rules[/dim]")
        console.print("[dim]  • Rules with dataset_id can be migrated without projects[/dim]\n")

    if select_all:
        selected_rules = rules
    else:
        # Enrich rules with association info for display
        rules_for_display = []
        for rule in rules:
            rule_copy = rule.copy()
            # Determine what the rule is associated with
            if rule.get('session_id') and rule.get('dataset_id'):
                rule_copy['association'] = 'Project+Dataset'
            elif rule.get('session_id'):
                rule_copy['association'] = 'Project'
            elif rule.get('dataset_id'):
                rule_copy['association'] = 'Dataset'
            else:
                rule_copy['association'] = 'None'
            
            # Use display_name if available, fallback to name
            if not rule_copy.get('name') and rule_copy.get('display_name'):
                rule_copy['name'] = rule_copy['display_name']
                
            rules_for_display.append(rule_copy)

        selected_rules = select_items(
            items=rules_for_display,
            title="Select Rules to Migrate",
            columns=[
                {"key": "name", "title": "Name", "width": 30},
                {"key": "rule_type", "title": "Type", "width": 25},
                {"key": "association", "title": "Association", "width": 15},
                {"key": "enabled", "title": "Enabled", "width": 10},
            ]
        )

    if not selected_rules:
        console.print("[yellow]No rules selected[/yellow]")
        return

    console.print(f"\nSelected {len(selected_rules)} rule(s)")

    if config.migration.dry_run:
        console.print("[dim]Mode: Dry Run (no changes)[/dim]")

    if strip_projects:
        console.print("[dim]Mode: Stripping project associations (creating as global rules)[/dim]")

    if not Confirm.ask("\nProceed?"):
        console.print("[yellow]Cancelled[/yellow]")
        return

    success_count = 0
    failed_count = 0
    skipped_count = 0

    for rule in selected_rules:
        try:
            rule_name = rule.get('display_name') or rule.get('name', 'unnamed')
            has_project = bool(rule.get('session_id'))
            has_dataset = bool(rule.get('dataset_id'))

            if has_project and not strip_projects:
                console.print(f"[yellow]⊘[/yellow] Skipped: {rule_name} (project-specific)")
                skipped_count += 1
                continue

            result = rules_migrator.create_rule(rule, strip_project_reference=strip_projects)
            if result:
                console.print(f"[green]✓[/green] Migrated: {rule_name}")
                success_count += 1
            else:
                # Result is None - check if it was logged as skipped
                console.print(f"[yellow]⊘[/yellow] Skipped: {rule_name} (cannot migrate without project or dataset)")
                skipped_count += 1
        except Exception as e:
            rule_name = rule.get('display_name') or rule.get('name', 'unnamed')
            console.print(f"[red]✗[/red] Failed {rule_name}: {e}")
            failed_count += 1

    console.print(f"\n[green]✓[/green] Migration completed")
    console.print(f"  Migrated: {success_count} rule(s)")
    if skipped_count > 0:
        console.print(f"  [yellow]Skipped: {skipped_count}[/yellow]")
    if failed_count > 0:
        console.print(f"  [red]Failed: {failed_count}[/red]")

    orchestrator.cleanup()


@cli.command()
@click.option('--skip-datasets', is_flag=True, help='Skip dataset migration')
@click.option('--skip-experiments', is_flag=True, help='Skip experiment migration')
@click.option('--skip-prompts', is_flag=True, help='Skip prompt migration')
@click.option('--skip-queues', is_flag=True, help='Skip annotation queue migration')
@click.option('--skip-rules', is_flag=True, help='Skip rules migration')
@click.option('--include-all-commits', is_flag=True, help='Include all prompt commit history')
@click.option('--strip-projects', is_flag=True, help='Strip project associations from rules')
@click.pass_context
def migrate_all(ctx, skip_datasets, skip_experiments, skip_prompts, skip_queues, skip_rules, include_all_commits, strip_projects):
    """Migrate all resources interactively."""
    config = ctx.obj['config']
    state_manager = ctx.obj['state_manager']

    display_banner()

    if not ensure_config(config):
        return

    orchestrator = MigrationOrchestrator(config, state_manager)

    # Test connections first
    console.print("Testing connections... ", end="")
    source_ok, dest_ok = orchestrator.test_connections_detailed()
    if not source_ok:
        console.print("[red]✗ Source connection failed[/red]")
        return
    if not dest_ok:
        console.print("[red]✗ Destination connection failed[/red]")
        return
    console.print("[green]✓[/green]\n")

    console.print("[bold cyan]LangSmith Data Migration Wizard[/bold cyan]\n")
    console.print("This wizard will guide you through migrating all your data.\n")

    # 1. Datasets and Experiments
    if not skip_datasets:
        console.print("[bold]Step 1: Datasets[/bold]")
        console.print("Fetching datasets... ", end="")
        from ..core.migrators import DatasetMigrator
        dataset_migrator = DatasetMigrator(
            orchestrator.source_client,
            orchestrator.dest_client,
            None,
            config
        )
        datasets = dataset_migrator.list_datasets()

        if datasets:
            console.print(f"found {len(datasets)}")

            if Confirm.ask(f"Migrate {len(datasets)} dataset(s)?"):
                include_exp = False
                if not skip_experiments:
                    include_exp = Confirm.ask("Include experiments with datasets?")

                try:
                    dataset_ids = [d["id"] for d in datasets]
                    orchestrator.migrate_datasets_parallel(dataset_ids, include_examples=True, include_experiments=include_exp)
                    console.print("[green]✓ Datasets migrated successfully[/green]\n")
                except Exception as e:
                    console.print(f"[red]✗ Dataset migration failed: {e}[/red]\n")
            else:
                console.print("[yellow]Skipped datasets[/yellow]\n")
        else:
            console.print("[yellow]none found[/yellow]\n")
    else:
        console.print("[dim]Skipping datasets (--skip-datasets)[/dim]\n")

    # 2. Prompts
    if not skip_prompts:
        console.print("[bold]Step 2: Prompts[/bold]")
        console.print("Fetching prompts... ", end="")
        from ..core.migrators import PromptMigrator
        prompt_migrator = PromptMigrator(
            orchestrator.source_client,
            orchestrator.dest_client,
            None,
            config
        )
        prompts = prompt_migrator.list_prompts()

        if prompts:
            console.print(f"found {len(prompts)}")
            console.print("[dim]Note: Prompt migration uses the SDK. API-created prompts may not be accessible.[/dim]")

            if Confirm.ask(f"Migrate {len(prompts)} prompt(s)?"):
                include_history = include_all_commits or Confirm.ask("Include full commit history?")

                success_count = 0
                failed_count = 0

                for prompt in prompts:
                    try:
                        result = prompt_migrator.migrate_prompt(
                            prompt['repo_handle'],
                            include_all_commits=include_history
                        )
                        if result:
                            console.print(f"[green]✓[/green] {prompt['repo_handle']}")
                            success_count += 1
                        else:
                            console.print(f"[red]✗[/red] {prompt['repo_handle']}")
                            failed_count += 1
                    except Exception as e:
                        console.print(f"[red]✗[/red] {prompt['repo_handle']}: {e}")
                        failed_count += 1

                console.print(f"[green]✓ Prompts migrated: {success_count} successful, {failed_count} failed[/green]\n")
            else:
                console.print("[yellow]Skipped prompts[/yellow]\n")
        else:
            console.print("[yellow]none found[/yellow]\n")
    else:
        console.print("[dim]Skipping prompts (--skip-prompts)[/dim]\n")

    # 3. Annotation Queues
    if not skip_queues:
        console.print("[bold]Step 3: Annotation Queues[/bold]")
        console.print("Fetching annotation queues... ", end="")
        from ..core.migrators import AnnotationQueueMigrator
        queue_migrator = AnnotationQueueMigrator(
            orchestrator.source_client,
            orchestrator.dest_client,
            None,
            config
        )
        queues = queue_migrator.list_queues()

        if queues:
            console.print(f"found {len(queues)}")

            if Confirm.ask(f"Migrate {len(queues)} annotation queue(s)?"):
                success_count = 0
                failed_count = 0

                for queue in queues:
                    try:
                        new_id = queue_migrator.create_queue(queue)
                        console.print(f"[green]✓[/green] {queue['name']}")
                        success_count += 1
                    except Exception as e:
                        console.print(f"[red]✗[/red] {queue['name']}: {e}")
                        failed_count += 1

                console.print(f"[green]✓ Queues migrated: {success_count} successful, {failed_count} failed[/green]\n")
            else:
                console.print("[yellow]Skipped queues[/yellow]\n")
        else:
            console.print("[yellow]none found[/yellow]\n")
    else:
        console.print("[dim]Skipping queues (--skip-queues)[/dim]\n")

    # 4. Rules
    if not skip_rules:
        console.print("[bold]Step 4: Project Rules[/bold]")
        console.print("Fetching rules... ", end="")
        from ..core.migrators import RulesMigrator
        rules_migrator = RulesMigrator(
            orchestrator.source_client,
            orchestrator.dest_client,
            None,
            config
        )
        rules = rules_migrator.list_rules()

        if rules:
            console.print(f"found {len(rules)}")

            # Check for project-specific rules
            project_specific = [r for r in rules if r.get('session_id')]
            if project_specific:
                console.print(f"[yellow]Note: {len(project_specific)} rule(s) are project-specific[/yellow]")

            if Confirm.ask(f"Migrate {len(rules)} rule(s)?"):
                strip = strip_projects
                ensure_projects = False
                
                if project_specific and not strip:
                    strip = Confirm.ask("Convert project-specific rules to global rules?")
                    if not strip:
                        ensure_projects = Confirm.ask("Create corresponding projects for project-specific rules?", default=True)

                success_count = 0
                failed_count = 0
                skipped_count = 0

                for rule in rules:
                    try:
                        has_project = bool(rule.get('session_id'))
                        rule_name = rule.get('display_name') or rule.get('name', 'unnamed')

                        if has_project and not strip and not ensure_projects:
                            console.print(f"[yellow]⊘[/yellow] {rule_name} (project-specific, skipping)")
                            skipped_count += 1
                            continue

                        result = rules_migrator.create_rule(
                            rule, 
                            strip_project_reference=strip,
                            ensure_project=ensure_projects
                        )
                        if result:
                            console.print(f"[green]✓[/green] {rule_name}")
                            success_count += 1
                        else:
                            console.print(f"[red]✗[/red] {rule_name}")
                            failed_count += 1
                    except Exception as e:
                        rule_name = rule.get('display_name') or rule.get('name', 'unnamed')
                        console.print(f"[red]✗[/red] {rule_name}: {e}")
                        failed_count += 1

                console.print(f"[green]✓ Rules migrated: {success_count} successful, {skipped_count} skipped, {failed_count} failed[/green]\n")
            else:
                console.print("[yellow]Skipped rules[/yellow]\n")
        else:
            console.print("[yellow]none found[/yellow]\n")
    else:
        console.print("[dim]Skipping rules (--skip-rules)[/dim]\n")

    console.print("[bold green]Migration wizard completed![/bold green]")
    orchestrator.cleanup()


@cli.command()
@click.pass_context
def clean(ctx):
    """Clean up old migration sessions."""
    state_manager = ctx.obj['state_manager']

    sessions = state_manager.list_sessions()

    if not sessions:
        console.print("[yellow]No migration sessions to clean[/yellow]")
        return

    console.print(f"Found {len(sessions)} migration session(s)")

    if Confirm.ask("Delete all migration sessions?"):
        for session in sessions:
            state_manager.delete_session(session["session_id"])
        console.print("[green]✓ All sessions deleted[/green]")
    else:
        console.print("[yellow]Cleanup cancelled[/yellow]")


def main():
    """Main entry point."""
    try:
        cli()
    except KeyboardInterrupt:
        console.print("\n[yellow]Migration interrupted by user[/yellow]")
        exit(1)
    except Exception as e:
        console.print(f"\n[red]Unexpected error: {e}[/red]")
        exit(1)


if __name__ == "__main__":
    main()
