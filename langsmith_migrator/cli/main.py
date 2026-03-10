"""Simplified CLI interface with improved architecture."""

import functools
import click
import time
from dotenv import load_dotenv
from rich.console import Console
from rich.table import Table
from rich.prompt import Confirm
from rich.progress import Progress

load_dotenv()

from ..utils.config import Config


def ssl_option(f):
    """
    Decorator kept for backwards compatibility.

    Note: --no-ssl is now handled globally in the cli() group, so this decorator
    is a no-op. It's kept to avoid breaking existing command decorations.
    """
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        # The global cli() already handles --no-ssl via the config object
        return f(*args, **kwargs)
    return wrapper
from ..utils.state import StateManager, MigrationStatus
from ..core.migrators import (
    MigrationOrchestrator,
    DatasetMigrator,
    AnnotationQueueMigrator,
    PromptMigrator,
    RulesMigrator,
    ChartMigrator,
)
from .tui_selector import select_items
from .tui_project_mapper import build_project_mapping_tui
from .tui_workspace_mapper import WorkspaceProjectResult
from ..utils.workspace import list_projects as _list_projects, list_workspaces as _list_workspaces, get_workspace_name
from ..utils.workspace_resolver import resolve_workspace_context, display_workspaces


console = Console()


def workspace_options(f):
    """Decorator that adds --source-workspace, --dest-workspace, --map-workspaces options."""
    f = click.option('--map-workspaces', is_flag=True,
                     help='Force workspace mapping TUI even for single-workspace instances')(f)
    f = click.option('--dest-workspace',
                     help='Destination workspace ID (skip auto-detection)')(f)
    f = click.option('--source-workspace',
                     help='Source workspace ID (skip auto-detection)')(f)
    return f


def _name_mapping_to_id_mapping(
    name_mapping: dict,
    source_projects: list,
    dest_projects: list,
) -> dict:
    """Convert a name-based project mapping to an ID-based mapping."""
    src_name_to_id = {p['name']: p['id'] for p in source_projects if 'name' in p and 'id' in p}
    dst_name_to_id = {p['name']: p['id'] for p in dest_projects if 'name' in p and 'id' in p}
    id_map = {}
    for src_name, dst_name in name_mapping.items():
        src_id = src_name_to_id.get(src_name)
        dst_id = dst_name_to_id.get(dst_name)
        if src_id and dst_id:
            id_map[src_id] = dst_id
    return id_map


_WS_CANCELLED = "__cancelled__"


def _resolve_workspaces(orchestrator, source_workspace=None, dest_workspace=None, map_workspaces=False):
    """Resolve workspace context from explicit IDs or auto-detection.

    Returns:
        - WorkspaceProjectResult if workspace scoping is active
        - None if no workspace scoping is needed (single-workspace or none found)
        - _WS_CANCELLED sentinel if the user explicitly cancelled the TUI
    """
    # Explicit single-pair override
    if source_workspace and dest_workspace:
        orchestrator.set_workspace_context(source_workspace, dest_workspace)
        console.print(f"[dim]Workspace: {source_workspace} -> {dest_workspace}[/dim]")
        return WorkspaceProjectResult(
            workspace_mapping={source_workspace: dest_workspace},
            project_mappings={},
            workspaces_to_create=[],
        )

    if source_workspace or dest_workspace:
        console.print("[yellow]Both --source-workspace and --dest-workspace must be provided together[/yellow]")
        return _WS_CANCELLED

    # Auto-detect (config is lazy-loaded inside resolve_workspace_context)
    result = resolve_workspace_context(
        orchestrator.source_client,
        orchestrator.dest_client,
        console,
        force_tui=map_workspaces,
    )

    # If force_tui was set and user cancelled, treat as abort
    if result is None and map_workspaces:
        return _WS_CANCELLED

    return result



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
@click.option('--source-key', envvar='LANGSMITH_OLD_API_KEY', help='Source API key (env: LANGSMITH_OLD_API_KEY)')
@click.option('--dest-key', envvar='LANGSMITH_NEW_API_KEY', help='Destination API key (env: LANGSMITH_NEW_API_KEY)')
@click.option('--source-url', envvar='LANGSMITH_OLD_BASE_URL', help='Source base URL (env: LANGSMITH_OLD_BASE_URL)')
@click.option('--dest-url', envvar='LANGSMITH_NEW_BASE_URL', help='Destination base URL (env: LANGSMITH_NEW_BASE_URL)')
@click.option('--no-ssl', is_flag=True, help='Disable SSL verification')
@click.option('--batch-size', type=click.IntRange(min=1, max=1000), help='Batch size for operations (1-1000, default: 100)')
@click.option('--workers', type=click.IntRange(min=1, max=10), help='Number of concurrent workers (1-10, default: 4)')
@click.option('--dry-run', is_flag=True, help='Run in dry-run mode (no changes)')
@click.option('--skip-existing', is_flag=True, help='Skip existing resources instead of updating them')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose output')
@click.pass_context
def cli(ctx, source_key, dest_key, source_url, dest_url, no_ssl, batch_size, workers, dry_run, skip_existing, verbose):
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
        skip_existing=skip_existing if skip_existing else None,
        verbose=verbose
    )

    ctx.obj['config'] = config
    ctx.obj['state_manager'] = StateManager()


@cli.command()
@ssl_option
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
        return

    # Discover and display workspaces (only when verbose)
    if config.migration.verbose:
        source_ws = _list_workspaces(orchestrator.source_client)
        dest_ws = _list_workspaces(orchestrator.dest_client)
        if source_ws or dest_ws:
            console.print()
            if source_ws:
                display_workspaces(console, source_ws, "Source")
            if dest_ws:
                display_workspaces(console, dest_ws, "Destination")

    orchestrator.cleanup()


@cli.command()
@ssl_option
@click.option('--include-experiments', is_flag=True, help='Include experiments with datasets')
@click.option('--all', 'select_all', is_flag=True, help='Migrate all datasets')
@workspace_options
@click.pass_context
def datasets(ctx, include_experiments, select_all, source_workspace, dest_workspace, map_workspaces):
    """Migrate datasets with interactive selection."""
    config = ctx.obj['config']
    state_manager = ctx.obj['state_manager']

    display_banner()

    if not ensure_config(config):
        return

    orchestrator = MigrationOrchestrator(config, state_manager)

    # Test connections first
    console.print("Testing connections... ", end="")
    source_ok, dest_ok, source_error, dest_error = orchestrator.test_connections_detailed()
    if not source_ok:
        console.print("[red]✗ Source connection failed[/red]")
        if source_error:
            console.print(f"[red]  {source_error}[/red]")
        return
    if not dest_ok:
        console.print("[yellow]⚠ Source OK, destination connection failed[/yellow]")
        if dest_error:
            console.print(f"[yellow]  {dest_error}[/yellow]")
        console.print("Continuing with source-only operations...")
    else:
        console.print("[green]✓[/green]")

    # Resolve workspace context
    ws_result = _resolve_workspaces(orchestrator, source_workspace, dest_workspace, map_workspaces)
    if ws_result is _WS_CANCELLED:
        console.print("[yellow]Cancelled[/yellow]")
        return

    ws_pairs = list(ws_result.workspace_mapping.items()) if ws_result else [(None, None)]

    dataset_migrator = DatasetMigrator(
        orchestrator.source_client,
        orchestrator.dest_client,
        None,
        config
    )

    try:
        for src_ws, dst_ws in ws_pairs:
            if src_ws and dst_ws:
                orchestrator.set_workspace_context(src_ws, dst_ws)
                console.print(f"\n[bold cyan]Workspace: {src_ws} -> {dst_ws}[/bold cyan]")

            # Get datasets
            console.print("Fetching datasets... ", end="")
            ds = dataset_migrator.list_datasets()

            if not ds:
                console.print("[yellow]none found[/yellow]")
                continue

            console.print(f"found {len(ds)}\n")

            # Select datasets
            if select_all:
                selected = ds
            else:
                selected = select_items(
                    items=ds,
                    title="Select Datasets to Migrate",
                    columns=[
                        {"key": "name", "title": "Name", "width": 40},
                        {"key": "id", "title": "ID", "width": 36},
                        {"key": "description", "title": "Description", "width": 50},
                        {"key": "example_count", "title": "Examples", "width": 10}
                    ]
                )

            if not selected:
                console.print("[yellow]No datasets selected[/yellow]")
                continue

            inc_exp = include_experiments
            if not inc_exp:
                inc_exp = Confirm.ask("\nInclude experiments with datasets?", default=False)

            console.print(f"\nSelected {len(selected)} dataset(s)")
            if inc_exp:
                console.print("[dim]Including experiments, runs, and feedback[/dim]")
            if config.migration.dry_run:
                console.print("[dim]Mode: Dry Run (no changes)[/dim]")

            if not Confirm.ask("\nProceed?"):
                console.print("[yellow]Cancelled[/yellow]")
                continue

            dataset_ids = [d["id"] for d in selected]

            try:
                orchestrator.migrate_datasets_parallel(
                    dataset_ids,
                    include_examples=True,
                    include_experiments=inc_exp
                )

                console.print("\n[green]✓[/green] Migration completed")

                if orchestrator.state:
                    stats = orchestrator.state.get_statistics()
                    if stats['completed'] > 0:
                        console.print(f"  Migrated: {stats['completed']} dataset(s)")
                    if stats['failed'] > 0:
                        console.print(f"  [red]Failed: {stats['failed']}[/red]")
                    if inc_exp and 'by_type' in stats and 'experiment' in stats['by_type']:
                        exp_stats = stats['by_type']['experiment']
                        console.print(f"  Experiments: {exp_stats['completed']} completed, {exp_stats['failed']} failed")

            except Exception as e:
                console.print(f"\n[red]Migration failed: {e}[/red]")

        if ws_result:
            orchestrator.clear_workspace_context()
    except Exception as e:
        console.print(f"\n[red]Migration failed: {e}[/red]")
        ctx.exit(1)
    finally:
        orchestrator.cleanup()


@cli.command()
@ssl_option
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
@ssl_option
@workspace_options
@click.pass_context
def queues(ctx, source_workspace, dest_workspace, map_workspaces):
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

    # Resolve workspace context
    ws_result = _resolve_workspaces(orchestrator, source_workspace, dest_workspace, map_workspaces)
    if ws_result is _WS_CANCELLED:
        console.print("[yellow]Cancelled[/yellow]")
        return

    ws_pairs = list(ws_result.workspace_mapping.items()) if ws_result else [(None, None)]

    queue_migrator = AnnotationQueueMigrator(
        orchestrator.source_client,
        orchestrator.dest_client,
        None,
        config
    )

    for src_ws, dst_ws in ws_pairs:
        if src_ws and dst_ws:
            orchestrator.set_workspace_context(src_ws, dst_ws)
            console.print(f"\n[bold cyan]Workspace: {src_ws} -> {dst_ws}[/bold cyan]")

        # Get queues
        console.print("\n[bold]Fetching annotation queues from source...[/bold]")

        queues = queue_migrator.list_queues()

        if not queues:
            console.print("[yellow]No annotation queues found[/yellow]")
            continue

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
            continue

        console.print(f"\n[bold]Migrating {len(selected_queues)} annotation queue(s)...[/bold]")

        # Perform migration
        success_count = 0
        failed_items = []

        with Progress(console=console) as progress:
            task = progress.add_task("Migrating queues...", total=len(selected_queues))
            for queue in selected_queues:
                try:
                    new_id = queue_migrator.create_queue(queue)
                    success_count += 1
                except Exception as e:
                    failed_items.append((queue['name'], str(e)))
                progress.advance(task)

        console.print(f"Queues: {success_count} migrated, {len(failed_items)} failed")
        if failed_items and config.migration.verbose:
            for name, err in failed_items:
                console.print(f"  [red]✗[/red] {name}: {err}")

    if ws_result:
        orchestrator.clear_workspace_context()


@cli.command()
@ssl_option
@click.option('--all', 'select_all', is_flag=True, help='Migrate all prompts')
@click.option('--include-all-commits', is_flag=True, help='Include all commit history')
@workspace_options
@click.pass_context
def prompts(ctx, select_all, include_all_commits, source_workspace, dest_workspace, map_workspaces):
    """Migrate prompts with interactive selection."""
    config = ctx.obj['config']
    state_manager = ctx.obj['state_manager']

    display_banner()

    if not ensure_config(config):
        return

    orchestrator = MigrationOrchestrator(config, state_manager)

    console.print("Testing connections... ", end="")
    source_ok, dest_ok, source_error, dest_error = orchestrator.test_connections_detailed()
    if not source_ok:
        console.print("[red]✗ Source connection failed[/red]")
        return
    if not dest_ok:
        console.print("[red]✗ Destination connection failed[/red]")
        return
    console.print("[green]✓[/green]")

    # Resolve workspace context
    ws_result = _resolve_workspaces(orchestrator, source_workspace, dest_workspace, map_workspaces)
    if ws_result is _WS_CANCELLED:
        console.print("[yellow]Cancelled[/yellow]")
        return

    ws_pairs = list(ws_result.workspace_mapping.items()) if ws_result else [(None, None)]

    # Create prompt migrator
    prompt_migrator = PromptMigrator(
        orchestrator.source_client,
        orchestrator.dest_client,
        None,
        config
    )

    for src_ws, dst_ws in ws_pairs:
        if src_ws and dst_ws:
            orchestrator.set_workspace_context(src_ws, dst_ws)
            console.print(f"\n[bold cyan]Workspace: {src_ws} -> {dst_ws}[/bold cyan]")

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
            continue
        console.print("[green]✓[/green]")

        console.print("Fetching prompts... ", end="")
        prompts = prompt_migrator.list_prompts()

        if not prompts:
            console.print("[yellow]none found[/yellow]")
            continue

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
            continue

        console.print(f"\nSelected {len(selected_prompts)} prompt(s)")

        if config.migration.dry_run:
            console.print("[dim]Mode: Dry Run (no changes)[/dim]")

        if include_all_commits:
            console.print("[dim]Including all commit history[/dim]")

        if not Confirm.ask("\nProceed?"):
            console.print("[yellow]Cancelled[/yellow]")
            continue

        success_count = 0
        has_405_error = False
        failed_items = []

        with Progress(console=console) as progress:
            task = progress.add_task("Migrating prompts...", total=len(selected_prompts))
            for prompt in selected_prompts:
                try:
                    result = prompt_migrator.migrate_prompt(
                        prompt['repo_handle'],
                        include_all_commits=include_all_commits
                    )
                    if result:
                        success_count += 1
                    else:
                        failed_items.append((prompt['repo_handle'], "migration returned None"))
                except Exception as e:
                    error_msg = str(e)
                    if "405" in error_msg or "Not Allowed" in error_msg:
                        has_405_error = True
                    failed_items.append((prompt['repo_handle'], error_msg))
                progress.advance(task)

        console.print(f"Prompts: {success_count} migrated, {len(failed_items)} failed")
        if failed_items and config.migration.verbose:
            for name, err in failed_items:
                console.print(f"  [red]✗[/red] {name}: {err}")
        if failed_items and has_405_error:
            console.print("\n[yellow]⚠ Some failures were due to 405 Not Allowed errors[/yellow]")
            console.print("[dim]This indicates the destination instance does not support prompt write operations.[/dim]")
            console.print("[dim]Possible solutions:[/dim]")
            console.print("[dim]  • Enable the prompts feature on your LangSmith instance[/dim]")
            console.print("[dim]  • Check nginx/proxy configuration for /api/v1/repos/* endpoints[/dim]")
            console.print("[dim]  • Contact your LangSmith administrator[/dim]")

    if ws_result:
        orchestrator.clear_workspace_context()

    orchestrator.cleanup()


@cli.command()
@ssl_option
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


@cli.command(name='list_workspaces')
@ssl_option
@click.option('--source', is_flag=True, help='List workspaces from source instance')
@click.option('--dest', is_flag=True, help='List workspaces from destination instance')
@click.pass_context
def list_workspaces_cmd(ctx, source, dest):
    """List workspaces accessible to the configured API keys."""
    config = ctx.obj['config']

    if not source and not dest:
        console.print("[yellow]Specify --source or --dest to list workspaces[/yellow]")
        return

    if not ensure_config(config):
        return

    orchestrator = MigrationOrchestrator(config, ctx.obj['state_manager'])

    if source:
        workspaces = _list_workspaces(orchestrator.source_client)
        display_workspaces(console, workspaces, "Source")

    if dest:
        workspaces = _list_workspaces(orchestrator.dest_client)
        display_workspaces(console, workspaces, "Destination")

    orchestrator.cleanup()


@cli.command()
@ssl_option
@click.option('--all', 'select_all', is_flag=True, help='Migrate all rules')
@click.option('--strip-projects', is_flag=True, help='Strip project associations and create as global rules')
@click.option('--project-mapping', type=str, help='JSON string or file path with project ID mapping (e.g., \'{"old-id": "new-id"}\')')
@click.option('--create-enabled', is_flag=True, help='Create rules as enabled (default is disabled to bypass API key/secrets validation)')
@click.option('--map-projects', is_flag=True, help='Launch interactive TUI to map source projects to destination projects')
@workspace_options
@click.pass_context
def rules(ctx, select_all, strip_projects, project_mapping, create_enabled, map_projects, source_workspace, dest_workspace, map_workspaces):
    """Migrate project rules (automation rules)."""
    config = ctx.obj['config']
    state_manager = ctx.obj['state_manager']

    display_banner()

    if not ensure_config(config):
        return

    orchestrator = MigrationOrchestrator(config, state_manager)

    console.print("Testing connections... ", end="")
    source_ok, dest_ok, source_error, dest_error = orchestrator.test_connections_detailed()
    if not source_ok:
        console.print("[red]✗ Source connection failed[/red]")
        return
    if not dest_ok:
        console.print("[red]✗ Destination connection failed[/red]")
        return
    console.print("[green]✓[/green]")

    # Resolve workspace context
    ws_result = _resolve_workspaces(orchestrator, source_workspace, dest_workspace, map_workspaces)
    if ws_result is _WS_CANCELLED:
        console.print("[yellow]Cancelled[/yellow]")
        return

    ws_pairs = list(ws_result.workspace_mapping.items()) if ws_result else [(None, None)]

    # --map-projects and --project-mapping are mutually exclusive
    if map_projects and project_mapping:
        console.print("[red]Error: --map-projects and --project-mapping are mutually exclusive[/red]")
        return

    # Parse custom project mapping once (outside loop, not workspace-scoped)
    custom_mapping = None
    if project_mapping:
        import json
        import os

        try:
            if os.path.isfile(project_mapping):
                with open(project_mapping, 'r') as f:
                    custom_mapping = json.load(f)
                console.print(f"Loaded project mapping from file: {project_mapping}")
            else:
                custom_mapping = json.loads(project_mapping)

            if not isinstance(custom_mapping, dict):
                console.print("[red]Error: Project mapping must be a JSON object/dict[/red]")
                return

            console.print(f"Using custom project mapping with {len(custom_mapping)} project(s)")

        except json.JSONDecodeError as e:
            console.print(f"[red]Error parsing project mapping JSON: {e}[/red]")
            return
        except Exception as e:
            console.print(f"[red]Error loading project mapping: {e}[/red]")
            return

    for src_ws, dst_ws in ws_pairs:
        if src_ws and dst_ws:
            orchestrator.set_workspace_context(src_ws, dst_ws)
            console.print(f"\n[bold cyan]Workspace: {src_ws} -> {dst_ws}[/bold cyan]")

        rules_migrator = RulesMigrator(
            orchestrator.source_client,
            orchestrator.dest_client,
            None,
            config
        )

        # Launch interactive TUI project mapper (inside loop for workspace-scoped projects)
        if map_projects:
            console.print("Fetching projects from both instances... ", end="")
            source_projects = _list_projects(orchestrator.source_client)
            dest_projects = _list_projects(orchestrator.dest_client)
            console.print(f"[green]✓[/green] ({len(source_projects)} source, {len(dest_projects)} destination)")

            name_mapping = build_project_mapping_tui(source_projects, dest_projects)
            if name_mapping is None:
                console.print("[yellow]Cancelled[/yellow]")
                continue

            id_map = _name_mapping_to_id_mapping(name_mapping, source_projects, dest_projects)
            rules_migrator._project_id_map = id_map
            console.print(f"Using interactive project mapping with {len(id_map)} project(s)")

        # Apply custom project mapping if provided
        if custom_mapping:
            rules_migrator._project_id_map = custom_mapping

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
            continue

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
                if rule.get('session_id') and rule.get('dataset_id'):
                    rule_copy['association'] = 'Project+Dataset'
                elif rule.get('session_id'):
                    rule_copy['association'] = 'Project'
                elif rule.get('dataset_id'):
                    rule_copy['association'] = 'Dataset'
                else:
                    rule_copy['association'] = 'None'

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
            continue

        console.print(f"\nSelected {len(selected_rules)} rule(s)")

        if config.migration.dry_run:
            console.print("[dim]Mode: Dry Run (no changes)[/dim]")

        if strip_projects:
            console.print("[dim]Mode: Stripping project associations (creating as global rules)[/dim]")

        if not Confirm.ask("\nProceed?"):
            console.print("[yellow]Cancelled[/yellow]")
            continue

        success_count = 0
        failed_items = []
        skipped_items = []

        with Progress(console=console) as progress:
            task = progress.add_task("Migrating rules...", total=len(selected_rules))
            for rule in selected_rules:
                try:
                    rule_name = rule.get('display_name') or rule.get('name', 'unnamed')
                    has_project = bool(rule.get('session_id'))
                    has_dataset = bool(rule.get('dataset_id'))
                    has_evaluators = bool(rule.get('evaluators') or rule.get('evaluator_prompt_handle'))

                    create_disabled = not create_enabled
                    result = rules_migrator.create_rule(rule, strip_project_reference=strip_projects, create_disabled=create_disabled)
                    if result:
                        success_count += 1
                    else:
                        if not has_dataset and not has_project:
                            skipped_items.append((rule_name, "no dataset or project"))
                        elif has_project and not has_dataset:
                            skipped_items.append((rule_name, "project not found in destination"))
                        elif has_evaluators:
                            failed_items.append((rule_name, "check prompts exist on destination"))
                        else:
                            failed_items.append((rule_name, "see verbose logs"))
                except Exception as e:
                    rule_name = rule.get('display_name') or rule.get('name', 'unnamed')
                    failed_items.append((rule_name, str(e)))
                progress.advance(task)

        console.print(f"Rules: {success_count} migrated, {len(skipped_items)} skipped, {len(failed_items)} failed")
        if (failed_items or skipped_items) and config.migration.verbose:
            for name, err in skipped_items:
                console.print(f"  [yellow]⊘[/yellow] {name}: {err}")
            for name, err in failed_items:
                console.print(f"  [red]✗[/red] {name}: {err}")

        # Show helpful message about disabled rules
        if success_count > 0 and not create_enabled:
            console.print(f"\n[cyan]Note:[/cyan] Rules were created as [yellow]disabled[/yellow] to bypass secrets validation.")
            console.print(f"  To enable rules:")
            console.print(f"  1. Configure required secrets (e.g., OPENAI_API_KEY) in destination workspace settings")
            console.print(f"  2. Enable each rule in the LangSmith UI or use --create-enabled flag")

    if ws_result:
        orchestrator.clear_workspace_context()

    orchestrator.cleanup()


@cli.command()
@ssl_option
@click.option('--skip-datasets', is_flag=True, help='Skip dataset migration')
@click.option('--skip-experiments', is_flag=True, help='Skip experiment migration')
@click.option('--skip-prompts', is_flag=True, help='Skip prompt migration')
@click.option('--skip-queues', is_flag=True, help='Skip annotation queue migration')
@click.option('--skip-rules', is_flag=True, help='Skip rules migration')
@click.option('--include-all-commits', is_flag=True, help='Include all prompt commit history')
@click.option('--strip-projects', is_flag=True, help='Strip project associations from rules')
@click.option('--map-projects', is_flag=True, help='Launch interactive TUI to map source projects to destination projects')
@workspace_options
@click.pass_context
def migrate_all(ctx, skip_datasets, skip_experiments, skip_prompts, skip_queues, skip_rules, include_all_commits, strip_projects, map_projects, source_workspace, dest_workspace, map_workspaces):
    """Migrate all resources interactively."""
    config = ctx.obj['config']
    state_manager = ctx.obj['state_manager']

    display_banner()

    if not ensure_config(config):
        return

    orchestrator = MigrationOrchestrator(config, state_manager)

    # Test connections first
    console.print("Testing connections... ", end="")
    source_ok, dest_ok, source_error, dest_error = orchestrator.test_connections_detailed()
    if not source_ok:
        console.print("[red]✗ Source connection failed[/red]")
        return
    if not dest_ok:
        console.print("[red]✗ Destination connection failed[/red]")
        return
    console.print("[green]✓[/green]\n")

    # Resolve workspace context (runs before asset discovery)
    ws_result = _resolve_workspaces(orchestrator, source_workspace, dest_workspace, map_workspaces)
    if ws_result is _WS_CANCELLED:
        console.print("[yellow]Cancelled[/yellow]")
        return

    console.print("[bold cyan]LangSmith Data Migration Wizard[/bold cyan]\n")
    console.print("This wizard will guide you through migrating all your data.\n")

    # If multi-workspace, iterate per pair; otherwise run once
    ws_pairs = list(ws_result.workspace_mapping.items()) if ws_result else [(None, None)]

    for ws_idx, (src_ws, dst_ws) in enumerate(ws_pairs):
        if src_ws and dst_ws:
            orchestrator.set_workspace_context(src_ws, dst_ws)
            console.print(f"\n[bold cyan]━━━ Workspace {ws_idx + 1}/{len(ws_pairs)}: {src_ws} -> {dst_ws} ━━━[/bold cyan]\n")

        # Use per-workspace project mapping from the TUI if available
        ws_project_mapping = None
        if ws_result and src_ws and src_ws in ws_result.project_mappings:
            ws_project_mapping = ws_result.project_mappings[src_ws]

        _migrate_all_for_workspace(ctx, orchestrator, config, skip_datasets, skip_experiments,
                                   skip_prompts, skip_queues, skip_rules, include_all_commits,
                                   strip_projects, map_projects, ws_project_mapping)

    if ws_result:
        orchestrator.clear_workspace_context()

    console.print("\n[bold green]Migration wizard completed![/bold green]")
    orchestrator.cleanup()


def _migrate_all_for_workspace(ctx, orchestrator, config, skip_datasets, skip_experiments,
                                skip_prompts, skip_queues, skip_rules, include_all_commits,
                                strip_projects, map_projects, ws_project_mapping=None):
    """Run the full migrate_all flow for a single workspace pair (or no workspace).

    Args:
        ws_project_mapping: Optional name-based project mapping from workspace TUI.
            If provided, --map-projects is skipped (already done at workspace level).
    """

    # Launch interactive TUI project mapper if requested (and not already done at workspace level)
    project_id_map = None
    if ws_project_mapping:
        # Convert the name mapping from the workspace TUI to an ID mapping
        console.print("Fetching projects for project mapping... ", end="")
        source_projects = _list_projects(orchestrator.source_client)
        dest_projects = _list_projects(orchestrator.dest_client)
        console.print(f"[green]✓[/green] ({len(source_projects)} source, {len(dest_projects)} destination)")
        project_id_map = _name_mapping_to_id_mapping(ws_project_mapping, source_projects, dest_projects)
        console.print(f"Using workspace-scoped project mapping with {len(project_id_map)} project(s)\n")
    elif map_projects:
        console.print("Fetching projects from both instances... ", end="")
        source_projects = _list_projects(orchestrator.source_client)
        dest_projects = _list_projects(orchestrator.dest_client)
        console.print(f"[green]✓[/green] ({len(source_projects)} source, {len(dest_projects)} destination)")

        name_mapping = build_project_mapping_tui(source_projects, dest_projects)
        if name_mapping is None:
            console.print("[yellow]Cancelled[/yellow]")
            return

        project_id_map = _name_mapping_to_id_mapping(name_mapping, source_projects, dest_projects)
        console.print(f"Using interactive project mapping with {len(project_id_map)} project(s)\n")

    # Track dataset ID mappings for use in rules migration
    dataset_id_mapping = {}

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
                    dataset_id_mapping = orchestrator.migrate_datasets_parallel(dataset_ids, include_examples=True, include_experiments=include_exp)
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
                failed_items = []

                with Progress(console=console) as progress:
                    task = progress.add_task("Migrating prompts...", total=len(prompts))
                    for prompt in prompts:
                        try:
                            result = prompt_migrator.migrate_prompt(
                                prompt['repo_handle'],
                                include_all_commits=include_history
                            )
                            if result:
                                success_count += 1
                            else:
                                failed_items.append((prompt['repo_handle'], "migration returned None"))
                        except Exception as e:
                            failed_items.append((prompt['repo_handle'], str(e)))
                        progress.advance(task)

                console.print(f"Prompts: {success_count} migrated, {len(failed_items)} failed")
                if failed_items and config.migration.verbose:
                    for name, err in failed_items:
                        console.print(f"  [red]✗[/red] {name}: {err}")
                console.print()
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
                failed_items = []

                with Progress(console=console) as progress:
                    task = progress.add_task("Migrating queues...", total=len(queues))
                    for queue in queues:
                        try:
                            new_id = queue_migrator.create_queue(queue)
                            success_count += 1
                        except Exception as e:
                            failed_items.append((queue['name'], str(e)))
                        progress.advance(task)

                console.print(f"Queues: {success_count} migrated, {len(failed_items)} failed")
                if failed_items and config.migration.verbose:
                    for name, err in failed_items:
                        console.print(f"  [red]✗[/red] {name}: {err}")
                console.print()
            else:
                console.print("[yellow]Skipped queues[/yellow]\n")
        else:
            console.print("[yellow]none found[/yellow]\n")
    else:
        console.print("[dim]Skipping queues (--skip-queues)[/dim]\n")

    # 4. Rules (evaluators)
    if not skip_rules:
        console.print("[bold]Step 4: Rules (Evaluators)[/bold]")
        console.print("[dim]Note: LLM evaluators reference prompts via hub_ref. Prompts were migrated in Step 2.[/dim]")
        console.print("Fetching rules... ", end="")
        from ..core.migrators import RulesMigrator
        rules_migrator = RulesMigrator(
            orchestrator.source_client,
            orchestrator.dest_client,
            None,
            config
        )

        # Pass dataset ID mapping from Step 1 so rules can reference correct destination datasets
        if dataset_id_mapping:
            rules_migrator._dataset_id_map = dataset_id_mapping
            console.print(f"[dim]Using dataset mapping from Step 1 ({len(dataset_id_mapping)} dataset(s))[/dim]")

        # Apply interactive project mapping if provided
        if project_id_map:
            rules_migrator._project_id_map = project_id_map
            console.print(f"[dim]Using interactive project mapping ({len(project_id_map)} project(s))[/dim]")

        rules = rules_migrator.list_rules()

        if rules:
            console.print(f"found {len(rules)}")

            # Check for rules with LLM evaluators
            rules_with_evaluators = [r for r in rules if r.get('evaluators') or r.get('evaluator_prompt_handle')]
            rules_with_code_evaluators = [r for r in rules if r.get('code_evaluators')]
            if rules_with_evaluators:
                console.print(f"[dim]  - {len(rules_with_evaluators)} rule(s) have LLM evaluators[/dim]")
            if rules_with_code_evaluators:
                console.print(f"[dim]  - {len(rules_with_code_evaluators)} rule(s) have code evaluators[/dim]")

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
                failed_items = []
                skipped_items = []

                with Progress(console=console) as progress:
                    task = progress.add_task("Migrating rules...", total=len(rules))
                    for rule in rules:
                        try:
                            has_project = bool(rule.get('session_id'))
                            has_dataset = bool(rule.get('dataset_id'))
                            has_evaluators = bool(rule.get('evaluators') or rule.get('evaluator_prompt_handle'))
                            rule_name = rule.get('display_name') or rule.get('name', 'unnamed')

                            result = rules_migrator.create_rule(
                                rule,
                                strip_project_reference=strip,
                                ensure_project=ensure_projects
                            )
                            if result:
                                success_count += 1
                            else:
                                if not has_dataset and not has_project:
                                    skipped_items.append((rule_name, "no dataset or project"))
                                elif has_project and not has_dataset and not ensure_projects:
                                    skipped_items.append((rule_name, "project not found in destination"))
                                elif has_evaluators:
                                    failed_items.append((rule_name, "check prompts exist on destination"))
                                else:
                                    failed_items.append((rule_name, "see verbose logs"))
                        except Exception as e:
                            rule_name = rule.get('display_name') or rule.get('name', 'unnamed')
                            failed_items.append((rule_name, str(e)))
                        progress.advance(task)

                console.print(f"Rules: {success_count} migrated, {len(skipped_items)} skipped, {len(failed_items)} failed")
                if (failed_items or skipped_items) and config.migration.verbose:
                    for name, err in skipped_items:
                        console.print(f"  [yellow]⊘[/yellow] {name}: {err}")
                    for name, err in failed_items:
                        console.print(f"  [red]✗[/red] {name}: {err}")
                console.print()
            else:
                console.print("[yellow]Skipped rules[/yellow]\n")
        else:
            console.print("[yellow]none found[/yellow]\n")
    else:
        console.print("[dim]Skipping rules (--skip-rules)[/dim]\n")


@cli.command()
@ssl_option
@click.option('--session', help='Migrate charts for a specific session/project (by name or ID)')
@click.option('--same-instance', is_flag=True, help='Source and destination are the same instance (use same session IDs)')
@click.option('--map-projects', is_flag=True, help='Launch interactive TUI to map source projects to destination projects')
@workspace_options
@click.pass_context
def charts(ctx, session, same_instance, map_projects, source_workspace, dest_workspace, map_workspaces):
    """Migrate monitoring charts from sessions/projects."""
    config = ctx.obj['config']
    state_manager = ctx.obj['state_manager']

    display_banner()

    if not ensure_config(config):
        return

    orchestrator = MigrationOrchestrator(config, state_manager)

    console.print("Testing connections... ", end="")
    source_ok, dest_ok, source_error, dest_error = orchestrator.test_connections_detailed()
    if not source_ok:
        console.print("[red]✗ Source connection failed[/red]")
        return
    if not dest_ok:
        console.print("[red]✗ Destination connection failed[/red]")
        return
    console.print("[green]✓[/green]")

    # Resolve workspace context
    ws_result = _resolve_workspaces(orchestrator, source_workspace, dest_workspace, map_workspaces)
    if ws_result is _WS_CANCELLED:
        console.print("[yellow]Cancelled[/yellow]")
        return

    ws_pairs = list(ws_result.workspace_mapping.items()) if ws_result else [(None, None)]

    # Auto-detect if same instance (same base URL) — not workspace-scoped
    if not same_instance:
        source_url = config.source.base_url.rstrip('/').lower()
        dest_url = config.destination.base_url.rstrip('/').lower()
        if source_url == dest_url:
            if config.source.api_key == config.destination.api_key:
                same_instance = True
                console.print("[dim]Detected same source and destination instance (same URL and API key)[/dim]")
            else:
                console.print("[dim]Detected same instance URL but different API keys (likely different workspaces).[/dim]")
                console.print("[dim]Will use project name matching instead of same session IDs.[/dim]")

    for src_ws, dst_ws in ws_pairs:
        if src_ws and dst_ws:
            orchestrator.set_workspace_context(src_ws, dst_ws)
            console.print(f"\n[bold cyan]Workspace: {src_ws} -> {dst_ws}[/bold cyan]")

        chart_migrator = ChartMigrator(
            orchestrator.source_client,
            orchestrator.dest_client,
            orchestrator.state,
            config
        )

        # Launch interactive TUI project mapper (inside loop for workspace-scoped projects)
        if map_projects:
            console.print("Fetching projects from both instances... ", end="")
            source_projects = _list_projects(orchestrator.source_client)
            dest_projects = _list_projects(orchestrator.dest_client)
            console.print(f"[green]✓[/green] ({len(source_projects)} source, {len(dest_projects)} destination)")

            name_mapping = build_project_mapping_tui(source_projects, dest_projects)
            if name_mapping is None:
                console.print("[yellow]Cancelled[/yellow]")
                continue

            id_map = _name_mapping_to_id_mapping(name_mapping, source_projects, dest_projects)
            chart_migrator._project_id_map = id_map
            console.print(f"Using interactive project mapping with {len(id_map)} project(s)")

        if session:
            # Migrate charts for a specific session
            console.print(f"\n[bold]Migrating charts for session: {session}[/bold]\n")

            console.print("Looking up session... ", end="")
            sessions = chart_migrator.list_sessions()

            if not sessions:
                console.print("[red]✗[/red]")
                console.print("[red]No sessions found in source[/red]")
                continue

            target_session = None
            for s in sessions:
                if s.get('id') == session or s.get('name') == session:
                    target_session = s
                    break

            if not target_session:
                console.print("[red]✗[/red]")
                console.print(f"[red]Session not found: {session}[/red]")
                console.print(f"\n[yellow]Available sessions:[/yellow]")
                for s in sessions[:10]:
                    console.print(f"  - {s.get('name', 'unnamed')} ({s.get('id', 'no-id')})")
                if len(sessions) > 10:
                    console.print(f"  ... and {len(sessions) - 10} more")
                continue

            console.print(f"[green]✓[/green] Found: {target_session.get('name', 'unnamed')}")
            source_session_id = target_session['id']

            if same_instance:
                dest_session_id = source_session_id
                console.print("[dim]Using same session ID for destination[/dim]\n")
            else:
                dest_session_id = orchestrator.state.get_mapped_id('project', source_session_id)
                if not dest_session_id:
                    console.print(f"[red]No destination mapping found for session {session}[/red]")
                    console.print("\n[yellow]This means the project/session hasn't been migrated yet.[/yellow]")
                    console.print("[yellow]Options:[/yellow]")
                    console.print("  1. Run 'langsmith-migrator datasets' first to migrate projects")
                    console.print("  2. Use --same-instance flag if source and dest are the same")
                    continue
                console.print(f"[dim]Mapped to destination session: {dest_session_id[:8]}...[/dim]\n")

            chart_mappings = chart_migrator.migrate_session_charts(
                source_session_id,
                dest_session_id
            )

            console.print(f"\n[green]✓[/green] Migrated {len(chart_mappings)} chart(s)")

        else:
            # Migrate all charts from all sessions
            console.print("\n[bold]Migrating charts from all sessions...[/bold]")

            if same_instance:
                console.print("[dim]Mode: Same instance (using same session IDs)[/dim]")
            else:
                console.print("[dim]Mode: Different instances (requires session ID mappings)[/dim]")

            console.print()

            if not Confirm.ask("Proceed with migration?"):
                console.print("[yellow]Cancelled[/yellow]")
                continue

            console.print()
            all_mappings = chart_migrator.migrate_all_charts(same_instance=same_instance)

            if all_mappings:
                console.print("\n[green]✓ Chart migration completed successfully[/green]")
            else:
                console.print("\n[yellow]No charts were migrated[/yellow]")

            if config.migration.verbose and all_mappings:
                console.print("\n[dim]Detailed chart mappings:[/dim]")
                for session_id, chart_map in all_mappings.items():
                    console.print(f"  Session {session_id[:8]}...: {len(chart_map)} charts")

    if ws_result:
        orchestrator.clear_workspace_context()

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
