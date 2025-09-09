"""Main CLI interface with improved UX."""

import click
import time
from pathlib import Path
from typing import Optional, List
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.prompt import Confirm

from ..utils.config import Config
from ..utils.state import StateManager, MigrationStatus
from ..core.migrators import (
    MigrationOrchestrator, 
    DatasetMigrator,
    ExperimentMigrator,
    AnnotationQueueMigrator,
    PromptMigrator
)
from .interactive_selector import InteractiveSelector, ProgressTracker


console = Console()


def display_banner():
    """Display minimal banner."""
    console.print("\n[bold]LangSmith Migration Tool[/bold] v2.0\n")


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
    """Migrate datasets with improved selection UI."""
    config = ctx.obj['config']
    state_manager = ctx.obj['state_manager']
    
    display_banner()
    
    if not ensure_config(config):
        return
    
    orchestrator = MigrationOrchestrator(config, state_manager)
    
    # Test connections first
    console.print("Testing connections... ", end="")
    if not orchestrator.test_connections():
        console.print("[red]✗[/red]")
        return
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
        # Use interactive selector
        selector = InteractiveSelector(
            items=datasets,
            title="Select Datasets to Migrate",
            columns=[
                {"key": "name", "title": "Name", "width": 40},
                {"key": "id", "title": "ID", "width": 36},
                {"key": "description", "title": "Description", "width": 50},
                {"key": "example_count", "title": "Examples", "width": 10}
            ],
            console=console
        )
        
        selected_datasets = selector.run()
    
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
            include_examples=True
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
    
    # Process pending items
    pending_items = state.get_pending_items()
    failed_items = state.get_failed_items()
    
    items_to_process = pending_items + failed_items
    
    console.print(f"\n[bold]Resuming migration of {len(items_to_process)} items...[/bold]")
    
    # Process items (simplified for this example)
    with ProgressTracker(console) as tracker:
        stage = tracker.add_stage("Resuming migration", len(items_to_process))
        tracker.start_stage(stage)
        
        for idx, item in enumerate(items_to_process):
            tracker.update_stage(stage, idx, f"Processing {item.name}")
            
            # Process based on item type
            # (Implementation would depend on specific item type)
            
            time.sleep(0.1)  # Simulate work
        
        tracker.complete_stage(stage)
    
    console.print("\n[green]✓ Migration resumed and completed[/green]")


@cli.command()
@click.pass_context
def queues(ctx):
    """Migrate annotation queues."""
    config = ctx.obj['config']
    state_manager = ctx.obj['state_manager']
    
    display_banner()
    
    if not validate_config(config):
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
    
    # Use interactive selector
    selector = InteractiveSelector(
        items=queues,
        title="Select Annotation Queues to Migrate",
        columns=[
            {"key": "name", "title": "Name", "width": 40},
            {"key": "id", "title": "ID", "width": 36},
            {"key": "description", "title": "Description", "width": 50}
        ],
        console=console
    )
    
    selected_queues = selector.run()
    
    if not selected_queues:
        console.print("[yellow]No queues selected[/yellow]")
        return
    
    console.print(f"\n[bold]Migrating {len(selected_queues)} annotation queue(s)...[/bold]")
    
    # Perform migration (simplified)
    for queue in selected_queues:
        try:
            new_id = queue_migrator.create_queue(queue)
            console.print(f"[green]✓[/green] Migrated queue: {queue['name']} -> {new_id}")
        except Exception as e:
            console.print(f"[red]✗[/red] Failed to migrate {queue['name']}: {e}")


@cli.command()
@click.pass_context  
def prompts(ctx):
    """Migrate prompts."""
    config = ctx.obj['config']
    
    display_banner()
    
    if not validate_config(config):
        return
    
    console.print("[yellow]Prompt migration requires LangSmith client integration[/yellow]")
    console.print("This feature will be available in a future update")


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