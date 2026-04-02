"""Workspace resolution: detect multi-workspace environments and build mappings."""

from typing import Dict, List, Optional

from rich.console import Console
from rich.prompt import Confirm
from rich.table import Table

from ..core.api_client import EnhancedAPIClient
from .workspace import create_workspace, get_workspace_name, list_workspaces, list_projects
from .migration_config import MigrationFileConfig, load_config, save_config
from ..cli.tui_workspace_mapper import WorkspaceProjectResult, build_workspace_mapping_tui


def resolve_workspace_context(
    source_client: EnhancedAPIClient,
    dest_client: EnhancedAPIClient,
    console: Console,
    saved_config: Optional[MigrationFileConfig] = None,
    force_tui: bool = False,
    non_interactive: bool = False,
) -> Optional[WorkspaceProjectResult]:
    """Detect workspaces on both sides and resolve a mapping if needed.

    Steps:
        1. Discover workspaces on source and destination.
        2. If both sides have <= 1 workspace and ``force_tui`` is False, return None.
        3. If ``saved_config`` has a workspace_mapping, offer to reuse it.
        4. Otherwise, launch the workspace mapping TUI.
        5. Create any new destination workspaces.
        6. Persist the mapping to the config file.

    Returns:
        A WorkspaceProjectResult with workspace and project mappings,
        or None if no mapping is needed / user cancelled.
    """
    console.print("Discovering workspaces... ", end="")
    source_workspaces = list_workspaces(source_client)
    dest_workspaces = list_workspaces(dest_client)
    console.print(
        f"[green]\u2713[/green] ({len(source_workspaces)} source, {len(dest_workspaces)} destination)"
    )

    # If both sides are single-workspace (or empty), no mapping needed
    if not force_tui and len(source_workspaces) <= 1 and len(dest_workspaces) <= 1:
        return None

    # Lazy-load saved config if not provided
    if saved_config is None:
        saved_config = load_config()

    # Check for saved mapping
    if saved_config and saved_config.workspace_mapping:
        _display_saved_mapping(console, saved_config.workspace_mapping, source_workspaces, dest_workspaces)
        if non_interactive or Confirm.ask("Reuse this workspace mapping?", default=True):
            return WorkspaceProjectResult(
                workspace_mapping=saved_config.workspace_mapping,
                project_mappings={},
                workspaces_to_create=[],
            )

    # In non-interactive mode, we cannot launch the TUI
    if non_interactive:
        console.print("[red]Error: --map-workspaces requires interactive mode (no saved mapping found)[/red]")
        return None

    # Build a callback for fetching projects scoped to a workspace
    def fetch_projects(ws_id: str, side: str) -> List[Dict]:
        client = source_client if side == "source" else dest_client
        original_header = client.session.headers.get("X-Tenant-Id")
        client.set_workspace(ws_id)
        try:
            return list_projects(client)
        finally:
            # Restore previous workspace
            if original_header:
                client.set_workspace(original_header)
            else:
                client.set_workspace(None)

    # Launch TUI
    tui_result = build_workspace_mapping_tui(
        source_workspaces,
        dest_workspaces,
        fetch_projects=fetch_projects,
        existing_mapping=(saved_config.workspace_mapping if saved_config else None),
    )

    if tui_result is None:
        return None

    # Create new destination workspaces
    if tui_result.workspaces_to_create:
        console.print(f"\nCreating {len(tui_result.workspaces_to_create)} new workspace(s) on destination...")
        for ws_spec in tui_result.workspaces_to_create:
            try:
                new_ws = create_workspace(dest_client, ws_spec["display_name"])
                new_id = new_ws.get("id", "")
                tui_result.workspace_mapping[ws_spec["source_id"]] = new_id
                console.print(f"  [green]\u2713[/green] Created: {ws_spec['display_name']} ({new_id[:8]}...)")
            except Exception as e:
                console.print(f"  [red]\u2717[/red] Failed to create {ws_spec['display_name']}: {e}")

    # Persist mapping
    if tui_result.workspace_mapping:
        config = saved_config or load_config() or MigrationFileConfig()
        config.workspace_mapping = tui_result.workspace_mapping
        path = save_config(config)
        console.print(f"[dim]Workspace mapping saved to {path}[/dim]")

    return tui_result if tui_result.workspace_mapping else None


def display_workspaces(
    console: Console,
    workspaces: List[Dict],
    label: str,
) -> None:
    """Display a list of workspaces in a table."""
    if not workspaces:
        console.print(f"  {label}: [dim]none found[/dim]")
        return

    table = Table(title=f"{label} Workspaces", show_header=True)
    table.add_column("Name", style="cyan", width=40)
    table.add_column("ID", style="dim", width=36)
    table.add_column("Handle", style="dim", width=20)

    for ws in workspaces:
        table.add_row(
            get_workspace_name(ws),
            ws.get("id", ""),
            ws.get("tenant_handle", ""),
        )

    console.print(table)


def _display_saved_mapping(
    console: Console,
    mapping: Dict[str, str],
    source_workspaces: List[Dict],
    dest_workspaces: List[Dict],
) -> None:
    """Display a previously saved workspace mapping."""
    src_id_to_name = {ws.get("id", ""): get_workspace_name(ws) for ws in source_workspaces}
    dst_id_to_name = {ws.get("id", ""): get_workspace_name(ws) for ws in dest_workspaces}

    console.print("\n[bold]Saved workspace mapping:[/bold]")
    for src_id, dst_id in mapping.items():
        src_name = src_id_to_name.get(src_id, src_id[:12] + "...")
        dst_name = dst_id_to_name.get(dst_id, dst_id[:12] + "...")
        console.print(f"  {src_name} -> {dst_name}")
    console.print()
