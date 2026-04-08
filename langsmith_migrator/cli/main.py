"""Simplified CLI interface with improved architecture."""

import csv
import functools
import logging
from pathlib import Path
from typing import Iterable
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
from ..utils.state import MigrationStatus, ResolutionOutcome, StateManager, VerificationState
from ..core.migrators import (
    MigrationOrchestrator,
    DatasetMigrator,
    AnnotationQueueMigrator,
    PromptMigrator,
    RulesMigrator,
    ChartMigrator,
    UserRoleMigrator,
)
from .tui_selector import select_items
from .tui_project_mapper import build_project_mapping_tui
from .tui_workspace_mapper import WorkspaceProjectResult
from ..utils.workspace import (
    discover_workspaces,
    get_workspace_name,
    list_projects as _list_projects,
    list_workspaces as _list_workspaces,
)
from ..utils.workspace_resolver import (
    WorkspaceResolutionError,
    display_workspaces,
    resolve_workspace_context,
)


console = Console()


class _SuppressRunCompressionNoise(logging.Filter):
    """Hide known low-signal LangSmith run compression info logs."""

    def filter(self, record: logging.LogRecord) -> bool:
        message = record.getMessage().lower()
        return "run compression is not enabled" not in message


def _install_log_filters() -> None:
    """Install process-wide filters for noisy third-party logs."""
    noise_filter = _SuppressRunCompressionNoise()
    root_logger = logging.getLogger()

    for handler in root_logger.handlers:
        handler.addFilter(noise_filter)

    # Also attach directly to the common LangSmith logger namespace.
    logging.getLogger("langsmith").addFilter(noise_filter)


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
_WS_ABORTED = "__aborted__"


def _resolve_workspaces(
    orchestrator,
    source_workspace=None,
    dest_workspace=None,
    map_workspaces=False,
    non_interactive=False,
):
    """Resolve workspace context from explicit IDs or auto-detection.

    Returns:
        - WorkspaceProjectResult if workspace scoping is active
        - None if no workspace scoping is needed (single-workspace or none found)
        - _WS_CANCELLED sentinel if the user explicitly cancelled the TUI
        - _WS_ABORTED sentinel if workspace resolution failed safely
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
    try:
        result = resolve_workspace_context(
            orchestrator.source_client,
            orchestrator.dest_client,
            console,
            force_tui=map_workspaces,
            non_interactive=non_interactive,
        )
    except WorkspaceResolutionError:
        return _WS_ABORTED

    # If force_tui was set and user cancelled, treat as abort
    if result is None and map_workspaces:
        return _WS_CANCELLED

    return result


def _workspace_scoped_project_id_map(orchestrator, ws_result, source_workspace_id):
    """Resolve a workspace TUI name mapping into IDs for standalone commands."""
    if not ws_result or not source_workspace_id:
        return None

    name_mapping = ws_result.project_mappings.get(source_workspace_id)
    if not name_mapping:
        return None

    console.print("Fetching projects for workspace-scoped mapping... ", end="")
    source_projects = _list_projects(orchestrator.source_client)
    dest_projects = _list_projects(orchestrator.dest_client)
    console.print(
        f"[green]✓[/green] ({len(source_projects)} source, {len(dest_projects)} destination)"
    )

    id_map = _name_mapping_to_id_mapping(name_mapping, source_projects, dest_projects)
    console.print(f"Using workspace-scoped project mapping with {len(id_map)} project(s)")
    return id_map


def _active_workspace_pair(orchestrator):
    """Return the currently scoped workspace pair."""
    return {
        "source": orchestrator.source_client.session.headers.get("X-Tenant-Id"),
        "dest": orchestrator.dest_client.session.headers.get("X-Tenant-Id"),
    }


def _state_item_id(item_type, source_id, source_workspace_id=None):
    """Build a stable state item ID, scoped by source workspace when available."""
    workspace_part = source_workspace_id or "default"
    return f"{item_type}_{workspace_part}_{source_id}"


def _ensure_migration_session(orchestrator, config):
    """Create a migration session lazily for non-dataset flows."""
    if orchestrator.state is None:
        orchestrator.state = orchestrator.state_manager.create_session(
            config.source.base_url,
            config.destination.base_url,
        )
    elif not orchestrator.state.remediation_bundle_path:
        orchestrator.state.remediation_bundle_path = str(
            orchestrator.state_manager._default_bundle_path(orchestrator.state.session_id).resolve()
        )
    config.state_manager = orchestrator.state_manager
    orchestrator.state_manager.current_state = orchestrator.state
    return orchestrator.state


def _ensure_state_item(orchestrator, config, item_type, source_id, name, metadata=None):
    """Ensure a CLI-selected item is tracked in migration state."""
    state = _ensure_migration_session(orchestrator, config)
    workspace_pair = _active_workspace_pair(orchestrator)
    item_id = _state_item_id(item_type, source_id, workspace_pair["source"])
    state.ensure_item(
        item_id,
        item_type,
        name,
        source_id,
        stage="selected",
        workspace_pair=workspace_pair,
        metadata=metadata or {},
    )
    orchestrator.state_manager.save()
    return item_id


def _mark_state_item_started(orchestrator, item_id):
    """Mark a tracked CLI item as in progress."""
    if not orchestrator.state:
        return
    orchestrator.state.update_item_status(
        item_id,
        MigrationStatus.IN_PROGRESS,
        stage="migrating",
    )
    orchestrator.state_manager.save()


def _mark_state_item_completed(orchestrator, item_id, destination_id=None, metadata=None):
    """Mark a tracked CLI item as completed."""
    if not orchestrator.state:
        return
    orchestrator.state.update_item_status(
        item_id,
        MigrationStatus.COMPLETED,
        destination_id=destination_id,
        stage="completed",
    )
    if metadata:
        orchestrator.state.update_item_checkpoint(item_id, metadata=metadata)
    item = orchestrator.state.get_item(item_id)
    if item and not item.terminal_state:
        orchestrator.state.mark_terminal(
            item_id,
            ResolutionOutcome.MIGRATED,
            f"{item.type}_migrated",
            verification_state=VerificationState.VERIFIED,
            evidence={"destination_id": destination_id} if destination_id else None,
        )
    orchestrator.state_manager.save()


def _mark_state_item_failed(orchestrator, item_id, error):
    """Mark a tracked CLI item as failed."""
    if not orchestrator.state:
        return
    orchestrator.state.update_item_status(
        item_id,
        MigrationStatus.FAILED,
        error=str(error),
        stage="failed",
    )
    item = orchestrator.state.get_item(item_id)
    if item and not item.terminal_state:
        orchestrator.state.mark_terminal(
            item_id,
            ResolutionOutcome.BLOCKED_WITH_CHECKPOINT,
            f"{item.type}_blocked",
            verification_state=VerificationState.BLOCKED,
            next_action="Review the remediation bundle, resolve the issue, and run `langsmith-migrator resume`.",
            evidence={"error": str(error)},
            error=str(error),
        )
    orchestrator.state_manager.save()


def _apply_item_workspace(orchestrator, item):
    """Restore the recorded workspace scope for a resumed item."""
    workspace_pair = getattr(item, "workspace_pair", {}) or {}
    source_ws = workspace_pair.get("source")
    dest_ws = workspace_pair.get("dest")
    if source_ws and dest_ws:
        orchestrator.set_workspace_context(source_ws, dest_ws)
    else:
        orchestrator.clear_workspace_context()


def _confirm_action(config: Config, prompt: str, *, default: bool = False, non_interactive_value: bool | None = None) -> bool:
    """Ask for confirmation unless the command is running in non-interactive mode."""
    if config.migration.non_interactive:
        return default if non_interactive_value is None else non_interactive_value
    return Confirm.ask(prompt, default=default)


_MEMBER_COLUMNS = [
    {"key": "email", "title": "Email", "width": 40},
    {"key": "full_name", "title": "Name", "width": 30},
    {"key": "role_name", "title": "Role", "width": 25},
]


def _select_or_all(config: Config, items: list, *, select_all: bool, title: str, columns: list[dict]) -> list:
    """Return interactive selections or all items in non-interactive mode."""
    if select_all or config.migration.non_interactive:
        return items
    return select_items(items=items, title=title, columns=columns)


def _load_members_csv(path: str) -> list[dict]:
    """Load and validate a members CSV file.

    Expected columns: ``email``, ``langsmith_role``.
    Optional columns: ``workspace_id`` (empty for org-level rows),
    ``workspace_name`` (informational, ignored).

    Click's ``type=click.Path(exists=True, dir_okay=False)`` already
    validates existence and file-type before this function is called.
    """
    csv_path = Path(path)
    required_columns = {"email", "langsmith_role"}

    try:
        # utf-8-sig allows BOM-prefixed files exported by spreadsheet tools.
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle)
            fieldnames = set(reader.fieldnames or [])
            missing = sorted(required_columns - fieldnames)
            if missing:
                raise click.ClickException(
                    "Members CSV is missing required columns: "
                    + ", ".join(missing)
                )

            rows: list[dict] = []
            for index, row in enumerate(reader, start=2):
                email = (row.get("email") or "").strip().lower()
                langsmith_role = (row.get("langsmith_role") or "").strip()
                workspace_id = (row.get("workspace_id") or "").strip()

                if not email:
                    raise click.ClickException(
                        f"Members CSV row {index} has an empty email value"
                    )
                if not langsmith_role:
                    raise click.ClickException(
                        f"Members CSV row {index} has an empty langsmith_role value"
                    )

                rows.append(
                    {
                        "email": email,
                        "langsmith_role": langsmith_role,
                        "workspace_id": workspace_id,
                    }
                )
    except click.ClickException:
        raise
    except csv.Error as e:
        raise click.ClickException(f"Failed parsing members CSV: {e}") from e
    except OSError as e:
        raise click.ClickException(f"Failed reading members CSV: {e}") from e

    if not rows:
        raise click.ClickException("Members CSV is empty")
    return rows


_BUILTIN_ROLE_ALIASES: dict[str, str] = {
    "organization admin": "ORGANIZATION_ADMIN",
    "organization user": "ORGANIZATION_USER",
    "workspace admin": "WORKSPACE_ADMIN",
}
_ORG_SCOPED_BUILTIN_ROLE_NAMES = {"ORGANIZATION_ADMIN", "ORGANIZATION_USER"}
_WORKSPACE_SCOPED_BUILTIN_ROLE_NAMES = {"WORKSPACE_ADMIN"}


def _resolve_csv_role_names(
    csv_rows: list[dict], source_roles: list[dict]
) -> tuple[list[dict], str | None]:
    """Resolve human-readable role names to source role IDs.

    Returns ``(resolved_rows, org_user_role_id)`` where each resolved row
    has ``role_id`` instead of ``langsmith_role``, and *org_user_role_id*
    is the source role ID for ORGANIZATION_USER (used as the default org
    role for users who only appear in workspace rows). Built-in role
    identifiers and documented aliases take precedence over colliding
    custom role display names.
    """
    role_lookup: dict[str, list[tuple[str, str]]] = {}
    builtin_by_name: dict[str, str] = {}
    preferred_builtin_by_label: dict[str, str] = {}
    org_user_role_id: str | None = None

    def register_role_label(label: str, role_id: str, role_name: str) -> None:
        candidates = role_lookup.setdefault(label, [])
        if not any(existing_role_id == role_id for existing_role_id, _ in candidates):
            candidates.append((role_id, role_name))

    for role in source_roles:
        role_name = role.get("name", "")
        role_id = role["id"]
        dn = (role.get("display_name") or "").strip().lower()

        if role_name != "CUSTOM":
            builtin_by_name[role_name] = role_id
            if dn:
                register_role_label(dn, role_id, role_name)
            builtin_label = role_name.lower()
            register_role_label(builtin_label, role_id, role_name)
            preferred_builtin_by_label[builtin_label] = role_id
            if role_name == "ORGANIZATION_USER":
                org_user_role_id = role_id
        elif dn:
            register_role_label(dn, role_id, role_name)

    for alias, builtin_name in _BUILTIN_ROLE_ALIASES.items():
        builtin_role_id = builtin_by_name.get(builtin_name)
        if builtin_role_id:
            register_role_label(alias, builtin_role_id, builtin_name)
            preferred_builtin_by_label[alias] = builtin_role_id

    ambiguous: set[str] = set()
    unresolved: set[str] = set()
    resolved_rows: list[dict] = []
    for row in csv_rows:
        label = row["langsmith_role"].strip().lower()
        candidates = role_lookup.get(label, [])
        if not candidates:
            unresolved.add(row["langsmith_role"])
            continue

        preferred_role_id = preferred_builtin_by_label.get(label)
        if preferred_role_id and any(
            candidate_role_id == preferred_role_id
            for candidate_role_id, _ in candidates
        ):
            role_id = preferred_role_id
        else:
            unique_role_ids = {candidate_role_id for candidate_role_id, _ in candidates}
            if len(unique_role_ids) != 1:
                ambiguous.add(row["langsmith_role"])
                continue
            role_id = next(iter(unique_role_ids))
        resolved_role_name = next(
            candidate_role_name
            for candidate_role_id, candidate_role_name in candidates
            if candidate_role_id == role_id
        )

        resolved_rows.append(
            {
                "email": row["email"],
                "langsmith_role": row["langsmith_role"],
                "role_id": role_id,
                "role_name": resolved_role_name,
                "workspace_id": row.get("workspace_id", ""),
            }
        )

    if ambiguous or unresolved:
        available = sorted(role_lookup.keys())
        errors: list[str] = []
        if ambiguous:
            errors.append("Ambiguous role name(s): " + ", ".join(sorted(ambiguous)))
        if unresolved:
            errors.append(
                "Could not resolve role name(s): "
                + ", ".join(sorted(unresolved))
            )
        errors.append("Available roles: " + ", ".join(available))
        raise click.ClickException(". ".join(errors))

    return resolved_rows, org_user_role_id


def _normalize_csv_role_scopes(
    csv_rows: list[dict],
) -> tuple[list[dict], int]:
    """Validate role scope usage and normalize supported org-role workspace rows.

    `Organization Admin` is the one supported org-scoped role on a workspace
    row. Those rows are treated as org-level admin access because org admins
    already have access across workspaces and do not need explicit workspace
    memberships. Other org-scoped roles on workspace rows are rejected so the
    CSV cannot silently grant less access than the operator intended.
    """
    normalized_rows: list[dict] = []
    org_admin_workspace_rows = 0
    errors: list[str] = []

    for row in csv_rows:
        workspace_id = (row.get("workspace_id") or "").strip()
        role_name = (row.get("role_name") or "").strip().upper()
        role_label = row.get("langsmith_role") or role_name or row.get("role_id") or "<unknown>"
        email = row["email"]

        if not workspace_id:
            if role_name in _WORKSPACE_SCOPED_BUILTIN_ROLE_NAMES:
                errors.append(
                    f"{email}: '{role_label}' is workspace-scoped and cannot be used "
                    "on an org-level row; add a workspace_id or choose an org-scoped role"
                )
                continue
            normalized_rows.append(row)
            continue

        if role_name == "ORGANIZATION_ADMIN":
            normalized_rows.append({**row, "workspace_id": ""})
            org_admin_workspace_rows += 1
            continue

        if role_name in _ORG_SCOPED_BUILTIN_ROLE_NAMES:
            errors.append(
                f"{email} in workspace {workspace_id}: '{role_label}' is org-scoped "
                "and cannot be used on a workspace row; leave workspace_id empty for "
                "org access or choose a workspace-scoped role"
            )
            continue

        normalized_rows.append(row)

    if errors:
        raise click.ClickException(
            "Members CSV has invalid role scope assignments: " + "; ".join(errors)
        )

    return normalized_rows, org_admin_workspace_rows


def _csv_rows_to_org_members(
    csv_rows: list[dict], default_org_role_id: str | None = None
) -> list[dict]:
    """Build org member payloads from CSV rows.

    Rows with an empty ``workspace_id`` are org-level assignments whose
    ``role_id`` is used directly.  Users who only appear in workspace
    rows (non-empty ``workspace_id``) are assigned *default_org_role_id*
    (typically ORGANIZATION_USER) so they can be invited to the org.
    """
    org_rows = [r for r in csv_rows if not r.get("workspace_id")]
    ws_rows = [r for r in csv_rows if r.get("workspace_id")]

    members_by_email: dict[str, dict] = {}

    for row in org_rows:
        email = row["email"]
        existing = members_by_email.get(email)
        if existing and existing["role_id"] != row["role_id"]:
            raise click.ClickException(
                "Members CSV has conflicting org-level roles for "
                f"{email}: '{existing['role_id']}' vs '{row['role_id']}'"
            )
        if not existing:
            members_by_email[email] = {
                "id": email,
                "email": email,
                "role_id": row["role_id"],
                "full_name": "",
            }

    for row in ws_rows:
        email = row["email"]
        if email not in members_by_email and default_org_role_id:
            members_by_email[email] = {
                "id": email,
                "email": email,
                "role_id": default_org_role_id,
                "full_name": "",
            }

    return list(members_by_email.values())


def _csv_rows_for_workspace(csv_rows: list[dict], source_workspace_id: str) -> list[dict]:
    """Build workspace member payloads for a specific source workspace."""
    rows_for_workspace = [row for row in csv_rows if row["workspace_id"] == source_workspace_id]
    members_by_email: dict[str, dict] = {}
    for row in rows_for_workspace:
        email = row["email"]
        existing = members_by_email.get(email)
        if existing and existing["role_id"] != row["role_id"]:
            raise click.ClickException(
                "Members CSV has conflicting role_id values for "
                f"{email} in workspace {source_workspace_id}: "
                f"'{existing['role_id']}' vs '{row['role_id']}'"
            )
        if not existing:
            members_by_email[email] = {
                "id": f"{source_workspace_id}:{email}",
                "email": email,
                "role_id": row["role_id"],
                "full_name": "",
            }
    return list(members_by_email.values())


def _normalize_single_instance_url(url: str) -> str:
    """Normalize base URLs before validating single-instance mode."""
    normalized = (url or "").rstrip("/").lower()
    for suffix in ("/api/v1", "/api/v2"):
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
            break
    return normalized.rstrip("/")


def _configure_single_instance(config: Config) -> None:
    """Mirror one resolved connection config onto both clients for single-instance runs."""
    candidates: list[tuple[str, str, str]] = []
    if config.source.api_key:
        candidates.append(("source", config.source.api_key, config.source.base_url))
    if config.destination.api_key:
        candidates.append(("destination", config.destination.api_key, config.destination.base_url))

    if not candidates:
        raise click.ClickException(
            "Single-instance mode requires a target API key and URL. "
            "Pass --api-key and --url, or provide one unambiguous configured connection."
        )

    unique_candidates = {
        (api_key, _normalize_single_instance_url(base_url)): base_url
        for _, api_key, base_url in candidates
    }
    if len(unique_candidates) != 1:
        raise click.ClickException(
            "Single-instance mode found multiple configured LangSmith targets. "
            "Pass --api-key and --url to choose the target instance explicitly."
        )

    canonical_key, _ = next(iter(unique_candidates))
    canonical_url = next(iter(unique_candidates.values()))

    config.source.api_key = canonical_key
    config.destination.api_key = canonical_key
    config.source.base_url = canonical_url
    config.destination.base_url = canonical_url


def _resolve_single_instance_workspace_ids(
    csv_rows: list[dict],
    available_workspace_ids: set[str],
    *,
    source_of_truth: bool,
) -> list[str]:
    """Return workspace IDs to reconcile for a single-instance CSV sync."""
    csv_workspace_ids = {
        (row.get("workspace_id") or "").strip()
        for row in csv_rows
        if (row.get("workspace_id") or "").strip()
    }
    unknown_workspace_ids = sorted(csv_workspace_ids - available_workspace_ids)
    if unknown_workspace_ids:
        raise click.ClickException(
            "Members CSV references unknown workspace_id value(s): "
            + ", ".join(unknown_workspace_ids)
        )

    workspace_ids = available_workspace_ids | csv_workspace_ids if source_of_truth else csv_workspace_ids
    return sorted(workspace_ids)


def _print_single_instance_users_summary(
    config: Config,
    *,
    csv_rows: list[dict],
    workspace_ids: list[str],
    csv_source_of_truth: bool,
    skip_workspace_members: bool,
) -> None:
    """Print a concise, high-signal summary for single-instance CSV sync runs."""
    org_rows = sum(1 for row in csv_rows if not row.get("workspace_id"))
    workspace_rows = len(csv_rows) - org_rows

    console.print("\n[bold]Single-Instance User Sync[/bold]")
    console.print(f"  Target: {config.destination.base_url}")
    console.print(f"  CSV rows: {len(csv_rows)} total ({org_rows} org-level, {workspace_rows} workspace-level)")
    console.print(
        f"  Execution: {'dry run (no changes will be sent)' if config.migration.dry_run else 'live apply'}"
    )
    console.print(
        f"  Removals: {'enabled (authoritative CSV sync)' if csv_source_of_truth else 'disabled (add/update only)'}"
    )
    console.print("  Row selection: disabled (all CSV rows will be applied)")
    if skip_workspace_members:
        console.print("  Workspace access: skipped")
    elif csv_source_of_truth:
        console.print(
            f"  Workspace access: authoritative across {len(workspace_ids)} target workspace(s); "
            "memberships missing from the CSV will be removed"
        )
    elif workspace_ids:
        console.print(
            f"  Workspace access: direct target workspace IDs from CSV ({len(workspace_ids)} workspace(s) in scope)"
        )
    else:
        console.print("  Workspace access: no workspace rows in the CSV")


def _workspace_scope_key(orchestrator) -> str:
    """Build a stable preflight scope key for the active workspace pair."""
    pair = _active_workspace_pair(orchestrator)
    source_ws = pair["source"] or "default"
    dest_ws = pair["dest"] or "default"
    return f"{source_ws}->{dest_ws}"


def _probe_lookup_capability(client, endpoint: str) -> tuple[bool, str]:
    """Probe a paginated lookup endpoint without mutating state."""
    try:
        list(client.get_paginated(endpoint, page_size=1))
        return True, "ok"
    except Exception as e:  # noqa: BLE001
        return False, str(e)


def _run_preflight(orchestrator, config: Config, resources: Iterable[str]) -> None:
    """Run a read-only capability and dependency preflight for the active workspace pair."""
    state = _ensure_migration_session(orchestrator, config)
    scope_key = _workspace_scope_key(orchestrator)
    resource_list = sorted(set(resources))
    workspace_pair = _active_workspace_pair(orchestrator)

    state.record_inventory(
        f"preflight:{scope_key}:workspace_pair",
        workspace_pair,
    )
    state.record_inventory(
        f"preflight:{scope_key}:resources",
        resource_list,
    )

    source_workspace_probe = discover_workspaces(orchestrator.source_client)
    dest_workspace_probe = discover_workspaces(orchestrator.dest_client)
    state.record_inventory(f"preflight:{scope_key}:source_workspaces", source_workspace_probe)
    state.record_inventory(f"preflight:{scope_key}:dest_workspaces", dest_workspace_probe)
    state.record_capability(
        f"workspaces:{scope_key}",
        "source_discovery",
        supported=bool(source_workspace_probe.get("workspaces"))
        or any(probe.get("supported") for probe in source_workspace_probe.get("probes", [])),
        detail="ok" if source_workspace_probe.get("workspaces") else "workspace_probe_completed",
        evidence=source_workspace_probe,
        probe="workspace_discovery",
    )
    state.record_capability(
        f"workspaces:{scope_key}",
        "dest_discovery",
        supported=bool(dest_workspace_probe.get("workspaces"))
        or any(probe.get("supported") for probe in dest_workspace_probe.get("probes", [])),
        detail="ok" if dest_workspace_probe.get("workspaces") else "workspace_probe_completed",
        evidence=dest_workspace_probe,
        probe="workspace_discovery",
    )

    for label, endpoint in (
        ("project_lookup", "/sessions"),
        ("dataset_lookup", "/datasets"),
    ):
        source_supported, source_detail = _probe_lookup_capability(orchestrator.source_client, endpoint)
        dest_supported, dest_detail = _probe_lookup_capability(orchestrator.dest_client, endpoint)
        state.record_capability(
            f"preflight:{scope_key}",
            f"source_{label}",
            supported=source_supported,
            detail=source_detail,
            probe=f"GET {endpoint}",
        )
        state.record_capability(
            f"preflight:{scope_key}",
            f"dest_{label}",
            supported=dest_supported,
            detail=dest_detail,
            probe=f"GET {endpoint}",
        )

    if "prompts" in resource_list or "rules" in resource_list:
        PromptMigrator(
            orchestrator.source_client,
            orchestrator.dest_client,
            state,
            config,
        ).probe_capabilities()
    if "rules" in resource_list:
        RulesMigrator(
            orchestrator.source_client,
            orchestrator.dest_client,
            state,
            config,
        ).probe_capabilities()
    if "charts" in resource_list:
        ChartMigrator(
            orchestrator.source_client,
            orchestrator.dest_client,
            state,
            config,
        ).probe_capabilities()

    dependency_edges = {
        f"experiments:{scope_key}": [f"datasets:{scope_key}"],
        f"runs:{scope_key}": [f"experiments:{scope_key}"],
        f"feedback:{scope_key}": [f"runs:{scope_key}"],
        f"rules:{scope_key}": [
            f"project_lookup:{scope_key}",
            f"dataset_lookup:{scope_key}",
            f"prompts:{scope_key}",
        ],
        f"charts:{scope_key}": [
            f"project_lookup:{scope_key}",
            f"section_scoped_create:{scope_key}",
        ],
        f"workspace_scoped_resources:{scope_key}": [f"workspaces:{scope_key}"],
    }
    for node, dependencies in dependency_edges.items():
        for dependency in dependencies:
            state.add_dependency(node, dependency)

    orchestrator.state_manager.save()


def _display_resolution_summary(orchestrator) -> None:
    """Print a consistent migration resolution summary."""
    if not orchestrator.state:
        return

    stats = orchestrator.state.get_statistics()
    terminal = stats.get("terminal", {})
    bundle_path = orchestrator.state.write_remediation_bundle()
    bundle_display = str(bundle_path.resolve()) if bundle_path else orchestrator.state.remediation_bundle_path

    console.print("\n[bold]Resolution Summary[/bold]")
    console.print(f"  Migrated: {terminal.get(ResolutionOutcome.MIGRATED.value, 0)}")
    console.print(
        f"  Verified downgrade: {terminal.get(ResolutionOutcome.MIGRATED_WITH_VERIFIED_DOWNGRADE.value, 0)}"
    )
    console.print(
        f"  Blocked: {terminal.get(ResolutionOutcome.BLOCKED_WITH_CHECKPOINT.value, 0)}"
    )
    console.print(
        f"  Exported/manual apply: {terminal.get(ResolutionOutcome.EXPORTED_WITH_MANUAL_APPLY.value, 0)}"
    )
    if bundle_display:
        console.print(f"  Remediation bundle: {bundle_display}")
    console.print("  Resume command: langsmith-migrator resume")

    actionable_items = orchestrator.state.get_checkpoint_items()
    if actionable_items:
        console.print("\n[bold]Actionable Next Steps[/bold]")
        for item in actionable_items[:5]:
            console.print(f"  • {item.name}: {item.next_action or item.outcome_code or 'needs attention'}")


def _needs_operator_action(state) -> bool:
    """Return True when the session requires manual remediation or follow-up."""
    if state is None:
        return False
    terminal = state.get_terminal_counts()
    return bool(
        terminal.get(ResolutionOutcome.BLOCKED_WITH_CHECKPOINT.value)
        or terminal.get(ResolutionOutcome.EXPORTED_WITH_MANUAL_APPLY.value)
        or state.remediation_queue
    )


def _exit_for_remediation_if_needed(ctx, config: Config, orchestrator) -> None:
    """Exit with code 2 in non-interactive mode when manual action is required."""
    if config.migration.non_interactive and orchestrator.state and _needs_operator_action(orchestrator.state):
        console.print(
            "\n[yellow]Manual or external follow-up is required. Review the remediation bundle and run `langsmith-migrator resume` after resolving the blockers.[/yellow]"
        )
        ctx.exit(2)



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
@click.option('--non-interactive', is_flag=True, help='Disable prompts and emit exit code 2 when remediation is required')
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose output')
@click.pass_context
def cli(ctx, source_key, dest_key, source_url, dest_url, no_ssl, batch_size, workers, dry_run, skip_existing, non_interactive, verbose):
    """LangSmith Migration Tool - Migrate data between LangSmith instances."""
    ctx.ensure_object(dict)
    _install_log_filters()

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
        non_interactive=non_interactive,
        verbose=verbose
    )

    ctx.obj['config'] = config
    ctx.obj['state_manager'] = StateManager()
    config.state_manager = ctx.obj['state_manager']


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
        orchestrator.cleanup()
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
    ws_result = _resolve_workspaces(orchestrator, source_workspace, dest_workspace, map_workspaces, non_interactive=config.migration.non_interactive)
    if ws_result is _WS_ABORTED:
        ctx.exit(1)
        return
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

            _run_preflight(
                orchestrator,
                config,
                ["datasets", "experiments"] if include_experiments else ["datasets"],
            )

            # Get datasets
            console.print("Fetching datasets... ", end="")
            ds = dataset_migrator.list_datasets()

            if not ds:
                console.print("[yellow]none found[/yellow]")
                continue

            console.print(f"found {len(ds)}\n")

            # Select datasets
            selected = _select_or_all(
                config,
                ds,
                select_all=select_all,
                title="Select Datasets to Migrate",
                columns=[
                    {"key": "name", "title": "Name", "width": 40},
                    {"key": "id", "title": "ID", "width": 36},
                    {"key": "description", "title": "Description", "width": 50},
                    {"key": "example_count", "title": "Examples", "width": 10},
                ],
            )

            if not selected:
                console.print("[yellow]No datasets selected[/yellow]")
                continue

            inc_exp = include_experiments
            if not inc_exp:
                inc_exp = _confirm_action(
                    config,
                    "\nInclude experiments with datasets?",
                    default=False,
                    non_interactive_value=False,
                )

            console.print(f"\nSelected {len(selected)} dataset(s)")
            if inc_exp:
                console.print("[dim]Including experiments, runs, and feedback[/dim]")
            if config.migration.dry_run:
                console.print("[dim]Mode: Dry Run (no changes)[/dim]")

            if not _confirm_action(config, "\nProceed?", default=True, non_interactive_value=True):
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
        _display_resolution_summary(orchestrator)
        _exit_for_remediation_if_needed(ctx, config, orchestrator)
    except Exception as e:
        console.print(f"\n[red]Migration failed: {e}[/red]")
        ctx.exit(1)
    finally:
        orchestrator.cleanup()


def _select_resume_session(config: Config, state_manager: StateManager):
    """Return the selected migration session, or None if the user cancels."""
    sessions = state_manager.list_sessions()

    if not sessions:
        console.print("[yellow]No previous migration sessions found[/yellow]")
        return None

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
            progress,
        )

    console.print(table)

    if config.migration.non_interactive:
        return sessions[0]

    choice = console.input("\nEnter session number to resume (or 'q' to quit): ")
    if choice.lower() == 'q':
        return None

    try:
        session_idx = int(choice) - 1
        if 0 <= session_idx < len(sessions):
            return sessions[session_idx]
        console.print("[red]Invalid selection[/red]")
        return None
    except ValueError:
        console.print("[red]Invalid input[/red]")
        return None


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
        orchestrator.cleanup()
        return

    # Resolve workspace context
    ws_result = _resolve_workspaces(orchestrator, source_workspace, dest_workspace, map_workspaces, non_interactive=config.migration.non_interactive)
    if ws_result is _WS_ABORTED:
        ctx.exit(1)
        return
    if ws_result is _WS_CANCELLED:
        console.print("[yellow]Cancelled[/yellow]")
        orchestrator.cleanup()
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

        _run_preflight(orchestrator, config, ["queues"])

        # Get queues
        console.print("\n[bold]Fetching annotation queues from source...[/bold]")

        queues = queue_migrator.list_queues()

        if not queues:
            console.print("[yellow]No annotation queues found[/yellow]")
            continue

        selected_queues = _select_or_all(
            config,
            queues,
            select_all=config.migration.non_interactive,
            title="Select Annotation Queues to Migrate",
            columns=[
                {"key": "name", "title": "Name", "width": 40},
                {"key": "id", "title": "ID", "width": 36},
                {"key": "description", "title": "Description", "width": 50},
            ],
        )

        if not selected_queues:
            console.print("[yellow]No queues selected[/yellow]")
            continue

        console.print(f"\n[bold]Migrating {len(selected_queues)} annotation queue(s)...[/bold]")
        _ensure_migration_session(orchestrator, config)
        queue_migrator.state = orchestrator.state

        # Perform migration
        success_count = 0
        failed_items = []

        with Progress(console=console) as progress:
            task = progress.add_task("Migrating queues...", total=len(selected_queues))
            for queue in selected_queues:
                item_id = _ensure_state_item(
                    orchestrator,
                    config,
                    "queue",
                    queue["id"],
                    queue["name"],
                    metadata={"queue": queue},
                )
                try:
                    _mark_state_item_started(orchestrator, item_id)
                    new_id = queue_migrator.create_queue(queue)
                    success_count += 1
                    _mark_state_item_completed(orchestrator, item_id, destination_id=new_id)
                except Exception as e:
                    failed_items.append((queue['name'], str(e)))
                    _mark_state_item_failed(orchestrator, item_id, e)
                progress.advance(task)

        console.print(f"Queues: {success_count} migrated, {len(failed_items)} failed")
        if failed_items and config.migration.verbose:
            for name, err in failed_items:
                console.print(f"  [red]✗[/red] {name}: {err}")

    if ws_result:
        orchestrator.clear_workspace_context()

    _display_resolution_summary(orchestrator)
    _exit_for_remediation_if_needed(ctx, config, orchestrator)
    orchestrator.cleanup()


@cli.command()
@ssl_option
@click.option(
    '--dry-run',
    'users_dry_run',
    is_flag=True,
    help='Preview this users sync without making POST/PATCH/DELETE changes. Same as the global --dry-run.',
)
@click.option('--roles-only', is_flag=True, help='Only migrate custom roles (skip members)')
@click.option('--skip-workspace-members', is_flag=True, help='Skip workspace member migration')
@click.option(
    '--single-instance',
    '--instance',
    is_flag=True,
    help='Use one target LangSmith instance for CSV-driven access sync instead of source→destination migration',
)
@click.option(
    '--csv-source-of-truth',
    '--sync',
    is_flag=True,
    help=(
        'Make the CSV authoritative for single-instance sync: remove org users, '
        'pending invites, and workspace memberships not present in the CSV. '
        'Without this flag, CSV mode only adds or updates access.'
    ),
)
@click.option(
    '--members-csv',
    '--csv',
    'members_csv',
    type=click.Path(exists=True, dir_okay=False, path_type=str),
    help=(
        "CSV file with member details (email, langsmith_role, workspace_id, "
        "workspace_name). Replaces source member API lookups. Rows with an "
        "empty workspace_id are org-level role assignments. Organization "
        "Admin on a workspace row is treated as org-level access only. In "
        "--single-instance mode, all CSV rows are applied automatically."
    ),
)
@click.option(
    '--api-key',
    'instance_key',
    help='API key for the single-instance CSV sync target. Must be provided together with --url.',
)
@click.option(
    '--url',
    'instance_url',
    help='Base URL for the single-instance CSV sync target. Must be provided together with --api-key.',
)
@workspace_options
@click.pass_context
def users(
    ctx,
    users_dry_run,
    roles_only,
    skip_workspace_members,
    single_instance,
    csv_source_of_truth,
    members_csv,
    instance_key,
    instance_url,
    source_workspace,
    dest_workspace,
    map_workspaces,
):
    """Migrate users and roles, or sync one LangSmith instance from a CSV."""
    config = ctx.obj['config']
    state_manager = ctx.obj['state_manager']

    display_banner()

    if users_dry_run:
        config.migration.dry_run = True

    if bool(instance_key) != bool(instance_url):
        raise click.ClickException(
            "--api-key and --url must be provided together for single-instance CSV sync"
        )

    if instance_key and instance_url:
        config.source.api_key = instance_key
        config.destination.api_key = instance_key
        config.source.base_url = instance_url
        config.destination.base_url = instance_url

    if instance_key or instance_url or csv_source_of_truth:
        single_instance = True

    if single_instance and roles_only:
        raise click.ClickException(
            "--roles-only cannot be combined with --single-instance because "
            "single-instance mode only supports CSV-driven member access sync"
        )
    if roles_only and members_csv:
        raise click.ClickException(
            "--roles-only cannot be combined with --members-csv because CSV "
            "input only applies to member migration"
        )
    if csv_source_of_truth and not members_csv:
        raise click.ClickException(
            "--csv-source-of-truth requires --members-csv"
        )
    if csv_source_of_truth and config.migration.skip_existing:
        raise click.ClickException(
            "--csv-source-of-truth cannot be combined with --skip-existing"
        )
    if csv_source_of_truth and skip_workspace_members:
        raise click.ClickException(
            "--csv-source-of-truth cannot be combined with "
            "--skip-workspace-members because authoritative sync must reconcile "
            "workspace access"
        )
    if single_instance:
        if not members_csv:
            raise click.ClickException("--single-instance requires --members-csv")
        if map_workspaces or source_workspace or dest_workspace:
            raise click.ClickException(
                "--single-instance cannot be combined with workspace mapping flags"
            )
        _configure_single_instance(config)

    csv_member_rows = _load_members_csv(members_csv) if members_csv else None
    apply_all_csv_rows = bool(single_instance and csv_member_rows is not None)
    csv_has_workspace_rows = bool(
        csv_member_rows and any(row.get("workspace_id") for row in csv_member_rows)
    )
    if skip_workspace_members and csv_has_workspace_rows:
        raise click.ClickException(
            "--skip-workspace-members cannot be used when the CSV contains "
            "workspace_id values. Remove the flag to apply workspace access, "
            "or remove workspace rows from the CSV."
        )

    if not ensure_config(config):
        return

    orchestrator = MigrationOrchestrator(config, state_manager)

    if not orchestrator.test_connections():
        console.print("\n[red]Cannot proceed without valid connections[/red]")
        orchestrator.cleanup()
        return

    available_workspace_ids: set[str] = set()
    ws_result = None
    if single_instance:
        available_workspace_ids = {
            ws.get("id", "")
            for ws in _list_workspaces(orchestrator.dest_client)
            if ws.get("id")
        }
        ws_pairs = [(None, None)]
    else:
        # Resolve workspace context (needed for phase 3)
        ws_result = _resolve_workspaces(
            orchestrator,
            source_workspace,
            dest_workspace,
            map_workspaces,
            non_interactive=config.migration.non_interactive,
        )
        if ws_result is _WS_ABORTED:
            ctx.exit(1)
            return
        if ws_result is _WS_CANCELLED:
            console.print("[yellow]Cancelled[/yellow]")
            orchestrator.cleanup()
            return
        ws_pairs = list(ws_result.workspace_mapping.items()) if ws_result else [(None, None)]

    # Clear workspace context for org-scoped phases 1-2
    orchestrator.clear_workspace_context()

    _ensure_migration_session(orchestrator, config)

    user_role_migrator = UserRoleMigrator(
        orchestrator.source_client,
        orchestrator.dest_client,
        orchestrator.state,
        config,
    )

    _run_preflight(orchestrator, config, ["users"])

    # ── Phase 1: Role synchronisation ──
    console.print("\n[bold]Phase 1: Synchronising roles...[/bold]")

    try:
        role_mapping = user_role_migrator.build_role_mapping()
        console.print(f"  [green]{len(role_mapping)} role(s) mapped[/green]")
    except Exception as e:
        console.print(f"  [red]Failed to build role mapping: {e}[/red]")
        orchestrator.cleanup()
        return

    if roles_only:
        console.print("\n[dim]--roles-only: skipping member migration[/dim]")
        orchestrator.cleanup()
        return

    default_org_role_id: str | None = None
    ws_only_emails: set[str] = set()
    org_admin_workspace_row_count = 0

    if csv_member_rows is not None:
        source_roles = user_role_migrator.list_source_roles()
        csv_member_rows, default_org_role_id = _resolve_csv_role_names(
            csv_member_rows, source_roles
        )
        csv_member_rows, org_admin_workspace_row_count = _normalize_csv_role_scopes(
            csv_member_rows
        )
        org_emails: set[str] = set()
        ws_emails: set[str] = set()
        for row in csv_member_rows:
            (ws_emails if row.get("workspace_id") else org_emails).add(row["email"])
        ws_only_emails = ws_emails - org_emails

        if default_org_role_id is None:
            if ws_only_emails:
                if single_instance:
                    raise click.ClickException(
                        "Single-instance CSV sync requires an ORGANIZATION_USER "
                        "role when the CSV contains workspace-only users"
                    )
                console.print(
                    f"  [yellow]Warning: no ORGANIZATION_USER role found on source; "
                    f"{len(ws_only_emails)} workspace-only user(s) will not be "
                    f"invited to the org.[/yellow]"
                )

        if single_instance:
            workspace_ids = _resolve_single_instance_workspace_ids(
                csv_member_rows,
                available_workspace_ids,
                source_of_truth=csv_source_of_truth,
            )
            ws_pairs = [(workspace_id, workspace_id) for workspace_id in workspace_ids]

        if org_admin_workspace_row_count:
            console.print(
                "  [cyan]Note:[/cyan] "
                f"{org_admin_workspace_row_count} workspace row(s) used "
                "Organization Admin and were treated as org-level admin access only."
            )

    if apply_all_csv_rows:
        _print_single_instance_users_summary(
            config,
            csv_rows=csv_member_rows,
            workspace_ids=[dst_ws for _, dst_ws in ws_pairs if dst_ws],
            csv_source_of_truth=csv_source_of_truth,
            skip_workspace_members=skip_workspace_members,
        )
        confirm_prompt = (
            "Proceed with authoritative single-instance CSV sync? This will "
            "remove access that is not present in the CSV."
            if csv_source_of_truth
            else "Proceed with single-instance CSV apply? This will add or "
            "update access for every CSV row."
        )
        if not _confirm_action(
            config,
            confirm_prompt,
            default=True,
            non_interactive_value=True,
        ):
            console.print("[yellow]Cancelled[/yellow]")
            orchestrator.cleanup()
            return

    # ── Phase 2: Org member migration ──
    console.print("\n[bold]Phase 2: Migrating organisation members...[/bold]")

    if csv_member_rows is not None:
        all_members = _csv_rows_to_org_members(
            csv_member_rows, default_org_role_id=default_org_role_id
        )
    else:
        org_members = user_role_migrator.list_source_org_members()
        pending_members = user_role_migrator.list_source_pending_org_members()
        all_members = org_members + [{**p, "_pending": True} for p in pending_members]

    if not all_members:
        console.print("  [yellow]No org members found[/yellow]")
    else:
        if apply_all_csv_rows:
            selected_members = all_members
        else:
            selected_members = _select_or_all(
                config,
                all_members,
                select_all=config.migration.non_interactive,
                title="Select Organisation Members to Migrate",
                columns=_MEMBER_COLUMNS,
            )

        if selected_members:
            migrated, skipped, failed = user_role_migrator.migrate_org_members(
                selected_members,
                remove_missing=csv_source_of_truth,
                remove_pending=csv_source_of_truth,
            )
            removal_note = ""
            if csv_source_of_truth:
                removal_note = (
                    f" [dim](includes {user_role_migrator._last_org_member_removals} removed)[/dim]"
                )
            console.print(
                f"  Org members: [green]{migrated} migrated[/green], "
                f"{skipped} skipped, [red]{failed} failed[/red]{removal_note}"
            )
        else:
            console.print("  [yellow]No members selected[/yellow]")

    # ── Phase 3: Workspace member migration ──
    if skip_workspace_members:
        console.print("\n[dim]--skip-workspace-members: skipping workspace member migration[/dim]")
    else:
        has_ws_pairs = any(s and d for s, d in ws_pairs)
        if has_ws_pairs:
            console.print("\n[bold]Phase 3: Migrating workspace members...[/bold]")

            # Always refresh destination org identity index before workspace phase.
            # Keep this best-effort so the run can continue gracefully.
            try:
                dest_members = user_role_migrator.list_dest_org_members()
                user_role_migrator._dest_email_to_identity = {
                    (m.get("email") or "").lower(): m
                    for m in dest_members
                    if m.get("email")
                }
            except Exception as e:
                console.print(
                    f"  [yellow]Warning: failed to refresh destination org identities: {e}[/yellow]"
                )

            for src_ws, dst_ws in ws_pairs:
                if not src_ws or not dst_ws:
                    continue
                orchestrator.set_workspace_context(src_ws, dst_ws)
                console.print(f"\n  [cyan]Workspace: {src_ws} -> {dst_ws}[/cyan]")

                if csv_member_rows is not None:
                    ws_members = _csv_rows_for_workspace(csv_member_rows, src_ws)
                else:
                    ws_members = user_role_migrator.list_source_workspace_members()
                if not ws_members and not csv_source_of_truth:
                    console.print("    [yellow]No workspace members found[/yellow]")
                    continue

                if apply_all_csv_rows:
                    selected_ws_members = ws_members
                else:
                    selected_ws_members = _select_or_all(
                        config,
                        ws_members,
                        select_all=config.migration.non_interactive,
                        title="Select Workspace Members to Migrate",
                        columns=_MEMBER_COLUMNS,
                    )

                if not selected_ws_members and not csv_source_of_truth:
                    console.print("    [yellow]No members selected[/yellow]")
                    continue

                try:
                    m, s, f = user_role_migrator.migrate_workspace_members(
                        selected_members=selected_ws_members,
                        remove_missing=csv_source_of_truth,
                    )
                    removal_note = ""
                    if csv_source_of_truth:
                        removal_note = (
                            f" [dim](includes {user_role_migrator._last_workspace_member_removals} removed)[/dim]"
                        )
                    console.print(
                        f"    [green]{m} migrated[/green], {s} skipped, "
                        f"[red]{f} failed[/red]{removal_note}"
                    )
                except Exception as e:
                    console.print(f"    [red]Failed: {e}[/red]")

            orchestrator.clear_workspace_context()
        else:
            console.print("\n[dim]No workspace pairs configured, skipping workspace member migration[/dim]")

    _display_resolution_summary(orchestrator)
    _exit_for_remediation_if_needed(ctx, config, orchestrator)
    orchestrator.cleanup()


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
        orchestrator.cleanup()
        return
    if not dest_ok:
        console.print("[red]✗ Destination connection failed[/red]")
        orchestrator.cleanup()
        return
    console.print("[green]✓[/green]")

    # Resolve workspace context
    ws_result = _resolve_workspaces(orchestrator, source_workspace, dest_workspace, map_workspaces, non_interactive=config.migration.non_interactive)
    if ws_result is _WS_ABORTED:
        ctx.exit(1)
        return
    if ws_result is _WS_CANCELLED:
        console.print("[yellow]Cancelled[/yellow]")
        orchestrator.cleanup()
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

        _run_preflight(orchestrator, config, ["prompts"])

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

        selected_prompts = _select_or_all(
            config,
            prompts,
            select_all=select_all,
            title="Select Prompts to Migrate",
            columns=[
                {"key": "repo_handle", "title": "Handle", "width": 40},
                {"key": "description", "title": "Description", "width": 50},
                {"key": "num_commits", "title": "Commits", "width": 10},
                {"key": "is_public", "title": "Public", "width": 8},
            ],
        )

        if not selected_prompts:
            console.print("[yellow]No prompts selected[/yellow]")
            continue

        console.print(f"\nSelected {len(selected_prompts)} prompt(s)")

        if config.migration.dry_run:
            console.print("[dim]Mode: Dry Run (no changes)[/dim]")

        if include_all_commits:
            console.print("[dim]Including all commit history[/dim]")

        if not _confirm_action(config, "\nProceed?", default=True, non_interactive_value=True):
            console.print("[yellow]Cancelled[/yellow]")
            continue

        _ensure_migration_session(orchestrator, config)
        prompt_migrator.state = orchestrator.state

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

    _display_resolution_summary(orchestrator)
    _exit_for_remediation_if_needed(ctx, config, orchestrator)
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
        orchestrator.cleanup()
        return
    if not dest_ok:
        console.print("[red]✗ Destination connection failed[/red]")
        orchestrator.cleanup()
        return
    console.print("[green]✓[/green]")

    # Resolve workspace context
    ws_result = _resolve_workspaces(orchestrator, source_workspace, dest_workspace, map_workspaces, non_interactive=config.migration.non_interactive)
    if ws_result is _WS_ABORTED:
        ctx.exit(1)
        return
    if ws_result is _WS_CANCELLED:
        console.print("[yellow]Cancelled[/yellow]")
        orchestrator.cleanup()
        return

    ws_pairs = list(ws_result.workspace_mapping.items()) if ws_result else [(None, None)]

    # --map-projects and --project-mapping are mutually exclusive
    if map_projects and project_mapping:
        console.print("[red]Error: --map-projects and --project-mapping are mutually exclusive[/red]")
        orchestrator.cleanup()
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

        _run_preflight(orchestrator, config, ["rules", "prompts"])

        rules_migrator = RulesMigrator(
            orchestrator.source_client,
            orchestrator.dest_client,
            None,
            config
        )

        ws_project_id_map = _workspace_scoped_project_id_map(orchestrator, ws_result, src_ws)
        if ws_project_id_map:
            rules_migrator._project_id_map = ws_project_id_map

        # Launch interactive TUI project mapper (inside loop for workspace-scoped projects)
        if map_projects and not ws_project_id_map:
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

        if select_all or config.migration.non_interactive:
            selected_rules = rules
        else:
            selected_rules = _select_or_all(
                config,
                rules_for_display,
                select_all=False,
                title="Select Rules to Migrate",
                columns=[
                    {"key": "name", "title": "Name", "width": 30},
                    {"key": "rule_type", "title": "Type", "width": 25},
                    {"key": "association", "title": "Association", "width": 15},
                    {"key": "enabled", "title": "Enabled", "width": 10},
                ],
            )

        if not selected_rules:
            console.print("[yellow]No rules selected[/yellow]")
            continue

        console.print(f"\nSelected {len(selected_rules)} rule(s)")

        if config.migration.dry_run:
            console.print("[dim]Mode: Dry Run (no changes)[/dim]")

        if strip_projects:
            console.print("[dim]Mode: Stripping project associations (creating as global rules)[/dim]")

        if not _confirm_action(config, "\nProceed?", default=True, non_interactive_value=True):
            console.print("[yellow]Cancelled[/yellow]")
            continue

        _ensure_migration_session(orchestrator, config)
        rules_migrator.state = orchestrator.state

        success_count = 0
        failed_items = []
        skipped_items = []

        with Progress(console=console) as progress:
            task = progress.add_task("Migrating rules...", total=len(selected_rules))
            for rule in selected_rules:
                rule_name = rule.get('display_name') or rule.get('name', 'unnamed')
                item_id = _ensure_state_item(
                    orchestrator,
                    config,
                    "rule",
                    rule.get("id", rule_name),
                    rule_name,
                    metadata={
                        "rule": rule,
                        "strip_projects": strip_projects,
                        "create_disabled": not create_enabled,
                        "project_id_map": dict(rules_migrator._project_id_map or {}),
                    },
                )
                try:
                    _mark_state_item_started(orchestrator, item_id)
                    has_project = bool(rule.get('session_id'))
                    has_dataset = bool(rule.get('dataset_id'))
                    has_evaluators = bool(rule.get('evaluators') or rule.get('evaluator_prompt_handle'))

                    create_disabled = not create_enabled
                    result = rules_migrator.create_rule(rule, strip_project_reference=strip_projects, create_disabled=create_disabled)
                    if result:
                        success_count += 1
                        _mark_state_item_completed(orchestrator, item_id, destination_id=result)
                    else:
                        if not has_dataset and not has_project:
                            skipped_items.append((rule_name, "no dataset or project"))
                            _mark_state_item_failed(orchestrator, item_id, "no dataset or project")
                        elif has_project and not has_dataset:
                            skipped_items.append((rule_name, "project not found in destination"))
                            _mark_state_item_failed(orchestrator, item_id, "project not found in destination")
                        elif has_evaluators:
                            failed_items.append((rule_name, "check prompts exist on destination"))
                            _mark_state_item_failed(orchestrator, item_id, "check prompts exist on destination")
                        else:
                            failed_items.append((rule_name, "see verbose logs"))
                            _mark_state_item_failed(orchestrator, item_id, "see verbose logs")
                except Exception as e:
                    failed_items.append((rule_name, str(e)))
                    _mark_state_item_failed(orchestrator, item_id, e)
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

    _display_resolution_summary(orchestrator)
    _exit_for_remediation_if_needed(ctx, config, orchestrator)
    orchestrator.cleanup()


@cli.command()
@ssl_option
@click.option('--skip-users', is_flag=True, help='Skip user and role migration')
@click.option('--skip-datasets', is_flag=True, help='Skip dataset migration')
@click.option('--skip-experiments', is_flag=True, help='Skip experiment migration')
@click.option('--skip-prompts', is_flag=True, help='Skip prompt migration')
@click.option('--skip-queues', is_flag=True, help='Skip annotation queue migration')
@click.option('--skip-rules', is_flag=True, help='Skip rules migration')
@click.option('--skip-charts', is_flag=True, help='Skip chart migration')
@click.option('--include-all-commits', is_flag=True, help='Include all prompt commit history')
@click.option('--strip-projects', is_flag=True, help='Strip project associations from rules')
@click.option('--map-projects', is_flag=True, help='Launch interactive TUI to map source projects to destination projects')
@click.option(
    '--rules-create-enabled',
    'rules_create_enabled',
    flag_value=True,
    default=None,
    help='Create migrated rules as enabled (default: disabled). If omitted, migrate-all asks interactively (default: No)'
)
@workspace_options
@click.pass_context
def migrate_all(ctx, skip_users, skip_datasets, skip_experiments, skip_prompts, skip_queues, skip_rules, skip_charts, include_all_commits, strip_projects, map_projects, rules_create_enabled, source_workspace, dest_workspace, map_workspaces):
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
        orchestrator.cleanup()
        return
    if not dest_ok:
        console.print("[red]✗ Destination connection failed[/red]")
        orchestrator.cleanup()
        return
    console.print("[green]✓[/green]\n")

    # Resolve workspace context (runs before asset discovery)
    ws_result = _resolve_workspaces(orchestrator, source_workspace, dest_workspace, map_workspaces, non_interactive=config.migration.non_interactive)
    if ws_result is _WS_ABORTED:
        ctx.exit(1)
        return
    if ws_result is _WS_CANCELLED:
        console.print("[yellow]Cancelled[/yellow]")
        orchestrator.cleanup()
        return

    console.print("[bold cyan]LangSmith Data Migration Wizard[/bold cyan]\n")
    console.print("This wizard will guide you through migrating all your data.\n")

    # Step 0: Users & Roles (org-scoped, runs once before workspace loop)
    if not skip_users:
        console.print("[bold]Step 0: Users & Roles[/bold]")

        # Clear workspace context for org-scoped phases
        orchestrator.clear_workspace_context()
        _ensure_migration_session(orchestrator, config)

        user_role_migrator = UserRoleMigrator(
            orchestrator.source_client,
            orchestrator.dest_client,
            orchestrator.state,
            config,
        )

        # Phase 1: Roles
        console.print("  Synchronising roles... ", end="")
        try:
            role_mapping = user_role_migrator.build_role_mapping()
            console.print(f"[green]{len(role_mapping)} mapped[/green]")
        except Exception as e:
            console.print(f"[red]failed: {e}[/red]")
            role_mapping = {}

        # Phase 2: Org members
        if role_mapping:
            org_members = user_role_migrator.list_source_org_members()
            if org_members:
                if _confirm_action(config, f"Migrate {len(org_members)} org member(s)?", default=True, non_interactive_value=True):
                    migrated, skipped, failed = user_role_migrator.migrate_org_members(org_members)
                    console.print(
                        f"  Org members: [green]{migrated} migrated[/green], "
                        f"{skipped} skipped, [red]{failed} failed[/red]"
                    )
                else:
                    console.print("  [yellow]Skipped org members[/yellow]")
            else:
                console.print("  [yellow]No org members found[/yellow]")

        console.print()
    else:
        console.print("[dim]Skipping users (--skip-users)[/dim]\n")

    # If multi-workspace, iterate per pair; otherwise run once
    ws_pairs = list(ws_result.workspace_mapping.items()) if ws_result else [(None, None)]

    for ws_idx, (src_ws, dst_ws) in enumerate(ws_pairs):
        if src_ws and dst_ws:
            orchestrator.set_workspace_context(src_ws, dst_ws)
            console.print(f"\n[bold cyan]━━━ Workspace {ws_idx + 1}/{len(ws_pairs)}: {src_ws} -> {dst_ws} ━━━[/bold cyan]\n")

        preflight_resources = []
        if not skip_datasets:
            preflight_resources.append("datasets")
            if not skip_experiments:
                preflight_resources.append("experiments")
        if not skip_prompts:
            preflight_resources.append("prompts")
        if not skip_queues:
            preflight_resources.append("queues")
        if not skip_rules:
            preflight_resources.append("rules")
        if not skip_charts:
            preflight_resources.append("charts")
        _run_preflight(orchestrator, config, preflight_resources)

        # Use per-workspace project mapping from the TUI if available
        ws_project_mapping = None
        if ws_result and src_ws and src_ws in ws_result.project_mappings:
            ws_project_mapping = ws_result.project_mappings[src_ws]

        _migrate_all_for_workspace(ctx, orchestrator, config, skip_datasets, skip_experiments,
                                   skip_prompts, skip_queues, skip_rules, skip_charts, include_all_commits,
                                   strip_projects, map_projects, rules_create_enabled, ws_project_mapping)

    if ws_result:
        orchestrator.clear_workspace_context()

    console.print("\n[bold green]Migration wizard completed![/bold green]")
    _display_resolution_summary(orchestrator)
    _exit_for_remediation_if_needed(ctx, config, orchestrator)
    orchestrator.cleanup()


def _migrate_all_for_workspace(ctx, orchestrator, config, skip_datasets, skip_experiments,
                                skip_prompts, skip_queues, skip_rules, skip_charts, include_all_commits,
                                strip_projects, map_projects, rules_create_enabled=None, ws_project_mapping=None):
    """Run the full migrate_all flow for a single workspace pair (or no workspace).

    Args:
        rules_create_enabled: If True, create rules enabled. If None, ask interactively
            (default prompt answer is No, i.e. rules disabled).
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

            if _confirm_action(config, f"Migrate {len(datasets)} dataset(s)?", default=True, non_interactive_value=True):
                include_exp = False
                if not skip_experiments:
                    include_exp = _confirm_action(
                        config,
                        "Include experiments with datasets?",
                        default=True,
                        non_interactive_value=True,
                    )

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

            if _confirm_action(config, f"Migrate {len(prompts)} prompt(s)?", default=True, non_interactive_value=True):
                include_history = include_all_commits or _confirm_action(
                    config,
                    "Include full commit history?",
                    default=False,
                    non_interactive_value=include_all_commits,
                )
                _ensure_migration_session(orchestrator, config)
                prompt_migrator.state = orchestrator.state

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

            if _confirm_action(config, f"Migrate {len(queues)} annotation queue(s)?", default=True, non_interactive_value=True):
                _ensure_migration_session(orchestrator, config)
                queue_migrator.state = orchestrator.state
                success_count = 0
                failed_items = []

                with Progress(console=console) as progress:
                    task = progress.add_task("Migrating queues...", total=len(queues))
                    for queue in queues:
                        item_id = _ensure_state_item(
                            orchestrator,
                            config,
                            "queue",
                            queue["id"],
                            queue["name"],
                            metadata={"queue": queue},
                        )
                        try:
                            _mark_state_item_started(orchestrator, item_id)
                            new_id = queue_migrator.create_queue(queue)
                            success_count += 1
                            _mark_state_item_completed(orchestrator, item_id, destination_id=new_id)
                        except Exception as e:
                            failed_items.append((queue['name'], str(e)))
                            _mark_state_item_failed(orchestrator, item_id, e)
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

            if _confirm_action(config, f"Migrate {len(rules)} rule(s)?", default=True, non_interactive_value=True):
                strip = strip_projects
                ensure_projects = False
                create_enabled = rules_create_enabled
                
                if project_specific and not strip:
                    strip = _confirm_action(
                        config,
                        "Convert project-specific rules to global rules?",
                        default=False,
                        non_interactive_value=strip_projects,
                    )
                    if not strip:
                        ensure_projects = _confirm_action(
                            config,
                            "Create corresponding projects for project-specific rules?",
                            default=True,
                            non_interactive_value=True,
                        )
                if create_enabled is None:
                    create_enabled = _confirm_action(
                        config,
                        "Create migrated rules as enabled?",
                        default=False,
                        non_interactive_value=False,
                    )
                create_disabled = not create_enabled

                success_count = 0
                failed_items = []
                skipped_items = []
                _ensure_migration_session(orchestrator, config)
                rules_migrator.state = orchestrator.state

                with Progress(console=console) as progress:
                    task = progress.add_task("Migrating rules...", total=len(rules))
                    for rule in rules:
                        rule_name = rule.get('display_name') or rule.get('name', 'unnamed')
                        item_id = _ensure_state_item(
                            orchestrator,
                            config,
                            "rule",
                            rule.get("id", rule_name),
                            rule_name,
                            metadata={
                                "rule": rule,
                                "strip_projects": strip,
                                "ensure_project": ensure_projects,
                                "project_id_map": dict(rules_migrator._project_id_map or {}),
                            },
                        )
                        try:
                            _mark_state_item_started(orchestrator, item_id)
                            has_project = bool(rule.get('session_id'))
                            has_dataset = bool(rule.get('dataset_id'))
                            has_evaluators = bool(rule.get('evaluators') or rule.get('evaluator_prompt_handle'))

                            result = rules_migrator.create_rule(
                                rule,
                                strip_project_reference=strip,
                                ensure_project=ensure_projects,
                                create_disabled=create_disabled
                            )
                            if result:
                                success_count += 1
                                _mark_state_item_completed(orchestrator, item_id, destination_id=result)
                            else:
                                if not has_dataset and not has_project:
                                    skipped_items.append((rule_name, "no dataset or project"))
                                    _mark_state_item_failed(orchestrator, item_id, "no dataset or project")
                                elif has_project and not has_dataset and not ensure_projects:
                                    skipped_items.append((rule_name, "project not found in destination"))
                                    _mark_state_item_failed(orchestrator, item_id, "project not found in destination")
                                elif has_evaluators:
                                    failed_items.append((rule_name, "check prompts exist on destination"))
                                    _mark_state_item_failed(orchestrator, item_id, "check prompts exist on destination")
                                else:
                                    failed_items.append((rule_name, "see verbose logs"))
                                    _mark_state_item_failed(orchestrator, item_id, "see verbose logs")
                        except Exception as e:
                            failed_items.append((rule_name, str(e)))
                            _mark_state_item_failed(orchestrator, item_id, e)
                        progress.advance(task)

                console.print(f"Rules: {success_count} migrated, {len(skipped_items)} skipped, {len(failed_items)} failed")
                if success_count > 0 and create_disabled:
                    console.print(f"\n[cyan]Note:[/cyan] Rules were created as [yellow]disabled[/yellow] to bypass secrets validation.")
                    console.print(f"  To enable rules:")
                    console.print(f"  1. Configure required secrets (e.g., OPENAI_API_KEY) in destination workspace settings")
                    console.print(f"  2. Enable each rule in the LangSmith UI or rerun with --rules-create-enabled")
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

    # 5. Charts
    if not skip_charts:
        console.print("[bold]Step 5: Charts[/bold]")
        from ..core.migrators import ChartMigrator
        chart_migrator = ChartMigrator(
            orchestrator.source_client,
            orchestrator.dest_client,
            orchestrator.state,
            config
        )

        if project_id_map:
            chart_migrator._project_id_map = dict(project_id_map)
            console.print(f"[dim]Using interactive project mapping ({len(project_id_map)} project(s))[/dim]")

        same_instance = False
        source_url = config.source.base_url.rstrip('/').lower()
        dest_url = config.destination.base_url.rstrip('/').lower()
        if source_url == dest_url:
            if config.source.api_key == config.destination.api_key:
                same_instance = True
                console.print("[dim]Detected same source and destination instance (same URL and API key)[/dim]")
            else:
                console.print("[dim]Detected same instance URL but different API keys (likely different workspaces).[/dim]")
                console.print("[dim]Will use project name matching instead of same session IDs.[/dim]")

        source_charts = chart_migrator.list_charts()
        if source_charts:
            console.print(f"found {len(source_charts)}")
            if _confirm_action(config, f"Migrate {len(source_charts)} chart(s)?", default=True, non_interactive_value=True):
                _ensure_migration_session(orchestrator, config)
                chart_migrator.state = orchestrator.state
                tracked_chart_items = {}

                if not same_instance and not chart_migrator._project_id_map:
                    chart_migrator._build_project_mapping()

                for chart in source_charts:
                    chart_id = chart.get("id")
                    if not chart_id:
                        continue
                    chart_title = chart.get("title") or chart.get("name") or "Untitled Chart"
                    source_session_id = chart_migrator._extract_session_id(chart)
                    dest_session_id = None
                    if source_session_id:
                        if same_instance:
                            dest_session_id = source_session_id
                        elif chart_migrator._project_id_map:
                            dest_session_id = chart_migrator._project_id_map.get(source_session_id)
                    item_id = _ensure_state_item(
                        orchestrator,
                        config,
                        "chart",
                        chart_id,
                        chart_title,
                        metadata={
                            "chart": chart,
                            "dest_session_id": dest_session_id,
                            "same_instance": same_instance,
                        },
                    )
                    tracked_chart_items[chart_id] = item_id
                    _mark_state_item_started(orchestrator, item_id)

                all_mappings = chart_migrator.migrate_all_charts(same_instance=same_instance)
                flat_chart_map = {}
                for session_chart_map in all_mappings.values():
                    flat_chart_map.update(session_chart_map)

                for chart_id, item_id in tracked_chart_items.items():
                    if chart_id in flat_chart_map:
                        _mark_state_item_completed(
                            orchestrator,
                            item_id,
                            destination_id=flat_chart_map[chart_id],
                        )
                    else:
                        _mark_state_item_failed(
                            orchestrator,
                            item_id,
                            "chart migration returned None",
                        )

                console.print(f"Charts: {len(flat_chart_map)} migrated, {len(tracked_chart_items) - len(flat_chart_map)} failed")
                console.print()
            else:
                console.print("[yellow]Skipped charts[/yellow]\n")
        else:
            console.print("[yellow]none found[/yellow]\n")
    else:
        console.print("[dim]Skipping charts (--skip-charts)[/dim]\n")


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
        orchestrator.cleanup()
        return
    if not dest_ok:
        console.print("[red]✗ Destination connection failed[/red]")
        orchestrator.cleanup()
        return
    console.print("[green]✓[/green]")

    # Resolve workspace context
    ws_result = _resolve_workspaces(orchestrator, source_workspace, dest_workspace, map_workspaces, non_interactive=config.migration.non_interactive)
    if ws_result is _WS_ABORTED:
        ctx.exit(1)
        return
    if ws_result is _WS_CANCELLED:
        console.print("[yellow]Cancelled[/yellow]")
        orchestrator.cleanup()
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

        _run_preflight(orchestrator, config, ["charts"])

        chart_migrator = ChartMigrator(
            orchestrator.source_client,
            orchestrator.dest_client,
            orchestrator.state,
            config
        )

        ws_project_id_map = _workspace_scoped_project_id_map(orchestrator, ws_result, src_ws)
        if ws_project_id_map:
            chart_migrator._project_id_map = ws_project_id_map

        # Launch interactive TUI project mapper (inside loop for workspace-scoped projects)
        if map_projects and not ws_project_id_map:
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
                dest_session_id = (
                    orchestrator.state.get_mapped_id('project', source_session_id)
                    if orchestrator.state
                    else None
                )
                if not dest_session_id:
                    console.print(f"[red]No destination mapping found for session {session}[/red]")
                    console.print("\n[yellow]This means the project/session hasn't been migrated yet.[/yellow]")
                    console.print("[yellow]Options:[/yellow]")
                    console.print("  1. Run 'langsmith-migrator datasets' first to migrate projects")
                    console.print("  2. Use --same-instance flag if source and dest are the same")
                    continue
                console.print(f"[dim]Mapped to destination session: {dest_session_id[:8]}...[/dim]\n")

            _ensure_migration_session(orchestrator, config)
            chart_migrator.state = orchestrator.state
            tracked_chart_items = {}
            for chart in chart_migrator.list_charts(source_session_id):
                chart_id = chart.get("id")
                if not chart_id:
                    continue
                chart_title = chart.get("title") or chart.get("name") or "Untitled Chart"
                item_id = _ensure_state_item(
                    orchestrator,
                    config,
                    "chart",
                    chart_id,
                    chart_title,
                    metadata={
                        "chart": chart,
                        "dest_session_id": dest_session_id,
                        "same_instance": same_instance,
                    },
                )
                tracked_chart_items[chart_id] = item_id
                _mark_state_item_started(orchestrator, item_id)

            chart_mappings = chart_migrator.migrate_session_charts(
                source_session_id,
                dest_session_id
            )

            for chart_id, item_id in tracked_chart_items.items():
                if chart_id in chart_mappings:
                    _mark_state_item_completed(
                        orchestrator,
                        item_id,
                        destination_id=chart_mappings[chart_id],
                    )
                else:
                    _mark_state_item_failed(
                        orchestrator,
                        item_id,
                        "chart migration returned None",
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

            if not _confirm_action(config, "Proceed with migration?", default=True, non_interactive_value=True):
                console.print("[yellow]Cancelled[/yellow]")
                continue

            console.print()
            _ensure_migration_session(orchestrator, config)
            chart_migrator.state = orchestrator.state
            tracked_chart_items = {}
            source_charts = chart_migrator.list_charts()
            if not same_instance:
                chart_migrator._build_project_mapping()
            for chart in source_charts:
                chart_id = chart.get("id")
                if not chart_id:
                    continue
                chart_title = chart.get("title") or chart.get("name") or "Untitled Chart"
                source_session_id = chart_migrator._extract_session_id(chart)
                dest_session_id = None
                if source_session_id:
                    if same_instance:
                        dest_session_id = source_session_id
                    elif chart_migrator._project_id_map:
                        dest_session_id = chart_migrator._project_id_map.get(source_session_id)
                item_id = _ensure_state_item(
                    orchestrator,
                    config,
                    "chart",
                    chart_id,
                    chart_title,
                    metadata={
                        "chart": chart,
                        "dest_session_id": dest_session_id,
                        "same_instance": same_instance,
                    },
                )
                tracked_chart_items[chart_id] = item_id
                _mark_state_item_started(orchestrator, item_id)
            all_mappings = chart_migrator.migrate_all_charts(same_instance=same_instance)
            flat_chart_map = {}
            for session_chart_map in all_mappings.values():
                flat_chart_map.update(session_chart_map)
            for chart_id, item_id in tracked_chart_items.items():
                if chart_id in flat_chart_map:
                    _mark_state_item_completed(
                        orchestrator,
                        item_id,
                        destination_id=flat_chart_map[chart_id],
                    )
                else:
                    _mark_state_item_failed(
                        orchestrator,
                        item_id,
                        "chart migration returned None",
                    )

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

    _display_resolution_summary(orchestrator)
    _exit_for_remediation_if_needed(ctx, config, orchestrator)
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


@cli.command()
@ssl_option
@click.pass_context
def resume(ctx):
    """Resume a previous migration session (retry pending/failed items)."""
    config = ctx.obj['config']
    state_manager = ctx.obj['state_manager']

    display_banner()

    if not ensure_config(config):
        return

    selected_session = _select_resume_session(config, state_manager)
    if not selected_session:
        return

    session_id = selected_session["session_id"]
    console.print(f"\nLoading session: {session_id[:16]}...")

    state = state_manager.load_session(session_id)
    if not state:
        console.print(f"[red]Failed to load session {session_id}[/red]")
        return

    orchestrator = MigrationOrchestrator(config, state_manager)
    try:
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

        # Attach state to orchestrator
        orchestrator.state = state
        orchestrator.state_manager.current_state = state
        config.state_manager = state_manager

        # Get resumable items
        resume_items = state.get_resume_items()
        checkpoint_items = state.get_checkpoint_items()

        console.print(f"\nResumable items: {len(resume_items)}")
        console.print(f"Checkpoint/manual items: {len(checkpoint_items)}")

        if checkpoint_items:
            console.print("\n[bold]Items requiring manual attention:[/bold]")
            for item in checkpoint_items[:10]:
                console.print(f"  • {item.name}: {item.next_action or item.outcome_code or 'needs attention'}")

        if not resume_items:
            console.print("\n[yellow]No items to resume automatically.[/yellow]")
            if checkpoint_items:
                console.print("[dim]Review the checkpoint items above and resolve them manually.[/dim]")
            return

        # Show what will be resumed
        console.print(f"\n[bold]Items to resume ({len(resume_items)}):[/bold]")
        type_counts: dict[str, int] = {}
        for item in resume_items:
            type_counts[item.type] = type_counts.get(item.type, 0) + 1
        for item_type, count in sorted(type_counts.items()):
            console.print(f"  {item_type}: {count}")

        if not _confirm_action(config, "\nProceed with resume?", default=True, non_interactive_value=True):
            console.print("[yellow]Cancelled[/yellow]")
            return

        # Run resume
        console.print(f"\n[bold]Resuming migration of {len(resume_items)} items[/bold]")
        results = orchestrator.resume_items(resume_items)

        # Save state
        state_manager.save()

        console.print("[green]Resume processing completed[/green]")

        # Report results
        resumed = results.get("resumed", [])
        blocked = results.get("blocked", [])
        console.print(f"\n[bold]Resume Results:[/bold]")
        console.print(f"  [green]Resumed successfully: {len(resumed)}[/green]")
        console.print(f"  [yellow]Blocked/needs attention: {len(blocked)}[/yellow]")

        if blocked and config.migration.verbose:
            console.print("\n[dim]Blocked items:[/dim]")
            for item_ref in blocked[:20]:
                console.print(f"  • {item_ref}")

        _display_resolution_summary(orchestrator)
        _exit_for_remediation_if_needed(ctx, config, orchestrator)
    finally:
        orchestrator.cleanup()


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
