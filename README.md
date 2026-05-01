# LangSmith Data Migration Tool

A Python CLI for migrating users and roles, datasets, experiments, annotation queues, project rules, prompts, and charts between LangSmith instances, plus CSV-driven access sync for a single LangSmith deployment.

## Quick Start

```bash
# Install (requires uv: https://docs.astral.sh/uv/)
uv tool install "langsmith-data-migration-tool @ https://github.com/langchain-ai/langsmith-data-migration-tool/releases/latest/download/langsmith_data_migration_tool-0.0.70-py3-none-any.whl"

# Set up environment variables
export LANGSMITH_OLD_API_KEY="your_source_api_key"
export LANGSMITH_NEW_API_KEY="your_destination_api_key"

# Test connections
langsmith-migrator test

# Start migrating
langsmith-migrator datasets
```

## Features

- **All-in-One Wizard**: Interactive migration of all resources (`migrate-all`)
- **Users & Roles**: Migrate custom roles, org members, and workspace memberships between instances
- **Single-Instance Access Sync**: Apply CSV-driven add/update or authoritative access sync to one LangSmith instance (`users --csv ... [--sync]`), including multi-row workspace role unionization for custom ABAC roles
- **Datasets**: Migrate datasets with examples and file attachments
- **Experiments**: Include experiments, runs, and feedback during dataset migration (`datasets --include-experiments`) or through `migrate-all`
- **Annotation Queues**: Transfer queue configurations
- **Project Rules**: Copy automation rules with project mapping and optional project creation in interactive flows
- **Prompts**: Migrate prompts (latest by default, full history with `--include-all-commits`)
- **Charts**: Migrate monitoring charts with filter preservation
- **Workspace Scoping**: Run resource migrations per workspace pair with explicit IDs or interactive workspace mapping
- **Remediation & Resume**: Persist migration state, write remediation bundles, print grouped actionable next steps, and retry pending/failed work with `resume`
- **Interactive CLI**: TUI-based selection with search/filter, plus `--non-interactive` mode for automation

## Limitations

### Trace Data Not Supported

This tool **does not support migrating trace data**. It migrates:
- Datasets and examples (including file attachments)
- Experiments, runs, and feedback
- Annotation queues
- Project rules
- Prompts
- Charts

For trace data, use LangSmith's **Bulk Export** functionality: [LangSmith Bulk Export Documentation](https://docs.langchain.com/langsmith/data-export#bulk-exporting-trace-data)

## Installation

**Prerequisites**: Python 3.12+, [uv](https://docs.astral.sh/uv/)

### Option 1: uv tool install (Recommended)
```bash
uv tool install "langsmith-data-migration-tool @ https://github.com/langchain-ai/langsmith-data-migration-tool/releases/latest/download/langsmith_data_migration_tool-0.0.70-py3-none-any.whl"

# To update an existing installation, use --force:
uv tool install --force "langsmith-data-migration-tool @ https://github.com/langchain-ai/langsmith-data-migration-tool/releases/latest/download/langsmith_data_migration_tool-0.0.70-py3-none-any.whl"
```

### Option 2: uvx (One-off execution, no install)
```bash
uvx --from "langsmith-data-migration-tool @ https://github.com/langchain-ai/langsmith-data-migration-tool/releases/latest/download/langsmith_data_migration_tool-0.0.70-py3-none-any.whl" langsmith-migrator test
```

### Option 3: pip
```bash
pip install "langsmith-data-migration-tool @ https://github.com/langchain-ai/langsmith-data-migration-tool/releases/latest/download/langsmith_data_migration_tool-0.0.70-py3-none-any.whl"
```

### Option 4: From source (Development/Contributing)
```bash
git clone https://github.com/langchain-ai/langsmith-data-migration-tool.git
cd langsmith-data-migration-tool
uv sync
# Run with: uv run langsmith-migrator <command>
```

## Configuration

### Environment Variables

```bash
export LANGSMITH_OLD_API_KEY="your_source_api_key"
export LANGSMITH_NEW_API_KEY="your_destination_api_key"

# Optional: Custom base URLs (default: https://api.smith.langchain.com)
export LANGSMITH_OLD_BASE_URL="https://your-source-instance.com"
export LANGSMITH_NEW_BASE_URL="https://your-destination-instance.com"
```

Or use a `.env` file (auto-loaded on startup):
```env
LANGSMITH_OLD_API_KEY=your_source_api_key
LANGSMITH_NEW_API_KEY=your_destination_api_key
LANGSMITH_OLD_BASE_URL=https://your-source-instance.com
LANGSMITH_NEW_BASE_URL=https://your-destination-instance.com
LANGSMITH_VERIFY_SSL=true
```

## Usage

```bash
# Test connections
langsmith-migrator test

# Interactive wizard for all resources
langsmith-migrator migrate-all
langsmith-migrator migrate-all --rules-create-enabled   # Create migrated rules as enabled

# Datasets
langsmith-migrator datasets                    # Interactive selection
langsmith-migrator datasets --all              # All datasets
langsmith-migrator datasets --include-experiments  # With experiments, runs, and feedback

# Note: When running `datasets` without `--include-experiments`, you'll be prompted
# interactively whether to include experiments. Experiments include all runs and feedback.

# Annotation queues
langsmith-migrator queues

# Prompts
langsmith-migrator prompts
langsmith-migrator prompts --all --include-all-commits

# Project rules
langsmith-migrator rules
langsmith-migrator rules --strip-projects      # As global rules
langsmith-migrator rules --project-mapping '{"old-project-id": "new-project-id"}'
langsmith-migrator rules --project-mapping mapping.json   # From file
langsmith-migrator rules --map-projects         # Interactive TUI project mapping
langsmith-migrator rules --create-enabled      # Create rules enabled (default: disabled)

# Charts
langsmith-migrator charts
langsmith-migrator charts --session "project-name"
langsmith-migrator charts --map-projects        # Interactive TUI project mapping
langsmith-migrator charts --same-instance       # Reuse source IDs only when both sides share IDs

# Utilities
langsmith-migrator list-projects --source
langsmith-migrator list_workspaces --source --dest
langsmith-migrator resume  # Resume pending/failed items from a prior migration session
langsmith-migrator clean
```

### Command Overview

- `test`: verify source and destination connectivity before running a migration
- `migrate-all`: guided end-to-end wizard for users, datasets, prompts, queues, rules, and charts
- `datasets`: migrate datasets; optionally include experiments, runs, and feedback
- `queues`: migrate annotation queues
- `prompts`: migrate prompts, optionally with full commit history
- `rules`: migrate automation rules with project mapping controls
- `charts`: migrate monitoring charts, either all sessions or one named session/project
- `users`: migrate users/roles between instances, or run single-instance CSV access sync
- `resume`: retry resumable items from a prior session and show grouped manual blockers
- `list-projects` / `list_workspaces`: inspect project and workspace IDs for mapping
- `clean`: remove saved migration sessions

### Users migration with CSV member input

When running `users`, you can provide member details from CSV instead of source member list APIs:

```bash
langsmith-migrator users --members-csv examples/users_members_example.csv --map-workspaces
```

For a single deployed LangSmith instance, `users` can also run as an access-sync command instead of a source→destination migration.

Safe default: add or update access from the CSV without removing anyone:

```bash
langsmith-migrator users \
  --api-key "$LANGSMITH_API_KEY" \
  --url "https://your-langsmith-instance.example.com" \
  --csv examples/users_members_example.csv
```

Preview the same run without making changes:

```bash
langsmith-migrator users \
  --dry-run \
  --api-key "$LANGSMITH_API_KEY" \
  --url "https://your-langsmith-instance.example.com" \
  --csv examples/users_members_example.csv
```

Authoritative mode: make the CSV the source of truth for access and remove anything not present:

```bash
langsmith-migrator users \
  --api-key "$LANGSMITH_API_KEY" \
  --url "https://your-langsmith-instance.example.com" \
  --csv examples/users_members_example.csv \
  --sync
```

Equivalent explicit form:

```bash
langsmith-migrator \
  --dest-key "$LANGSMITH_API_KEY" \
  --dest-url "https://your-langsmith-instance.example.com" \
  users \
  --single-instance \
  --members-csv examples/users_members_example.csv \
  --csv-source-of-truth
```

In `--single-instance` mode, the command mirrors the provided instance configuration onto both internal clients, so you only need one working LangSmith connection. Workspace rows use the target workspace IDs directly; there is no workspace mapping step. All CSV rows are applied automatically after a single confirmation summary; there is no row-selection step in this mode. `--api-key`/`--url` imply `--single-instance`, `--csv` is a short alias for `--members-csv`, and `--sync` is a short alias for `--csv-source-of-truth`.
`users --dry-run` is also supported as a command-local preview flag if you prefer to put dry-run after the subcommand instead of before it.
For cron jobs and other headless runs, `users --non-interactive` is also supported as a command-local alias for the global `langsmith-migrator --non-interactive users ...` form. In headless mode, missing credentials fail fast instead of prompting.

CSV schema:

```csv
email,langsmith_role,workspace_id,workspace_name
alice@example.com,Organization Admin,,
alice@example.com,Workspace Admin,ws_src_prod_us,Production US
```

Example CSVs:
- `examples/users_members_example.csv`: minimal org and workspace member rows.
- `examples/users_members_multi_workspace_roles_example.csv`: users with multiple workspace assignments, including different workspace roles.
- `examples/users_members_sync_remove_workspace_b_example.csv`: desired-state CSV for `--csv-source-of-truth` when a user should keep Workspace A and lose Workspace B.
- `examples/users_members_sync_remove_user_example.csv`: desired-state CSV for `--csv-source-of-truth` after a removed user has been omitted entirely.
- `examples/users_members_pending_mixed_workspace_roles_example.csv`: pending-invite examples for same-role multi-workspace invites and mixed workspace-role rows.

These examples are templates. Do not run them with `--csv-source-of-truth` against a populated target unless the file has been expanded into the full desired org and workspace access state for that target; authoritative sync removes org users, pending invites, and workspace memberships that are not listed.

Notes:
- `email` and `langsmith_role` are required.
- `workspace_id` is optional. Leave it empty for org-level role assignments.
- `workspace_name` is optional. When provided in single-instance CSV sync, it is validated against the target workspace display name, name, or tenant handle; `workspace_id` remains authoritative for applying access.
- `langsmith_role` should be a built-in LangSmith role name (for example `Organization Admin`, `Organization Operator`, `Organization User`, `Organization Viewer`, `Workspace Admin`, `Workspace User`, or `Workspace Viewer`) or a custom role `display_name`.
- Multiple rows for the same user and workspace are combined. Built-in workspace roles collapse to the highest-privilege built-in role; custom ABAC roles are unioned with each other and with any built-in workspace role so their policy attachments are preserved.
- Users who only appear in workspace rows are invited to the org with the source `ORGANIZATION_USER` role before workspace membership is applied.
- Workspace-only users with multiple workspace roles cannot have all workspace access attached to the initial org invite. The command calls this out before apply, attempts phase 3 workspace membership application, and may require a rerun after the invite is accepted on target versions that block workspace membership for pending org invites.
- `Organization Admin` on a workspace row is treated as org-level admin access only. No explicit workspace membership is created because org admins already have workspace access.
- Other org-scoped roles cannot be used on workspace rows. If you want org-level access, leave `workspace_id` empty.
- Workspace-scoped roles such as `Workspace Admin` cannot be used on org-level rows.
- `--sync` / `--csv-source-of-truth` is the only mode that removes access. Without it, single-instance CSV mode only adds or updates access.
- `--csv-source-of-truth` is available with `--single-instance` and makes the CSV authoritative for access:
  - users missing from the CSV are removed from the org
  - pending org invites missing from the CSV are cancelled
  - workspace memberships missing from the CSV are removed
  - workspaces omitted from the CSV are treated as having no desired memberships

Guardrails:
- `--api-key` and `--url` must be provided together when either is used.
- `--single-instance` requires `--csv` / `--members-csv`.
- `--sync` requires `--csv` and cannot be combined with `--skip-existing` or `--skip-workspace-members`.
- `--single-instance` cannot be combined with workspace mapping flags.
- `--roles-only` cannot be combined with `--single-instance` or `--members-csv`.
- If the CSV contains workspace rows and `--skip-workspace-members` is set, the command fails instead of silently ignoring those rows.
- If the CSV contains workspace-only users, the target instance must have an `ORGANIZATION_USER` role available or the command fails before applying member changes.
- If the CSV references unknown `workspace_id` values, the command fails before any membership changes are applied.

Operational notes:
- Authoritative sync can remove active org members, cancel pending invites, and remove workspace memberships. Use an Organization Admin PAT for unattended runs that need removal semantics.
- If the destination API key cannot manage org members or pending invites, 401/403 responses are reported as Organization Admin PAT blockers with grouped remediation and resume metadata.
- Pending invites whose org role or workspace access does not match the CSV are refreshed when the target supports invite cancellation; if the target lacks a cancellation endpoint, the command reports an explicit manual follow-up instead of silently accepting stale access.

### CLI Options

```bash
--source-key TEXT       Source API key
--dest-key TEXT         Destination API key
--source-url TEXT       Source base URL
--dest-url TEXT         Destination base URL
--no-ssl                Disable SSL verification
--batch-size INTEGER    Batch size for operations (1-1000, default: 100)
--workers INTEGER       Number of concurrent workers (1-10, default: 4)
--dry-run               Run without making changes
--skip-existing         Skip existing resources instead of updating them
--non-interactive       Disable prompts and exit with code 2 when remediation is required
--verbose, -v           Verbose output
```

### Dataset Options

```bash
--include-experiments   Include experiments with datasets
--all                   Migrate all datasets
```

### Prompt Options

```bash
--all                   Migrate all prompts
--include-all-commits   Include all commit history
```

### Rules Options

```bash
--strip-projects        Strip project associations and create as global rules
--project-mapping TEXT  JSON string or file path with project ID mapping (e.g., '{"old-id": "new-id"}')
--map-projects          Launch interactive TUI to map source projects to destination projects
--create-enabled        Create rules as enabled (default: disabled to bypass secrets validation)
--all                   Migrate all rules without interactive selection
```

### Migrate-All Rules Options

```bash
--rules-create-enabled  Create migrated rules as enabled (default: disabled)
```

If `--rules-create-enabled` is omitted, `migrate-all` asks interactively whether to create rules enabled.
The prompt default is `No` (rules are created disabled).

### Migrate-All Options

```bash
--skip-users            Skip user and role migration
--skip-datasets         Skip dataset migration
--skip-experiments      Skip experiment migration
--skip-prompts          Skip prompt migration
--skip-queues           Skip annotation queue migration
--skip-rules            Skip rules migration
--skip-charts           Skip chart migration
--include-all-commits   Include all prompt commit history
--strip-projects        Strip project associations from rules
--map-projects          Launch interactive TUI to map source projects to destination projects
--rules-create-enabled  Create migrated rules as enabled instead of asking interactively
```

### Chart Options

```bash
--session TEXT          Migrate charts for a specific session/project (by name or ID)
--map-projects          Launch interactive TUI to map source projects to destination projects
--same-instance         Reuse source project/session IDs on destination only when both sides truly share IDs
```

### Users Options

```bash
--dry-run                  Preview this users sync without making POST/PATCH/DELETE changes
--non-interactive          Disable prompts for this users run. Same as the global --non-interactive.
--roles-only               Only migrate custom roles (skip member migration)
--skip-workspace-members   Skip workspace member migration
--single-instance, --instance
                           Use one target LangSmith instance for CSV-driven access sync instead of source→destination migration
--csv-source-of-truth, --sync
                           Make the CSV authoritative for single-instance sync: any active org user or pending invite not
                           present in the CSV will be removed, and workspace memberships not present in the CSV will also
                           be removed. Without this flag, CSV mode only adds or updates access.
--members-csv, --csv PATH  CSV file with member details (email, langsmith_role, workspace_id, workspace_name)
                           Replaces source member API lookups. In --single-instance mode, all CSV rows are applied
                           automatically.
--api-key TEXT             API key for the single-instance CSV sync target. Must be provided together with --url.
--url TEXT                 Base URL for the single-instance CSV sync target. Must be provided together with --api-key.
--source-workspace TEXT    Source workspace ID (skip auto-detection)
--dest-workspace TEXT      Destination workspace ID (skip auto-detection)
--map-workspaces           Force workspace mapping TUI even for single-workspace instances
```

### Common Workspace Options

These flags are available on `datasets`, `queues`, `prompts`, `rules`, `charts`, `migrate-all`, and `users`:

```bash
--source-workspace TEXT    Source workspace ID (skip auto-detection)
--dest-workspace TEXT      Destination workspace ID (skip auto-detection)
--map-workspaces           Force workspace mapping TUI even for single-workspace instances
```

Migration proceeds in three phases:
1. **Role sync** (org-scoped): match built-in roles by name, create/update custom roles
2. **Org members** (org-scoped): invite missing members, update roles for existing ones
3. **Workspace members** (per workspace pair): add members to workspaces with correct roles

Rules are created disabled by default. Use `--create-enabled` on the `rules` command to override.

### Migrate-All Users Options

```bash
--skip-users               Skip user and role migration in migrate-all wizard
```

When `--skip-users` is omitted, `migrate-all` runs user/role migration as Step 0 before all other resources. Phases 1-2 (roles + org members) run once; phase 3 (workspace members) runs per workspace pair.

### Project Mapping

Rules and charts reference projects by ID. When migrating between instances, project IDs differ.

- **Interactive TUI (`--map-projects`)**: Launch a visual TUI to map source projects to destination projects. Available on `rules`, `charts`, and `migrate-all` commands. Select a source project and type a destination name directly — existing projects appear as filterable suggestions below the input. Supports auto-match by name, skip, and custom name entry.
- **Rules (`--project-mapping`)**: Supply an explicit source→destination project ID mapping as JSON or a file path. Use `list-projects --source` and `list-projects --dest` to get IDs. The mapping is applied to both top-level project associations and project IDs embedded inside rule filters. Mutually exclusive with `--map-projects`.
- **Rules queue targets**: When a rule references an annotation queue, the migrator first reuses any saved queue migration mapping, then falls back to an exact-name queue match in the destination workspace. If neither is safe, the rule is exported for remediation instead of being posted with the source queue ID.
- **Charts**: Without `--map-projects`, project mapping is built automatically by matching project names between source and destination. When both sides point at the same deployment URL but use different API keys/workspaces, charts still remap project/session IDs; the tool does not treat that as `--same-instance`. Workspace-scoped `--map-projects` mappings are resolved against the active workspace pair so duplicate project names in other workspaces are ignored.
- **`migrate-all`**: Supports `--strip-projects`, `--map-projects`, and `--rules-create-enabled` for rules. Use the standalone `rules` command for `--project-mapping` JSON mappings.

### Interactive Selection

Keyboard shortcuts in resource selection TUI:
- `↑↓` Navigate | `Space` Toggle | `a` Select all | `n` Clear
- `/` Search | `Enter` Confirm | `Esc` Cancel

Keyboard shortcuts in project mapper TUI (`--map-projects`):
- `Enter/Space` Edit destination | `s` Skip | `m` Same name | `u` Unmap
- `a` Auto-match all | `/` Search | `Ctrl+S` Save | `Esc` Cancel

Keyboard shortcuts in workspace mapper TUI (`--map-workspaces`):
- `Enter` Pick destination | `n` Create new | `c` Create all unmapped
- `p` Map projects | `s` Skip | `a` Auto-match all | `u` Unmap
- `Ctrl+S` Save | `Esc` Cancel

### Workspace Scoping

For multi-workspace organizations, all resource commands support workspace-scoped migration:

```bash
# Interactive workspace mapping TUI (available on all commands)
langsmith-migrator datasets --map-workspaces
langsmith-migrator queues --map-workspaces
langsmith-migrator prompts --map-workspaces
langsmith-migrator rules --map-workspaces --map-projects
langsmith-migrator charts --map-workspaces --map-projects
langsmith-migrator migrate-all --map-workspaces

# Explicit workspace pair
langsmith-migrator datasets --source-workspace WS_ID --dest-workspace WS_ID
```

When using `--map-workspaces`, each command iterates all mapped workspace pairs, running the full fetch/select/migrate flow per pair. For `rules` and `charts` with `--map-projects`, the project mapping TUI is shown per workspace pair so projects are correctly scoped.

### Resume and Remediation

Every migration session persists state and writes a remediation bundle when there are blocked or manual-follow-up items.

- Session state is stored under `~/.langsmith-migrator/state`.
- Remediation bundles are written under `./.langsmith-migrator/remediation/<session_id>` by default.
- The CLI prints a **Resolution Summary** with grouped **Actionable Next Steps** instead of one repeated line per failed item.
- `--non-interactive` disables prompts and exits with status code `2` if manual remediation is still required.

`langsmith-migrator resume` retries pending or failed items from a previous session. Today that includes:

- datasets
- experiments
- prompts
- annotation queues
- rules
- charts
- org members
- workspace members

For chart items, `resume` revalidates saved `--same-instance` metadata against the current source/destination and workspace context. If the mode changed, it re-resolves the destination project/session before retrying; if that cannot be resolved safely, the item is checkpointed with guidance to rerun `charts` with project mapping. Global or dataset-only charts that have no project/session dependency can resume with `dest_session_id=None`.

Use `langsmith-migrator clean` to remove saved sessions once you no longer need their state or remediation bundles.

## SSL Certificate Issues

For self-hosted instances with SSL errors:

```bash
# Use --no-ssl flag
langsmith-migrator --no-ssl datasets

# Or set environment variable
export LANGSMITH_VERIFY_SSL=false
```

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions welcome! Fork, create a feature branch, and submit a Pull Request.

### Release checklist

For release changes, update all of:
- `pyproject.toml` version
- `CHANGELOG.md` release notes
- `README.md` release-facing docs/examples

CI enforces this on pull requests: if `pyproject.toml` or `CHANGELOG.md` changes, `README.md` must also be updated (including dependency-only bumps).

## Support

For issues or questions: [GitHub repository](https://github.com/langchain-ai/langsmith-data-migration-tool)
