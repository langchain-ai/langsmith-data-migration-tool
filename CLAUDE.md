# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

LangSmith Data Migration Tool - A Python CLI for migrating datasets, experiments, annotation queues, project rules, prompts, charts, and Fleet resources between LangSmith instances. Built with Click CLI, Textual TUI, and the LangSmith SDK.

## Development Commands

```bash
# Install dependencies
uv sync

# Run tests
uv run pytest
uv run pytest --cov                    # With coverage
uv run pytest tests/unit/ -v           # Unit tests only
uv run pytest tests/test_rules_migrator.py::test_name -v  # Single test

# Build
uv build

# Run CLI
uv run langsmith-migrator --help
uv run langsmith-migrator test         # Test connections
uv run langsmith-migrator datasets     # Migrate datasets
uv run langsmith-migrator queues       # Migrate annotation queues
uv run langsmith-migrator prompts      # Migrate prompts
uv run langsmith-migrator rules        # Migrate project rules
uv run langsmith-migrator rules --strip-projects                    # Rules as global (no project)
uv run langsmith-migrator rules --project-mapping '{"old": "new"}'  # Custom project ID mapping
uv run langsmith-migrator rules --map-projects                      # Interactive TUI project mapping
uv run langsmith-migrator rules --create-enabled                    # Create rules enabled
uv run langsmith-migrator charts       # Migrate charts
uv run langsmith-migrator charts --map-projects                     # Charts with interactive project mapping
uv run langsmith-migrator charts --project-mapping '{"old": "new"}' # Charts with headless project ID mapping (no TUI)
uv run langsmith-migrator charts --same-instance                    # Reuse source session IDs
uv run langsmith-migrator migrate-all  # Migrate everything
uv run langsmith-migrator migrate-all --map-projects                # Migrate all with interactive project mapping
uv run langsmith-migrator migrate-all --project-mapping '{"old": "new"}'  # Migrate all with headless project ID mapping (no TUI)
uv run langsmith-migrator migrate-all --rules-create-enabled        # Create migrated rules enabled
uv run langsmith-migrator fleet           # Migrate Fleet resources (agents, skills, MCP servers, etc.)
uv run langsmith-migrator fleet --skip-agents --skip-skills          # Skip specific Fleet resources
uv run langsmith-migrator contexts     # Migrate Context Hub agents & skills (full commit history by default)
uv run langsmith-migrator contexts --agents-only                   # Only agent contexts
uv run langsmith-migrator contexts --skills-only                   # Only skill contexts
uv run langsmith-migrator contexts --latest-only                   # Copy only each context's latest commit (skip history)
uv run langsmith-migrator contexts --no-tags                       # Skip commit tags (production/staging + custom); tags are migrated by default
uv run langsmith-migrator contexts --same-instance                 # Preserve linked-repo commit pins
uv run langsmith-migrator contexts --include-external              # Also migrate source=external repos (hidden by default to match the UI)
uv run langsmith-migrator resume       # Resume interrupted dataset migration
uv run langsmith-migrator list-projects # List available projects
uv run langsmith-migrator list_workspaces --source --dest           # List workspaces
uv run langsmith-migrator export-users --source -o users.csv        # Export active org/workspace members to a members CSV (pending invites excluded)
uv run langsmith-migrator clean        # Clean migration state

# Workspace-scoped migration (available on all resource commands)
uv run langsmith-migrator datasets --map-workspaces                 # Interactive workspace mapping TUI
uv run langsmith-migrator queues --map-workspaces                   # Queues across all workspace pairs
uv run langsmith-migrator prompts --map-workspaces                  # Prompts across all workspace pairs
uv run langsmith-migrator rules --map-workspaces --map-projects     # Rules with per-workspace project mapping
uv run langsmith-migrator charts --map-workspaces --map-projects    # Charts with per-workspace project mapping
uv run langsmith-migrator datasets --source-workspace WS_ID --dest-workspace WS_ID  # Explicit workspace pair
```

## Architecture

### Core Pattern: Orchestrator + Specialized Migrators

```
MigrationOrchestrator (core/migrators/orchestrator.py)
    ├── Manages source/destination API clients
    ├── Handles state persistence and resume capability
    └── Coordinates parallel migrations via ThreadPoolExecutor

BaseMigrator (core/migrators/base.py)
    ├── DatasetMigrator    - Datasets with examples and attachments
    ├── ExperimentMigrator - Experiments linked to datasets
    ├── FeedbackMigrator   - Feedback records for experiment runs
    ├── AnnotationQueueMigrator
    ├── PromptMigrator     - Uses LangSmith SDK for prompt operations
    ├── RulesMigrator      - Project automation rules (v3+ evaluators)
    ├── ChartMigrator      - Monitoring charts and dashboards
    ├── UserRoleMigrator   - Users, roles, and workspace memberships
    ├── FleetSkillMigrator          - Fleet shared workspace skills
    ├── FleetMcpServerMigrator      - Fleet MCP servers and integrations
    ├── FleetAgentMigrator          - Fleet agents with cross-reference remapping
    ├── FleetScheduleMigrator       - Fleet agent cron schedules
    ├── FleetSecretMigrator         - Fleet workspace secrets (names only, write-only API)
    ├── FleetAuthProviderMigrator   - Fleet OAuth auth providers (structure only, client_secret write-only)
    ├── FleetTriggerMigrator        - Fleet triggers
    ├── FleetWebhookMigrator        - Fleet platform webhooks
    ├── FleetUsageLimitMigrator     - Fleet spend limits
    ├── FleetSandboxPolicyMigrator  - Fleet sandbox policies
    └── ContextHubMigrator          - Context Hub agents & skills via the SDK directories API
```

Note: `ContextHubMigrator` uses the LangSmith SDK for pulls/pushes
(`push_agent`/`push_skill`, `pull_agent`/`pull_skill`) over managed sessions,
mirroring `PromptMigrator`. It lists repos via the raw `/repos` hub endpoint
(not the SDK's `list_agents`/`list_skills`) because that endpoint exposes the
`source` field, which the SDK's typed model drops. By default the listing
matches the Context Hub UI, which sends `exclude_source=external` and hides
externally-created repos (e.g. Agent Builder drafts with UUID handles); the
migrator both sends `exclude_source=external` and filters `source == "external"`
client-side (the deployed backend may ignore the query param). Pass
`include_external=True` (CLI `--include-external`) to migrate every repo.
By default the migrator replays the **full commit history**: it enumerates the
commit chain with `list_prompt_commits` (which works for directory-type repos)
and pushes each commit oldest->newest, chaining `parent_commit`. Because
directory commits are content-addressed, this reproduces the source's commit
hashes. Repo metadata is applied on the first commit. Pass
`include_all_commits=False` (CLI `--latest-only`) to copy only the latest commit
as a single fresh commit. Commit tags are migrated by default (CLI `--no-tags`
to skip): after a repo's commits are pushed, each source tag - including the
`production` / `staging` environment tags behind the Context Hub promote feature
- is re-created on the destination via `/repos/{owner}/{repo}/tags` pointing at
the commit with the same content-addressed hash (tags whose target commit is not
present on the destination are skipped and recorded as a `degraded` issue).
Cross-instance, linked-repo entries
(`skills/...`, `agents/...`) have their source commit pins stripped so links
resolve to the destination's latest commit of each linked repo (recorded as a
`degraded` issue); `--same-instance` preserves them.

### Key Components

- **EnhancedAPIClient** (`core/api_client.py`): HTTP wrapper with retry logic, rate limiting, and pagination support (offset-based and cursor-based)
- **State Management** (`utils/state.py`): Session tracking, ID mappings, and resume capability
- **TUI Selector** (`cli/tui_selector.py`): Textual-based interactive selection with search/filter
- **TUI Project Mapper** (`cli/tui_project_mapper.py`): Text-input-first project mapping with suggestion filtering
- **TUI Workspace Mapper** (`cli/tui_workspace_mapper.py`): Interactive N-to-N workspace mapping with create-new support
- **Workspace Resolver** (`utils/workspace_resolver.py`): Auto-detection and resolution of multi-workspace environments
- **Config** (`utils/config.py`): Environment variables, CLI arguments, and `.env` file handling
- **Pagination** (`utils/pagination.py`): `PaginationHelper` for offset-based APIs, `CursorPaginationHelper` for Fleet cursor-based APIs

### Entry Points

- CLI: `langsmith_migrator/__main__.py` → `cli/main.py` (Click commands)
- Package entry point: `langsmith-migrator` command

## Configuration

Environment variables (can also use CLI flags or a `.env` file — auto-loaded on startup):
- `LANGSMITH_OLD_API_KEY` / `LANGSMITH_NEW_API_KEY` - Source/destination API keys
- `LANGSMITH_OLD_BASE_URL` / `LANGSMITH_NEW_BASE_URL` - Instance URLs
- `MIGRATION_BATCH_SIZE` (default: 100)
- `MIGRATION_WORKERS` (default: 4)
- `MIGRATION_CHUNK_SIZE` (default: 1000)
- `MIGRATION_RATE_LIMIT_DELAY` (default: 0.1)
- `MIGRATION_STREAM_EXAMPLES` (default: true)
- `MIGRATION_DRY_RUN`, `MIGRATION_VERBOSE`, `MIGRATION_SKIP_EXISTING`
- `LANGSMITH_VERIFY_SSL` (default: true)

## Key Design Patterns

1. **Streaming for large datasets**: Examples are processed in chunks to avoid memory issues
2. **Retry with exponential backoff**: All API calls use retry logic in `utils/retry.py`
3. **ID Mapping**: Migrators track source→destination ID mappings for cross-references
4. **Rules disabled by default**: Rules are created disabled by default in all flows to avoid secrets validation issues; use explicit flags (`--create-enabled` / `--rules-create-enabled`) to create enabled rules
5. **Run timestamps anchored to "now" on migration**: When migrating experiment
   data (datasets `--include-experiments` or `migrate-all`), every run is
   shifted by `now - max(experiment.end_time, experiment.start_time)` so it
   fits the destination's 24h `POST /runs/batch` window. Offsets between
   runs in the same experiment are preserved. The shift is persisted per
   experiment in state so resumes stay consistent.

## Testing

- Tests use pytest with fixtures in `tests/conftest.py`
- Unit tests: `tests/unit/`
- Functional tests: `tests/functional/`
- HTTP mocking with `respx` library

## Release Process

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md` with new version section
3. Update `README.md` alongside the release changes - CI (`test.yml`'s
   "Ensure README updated for release changes" check) fails the PR if
   `pyproject.toml`'s `version` line or `CHANGELOG.md` changed without a
   corresponding `README.md` change
4. Commit and push changes (via PR if branch protection enabled)
5. Create and push git tag:
   ```bash
   git tag -a v0.0.x -m "Release v0.0.x"
   git push origin v0.0.x
   ```

Pushing the tag is the last manual step. `.github/workflows/release.yml`
triggers on the tag push and handles the rest automatically: it builds the
wheel/sdist, extracts that version's notes from `CHANGELOG.md`, and creates
(or updates) the GitHub release with the artifacts uploaded - no need to run
`uv build` or `gh release create`/`upload` by hand.

### Installing from Release

```bash
uv tool install --force "langsmith-data-migration-tool @ https://github.com/langchain-ai/langsmith-data-migration-tool/releases/download/v0.0.x/langsmith_data_migration_tool-0.0.x-py3-none-any.whl"
```
