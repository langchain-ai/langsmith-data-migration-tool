# AGENTS.md

This file provides guidance to Codex (Codex.ai/code) when working with code in this repository.

## Project Overview

LangSmith Data Migration Tool - A Python CLI for migrating datasets, experiments, annotation queues, project rules, prompts, and charts between LangSmith instances. Built with Click CLI, Textual TUI, and the LangSmith SDK.

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
uv run langsmith-migrator migrate_all  # Migrate everything
uv run langsmith-migrator migrate_all --map-projects                # Migrate all with interactive project mapping
uv run langsmith-migrator resume       # Resume a previous session
uv run langsmith-migrator list_projects # List available projects
uv run langsmith-migrator list_workspaces --source --dest           # List workspaces
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
    └── ChartMigrator      - Monitoring charts and dashboards
```

### Key Components

- **EnhancedAPIClient** (`core/api_client.py`): HTTP wrapper with retry logic, rate limiting, and pagination support
- **State Management** (`utils/state.py`): Session tracking, ID mappings, and resume capability
- **TUI Selector** (`cli/tui_selector.py`): Textual-based interactive selection with search/filter
- **TUI Project Mapper** (`cli/tui_project_mapper.py`): Text-input-first project mapping with suggestion filtering
- **TUI Workspace Mapper** (`cli/tui_workspace_mapper.py`): Interactive N-to-N workspace mapping with create-new support
- **Workspace Resolver** (`utils/workspace_resolver.py`): Auto-detection and resolution of multi-workspace environments
- **Config** (`utils/config.py`): Environment variables, CLI arguments, and `.env` file handling
- **Pagination** (`utils/pagination.py`): Pagination helpers for API list endpoints

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
4. **Rules disabled by default**: Rules are created disabled to avoid secrets validation issues

## Testing

- Tests use pytest with fixtures in `tests/conftest.py`
- Unit tests: `tests/unit/`
- Functional tests: `tests/functional/`
- HTTP mocking with `respx` library
- When testing `Config`, use an autouse fixture to `monkeypatch.delenv` all `LANGSMITH_*` and `MIGRATION_*` env vars to prevent host environment leakage
- Retry tests should patch `langsmith_migrator.utils.retry._jittered_sleep` instead of `time.sleep` to test wait logic without actual delays
- Pagination tests should verify that non-`NotFoundError` exceptions propagate (not silently swallowed)

## Release Process

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md` with new version section
3. Commit and push changes (via PR if branch protection enabled)
4. Create and push git tag:
   ```bash
   git tag -a v0.0.x -m "Release v0.0.x"
   git push origin v0.0.x
   ```
5. Create GitHub release:
   ```bash
   gh release create v0.0.x --title "v0.0.x" --notes "Release notes here"
   ```
6. Build and upload artifacts:
   ```bash
   uv build
   gh release upload v0.0.x dist/langsmith_data_migration_tool-0.0.x-py3-none-any.whl dist/langsmith_data_migration_tool-0.0.x.tar.gz
   ```

### Installing from Release

```bash
uv tool install --force "langsmith-data-migration-tool @ https://github.com/langchain-ai/langsmith-data-migration-tool/releases/download/v0.0.x/langsmith_data_migration_tool-0.0.x-py3-none-any.whl"
```
functional/`
- HTTP mocking with `respx` library

## Release Process

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md` with new version section
3. Commit and push changes (via PR if branch protection enabled)
4. Create and push git tag:
   ```bash
   git tag -a v0.0.x -m "Release v0.0.x"
   git push origin v0.0.x
   ```
5. Create GitHub release:
   ```bash
   gh release create v0.0.x --title "v0.0.x" --notes "Release notes here"
   ```
6. Build and upload artifacts:
   ```bash
   uv build
   gh release upload v0.0.x dist/langsmith_data_migration_tool-0.0.x-py3-none-any.whl dist/langsmith_data_migration_tool-0.0.x.tar.gz
   ```

### Installing from Release

```bash
uv tool install --force "langsmith-data-migration-tool @ https://github.com/langchain-ai/langsmith-data-migration-tool/releases/download/v0.0.x/langsmith_data_migration_tool-0.0.x-py3-none-any.whl"
```
