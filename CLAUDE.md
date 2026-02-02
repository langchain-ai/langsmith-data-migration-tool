# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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
- **Config** (`utils/config.py`): Environment variables and CLI argument handling

### Entry Points

- CLI: `langsmith_migrator/__main__.py` → `cli/main.py` (Click commands)
- Package entry point: `langsmith-migrator` command

## Configuration

Environment variables (can also use CLI flags):
- `LANGSMITH_OLD_API_KEY` / `LANGSMITH_NEW_API_KEY` - Source/destination API keys
- `LANGSMITH_OLD_BASE_URL` / `LANGSMITH_NEW_BASE_URL` - Instance URLs
- `MIGRATION_BATCH_SIZE` (default: 100)
- `MIGRATION_WORKERS` (default: 4)
- `MIGRATION_DRY_RUN`, `MIGRATION_VERBOSE`, `MIGRATION_SKIP_EXISTING`

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

## Release Process

1. Update version in `pyproject.toml`
2. Update `CHANGELOG.md`
3. Create and push git tag: `git tag -a v0.0.x -m "Release v0.0.x" && git push origin v0.0.x`
4. GitHub Actions automatically builds and creates release
