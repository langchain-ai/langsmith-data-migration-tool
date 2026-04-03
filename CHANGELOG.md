# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.0.59] - 2026-04-02

### Fixed
- **TUI: Enter confirms selection** — Enter key now correctly confirms the selection instead of toggling rows. Space remains the toggle key.
- **TUI: Escape from search preserves filter** — Pressing Escape while in the search box returns focus to the table without clearing the search filter.
- **Role sync no longer auto-creates all custom roles** — Phase 1 matches built-in roles only. Custom roles are auto-synced only for the members you select in Phase 2. Use `--roles-only` for interactive custom role selection.

## [0.0.58] - 2026-04-02

### Added
- **`resume` CLI command**: New command to resume interrupted migrations. Loads the latest session, displays resumable items, and dispatches each to the correct migrator.
- **Prompt state tracking**: Both standalone `prompts` command and `migrate_all` now create state items for each prompt, enabling resume on failure.
- **`request_with_retry` utility**: New retry-aware wrapper for raw `requests.Session` calls used by prompt, rules, and dataset migrators.
- **Feedback source remapping**: `feedback_source` and `source_run_id` fields now have embedded run IDs remapped to destination IDs.
- **URL sanitization in errors**: `APIError.__str__` now redacts query parameters from URLs to prevent API key leakage in logs and state files.
- **Non-interactive guards**: `--map-projects` and `--map-workspaces` now error clearly in `--non-interactive` mode instead of hanging.
- **311 tests** (up from 205) with new test files for API client, retry, pagination, config, state transitions, dataset migrator, and feedback migrator.

### Fixed
- **Thread safety**: Parallel dataset migration now creates per-worker API clients instead of sharing a single `requests.Session` across threads.
- **Atomic state writes**: `StateManager.save()` and `save_config()` now use temp file + `os.replace()` to prevent data loss on interruption.
- **Attempt counting**: `update_item_status` only increments attempts on `FAILED` transitions, fixing inflated counts that prevented resume.
- **Resume infinite loop**: `get_resume_items()` now excludes items that already have a terminal state.
- **Terminal state guard**: `mark_terminal` prevents overwriting successful terminal states (MIGRATED cannot be demoted to BLOCKED).
- **Feedback data fidelity**: Switched from allowlist to denylist approach, preserving `feedback_config`, `extra`, `source_run_id`, and `source_info` fields.
- **Reference example ID**: Unmapped `reference_example_id` now preserved with warning instead of silently dropped.
- **Deterministic evaluator keys**: Replaced Python's process-salted `hash()` with stable `hashlib.sha256` for evaluator feedback keys.
- **O(n²) run mapping**: Replaced per-run `{**dict_a, **dict_b}` merge with `ChainMap` for large experiment migrations.
- **Session cleanup**: All 11 CLI commands now use `ctx.with_resource(orchestrator)` for guaranteed HTTP session cleanup on any exit path.
- **Pagination safety**: `PaginationHelper` only catches `NotFoundError` instead of silently swallowing all exceptions. Caller's `params` dict is no longer mutated.
- **Config validation**: Added bounds checks for `timeout`, `chunk_size`, and `rate_limit_delay`. Fixed `batch_size=0` falling through to env var default.
- **Silent exception swallowing**: `list_projects` and `create_workspace` now propagate auth/network errors instead of returning empty results.
- **Rules data correctness**: `_clean_none_values` preserves positional `None`s in lists; `alerts`/`webhooks` now cleaned; unmapped `add_to_dataset_id` no longer falls back to source ID; rule update failures now mark terminal state; symmetric verification normalization.
- **Multi-session charts**: Charts with multiple sessions now map each session ID individually instead of collapsing to a single session.
- **Org/workspace member resume**: `resume_items` now properly marks terminal state and saves after successful org/ws member migration.
- **`migrate_all` prompt guard**: Added `check_prompts_api_available` before prompt migration in `migrate_all`.
- **Request body removed from errors**: 404 error info no longer includes `response.request.body` which could contain secrets.

### Removed
- Dead `CustomNameScreen` class from `tui_project_mapper.py`.

## [0.0.57] - 2026-03-31

### Added
- **User and role migration**: New `users` CLI command migrates custom workspace roles, org members, and workspace memberships between LangSmith instances.
- **Custom role sync**: Matches built-in roles by name and creates/updates custom roles by display name with full permission mapping.
- **Workspace member migration**: Per-workspace-pair member migration with `--map-workspaces` support.
- **Granular control**: `--roles-only` to sync only custom roles, `--skip-workspace-members` to skip workspace-level migration.
- **Resume support**: Added `org_member` and `ws_member` item types to the resume workflow.

## [0.0.56] - 2026-04-01

### Added
- **migrate-all charts support**: Included chart migration as step 5 in `migrate-all`, with same-instance detection and migration-state tracking.
- **Charts skip flag for migrate-all**: Added `--skip-charts` to bypass chart migration when desired.

### Improved
- **Wizard control for charts**: Users can now explicitly skip charts from the interactive `migrate-all` wizard prompt even when charts are available.
- **Workspace/project mapping parity**: `migrate-all` chart migration now honors workspace-scoped and project-mapped flows consistently.

## [0.0.55] - 2026-04-01

### Fixed
- **CLI log noise**: Suppressed low-signal `run compression is not enabled` output during normal migration runs
- **Release workflow reliability**: Made release publishing idempotent when the tag/release already exists

## [0.0.54] - 2026-03-31

### Removed
- **Trivy vulnerability scanner**: Removed Trivy scan and SARIF upload steps from security workflow

### Fixed
- **Workspace mapper cursor**: Preserve cursor position when navigating workspace mapper TUI

### Changed
- **LangSmith SDK**: Bumped minimum `langsmith` dependency to `>=0.7.23`

## [0.0.53] - 2026-03-31

### Added
- **Comprehensive CLI functional test suite**: End-to-end test coverage for all CLI commands including datasets, queues, prompts, rules, charts, migrate-all, resume, and workspace flows
- **Migration resolution regression tests**: Coverage for experiment resume, state resolution, and remediation bundle preservation

### Fixed
- **Rules enable defaults**: Aligned `--create-enabled` / `--rules-create-enabled` flag behavior across `rules` and `migrate-all` commands to match documentation
- **Migration reliability**: Addressed gaps in state resolution, remediation bundle preservation on resume, and resolver CLI flows
- **Ruff lint**: Removed unused `commits_by_hash` variable in prompt migrator

## [0.0.51] - 2026-03-10

### Added
- **Workspace TUI "Create all unmapped" bulk action**: Press `c` in the workspace mapper TUI to mark all unmapped source workspaces as "create new" in one keystroke
- **All commands iterate all workspace pairs**: `queues`, `prompts`, `rules`, and `charts` now iterate every mapped workspace pair when using `--map-workspaces`, matching the behavior of `datasets` and `migrate-all`

### Fixed
- **Workspace scoping for queues/prompts/rules/charts**: Previously these commands only activated the first workspace pair and silently ignored the rest; resources now land in the correct workspace

### Removed
- Internal `_activate_workspace_or_cancel` helper (replaced by per-command workspace iteration loops)

## [0.0.50] - 2026-03-06

### Added
- **`.env` file support**: API keys and configuration are now auto-loaded from a `.env` file on startup via `python-dotenv`

### Improved
- **Project Mapper TUI**: Redesigned destination picker to be text-input-first — type a destination name directly with existing projects shown as filterable suggestions below
- **Project Mapper UX**: Fixed focus stealing when typing in the destination name input
- **Project Mapper UX**: Updated help text to clarify `Enter/Space` opens the destination editor

### Fixed
- **TUI Key Handling**: App-level priority keybindings (`s`, `m`, `u`, `a`, `q`) no longer intercept keystrokes when the destination picker modal is open

## [0.0.42] - 2026-02-02

### Fixed
- **Experiment Run Migration**: Fixed `trace_id does not match first part of dotted_order` validation errors
  - Root runs now correctly set `trace_id = run_id` (API requirement)
  - Child runs properly look up `trace_id` from mapping set by root run
  - Added `_regenerate_dotted_order()` to remap UUIDs in dotted_order field
  - Runs sorted by dotted_order to ensure parents processed before children
  - Generate new UUIDs for destination runs to avoid conflicts
  - Remove None values from run payloads to avoid 422 errors

### Improved
- **API Error Messages**: Enhanced error detail extraction to handle validation errors and various error response formats

### Added
- **Dev Dependencies**: Added pytest as dev dependency

## [0.0.41] - 2026-02-02

### Added
- **Production Readiness Audit Fixes**: Comprehensive error handling and robustness improvements
- **New Exception Classes**: `AuthenticationError` (401/403) and `ConflictError` (409) for explicit HTTP status handling
- **Batch Error Tracking**: `BatchResult` and `BatchItemResult` classes to track individual item success/failure in batch operations
- **Thread Safety**: Thread locks for shared state access during parallel migrations
- **Attachment Validation**: Size limits (100MB max) and content-type validation for attachment downloads
- **URL Validation**: Configuration validates URL format (must include scheme)
- **CLI Input Validation**: `--batch-size` (1-1000) and `--workers` (1-10) now use IntRange constraints
- **Environment Variable Documentation**: CLI help text now shows environment variable names

### Fixed
- **Silent Data Loss**: Batch operations now track which items failed instead of returning `[None]`
- **Authentication Errors**: 401/403 errors now raise `AuthenticationError` with clear guidance
- **Conflict Handling**: 409 errors now raise `ConflictError` for proper duplicate handling
- **Bare `except:` Clauses**: Replaced 3 bare except clauses with specific exception types
- **Rate Limiting**: Added `Retry-After` header support and maximum backoff cap (60 seconds)
- **Network Errors**: Now handles `Timeout`, `ReadTimeout`, and `socket.timeout` in addition to `ConnectionError`
- **Response Validation**: All API responses now validated before accessing fields
- **Environment Parsing**: Safe parsing of numeric environment variables with try-catch

### Changed
- **API Client**: `test_connection()` now returns `Tuple[bool, Optional[str]]` (success, error_message)
- **Orchestrator**: `test_connections_detailed()` now returns 4-tuple with error messages
- **Logging**: Filtered field warnings in rules migration changed from "info" to "warning" level
- **Chart Migration**: Added verbose logging when charts are filtered out

### Security
- **Attachment Downloads**: Enforces size limits during streaming to prevent memory exhaustion
- **Content-Type Validation**: Validates attachment MIME types before download

## [0.0.4] - 2025-02-02

### Added
- **Feedback Migration**: New `FeedbackMigrator` class to migrate feedback records for experiment runs
  - Automatically migrates feedback when using `--include-experiments` with datasets
  - Maps run IDs correctly between source and destination
- **CLAUDE.md**: Development guidance file for Claude Code

### Fixed
- **Multi-experiment Run Migration**: Fixed bug where only the first experiment's runs were migrated
  - Now queries runs per-experiment instead of all at once (LangSmith API only processes first session ID in list)
  - Added detailed logging for run migration progress
- **SSL Option**: Added `@ssl_option` decorator to all CLI commands for consistent SSL handling

### Changed
- **README.md**: Simplified from ~430 lines to ~170 lines for better readability
  - Kept full Limitations section for transparency
  - Condensed installation, configuration, and usage sections
- **Experiment Prompt**: CLI now prompts whether to include experiments when running `datasets` command

## [0.0.3] - 2025-01-15

### Fixed
- **Dataset Migration**: Fixed metadata migration with upsert support
- **Example Migration**: Improved example metadata handling

## [0.0.2] - 2025-12-08

### Added
- **Rules Migration**: Full support for v3+ evaluator rules
  - Automatic project and dataset ID mapping
  - Support for LLM evaluators with hub prompts
  - Support for code evaluators
  - `--create-enabled` flag to control rule state on creation
  - Rules created as disabled by default to bypass secrets validation
- **Prompt Migration**: Enhanced prompt migration with model configuration
  - Proper parent commit handling for existing prompts
  - `include_model=true` support for evaluator prompts
  - 409 conflict handling for up-to-date prompts
- **Enhanced Error Handling**:
  - Detailed error messages for evaluator validation failures
  - Better guidance for missing secrets/API keys
  - Improved 409 conflict resolution

### Changed
- Rules are now created as **disabled by default** to bypass API key/secrets validation
  - Use `--create-enabled` flag to create rules as enabled
  - Helpful post-migration message explaining how to enable rules
- Improved prompt commit handling with automatic parent commit detection
- Better verbose logging for debugging migration issues

### Fixed
- Fixed "RunnableSequence must have at least 2 steps" error for v3+ evaluators
- Fixed parent commit validation failures when updating existing prompts
- Fixed 409 errors being incorrectly reported as failures when prompts are up-to-date

## [0.0.1] - 2025-12-08

### Added
- Initial release of LangSmith Data Migration Tool
- Dataset migration with examples and attachments support
- Experiment migration alongside datasets
- Annotation queue migration with configurations
- Project rules migration with automatic ID mapping
- Prompt migration with version history support
- Chart and dashboard migration with automatic section creation
- Interactive CLI with user-friendly selection UI
- Connection testing functionality
- All-in-one migration wizard (`migrate-all` command)
- SSL certificate verification options for self-hosted instances
- Batch processing and concurrent workers for performance
- Dry-run mode for testing migrations
- Comprehensive error handling and retry logic
- Progress tracking and verbose output options
- Session management with resume capability

### Features
- **Dataset Migration**: Migrate datasets with examples and file attachments
- **Experiment Migration**: Transfer experiments and their runs
- **Annotation Queues**: Migrate queue configurations and settings
- **Project Rules**: Copy automation rules between instances with automatic project mapping
- **Prompts**: Migrate prompts and their version history
- **Charts**: Transfer monitoring charts and dashboards
- **Interactive TUI**: User-friendly command-line interface with search and selection
- **Automatic Mapping**: Smart ID mapping for projects, datasets, and resources

### Documentation
- Comprehensive README with installation instructions
- Usage examples for all commands
- Troubleshooting guide
- Configuration documentation
- API reference for core classes

[Unreleased]: https://github.com/langchain-ai/langsmith-data-migration-tool/compare/v0.0.51...HEAD
[0.0.51]: https://github.com/langchain-ai/langsmith-data-migration-tool/compare/v0.0.50...v0.0.51
[0.0.41]: https://github.com/langchain-ai/langsmith-data-migration-tool/compare/v0.0.4...v0.0.41
[0.0.4]: https://github.com/langchain-ai/langsmith-data-migration-tool/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/langchain-ai/langsmith-data-migration-tool/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/langchain-ai/langsmith-data-migration-tool/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/langchain-ai/langsmith-data-migration-tool/releases/tag/v0.0.1
