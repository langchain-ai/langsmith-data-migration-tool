# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/langchain-ai/langsmith-data-migration-tool/compare/v0.0.41...HEAD
[0.0.41]: https://github.com/langchain-ai/langsmith-data-migration-tool/compare/v0.0.4...v0.0.41
[0.0.4]: https://github.com/langchain-ai/langsmith-data-migration-tool/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/langchain-ai/langsmith-data-migration-tool/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/langchain-ai/langsmith-data-migration-tool/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/langchain-ai/langsmith-data-migration-tool/releases/tag/v0.0.1
