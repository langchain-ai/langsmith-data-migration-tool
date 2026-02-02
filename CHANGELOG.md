# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/langchain-ai/langsmith-data-migration-tool/compare/v0.0.4...HEAD
[0.0.4]: https://github.com/langchain-ai/langsmith-data-migration-tool/compare/v0.0.3...v0.0.4
[0.0.3]: https://github.com/langchain-ai/langsmith-data-migration-tool/compare/v0.0.2...v0.0.3
[0.0.2]: https://github.com/langchain-ai/langsmith-data-migration-tool/compare/v0.0.1...v0.0.2
[0.0.1]: https://github.com/langchain-ai/langsmith-data-migration-tool/releases/tag/v0.0.1
