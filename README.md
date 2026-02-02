# LangSmith Data Migration Tool

A Python CLI for migrating datasets, experiments, annotation queues, project rules, prompts, and charts between LangSmith instances.

## Quick Start

```bash
# Clone and install
git clone https://github.com/langchain-ai/langsmith-data-migration-tool.git
cd langsmith-data-migration-tool
uv sync

# Set up environment variables
export LANGSMITH_OLD_API_KEY="your_source_api_key"
export LANGSMITH_NEW_API_KEY="your_destination_api_key"

# Test connections
uv run langsmith-migrator test

# Start migrating
uv run langsmith-migrator datasets
```

## Features

- **All-in-One Wizard**: Interactive migration of all resources (`migrate-all`)
- **Datasets**: Migrate datasets with examples and file attachments
- **Experiments**: Migrate experiments with runs and feedback (`--include-experiments`)
- **Annotation Queues**: Transfer queue configurations
- **Project Rules**: Copy automation rules with automatic project creation
- **Prompts**: Migrate prompts with version history
- **Charts**: Migrate monitoring charts with filter preservation
- **Interactive CLI**: TUI-based selection with search/filter

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

### Prompts and Rules Feature Availability

**Prompts** and **Project Rules** require these features to be enabled on both instances:

- **Prompts**: `405 Not Allowed` errors indicate the prompts feature isn't enabled. Contact your administrator to enable `/api/v1/repos/*` endpoints.
- **Project Rules**: May not be available on all instances. The tool handles missing endpoints gracefully.

## Installation

**Prerequisites**: Python 3.12+, [uv](https://docs.astral.sh/uv/) (recommended)

```bash
git clone https://github.com/langchain-ai/langsmith-data-migration-tool.git
cd langsmith-data-migration-tool
uv sync
```

Alternative with pip:
```bash
pip install https://github.com/langchain-ai/langsmith-data-migration-tool/releases/latest/download/langsmith_data_migration_tool-0.0.1-py3-none-any.whl
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

Or use a `.env` file:
```env
LANGSMITH_OLD_API_KEY=your_source_api_key
LANGSMITH_NEW_API_KEY=your_destination_api_key
LANGSMITH_VERIFY_SSL=true
```

## Usage

```bash
# Test connections
uv run langsmith-migrator test

# Interactive wizard for all resources
uv run langsmith-migrator migrate-all

# Datasets
uv run langsmith-migrator datasets                    # Interactive selection
uv run langsmith-migrator datasets --all              # All datasets
uv run langsmith-migrator datasets --include-experiments  # With experiments

# Annotation queues
uv run langsmith-migrator queues

# Prompts
uv run langsmith-migrator prompts
uv run langsmith-migrator prompts --all --include-all-commits

# Project rules
uv run langsmith-migrator rules
uv run langsmith-migrator rules --strip-projects      # As global rules

# Charts
uv run langsmith-migrator charts
uv run langsmith-migrator charts --session "project-name"

# Utilities
uv run langsmith-migrator list-projects --source
uv run langsmith-migrator resume
uv run langsmith-migrator clean
```

### CLI Options

```bash
--source-key TEXT       Source API key
--dest-key TEXT         Destination API key
--source-url TEXT       Source base URL
--dest-url TEXT         Destination base URL
--no-ssl                Disable SSL verification
--batch-size INTEGER    Batch size (default: 100)
--workers INTEGER       Concurrent workers (default: 4)
--dry-run               Run without making changes
--verbose, -v           Verbose output
```

### Interactive Selection

Keyboard shortcuts in TUI:
- `↑↓` Navigate | `Space` Toggle | `a` Select all | `n` Clear
- `/` Search | `Enter` Confirm | `Esc` Cancel

## SSL Certificate Issues

For self-hosted instances with SSL errors:

```bash
# Use --no-ssl flag
uv run langsmith-migrator --no-ssl datasets

# Or set environment variable
export LANGSMITH_VERIFY_SSL=false
```

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions welcome! Fork, create a feature branch, and submit a Pull Request.

## Support

For issues or questions: [GitHub repository](https://github.com/langchain-ai/langsmith-data-migration-tool)
