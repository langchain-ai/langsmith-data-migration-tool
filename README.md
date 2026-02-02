# LangSmith Data Migration Tool

A Python CLI for migrating datasets, experiments, annotation queues, project rules, prompts, and charts between LangSmith instances.

## Quick Start

```bash
# Install (requires uv: https://docs.astral.sh/uv/)
uv tool install "langsmith-data-migration-tool @ https://github.com/langchain-ai/langsmith-data-migration-tool/releases/latest/download/langsmith_data_migration_tool-0.0.41-py3-none-any.whl"

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

## Installation

**Prerequisites**: Python 3.12+, [uv](https://docs.astral.sh/uv/)

### Option 1: uv tool install (Recommended)
```bash
uv tool install "langsmith-data-migration-tool @ https://github.com/langchain-ai/langsmith-data-migration-tool/releases/latest/download/langsmith_data_migration_tool-0.0.41-py3-none-any.whl"
```

### Option 2: uvx (One-off execution, no install)
```bash
uvx --from "langsmith-data-migration-tool @ https://github.com/langchain-ai/langsmith-data-migration-tool/releases/latest/download/langsmith_data_migration_tool-0.0.41-py3-none-any.whl" langsmith-migrator test
```

### Option 3: pip
```bash
pip install "langsmith-data-migration-tool @ https://github.com/langchain-ai/langsmith-data-migration-tool/releases/latest/download/langsmith_data_migration_tool-0.0.41-py3-none-any.whl"
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

Or use a `.env` file:
```env
LANGSMITH_OLD_API_KEY=your_source_api_key
LANGSMITH_NEW_API_KEY=your_destination_api_key
LANGSMITH_VERIFY_SSL=true
```

## Usage

```bash
# Test connections
langsmith-migrator test

# Interactive wizard for all resources
langsmith-migrator migrate-all

# Datasets
langsmith-migrator datasets                    # Interactive selection
langsmith-migrator datasets --all              # All datasets
langsmith-migrator datasets --include-experiments  # With experiments

# Annotation queues
langsmith-migrator queues

# Prompts
langsmith-migrator prompts
langsmith-migrator prompts --all --include-all-commits

# Project rules
langsmith-migrator rules
langsmith-migrator rules --strip-projects      # As global rules

# Charts
langsmith-migrator charts
langsmith-migrator charts --session "project-name"

# Utilities
langsmith-migrator list-projects --source
langsmith-migrator resume
langsmith-migrator clean
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
langsmith-migrator --no-ssl datasets

# Or set environment variable
export LANGSMITH_VERIFY_SSL=false
```

## License

MIT License - see [LICENSE](LICENSE) for details.

## Contributing

Contributions welcome! Fork, create a feature branch, and submit a Pull Request.

## Support

For issues or questions: [GitHub repository](https://github.com/langchain-ai/langsmith-data-migration-tool)
