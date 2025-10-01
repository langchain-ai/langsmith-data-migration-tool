# LangSmith Data Migration Tool

A comprehensive Python tool for migrating datasets, experiments, annotation queues, project rules, and prompts between LangSmith instances.

## Features

- **Dataset Migration**: Migrate datasets with examples, attachments, and associated experiments
- **Attachment Support**: Automatically downloads and re-uploads file attachments (images, documents, etc.) with examples
- **Annotation Queue Migration**: Transfer annotation queues with their configurations
- **Project Rules Migration**: Copy tracing project rules between instances
- **Prompt Migration**: Migrate prompts and their versions
- **Interactive CLI**: User-friendly command-line interface with improved selection UX

## Limitations

### Trace Data Not Supported
This migration tool **does not support migrating trace data** between LangSmith instances. The tool is designed specifically for migrating:
- Datasets and their examples (including file attachments)
- Experiments and evaluators
- Annotation queues
- Project rules
- Prompts

For migrating trace data, please use LangSmith's official **Bulk Export** functionality, which allows you to export traces to external storage systems like S3, BigQuery, or Snowflake.

### Dataset Attachments
The tool automatically handles examples with file attachments:
- Downloads attachments from source using presigned URLs
- Preserves original filenames and MIME types
- Re-uploads attachments using the LangSmith SDK
- Supports all file types (images, PDFs, documents, etc.) 

📚 **Learn more about trace exports**: [LangSmith Bulk Export Documentation](https://docs.langchain.com/langsmith/data-export#bulk-exporting-trace-data)

## Installation

```bash
# Install with uv (recommended)
uv sync

# Activate the virtual environment
source .venv/bin/activate  # On Unix/macOS
# or
.venv\Scripts\activate  # On Windows
```

## Configuration

Set up environment variables for your source and destination LangSmith instances:

### Required Environment Variables

```bash
export LANGSMITH_OLD_API_KEY="your_source_api_key"
export LANGSMITH_NEW_API_KEY="your_destination_api_key"

# Base URLs (default: https://api.smith.langchain.com)
export LANGSMITH_OLD_BASE_URL="https://your-source-instance.com"
export LANGSMITH_NEW_BASE_URL="https://your-destination-instance.com"

```

### Using .env File

Alternatively, create a `.env` file in the project directory:

```env
LANGSMITH_OLD_API_KEY=your_source_api_key
LANGSMITH_NEW_API_KEY=your_destination_api_key
LANGSMITH_OLD_BASE_URL=https://your-source-instance.com
LANGSMITH_NEW_BASE_URL=https://your-destination-instance.com
LANGSMITH_VERIFY_SSL=true
```

## Usage

The tool is invoked using the `langsmith-migrator` command (or `python -m langsmith_migrator`):

```bash
# Test connections to both instances
langsmith-migrator test

# Interactive dataset selection
langsmith-migrator datasets

# Migrate all datasets (skip selection UI)
langsmith-migrator datasets --all

# Include related experiments
langsmith-migrator datasets --include-experiments

# Migrate annotation queues
langsmith-migrator queues

# Resume previous migration
langsmith-migrator resume

# Clean up old sessions
langsmith-migrator clean
```

### CLI Options

All commands support the following global options:

```bash
--source-key TEXT       Source API key (or set LANGSMITH_OLD_API_KEY)
--dest-key TEXT         Destination API key (or set LANGSMITH_NEW_API_KEY)
--source-url TEXT       Source base URL (or set LANGSMITH_OLD_BASE_URL)
--dest-url TEXT         Destination base URL (or set LANGSMITH_NEW_BASE_URL)
--no-ssl                Disable SSL certificate verification
--batch-size INTEGER    Batch size for operations (default: 100)
--workers INTEGER       Number of concurrent workers (default: 4)
--dry-run               Run without making changes
--verbose, -v           Enable verbose output
```

### SSL Certificate Issues

If you encounter SSL certificate verification errors with self-hosted instances:

```bash
# Option 1: Use the --no-ssl flag
langsmith-migrator --no-ssl datasets

# Option 2: Set the environment variable
export LANGSMITH_VERIFY_SSL=false
langsmith-migrator datasets
```

### Interactive Selection UI

When using `langsmith-migrator datasets` or `langsmith-migrator queues`, you'll see an interactive TUI with:

**Keyboard Shortcuts:**
- `↑↓` - Navigate through items
- `Space` - Toggle selection on current item
- `a` - Select all visible items
- `n` - Clear all selections
- `/` - Focus search box
- `Enter` - Confirm and proceed
- `Esc` - Cancel and exit

## API Classes
The tool is organized into several specialized classes:
- **`LangsmithMigrator`**: Main orchestrator class
- **`DatasetMigrator`**: Handles dataset and example migration
- **`ExperimentMigrator`**: Manages experiment migration
- **`AnnotationQueueMigrator`**: Handles annotation queue migration
- **`APIClient`**: Wrapper for HTTP requests with error handling

## Migration Modes

### Dataset Migration Modes

| Mode | Description |
|------|-------------|
| `EXAMPLES` | Migrate dataset metadata and all examples |
| `EXAMPLES_AND_EXPERIMENTS` | Migrate dataset, examples, experiments, and runs |
| `DATASET_ONLY` | Migrate only dataset metadata |

### Annotation Queue Migration Modes

| Mode | Description |
|------|-------------|
| `QUEUE_AND_DATASET` | Migrate queue and its associated default dataset |
| `QUEUE_ONLY` | Migrate only the queue configuration |

