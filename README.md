# LangSmith Data Migration Tool

A comprehensive Python tool for migrating datasets, experiments, annotation queues, project rules, and prompts between LangSmith instances.

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

# Start migrating datasets
uv run langsmith-migrator datasets
```

## Features

- **All-in-One Migration Wizard**: Interactive wizard that walks you through migrating all resources (`migrate-all`)
- **Dataset Migration**: Migrate datasets with examples and attachments
- **Experiment Migration**: Migrate experiments and their runs alongside datasets (use `--include-experiments`)
- **Attachment Support**: Automatically downloads and re-uploads file attachments (images, documents, etc.) with examples
- **Annotation Queue Migration**: Transfer annotation queues with their configurations
- **Project Rules Migration**: Copy tracing project rules between instances (with `--strip-projects` to handle project mapping)
- **Prompt Migration**: Migrate prompts and their versions with detailed progress tracking
- **Chart Migration**: Migrate monitoring charts and dashboards with automatic creation of missing sections and filter preservation
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

📚 **Learn more about trace exports**: [LangSmith Bulk Export Documentation](https://docs.langchain.com/langsmith/data-export#bulk-exporting-trace-data)

### Prompts and Rules Feature Availability
**Prompts** and **Project Rules** migration require that these features are enabled on both source and destination LangSmith instances:

- **Prompts**: If you encounter `405 Not Allowed` errors when migrating prompts, the destination instance may not have the prompts feature enabled. This can happen with:
  - Self-hosted instances that haven't enabled the prompts feature
  - Older versions of LangSmith
  - Instances with restrictive nginx/proxy configurations
  
  **Solution**: Contact your LangSmith administrator to enable the prompts feature or configure the `/api/v1/repos/*` endpoints.

- **Project Rules**: Rules (automation rules) may not be available on all LangSmith instances. The tool will gracefully handle missing endpoints and provide informative messages.

## Installation

### Prerequisites
- Python 3.12 or higher
- [uv](https://docs.astral.sh/uv/) package manager (recommended) or pip

### Option 1: Install from GitHub Release (Recommended)

Download and install the latest release directly:

```bash
# Install the latest release wheel file
pip install https://github.com/langchain-ai/langsmith-data-migration-tool/releases/latest/download/langsmith_data_migration_tool-0.0.1-py3-none-any.whl

# Or download manually from the releases page and install
pip install langsmith_data_migration_tool-0.0.1-py3-none-any.whl

# Run the tool
langsmith-migrator --help
```

### Option 2: Install from Source with uv

```bash
# Clone the repository
git clone https://github.com/langchain-ai/langsmith-data-migration-tool.git
cd langsmith-data-migration-tool

# Install dependencies and the package
uv sync

# Run the tool using uv
uv run langsmith-migrator --help
```

### Option 3: Install from Source with pip

```bash
# Clone the repository
git clone https://github.com/langchain-ai/langsmith-data-migration-tool.git
cd langsmith-data-migration-tool

# Create a virtual environment
python -m venv .venv

# Activate the virtual environment
source .venv/bin/activate  # On Unix/macOS
# or
.venv\Scripts\activate  # On Windows

# Install the package in editable mode
pip install -e .

# Run the tool
langsmith-migrator --help
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

### Running the Tool

If installed with uv, use:
```bash
uv run langsmith-migrator [COMMAND] [OPTIONS]
```

If installed with pip and activated in virtual environment, use:
```bash
langsmith-migrator [COMMAND] [OPTIONS]
```

You can also run it as a Python module:
```bash
python -m langsmith_migrator [COMMAND] [OPTIONS]
```

### Available Commands

```bash
# Test connections to both instances
langsmith-migrator test

# Migrate all resources with interactive wizard
langsmith-migrator migrate-all

# Interactive dataset selection
langsmith-migrator datasets

# Migrate all datasets (skip selection UI)
langsmith-migrator datasets --all

# Include related experiments with datasets
langsmith-migrator datasets --include-experiments

# Migrate all datasets with experiments
langsmith-migrator datasets --all --include-experiments

# Migrate annotation queues
langsmith-migrator queues

# Migrate prompts
langsmith-migrator prompts

# Migrate all prompts with full commit history
langsmith-migrator prompts --all --include-all-commits

# Migrate project rules (automation rules)
langsmith-migrator rules

# Migrate monitoring charts from all projects
langsmith-migrator charts

# Migrate charts for a specific project/session
langsmith-migrator charts --session "my-project-name"
langsmith-migrator charts --session "project-uuid"

# If source and destination are the same instance (auto-detected)
# Use --same-instance to force using same session IDs
langsmith-migrator charts --same-instance

# List projects to help create ID mappings
langsmith-migrator list-projects --source
langsmith-migrator list-projects --dest

# Migrate rules with custom project ID mapping
langsmith-migrator rules --project-mapping '{"source-proj-id": "dest-proj-id"}'

# Or use a mapping file
langsmith-migrator rules --project-mapping ./project-mapping.json

# Migrate everything interactively with wizard
langsmith-migrator migrate-all

# Skip specific resource types in wizard
langsmith-migrator migrate-all --skip-prompts --skip-rules

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

When using `langsmith-migrator datasets`:
- **Default**: Migrate dataset metadata and all examples
- **With `--include-experiments`**: Migrate dataset, examples, experiments, and their runs

### Rules Migration Modes

When using `langsmith-migrator rules`:
- **Default**: Migrate all rules, automatically creating any missing projects in the destination
- **With `--strip-projects`**: Convert project-specific rules to global rules (removes project associations)

**Automatic Project Creation**: When migrating project-specific rules, the tool automatically:
1. Matches projects between source and destination by name
2. Creates any missing projects in the destination with the same name and metadata
3. Maps rule references to the correct project IDs

This ensures project-specific rules migrate successfully without manual project setup. Projects are only created if they're referenced by a rule being migrated.

### Annotation Queue Migration Modes

| Mode | Description |
|------|-------------|
| `QUEUE_AND_DATASET` | Migrate queue and its associated default dataset |
| `QUEUE_ONLY` | Migrate only the queue configuration |

### Chart & Dashboard Migration

The `charts` command (`langsmith-migrator charts`) handles the migration of monitoring charts and their organization.

- **Automatic Dashboard Creation**: The tool automatically detects dashboard sections in the source project and creates them in the destination if they don't exist, maintaining the same structure.
- **Common Filter Support**: Session (Project) filters are correctly preserved. When migrating to a different instance or project, project ID filters are updated to match the new destination project IDs.
- **Same Instance Detection**: The tool smartly detects if you are migrating within the same workspace by checking both the API Key and Base URL.
  - If identical: It preserves exact references.
  - If different: It maps references to new IDs.

## Troubleshooting

### Command Not Found

If you get `command not found: langsmith-migrator`:

**Using uv:**
```bash
# Always prefix with uv run
uv run langsmith-migrator --help
```

**Using pip:**
```bash
# Make sure virtual environment is activated
source .venv/bin/activate  # Unix/macOS
.venv\Scripts\activate     # Windows

# Or run as module
python -m langsmith_migrator --help
```

### Module Not Found Error

If you see `ModuleNotFoundError: No module named 'langsmith_migrator.__main__'`:

```bash
# Reinstall the package
uv sync
# or
pip install -e .
```

### Missing Dependencies

If you encounter import errors:

```bash
# Using uv - sync dependencies
uv sync

# Using pip - install from requirements
pip install -r requirements.txt
```

### SSL Certificate Errors

For self-hosted instances with SSL issues:

```bash
# Option 1: Use --no-ssl flag
uv run langsmith-migrator --no-ssl test

# Option 2: Set environment variable
export LANGSMITH_VERIFY_SSL=false
uv run langsmith-migrator test
```

### Connection Issues

If connection tests fail:

1. Verify API keys are correct
2. Check base URLs (should include `https://`)
3. Ensure network connectivity to both instances
4. Try verbose mode for more details: `uv run langsmith-migrator -v test`

### Rules Migration Issues

If rules fail to migrate:

1. **Check project/dataset associations:**
   - Rules require either a `session_id` (project) or `dataset_id` (dataset)
   - The tool automatically maps IDs by matching project/dataset names
   - Missing projects are created automatically in the destination
   - If a dataset doesn't exist in the destination, migrate datasets first

2. **For dataset-specific rules, migrate datasets first:**
   ```bash
   # First migrate datasets if rules reference them
   langsmith-migrator datasets --all

   # Then migrate rules (projects created automatically)
   langsmith-migrator rules
   ```

3. **Possible causes for skipped rules:**
   - Dataset names don't match between source and destination (for dataset-specific rules)
   - Referenced datasets haven't been migrated yet
   - No rules have been created in the source instance
   - Project creation failed due to API errors or permissions

## Development

### Setting up Development Environment

```bash
# Clone the repository
git clone https://github.com/langchain-ai/langsmith-data-migration-tool.git
cd langsmith-data-migration-tool

# Install dependencies using uv
uv sync

# Run tests
uv run pytest

# Run tests with coverage
uv run pytest --cov=langsmith_migrator --cov-report=term-missing
```

### Running from Source

```bash
# Run the tool directly from source
uv run langsmith-migrator --help

# Or use as a Python module
uv run python -m langsmith_migrator --help
```

### Building the Package

```bash
# Build distribution packages (wheel and source)
uv build

# The built packages will be in the dist/ directory
ls -lh dist/
```

## Releasing

### Creating a New Release

This project uses automated GitHub releases. To create a new release:

1. **Update the version** in `pyproject.toml`:
   ```toml
   version = "0.0.2"  # Update to your new version
   ```

2. **Update CHANGELOG.md**:
   - Move items from `[Unreleased]` to a new version section
   - Add the release date
   - Create a new empty `[Unreleased]` section

3. **Commit the changes**:
   ```bash
   git add pyproject.toml CHANGELOG.md
   git commit -m "Prepare release v0.0.2"
   git push origin main
   ```

4. **Create and push a git tag**:
   ```bash
   git tag -a v0.0.2 -m "Release version 0.0.2"
   git push origin v0.0.2
   ```

5. **Automated workflow**: The GitHub Actions workflow will automatically:
   - Build the package (wheel and source distribution)
   - Create a GitHub release with the changelog notes
   - Upload distribution files for easy download

### Installing from GitHub Release

Users can install directly from a GitHub release:

```bash
# Download the wheel file from the release page, then:
pip install langsmith_data_migration_tool-0.0.1-py3-none-any.whl

# Or install directly from the release URL:
pip install https://github.com/langchain-ai/langsmith-data-migration-tool/releases/download/v0.0.1/langsmith_data_migration_tool-0.0.1-py3-none-any.whl
```

### Release Checklist

Before creating a release, ensure:

- [ ] All tests pass (`uv run pytest`)
- [ ] Version number is updated in `pyproject.toml`
- [ ] CHANGELOG.md is updated with all changes
- [ ] README.md is up to date with new features
- [ ] All changes are committed to main branch
- [ ] Git tag matches the version in `pyproject.toml`

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## Support

For issues, questions, or contributions, please visit the [GitHub repository](https://github.com/langchain-ai/langsmith-data-migration-tool).

