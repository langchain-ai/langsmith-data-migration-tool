# LangSmith Data Migration Tool

A comprehensive Python tool for migrating datasets, experiments, annotation queues, project rules, and prompts between LangSmith instances.

## Features

- **Dataset Migration**: Migrate datasets with examples and associated experiments
- **Annotation Queue Migration**: Transfer annotation queues with their configurations
- **Project Rules Migration**: Copy tracing project rules between instances
- **Prompt Migration**: Migrate prompts and their versions
- **Interactive CLI**: User-friendly command-line interface with progress bars

## Limitations

### Trace Data Not Supported
This migration tool **does not support migrating trace data** between LangSmith instances. The tool is designed specifically for migrating:
- Datasets and their examples
- Experiments and evaluation runs
- Annotation queues
- Project rules
- Prompts

For migrating trace data, please use LangSmith's official **Bulk Export** functionality, which allows you to export traces to external storage systems like S3, BigQuery, or Snowflake. 

📚 **Learn more about trace exports**: [LangSmith Bulk Export Documentation](https://docs.langchain.com/langsmith/data-export#bulk-exporting-trace-data)

## Installation
1. Clone or download this repository
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

Set up environment variables for your source and destination LangSmith instances:

### Required Environment Variables

```bash
export LANGSMITH_OLD_API_KEY="your_source_api_key"
export LANGSMITH_NEW_API_KEY="your_destination_api_key"
```

### Optional Environment Variables

```bash
# Base URLs (default: https://api.smith.langchain.com)
export LANGSMITH_OLD_BASE_URL="https://your-source-instance.com"
export LANGSMITH_NEW_BASE_URL="https://your-destination-instance.com"

# SSL verification (default: true)
export LANGSMITH_VERIFY_SSL="false"  # Set to false to disable SSL verification
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

Run the migration tool:

```bash
python migration.py
```

The tool provides an interactive menu with the following options:

### 1. Dataset Migration

Migrate datasets with three modes:
- **Examples only**: Migrate dataset metadata and examples
- **Examples and experiments**: Migrate dataset, examples, and associated experiments with runs
- **Dataset metadata only**: Migrate only the dataset structure (no examples or experiments)

### 2. Annotation Queue Migration

Migrate annotation queues with two modes:
- **Queue and associated dataset**: Migrate the queue and its default dataset
- **Queue only**: Migrate only the queue configuration

### 3. Project Rules Migration

Migrate tracing project rules between projects. Requires:
- Source project ID
- Destination project ID

The tool will automatically migrate any datasets or annotation queues referenced by the rules.

### 4. Prompt Migration

Migrate prompts and their versions between instances.

## API Classes
The tool is organized into several specialized classes:
- **`LangsmithMigrator`**: Main orchestrator class
- **`DatasetMigrator`**: Handles dataset and example migration
- **`ExperimentMigrator`**: Manages experiment and run migration
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

