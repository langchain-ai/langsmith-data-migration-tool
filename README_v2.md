# LangSmith Migration Tool v2.0

## Improvements Over Original

### 🎯 Better Selection UX
- **Interactive selector with "Select All" option** - No more selecting items one by one
- **Search/filter functionality** - Quickly find resources by name or ID  
- **Keyboard shortcuts** - Number keys (1-9) for quick selection
- **Visual feedback** - Clear indication of selected items
- **Pagination** - Handle large lists efficiently

### 🏗️ Improved Architecture
- **Modular design** - Separated UI, business logic, and API layers
- **Streaming & batching** - Memory-efficient processing of large datasets
- **Parallel processing** - Concurrent migrations with configurable workers
- **Retry logic** - Automatic retries with exponential backoff
- **Better error handling** - Detailed error messages and recovery options

### 💾 State Management
- **Resume capability** - Continue failed migrations from where they left off
- **Progress persistence** - Track what's been migrated
- **ID mappings** - Maintain source-to-destination mappings

### 📊 Enhanced Progress Tracking
- **Multi-level progress** - Overall and per-stage progress
- **Real-time updates** - See what's being processed
- **Statistics** - Success/failure counts and rates
- **ETA calculations** - Estimated completion time

## Installation

```bash
pip install -r requirements.txt
```

## Configuration

Set environment variables:

```bash
# Required
export LANGSMITH_OLD_API_KEY="your-source-api-key"
export LANGSMITH_NEW_API_KEY="your-destination-api-key"

# Optional
export LANGSMITH_OLD_BASE_URL="https://api.smith.langchain.com"
export LANGSMITH_NEW_BASE_URL="https://api.smith.langchain.com"
export LANGSMITH_VERIFY_SSL="true"

# Performance tuning
export MIGRATION_BATCH_SIZE="100"
export MIGRATION_WORKERS="4"
export MIGRATION_STREAM_EXAMPLES="true"
export MIGRATION_RATE_LIMIT_DELAY="0.1"
```

## Usage

### Test Connections
```bash
python migrate.py test
```

### Migrate Datasets

Interactive selection with improved UI:
```bash
python migrate.py datasets
```

Migrate all datasets:
```bash
python migrate.py datasets --all
```

Include experiments:
```bash
python migrate.py datasets --include-experiments
```

### Dry Run Mode
Preview what will be migrated without making changes:
```bash
python migrate.py --dry-run datasets
```

### Resume Failed Migration
```bash
python migrate.py resume
```

### Other Resources
```bash
python migrate.py queues    # Migrate annotation queues
python migrate.py prompts   # Migrate prompts
```

### Clean Up Sessions
```bash
python migrate.py clean     # Remove old migration sessions
```

## CLI Options

Global options:
- `--source-key`: Override source API key
- `--dest-key`: Override destination API key
- `--source-url`: Override source URL
- `--dest-url`: Override destination URL
- `--no-ssl`: Disable SSL verification
- `--batch-size`: Set batch size
- `--workers`: Number of concurrent workers
- `--dry-run`: Preview mode
- `--verbose`: Enable verbose logging

## Key Improvements in Action

### 1. Better Selection Interface
Instead of the confusing Space/Enter selection:
- Use arrow keys or j/k to navigate
- Press Space to toggle selection
- Press 'a' to select all visible items
- Press 'A' to select ALL items (including those not visible)
- Press '/' to search/filter
- Press 1-9 to quickly toggle items
- Press Enter to confirm

### 2. Memory-Efficient Streaming
Large datasets are now processed in chunks rather than loading everything into memory:
- Examples are streamed from source
- Processed in configurable batches
- Written to destination incrementally

### 3. Parallel Processing
Multiple datasets can be migrated concurrently:
- Configurable number of workers
- Automatic rate limiting
- Progress tracking across all workers

### 4. Resume Capability
If migration fails:
- State is automatically saved
- Run `migrate.py resume` to see available sessions
- Select session to continue from where it stopped
- Failed items are retried automatically

### 5. Better Error Handling
- Specific error types (RateLimitError, NotFoundError, etc.)
- Automatic retries for transient failures
- Detailed error messages with context
- Option to continue on errors

## Architecture

```
langsmith_migrator/
├── cli/
│   ├── interactive_selector.py  # Enhanced selection UI
│   └── main.py                  # CLI commands
├── core/
│   ├── api_client.py            # Enhanced API client with retry
│   └── migrators.py             # Migration logic
├── utils/
│   ├── config.py                # Configuration management
│   └── state.py                 # State persistence
└── __init__.py
```

## Performance Considerations

1. **Streaming**: Large datasets are processed in chunks to avoid memory issues
2. **Batching**: API calls are batched to reduce overhead
3. **Concurrency**: Multiple items processed in parallel
4. **Rate Limiting**: Configurable delays to avoid hitting API limits
5. **Connection Pooling**: Reuses HTTP connections for better performance

## Error Recovery

The tool handles various failure scenarios:
- **Network errors**: Automatic retry with backoff
- **Rate limits**: Longer delays and retries
- **Partial failures**: Continue with next items
- **Server errors**: Automatic retry
- **Invalid data**: Skip and log errors

## Comparison with Original

| Feature | Original | v2.0 |
|---------|----------|------|
| Selection UI | Space/Enter only | Full keyboard navigation + Select All |
| Memory Usage | Loads all data | Streaming chunks |
| Error Handling | Basic exceptions | Typed errors with retry |
| Resume Capability | None | Full state persistence |
| Progress Display | Simple spinner | Multi-level with ETA |
| Architecture | Monolithic 955-line file | Modular components |
| Concurrency | Sequential only | Parallel with workers |
| Configuration | ENV vars only | ENV vars + CLI args |
| Batch Processing | Fixed batches | Configurable streaming |