# LangSmith Migration Tool - Interactive Credentials

## New Interactive Mode

When API keys are not set via environment variables, the tool now **interactively prompts** for them:

```bash
$ uv run migrate.py datasets

LangSmith Migration Tool v2.0

Source Instance Configuration
Source API Key: ******** (hidden input)
Source URL [https://api.smith.langchain.com]: (press Enter for default)

Destination Instance Configuration  
Destination API Key: ******** (hidden input)
Destination URL [https://api.smith.langchain.com]: (press Enter for default)

Testing connections... ✓
Fetching datasets... found 5
```

## Minimal, Clean UX

The entire interface has been simplified:

### Before:
```
╭───────────────────────────────────────────────────────────────╮
│ 🚀 LangSmith Migration Tool v2.0                              │
│ Migrate datasets, experiments, annotation queues, and prompts │
│ between LangSmith instances with improved UX                  │
╰───────────────────────────────────────────────────────────────╯

Configuration errors:
  • Source API key is required (LANGSMITH_OLD_API_KEY)
  • Destination API key is required (LANGSMITH_NEW_API_KEY)

Please set the required environment variables or provide CLI arguments
```

### After:
```
LangSmith Migration Tool v2.0

Source Instance Configuration
Source API Key: ******
Source URL [https://api.smith.langchain.com]: 

Destination Instance Configuration
Destination API Key: ******
Destination URL [https://api.smith.langchain.com]:

Testing connections... ✓
Fetching datasets... found 5
```

## Features

### 1. **Smart Credential Prompting**
- Only prompts when credentials are missing
- Secure password-style input for API keys
- Smart defaults for URLs
- Remembers CLI arguments if provided

### 2. **Minimal Output**
- Single-line status messages
- Clean checkmarks (✓) and crosses (✗)
- No excessive decoration or panels
- Compact command menus

### 3. **Streamlined Selection**
The dataset selector is now cleaner:

```
Select Datasets to Migrate (0/5 selected)
    #  Name                          ID                                    Description
─────────────────────────────────────────────────────────────────────────────────────
    1  Customer Support Dataset      d1234567-89ab-cdef-0123-456789abcdef  Support conversations
    2  Product Reviews              d2345678-9abc-def0-1234-56789abcdef0  Review sentiment analysis
    3  Email Classification         d3456789-abcd-ef01-2345-6789abcdef01  Email categorization

Commands: 1-20 (toggle) | a (all visible) | A (ALL) | n (none) | f (filter) | Enter (confirm) | q (cancel)

Command: _
```

## Usage Examples

### Interactive Mode (No Environment Variables)
```bash
# Tool will prompt for credentials
uv run migrate.py datasets

# Or test connections
uv run migrate.py test
```

### With Environment Variables (Non-Interactive)
```bash
export LANGSMITH_OLD_API_KEY="your-source-key"
export LANGSMITH_NEW_API_KEY="your-dest-key"

# No prompting needed
uv run migrate.py datasets
```

### Mixed Mode
```bash
# Set only source, prompt for destination
export LANGSMITH_OLD_API_KEY="your-source-key"
uv run migrate.py datasets
# Will only prompt for destination API key
```

### Custom URLs
When prompted, you can override the default URL:
```
Source URL [https://api.smith.langchain.com]: https://custom.langsmith.com
```

Or just press Enter to use the default.

## Command Line Options

All credentials can still be provided via CLI:
```bash
uv run migrate.py \
  --source-key "your-source-key" \
  --dest-key "your-dest-key" \
  --source-url "https://custom-source.com" \
  --dest-url "https://custom-dest.com" \
  datasets
```

## Benefits

1. **User-Friendly**: No need to set environment variables first
2. **Secure**: API keys are hidden during input
3. **Flexible**: Mix environment variables, CLI args, and prompts
4. **Clean**: Minimal, distraction-free interface
5. **Smart**: Only prompts for what's missing