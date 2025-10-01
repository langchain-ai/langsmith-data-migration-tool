"""Main entry point for the LangSmith migration tool CLI."""

import sys
from langsmith_migrator.cli.main import cli

def main():
    """Main entry point."""
    sys.exit(cli())

if __name__ == "__main__":
    main()
