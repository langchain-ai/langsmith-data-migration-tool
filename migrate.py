#!/usr/bin/env python
"""Entry point for the LangSmith Migration Tool."""

from dotenv import load_dotenv
from langsmith_migrator.cli.main import main

if __name__ == "__main__":
    load_dotenv()
    main()