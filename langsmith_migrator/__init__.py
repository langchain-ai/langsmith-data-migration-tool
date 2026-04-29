"""LangSmith Migration Tool - A robust tool for migrating data between LangSmith instances."""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("langsmith-data-migration-tool")
except PackageNotFoundError:
    __version__ = "0.0.68"
