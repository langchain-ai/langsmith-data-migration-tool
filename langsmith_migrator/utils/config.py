"""Configuration management using environment variables and CLI arguments."""

import os
from dataclasses import asdict, dataclass, field
from typing import Any, Dict, List, Optional
import urllib3
from urllib3.exceptions import InsecureRequestWarning
from rich.prompt import Prompt
from rich.console import Console
import getpass


@dataclass
class ConnectionConfig:
    """Configuration for a LangSmith connection."""
    api_key: str
    base_url: str = "https://api.smith.langchain.com"
    verify_ssl: bool = True
    timeout: int = 30
    max_retries: int = 3


@dataclass
class MigrationConfig:
    """Configuration for migration operations."""
    batch_size: int = 100
    concurrent_workers: int = 4
    dry_run: bool = False
    skip_existing: bool = False
    resume_on_error: bool = True
    verbose: bool = False
    interactive: bool = True
    non_interactive: bool = False

    # Performance settings
    stream_examples: bool = True  # Stream instead of loading all into memory
    chunk_size: int = 1000  # Process in chunks
    rate_limit_delay: float = 0.1  # Delay between API calls


def _env_int(name: str, default: int) -> int:
    """Env var or default, never an exception: a typo must not end a migration."""
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


@dataclass
class S3Config:
    """Transfer tuning for ``s3://`` archives.

    Every value is an environment variable as well as a CLI flag, because the
    right setting differs per environment - an in-region host wants large parts
    and high concurrency, a laptop on hotel wifi wants the opposite - and the
    measurements that picked these defaults cannot pick one for all of them. An
    env var is also what makes an operator's tuning reproducible across runs and
    shareable as a ``.env``, rather than living in one person's shell history.
    """

    part_bytes: int = 8 * 1024**2
    upload_concurrency: int = 4
    download_concurrency: int = 4
    download_chunk_bytes: int = 2 * 1024**2
    max_pool_connections: int = 64
    connect_timeout: int = 10
    read_timeout: int = 120
    max_attempts: int = 6
    retry_mode: str = "adaptive"
    endpoint_url: Optional[str] = None
    addressing_style: str = "auto"
    sse: Optional[str] = None
    sse_kms_key_id: Optional[str] = None
    storage_class: Optional[str] = None
    # Populated by validation; the CLI prints these rather than clamping silently.
    notes: List[str] = field(default_factory=list)

    # Env var names are mechanical: MIGRATION_S3_ + the field name upper-cased.
    # Kept as three tuples rather than fifteen call lines so a name cannot be
    # mistyped in only one of the two places it would otherwise appear.
    _INTS = (
        "part_bytes", "upload_concurrency", "download_concurrency",
        "download_chunk_bytes", "max_pool_connections", "connect_timeout",
        "read_timeout", "max_attempts",
    )
    _STRS = ("retry_mode", "addressing_style")
    _OPTS = ("endpoint_url", "sse", "sse_kms_key_id", "storage_class")

    @classmethod
    def from_env(cls, **overrides: Any) -> "S3Config":
        """CLI argument > environment variable > measured default."""
        cfg = cls()
        for name in cls._INTS:
            setattr(cfg, name, _env_int(cls._env(name), getattr(cfg, name)))
        for name in cls._STRS:
            setattr(cfg, name, os.getenv(cls._env(name), getattr(cfg, name)))
        for name in cls._OPTS:
            setattr(cfg, name, os.getenv(cls._env(name)) or None)
        for key, value in overrides.items():
            if value is not None:
                setattr(cfg, key, value)
        return cfg

    @staticmethod
    def _env(name: str) -> str:
        return f"MIGRATION_S3_{name.upper()}"

    def validate(self, max_window_bytes: int) -> "S3Config":
        """Clamp loudly and refuse the impossible.

        A silently clamped part size is what makes a later tuning result
        inexplicable, so every adjustment lands in ``notes`` for the caller to
        print. A part size too small to span the largest window we would accept
        is refused here rather than on part 10,001, hours in.
        """
        from ..core.trace_s3 import check_part_ceiling, clamp_part_bytes

        self.part_bytes, note = clamp_part_bytes(self.part_bytes)
        if note:
            self.notes.append(note)
        for name in ("upload_concurrency", "download_concurrency"):
            value = getattr(self, name)
            clamped = max(1, min(32, value))
            if clamped != value:
                setattr(self, name, clamped)
                self.notes.append(f"{name.replace('_', ' ')} clamped to {clamped}")
        if self.sse_kms_key_id and self.sse != "aws:kms":
            raise ValueError(
                "--s3-kms-key-id needs --s3-sse aws:kms; it would otherwise be ignored"
            )
        check_part_ceiling(self.part_bytes, max_window_bytes)
        return self

    def size_pool(self, in_flight: int) -> "S3Config":
        """Keep the connection pool ahead of the requests we intend to make.

        prefetch_windows x upload_concurrency requests can be outstanding at
        once; past the pool size botocore queues them, so raising concurrency
        without raising the pool makes an export *slower*. Mirrors what
        ``_size_connection_pools`` already does for the LangSmith clients.
        """
        needed = in_flight + 8
        if self.max_pool_connections < needed:
            self.notes.append(
                f"connection pool raised {self.max_pool_connections} -> {needed} "
                f"to cover {in_flight} concurrent requests"
            )
            self.max_pool_connections = needed
        return self

    def store_kwargs(self) -> Dict[str, Any]:
        return {k: v for k, v in asdict(self).items() if k != "notes"}


class Config:
    """Main configuration class that loads from environment variables."""

    def __init__(self,
                 source_api_key: Optional[str] = None,
                 dest_api_key: Optional[str] = None,
                 source_url: Optional[str] = None,
                 dest_url: Optional[str] = None,
                 verify_ssl: Optional[bool] = None,
                 batch_size: Optional[int] = None,
                 concurrent_workers: Optional[int] = None,
                 dry_run: bool = False,
                 skip_existing: Optional[bool] = None,
                 verbose: bool = False,
                 non_interactive: bool = False):
        """
        Initialize configuration from CLI args, falling back to environment variables.

        Args:
            source_api_key: Source instance API key (overrides env)
            dest_api_key: Destination instance API key (overrides env)
            source_url: Source instance URL (overrides env)
            dest_url: Destination instance URL (overrides env)
            verify_ssl: Whether to verify SSL (overrides env)
            batch_size: Batch size for operations (overrides env)
            concurrent_workers: Number of concurrent workers (overrides env)
            dry_run: Whether to run in dry-run mode
            skip_existing: If True, skip existing resources; if False, update them (overrides env)
            verbose: Whether to enable verbose logging
            non_interactive: Disable guided remediation prompts
        """
        # Determine SSL verification setting
        # Priority: CLI arg > env var > default (True)
        ssl_verify = verify_ssl if verify_ssl is not None else (os.getenv('LANGSMITH_VERIFY_SSL', 'true').lower() != 'false')

        # Source connection
        self.source = ConnectionConfig(
            api_key=source_api_key or os.getenv('LANGSMITH_OLD_API_KEY', ''),
            base_url=source_url or os.getenv('LANGSMITH_OLD_BASE_URL', 'https://api.smith.langchain.com'),
            verify_ssl=ssl_verify
        )

        # Destination connection
        self.destination = ConnectionConfig(
            api_key=dest_api_key or os.getenv('LANGSMITH_NEW_API_KEY', ''),
            base_url=dest_url or os.getenv('LANGSMITH_NEW_BASE_URL', 'https://api.smith.langchain.com'),
            verify_ssl=ssl_verify
        )

        # Migration settings
        # Priority: CLI arg > env var > default (False, meaning update by default)
        should_skip = skip_existing if skip_existing is not None else (os.getenv('MIGRATION_SKIP_EXISTING', 'false').lower() == 'true')

        # Parse migration settings with safe defaults
        try:
            parsed_batch_size = batch_size or int(os.getenv('MIGRATION_BATCH_SIZE', '100'))
        except ValueError:
            parsed_batch_size = 100

        try:
            parsed_workers = concurrent_workers or int(os.getenv('MIGRATION_WORKERS', '4'))
        except ValueError:
            parsed_workers = 4

        try:
            parsed_chunk_size = int(os.getenv('MIGRATION_CHUNK_SIZE', '1000'))
        except ValueError:
            parsed_chunk_size = 1000

        try:
            parsed_rate_limit = float(os.getenv('MIGRATION_RATE_LIMIT_DELAY', '0.1'))
        except ValueError:
            parsed_rate_limit = 0.1

        self.migration = MigrationConfig(
            batch_size=parsed_batch_size,
            concurrent_workers=parsed_workers,
            dry_run=dry_run or os.getenv('MIGRATION_DRY_RUN', 'false').lower() == 'true',
            verbose=verbose or os.getenv('MIGRATION_VERBOSE', 'false').lower() == 'true',
            skip_existing=should_skip,
            interactive=not non_interactive,
            non_interactive=non_interactive,
            stream_examples=os.getenv('MIGRATION_STREAM_EXAMPLES', 'true').lower() != 'false',
            chunk_size=parsed_chunk_size,
            rate_limit_delay=parsed_rate_limit
        )
        self.state_manager = None

        # Disable SSL warnings if needed
        if not self.source.verify_ssl or not self.destination.verify_ssl:
            urllib3.disable_warnings(InsecureRequestWarning)

    def _validate_url(self, url: str) -> tuple[bool, str]:
        """
        Validate that a URL has proper format.

        Returns:
            Tuple of (is_valid, error_message)
        """
        if not url:
            return False, "URL is empty"

        # Must have a valid scheme
        if not url.startswith('http://') and not url.startswith('https://'):
            return False, f"URL must start with http:// or https:// (got: {url})"

        # Basic structure check
        try:
            from urllib.parse import urlparse
            parsed = urlparse(url)
            if not parsed.netloc:
                return False, f"URL missing hostname (got: {url})"
        except Exception as e:
            return False, f"Invalid URL format: {e}"

        return True, ""

    def validate(self) -> tuple[bool, list[str]]:
        """
        Validate the configuration.

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []

        if not self.source.api_key:
            errors.append("Source API key is required (LANGSMITH_OLD_API_KEY)")

        if not self.destination.api_key:
            errors.append("Destination API key is required (LANGSMITH_NEW_API_KEY)")

        if not self.source.base_url:
            errors.append("Source base URL is required")
        else:
            valid, error = self._validate_url(self.source.base_url)
            if not valid:
                errors.append(f"Invalid source URL: {error}")

        if not self.destination.base_url:
            errors.append("Destination base URL is required")
        else:
            valid, error = self._validate_url(self.destination.base_url)
            if not valid:
                errors.append(f"Invalid destination URL: {error}")

        if self.migration.batch_size <= 0:
            errors.append("Batch size must be positive")

        if self.migration.batch_size > 1000:
            errors.append("Batch size should not exceed 1000 for optimal performance")

        if self.migration.concurrent_workers <= 0:
            errors.append("Concurrent workers must be positive")

        if self.migration.concurrent_workers > 10:
            errors.append("Concurrent workers should not exceed 10 to avoid rate limiting")

        return len(errors) == 0, errors

    def prompt_for_credentials(self, console: Optional[Console] = None) -> None:
        """
        Interactively prompt for missing credentials.

        Args:
            console: Rich console for output
        """
        if console is None:
            console = Console()

        # Check if we need to prompt for source credentials
        if not self.source.api_key:
            console.print("\n[cyan]Source Instance Configuration[/cyan]")
            self.source.api_key = getpass.getpass("Source API Key: ")

            # Only prompt for URL if not already set
            if self.source.base_url == "https://api.smith.langchain.com":
                new_url = Prompt.ask(
                    "Source URL",
                    default="https://api.smith.langchain.com"
                )
                if new_url:
                    self.source.base_url = new_url

        # Check if we need to prompt for destination credentials
        if not self.destination.api_key:
            console.print("\n[cyan]Destination Instance Configuration[/cyan]")
            self.destination.api_key = getpass.getpass("Destination API Key: ")

            # Only prompt for URL if not already set
            if self.destination.base_url == "https://api.smith.langchain.com":
                new_url = Prompt.ask(
                    "Destination URL",
                    default="https://api.smith.langchain.com"
                )
                if new_url:
                    self.destination.base_url = new_url

        console.print()  # Add blank line after prompting

    def display_summary(self, console) -> None:
        """Display minimal configuration summary."""
        if self.migration.verbose:
            # Only show detailed config in verbose mode
            console.print("[dim]Configuration:[/dim]")
            console.print(f"  Source: {self.source.base_url}")
            console.print(f"  Destination: {self.destination.base_url}")
            console.print(f"  Mode: {'Dry Run' if self.migration.dry_run else 'Live'}")
            console.print()
