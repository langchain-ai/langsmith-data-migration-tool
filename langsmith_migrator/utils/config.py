"""Configuration management using environment variables and CLI arguments."""

import os
from dataclasses import dataclass
from typing import Optional
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
    skip_existing: bool = True
    resume_on_error: bool = True
    verbose: bool = False
    
    # Performance settings
    stream_examples: bool = True  # Stream instead of loading all into memory
    chunk_size: int = 1000  # Process in chunks
    rate_limit_delay: float = 0.1  # Delay between API calls
    
    # Selection of what to migrate
    include_experiments: bool = True
    include_annotations: bool = True
    include_rules: bool = False


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
                 verbose: bool = False):
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
            verbose: Whether to enable verbose logging
        """
        # Source connection
        self.source = ConnectionConfig(
            api_key=source_api_key or os.getenv('LANGSMITH_OLD_API_KEY', ''),
            base_url=source_url or os.getenv('LANGSMITH_OLD_BASE_URL', 'https://api.smith.langchain.com'),
            verify_ssl=verify_ssl if verify_ssl is not None else os.getenv('LANGSMITH_VERIFY_SSL', 'true').lower() != 'false'
        )
        
        # Destination connection
        self.destination = ConnectionConfig(
            api_key=dest_api_key or os.getenv('LANGSMITH_NEW_API_KEY', ''),
            base_url=dest_url or os.getenv('LANGSMITH_NEW_BASE_URL', 'https://api.smith.langchain.com'),
            verify_ssl=verify_ssl if verify_ssl is not None else os.getenv('LANGSMITH_VERIFY_SSL', 'true').lower() != 'false'
        )
        
        # Migration settings
        self.migration = MigrationConfig(
            batch_size=batch_size or int(os.getenv('MIGRATION_BATCH_SIZE', '100')),
            concurrent_workers=concurrent_workers or int(os.getenv('MIGRATION_WORKERS', '4')),
            dry_run=dry_run or os.getenv('MIGRATION_DRY_RUN', 'false').lower() == 'true',
            verbose=verbose or os.getenv('MIGRATION_VERBOSE', 'false').lower() == 'true',
            skip_existing=os.getenv('MIGRATION_SKIP_EXISTING', 'true').lower() != 'false',
            stream_examples=os.getenv('MIGRATION_STREAM_EXAMPLES', 'true').lower() != 'false',
            chunk_size=int(os.getenv('MIGRATION_CHUNK_SIZE', '1000')),
            rate_limit_delay=float(os.getenv('MIGRATION_RATE_LIMIT_DELAY', '0.1'))
        )
        
        # Disable SSL warnings if needed
        if not self.source.verify_ssl or not self.destination.verify_ssl:
            urllib3.disable_warnings(InsecureRequestWarning)
    
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
        
        if self.migration.batch_size <= 0:
            errors.append("Batch size must be positive")
        
        if self.migration.concurrent_workers <= 0:
            errors.append("Concurrent workers must be positive")
        
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