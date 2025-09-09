#!/usr/bin/env python
"""Test script to demonstrate the interactive prompting."""

import sys
from langsmith_migrator.utils.config import Config
from langsmith_migrator.cli.main import ensure_config
from rich.console import Console

def test_prompting():
    """Test the interactive prompting flow."""
    console = Console()
    
    console.print("\n[bold]Demo: Interactive Credential Prompting[/bold]\n")
    console.print("This demo shows how the tool prompts for missing credentials.")
    console.print("Since this is just a test, you can enter any values.\n")
    
    # Create config without any env vars
    config = Config()
    
    # Show initial state
    console.print("[dim]Initial state:[/dim]")
    is_valid, errors = config.validate()
    console.print(f"  Valid: {is_valid}")
    if not is_valid:
        console.print(f"  Errors: {len(errors)}")
    
    # Now ensure config (which will prompt)
    console.print("\n[cyan]Now the tool will prompt for missing credentials:[/cyan]")
    
    if ensure_config(config):
        console.print("\n[green]✓[/green] Configuration is now valid!")
        console.print(f"  Source: {config.source.base_url}")
        console.print(f"  Destination: {config.destination.base_url}")
        console.print(f"  API keys configured: Yes")
    else:
        console.print("\n[red]Configuration still invalid[/red]")

if __name__ == "__main__":
    try:
        test_prompting()
    except (KeyboardInterrupt, EOFError):
        print("\n\nDemo cancelled by user")
        sys.exit(0)