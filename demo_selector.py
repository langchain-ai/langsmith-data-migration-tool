#!/usr/bin/env python
"""Demo script to show the improved selection UI."""

from langsmith_migrator.cli.interactive_selector import InteractiveSelector
from rich.console import Console

# Create sample data
sample_datasets = [
    {
        "id": "d1234567-89ab-cdef-0123-456789abcdef",
        "name": "Customer Support Dataset",
        "description": "Contains customer support conversations and resolutions",
        "example_count": 1523
    },
    {
        "id": "d2345678-9abc-def0-1234-56789abcdef0",
        "name": "Product Reviews",
        "description": "Product review sentiment analysis dataset",
        "example_count": 3421
    },
    {
        "id": "d3456789-abcd-ef01-2345-6789abcdef01",
        "name": "Email Classification",
        "description": "Email categorization and priority assignment",
        "example_count": 892
    },
    {
        "id": "d4567890-bcde-f012-3456-789abcdef012",
        "name": "FAQ Generation",
        "description": "Question-answer pairs for FAQ automation",
        "example_count": 456
    },
    {
        "id": "d5678901-cdef-0123-4567-89abcdef0123",
        "name": "Intent Recognition",
        "description": "User intent classification for chatbots",
        "example_count": 2105
    },
    {
        "id": "d6789012-def0-1234-5678-9abcdef01234",
        "name": "Translation Pairs",
        "description": "Multi-language translation examples",
        "example_count": 5678
    },
    {
        "id": "d7890123-ef01-2345-6789-abcdef012345",
        "name": "Code Generation",
        "description": "Natural language to code examples",
        "example_count": 1234
    },
    {
        "id": "d8901234-f012-3456-789a-bcdef0123456",
        "name": "Document Summarization",
        "description": "Long-form document summary pairs",
        "example_count": 789
    }
]

def main():
    console = Console()
    
    console.print("\n[bold blue]Demo: Improved Selection UI[/bold blue]\n")
    console.print("This demonstrates the new selection interface with:")
    console.print("• ✓ Checkbox-style selection")
    console.print("• 🔍 Search/filter capability")
    console.print("• 📦 Select All option")
    console.print("• 🔢 Number shortcuts for quick selection")
    console.print("• 📄 Pagination for large lists\n")
    
    input("Press Enter to start the demo...")
    
    # Create selector
    selector = InteractiveSelector(
        items=sample_datasets,
        title="Select Datasets to Migrate",
        columns=[
            {"key": "name", "title": "Name", "width": 30},
            {"key": "id", "title": "ID", "width": 36},
            {"key": "description", "title": "Description", "width": 40},
            {"key": "example_count", "title": "Examples", "width": 10}
        ],
        console=console
    )
    
    # Run selector
    selected = selector.run()
    
    # Display results
    console.clear()
    if selected:
        console.print(f"\n[green]✓ You selected {len(selected)} dataset(s):[/green]\n")
        for item in selected:
            console.print(f"  • {item['name']} ({item['example_count']} examples)")
    else:
        console.print("\n[yellow]No items selected[/yellow]")
    
    console.print("\n[bold]Key improvements over the original:[/bold]")
    console.print("1. No more confusing Space/Enter - clear command menu")
    console.print("2. 'a' selects all visible, 'A' selects ALL items")
    console.print("3. Search/filter with 'f' command")
    console.print("4. Number shortcuts (1-20) for quick selection")
    console.print("5. Range selection (e.g., '1-5' to select items 1 through 5)")
    console.print("6. Visual checkboxes show selection state")
    console.print("7. Pagination with '<' and '>' for large lists")

if __name__ == "__main__":
    main()