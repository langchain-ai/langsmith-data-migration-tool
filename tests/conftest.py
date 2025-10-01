"""Shared fixtures and configuration for tests."""

import pytest
from unittest.mock import Mock
from langsmith_migrator.core.api_client import EnhancedAPIClient


@pytest.fixture
def mock_api_client():
    """Create a mock API client for testing."""
    client = Mock(spec=EnhancedAPIClient)
    client.base_url = "https://api.test.langsmith.com/api/v1"
    client.headers = {"X-API-Key": "test-key"}
    return client


@pytest.fixture
def sample_dataset():
    """Sample dataset data for testing."""
    return {
        "id": "dataset-123",
        "name": "Test Dataset",
        "description": "A test dataset",
        "created_at": "2024-01-01T00:00:00Z",
        "inputs_schema_definition": {},
        "outputs_schema_definition": {},
        "externally_managed": False,
        "transformations": [],
        "data_type": "kv"
    }


@pytest.fixture
def sample_examples():
    """Sample examples data for testing."""
    return [
        {
            "id": "example-1",
            "dataset_id": "dataset-123",
            "inputs": {"question": "What is 2+2?"},
            "outputs": {"answer": "4"},
            "metadata": {"difficulty": "easy"},
            "attachments": {
                "document": ("text/plain", b"Sample document content"),
                "image": ("image/png", b"fake_image_data")
            },
            "created_at": "2024-01-01T00:00:00Z"
        },
        {
            "id": "example-2",
            "dataset_id": "dataset-123",
            "inputs": {"question": "What is the capital of France?"},
            "outputs": {"answer": "Paris"},
            "metadata": {"difficulty": "medium"},
            "attachments": {},
            "created_at": "2024-01-01T00:00:00Z"
        }
    ]


@pytest.fixture
def sample_config():
    """Sample migration configuration."""
    from langsmith_migrator.utils.config import Config
    return Config(
        source_api_key="source-key",
        dest_api_key="dest-key",
        source_url="https://source.api.test.com",
        dest_url="https://dest.api.test.com",
        dry_run=False,
        verbose=False,
        batch_size=50,
        concurrent_workers=2
    )


@pytest.fixture
def migration_state():
    """Sample migration state."""
    import time
    from langsmith_migrator.utils.state import MigrationState
    return MigrationState(
        session_id="test-session",
        started_at=time.time(),
        updated_at=time.time(),
        source_url="https://source.test.com",
        destination_url="https://dest.test.com"
    )