"""Unit tests for DatasetMigrator."""

from unittest.mock import Mock, patch, MagicMock

import pytest
import requests

from langsmith_migrator.core.api_client import EnhancedAPIClient, NotFoundError
from langsmith_migrator.core.migrators.dataset import (
    DatasetMigrator,
    MAX_ATTACHMENT_SIZE_BYTES,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _mock_client() -> Mock:
    """Create a mock EnhancedAPIClient with the minimum attributes."""
    client = Mock(spec=EnhancedAPIClient)
    client.base_url = "https://api.test.langsmith.com/api/v1"
    client.headers = {"X-API-Key": "test-key"}
    client.session = Mock()
    client.session.headers = {}
    client.verify_ssl = True
    return client


def _make_config(dry_run=False, skip_existing=False, verbose=False, verify_ssl=True):
    """Build a lightweight config mock matching the attrs the migrator reads."""
    config = Mock()
    config.migration.dry_run = dry_run
    config.migration.skip_existing = skip_existing
    config.migration.verbose = verbose
    config.migration.batch_size = 100
    config.migration.stream_examples = True
    config.migration.chunk_size = 1000
    config.source.verify_ssl = verify_ssl
    config.destination.verify_ssl = verify_ssl
    config.state_manager = None
    return config


def _make_migrator(source=None, dest=None, config=None, state=None):
    """Convenience factory for DatasetMigrator with sensible defaults."""
    return DatasetMigrator(
        source_client=source or _mock_client(),
        dest_client=dest or _mock_client(),
        state=state or Mock(),
        config=config or _make_config(),
    )


# ---------------------------------------------------------------------------
# Dataset CRUD
# ---------------------------------------------------------------------------


class TestFindExistingDataset:
    """Tests for DatasetMigrator.find_existing_dataset."""

    def test_find_existing_dataset_by_name(self):
        """When the destination returns exactly one dataset with a matching name,
        find_existing_dataset should return its id."""
        dest = _mock_client()
        dest.get.return_value = [{"id": "dest-ds-1", "name": "My Dataset"}]

        migrator = _make_migrator(dest=dest)
        result = migrator.find_existing_dataset("My Dataset")

        assert result == "dest-ds-1"
        dest.get.assert_called_once_with("/datasets", params={"name": "My Dataset"})

    def test_find_existing_dataset_not_found(self):
        """When the destination returns an empty list, should return None."""
        dest = _mock_client()
        dest.get.return_value = []

        migrator = _make_migrator(dest=dest)
        result = migrator.find_existing_dataset("Nonexistent")

        assert result is None

    def test_find_existing_dataset_not_found_on_404(self):
        """When the destination raises NotFoundError, should return None."""
        dest = _mock_client()
        dest.get.side_effect = NotFoundError("Not found")

        migrator = _make_migrator(dest=dest)
        result = migrator.find_existing_dataset("Missing")

        assert result is None

    def test_find_existing_dataset_multiple_warns(self):
        """When multiple datasets match, should return None (ambiguous)."""
        dest = _mock_client()
        dest.get.return_value = [
            {"id": "ds-a", "name": "Dup"},
            {"id": "ds-b", "name": "Dup"},
        ]

        migrator = _make_migrator(dest=dest, config=_make_config(verbose=True))
        result = migrator.find_existing_dataset("Dup")

        assert result is None


class TestCreateDataset:
    """Tests for DatasetMigrator.create_dataset."""

    def test_create_dataset_dry_run(self):
        """In dry-run mode, no POST should be issued and a placeholder id returned."""
        dest = _mock_client()
        config = _make_config(dry_run=True)
        migrator = _make_migrator(dest=dest, config=config)

        dataset = {"id": "src-1", "name": "DS1", "description": "test"}
        result = migrator.create_dataset(dataset)

        assert result == "dry-run-src-1"
        dest.post.assert_not_called()

    def test_create_dataset_posts_payload(self):
        """Normal creation should POST to /datasets and return the new id."""
        dest = _mock_client()
        dest.post.return_value = {"id": "new-ds-99"}

        migrator = _make_migrator(dest=dest)
        dataset = {
            "id": "src-1",
            "name": "DS1",
            "description": "desc",
            "data_type": "kv",
        }
        result = migrator.create_dataset(dataset)

        assert result == "new-ds-99"
        dest.post.assert_called_once()
        call_args = dest.post.call_args
        assert call_args[0][0] == "/datasets"
        assert call_args[0][1]["name"] == "DS1"

    def test_create_dataset_skips_existing(self):
        """With skip_existing=True and an existing dataset, migrate_dataset
        returns the existing id without creating a new one."""
        source = _mock_client()
        dest = _mock_client()
        config = _make_config(skip_existing=True)

        source.get.return_value = {
            "id": "src-1",
            "name": "DS1",
            "description": "test",
        }
        # find_existing_dataset calls dest.get
        dest.get.return_value = [{"id": "existing-ds", "name": "DS1"}]

        migrator = _make_migrator(source=source, dest=dest, config=config)
        new_id, mapping = migrator.migrate_dataset("src-1", include_examples=False)

        assert new_id == "existing-ds"
        assert mapping == {}
        # No POST should have been made (no dataset creation)
        dest.post.assert_not_called()


# ---------------------------------------------------------------------------
# Example streaming
# ---------------------------------------------------------------------------


class TestStreamExamples:
    """Tests for DatasetMigrator.stream_examples."""

    def test_stream_examples_pagination(self):
        """stream_examples should pass the correct params (including
        dataset id and select fields) to source.get_paginated."""
        source = _mock_client()
        example_a = {"id": "ex-1", "inputs": {"q": "hi"}}
        example_b = {"id": "ex-2", "inputs": {"q": "bye"}}
        source.get_paginated.return_value = iter([example_a, example_b])

        migrator = _make_migrator(source=source)
        results = list(migrator.stream_examples("dataset-abc"))

        assert results == [example_a, example_b]
        source.get_paginated.assert_called_once_with(
            "/examples",
            params={
                "dataset": "dataset-abc",
                "select": ["attachment_urls", "outputs", "metadata"],
            },
        )

    def test_stream_examples_empty_dataset(self):
        """An empty dataset should yield zero examples."""
        source = _mock_client()
        source.get_paginated.return_value = iter([])

        migrator = _make_migrator(source=source)
        results = list(migrator.stream_examples("empty-ds"))

        assert results == []


# ---------------------------------------------------------------------------
# Attachment safety
# ---------------------------------------------------------------------------


class TestDownloadAttachments:
    """Tests for DatasetMigrator.download_attachments safety checks."""

    def test_download_attachments_skips_no_presigned_url(self):
        """Attachments without a presigned_url should be silently skipped."""
        config = _make_config(verbose=True)
        migrator = _make_migrator(config=config)

        attachments = {
            "attachment.doc": {
                "mime_type": "text/plain",
                # no presigned_url key
            }
        }

        result = migrator.download_attachments(attachments)

        assert result == {}

    @patch("langsmith_migrator.core.migrators.dataset.request_with_retry")
    def test_download_attachments_skips_oversized(self, mock_retry):
        """Attachments whose HEAD Content-Length exceeds MAX_ATTACHMENT_SIZE_BYTES
        should be skipped without attempting the full download."""
        config = _make_config(verbose=True)
        migrator = _make_migrator(config=config)

        head_resp = Mock()
        head_resp.raise_for_status = Mock()
        head_resp.headers = {
            "Content-Length": str(MAX_ATTACHMENT_SIZE_BYTES + 1),
            "Content-Type": "application/octet-stream",
        }
        mock_retry.return_value = head_resp

        attachments = {
            "attachment.bigfile": {
                "presigned_url": "https://s3.example.com/bigfile",
                "mime_type": "application/octet-stream",
            }
        }

        result = migrator.download_attachments(attachments)

        assert result == {}
        mock_retry.assert_called_once()

    @patch("langsmith_migrator.core.migrators.dataset.requests.get")
    @patch("langsmith_migrator.core.migrators.dataset.request_with_retry")
    def test_download_attachments_succeeds_within_limit(self, mock_retry, mock_get):
        """An attachment within the size limit should be downloaded and returned."""
        config = _make_config(verbose=True)
        migrator = _make_migrator(config=config)

        # HEAD response within limit
        head_resp = Mock()
        head_resp.raise_for_status = Mock()
        head_resp.headers = {
            "Content-Length": "1024",
            "Content-Type": "text/plain",
        }
        mock_retry.return_value = head_resp

        # GET response (streaming download)
        content = b"hello world"
        get_resp = MagicMock()
        get_resp.__enter__ = Mock(return_value=get_resp)
        get_resp.__exit__ = Mock(return_value=False)
        get_resp.raise_for_status = Mock()
        get_resp.iter_content.return_value = [content]
        get_resp.headers = {"Content-Length": str(len(content))}
        mock_get.return_value = get_resp

        attachments = {
            "attachment.readme": {
                "presigned_url": "https://s3.example.com/readme",
                "mime_type": "text/plain",
            }
        }

        result = migrator.download_attachments(attachments)

        assert "attachment.readme" in result
        mime_type, temp_path, original_filename = result["attachment.readme"]
        assert mime_type == "text/plain"
        assert original_filename == "readme"

        # Clean up the temp file
        import os
        if os.path.exists(temp_path):
            os.remove(temp_path)

    def test_download_attachments_empty_dict(self):
        """Passing an empty attachments dict should return empty."""
        migrator = _make_migrator()
        assert migrator.download_attachments({}) == {}

    def test_download_attachments_none_input(self):
        """Passing None should return empty (guarded by `if not attachments`)."""
        migrator = _make_migrator()
        assert migrator.download_attachments(None) == {}
