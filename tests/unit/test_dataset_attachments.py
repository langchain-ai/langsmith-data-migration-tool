"""Attachment handling in DatasetMigrator: URL resolution and SDK client wiring."""

from unittest.mock import Mock

import pytest

from langsmith_migrator.core.migrators.dataset import DatasetMigrator

SOURCE_BASE = "https://source.api.test.com/api/v1"
DEST_BASE = "https://dest.api.test.com/api/v1"
RELATIVE_PRESIGNED = "/api/v1/public/download?jwt=eyJhbGciOiJIUzI1NiJ9.payload.sig"
DEST_INFO = {"version": "0.17.23", "instance_flags": {"dataset_examples_multipart_enabled": True}}


@pytest.fixture
def migrator(sample_config):
    source = Mock()
    source.base_url = SOURCE_BASE
    source.verify_ssl = True
    dest = Mock()
    dest.base_url = DEST_BASE
    dest.headers = {"X-API-Key": "dest-key"}
    dest.get.return_value = DEST_INFO
    return DatasetMigrator(source, dest, None, sample_config)


def test_relative_presigned_url_is_resolved_against_source_host(migrator):
    """Self-hosted LangSmith returns /api/v1/public/download?jwt=... with no scheme or host."""
    resolved = migrator._absolute_source_url(RELATIVE_PRESIGNED)

    assert resolved == f"https://source.api.test.com{RELATIVE_PRESIGNED}"


def test_same_host_absolute_url_passes_through(migrator):
    same_host = f"https://source.api.test.com{RELATIVE_PRESIGNED}"

    assert migrator._absolute_source_url(same_host) == same_host


def test_foreign_host_absolute_url_is_rejected(migrator):
    """A presigned URL pointing at any other host must never be fetched."""
    with pytest.raises(PermissionError):
        migrator._absolute_source_url("https://evil.example.com/steal-me")


def test_protocol_relative_url_is_rejected(migrator):
    """`//host/path` parses with an empty scheme but still names its own host -
    it must not be treated as a same-host relative path just because
    urlparse().scheme is falsy."""
    with pytest.raises(PermissionError):
        migrator._absolute_source_url("//evil.example.com/steal-me")


def test_download_attachments_requests_the_resolved_url_without_redirects(migrator, monkeypatch):
    seen_calls = []

    def fake_head(url, **kwargs):
        seen_calls.append(("HEAD", url, kwargs.get("allow_redirects")))
        response = Mock()
        response.headers = {"Content-Length": "5", "Content-Type": "text/plain"}
        return response

    def fake_get(url, **kwargs):
        seen_calls.append(("GET", url, kwargs.get("allow_redirects")))
        response = Mock()
        response.headers = {"Content-Length": "5", "Content-Type": "text/plain"}
        response.iter_content.return_value = [b"hello"]
        context = Mock()
        context.__enter__ = Mock(return_value=response)
        context.__exit__ = Mock(return_value=False)
        return context

    monkeypatch.setattr("langsmith_migrator.core.migrators.dataset.requests.head", fake_head)
    monkeypatch.setattr("langsmith_migrator.core.migrators.dataset.requests.get", fake_get)

    downloaded = migrator.download_attachments(
        {"attachment.notes.txt": {"presigned_url": RELATIVE_PRESIGNED, "mime_type": "text/plain"}}
    )

    expected = f"https://source.api.test.com{RELATIVE_PRESIGNED}"
    assert seen_calls == [("HEAD", expected, False), ("GET", expected, False)]
    assert set(downloaded) == {"attachment.notes.txt"}


def test_download_attachments_skips_rejected_host_without_making_requests(migrator, monkeypatch):
    calls = []
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.dataset.requests.head",
        lambda *a, **k: calls.append("HEAD"),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.dataset.requests.get",
        lambda *a, **k: calls.append("GET"),
    )

    downloaded = migrator.download_attachments(
        {"attachment.notes.txt": {"presigned_url": "https://evil.example.com/x", "mime_type": "text/plain"}}
    )

    assert downloaded == {}
    assert calls == []


def test_destination_sdk_client_receives_instance_info(migrator, monkeypatch):
    """The SDK strips attachments unless it can see dataset_examples_multipart_enabled."""
    captured_kwargs = {}

    class FakeClient:
        def __init__(self, **kwargs):
            captured_kwargs.update(kwargs)

    monkeypatch.setattr("langsmith.Client", FakeClient)

    migrator.create_examples_with_attachments("dataset-123", [])

    migrator.dest.get.assert_called_once_with("/info")
    assert captured_kwargs["info"] == DEST_INFO
    assert captured_kwargs["api_url"] == "https://dest.api.test.com"


def test_destination_info_is_fetched_once_and_cached(migrator, monkeypatch):
    """/info must not be re-fetched (and re-retried) on every batch."""
    monkeypatch.setattr("langsmith.Client", Mock())

    migrator.create_examples_with_attachments("dataset-123", [])
    migrator.create_examples_with_attachments("dataset-123", [])

    migrator.dest.get.assert_called_once_with("/info")


def test_destination_info_fetch_failure_falls_back_to_empty_dict(migrator, monkeypatch):
    """A destination where /info isn't reachable must not fail the whole batch -
    it should fall back to {} so examples still land, just without the
    attachments multipart flag."""
    captured_kwargs = {}

    class FakeClient:
        def __init__(self, **kwargs):
            captured_kwargs.update(kwargs)

    monkeypatch.setattr("langsmith.Client", FakeClient)
    migrator.dest.get.side_effect = Exception("404 Not Found")

    migrator.create_examples_with_attachments("dataset-123", [])

    assert captured_kwargs["info"] == {}
