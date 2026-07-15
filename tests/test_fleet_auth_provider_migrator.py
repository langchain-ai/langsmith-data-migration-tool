"""Unit tests for FleetAuthProviderMigrator."""

from unittest.mock import Mock

import pytest

from langsmith_migrator.core.api_client import EnhancedAPIClient
from langsmith_migrator.core.migrators import FleetAuthProviderMigrator


def _mock_client() -> Mock:
    client = Mock(spec=EnhancedAPIClient)
    client.session = Mock()
    client.session.headers = {}
    return client


class TestFleetAuthProviderMigrator:
    """Test cases for FleetAuthProviderMigrator."""

    @pytest.fixture
    def auth_migrator(self, sample_config, migration_state):
        source = _mock_client()
        dest = _mock_client()
        return FleetAuthProviderMigrator(source, dest, migration_state, sample_config)

    @pytest.fixture
    def sample_provider(self):
        return {
            "id": "provider-1",
            "provider_slug": "google",
            "owner": "workspace",
            "name": "Google OAuth",
            "client_id": "client-123",
            "auth_url": "https://accounts.google.com/o/oauth2/auth",
            "token_url": "https://oauth2.googleapis.com/token",
            "uses_pkce": False,
            "token_endpoint_auth_method": "client_secret_post",
            "authorization_params": {"access_type": "offline"},
            "allowed_redirect_uris": [
                "https://source.example.com/api-host/v2/auth/callback/google",
                "https://source.example.com/host-oauth-callback/google",
            ],
            "default_redirect_uri": "https://source.example.com/api-host/v2/auth/callback/google",
        }

    def test_list_providers(self, auth_migrator, sample_provider):
        """Test listing auth providers."""
        auth_migrator.source.get_cursor_paginated.return_value = [sample_provider]

        result = auth_migrator.list_providers()

        assert len(result) == 1
        auth_migrator.source.get_cursor_paginated.assert_called_once_with("/v1/fleet/auth-providers")

    def test_list_providers_not_found(self, auth_migrator):
        """Test listing when endpoint not found."""
        from langsmith_migrator.core.api_client import NotFoundError

        auth_migrator.source.get_cursor_paginated.side_effect = NotFoundError(
            "Not found", status_code=404, request_info={}
        )

        result = auth_migrator.list_providers()

        assert len(result) == 0

    def test_create_provider(self, auth_migrator, sample_provider):
        """Test creating an auth provider."""
        auth_migrator.dest.get_cursor_paginated.return_value = []
        auth_migrator.dest.post.return_value = {"provider_slug": "google"}

        result = auth_migrator.create_provider(sample_provider)

        assert result == "google"
        call_args = auth_migrator.dest.post.call_args
        assert call_args[0][0] == "/v1/fleet/auth-providers"
        payload = call_args[0][1]
        assert payload["provider_slug"] == "google"
        assert payload["client_id"] == "client-123"
        assert payload["uses_pkce"] is False
        # client_secret should NOT be in the payload (write-only on source)
        assert "client_secret" not in payload

    def test_create_provider_remaps_redirect_uris(self, auth_migrator, sample_provider):
        """Redirect URIs should be updated to use the destination base URL."""
        auth_migrator.dest.get_cursor_paginated.return_value = []
        auth_migrator.dest.post.return_value = {"provider_slug": "google"}

        auth_migrator.create_provider(sample_provider, dest_base_url="https://dest.example.com")

        payload = auth_migrator.dest.post.call_args[0][1]
        assert all("dest.example.com" in uri for uri in payload["allowed_redirect_uris"])
        assert "dest.example.com" in payload["default_redirect_uri"]

    def test_create_provider_skips_platform_owned(self, auth_migrator):
        """Platform-owned providers should be skipped."""
        provider = {"provider_slug": "gmail", "owner": "platform", "name": "Gmail"}

        result = auth_migrator.create_provider(provider)

        assert result is None
        auth_migrator.dest.post.assert_not_called()

    def test_create_provider_existing_left_intact(self, auth_migrator, sample_provider):
        """Existing providers should be left intact (org-scoped infrastructure)."""
        auth_migrator.dest.get_cursor_paginated.return_value = [
            {"provider_slug": "google", "name": "Google OAuth"},
        ]

        result = auth_migrator.create_provider(sample_provider)

        assert result == "google"
        auth_migrator.dest.post.assert_not_called()
        auth_migrator.dest.patch.assert_not_called()

    def test_create_provider_dry_run(self, auth_migrator, sample_provider):
        """Test dry run mode."""
        auth_migrator.dest.get_cursor_paginated.return_value = []
        auth_migrator.config.migration.dry_run = True

        result = auth_migrator.create_provider(sample_provider)

        assert result.startswith("dry-run-")
        auth_migrator.dest.post.assert_not_called()

    def test_create_provider_failure(self, auth_migrator, sample_provider):
        """Test failure handling."""
        auth_migrator.dest.get_cursor_paginated.return_value = []
        auth_migrator.dest.post.side_effect = Exception("API error")

        result = auth_migrator.create_provider(sample_provider)

        assert result is None

    def test_create_provider_reserved_slug_conflict(self, auth_migrator):
        """Reserved built-in slug conflicts should be skipped, not failed."""
        from langsmith_migrator.core.api_client import ConflictError

        provider = {
            "provider_slug": "salesforce-oauth-provider",
            "owner": "workspace",
            "name": "Salesforce",
            "client_id": "client-123",
            "auth_url": "https://login.salesforce.com/auth",
            "token_url": "https://login.salesforce.com/token",
            "uses_pkce": False,
        }
        auth_migrator.dest.get_cursor_paginated.return_value = []
        auth_migrator.dest.post.side_effect = ConflictError(
            "provider_slug is reserved for a built-in provider",
            request_info={},
        )

        result = auth_migrator.create_provider(provider)

        assert result is None

    def test_remap_redirect_uri_replaces_hostname(self):
        """Test the static redirect URI remapping method."""
        result = FleetAuthProviderMigrator._remap_redirect_uri(
            "https://source.example.com/api-host/v2/auth/callback/google",
            "https://dest.example.com",
        )
        assert "dest.example.com" in result
        assert "source.example.com" not in result
        # Path should be preserved
        assert "/api-host/v2/auth/callback/google" in result

    def test_remap_redirect_uri_no_scheme(self):
        """Test remapping when dest base URL has no scheme."""
        result = FleetAuthProviderMigrator._remap_redirect_uri(
            "https://source.example.com/callback",
            "dest.example.com",
        )
        # Should fall back to original URI when dest has no netloc
        assert result == "https://source.example.com/callback"
