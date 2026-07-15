"""Unit tests for FleetMcpServerMigrator."""

from unittest.mock import Mock

import pytest

from langsmith_migrator.core.api_client import EnhancedAPIClient
from langsmith_migrator.core.migrators import FleetMcpServerMigrator


def _mock_client() -> Mock:
    client = Mock(spec=EnhancedAPIClient)
    client.session = Mock()
    client.session.headers = {}
    return client


class TestFleetMcpServerMigrator:
    """Test cases for FleetMcpServerMigrator."""

    @pytest.fixture
    def mcp_migrator(self, sample_config, migration_state):
        source = _mock_client()
        dest = _mock_client()
        return FleetMcpServerMigrator(source, dest, migration_state, sample_config)

    @pytest.fixture
    def sample_server(self):
        return {
            "id": "server-1",
            "name": "Custom MCP",
            "url": "https://mcp.example.com/sse",
            "auth_type": "headers",
            "headers": [{"Authorization": "Bearer token"}],
        }

    def test_list_mcp_servers(self, mcp_migrator, sample_server):
        """Test listing MCP servers."""
        mcp_migrator.source.get_cursor_paginated.return_value = [sample_server]

        result = mcp_migrator.list_mcp_servers()

        assert len(result) == 1
        mcp_migrator.source.get_cursor_paginated.assert_called_once_with("/v1/fleet/mcp-servers")

    def test_create_mcp_server(self, mcp_migrator, sample_server):
        """Test creating an MCP server."""
        mcp_migrator.dest.get_cursor_paginated.return_value = []
        mcp_migrator.dest.post.return_value = {"id": "new-server-id"}

        result = mcp_migrator.create_mcp_server(sample_server)

        assert result == "new-server-id"
        call_args = mcp_migrator.dest.post.call_args
        assert call_args[0][0] == "/v1/fleet/mcp-servers"
        assert call_args[0][1]["name"] == "Custom MCP"
        assert call_args[0][1]["url"] == "https://mcp.example.com/sse"

    def test_create_mcp_server_with_oauth_provider_remap(self, mcp_migrator):
        """OAuth provider ID should be remapped when a mapping is provided."""
        server = {
            "id": "server-1",
            "name": "OAuth MCP",
            "url": "https://mcp.example.com/mcp",
            "auth_type": "oauth",
            "oauth_provider_id": "source-provider-id",
        }
        mcp_migrator.dest.get_cursor_paginated.return_value = []
        mcp_migrator.dest.post.return_value = {"id": "new-server-id"}

        oauth_map = {"source-provider-id": "dest-provider-id"}
        result = mcp_migrator.create_mcp_server(server, oauth_map)

        assert result == "new-server-id"
        payload = mcp_migrator.dest.post.call_args[0][1]
        assert payload["oauth_provider_id"] == "dest-provider-id"

    def test_create_mcp_server_existing_skip(self, mcp_migrator, sample_server):
        """Test skipping an existing MCP server (always skips)."""
        mcp_migrator.dest.get_cursor_paginated.return_value = [
            {"id": "existing", "name": "Custom MCP"},
        ]

        result = mcp_migrator.create_mcp_server(sample_server)

        assert result == "existing"
        mcp_migrator.dest.post.assert_not_called()

    def test_create_integration_skips_platform_owned(self, mcp_migrator):
        """Platform-owned integrations should be skipped."""
        integration = {"id": "int-1", "name": "Gmail", "owner": "platform"}

        result = mcp_migrator.create_integration(integration)

        assert result is None
        mcp_migrator.dest.post.assert_not_called()

    def test_create_integration_workspace_owned(self, mcp_migrator):
        """Workspace-owned integrations should be created."""
        integration = {
            "id": "int-1",
            "name": "Custom Integration",
            "owner": "workspace",
            "url": "https://api.example.com/mcp",
            "source": "custom",
            "auth_methods": [{"type": "headers"}],
            "headers": [{"key": "Authorization", "value": "Bearer token"}],
        }
        mcp_migrator.dest.get_cursor_paginated.return_value = []
        mcp_migrator.dest.post.return_value = {"id": "new-int-id"}

        result = mcp_migrator.create_integration(integration)

        assert result == "new-int-id"
        call_args = mcp_migrator.dest.post.call_args
        assert call_args[0][0] == "/v1/fleet/integrations"

    def test_list_integrations(self, mcp_migrator):
        """Test listing integrations."""
        mcp_migrator.source.get_cursor_paginated.return_value = [
            {"id": "int-1", "name": "Custom", "owner": "workspace"}
        ]

        result = mcp_migrator.list_integrations()

        assert len(result) == 1
        mcp_migrator.source.get_cursor_paginated.assert_called_with("/v1/fleet/integrations")

    def test_list_integrations_not_found(self, mcp_migrator):
        """Test listing when endpoint not found."""
        from langsmith_migrator.core.api_client import NotFoundError

        mcp_migrator.source.get_cursor_paginated.side_effect = NotFoundError(
            "Not found", status_code=404, request_info={}
        )

        result = mcp_migrator.list_integrations()

        assert len(result) == 0

    def test_find_existing_mcp_server(self, mcp_migrator):
        """find_existing_mcp_server should search destination by name."""
        mcp_migrator.dest.get_cursor_paginated.return_value = [
            {"id": "dest-server", "name": "Custom MCP"},
        ]

        result = mcp_migrator.find_existing_mcp_server("Custom MCP")

        assert result == "dest-server"

    def test_create_mcp_server_dry_run(self, mcp_migrator, sample_server):
        """Test dry run mode."""
        mcp_migrator.dest.get_cursor_paginated.return_value = []
        mcp_migrator.config.migration.dry_run = True

        result = mcp_migrator.create_mcp_server(sample_server)

        assert result.startswith("dry-run-")
        mcp_migrator.dest.post.assert_not_called()

    def test_create_integration_existing_skip(self, mcp_migrator):
        """Test skipping an existing workspace integration (always skips)."""
        integration = {
            "id": "int-1",
            "name": "Custom Integration",
            "owner": "workspace",
            "url": "https://api.example.com/mcp",
        }
        mcp_migrator.dest.get_cursor_paginated.return_value = [
            {"id": "existing", "name": "Custom Integration"},
        ]

        result = mcp_migrator.create_integration(integration)

        assert result == "existing"
        mcp_migrator.dest.post.assert_not_called()
