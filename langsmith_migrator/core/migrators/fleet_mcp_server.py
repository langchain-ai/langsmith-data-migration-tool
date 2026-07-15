"""Fleet MCP server and integration migration logic."""

from typing import Dict, List, Any, Optional

from .base import BaseMigrator
from ..api_client import NotFoundError


class FleetMcpServerMigrator(BaseMigrator):
    """Handles migration of Fleet MCP servers and integrations."""

    def list_mcp_servers(self) -> List[Dict[str, Any]]:
        """List all MCP servers from the source workspace."""
        servers = []
        try:
            for server in self.source.get_cursor_paginated("/v1/fleet/mcp-servers"):
                if isinstance(server, dict):
                    servers.append(server)
        except NotFoundError:
            self.log("Fleet MCP servers endpoint not found", "warning")
        except Exception as e:
            self.log(f"Failed to list Fleet MCP servers: {e}", "warning")
        return servers

    def list_integrations(self) -> List[Dict[str, Any]]:
        """List all integrations from the source workspace."""
        integrations = []
        try:
            for integration in self.source.get_cursor_paginated("/v1/fleet/integrations"):
                if isinstance(integration, dict):
                    integrations.append(integration)
        except NotFoundError:
            self.log("Fleet integrations endpoint not found", "warning")
        except Exception as e:
            self.log(f"Failed to list Fleet integrations: {e}", "warning")
        return integrations

    def find_existing_mcp_server(self, name: str) -> Optional[str]:
        """Check if an MCP server with the same name exists in destination."""
        server = self.dest_index(
            "_dest_mcp_servers",
            "/v1/fleet/mcp-servers",
            "name",
            error_label="MCP server",
        ).get(name)
        return server.get("id") if server else None

    def find_existing_integration(self, name: str) -> Optional[str]:
        """Check if an integration with the same name exists in destination."""
        integration = self.dest_index(
            "_dest_integrations",
            "/v1/fleet/integrations",
            "name",
            error_label="integration",
        ).get(name)
        return integration.get("id") if integration else None

    def create_mcp_server(
        self,
        server: Dict[str, Any],
        oauth_provider_id_map: Optional[Dict[str, str]] = None,
    ) -> str:
        """Create or update an MCP server in the destination workspace.

        Args:
            server: Source MCP server record.
            oauth_provider_id_map: Optional mapping of source to destination
                auth provider IDs for remapping oauth_provider_id.

        Returns the destination MCP server ID.
        """
        name = server.get("name", "")
        existing_id = self.find_existing_mcp_server(name)

        if existing_id:
            self.log(f"MCP server '{name}' already exists, skipping", "warning")
            return existing_id

        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create MCP server: {name}")
            return f"dry-run-{server.get('id', name)}"

        payload = {
            "name": name,
            "url": server.get("url", ""),
            "auth_type": server.get("auth_type", "headers"),
            "headers": server.get("headers", []),
        }

        oauth_provider_id = server.get("oauth_provider_id")
        if oauth_provider_id and oauth_provider_id_map:
            mapped = oauth_provider_id_map.get(oauth_provider_id)
            if mapped:
                payload["oauth_provider_id"] = mapped
        elif oauth_provider_id:
            payload["oauth_provider_id"] = oauth_provider_id

        if server.get("oauth_mode"):
            payload["oauth_mode"] = server["oauth_mode"]

        response = self.dest.post("/v1/fleet/mcp-servers", payload)

        if not isinstance(response, dict):
            from ..api_client import APIError
            raise APIError(
                f"Invalid response creating MCP server: expected dict, got {type(response)}"
            )
        if "id" not in response:
            from ..api_client import APIError
            raise APIError(
                f"Invalid response creating MCP server: missing 'id'. Response: {response}"
            )

        self.register_dest_item("_dest_mcp_servers", name, response)
        return response["id"]

    def _update_mcp_server(
        self,
        server_id: str,
        server: Dict[str, Any],
        oauth_provider_id_map: Optional[Dict[str, str]] = None,
    ) -> None:
        """Update an existing MCP server in destination."""
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would update MCP server: {server.get('name')} ({server_id})")
            return

        payload: Dict[str, Any] = {
            "name": server.get("name"),
            "url": server.get("url"),
            "headers": server.get("headers", []),
        }

        oauth_provider_id = server.get("oauth_provider_id")
        if oauth_provider_id and oauth_provider_id_map:
            mapped = oauth_provider_id_map.get(oauth_provider_id)
            if mapped:
                payload["oauth_provider_id"] = mapped
        elif oauth_provider_id:
            payload["oauth_provider_id"] = oauth_provider_id

        payload = {k: v for k, v in payload.items() if v is not None}
        self.dest.patch(f"/v1/fleet/mcp-servers/{server_id}", payload)
        self.log(f"Updated MCP server: {server.get('name')} ({server_id})", "success")

    def create_integration(
        self,
        integration: Dict[str, Any],
    ) -> Optional[str]:
        """Create a workspace-owned integration in the destination.

        Platform-owned integrations are skipped (they are built-in).

        Returns the destination integration ID, or None if skipped.
        """
        owner = integration.get("owner", "")
        if owner == "platform":
            self.log(
                f"Skipping platform-owned integration '{integration.get('name')}'",
                "info",
            )
            return None

        name = integration.get("name", "")
        existing_id = self.find_existing_integration(name)

        if existing_id:
            self.log(f"Integration '{name}' already exists, skipping", "warning")
            return existing_id

        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create integration: {name}")
            return f"dry-run-{integration.get('id', name)}"

        payload: Dict[str, Any] = {
            "name": name,
            "url": integration.get("url", ""),
        }

        source = integration.get("source")
        if source:
            payload["source"] = source

        external_system_id = integration.get("external_system_id")
        if external_system_id:
            payload["external_system_id"] = external_system_id

        auth_methods = integration.get("auth_methods")
        if auth_methods:
            payload["auth_methods"] = auth_methods

        headers = integration.get("headers")
        if headers:
            payload["headers"] = headers

        response = self.dest.post("/v1/fleet/integrations", payload)

        if not isinstance(response, dict):
            from ..api_client import APIError
            raise APIError(
                f"Invalid response creating integration: expected dict, got {type(response)}"
            )

        self.register_dest_item("_dest_integrations", name, response)
        return response.get("id")
