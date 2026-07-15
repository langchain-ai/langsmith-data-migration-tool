"""Fleet auth provider migration logic."""

from typing import Dict, List, Any, Optional

from .base import BaseMigrator
from ..api_client import ConflictError, NotFoundError


class FleetAuthProviderMigrator(BaseMigrator):
    """Handles migration of Fleet OAuth auth providers.

    The ``client_secret`` field is write-only: it appears in create/update
    requests but is intentionally omitted from the provider response. This
    migrator copies the provider structure (slug, name, client_id, URLs,
    PKCE settings, redirect URIs) but cannot copy the secret. The customer
    must re-enter ``client_secret`` manually on the destination.
    """

    def list_providers(self) -> List[Dict[str, Any]]:
        """List all auth providers from the source workspace."""
        providers = []
        try:
            for provider in self.source.get_cursor_paginated("/v1/fleet/auth-providers"):
                if isinstance(provider, dict):
                    providers.append(provider)
        except NotFoundError:
            self.log("Fleet auth-providers endpoint not found", "warning")
        except Exception as e:
            self.log(f"Failed to list Fleet auth providers: {e}", "warning")
        return providers

    def find_existing_provider(self, provider_slug: str) -> Optional[str]:
        """Check if a provider with the same slug exists in destination."""
        provider = self.dest_index(
            "_dest_providers",
            "/v1/fleet/auth-providers",
            "provider_slug",
            error_label="auth provider",
        ).get(provider_slug)
        return provider.get("provider_slug") if provider else None

    def create_provider(
        self,
        provider: Dict[str, Any],
        dest_base_url: Optional[str] = None,
    ) -> Optional[str]:
        """Create an auth provider in the destination workspace.

        Args:
            provider: Source auth provider record (ProviderResponse shape).
            dest_base_url: Optional destination base URL for updating
                redirect URIs to point at the BYOC hostname.

        Returns the destination provider slug, or None if skipped/failed.
        """
        owner = provider.get("owner", "")
        if owner == "platform":
            self.log(
                f"Skipping platform-owned auth provider '{provider.get('provider_slug')}'",
                "info",
            )
            return None

        provider_slug = provider.get("provider_slug", "")
        existing = self.find_existing_provider(provider_slug)
        if existing:
            self.log(
                f"Auth provider '{provider_slug}' already exists on destination, "
                f"leaving intact (auth providers are org-scoped infrastructure)",
                "warning",
            )
            return existing

        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would create auth provider: {provider_slug}")
            return f"dry-run-{provider_slug}"

        payload: Dict[str, Any] = {
            "provider_slug": provider_slug,
            "name": provider.get("name", ""),
            "client_id": provider.get("client_id", ""),
            "auth_url": provider.get("auth_url", ""),
            "token_url": provider.get("token_url", ""),
            "uses_pkce": provider.get("uses_pkce", False),
        }

        token_endpoint_auth_method = provider.get("token_endpoint_auth_method")
        if token_endpoint_auth_method:
            payload["token_endpoint_auth_method"] = token_endpoint_auth_method

        authorization_params = provider.get("authorization_params")
        if authorization_params:
            payload["authorization_params"] = authorization_params

        allowed_redirect_uris = provider.get("allowed_redirect_uris", [])
        if allowed_redirect_uris and dest_base_url:
            payload["allowed_redirect_uris"] = self._remap_redirect_uris(
                allowed_redirect_uris, dest_base_url
            )
        elif allowed_redirect_uris:
            payload["allowed_redirect_uris"] = allowed_redirect_uris

        default_redirect_uri = provider.get("default_redirect_uri")
        if default_redirect_uri:
            if dest_base_url:
                payload["default_redirect_uri"] = self._remap_redirect_uri(
                    default_redirect_uri, dest_base_url
                )
            else:
                payload["default_redirect_uri"] = default_redirect_uri

        try:
            response = self.dest.post("/v1/fleet/auth-providers", payload)
            if isinstance(response, dict):
                created_slug = response.get("provider_slug", provider_slug)
                self.register_dest_item("_dest_providers", created_slug, response)
                return created_slug
        except ConflictError:
            self.log(
                f"Auth provider '{provider_slug}' conflicts with a built-in "
                f"reserved slug on destination, skipping. Configure it manually "
                f"via the Fleet UI or Helm chart.",
                "warning",
            )
            return None
        except Exception as e:
            self.log(f"Failed to create auth provider '{provider_slug}': {e}", "error")

        return None

    def _update_provider(
        self,
        provider_slug: str,
        provider: Dict[str, Any],
        dest_base_url: Optional[str] = None,
    ) -> None:
        """Update an existing auth provider in destination."""
        if self.config.migration.dry_run:
            self.log(f"[DRY RUN] Would update auth provider: {provider_slug}")
            return

        payload: Dict[str, Any] = {
            "name": provider.get("name"),
            "client_id": provider.get("client_id"),
            "auth_url": provider.get("auth_url"),
            "token_url": provider.get("token_url"),
        }

        uses_pkce = provider.get("uses_pkce")
        if uses_pkce is not None:
            payload["uses_pkce"] = uses_pkce

        allowed_redirect_uris = provider.get("allowed_redirect_uris", [])
        if allowed_redirect_uris and dest_base_url:
            payload["allowed_redirect_uris"] = self._remap_redirect_uris(
                allowed_redirect_uris, dest_base_url
            )
        elif allowed_redirect_uris:
            payload["allowed_redirect_uris"] = allowed_redirect_uris

        payload = {k: v for k, v in payload.items() if v is not None}
        self.dest.patch(f"/v1/fleet/auth-providers/{provider_slug}", payload)
        self.log(f"Updated auth provider: {provider_slug}", "success")

    @staticmethod
    def _remap_redirect_uris(
        uris: List[str], dest_base_url: str
    ) -> List[str]:
        """Update redirect URIs to point at the destination hostname."""
        return [
            FleetAuthProviderMigrator._remap_redirect_uri(uri, dest_base_url)
            for uri in uris
        ]

    @staticmethod
    def _remap_redirect_uri(uri: str, dest_base_url: str) -> str:
        """Replace the hostname in a redirect URI with the destination base URL."""
        from urllib.parse import urlparse, urlunparse

        parsed = urlparse(uri)
        dest_parsed = urlparse(dest_base_url)

        if not dest_parsed.netloc:
            return uri

        return urlunparse((
            dest_parsed.scheme or parsed.scheme or "https",
            dest_parsed.netloc,
            parsed.path,
            parsed.params,
            parsed.query,
            parsed.fragment,
        ))
