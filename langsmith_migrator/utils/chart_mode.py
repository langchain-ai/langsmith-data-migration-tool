"""Helpers for deciding whether chart migration may reuse source IDs."""

from __future__ import annotations

from .config import Config


def normalize_deployment_url(base_url: str) -> str:
    """Normalize a LangSmith deployment URL for same-deployment comparisons."""
    normalized = (base_url or "").strip().rstrip("/").lower()
    for suffix in ("/api/v1", "/api/v2"):
        if normalized.endswith(suffix):
            normalized = normalized[: -len(suffix)]
            break
    return normalized


def is_same_deployment(config: Config) -> bool:
    """Return True when source and destination point at the same deployment."""
    return normalize_deployment_url(config.source.base_url) == normalize_deployment_url(
        config.destination.base_url
    )


def workspace_pair_allows_same_instance(
    source_workspace_id: str | None = None,
    dest_workspace_id: str | None = None,
) -> bool:
    """Return True when chart IDs can safely be reused for a workspace pair."""
    if not source_workspace_id and not dest_workspace_id:
        return True
    return bool(source_workspace_id) and source_workspace_id == dest_workspace_id


def should_reuse_chart_ids(
    config: Config,
    source_workspace_id: str | None = None,
    dest_workspace_id: str | None = None,
) -> bool:
    """Return whether chart migration should run in same-instance ID reuse mode."""
    if not is_same_deployment(config):
        return False

    same_workspace_scope = (
        bool(source_workspace_id)
        and source_workspace_id == dest_workspace_id
    )
    return workspace_pair_allows_same_instance(
        source_workspace_id,
        dest_workspace_id,
    ) and (
        config.source.api_key == config.destination.api_key
        or same_workspace_scope
    )
