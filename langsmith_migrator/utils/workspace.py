"""Workspace discovery helpers."""

from typing import Dict, List, Optional

from ..core.api_client import APIError, EnhancedAPIClient, NotFoundError


def _api_prefix(client: EnhancedAPIClient) -> str:
    """'' when the base URL already carries the API version, else '/api/v1'.

    Base URLs are accepted with or without the '/api/v1' suffix; adding it
    unconditionally produced '/api/v1/api/v1/workspaces' probes.
    """
    base_url = str(getattr(client, "base_url", "") or "")
    return "" if ("/api/v1" in base_url or "/api/v2" in base_url) else "/api/v1"


def _endpoint_unsupported(error: Exception) -> bool:
    """404 or 405: the server has no such route; try the next endpoint."""
    if isinstance(error, NotFoundError):
        return True
    return isinstance(error, APIError) and getattr(error, "status_code", None) == 405


def discover_workspaces(client: EnhancedAPIClient) -> Dict[str, object]:
    """Discover workspaces and record endpoint-level probe outcomes.

    Tries multiple endpoints since the workspace API varies across LangSmith versions.

    Args:
        client: An EnhancedAPIClient instance.

    Returns:
        Dict with ``workspaces`` and ``probes`` keys.
    """
    prefix = _api_prefix(client)
    endpoints = [f"{prefix}/workspaces", f"{prefix}/orgs/current/workspaces"]
    probes: List[Dict[str, object]] = []

    for endpoint in endpoints:
        try:
            response = client.get(endpoint)
            # Response may be a list directly or wrapped in a key
            if isinstance(response, list):
                probes.append({"endpoint": endpoint, "supported": True, "detail": "list"})
                return {"workspaces": response, "probes": probes}
            if isinstance(response, dict):
                # Common wrapper keys
                for key in ("workspaces", "items", "results"):
                    if key in response and isinstance(response[key], list):
                        probes.append(
                            {"endpoint": endpoint, "supported": True, "detail": f"wrapped:{key}"}
                        )
                        return {"workspaces": response[key], "probes": probes}
                # Single workspace returned as dict
                if "id" in response:
                    probes.append({"endpoint": endpoint, "supported": True, "detail": "single"})
                    return {"workspaces": [response], "probes": probes}
            probes.append({"endpoint": endpoint, "supported": True, "detail": "empty"})
            return {"workspaces": [], "probes": probes}
        except NotFoundError as e:
            probes.append({"endpoint": endpoint, "supported": False, "detail": str(e)})
            continue
        except Exception as e:
            probes.append({"endpoint": endpoint, "supported": False, "detail": str(e)})
            continue

    return {"workspaces": [], "probes": probes}


def list_workspaces(client: EnhancedAPIClient) -> List[Dict]:
    """Discover workspaces accessible to the current API key."""
    result = discover_workspaces(client)
    return list(result.get("workspaces", []))


def create_workspace(
    client: EnhancedAPIClient,
    display_name: str,
    tenant_handle: Optional[str] = None,
) -> Dict:
    """Create a new workspace on the destination instance.

    Tries multiple endpoints since the workspace API varies across LangSmith versions.

    Args:
        client: An EnhancedAPIClient instance.
        display_name: Human-readable name for the workspace.
        tenant_handle: Optional URL-safe handle for the workspace.

    Returns:
        The created workspace dict from the API response.
    """
    payload: Dict = {"display_name": display_name}
    if tenant_handle:
        payload["tenant_handle"] = tenant_handle

    endpoints = [f"{_api_prefix(client)}/workspaces"]
    last_error: Optional[Exception] = None
    for endpoint in endpoints:
        try:
            return client.post(endpoint, payload)
        except Exception as e:
            if _endpoint_unsupported(e):
                continue
            last_error = e
            continue

    if last_error:
        raise last_error
    raise NotFoundError("No workspace creation endpoint found", status_code=404)


def get_workspace_name(ws: Dict) -> str:
    """Return a human-readable name for a workspace dict.

    Prefers ``display_name``, falls back to ``name``, then ``tenant_handle``.
    """
    return ws.get("display_name") or ws.get("name") or ws.get("tenant_handle") or str(ws.get("id", "unknown"))


def list_projects(client: EnhancedAPIClient) -> List[Dict]:
    """List all projects (sessions) from an instance.

    Args:
        client: An EnhancedAPIClient instance.

    Returns:
        List of project dicts.
    """
    projects = []
    try:
        for project in client.get_paginated("/sessions", page_size=100):
            if isinstance(project, dict):
                projects.append(project)
    except Exception:  # noqa: S110
        pass
    return projects
