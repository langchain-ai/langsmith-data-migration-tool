"""Workspace discovery helpers."""

from typing import Dict, List

from ..core.api_client import EnhancedAPIClient, NotFoundError


def list_workspaces(client: EnhancedAPIClient) -> List[Dict]:
    """Discover workspaces accessible to the current API key.

    Tries multiple endpoints since the workspace API varies across LangSmith versions.

    Args:
        client: An EnhancedAPIClient instance.

    Returns:
        List of workspace dicts with at least 'id' and 'display_name'/'name' keys.
        Empty list if workspaces are not supported or none found.
    """
    endpoints = ["/workspaces", "/orgs/current/workspaces"]

    for endpoint in endpoints:
        try:
            response = client.get(endpoint)
            # Response may be a list directly or wrapped in a key
            if isinstance(response, list):
                return response
            if isinstance(response, dict):
                # Common wrapper keys
                for key in ("workspaces", "items", "results"):
                    if key in response and isinstance(response[key], list):
                        return response[key]
                # Single workspace returned as dict
                if "id" in response:
                    return [response]
            return []
        except NotFoundError:
            continue
        except Exception:
            continue

    return []


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
    except Exception:
        pass
    return projects
