#!/usr/bin/env python3
"""Cleanup script: delete Fleet resources from a workspace.

WARNING: This script deletes resources from the workspace specified as
input. Under no circumstances does it touch any other workspace.

Usage:
    uv run python3 scripts/cleanup_dest_fleet.py <WORKSPACE_ID> [--dry-run]

Reads LANGSMITH_NEW_API_KEY and LANGSMITH_NEW_BASE_URL from environment.
"""

import os
import sys

import requests

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

API_KEY = os.environ.get("LANGSMITH_NEW_API_KEY", "")
BASE_URL = os.environ.get("LANGSMITH_NEW_BASE_URL", "")

DRY_RUN = "--dry-run" in sys.argv

# Parse workspace ID from positional args
_args = [a for a in sys.argv[1:] if not a.startswith("--")]
if not _args:
    print("Usage: uv run python3 scripts/cleanup_dest_fleet.py <WORKSPACE_ID> [--dry-run]")
    sys.exit(1)
WORKSPACE_ID = _args[0]


# ---------------------------------------------------------------------------
# HTTP layer
# ---------------------------------------------------------------------------

def make_url(path):
    """Build a full URL. Fleet endpoints are at /v1/ not /api/v1/."""
    base = BASE_URL.rstrip("/")
    if path.startswith("/v1/"):
        from urllib.parse import urlparse

        parsed = urlparse(base)
        return f"{parsed.scheme}://{parsed.netloc}{path}"
    if not base.endswith("/api/v1"):
        base = base + "/api/v1"
    return f"{base}{path}"


def _headers():
    return {
        "X-Api-Key": API_KEY,
        "X-Tenant-Id": WORKSPACE_ID,
    }


def api_delete(path):
    """Delete a resource by path. Returns True on success."""
    url = make_url(path)
    if DRY_RUN:
        return True
    try:
        resp = requests.delete(url, headers=_headers(), timeout=15)
        return resp.status_code in (200, 204, 404)
    except Exception:  # noqa: S110
        pass
        return False


def api_get(path, params=None):
    """GET a JSON response from the API."""
    url = make_url(path)
    try:
        resp = requests.get(url, headers=_headers(), params=params, timeout=30)
        if resp.status_code == 200:
            return resp.json()
    except Exception:  # noqa: S110
        pass
        pass
    return None


def cursor_paginate(path, page_size=100):
    """Paginate through cursor-based Fleet list endpoints."""
    cursor = None
    items = []
    while True:
        params = {"page_size": page_size}
        if cursor:
            params["cursor"] = cursor
        data = api_get(path, params)
        if not data or not isinstance(data, dict):
            break
        batch = data.get("items", [])
        if not batch:
            break
        items.extend(batch)
        cursor = data.get("next_cursor")
        if not cursor:
            break
    return items


# ---------------------------------------------------------------------------
# Safety checks
# ---------------------------------------------------------------------------

if not API_KEY:
    print("ERROR: LANGSMITH_NEW_API_KEY not set")
    sys.exit(1)

if not BASE_URL:
    print("ERROR: LANGSMITH_NEW_BASE_URL not set")
    sys.exit(1)

if "smith.langchain.com" not in BASE_URL:
    print(f"ERROR: Base URL doesn't look like LangSmith: {BASE_URL}")
    sys.exit(1)

print(f"Cleanup target: {BASE_URL}")
print(f"Workspace: {WORKSPACE_ID}")
print(f"Dry run: {DRY_RUN}")
print()


# ---------------------------------------------------------------------------
# Deletion helpers (count-based, no response-derived data in output)
# ---------------------------------------------------------------------------

def delete_count(path, ids):
    """Delete a list of resources by ID. Returns count of successes."""
    ok = 0
    for rid in ids:
        if api_delete(f"{path}/{rid}"):
            ok += 1
    return ok


def delete_by_name(path, names):
    """Delete resources identified by name. Returns count of successes."""
    ok = 0
    for name in names:
        if api_delete(f"{path}/{name}"):
            ok += 1
    return ok


# ---------------------------------------------------------------------------
# 1. Delete agents
# ---------------------------------------------------------------------------

print("=== Deleting Fleet agents ===")
all_agent_ids = set()
for audience in ("user", "tenant"):
    cursor = None
    while True:
        params = {"page_size": 100, "audience": audience}
        if cursor:
            params["cursor"] = cursor
        data = api_get("/v1/fleet/agents", params)
        if not data or not isinstance(data, dict):
            break
        batch = data.get("items", [])
        if not batch:
            break
        for agent in batch:
            aid = agent.get("id")
            if aid and aid not in all_agent_ids:
                all_agent_ids.add(aid)
        cursor = data.get("next_cursor")
        if not cursor:
            break

if all_agent_ids:
    deleted = delete_count("/v1/fleet/agents", all_agent_ids)
    print(f"  {deleted}/{len(all_agent_ids)} deleted")
else:
    print("  No agents found")

print()

# ---------------------------------------------------------------------------
# 2. Delete schedules (nested under agents, deleted with agents)
# ---------------------------------------------------------------------------

print("=== Schedules (deleted with agents) ===")
print("  Skipped (schedules are deleted when their parent agent is deleted)")

print()

# ---------------------------------------------------------------------------
# 3. Delete triggers
# ---------------------------------------------------------------------------

print("=== Deleting Fleet triggers ===")
triggers = cursor_paginate("/v1/fleet/triggers")
trigger_ids = [t.get("id") for t in triggers if t.get("id")]
if trigger_ids:
    deleted = delete_count("/v1/fleet/triggers", trigger_ids)
    print(f"  {deleted}/{len(trigger_ids)} deleted")
else:
    print("  No triggers found")

print()

# ---------------------------------------------------------------------------
# 4. Delete webhooks
# ---------------------------------------------------------------------------

print("=== Deleting Fleet webhooks ===")
webhooks = cursor_paginate("/v1/platform/fleet-webhooks")
webhook_ids = [w.get("id") for w in webhooks if w.get("id")]
if webhook_ids:
    deleted = delete_count("/v1/platform/fleet-webhooks", webhook_ids)
    print(f"  {deleted}/{len(webhook_ids)} deleted")
else:
    print("  No webhooks found")

print()

# ---------------------------------------------------------------------------
# 4b. Delete auth providers (workspace-owned only)
# ---------------------------------------------------------------------------

print("=== Deleting Fleet auth providers (workspace-owned) ===")
providers = cursor_paginate("/v1/fleet/auth-providers")
provider_slugs = [
    p.get("provider_slug") for p in providers
    if p.get("provider_slug") and p.get("owner") != "platform"
]
platform_count = sum(1 for p in providers if p.get("owner") == "platform")
if provider_slugs:
    deleted = delete_by_name("/v1/fleet/auth-providers", provider_slugs)
    print(f"  {deleted}/{len(provider_slugs)} deleted")
    if platform_count:
        print(f"  {platform_count} platform-owned skipped")
elif platform_count:
    print(f"  {platform_count} platform-owned skipped")
else:
    print("  No auth providers found")

print()

# ---------------------------------------------------------------------------
# 5. Delete skills
# ---------------------------------------------------------------------------

print("=== Deleting Fleet skills ===")
skills = cursor_paginate("/v1/fleet/skills")
skill_ids = [s.get("id") for s in skills if s.get("id")]
if skill_ids:
    deleted = delete_count("/v1/fleet/skills", skill_ids)
    print(f"  {deleted}/{len(skill_ids)} deleted")
else:
    print("  No skills found")

print()

# ---------------------------------------------------------------------------
# 6. Delete MCP servers
# ---------------------------------------------------------------------------

print("=== Deleting Fleet MCP servers ===")
mcp_servers = cursor_paginate("/v1/fleet/mcp-servers")
mcp_ids = [s.get("id") for s in mcp_servers if s.get("id")]
if mcp_ids:
    deleted = delete_count("/v1/fleet/mcp-servers", mcp_ids)
    print(f"  {deleted}/{len(mcp_ids)} deleted")
else:
    print("  No MCP servers found")

print()

# ---------------------------------------------------------------------------
# 7. Delete integrations (workspace-owned only)
# ---------------------------------------------------------------------------

print("=== Deleting Fleet integrations (workspace-owned) ===")
integrations = cursor_paginate("/v1/fleet/integrations")
integration_ids = [
    i.get("id") for i in integrations
    if i.get("id") and i.get("owner") != "platform"
]
platform_count = sum(1 for i in integrations if i.get("owner") == "platform")
if integration_ids:
    deleted = delete_count("/v1/fleet/integrations", integration_ids)
    print(f"  {deleted}/{len(integration_ids)} deleted")
    if platform_count:
        print(f"  {platform_count} platform-owned skipped")
elif platform_count:
    print(f"  {platform_count} platform-owned skipped")
else:
    print("  No integrations found")

print()

# ---------------------------------------------------------------------------
# 8. Delete secrets
# ---------------------------------------------------------------------------

print("=== Deleting Fleet secrets ===")
secrets = cursor_paginate("/v1/fleet/secrets")
secret_names = [s.get("name") for s in secrets if s.get("name")]
if secret_names:
    deleted = delete_by_name("/v1/fleet/secrets", secret_names)
    print(f"  {deleted}/{len(secret_names)} deleted")
else:
    print("  No secrets found")

print()

# ---------------------------------------------------------------------------
# 9. Delete usage limits
# ---------------------------------------------------------------------------

print("=== Deleting Fleet usage limits ===")
limits = cursor_paginate("/v1/platform/fleet/usage/limits")
limit_ids = [item.get("id") for item in limits if item.get("id")]
if limit_ids:
    deleted = delete_count("/v1/platform/fleet/usage/limits", limit_ids)
    print(f"  {deleted}/{len(limit_ids)} deleted")
else:
    print("  No usage limits found")

print()

# ---------------------------------------------------------------------------
# 10. Delete sandbox policies
# ---------------------------------------------------------------------------

print("=== Deleting Fleet sandbox policies ===")
policies = cursor_paginate("/v1/platform/fleet/sandboxes/policies")
policy_ids = [p.get("id") for p in policies if p.get("id")]
if policy_ids:
    deleted = delete_count("/v1/platform/fleet/sandboxes/policies", policy_ids)
    print(f"  {deleted}/{len(policy_ids)} deleted")
else:
    print("  No sandbox policies found")

print()
print("=== Cleanup complete ===")
if DRY_RUN:
    print("This was a dry run. Remove --dry-run to actually delete.")
