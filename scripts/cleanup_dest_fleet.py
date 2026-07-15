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
# Helpers
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


def headers():
    return {
        "X-Api-Key": API_KEY,
        "X-Tenant-Id": WORKSPACE_ID,
    }


def delete(path, label):
    url = make_url(path)
    if DRY_RUN:
        print(f"  [DRY RUN] Would DELETE {label}: {url}")
        return True
    try:
        resp = requests.delete(url, headers=headers(), timeout=15)
        if resp.status_code in (200, 204, 404):
            print(f"  Deleted {label}")
            return True
        else:
            print(f"  FAILED to delete {label}: {resp.status_code} {resp.text[:200]}")
            return False
    except Exception as e:
        print(f"  ERROR deleting {label}: {e}")
        return False


def get_json(path, params=None):
    url = make_url(path)
    try:
        resp = requests.get(url, headers=headers(), params=params, timeout=30)
        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"  GET {path} returned {resp.status_code}")
            return None
    except Exception as e:
        print(f"  GET {path} failed: {e}")
        return None


def cursor_paginate(path, page_size=100):
    """Paginate through cursor-based Fleet list endpoints."""
    cursor = None
    items = []
    while True:
        params = {"page_size": page_size}
        if cursor:
            params["cursor"] = cursor
        data = get_json(path, params)
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
# Safety check
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
        data = get_json("/v1/fleet/agents", params)
        if not data or not isinstance(data, dict):
            break
        batch = data.get("items", [])
        if not batch:
            break
        for agent in batch:
            aid = agent.get("id")
            name = agent.get("name", "")
            if aid and aid not in all_agent_ids:
                all_agent_ids.add(aid)
                delete(f"/v1/fleet/agents/{aid}", f"agent '{name}' ({aid})")
        cursor = data.get("next_cursor")
        if not cursor:
            break

if not all_agent_ids:
    print("  No agents found")

print()

# ---------------------------------------------------------------------------
# 2. Delete schedules (nested under agents, already deleted above)
# ---------------------------------------------------------------------------

print("=== Schedules (deleted with agents) ===")
print("  Skipped (schedules are deleted when their parent agent is deleted)")

print()

# ---------------------------------------------------------------------------
# 3. Delete triggers
# ---------------------------------------------------------------------------

print("=== Deleting Fleet triggers ===")
triggers = cursor_paginate("/v1/fleet/triggers")
for trigger in triggers:
    tid = trigger.get("id")
    name = trigger.get("name", "")
    if tid:
        delete(f"/v1/fleet/triggers/{tid}", f"trigger '{name}' ({tid})")

if not triggers:
    print("  No triggers found")

print()

# ---------------------------------------------------------------------------
# 4. Delete webhooks
# ---------------------------------------------------------------------------

print("=== Deleting Fleet webhooks ===")
webhooks = cursor_paginate("/v1/platform/fleet-webhooks")
for webhook in webhooks:
    wid = webhook.get("id")
    name = webhook.get("name", "")
    if wid:
        delete(f"/v1/platform/fleet-webhooks/{wid}", f"webhook '{name}' ({wid})")

if not webhooks:
    print("  No webhooks found")

print()

# ---------------------------------------------------------------------------
# 4b. Delete auth providers (workspace-owned only, skip platform/built-in)
# ---------------------------------------------------------------------------

print("=== Deleting Fleet auth providers (workspace-owned) ===")
providers = cursor_paginate("/v1/fleet/auth-providers")
for provider in providers:
    slug = provider.get("provider_slug", "")
    name = provider.get("name", "")
    owner = provider.get("owner", "")
    if owner == "platform":
        print(f"  Skipping platform-owned auth provider '{name}' ({slug})")
        continue
    if slug:
        delete(f"/v1/fleet/auth-providers/{slug}", f"auth provider '{name}' ({slug})")

if not providers:
    print("  No auth providers found")

print()

# ---------------------------------------------------------------------------
# 5. Delete skills
# ---------------------------------------------------------------------------

print("=== Deleting Fleet skills ===")
skills = cursor_paginate("/v1/fleet/skills")
for skill in skills:
    sid = skill.get("id")
    name = skill.get("name", "")
    if sid:
        delete(f"/v1/fleet/skills/{sid}", f"skill '{name}' ({sid})")

if not skills:
    print("  No skills found")

print()

# ---------------------------------------------------------------------------
# 6. Delete MCP servers
# ---------------------------------------------------------------------------

print("=== Deleting Fleet MCP servers ===")
mcp_servers = cursor_paginate("/v1/fleet/mcp-servers")
for server in mcp_servers:
    sid = server.get("id")
    name = server.get("name", "")
    if sid:
        delete(f"/v1/fleet/mcp-servers/{sid}", f"MCP server '{name}' ({sid})")

if not mcp_servers:
    print("  No MCP servers found")

print()

# ---------------------------------------------------------------------------
# 7. Delete integrations (workspace-owned only)
# ---------------------------------------------------------------------------

print("=== Deleting Fleet integrations (workspace-owned) ===")
integrations = cursor_paginate("/v1/fleet/integrations")
for integration in integrations:
    iid = integration.get("id")
    name = integration.get("name", "")
    owner = integration.get("owner", "")
    if owner == "platform":
        print(f"  Skipping platform-owned integration '{name}'")
        continue
    if iid:
        delete(f"/v1/fleet/integrations/{iid}", f"integration '{name}' ({iid})")

if not integrations:
    print("  No integrations found")

print()

# ---------------------------------------------------------------------------
# 8. Delete secrets
# ---------------------------------------------------------------------------

print("=== Deleting Fleet secrets ===")
secrets = cursor_paginate("/v1/fleet/secrets")
for secret in secrets:
    name = secret.get("name", "")
    if name:
        delete(f"/v1/fleet/secrets/{name}", f"secret '{name}'")

if not secrets:
    print("  No secrets found")

print()

# ---------------------------------------------------------------------------
# 9. Delete usage limits
# ---------------------------------------------------------------------------

print("=== Deleting Fleet usage limits ===")
limits = cursor_paginate("/v1/platform/fleet/usage/limits")
for limit in limits:
    lid = limit.get("id")
    if lid:
        delete(f"/v1/platform/fleet/usage/limits/{lid}", f"usage limit ({lid})")

if not limits:
    print("  No usage limits found")

print()

# ---------------------------------------------------------------------------
# 10. Delete sandbox policies
# ---------------------------------------------------------------------------

print("=== Deleting Fleet sandbox policies ===")
policies = cursor_paginate("/v1/platform/fleet/sandboxes/policies")
for policy in policies:
    pid = policy.get("id")
    name = policy.get("name", "")
    if pid:
        delete(f"/v1/platform/fleet/sandboxes/policies/{pid}", f"sandbox policy '{name}' ({pid})")

if not policies:
    print("  No sandbox policies found")

print()
print("=== Cleanup complete ===")
if DRY_RUN:
    print("This was a dry run. Remove --dry-run to actually delete.")
