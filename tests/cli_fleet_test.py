"""Tests for the fleet CLI command skip-flag behavior and orchestrated flow."""

import contextlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from langsmith_migrator.cli import main as cli_main


class _DummyProgress:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        return False

    def add_task(self, *args, **kwargs):
        return 1

    def advance(self, task_id):
        return None


# ---------------------------------------------------------------------------
# Fake migrators that record calls and return predictable IDs
# ---------------------------------------------------------------------------


class _FakeFleetSecretMigrator:
    create_calls: list = []

    def __init__(self, source_client, dest_client, state, config):
        pass

    def list_secrets(self):
        return [{"name": "OPENAI_API_KEY", "set": True}]

    def list_dest_secrets(self):
        return []

    def create_secret_placeholder(self, name):
        _FakeFleetSecretMigrator.create_calls.append(name)
        return True


class _FakeFleetAuthProviderMigrator:
    create_calls: list = []

    def __init__(self, source_client, dest_client, state, config):
        pass

    def list_providers(self):
        return [{"provider_slug": "google", "owner": "workspace", "name": "Google"}]

    def create_provider(self, provider, dest_base_url=None):
        slug = provider.get("provider_slug", "")
        _FakeFleetAuthProviderMigrator.create_calls.append(slug)
        return f"dest-{slug}"


class _FakeFleetSandboxPolicyMigrator:
    create_calls: list = []

    def __init__(self, source_client, dest_client, state, config):
        pass

    def list_policies(self):
        return [{"id": "policy-1", "name": "Default Sandbox"}]

    def create_policy(self, policy):
        _FakeFleetSandboxPolicyMigrator.create_calls.append(policy.get("id"))
        return "dest-policy-1"


class _FakeFleetMcpServerMigrator:
    mcp_create_calls: list = []
    integration_create_calls: list = []

    def __init__(self, source_client, dest_client, state, config):
        pass

    def list_mcp_servers(self):
        return [{"id": "mcp-1", "name": "Custom MCP", "url": "https://old/mcp", "auth_type": "headers"}]

    def list_integrations(self):
        return [{"id": "int-1", "name": "Custom Int", "owner": "workspace", "url": "https://old/int"}]

    def create_mcp_server(self, server, oauth_provider_id_map=None):
        _FakeFleetMcpServerMigrator.mcp_create_calls.append(server.get("id"))
        return "dest-mcp-1"

    def create_integration(self, integration):
        _FakeFleetMcpServerMigrator.integration_create_calls.append(integration.get("id"))
        return "dest-int-1"


class _FakeFleetSkillMigrator:
    create_calls: list = []

    def __init__(self, source_client, dest_client, state, config):
        pass

    def list_skills(self):
        return [{"id": "web-research", "name": "web-research", "files": {"SKILL.md": {"content": "# web"}}}]

    def get_skill(self, skill_id):
        return {"id": skill_id, "name": "web-research", "files": {"SKILL.md": {"content": "# web"}}}

    def create_skill(self, skill):
        _FakeFleetSkillMigrator.create_calls.append(skill.get("id"))
        return "dest-web-research"


class _FakeFleetAgentMigrator:
    create_calls: list = []
    id_mappings_received: dict = {}

    def __init__(self, source_client, dest_client, state, config):
        pass

    def list_agents(self):
        return [{"id": "agent-1", "name": "Test Agent"}]

    def list_dest_models(self):
        return ["anthropic:claude-sonnet-4-6"]

    def get_agent(self, agent_id):
        return {
            "id": agent_id,
            "name": "Test Agent",
            "system_prompt": "You are helpful.",
            "tools": {"tools": [{"name": "search", "mcp_server_url": "https://old/mcp"}]},
            "files": {"skills/web-research": {"type": "skill", "repo_handle": "web-research"}},
        }

    def create_agent(self, agent, id_mappings, skip_skills=False, skip_mcp_servers=False, dest_model_ids=None, dest_user_ids=None):
        _FakeFleetAgentMigrator.create_calls.append(agent.get("id"))
        _FakeFleetAgentMigrator.id_mappings_received = dict(id_mappings)
        return "dest-agent-1"


class _FakeFleetScheduleMigrator:
    create_calls: list = []

    def __init__(self, source_client, dest_client, state, config):
        pass

    def list_schedules(self, agent_id):
        return [{"id": "sched-1", "cron": "0 9 * * *", "display_name": "Daily"}]

    def create_schedule(self, dest_agent_id, schedule):
        _FakeFleetScheduleMigrator.create_calls.append((dest_agent_id, schedule.get("id")))
        return "dest-sched-1"


class _FakeFleetTriggerMigrator:
    create_calls: list = []

    def __init__(self, source_client, dest_client, state, config):
        pass

    def list_triggers(self):
        return [{"id": "trigger-1", "agent_id": "agent-1", "template_id": "tpl-1", "config": {}}]

    def create_trigger(self, trigger, agent_id_map):
        _FakeFleetTriggerMigrator.create_calls.append((trigger.get("id"), agent_id_map))
        return "dest-trigger-1"


class _FakeFleetWebhookMigrator:
    create_calls: list = []

    def __init__(self, source_client, dest_client, state, config):
        pass

    def list_webhooks(self):
        return [{"id": "webhook-1", "name": "My Webhook", "url": "https://hook.example.com"}]

    def create_webhook(self, webhook):
        _FakeFleetWebhookMigrator.create_calls.append(webhook.get("id"))
        return "dest-webhook-1"


class _FakeFleetUsageLimitMigrator:
    create_calls: list = []

    def __init__(self, source_client, dest_client, state, config):
        pass

    def list_limits(self):
        return [{"id": "limit-1", "subject_type": "agent", "subject_id": "agent-1", "limit_usd": 50.0}]

    def create_limit(self, limit, agent_id_map, user_id_map=None):
        _FakeFleetUsageLimitMigrator.create_calls.append((limit.get("id"), agent_id_map))
        return "dest-limit-1"


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


class _FakeState:
    def __init__(self):
        self.session_id = "test-session"
        self.remediation_bundle_path = None
        self.id_mappings = {}
        self.mark_terminal_calls: list = []

    def ensure_item(self, *args, **kwargs):
        return None

    def update_item_status(self, *args, **kwargs):
        return None

    def update_item_checkpoint(self, *args, **kwargs):
        return None

    def get_item(self, *args, **kwargs):
        return None

    def mark_terminal(self, *args, **kwargs):
        # code is the 3rd positional arg (after item_id and ResolutionOutcome)
        call = dict(kwargs)
        if len(args) >= 3:
            call["code"] = args[2]
        if len(args) >= 2:
            call["outcome"] = str(args[1])
        self.mark_terminal_calls.append(call)
        return None

    def set_mapped_id(self, item_type, source_id, dest_id):
        if item_type not in self.id_mappings:
            self.id_mappings[item_type] = {}
        self.id_mappings[item_type][source_id] = dest_id

    def record_inventory(self, *args, **kwargs):
        return None

    def record_capability(self, *args, **kwargs):
        return None


class _FakeStateManager:
    def __init__(self):
        self.current_state = None

    def create_session(self, source_url, destination_url):
        return _FakeState()

    def _default_bundle_path(self, session_id):
        return Path(f"/tmp/{session_id}")

    def save(self):
        return None


def _make_config():
    return SimpleNamespace(
        migration=SimpleNamespace(
            verbose=False,
            non_interactive=False,
            dry_run=False,
            skip_existing=False,
        ),
        source=SimpleNamespace(
            base_url="https://source.example",
            api_key="src-key",
            verify_ssl=True,
            timeout=30,
            max_retries=3,
        ),
        destination=SimpleNamespace(
            base_url="https://dest.example",
            api_key="dest-key",
            verify_ssl=True,
            timeout=30,
            max_retries=3,
        ),
        state_manager=None,
    )


def _make_orchestrator():
    dest_client = SimpleNamespace(
        session=SimpleNamespace(headers={}),
        get_paginated=lambda *a, **kw: iter([{"ls_user_id": "user-1"}, {"ls_user_id": "user-2"}]),
    )
    return SimpleNamespace(
        source_client=SimpleNamespace(session=SimpleNamespace(headers={})),
        dest_client=dest_client,
        state=None,
        state_manager=_FakeStateManager(),
        workspace_pair=lambda: {"source": None, "dest": None},
    )


_ALL_PATCHES = [
    ("langsmith_migrator.cli.main.FleetSecretMigrator", _FakeFleetSecretMigrator),
    ("langsmith_migrator.cli.main.FleetAuthProviderMigrator", _FakeFleetAuthProviderMigrator),
    ("langsmith_migrator.cli.main.FleetSandboxPolicyMigrator", _FakeFleetSandboxPolicyMigrator),
    ("langsmith_migrator.cli.main.FleetMcpServerMigrator", _FakeFleetMcpServerMigrator),
    ("langsmith_migrator.cli.main.FleetSkillMigrator", _FakeFleetSkillMigrator),
    ("langsmith_migrator.cli.main.FleetAgentMigrator", _FakeFleetAgentMigrator),
    ("langsmith_migrator.cli.main.FleetScheduleMigrator", _FakeFleetScheduleMigrator),
    ("langsmith_migrator.cli.main.FleetTriggerMigrator", _FakeFleetTriggerMigrator),
    ("langsmith_migrator.cli.main.FleetWebhookMigrator", _FakeFleetWebhookMigrator),
    ("langsmith_migrator.cli.main.FleetUsageLimitMigrator", _FakeFleetUsageLimitMigrator),
    ("langsmith_migrator.cli.main.Progress", _DummyProgress),
]


def _reset_fake_calls():
    _FakeFleetSecretMigrator.create_calls = []
    _FakeFleetAuthProviderMigrator.create_calls = []
    _FakeFleetSandboxPolicyMigrator.create_calls = []
    _FakeFleetMcpServerMigrator.mcp_create_calls = []
    _FakeFleetMcpServerMigrator.integration_create_calls = []
    _FakeFleetSkillMigrator.create_calls = []
    _FakeFleetAgentMigrator.create_calls = []
    _FakeFleetAgentMigrator.id_mappings_received = {}
    _FakeFleetScheduleMigrator.create_calls = []
    _FakeFleetTriggerMigrator.create_calls = []
    _FakeFleetWebhookMigrator.create_calls = []
    _FakeFleetUsageLimitMigrator.create_calls = []


def _patch_all():
    """Context manager that patches all Fleet migrator classes."""
    stack = contextlib.ExitStack()
    for target, fake in _ALL_PATCHES:
        stack.enter_context(patch(target, fake))
    return stack


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_fleet_full_flow_all_phases_execute():
    """All phases should run in order and pass ID mappings to later phases."""
    _reset_fake_calls()

    with _patch_all():
        orchestrator = _make_orchestrator()
        cli_main._migrate_fleet_for_workspace(
            orchestrator=orchestrator,
            config=_make_config(),
        )

    # Every phase should have been called
    assert len(_FakeFleetSecretMigrator.create_calls) == 1
    assert _FakeFleetSecretMigrator.create_calls[0] == "OPENAI_API_KEY"
    assert len(_FakeFleetAuthProviderMigrator.create_calls) == 1
    assert _FakeFleetAuthProviderMigrator.create_calls[0] == "google"
    assert len(_FakeFleetSandboxPolicyMigrator.create_calls) == 1
    assert len(_FakeFleetMcpServerMigrator.mcp_create_calls) == 1
    assert len(_FakeFleetMcpServerMigrator.integration_create_calls) == 1
    assert len(_FakeFleetSkillMigrator.create_calls) == 1
    assert len(_FakeFleetAgentMigrator.create_calls) == 1
    assert len(_FakeFleetScheduleMigrator.create_calls) == 1
    assert _FakeFleetScheduleMigrator.create_calls[0] == ("dest-agent-1", "sched-1")
    assert len(_FakeFleetTriggerMigrator.create_calls) == 1
    assert len(_FakeFleetWebhookMigrator.create_calls) == 1
    assert len(_FakeFleetUsageLimitMigrator.create_calls) == 1


def test_fleet_id_mappings_flow_between_phases():
    """ID mappings from early phases should be available to the agent migrator."""
    _reset_fake_calls()

    with _patch_all():
        orchestrator = _make_orchestrator()
        cli_main._migrate_fleet_for_workspace(
            orchestrator=orchestrator,
            config=_make_config(),
        )

    received = _FakeFleetAgentMigrator.id_mappings_received
    assert "fleet_skills" in received
    assert received["fleet_skills"].get("web-research") == "dest-web-research"
    assert "fleet_mcp_servers" in received
    assert received["fleet_mcp_servers"].get("mcp-1") == "dest-mcp-1"
    assert "fleet_sandbox_policies" in received
    assert received["fleet_sandbox_policies"].get("policy-1") == "dest-policy-1"
    assert "fleet_auth_providers" in received
    assert received["fleet_auth_providers"].get("google") == "dest-google"


def test_fleet_skip_agents_cascades_to_schedules_triggers_limits():
    """Skipping agents should also skip schedules, triggers, and usage limits."""
    _reset_fake_calls()

    with _patch_all():
        cli_main._migrate_fleet_for_workspace(
            orchestrator=_make_orchestrator(),
            config=_make_config(),
            skip_secrets=True,
            skip_auth_providers=True,
            skip_mcp_servers=True,
            skip_skills=False,
            skip_agents=True,
            skip_schedules=False,
            skip_triggers=False,
            skip_webhooks=False,
            skip_usage_limits=False,
            skip_sandbox_policies=True,
        )

    assert len(_FakeFleetAgentMigrator.create_calls) == 0
    assert len(_FakeFleetScheduleMigrator.create_calls) == 0
    assert len(_FakeFleetTriggerMigrator.create_calls) == 0
    assert len(_FakeFleetUsageLimitMigrator.create_calls) == 0
    # But webhooks (independent of agents) should still run
    assert len(_FakeFleetWebhookMigrator.create_calls) == 1


def test_fleet_skip_skills_still_migrates_agents():
    """Skipping skills should not prevent agent migration."""
    _reset_fake_calls()

    with _patch_all():
        cli_main._migrate_fleet_for_workspace(
            orchestrator=_make_orchestrator(),
            config=_make_config(),
            skip_secrets=True,
            skip_auth_providers=True,
            skip_mcp_servers=True,
            skip_skills=True,
            skip_agents=False,
            skip_schedules=True,
            skip_triggers=True,
            skip_webhooks=True,
            skip_usage_limits=True,
            skip_sandbox_policies=True,
        )

    assert len(_FakeFleetAgentMigrator.create_calls) == 1
    assert len(_FakeFleetSkillMigrator.create_calls) == 0


def test_fleet_skip_mcp_servers_still_migrates_agents():
    """Skipping MCP servers should not prevent agent migration."""
    _reset_fake_calls()

    with _patch_all():
        cli_main._migrate_fleet_for_workspace(
            orchestrator=_make_orchestrator(),
            config=_make_config(),
            skip_secrets=True,
            skip_auth_providers=True,
            skip_mcp_servers=True,
            skip_skills=True,
            skip_agents=False,
            skip_schedules=True,
            skip_triggers=True,
            skip_webhooks=True,
            skip_usage_limits=True,
            skip_sandbox_policies=True,
        )

    assert len(_FakeFleetAgentMigrator.create_calls) == 1
    assert len(_FakeFleetMcpServerMigrator.mcp_create_calls) == 0


def test_fleet_skip_all_resource_types():
    """Skipping everything should not error and should create nothing."""
    _reset_fake_calls()

    with _patch_all():
        cli_main._migrate_fleet_for_workspace(
            orchestrator=_make_orchestrator(),
            config=_make_config(),
            skip_secrets=True,
            skip_auth_providers=True,
            skip_mcp_servers=True,
            skip_skills=True,
            skip_agents=True,
            skip_schedules=True,
            skip_triggers=True,
            skip_webhooks=True,
            skip_usage_limits=True,
            skip_sandbox_policies=True,
        )

    assert len(_FakeFleetSecretMigrator.create_calls) == 0
    assert len(_FakeFleetAgentMigrator.create_calls) == 0
    assert len(_FakeFleetWebhookMigrator.create_calls) == 0


def test_fleet_webhooks_independent_of_agent_skip():
    """Webhooks should be migrated even when agents are skipped."""
    _reset_fake_calls()

    with _patch_all():
        cli_main._migrate_fleet_for_workspace(
            orchestrator=_make_orchestrator(),
            config=_make_config(),
            skip_secrets=True,
            skip_auth_providers=True,
            skip_mcp_servers=True,
            skip_skills=True,
            skip_agents=True,
            skip_schedules=True,
            skip_triggers=True,
            skip_webhooks=False,
            skip_usage_limits=True,
            skip_sandbox_policies=True,
        )

    assert len(_FakeFleetWebhookMigrator.create_calls) == 1
    assert len(_FakeFleetAgentMigrator.create_calls) == 0


def test_fleet_secrets_marked_as_exported_with_manual_apply():
    """Secrets should be marked with EXPORTED_WITH_MANUAL_APPLY in state."""
    _reset_fake_calls()

    with _patch_all():
        state = _FakeState()
        orchestrator = _make_orchestrator()
        orchestrator.state_manager = _FakeStateManager()
        orchestrator.state_manager.create_session = lambda s, d: state

        cli_main._migrate_fleet_for_workspace(
            orchestrator=orchestrator,
            config=_make_config(),
            skip_auth_providers=True,
            skip_mcp_servers=True,
            skip_skills=True,
            skip_agents=True,
            skip_schedules=True,
            skip_triggers=True,
            skip_webhooks=True,
            skip_usage_limits=True,
            skip_sandbox_policies=True,
        )

    secret_marked = any(
        call.get("code") == "secret_value_write_only"
        for call in state.mark_terminal_calls
    )
    assert secret_marked, (
        f"Expected secret to be marked with 'secret_value_write_only', "
        f"got calls: {state.mark_terminal_calls}"
    )


def test_fleet_auth_providers_marked_as_exported_with_manual_apply():
    """Auth providers should be marked with EXPORTED_WITH_MANUAL_APPLY for client_secret."""
    _reset_fake_calls()

    with _patch_all():
        state = _FakeState()
        orchestrator = _make_orchestrator()
        orchestrator.state_manager = _FakeStateManager()
        orchestrator.state_manager.create_session = lambda s, d: state

        cli_main._migrate_fleet_for_workspace(
            orchestrator=orchestrator,
            config=_make_config(),
            skip_secrets=True,
            skip_mcp_servers=True,
            skip_skills=True,
            skip_agents=True,
            skip_schedules=True,
            skip_triggers=True,
            skip_webhooks=True,
            skip_usage_limits=True,
            skip_sandbox_policies=True,
        )

    provider_marked = any(
        call.get("code") == "client_secret_write_only"
        for call in state.mark_terminal_calls
    )
    assert provider_marked, (
        f"Expected auth provider to be marked with 'client_secret_write_only', "
        f"got calls: {state.mark_terminal_calls}"
    )


def test_fleet_trigger_receives_agent_id_map():
    """Triggers should receive the agent ID mapping from the agent phase."""
    _reset_fake_calls()

    with _patch_all():
        cli_main._migrate_fleet_for_workspace(
            orchestrator=_make_orchestrator(),
            config=_make_config(),
            skip_secrets=True,
            skip_auth_providers=True,
            skip_mcp_servers=True,
            skip_skills=True,
            skip_agents=False,
            skip_schedules=True,
            skip_triggers=False,
            skip_webhooks=True,
            skip_usage_limits=True,
            skip_sandbox_policies=True,
        )

    assert len(_FakeFleetTriggerMigrator.create_calls) == 1
    trigger_id, agent_id_map = _FakeFleetTriggerMigrator.create_calls[0]
    assert trigger_id == "trigger-1"
    assert agent_id_map.get("agent-1") == "dest-agent-1"


def test_fleet_usage_limit_receives_agent_id_map():
    """Usage limits should receive the agent ID mapping from the agent phase."""
    _reset_fake_calls()

    with _patch_all():
        cli_main._migrate_fleet_for_workspace(
            orchestrator=_make_orchestrator(),
            config=_make_config(),
            skip_secrets=True,
            skip_auth_providers=True,
            skip_mcp_servers=True,
            skip_skills=True,
            skip_agents=False,
            skip_schedules=True,
            skip_triggers=True,
            skip_webhooks=True,
            skip_usage_limits=False,
            skip_sandbox_policies=True,
        )

    assert len(_FakeFleetUsageLimitMigrator.create_calls) == 1
    limit_id, agent_id_map = _FakeFleetUsageLimitMigrator.create_calls[0]
    assert limit_id == "limit-1"
    assert agent_id_map.get("agent-1") == "dest-agent-1"
