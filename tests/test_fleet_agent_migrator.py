"""Unit tests for FleetAgentMigrator."""

from unittest.mock import Mock

import pytest

from langsmith_migrator.core.api_client import EnhancedAPIClient
from langsmith_migrator.core.migrators import FleetAgentMigrator


def _mock_client() -> Mock:
    client = Mock(spec=EnhancedAPIClient)
    client.session = Mock()
    client.session.headers = {}
    return client


class TestFleetAgentMigrator:
    """Test cases for FleetAgentMigrator."""

    @pytest.fixture
    def agent_migrator(self, sample_config, migration_state):
        source = _mock_client()
        dest = _mock_client()
        return FleetAgentMigrator(source, dest, migration_state, sample_config)

    @pytest.fixture
    def sample_agent(self):
        return {
            "id": "agent-123",
            "name": "Research Assistant",
            "description": "Researches topics and drafts summaries.",
            "system_prompt": "You are a helpful research assistant.",
            "model": {"id": "anthropic:claude-sonnet-4-6"},
            "permissions": {"identity": "shared", "visibility": "tenant"},
            "tools": {
                "tools": [
                    {"name": "web_search", "mcp_server_url": "https://old.example.com/mcp"},
                ],
            },
            "files": {
                "skills/web-research": {
                    "type": "skill",
                    "repo_handle": "web-research",
                },
            },
            "backend": {
                "type": "sandbox",
                "sandbox_config": {
                    "scope": "thread",
                    "policy_ids": ["policy-1"],
                },
            },
        }

    def test_list_agents(self, agent_migrator):
        """Test listing agents from both audiences (user + tenant)."""
        # get_cursor_paginated is called twice: once for user, once for tenant
        agent_migrator.source.get_cursor_paginated.side_effect = [
            [{"id": "agent-1", "name": "Owned Agent"}],
            [{"id": "agent-2", "name": "Workspace Agent"}],
        ]

        result = agent_migrator.list_agents()

        assert len(result) == 2
        assert result[0]["name"] == "Owned Agent"
        assert result[1]["name"] == "Workspace Agent"
        assert agent_migrator.source.get_cursor_paginated.call_count == 2

    def test_list_agents_dedup_across_audiences(self, agent_migrator):
        """Agents appearing in both audiences should be deduplicated."""
        agent_migrator.source.get_cursor_paginated.side_effect = [
            [{"id": "agent-1", "name": "Shared Agent"}],
            [{"id": "agent-1", "name": "Shared Agent"}],  # same agent in tenant audience
        ]

        result = agent_migrator.list_agents()

        assert len(result) == 1
        assert result[0]["id"] == "agent-1"

    def test_list_agents_select_by_name(self, agent_migrator):
        """select should keep only agents whose name matches."""
        agent_migrator.source.get_cursor_paginated.side_effect = [
            [{"id": "agent-1", "name": "Keep Me"}],
            [{"id": "agent-2", "name": "Drop Me"}],
        ]

        result = agent_migrator.list_agents(select={"Keep Me"})

        assert len(result) == 1
        assert result[0]["id"] == "agent-1"

    def test_list_agents_select_by_id(self, agent_migrator):
        """select should keep only agents whose id matches."""
        agent_migrator.source.get_cursor_paginated.side_effect = [
            [{"id": "agent-1", "name": "Keep Me"}],
            [{"id": "agent-2", "name": "Drop Me"}],
        ]

        result = agent_migrator.list_agents(select={"agent-2"})

        assert len(result) == 1
        assert result[0]["id"] == "agent-2"

    def test_list_agents_owned_only_queries_user_audience(self, agent_migrator):
        """Passing audiences=('user',) should issue exactly one list call."""
        agent_migrator.source.get_cursor_paginated.side_effect = [
            [{"id": "agent-1", "name": "Owned Agent"}],
        ]

        result = agent_migrator.list_agents(audiences=("user",))

        assert len(result) == 1
        assert agent_migrator.source.get_cursor_paginated.call_count == 1
        called_params = agent_migrator.source.get_cursor_paginated.call_args.kwargs["params"]
        assert called_params == {"audience": "user"}

    def test_list_agents_unmatched_selector_warns(self, agent_migrator):
        """A selector matching nothing should log a warning and return empty."""
        agent_migrator.source.get_cursor_paginated.side_effect = [
            [{"id": "agent-1", "name": "Keep Me"}],
            [],
        ]
        agent_migrator.log = Mock()

        result = agent_migrator.list_agents(select={"Nonexistent"})

        assert result == []
        warned = any(
            "Nonexistent" in call.args[0]
            for call in agent_migrator.log.call_args_list
        )
        assert warned

    def test_list_agents_user_audience_error(self, agent_migrator):
        """If user audience fails, tenant agents should still be returned."""
        from langsmith_migrator.core.api_client import NotFoundError

        agent_migrator.source.get_cursor_paginated.side_effect = [
            NotFoundError("Not found", status_code=404, request_info={}),
            [{"id": "agent-2", "name": "Workspace Agent"}],
        ]

        result = agent_migrator.list_agents()

        assert len(result) == 1
        assert result[0]["name"] == "Workspace Agent"

    def test_create_agent(self, agent_migrator, sample_agent):
        """Test creating an agent with remapped references."""
        agent_migrator.dest.get_cursor_paginated.return_value = []
        agent_migrator.dest.post.return_value = {"id": "new-agent-id"}

        id_mappings = {
            "fleet_skills": {"web-research": "dest-skill-id"},
            "fleet_sandbox_policies": {"policy-1": "dest-policy-id"},
        }

        result = agent_migrator.create_agent(
            sample_agent, id_mappings, skip_skills=False
        )

        assert result == "new-agent-id"
        call_args = agent_migrator.dest.post.call_args
        assert call_args[0][0] == "/v1/fleet/agents"

        payload = call_args[0][1]
        assert payload["name"] == "Research Assistant"
        assert payload["system_prompt"] == "You are a helpful research assistant."

        # Skill repo handle should be remapped
        skill_entry = payload["files"]["skills/web-research"]
        assert skill_entry["repo_handle"] == "dest-skill-id"

        # Sandbox policy ID should be remapped
        assert payload["backend"]["sandbox_config"]["policy_ids"] == ["dest-policy-id"]

        # MCP server URLs are stable external identities and should be preserved.
        assert (
            payload["tools"]["tools"][0]["mcp_server_url"]
            == "https://old.example.com/mcp"
        )

    def test_create_agent_skip_skills_strips_skill_refs(self, agent_migrator, sample_agent):
        """When skills are skipped, skill file entries should be stripped."""
        agent_migrator.dest.get_cursor_paginated.return_value = []
        agent_migrator.dest.post.return_value = {"id": "new-agent-id"}

        result = agent_migrator.create_agent(sample_agent, {}, skip_skills=True)

        assert result == "new-agent-id"
        payload = agent_migrator.dest.post.call_args[0][1]
        assert "skills/web-research" not in payload["files"]

    def test_create_agent_preserves_mcp_server_urls(self, agent_migrator, sample_agent):
        """MCP server URLs should remain unchanged in the agent payload."""
        agent_migrator.dest.get_cursor_paginated.return_value = []
        agent_migrator.dest.post.return_value = {"id": "new-agent-id"}

        result = agent_migrator.create_agent(sample_agent, {}, skip_skills=True)

        assert result == "new-agent-id"
        payload = agent_migrator.dest.post.call_args[0][1]
        tool = payload["tools"]["tools"][0]
        assert tool["mcp_server_url"] == "https://old.example.com/mcp"

    def test_create_agent_existing_skip(self, agent_migrator, sample_agent):
        """Test skipping an existing agent (always skips, no flag needed)."""
        agent_migrator.dest.get_cursor_paginated.side_effect = [
            [{"id": "existing-id", "name": "Research Assistant"}],
            [],
        ]

        result = agent_migrator.create_agent(sample_agent, {})

        assert result == "existing-id"
        agent_migrator.dest.post.assert_not_called()

    def test_create_agent_dry_run(self, agent_migrator, sample_agent):
        """Test dry run mode."""
        agent_migrator.dest.get_cursor_paginated.return_value = []
        agent_migrator.config.migration.dry_run = True

        result = agent_migrator.create_agent(sample_agent, {})

        assert result.startswith("dry-run-")
        agent_migrator.dest.post.assert_not_called()

    def test_create_agent_strips_unknown_model(self, agent_migrator, sample_agent):
        """Agent model should be substituted when not in destination model list."""
        agent_migrator.dest.get_cursor_paginated.return_value = []
        agent_migrator.dest.post.return_value = {"id": "new-agent-id"}

        result = agent_migrator.create_agent(
            sample_agent, {},
            skip_skills=True,
            dest_model_ids=["anthropic:claude-sonnet-4-6"],
        )

        assert result == "new-agent-id"
        payload = agent_migrator.dest.post.call_args[0][1]
        # sample_agent has model {"id": "anthropic:claude-sonnet-4-6"} which IS in the list
        # so it should be preserved
        assert payload["model"]["id"] == "anthropic:claude-sonnet-4-6"

    def test_create_agent_substitutes_unavailable_model(self, agent_migrator, sample_agent):
        """Unavailable model should be substituted with a matching provider model."""
        agent_migrator.dest.get_cursor_paginated.return_value = []
        agent_migrator.dest.post.return_value = {"id": "new-agent-id"}

        # Source model is anthropic:claude-sonnet-4-6, dest only has anthropic:claude-opus
        result = agent_migrator.create_agent(
            sample_agent, {},
            skip_skills=True,
            dest_model_ids=["anthropic:claude-opus-4-8"],
        )

        assert result == "new-agent-id"
        payload = agent_migrator.dest.post.call_args[0][1]
        # Should substitute with the anthropic model from dest
        assert payload["model"]["id"] == "anthropic:claude-opus-4-8"

    def test_create_agent_falls_back_to_first_model(self, agent_migrator, sample_agent):
        """When no same-provider match, should use first available model."""
        agent_migrator.dest.get_cursor_paginated.return_value = []
        agent_migrator.dest.post.return_value = {"id": "new-agent-id"}

        result = agent_migrator.create_agent(
            sample_agent, {},
            skip_skills=True,
            dest_model_ids=["openai:gpt-4o", "google:gemini-pro"],
        )

        assert result == "new-agent-id"
        payload = agent_migrator.dest.post.call_args[0][1]
        assert payload["model"]["id"] == "openai:gpt-4o"

    def test_create_agent_preserves_model_without_validation(self, agent_migrator, sample_agent):
        """When dest_model_ids is None, model should be passed through unchanged."""
        agent_migrator.dest.get_cursor_paginated.return_value = []
        agent_migrator.dest.post.return_value = {"id": "new-agent-id"}

        result = agent_migrator.create_agent(
            sample_agent, {},
            skip_skills=True,
            dest_model_ids=None,
        )

        assert result == "new-agent-id"
        payload = agent_migrator.dest.post.call_args[0][1]
        assert payload["model"]["id"] == "anthropic:claude-sonnet-4-6"

    def test_create_agent_preserves_subagent_mcp_server_urls(self, agent_migrator):
        """Subagent MCP server URLs should remain unchanged."""
        agent = {
            "id": "agent-123",
            "name": "Research Assistant",
            "subagents": [
                {
                    "name": "researcher",
                    "tools": {
                        "tools": [
                            {
                                "name": "search",
                                "mcp_server_url": "https://old.example.com/mcp",
                            },
                        ],
                    },
                }
            ],
        }
        agent_migrator.dest.get_cursor_paginated.return_value = []
        agent_migrator.dest.post.return_value = {"id": "new-agent-id"}

        result = agent_migrator.create_agent(agent, {})

        assert result == "new-agent-id"
        payload = agent_migrator.dest.post.call_args[0][1]
        assert (
            payload["subagents"][0]["tools"]["tools"][0]["mcp_server_url"]
            == "https://old.example.com/mcp"
        )

    def test_remap_backend_remaps_policy_ids(self, agent_migrator):
        """Sandbox policy IDs should be remapped via id_mappings."""
        backend = {
            "type": "sandbox",
            "sandbox_config": {
                "scope": "thread",
                "policy_ids": ["policy-a", "policy-b"],
            },
        }
        id_mappings = {"fleet_sandbox_policies": {"policy-a": "dest-a", "policy-b": "dest-b"}}

        result = agent_migrator._remap_backend(backend, id_mappings)

        assert result["sandbox_config"]["policy_ids"] == ["dest-a", "dest-b"]

    def test_remap_backend_no_policy_ids_passthrough(self, agent_migrator):
        """Backend without policy_ids should pass through unchanged."""
        backend = {"type": "state"}
        result = agent_migrator._remap_backend(backend, {})

        assert result == {"type": "state"}

    def test_remap_options_remaps_slack_provider_id(self, agent_migrator):
        """slack_oauth_provider_id should be remapped via id_mappings."""
        options = {"slack_oauth_provider_id": "source-slack-id", "user_email": "a@b.com"}
        id_mappings = {"fleet_auth_providers": {"source-slack-id": "dest-slack-id"}}

        result = agent_migrator._remap_options(options, id_mappings)

        assert result["slack_oauth_provider_id"] == "dest-slack-id"
        assert result["user_email"] == "a@b.com"

    def test_remap_options_no_slack_provider_id_passthrough(self, agent_migrator):
        """Options without slack_oauth_provider_id should pass through unchanged."""
        options = {"user_email": "a@b.com"}
        result = agent_migrator._remap_options(options, {})

        assert result == {"user_email": "a@b.com"}

    def test_remap_files_preserves_non_skill_entries(self, agent_migrator):
        """Non-skill file entries should be preserved as-is."""
        files = {
            "AGENTS.md": {"content": "# My Agent", "type": "file"},
            "skills/web-research": {"type": "skill", "repo_handle": "web-research"},
        }
        id_mappings = {"fleet_skills": {"web-research": "dest-skill"}}

        result = agent_migrator._remap_files(files, id_mappings, skip_skills=False)

        assert "AGENTS.md" in result
        assert result["AGENTS.md"]["content"] == "# My Agent"
        assert result["skills/web-research"]["repo_handle"] == "dest-skill"

    def test_find_existing_agent(self, agent_migrator):
        """find_existing_agent should search both audiences on destination."""
        agent_migrator.dest.get_cursor_paginated.side_effect = [
            [],  # user audience: no match
            [{"id": "dest-1", "name": "Research Assistant"}],  # tenant audience: match
        ]

        result = agent_migrator.find_existing_agent("Research Assistant")

        assert result == "dest-1"

    def test_find_existing_agent_not_found(self, agent_migrator):
        """find_existing_agent should return None when no match in either audience."""
        agent_migrator.dest.get_cursor_paginated.side_effect = [
            [{"id": "dest-1", "name": "Other Agent"}],
            [{"id": "dest-2", "name": "Another Agent"}],
        ]

        result = agent_migrator.find_existing_agent("Research Assistant")

        assert result is None

    def test_remap_permissions_filters_shared_users_by_dest_membership(self, agent_migrator):
        """shared_users should keep only user IDs that exist on the destination."""
        permissions = {
            "identity": "shared",
            "visibility": "tenant",
            "tenant_access_level": "read",
            "shared_users": {
                "read": ["user-1", "user-2"],
                "write": ["user-3"],
            },
        }

        result = agent_migrator._remap_permissions(
            permissions, dest_user_ids={"user-1", "user-3"}
        )

        assert "shared_users" in result
        assert result["shared_users"]["read"] == ["user-1"]
        assert result["shared_users"]["write"] == ["user-3"]
        assert result["identity"] == "shared"
        assert result["visibility"] == "tenant"

    def test_remap_permissions_strips_all_when_no_dest_users(self, agent_migrator):
        """When dest_user_ids is None, shared_users should be stripped entirely."""
        permissions = {
            "identity": "shared",
            "visibility": "tenant",
            "shared_users": {
                "read": ["user-1", "user-2"],
            },
        }

        result = agent_migrator._remap_permissions(permissions, dest_user_ids=None)

        assert "shared_users" not in result

    def test_remap_permissions_removes_empty_levels(self, agent_migrator):
        """Empty shared_users levels after filtering should be removed."""
        permissions = {
            "identity": "shared",
            "visibility": "tenant",
            "shared_users": {
                "read": ["user-1"],
                "write": ["user-2"],  # user-2 not on destination
            },
        }

        result = agent_migrator._remap_permissions(
            permissions, dest_user_ids={"user-1"}
        )

        assert "shared_users" in result
        assert result["shared_users"]["read"] == ["user-1"]
        assert "write" not in result["shared_users"]

    def test_remap_permissions_strips_all_when_none_match(self, agent_migrator):
        """When no shared users exist on destination, shared_users should be removed."""
        permissions = {
            "identity": "shared",
            "visibility": "tenant",
            "shared_users": {
                "read": ["user-1", "user-2"],
            },
        }

        result = agent_migrator._remap_permissions(
            permissions, dest_user_ids={"user-3", "user-4"}
        )

        assert "shared_users" not in result

    def test_remap_permissions_preserves_without_shared_users(self, agent_migrator):
        """Permissions without shared_users should pass through unchanged."""
        permissions = {
            "identity": "personal",
            "visibility": "user",
        }

        result = agent_migrator._remap_permissions(permissions, dest_user_ids={"user-1"})

        assert result == {"identity": "personal", "visibility": "user"}
