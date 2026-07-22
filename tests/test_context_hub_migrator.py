"""Unit tests for ContextHubMigrator."""

from unittest.mock import Mock, patch

import pytest
from langsmith.schemas import AgentContext, FileEntry, SkillContext, SkillEntry

from langsmith_migrator.core.api_client import EnhancedAPIClient
from langsmith_migrator.core.migrators import ContextHubMigrator


def _mock_client() -> Mock:
    client = Mock(spec=EnhancedAPIClient)
    client.session = Mock()
    client.session.headers = {}
    return client


def _migrator(sample_config, migration_state=None, *, same_instance=False, include_external=False):
    source = _mock_client()
    dest = _mock_client()
    with patch("langsmith_migrator.core.migrators.context_hub.Client"):
        migrator = ContextHubMigrator(
            source,
            dest,
            migration_state,
            sample_config,
            same_instance=same_instance,
            include_external=include_external,
        )
    migrator.source_ls_client = Mock()
    migrator.dest_ls_client = Mock()
    return migrator


def _repo(**overrides):
    """A raw ``/repos`` list item (dict), as the hub endpoint returns."""
    repo = {
        "id": "repo-1",
        "repo_handle": "email-assistant",
        "owner": "-",
        "full_name": "-/email-assistant",
        "description": "Triages email",
        "readme": "# Email",
        "is_public": False,
        "tags": ["email"],
        "num_commits": 3,
        "source": None,
    }
    repo.update(overrides)
    return repo


def _set_repos(migrator, *, agents=None, skills=None):
    """Route migrator.source.get('/repos', ...) by repo_type query param."""

    def fake_get(endpoint, params=None):
        params = params or {}
        rt = params.get("repo_type")
        items = (agents or []) if rt == "agent" else (skills or [])
        return {"repos": list(items)}

    migrator.source.get.side_effect = fake_get


# ---------------------------------------------------------------------------
# Reads
# ---------------------------------------------------------------------------
def test_list_contexts_includes_agents_and_skills(sample_config):
    migrator = _migrator(sample_config)
    _set_repos(
        migrator,
        agents=[_repo(id="a1", repo_handle="email-assistant")],
        skills=[_repo(id="s1", repo_handle="deep-research")],
    )

    result = migrator.list_contexts()

    assert [(c["repo_type"], c["repo_handle"]) for c in result] == [
        ("agent", "email-assistant"),
        ("skill", "deep-research"),
    ]
    # Listing is scoped to private, non-archived repos and excludes external
    # source by default (matching the Context Hub UI).
    first_params = migrator.source.get.call_args_list[0].kwargs["params"]
    assert first_params["is_public"] == "false"
    assert first_params["is_archived"] == "false"
    assert first_params["exclude_source"] == "external"


def test_list_contexts_hides_external_source_by_default(sample_config):
    """Matches the UI: source=external repos are hidden unless include_external."""
    migrator = _migrator(sample_config)
    _set_repos(
        migrator,
        agents=[
            _repo(id="a1", repo_handle="my-agent", source=None),
            _repo(id="a2", repo_handle="internal-agent", source="internal"),
            _repo(id="a3", repo_handle="9eacd66c-uuid", source="external"),
        ],
        skills=[],
    )

    result = migrator.list_contexts(include_agents=True, include_skills=False)

    handles = {c["repo_handle"] for c in result}
    assert handles == {"my-agent", "internal-agent"}
    assert "9eacd66c-uuid" not in handles


def test_list_contexts_include_external_keeps_external(sample_config):
    migrator = _migrator(sample_config, include_external=True)
    _set_repos(
        migrator,
        agents=[
            _repo(id="a1", repo_handle="my-agent", source=None),
            _repo(id="a3", repo_handle="9eacd66c-uuid", source="external"),
        ],
        skills=[],
    )

    result = migrator.list_contexts(include_agents=True, include_skills=False)

    assert {c["repo_handle"] for c in result} == {"my-agent", "9eacd66c-uuid"}
    # exclude_source is not sent when include_external is set.
    assert "exclude_source" not in migrator.source.get.call_args_list[0].kwargs["params"]


def test_list_contexts_agents_only(sample_config):
    migrator = _migrator(sample_config)
    _set_repos(migrator, agents=[_repo(id="a1")], skills=[_repo(id="s1")])

    result = migrator.list_contexts(include_agents=True, include_skills=False)

    assert len(result) == 1
    assert result[0]["repo_type"] == "agent"
    # Only the agent repo_type was queried.
    queried_types = {c.kwargs["params"]["repo_type"] for c in migrator.source.get.call_args_list}
    assert queried_types == {"agent"}


def test_list_contexts_paginates(sample_config):
    migrator = _migrator(sample_config)
    page1 = [_repo(id=f"a{i}", repo_handle=f"agent-{i}") for i in range(100)]
    page2 = [_repo(id="a100", repo_handle="agent-100")]

    calls = {"n": 0}

    def fake_get(endpoint, params=None):
        if params.get("repo_type") != "agent":
            return {"repos": []}
        calls["n"] += 1
        return {"repos": page1 if calls["n"] == 1 else page2}

    migrator.source.get.side_effect = fake_get

    result = migrator.list_contexts(include_agents=True, include_skills=False)

    assert len(result) == 101
    agent_calls = [
        c for c in migrator.source.get.call_args_list if c.kwargs["params"]["repo_type"] == "agent"
    ]
    assert len(agent_calls) == 2
    assert agent_calls[1].kwargs["params"]["offset"] == 100


def test_get_context_files_uses_pull_agent(sample_config):
    migrator = _migrator(sample_config)
    ctx = AgentContext(
        commit_id="00000000-0000-0000-0000-000000000001",
        commit_hash="abc123",
        files={"AGENTS.md": FileEntry(content="hello")},
    )
    migrator.source_ls_client.pull_agent.return_value = ctx

    snapshot = migrator.get_context_files({"repo_type": "agent", "repo_handle": "email-assistant"})

    assert snapshot["commit_hash"] == "abc123"
    assert snapshot["files"]["AGENTS.md"].content == "hello"
    migrator.source_ls_client.pull_agent.assert_called_once_with("email-assistant")


# ---------------------------------------------------------------------------
# Transform: cross-repo link handling
# ---------------------------------------------------------------------------
def test_prepare_files_strips_link_pins_cross_instance(sample_config, migration_state):
    migrator = _migrator(sample_config, migration_state, same_instance=False)
    files = {
        "AGENTS.md": FileEntry(content="hi"),
        "skills/research": SkillEntry(
            repo_handle="deep-research",
            commit_id="00000000-0000-0000-0000-000000000009",
            commit_hash="deadbeef",
        ),
    }

    prepared = migrator._prepare_files(files, item_id="item-1")

    # Inline entry preserved verbatim.
    assert prepared["AGENTS.md"].content == "hi"
    # Link entry keeps its handle but loses the source-instance pin.
    link = prepared["skills/research"]
    assert link.repo_handle == "deep-research"
    assert link.commit_id is None
    assert link.commit_hash is None
    # The downgrade is recorded as an issue.
    assert any(i.code == "context_link_pin_stripped" for i in migration_state.issue_log)


def test_prepare_files_preserves_link_pins_same_instance(sample_config, migration_state):
    migrator = _migrator(sample_config, migration_state, same_instance=True)
    files = {
        "skills/research": SkillEntry(
            repo_handle="deep-research",
            commit_id="00000000-0000-0000-0000-000000000009",
            commit_hash="deadbeef",
        ),
    }

    prepared = migrator._prepare_files(files, item_id="item-1")

    link = prepared["skills/research"]
    assert str(link.commit_id) == "00000000-0000-0000-0000-000000000009"
    assert link.commit_hash == "deadbeef"
    assert not migration_state.issue_log


def test_prepare_files_no_issue_for_unpinned_link(sample_config, migration_state):
    migrator = _migrator(sample_config, migration_state, same_instance=False)
    files = {"skills/research": SkillEntry(repo_handle="deep-research")}

    prepared = migrator._prepare_files(files, item_id="item-1")

    assert prepared["skills/research"].commit_id is None
    # No pin existed, so nothing was stripped and no issue is recorded.
    assert not migration_state.issue_log


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------
def test_create_context_pushes_agent_with_metadata(sample_config):
    migrator = _migrator(sample_config)
    ctx = AgentContext(
        commit_id="00000000-0000-0000-0000-000000000001",
        commit_hash="abc123",
        files={"AGENTS.md": FileEntry(content="hello")},
    )
    migrator.source_ls_client.pull_agent.return_value = ctx
    migrator.dest_ls_client.push_agent.return_value = "https://dest/commits/xyz"

    summary = {
        "repo_type": "agent",
        "repo_handle": "email-assistant",
        "description": "Triages email",
        "readme": "# Email",
        "tags": ["email"],
        "is_public": False,
    }
    url = migrator.create_context(summary)

    assert url == "https://dest/commits/xyz"
    _, kwargs = migrator.dest_ls_client.push_agent.call_args
    assert kwargs["description"] == "Triages email"
    assert kwargs["tags"] == ["email"]
    assert kwargs["is_public"] is False
    assert kwargs["files"]["AGENTS.md"].content == "hello"


def test_create_context_pushes_skill(sample_config):
    migrator = _migrator(sample_config)
    ctx = SkillContext(
        commit_id="00000000-0000-0000-0000-000000000002",
        commit_hash="def456",
        files={"SKILL.md": FileEntry(content="do research")},
    )
    migrator.source_ls_client.pull_skill.return_value = ctx
    migrator.dest_ls_client.push_skill.return_value = "https://dest/commits/skill"

    summary = {
        "repo_type": "skill",
        "repo_handle": "deep-research",
        "description": None,
        "readme": None,
        "tags": [],
        "is_public": None,
    }
    url = migrator.create_context(summary)

    assert url == "https://dest/commits/skill"
    migrator.dest_ls_client.push_skill.assert_called_once()
    # Empty tag list is normalized to None so the SDK does not patch metadata.
    _, kwargs = migrator.dest_ls_client.push_skill.call_args
    assert kwargs["tags"] is None


def test_context_exists_delegates_by_type(sample_config):
    migrator = _migrator(sample_config)
    migrator.dest_ls_client.agent_exists.return_value = True
    migrator.dest_ls_client.skill_exists.return_value = False

    assert migrator.context_exists({"repo_type": "agent", "repo_handle": "a"}) is True
    assert migrator.context_exists({"repo_type": "skill", "repo_handle": "s"}) is False
