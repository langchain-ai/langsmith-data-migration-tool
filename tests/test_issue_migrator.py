"""Unit tests for IssueMigrator (engine issues + issues-agent)."""

from unittest.mock import Mock

import pytest

from langsmith_migrator.core.api_client import EnhancedAPIClient
from langsmith_migrator.core.migrators import IssueMigrator


def _mock_client() -> Mock:
    client = Mock(spec=EnhancedAPIClient)
    client.session = Mock()
    client.session.headers = {}
    return client


class TestIssueMigrator:
    """Test cases for IssueMigrator."""

    @pytest.fixture
    def issue_migrator(self, sample_config, migration_state):
        source = _mock_client()
        dest = _mock_client()
        migrator = IssueMigrator(source, dest, migration_state, sample_config)
        # Inject a project map so session_ids resolve without a network call.
        migrator._project_id_map = {"src-session": "dst-session"}
        return migrator

    @pytest.fixture
    def sample_issue(self):
        return {
            "id": "issue-1",
            "session_id": "src-session",
            "name": "High latency on checkout",
            "description": "Requests exceed 5s",
            "severity": 1,
            "status": "watching",
            "tags": ["latency"],
            "actions": ["investigate"],
            "proposed_fix": "Add a retry with backoff on the checkout call.",
            "fix_prompt": "Fix the latency in checkout.",
            "fix_branch": "engine/fix-checkout-latency",
            "fix_pr_number": 42,
            # Linked runs on the source — MUST NOT be sent to the destination.
            "traces": [
                {"run_id": "run-1", "trace_id": "trace-1", "start_time": "2026-01-01T00:00:00Z"},
            ],
        }

    @pytest.fixture
    def sample_agent(self):
        return {
            "id": "agent-1",
            "tenant_id": "tenant-src",
            "tenant_name": "Acme",
            "session_id": "src-session",
            "session_name": "checkout",
            "issue_count": 12,
            "github_repo_url": "https://github.com/acme/app",
            "priorities": ["latency"],
            "cron_enabled": True,
            # Source-instance-only LSD references.
            "latest_thread_id": "thread-src",
            "latest_run_id": "run-src",
            "created_at": "2026-01-01T00:00:00Z",
            "updated_at": "2026-01-02T00:00:00Z",
        }

    # ------------------------------------------------------------------
    # Issues
    # ------------------------------------------------------------------
    def test_build_project_mapping_excludes_experiments(self, issue_migrator):
        """Experiment sessions (reference_dataset_id set) are not mapped/created."""
        issue_migrator._project_id_map = None
        issue_migrator.state = None
        source_sessions = [
            {"id": "src-live", "name": "juji-prod"},
            {"id": "src-exp", "name": "an-experiment", "reference_dataset_id": "ds-1"},
        ]

        issue_migrator.source.get_paginated.return_value = iter(source_sessions)
        issue_migrator.dest.get_paginated.return_value = iter([])
        issue_migrator.dest.post.return_value = {"id": "dst-live", "name": "juji-prod"}

        result = issue_migrator.build_project_mapping(create_missing=True)

        # Only the live project is mapped; the experiment is excluded.
        assert result == {"src-live": "dst-live"}
        # Exactly one project created (the live one, not the experiment).
        assert issue_migrator.dest.post.call_count == 1
        created_payload = issue_migrator.dest.post.call_args[0][1]
        assert created_payload["name"] == "juji-prod"

    def test_map_projects_for_sessions_only_maps_referenced(self, issue_migrator):
        """Only sessions with Engine data are mapped/created, not the workspace."""
        issue_migrator._project_id_map = None
        issue_migrator.state = None
        # Source workspace has 3 projects; only 1 has Engine data.
        source_sessions = [
            {"id": "src-a", "name": "has-engine"},
            {"id": "src-b", "name": "no-engine-1"},
            {"id": "src-c", "name": "no-engine-2"},
        ]

        # source returns the 3 projects; dest returns nothing.
        issue_migrator.source.get_paginated.return_value = iter(source_sessions)
        issue_migrator.dest.get_paginated.return_value = iter([])
        issue_migrator.dest.post.return_value = {"id": "dst-a", "name": "has-engine"}

        result = issue_migrator.map_projects_for_sessions(["src-a"], create_missing=True)

        # Only src-a mapped; src-b/src-c are ignored.
        assert result == {"src-a": "dst-a"}
        # Exactly one project created (has-engine), not all three.
        assert issue_migrator.dest.post.call_count == 1
        assert issue_migrator.dest.post.call_args[0][1]["name"] == "has-engine"

    def test_map_single_project_only_maps_one(self, issue_migrator):
        """Scoped mapping must not create/list the whole workspace."""
        issue_migrator._project_id_map = None
        issue_migrator.state = None
        # Destination has a same-named project already -> map by name, no create.
        issue_migrator.dest.get_paginated.return_value = [
            {"id": "dst-juji", "name": "juji-prod"},
        ]
        source_project = {"id": "src-juji", "name": "juji-prod"}

        result = issue_migrator.map_single_project(source_project, create_missing=True)

        assert result == "dst-juji"
        assert issue_migrator._project_id_map == {"src-juji": "dst-juji"}
        # Only the one project is mapped; no project creation happened.
        issue_migrator.dest.post.assert_not_called()

    def test_map_single_project_creates_when_missing(self, issue_migrator):
        """When the project is absent on the destination it is created (only it)."""
        issue_migrator._project_id_map = None
        issue_migrator.state = None
        issue_migrator.dest.get_paginated.return_value = []  # nothing on dest
        issue_migrator.dest.post.return_value = {"id": "new-juji", "name": "juji-prod"}
        source_project = {"id": "src-juji", "name": "juji-prod"}

        result = issue_migrator.map_single_project(source_project, create_missing=True)

        assert result == "new-juji"
        assert issue_migrator._project_id_map == {"src-juji": "new-juji"}
        # Exactly one project created.
        assert issue_migrator.dest.post.call_count == 1
        assert issue_migrator.dest.post.call_args[0][0] == "/sessions"

    def test_list_issue_agents_scoped_to_session(self, issue_migrator, sample_agent):
        """--session scoping uses the per-session agent endpoint."""
        issue_migrator.source.get.return_value = sample_agent

        result = issue_migrator.list_issue_agents("src-session")

        assert len(result) == 1
        issue_migrator.source.get.assert_called_once_with(
            "/v1/platform/sessions/src-session/issues-agent"
        )
        issue_migrator.source.get_paginated.assert_not_called()

    def test_list_issue_agents_all_when_no_session(self, issue_migrator, sample_agent):
        """Without a session, all agents are listed via pagination."""
        issue_migrator.source.get_paginated.return_value = [sample_agent]

        result = issue_migrator.list_issue_agents()

        assert len(result) == 1
        issue_migrator.source.get_paginated.assert_called_once_with(
            "/v1/platform/issues-agent"
        )

    def test_list_issues_scoped_to_session(self, issue_migrator, sample_issue):
        issue_migrator.source.get_paginated.return_value = [sample_issue]

        result = issue_migrator.list_issues("src-session")

        assert len(result) == 1
        _, kwargs = issue_migrator.source.get_paginated.call_args
        assert issue_migrator.source.get_paginated.call_args[0][0] == "/v1/platform/issues"
        assert kwargs["params"] == {"session_id": "src-session"}

    def test_create_issue_strips_traces_and_remaps_session(
        self, issue_migrator, sample_issue
    ):
        """The traces array must never be sent, and session_id must be remapped."""
        issue_migrator.dest.post.return_value = {"id": "new-issue", "status": "open"}

        result = issue_migrator.create_issue(sample_issue)

        assert result == "new-issue"
        endpoint, payload = issue_migrator.dest.post.call_args[0]
        assert endpoint == "/v1/platform/issues"
        # Run links and Engine-generated advisory actions are never sent.
        assert "traces" not in payload
        assert "actions" not in payload
        # Source-instance GitHub references are not sent.
        assert "fix_branch" not in payload
        assert "fix_pr_number" not in payload
        assert payload["session_id"] == "dst-session"
        assert payload["name"] == "High latency on checkout"
        assert payload["severity"] == 1
        assert payload["tags"] == ["latency"]
        # Self-contained Engine-authored fix content is carried over.
        assert payload["proposed_fix"] == "Add a retry with backoff on the checkout call."
        assert payload["fix_prompt"] == "Fix the latency in checkout."

    def test_create_issue_patches_status_when_differs(self, issue_migrator, sample_issue):
        """Issues are created `open`; source status should be restored via PATCH."""
        issue_migrator.dest.post.return_value = {"id": "new-issue", "status": "open"}

        issue_migrator.create_issue(sample_issue)

        endpoint, payload = issue_migrator.dest.patch.call_args[0]
        assert endpoint == "/v1/platform/issues/new-issue"
        assert payload == {"status": "watching"}

    def test_create_issue_skips_when_project_unmapped(self, issue_migrator, sample_issue):
        """Issues whose project isn't on the destination are skipped, not posted."""
        issue_migrator._project_id_map = {}  # no mapping

        result = issue_migrator.create_issue(sample_issue)

        assert result is None
        issue_migrator.dest.post.assert_not_called()

    def test_create_issue_skips_existing_duplicate(self, issue_migrator, sample_issue):
        """An issue with the same name in the destination project is skipped."""
        issue_migrator.dest.get_paginated.return_value = [
            {"id": "existing-issue", "name": "High latency on checkout"},
        ]

        result = issue_migrator.create_issue(sample_issue)

        assert result == "existing-issue"
        assert issue_migrator.last_issue_skipped_existing is True
        issue_migrator.dest.post.assert_not_called()

    def test_create_issue_creates_when_no_duplicate(self, issue_migrator, sample_issue):
        """A non-duplicate issue is created and not flagged as skipped."""
        issue_migrator.dest.get_paginated.return_value = [
            {"id": "other", "name": "A different issue"},
        ]
        issue_migrator.dest.post.return_value = {"id": "new-issue", "status": "open"}

        result = issue_migrator.create_issue(sample_issue)

        assert result == "new-issue"
        assert issue_migrator.last_issue_skipped_existing is False
        issue_migrator.dest.post.assert_called_once()

    def test_create_issue_dry_run(self, issue_migrator, sample_issue):
        issue_migrator.config.migration.dry_run = True

        result = issue_migrator.create_issue(sample_issue)

        assert result.startswith("dry-run-")
        issue_migrator.dest.post.assert_not_called()

    # ------------------------------------------------------------------
    # Issues agent
    # ------------------------------------------------------------------
    def test_create_issue_agent_strips_source_only_fields(
        self, issue_migrator, sample_agent
    ):
        issue_migrator.dest.get_cursor_paginated.return_value = []
        issue_migrator.dest.post.return_value = {"id": "new-agent"}

        result = issue_migrator.create_issue_agent(sample_agent)

        assert result == "new-agent"
        endpoint, payload = issue_migrator.dest.post.call_args[0]
        assert endpoint == "/v1/platform/sessions/dst-session/issues-agent"
        # Source-instance-only fields are stripped.
        for stripped in (
            "id",
            "tenant_id",
            "tenant_name",
            "session_id",
            "session_name",
            "issue_count",
            "latest_thread_id",
            "latest_run_id",
            "created_at",
            "updated_at",
        ):
            assert stripped not in payload
        # Config that should carry over.
        assert payload["github_repo_url"] == "https://github.com/acme/app"
        assert payload["priorities"] == ["latency"]

    def test_create_issue_agent_skips_when_project_unmapped(
        self, issue_migrator, sample_agent
    ):
        issue_migrator._project_id_map = {}

        result = issue_migrator.create_issue_agent(sample_agent)

        assert result is None
        issue_migrator.dest.post.assert_not_called()

    def test_create_issue_agent_existing_skip(self, issue_migrator, sample_agent):
        issue_migrator.dest.get_cursor_paginated.return_value = [
            {"id": "existing-agent", "session_id": "dst-session"},
        ]

        result = issue_migrator.create_issue_agent(sample_agent)

        assert result == "existing-agent"
        issue_migrator.dest.post.assert_not_called()
