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
        assert payload["session_id"] == "dst-session"
        assert payload["name"] == "High latency on checkout"
        assert payload["severity"] == 1
        assert payload["tags"] == ["latency"]

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
