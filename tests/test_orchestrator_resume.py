"""Targeted resume behavior tests for MigrationOrchestrator."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

from langsmith_migrator.core.migrators.orchestrator import MigrationOrchestrator
from langsmith_migrator.utils.state import (
    MigrationItem,
    MigrationStatus,
    ResolutionOutcome,
    StateManager,
)


class _FakeClient:
    def __init__(self) -> None:
        self.session = SimpleNamespace(headers={})

    def set_workspace(self, workspace_id: str | None) -> None:
        if workspace_id is None:
            self.session.headers.pop("X-Tenant-Id", None)
        else:
            self.session.headers["X-Tenant-Id"] = workspace_id

    def close(self) -> None:
        return None


class _FakeConsole:
    def __init__(self) -> None:
        self.text = ""

    def print(self, *args, end="\n", **kwargs) -> None:
        self.text += "".join(str(arg) for arg in args) + end


def test_resume_items_continues_non_user_items_when_dest_org_prefetch_fails(
    monkeypatch, sample_config, migration_state, tmp_path
):
    """Resume should continue non-user items even if ws-member dest-org prefetch fails."""
    clients = [_FakeClient(), _FakeClient()]
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.orchestrator.EnhancedAPIClient",
        lambda **kwargs: clients.pop(0),
    )

    # Stub migrator constructors used in resume_items setup.
    prompt_migrator = Mock()
    prompt_migrator.migrate_prompt.return_value = True
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.prompt.PromptMigrator",
        lambda *args, **kwargs: prompt_migrator,
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.annotation_queue.AnnotationQueueMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.rules.RulesMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.chart.ChartMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.orchestrator.ExperimentMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.orchestrator.FeedbackMigrator",
        lambda *args, **kwargs: Mock(),
    )

    user_role_migrator = Mock()
    user_role_migrator._dest_email_to_identity = {}
    user_role_migrator.list_dest_org_members.side_effect = Exception("dest lookup unavailable")
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.user_role.UserRoleMigrator",
        lambda *args, **kwargs: user_role_migrator,
    )

    state_manager = StateManager(tmp_path / "state")
    orchestrator = MigrationOrchestrator(sample_config, state_manager)
    orchestrator.state = migration_state
    orchestrator.state.id_mappings["roles"] = {"src-role": "dst-role"}

    ws_item = MigrationItem(
        id="ws_member_ws-src_alice@example.com",
        type="ws_member",
        name="alice@example.com",
        source_id="alice@example.com",
        status=MigrationStatus.PENDING,
        metadata={
            "member": {"id": "src-ws-1", "email": "alice@example.com", "role_id": "src-role"}
        },
        workspace_pair={"source": "ws-src", "dest": "ws-dst"},
    )
    prompt_item = MigrationItem(
        id="prompt_default_team_prompt-a",
        type="prompt",
        name="team/prompt-a",
        source_id="team/prompt-a",
        status=MigrationStatus.PENDING,
        metadata={"include_all_commits": True},
    )

    migration_state.add_item(ws_item)
    migration_state.add_item(prompt_item)

    results = orchestrator.resume_items([ws_item, prompt_item])

    assert "ws_member:alice@example.com" in results["blocked"]
    prompt_migrator.migrate_prompt.assert_called_once_with(
        "team/prompt-a",
        include_all_commits=True,
    )


def test_resume_items_forwards_chart_same_instance_metadata(
    monkeypatch, sample_config, migration_state, tmp_path
):
    """Resume should preserve same-instance chart metadata when retrying chart items."""

    sample_config.source.base_url = "https://api.smith.langchain.com"
    sample_config.destination.base_url = "https://api.smith.langchain.com/api/v1"
    sample_config.destination.api_key = sample_config.source.api_key

    clients = [_FakeClient(), _FakeClient()]
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.orchestrator.EnhancedAPIClient",
        lambda **kwargs: clients.pop(0),
    )

    chart_migrator = Mock()
    chart_migrator.migrate_chart.return_value = "dest-chart-1"
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.chart.ChartMigrator",
        lambda *args, **kwargs: chart_migrator,
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.prompt.PromptMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.annotation_queue.AnnotationQueueMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.rules.RulesMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.orchestrator.ExperimentMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.orchestrator.FeedbackMigrator",
        lambda *args, **kwargs: Mock(),
    )

    state_manager = StateManager(tmp_path / "state")
    orchestrator = MigrationOrchestrator(sample_config, state_manager)
    orchestrator.state = migration_state

    chart_item = MigrationItem(
        id="chart_default_chart-1",
        type="chart",
        name="Chart One",
        source_id="chart-1",
        status=MigrationStatus.PENDING,
        metadata={
            "chart": {"id": "chart-1", "title": "Chart One", "series": []},
            "dest_session_id": "source-session",
            "same_instance": True,
        },
    )
    migration_state.add_item(chart_item)

    results = orchestrator.resume_items([chart_item])

    assert results["resumed"] == ["chart:chart-1"]
    chart_migrator.migrate_chart.assert_called_once_with(
        {"id": "chart-1", "title": "Chart One", "series": []},
        "source-session",
        same_instance=True,
    )


def test_resume_items_re_resolves_chart_when_same_instance_metadata_is_stale(
    monkeypatch, sample_config, migration_state, tmp_path
):
    """Stale same-instance chart resume metadata should be repaired before replay."""

    sample_config.source.base_url = "https://api.smith.langchain.com"
    sample_config.destination.base_url = "https://api.smith.langchain.com"
    sample_config.source.api_key = "source-workspace-key"
    sample_config.destination.api_key = "dest-workspace-key"
    migration_state.id_mappings["project"] = {"source-session": "dest-session"}

    clients = [_FakeClient(), _FakeClient()]
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.orchestrator.EnhancedAPIClient",
        lambda **kwargs: clients.pop(0),
    )

    chart_migrator = Mock()
    chart_migrator._extract_session_id.return_value = "source-session"
    chart_migrator.resolve_destination_session_id.return_value = "dest-session"
    chart_migrator.migrate_chart.return_value = "dest-chart-1"
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.chart.ChartMigrator",
        lambda *args, **kwargs: chart_migrator,
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.prompt.PromptMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.annotation_queue.AnnotationQueueMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.rules.RulesMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.orchestrator.ExperimentMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.orchestrator.FeedbackMigrator",
        lambda *args, **kwargs: Mock(),
    )

    state_manager = StateManager(tmp_path / "state")
    orchestrator = MigrationOrchestrator(sample_config, state_manager)
    orchestrator.console = _FakeConsole()
    orchestrator.state = migration_state

    chart_payload = {
        "id": "chart-1",
        "title": "Chart One",
        "project_id": "source-session",
        "series": [],
    }
    chart_item = MigrationItem(
        id="chart_default_chart-1",
        type="chart",
        name="Chart One",
        source_id="chart-1",
        status=MigrationStatus.PENDING,
        metadata={
            "chart": chart_payload,
            "dest_session_id": "source-session",
            "same_instance": True,
        },
    )
    migration_state.add_item(chart_item)

    results = orchestrator.resume_items([chart_item])

    assert results["resumed"] == ["chart:chart-1"]
    assert results["blocked"] == []
    assert "Chart resume context changed" in orchestrator.console.text
    chart_migrator.resolve_destination_session_id.assert_called_once_with(
        "source-session",
        same_instance=False,
    )
    chart_migrator.migrate_chart.assert_called_once_with(
        chart_payload,
        "dest-session",
        same_instance=False,
    )
    assert chart_item.metadata["same_instance"] is False
    assert chart_item.metadata["dest_session_id"] == "dest-session"
    assert chart_item.metadata["previous_same_instance"] is True
    assert chart_item.metadata["previous_dest_session_id"] == "source-session"


def test_resume_items_re_resolves_chart_when_dest_session_metadata_is_missing(
    monkeypatch, sample_config, migration_state, tmp_path
):
    """Resume should repair chart items that checkpointed without a destination session."""

    sample_config.source.base_url = "https://api.smith.langchain.com"
    sample_config.destination.base_url = "https://api.smith.langchain.com"
    sample_config.source.api_key = "source-workspace-key"
    sample_config.destination.api_key = "dest-workspace-key"

    clients = [_FakeClient(), _FakeClient()]
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.orchestrator.EnhancedAPIClient",
        lambda **kwargs: clients.pop(0),
    )

    chart_migrator = Mock()
    chart_migrator._extract_session_id.return_value = "source-session"
    chart_migrator.resolve_destination_session_id.return_value = "dest-session"
    chart_migrator.migrate_chart.return_value = "dest-chart-1"
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.chart.ChartMigrator",
        lambda *args, **kwargs: chart_migrator,
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.prompt.PromptMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.annotation_queue.AnnotationQueueMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.rules.RulesMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.orchestrator.ExperimentMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.orchestrator.FeedbackMigrator",
        lambda *args, **kwargs: Mock(),
    )

    state_manager = StateManager(tmp_path / "state")
    orchestrator = MigrationOrchestrator(sample_config, state_manager)
    orchestrator.console = _FakeConsole()
    orchestrator.state = migration_state

    chart_payload = {
        "id": "chart-1",
        "title": "Chart One",
        "project_id": "source-session",
        "series": [],
    }
    chart_item = MigrationItem(
        id="chart_default_chart-1",
        type="chart",
        name="Chart One",
        source_id="chart-1",
        status=MigrationStatus.PENDING,
        metadata={
            "chart": chart_payload,
            "dest_session_id": None,
            "same_instance": False,
        },
    )
    migration_state.add_item(chart_item)

    results = orchestrator.resume_items([chart_item])

    assert results["resumed"] == ["chart:chart-1"]
    assert results["blocked"] == []
    assert "missing destination session" in orchestrator.console.text
    chart_migrator.resolve_destination_session_id.assert_called_once_with(
        "source-session",
        same_instance=False,
    )
    chart_migrator.migrate_chart.assert_called_once_with(
        chart_payload,
        "dest-session",
        same_instance=False,
    )
    assert chart_item.metadata["same_instance"] is False
    assert chart_item.metadata["dest_session_id"] == "dest-session"
    assert chart_item.metadata["previous_dest_session_id"] is None


def test_resume_items_allows_chart_without_project_dependency_and_no_dest_session(
    monkeypatch, sample_config, migration_state, tmp_path
):
    """Charts without project/session filters do not need destination session metadata."""

    sample_config.source.base_url = "https://api.smith.langchain.com"
    sample_config.destination.base_url = "https://api.smith.langchain.com"
    sample_config.source.api_key = "source-workspace-key"
    sample_config.destination.api_key = "dest-workspace-key"

    clients = [_FakeClient(), _FakeClient()]
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.orchestrator.EnhancedAPIClient",
        lambda **kwargs: clients.pop(0),
    )

    chart_migrator = Mock()
    chart_migrator._extract_session_id.return_value = None
    chart_migrator.resolve_destination_session_id.return_value = None
    chart_migrator.migrate_chart.return_value = "dest-chart-1"
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.chart.ChartMigrator",
        lambda *args, **kwargs: chart_migrator,
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.prompt.PromptMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.annotation_queue.AnnotationQueueMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.rules.RulesMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.orchestrator.ExperimentMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.orchestrator.FeedbackMigrator",
        lambda *args, **kwargs: Mock(),
    )

    state_manager = StateManager(tmp_path / "state")
    orchestrator = MigrationOrchestrator(sample_config, state_manager)
    orchestrator.console = _FakeConsole()
    orchestrator.state = migration_state

    chart_payload = {
        "id": "chart-1",
        "title": "Global Chart",
        "series": [{"filters": {}}],
    }
    chart_item = MigrationItem(
        id="chart_default_chart-1",
        type="chart",
        name="Global Chart",
        source_id="chart-1",
        status=MigrationStatus.PENDING,
        metadata={
            "chart": chart_payload,
            "dest_session_id": None,
            "same_instance": False,
        },
    )
    migration_state.add_item(chart_item)

    results = orchestrator.resume_items([chart_item])

    assert results["resumed"] == ["chart:chart-1"]
    assert results["blocked"] == []
    chart_migrator.resolve_destination_session_id.assert_not_called()
    chart_migrator.migrate_chart.assert_called_once_with(
        chart_payload,
        None,
        same_instance=False,
    )


def test_resume_items_refreshes_stale_chart_mode_without_project_dependency(
    monkeypatch, sample_config, migration_state, tmp_path
):
    """Resume mode can refresh without resolving sessions for global charts."""

    sample_config.source.base_url = "https://api.smith.langchain.com"
    sample_config.destination.base_url = "https://api.smith.langchain.com"
    sample_config.source.api_key = "source-workspace-key"
    sample_config.destination.api_key = "dest-workspace-key"

    clients = [_FakeClient(), _FakeClient()]
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.orchestrator.EnhancedAPIClient",
        lambda **kwargs: clients.pop(0),
    )

    chart_migrator = Mock()
    chart_migrator._extract_session_id.return_value = None
    chart_migrator.resolve_destination_session_id.return_value = None
    chart_migrator.migrate_chart.return_value = "dest-chart-1"
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.chart.ChartMigrator",
        lambda *args, **kwargs: chart_migrator,
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.prompt.PromptMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.annotation_queue.AnnotationQueueMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.rules.RulesMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.orchestrator.ExperimentMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.orchestrator.FeedbackMigrator",
        lambda *args, **kwargs: Mock(),
    )

    state_manager = StateManager(tmp_path / "state")
    orchestrator = MigrationOrchestrator(sample_config, state_manager)
    orchestrator.console = _FakeConsole()
    orchestrator.state = migration_state

    chart_payload = {
        "id": "chart-1",
        "title": "Global Chart",
        "series": [{"filters": {}}],
    }
    chart_item = MigrationItem(
        id="chart_default_chart-1",
        type="chart",
        name="Global Chart",
        source_id="chart-1",
        status=MigrationStatus.PENDING,
        metadata={
            "chart": chart_payload,
            "dest_session_id": None,
            "same_instance": True,
        },
    )
    migration_state.add_item(chart_item)

    results = orchestrator.resume_items([chart_item])

    assert results["resumed"] == ["chart:chart-1"]
    assert results["blocked"] == []
    assert "no project/session dependency" in orchestrator.console.text
    chart_migrator.resolve_destination_session_id.assert_not_called()
    chart_migrator.migrate_chart.assert_called_once_with(
        chart_payload,
        None,
        same_instance=False,
    )
    assert chart_item.metadata["same_instance"] is False
    assert chart_item.metadata["dest_session_id"] is None
    assert chart_item.metadata["previous_same_instance"] is True
    assert chart_item.metadata["previous_dest_session_id"] is None


def test_resume_items_blocks_chart_when_stale_context_cannot_resolve_destination(
    monkeypatch, sample_config, migration_state, tmp_path
):
    """Stale chart resume metadata should checkpoint if remap resolution is unsafe."""

    sample_config.source.base_url = "https://api.smith.langchain.com"
    sample_config.destination.base_url = "https://api.smith.langchain.com"
    sample_config.source.api_key = "source-workspace-key"
    sample_config.destination.api_key = "dest-workspace-key"

    clients = [_FakeClient(), _FakeClient()]
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.orchestrator.EnhancedAPIClient",
        lambda **kwargs: clients.pop(0),
    )

    chart_migrator = Mock()
    chart_migrator._extract_session_id.return_value = "source-session"
    chart_migrator.resolve_destination_session_id.return_value = None
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.chart.ChartMigrator",
        lambda *args, **kwargs: chart_migrator,
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.prompt.PromptMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.annotation_queue.AnnotationQueueMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.rules.RulesMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.orchestrator.ExperimentMigrator",
        lambda *args, **kwargs: Mock(),
    )
    monkeypatch.setattr(
        "langsmith_migrator.core.migrators.orchestrator.FeedbackMigrator",
        lambda *args, **kwargs: Mock(),
    )

    state_manager = StateManager(tmp_path / "state")
    orchestrator = MigrationOrchestrator(sample_config, state_manager)
    orchestrator.console = _FakeConsole()
    orchestrator.state = migration_state

    chart_item = MigrationItem(
        id="chart_default_chart-1",
        type="chart",
        name="Chart One",
        source_id="chart-1",
        status=MigrationStatus.PENDING,
        metadata={
            "chart": {
                "id": "chart-1",
                "title": "Chart One",
                "project_id": "source-session",
                "series": [],
            },
            "dest_session_id": "source-session",
            "same_instance": True,
        },
    )
    migration_state.add_item(chart_item)

    results = orchestrator.resume_items([chart_item])

    assert results["resumed"] == []
    assert results["blocked"] == ["chart:chart-1"]
    assert "Cannot resume chart" in orchestrator.console.text
    chart_migrator.migrate_chart.assert_not_called()
    assert chart_item.terminal_state == ResolutionOutcome.BLOCKED_WITH_CHECKPOINT.value
    assert chart_item.outcome_code == "chart_resume_context_changed"
    assert "fresh `langsmith-migrator charts` run" in chart_item.next_action
