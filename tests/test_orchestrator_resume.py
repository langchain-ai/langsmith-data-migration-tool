"""Targeted resume behavior tests for MigrationOrchestrator."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import Mock

from langsmith_migrator.core.migrators.orchestrator import MigrationOrchestrator
from langsmith_migrator.utils.state import MigrationItem, MigrationStatus, StateManager


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
