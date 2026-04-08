"""Functional coverage for the Click CLI surface."""

from __future__ import annotations

import json
from unittest.mock import Mock

from langsmith_migrator.cli import main as cli_main
from langsmith_migrator.cli.tui_workspace_mapper import WorkspaceProjectResult
from langsmith_migrator.utils.state import MigrationItem, MigrationState, MigrationStatus


def build_state(
    session_id: str,
    *,
    id_mappings: dict[str, dict[str, str]] | None = None,
) -> MigrationState:
    """Create a migration state for tests."""

    return MigrationState(
        session_id=session_id,
        started_at=1.0,
        updated_at=2.0,
        source_url="https://source.example",
        destination_url="https://dest.example",
        id_mappings=id_mappings or {},
    )


def save_session(state_manager, state: MigrationState) -> None:
    """Persist a migration state to the test state directory."""

    state_path = state_manager.state_dir / f"{state.session_id}.json"
    state_path.write_text(json.dumps(state.to_dict()), encoding="utf-8")


def test_registered_command_names_match_public_cli_surface():
    """Keep the Click command registry stable."""

    assert sorted(cli_main.cli.commands) == [
        "charts",
        "clean",
        "datasets",
        "list-projects",
        "list_workspaces",
        "migrate-all",
        "prompts",
        "queues",
        "resume",
        "rules",
        "test",
        "users",
    ]


def test_name_mapping_to_id_mapping_ignores_unknown_project_names():
    """Project mapping should only include names present on both sides."""

    result = cli_main._name_mapping_to_id_mapping(
        {"Source A": "Dest A", "Missing": "Dest B"},
        source_projects=[
            {"id": "src-a", "name": "Source A"},
            {"id": "src-b", "name": "Source B"},
        ],
        dest_projects=[
            {"id": "dst-a", "name": "Dest A"},
            {"id": "dst-b", "name": "Dest B"},
        ],
    )

    assert result == {"src-a": "dst-a"}


def test_resolve_workspaces_with_explicit_pair_sets_context(monkeypatch):
    """Providing both workspace IDs should skip discovery and scope the orchestrator."""

    calls = []

    class StubOrchestrator:
        def set_workspace_context(self, source_ws_id, dest_ws_id):
            calls.append((source_ws_id, dest_ws_id))

    class SimpleConsole:
        def __init__(self):
            self.text = ""

        def print(self, *args, end="\n", **kwargs):
            self.text += "".join(str(arg) for arg in args) + end

    monkeypatch.setattr(cli_main, "console", SimpleConsole())

    result = cli_main._resolve_workspaces(
        StubOrchestrator(),
        source_workspace="src-ws",
        dest_workspace="dst-ws",
    )

    assert calls == [("src-ws", "dst-ws")]
    assert result.workspace_mapping == {"src-ws": "dst-ws"}


def test_resolve_workspaces_requires_both_workspace_ids(monkeypatch):
    """Partial workspace scoping should abort with the cancellation sentinel."""

    class StubOrchestrator:
        source_client = object()
        dest_client = object()

    class SimpleConsole:
        def __init__(self):
            self.text = ""

        def print(self, *args, end="\n", **kwargs):
            self.text += "".join(str(arg) for arg in args) + end

    console = SimpleConsole()
    monkeypatch.setattr(cli_main, "console", console)

    result = cli_main._resolve_workspaces(StubOrchestrator(), source_workspace="src-ws")

    assert result == cli_main._WS_CANCELLED
    assert "must be provided together" in console.text


def test_resolve_workspaces_returns_cancelled_when_forced_tui_is_cancelled(monkeypatch):
    """A forced workspace TUI cancellation should map to the public cancelled sentinel."""

    class StubOrchestrator:
        source_client = object()
        dest_client = object()

    monkeypatch.setattr(cli_main, "resolve_workspace_context", lambda *args, **kwargs: None)

    result = cli_main._resolve_workspaces(StubOrchestrator(), map_workspaces=True)

    assert result == cli_main._WS_CANCELLED


def test_resolve_workspaces_returns_aborted_when_headless_resolution_fails(monkeypatch):
    """Headless workspace-resolution failures should map to the abort sentinel."""

    class StubOrchestrator:
        source_client = object()
        dest_client = object()

    def _raise(*args, **kwargs):
        raise cli_main.WorkspaceResolutionError("workspace mapping required")

    monkeypatch.setattr(cli_main, "resolve_workspace_context", _raise)

    result = cli_main._resolve_workspaces(
        StubOrchestrator(),
        non_interactive=True,
    )

    assert result == cli_main._WS_ABORTED


def test_test_command_uses_global_options_and_lists_workspaces(cli_harness):
    """The smoke-test command should honor global config flags and verbose workspace listing."""

    cli_harness.orchestrator_factory.source_client.get_responses["/api/v1/workspaces"] = [
        {"id": "src-ws", "display_name": "Source Workspace", "tenant_handle": "source"},
    ]
    cli_harness.orchestrator_factory.dest_client.get_responses["/api/v1/workspaces"] = [
        {"id": "dst-ws", "display_name": "Destination Workspace", "tenant_handle": "dest"},
    ]

    result = cli_harness.invoke(
        [
            "--no-ssl",
            "--batch-size",
            "25",
            "--workers",
            "2",
            "--dry-run",
            "--skip-existing",
            "-v",
            "test",
        ]
    )

    assert result.exit_code == 0
    orchestrator = cli_harness.orchestrator_factory.instances[0]
    assert orchestrator.config.source.verify_ssl is False
    assert orchestrator.config.destination.verify_ssl is False
    assert orchestrator.config.migration.batch_size == 25
    assert orchestrator.config.migration.concurrent_workers == 2
    assert orchestrator.config.migration.dry_run is True
    assert orchestrator.config.migration.skip_existing is True
    assert orchestrator.config.migration.verbose is True
    assert "Source Workspaces" in cli_harness.console.text
    assert "Destination Workspaces" in cli_harness.console.text


def test_test_command_exits_nonzero_when_connections_fail(cli_harness):
    """Connection smoke tests should surface failure via the command exit code."""

    cli_harness.orchestrator_factory.test_connections_value = False

    result = cli_harness.invoke(["test"])

    assert result.exit_code == 1
    assert "✗" in cli_harness.console.text
    assert cli_harness.orchestrator_factory.instances[0].cleanup_called is True


def test_users_command_members_csv_replaces_source_member_apis(cli_harness, monkeypatch, tmp_path):
    """users --members-csv should bypass source member listing APIs."""
    cli_harness.controls.workspace_result = WorkspaceProjectResult(
        workspace_mapping={"src-ws": "dst-ws"},
        project_mappings={},
        workspaces_to_create=[],
    )
    csv_path = tmp_path / "members.csv"
    csv_path.write_text(
        "email,langsmith_role,workspace_id\nalice@example.com,Workspace Admin,src-ws\n",
        encoding="utf-8",
    )

    user_role_migrator = Mock()
    user_role_migrator.build_role_mapping.return_value = {"src-role": "dst-role"}
    user_role_migrator._dest_email_to_identity = {}
    user_role_migrator.list_dest_org_members.return_value = []
    user_role_migrator.migrate_org_members.return_value = (1, 0, 0)
    user_role_migrator.migrate_workspace_members.return_value = (1, 0, 0)
    user_role_migrator.list_source_org_members.side_effect = AssertionError("org API should not be used in CSV mode")
    user_role_migrator.list_source_pending_org_members.side_effect = AssertionError(
        "pending API should not be used in CSV mode"
    )
    user_role_migrator.list_source_workspace_members.side_effect = AssertionError(
        "workspace API should not be used in CSV mode"
    )

    monkeypatch.setattr(cli_main, "UserRoleMigrator", lambda *args, **kwargs: user_role_migrator)

    resolved_rows = [{"email": "alice@example.com", "role_id": "src-role", "workspace_id": "src-ws"}]
    org_members = [{"id": "alice@example.com", "email": "alice@example.com", "role_id": "src-role"}]
    ws_members = [{"id": "src-ws:alice@example.com", "email": "alice@example.com", "role_id": "src-role"}]
    monkeypatch.setattr(cli_main, "_load_members_csv", lambda _: resolved_rows)
    monkeypatch.setattr(cli_main, "_resolve_csv_role_names", lambda rows, roles: (resolved_rows, "src-user"))
    monkeypatch.setattr(cli_main, "_csv_rows_to_org_members", lambda rows, **kw: org_members)
    monkeypatch.setattr(cli_main, "_csv_rows_for_workspace", lambda rows, ws_id: ws_members)

    result = cli_harness.invoke(["users", "--members-csv", str(csv_path)])

    assert result.exit_code == 0
    user_role_migrator.migrate_org_members.assert_called_once_with(org_members)
    user_role_migrator.migrate_workspace_members.assert_called_once_with(
        selected_members=ws_members
    )


def test_users_command_members_csv_supports_utf8_bom(cli_harness, monkeypatch, tmp_path):
    """users --members-csv accepts UTF-8 BOM-prefixed CSV headers."""
    cli_harness.controls.workspace_result = WorkspaceProjectResult(
        workspace_mapping={"src-ws": "dst-ws"},
        project_mappings={},
        workspaces_to_create=[],
    )
    csv_path = tmp_path / "members_bom.csv"
    csv_path.write_text(
        "email,langsmith_role,workspace_id\nalice@example.com,Workspace Admin,src-ws\n",
        encoding="utf-8-sig",
    )

    user_role_migrator = Mock()
    user_role_migrator.build_role_mapping.return_value = {"src-ws-admin": "dst-ws-admin"}
    user_role_migrator._dest_email_to_identity = {}
    user_role_migrator.list_dest_org_members.return_value = []
    user_role_migrator.migrate_org_members.return_value = (1, 0, 0)
    user_role_migrator.migrate_workspace_members.return_value = (1, 0, 0)
    user_role_migrator.list_source_roles.return_value = [
        {"id": "src-admin", "name": "ORGANIZATION_ADMIN", "display_name": "Admin"},
        {"id": "src-user", "name": "ORGANIZATION_USER", "display_name": "User"},
        {"id": "src-ws-admin", "name": "WORKSPACE_ADMIN", "display_name": "Workspace Admin"},
    ]
    monkeypatch.setattr(cli_main, "UserRoleMigrator", lambda *args, **kwargs: user_role_migrator)

    result = cli_harness.invoke(["users", "--members-csv", str(csv_path)])

    assert result.exit_code == 0
    # alice only appears in a workspace row, so she gets the default org role (ORGANIZATION_USER)
    user_role_migrator.migrate_org_members.assert_called_once_with(
        [{"id": "alice@example.com", "email": "alice@example.com", "role_id": "src-user", "full_name": ""}]
    )
    user_role_migrator.migrate_workspace_members.assert_called_once_with(
        selected_members=[
            {
                "id": "src-ws:alice@example.com",
                "email": "alice@example.com",
                "role_id": "src-ws-admin",
                "full_name": "",
            }
        ]
    )


def test_users_command_without_csv_keeps_api_member_paths(cli_harness, monkeypatch):
    """users without --members-csv should keep API-driven member discovery."""
    cli_harness.controls.workspace_result = WorkspaceProjectResult(
        workspace_mapping={"src-ws": "dst-ws"},
        project_mappings={},
        workspaces_to_create=[],
    )

    user_role_migrator = Mock()
    user_role_migrator.build_role_mapping.return_value = {"src-role": "dst-role"}
    user_role_migrator._dest_email_to_identity = {}
    user_role_migrator.list_dest_org_members.return_value = []
    user_role_migrator.list_source_org_members.return_value = [
        {"id": "src-org-1", "email": "alice@example.com", "role_id": "src-role"}
    ]
    user_role_migrator.list_source_pending_org_members.return_value = []
    user_role_migrator.list_source_workspace_members.return_value = [
        {"id": "src-ws-1", "email": "alice@example.com", "role_id": "src-role"}
    ]
    user_role_migrator.migrate_org_members.return_value = (1, 0, 0)
    user_role_migrator.migrate_workspace_members.return_value = (1, 0, 0)

    monkeypatch.setattr(cli_main, "UserRoleMigrator", lambda *args, **kwargs: user_role_migrator)

    result = cli_harness.invoke(["users"])

    assert result.exit_code == 0
    user_role_migrator.list_source_org_members.assert_called_once()
    user_role_migrator.list_source_pending_org_members.assert_called_once()
    user_role_migrator.list_source_workspace_members.assert_called_once()


def test_users_command_always_refreshes_dest_org_identities_for_phase3(
    cli_harness, monkeypatch
):
    """Workspace phase refreshes destination org identities even when cache is empty dict."""
    cli_harness.controls.workspace_result = WorkspaceProjectResult(
        workspace_mapping={"src-ws": "dst-ws"},
        project_mappings={},
        workspaces_to_create=[],
    )

    user_role_migrator = Mock()
    user_role_migrator.build_role_mapping.return_value = {"src-role": "dst-role"}
    user_role_migrator._dest_email_to_identity = {}
    user_role_migrator.list_source_org_members.return_value = []
    user_role_migrator.list_source_pending_org_members.return_value = []
    user_role_migrator.list_dest_org_members.return_value = [
        {"id": "dst-org-1", "email": "alice@example.com", "role_id": "dst-role"}
    ]
    user_role_migrator.list_source_workspace_members.return_value = [
        {"id": "src-ws-1", "email": "alice@example.com", "role_id": "src-role"}
    ]
    user_role_migrator.migrate_workspace_members.return_value = (1, 0, 0)
    monkeypatch.setattr(cli_main, "UserRoleMigrator", lambda *args, **kwargs: user_role_migrator)

    result = cli_harness.invoke(["users"])

    assert result.exit_code == 0
    user_role_migrator.list_dest_org_members.assert_called_once()
    user_role_migrator.migrate_workspace_members.assert_called_once()


def test_users_command_dest_org_refresh_failure_is_graceful(cli_harness, monkeypatch):
    """Failure refreshing destination identities logs warning and continues."""
    cli_harness.controls.workspace_result = WorkspaceProjectResult(
        workspace_mapping={"src-ws": "dst-ws"},
        project_mappings={},
        workspaces_to_create=[],
    )

    user_role_migrator = Mock()
    user_role_migrator.build_role_mapping.return_value = {"src-role": "dst-role"}
    user_role_migrator._dest_email_to_identity = {}
    user_role_migrator.list_source_org_members.return_value = []
    user_role_migrator.list_source_pending_org_members.return_value = []
    user_role_migrator.list_dest_org_members.side_effect = Exception("dest lookup failed")
    user_role_migrator.list_source_workspace_members.return_value = [
        {"id": "src-ws-1", "email": "alice@example.com", "role_id": "src-role"}
    ]
    user_role_migrator.migrate_workspace_members.return_value = (0, 0, 1)
    monkeypatch.setattr(cli_main, "UserRoleMigrator", lambda *args, **kwargs: user_role_migrator)

    result = cli_harness.invoke(["users"])

    assert result.exit_code == 0
    assert "failed to refresh destination org identities" in cli_harness.console.text
    user_role_migrator.migrate_workspace_members.assert_called_once()


def test_datasets_command_migrates_selected_datasets_with_workspace_scope(cli_harness):
    """Dataset migration should pass selected IDs into the orchestrator and clean up workspace scope."""

    cli_harness.controls.workspace_result = WorkspaceProjectResult(
        workspace_mapping={"src-ws": "dst-ws"},
        project_mappings={},
        workspaces_to_create=[],
    )
    cli_harness.migrators.dataset.list_datasets.return_value = [
        {"id": "dataset-1", "name": "Dataset One", "description": "First"},
        {"id": "dataset-2", "name": "Dataset Two", "description": "Second"},
    ]
    cli_harness.orchestrator_factory.migrate_datasets_return = {
        "dataset-1": "dest-1",
        "dataset-2": "dest-2",
    }
    cli_harness.controls.confirm_answers = [True]

    result = cli_harness.invoke(["datasets", "--all", "--include-experiments"])

    assert result.exit_code == 0
    orchestrator = cli_harness.orchestrator_factory.instances[0]
    assert orchestrator.workspace_calls == [("src-ws", "dst-ws")]
    assert orchestrator.migrate_dataset_calls == [
        {
            "dataset_ids": ["dataset-1", "dataset-2"],
            "include_examples": True,
            "include_experiments": True,
        }
    ]
    assert orchestrator.clear_workspace_called is True
    assert "Migration completed" in cli_harness.console.text


def test_datasets_command_exits_nonzero_when_workspace_resolution_aborts(cli_harness):
    """Commands should stop instead of migrating unscoped after workspace-resolution errors."""

    cli_harness.controls.workspace_result = cli_main._WS_ABORTED

    result = cli_harness.invoke(["datasets"])

    assert result.exit_code == 1
    assert cli_harness.orchestrator_factory.instances[0].migrate_dataset_calls == []


def test_prompts_command_surfaces_unavailable_prompts_api(cli_harness):
    """Prompt migration should stop early when the destination API is unavailable."""

    cli_harness.migrators.prompt.check_prompts_api_available.return_value = (False, "Feature disabled")

    result = cli_harness.invoke(["prompts", "--all"])

    assert result.exit_code == 0
    cli_harness.migrators.prompt.list_prompts.assert_not_called()
    assert "Feature disabled" in cli_harness.console.text


def test_prompts_command_cleans_up_on_connection_failure(cli_harness):
    """Prompt command should clean up the orchestrator even on early connection failures."""

    cli_harness.orchestrator_factory.test_connections_detailed_value = (
        False,
        True,
        "source failure",
        None,
    )

    result = cli_harness.invoke(["prompts", "--all"])

    assert result.exit_code == 0
    assert cli_harness.orchestrator_factory.instances[0].cleanup_called is True


def test_prompts_command_reports_405_failures_with_helpful_guidance(cli_harness):
    """405 failures should be summarized with the dedicated troubleshooting message."""

    cli_harness.migrators.prompt.list_prompts.return_value = [
        {"repo_handle": "team/prompt-a"},
        {"repo_handle": "team/prompt-b"},
    ]
    cli_harness.migrators.prompt.migrate_prompt.side_effect = [
        True,
        Exception("405 Not Allowed"),
    ]
    cli_harness.controls.confirm_answers = [True]

    result = cli_harness.invoke(["prompts", "--all", "--include-all-commits"])

    assert result.exit_code == 0
    assert cli_harness.migrators.prompt.migrate_prompt.call_count == 2
    cli_harness.migrators.prompt.migrate_prompt.assert_any_call(
        "team/prompt-a",
        include_all_commits=True,
    )
    cli_harness.migrators.prompt.migrate_prompt.assert_any_call(
        "team/prompt-b",
        include_all_commits=True,
    )
    assert "Prompts: 1 migrated, 1 failed" in cli_harness.console.text
    assert "405 Not Allowed errors" in cli_harness.console.text


def test_queues_command_migrates_selected_queues_and_reports_failures(cli_harness):
    """Queue migration should continue through per-item failures and summarize the result."""

    cli_harness.migrators.queue.list_queues.return_value = [
        {"id": "queue-1", "name": "Queue One", "description": "First"},
        {"id": "queue-2", "name": "Queue Two", "description": "Second"},
    ]
    cli_harness.migrators.queue.create_queue.side_effect = [
        "new-queue-1",
        Exception("queue create failed"),
    ]

    result = cli_harness.invoke(["queues"])

    assert result.exit_code == 0
    assert cli_harness.migrators.queue.create_queue.call_count == 2
    assert "Queues: 1 migrated, 1 failed" in cli_harness.console.text


def test_list_projects_requires_a_target_side(cli_harness):
    """The list-projects helper should demand --source or --dest."""

    result = cli_harness.invoke(["list-projects"], add_base_args=False)

    assert result.exit_code == 0
    assert "Specify --source or --dest" in cli_harness.console.text


def test_list_projects_queries_each_requested_instance(cli_harness):
    """Project listing should call the source and destination pagination endpoints."""

    cli_harness.orchestrator_factory.source_client.paginated_results = [
        {"id": "src-project", "name": "Source Project"},
    ]
    cli_harness.orchestrator_factory.dest_client.paginated_results = [
        {"id": "dst-project", "name": "Destination Project"},
    ]

    result = cli_harness.invoke(["list-projects", "--source", "--dest"])

    assert result.exit_code == 0
    assert cli_harness.orchestrator_factory.source_client.get_paginated_calls == [
        ("/sessions", 100)
    ]
    assert cli_harness.orchestrator_factory.dest_client.get_paginated_calls == [
        ("/sessions", 100)
    ]


def test_list_workspaces_queries_each_requested_instance(cli_harness):
    """Workspace listing should call the discovery endpoints for each requested side."""

    cli_harness.orchestrator_factory.source_client.get_responses["/api/v1/workspaces"] = [
        {"id": "src-ws", "display_name": "Source Workspace", "tenant_handle": "source"},
    ]
    cli_harness.orchestrator_factory.dest_client.get_responses["/api/v1/workspaces"] = [
        {"id": "dst-ws", "display_name": "Destination Workspace", "tenant_handle": "dest"},
    ]

    result = cli_harness.invoke(["list_workspaces", "--source", "--dest"])

    assert result.exit_code == 0
    assert cli_harness.orchestrator_factory.source_client.get_calls == ["/api/v1/workspaces"]
    assert cli_harness.orchestrator_factory.dest_client.get_calls == ["/api/v1/workspaces"]
    assert "Source Workspaces" in cli_harness.console.text
    assert "Destination Workspaces" in cli_harness.console.text


def test_rules_command_rejects_conflicting_project_mapping_modes(cli_harness):
    """The rules CLI should guard against mutually exclusive mapping inputs."""

    result = cli_harness.invoke(
        ["rules", "--all", "--map-projects", "--project-mapping", '{"a": "b"}']
    )

    assert result.exit_code == 0
    cli_harness.migrators.rules.list_rules.assert_not_called()
    assert "mutually exclusive" in cli_harness.console.text


def test_rules_command_uses_custom_project_mapping_and_create_enabled(cli_harness):
    """Inline project mappings and create-enabled should flow into rule creation."""

    cli_harness.migrators.rules.list_rules.return_value = [
        {
            "id": "rule-1",
            "display_name": "Rule One",
            "dataset_id": "dataset-1",
        }
    ]
    cli_harness.migrators.rules.create_rule.return_value = "dest-rule-1"
    cli_harness.controls.confirm_answers = [True]

    result = cli_harness.invoke(
        [
            "rules",
            "--all",
            "--project-mapping",
            '{"src-project": "dst-project"}',
            "--create-enabled",
        ]
    )

    assert result.exit_code == 0
    assert cli_harness.migrators.rules._project_id_map == {"src-project": "dst-project"}
    cli_harness.migrators.rules.create_rule.assert_called_once_with(
        cli_harness.migrators.rules.list_rules.return_value[0],
        strip_project_reference=False,
        create_disabled=False,
    )
    assert "Rules: 1 migrated, 0 skipped, 0 failed" in cli_harness.console.text


def test_rules_command_uses_workspace_project_mappings(cli_harness):
    """Standalone rules migration should consume per-workspace project mappings from workspace resolution."""

    cli_harness.controls.workspace_result = WorkspaceProjectResult(
        workspace_mapping={"src-ws": "dst-ws"},
        project_mappings={"src-ws": {"Source Project": "Destination Project"}},
        workspaces_to_create=[],
    )
    cli_harness.orchestrator_factory.source_client.paginated_results = [
        {"id": "source-project-id", "name": "Source Project"},
    ]
    cli_harness.orchestrator_factory.dest_client.paginated_results = [
        {"id": "dest-project-id", "name": "Destination Project"},
    ]
    cli_harness.migrators.rules.list_rules.return_value = [
        {"id": "rule-1", "display_name": "Rule One", "dataset_id": "dataset-1"},
    ]
    cli_harness.migrators.rules.create_rule.return_value = "dest-rule-1"
    cli_harness.controls.confirm_answers = [True]

    result = cli_harness.invoke(["rules", "--all"])

    assert result.exit_code == 0
    assert cli_harness.migrators.rules._project_id_map == {
        "source-project-id": "dest-project-id"
    }
    assert "Using workspace-scoped project mapping" in cli_harness.console.text


def test_migrate_all_runs_every_step_and_applies_workspace_project_mappings(cli_harness):
    """The all-in-one wizard should thread dataset and project mappings through later steps."""

    cli_harness.controls.workspace_result = WorkspaceProjectResult(
        workspace_mapping={"src-ws": "dst-ws"},
        project_mappings={"src-ws": {"Source Project": "Destination Project"}},
        workspaces_to_create=[],
    )
    cli_harness.orchestrator_factory.source_client.paginated_results = [
        {"id": "source-project-id", "name": "Source Project"},
    ]
    cli_harness.orchestrator_factory.dest_client.paginated_results = [
        {"id": "dest-project-id", "name": "Destination Project"},
    ]
    cli_harness.migrators.dataset.list_datasets.return_value = [
        {"id": "dataset-1", "name": "Dataset One"},
    ]
    cli_harness.migrators.prompt.list_prompts.return_value = [
        {"repo_handle": "team/prompt-a"},
    ]
    cli_harness.migrators.queue.list_queues.return_value = [
        {"id": "queue-1", "name": "Queue One"},
    ]
    cli_harness.migrators.rules.list_rules.return_value = [
        {"id": "rule-1", "display_name": "Rule One", "dataset_id": "dataset-1"},
    ]
    cli_harness.migrators.chart.list_charts.return_value = [
        {"id": "chart-1", "title": "Chart One", "project_id": "source-project-id"},
    ]
    cli_harness.orchestrator_factory.migrate_datasets_return = {"dataset-1": "dest-dataset-1"}
    cli_harness.migrators.prompt.migrate_prompt.return_value = True
    cli_harness.migrators.queue.create_queue.return_value = "dest-queue-1"
    cli_harness.migrators.rules.create_rule.return_value = "dest-rule-1"
    cli_harness.migrators.chart.migrate_all_charts.return_value = {
        "source-project-id": {"chart-1": "dest-chart-1"}
    }
    cli_harness.controls.confirm_answers = [True, True, True, True, True, True]

    result = cli_harness.invoke(["migrate-all", "--include-all-commits"])

    assert result.exit_code == 0
    orchestrator = cli_harness.orchestrator_factory.instances[0]
    assert orchestrator.workspace_calls == [("src-ws", "dst-ws")]
    assert orchestrator.migrate_dataset_calls == [
        {
            "dataset_ids": ["dataset-1"],
            "include_examples": True,
            "include_experiments": True,
        }
    ]
    assert cli_harness.migrators.rules._dataset_id_map == {"dataset-1": "dest-dataset-1"}
    assert cli_harness.migrators.rules._project_id_map == {
        "source-project-id": "dest-project-id"
    }
    assert cli_harness.migrators.prompt.migrate_prompt.call_count == 1
    assert cli_harness.migrators.queue.create_queue.call_count == 1
    assert cli_harness.migrators.rules.create_rule.call_count == 1
    cli_harness.migrators.chart.migrate_all_charts.assert_called_once_with(same_instance=False)
    assert cli_harness.migrators.chart._project_id_map == {
        "source-project-id": "dest-project-id"
    }
    assert orchestrator.clear_workspace_called is True
    assert "Migration wizard completed!" in cli_harness.console.text


def test_migrate_all_supports_skipping_charts_via_flag_and_prompt(cli_harness):
    """migrate-all should support chart skipping through both CLI flag and wizard confirmation."""

    # Flag path: charts step is bypassed entirely.
    cli_harness.migrators.chart.list_charts.return_value = [
        {"id": "chart-1", "title": "Chart One"},
    ]
    result_flag = cli_harness.invoke(["migrate-all", "--skip-charts"])
    assert result_flag.exit_code == 0
    cli_harness.migrators.chart.list_charts.assert_not_called()
    assert "Skipping charts (--skip-charts)" in cli_harness.console.text

    # Prompt path: charts are discovered but user opts out interactively.
    cli_harness.migrators.chart.reset_mock()
    cli_harness.migrators.chart.list_charts.return_value = [
        {"id": "chart-1", "title": "Chart One", "project_id": "source-project-id"},
    ]
    cli_harness.controls.workspace_result = WorkspaceProjectResult(
        workspace_mapping={"src-ws": "dst-ws"},
        project_mappings={"src-ws": {"Source Project": "Destination Project"}},
        workspaces_to_create=[],
    )
    cli_harness.orchestrator_factory.source_client.paginated_results = [
        {"id": "source-project-id", "name": "Source Project"},
    ]
    cli_harness.orchestrator_factory.dest_client.paginated_results = [
        {"id": "dest-project-id", "name": "Destination Project"},
    ]
    cli_harness.controls.confirm_answers = [False]

    result_prompt = cli_harness.invoke(["migrate-all"])
    assert result_prompt.exit_code == 0
    cli_harness.migrators.chart.list_charts.assert_called_once()
    cli_harness.migrators.chart.migrate_all_charts.assert_not_called()
    assert "Skipped charts" in cli_harness.console.text


def test_charts_command_auto_detects_same_instance(cli_harness):
    """Charts migration should flip into same-instance mode automatically when URLs and keys match."""

    cli_harness.migrators.chart.migrate_all_charts.return_value = {
        "session-1": {"chart-1": "dest-chart-1"}
    }
    cli_harness.controls.confirm_answers = [True]

    cli_harness.console.clear()
    result = cli_harness.runner.invoke(
        cli_main.cli,
        [
            "--source-key",
            "shared-key",
            "--dest-key",
            "shared-key",
            "--source-url",
            "https://same.example",
            "--dest-url",
            "https://same.example",
            "charts",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0
    cli_harness.migrators.chart.migrate_all_charts.assert_called_once_with(same_instance=True)
    assert "Detected same source and destination instance" in cli_harness.console.text


def test_charts_command_uses_workspace_project_mappings(cli_harness):
    """Standalone chart migration should consume per-workspace project mappings from workspace resolution."""

    cli_harness.controls.workspace_result = WorkspaceProjectResult(
        workspace_mapping={"src-ws": "dst-ws"},
        project_mappings={"src-ws": {"Source Project": "Destination Project"}},
        workspaces_to_create=[],
    )
    cli_harness.orchestrator_factory.source_client.paginated_results = [
        {"id": "source-project-id", "name": "Source Project"},
    ]
    cli_harness.orchestrator_factory.dest_client.paginated_results = [
        {"id": "dest-project-id", "name": "Destination Project"},
    ]
    cli_harness.migrators.chart.migrate_all_charts.return_value = {
        "source-project-id": {"chart-1": "dest-chart-1"}
    }
    cli_harness.controls.confirm_answers = [True]

    result = cli_harness.invoke(["charts"])

    assert result.exit_code == 0
    assert cli_harness.migrators.chart._project_id_map == {
        "source-project-id": "dest-project-id"
    }
    assert "Using workspace-scoped project mapping" in cli_harness.console.text


def test_charts_command_uses_saved_project_mapping_for_session_migration(cli_harness):
    """Session-scoped chart migration should resolve the destination project from saved state."""

    cli_harness.orchestrator_factory.state = build_state(
        "chart-session",
        id_mappings={"project": {"source-session": "dest-session"}},
    )
    cli_harness.migrators.chart.list_sessions.return_value = [
        {"id": "source-session", "name": "Session A"},
    ]
    cli_harness.migrators.chart.migrate_session_charts.return_value = {
        "chart-1": "dest-chart-1"
    }

    result = cli_harness.invoke(["charts", "--session", "Session A"])

    assert result.exit_code == 0
    cli_harness.migrators.chart.migrate_session_charts.assert_called_once_with(
        "source-session",
        "dest-session",
    )
    assert "Mapped to destination session" in cli_harness.console.text


def test_resume_command_retries_pending_datasets(cli_harness):
    """Resume should reload a saved session and rerun pending datasets."""

    state = build_state("migration_resume")
    state.add_item(
        MigrationItem(
            id="dataset_dataset-1",
            type="dataset",
            name="Dataset One",
            source_id="dataset-1",
            status=MigrationStatus.PENDING,
        )
    )
    save_session(cli_harness.state_manager, state)
    cli_harness.console.inputs = ["1"]
    cli_harness.controls.confirm_answers = [True]

    result = cli_harness.invoke(["resume"])

    assert result.exit_code == 0
    orchestrator = cli_harness.orchestrator_factory.instances[0]
    assert orchestrator.state.session_id == "migration_resume"
    assert orchestrator.migrate_dataset_calls == [
        {
            "dataset_ids": ["dataset-1"],
            "include_examples": True,
            "include_experiments": False,
        }
    ]
    assert "Resuming migration of 1 items" in cli_harness.console.text


def test_resume_command_retries_prompt_queue_rule_and_chart_items(cli_harness):
    """Resume should replay tracked non-dataset items using their saved state metadata."""

    state = build_state("migration_resume_non_dataset")
    state.add_item(
        MigrationItem(
            id="prompt_default_team_prompt-a",
            type="prompt",
            name="team/prompt-a",
            source_id="team/prompt-a",
            status=MigrationStatus.PENDING,
            metadata={"include_all_commits": True},
        )
    )
    state.add_item(
        MigrationItem(
            id="queue_default_queue-1",
            type="queue",
            name="Queue One",
            source_id="queue-1",
            status=MigrationStatus.PENDING,
            metadata={"queue": {"id": "queue-1", "name": "Queue One"}},
        )
    )
    state.add_item(
        MigrationItem(
            id="rule_default_rule-1",
            type="rule",
            name="Rule One",
            source_id="rule-1",
            status=MigrationStatus.PENDING,
            metadata={
                "rule": {"id": "rule-1", "display_name": "Rule One"},
                "strip_projects": True,
                "create_disabled": True,
                "project_id_map": {"source-project-id": "dest-project-id"},
            },
        )
    )
    state.add_item(
        MigrationItem(
            id="chart_default_chart-1",
            type="chart",
            name="Chart One",
            source_id="chart-1",
            status=MigrationStatus.PENDING,
            metadata={
                "chart": {"id": "chart-1", "title": "Chart One", "series": [{"filters": {}}]},
                "dest_session_id": "dest-session-1",
            },
        )
    )
    save_session(cli_harness.state_manager, state)
    cli_harness.console.inputs = ["1"]
    cli_harness.controls.confirm_answers = [True]
    cli_harness.migrators.prompt.migrate_prompt.return_value = "team/prompt-a"
    cli_harness.migrators.queue.create_queue.return_value = "dest-queue-1"
    cli_harness.migrators.rules.create_rule.return_value = "dest-rule-1"
    cli_harness.migrators.chart.migrate_chart.return_value = "dest-chart-1"

    result = cli_harness.invoke(["resume"])

    assert result.exit_code == 0
    cli_harness.migrators.prompt.migrate_prompt.assert_called_once_with(
        "team/prompt-a",
        include_all_commits=True,
    )
    cli_harness.migrators.queue.create_queue.assert_called_once_with(
        {"id": "queue-1", "name": "Queue One"}
    )
    assert cli_harness.migrators.rules._project_id_map == {
        "source-project-id": "dest-project-id"
    }
    cli_harness.migrators.rules.create_rule.assert_called_once_with(
        {"id": "rule-1", "display_name": "Rule One"},
        strip_project_reference=True,
        ensure_project=False,
        create_disabled=True,
    )
    cli_harness.migrators.chart.migrate_chart.assert_called_once_with(
        {"id": "chart-1", "title": "Chart One", "series": [{"filters": {}}]},
        "dest-session-1",
    )
    assert "Resume processing completed" in cli_harness.console.text


def test_resume_command_non_interactive_uses_latest_session_without_console_input(cli_harness):
    """Non-interactive resume should auto-select the latest session and dispatch through the orchestrator."""

    state = build_state("migration_resume_non_interactive")
    state.add_item(
        MigrationItem(
            id="prompt_default_team_prompt-a",
            type="prompt",
            name="team/prompt-a",
            source_id="team/prompt-a",
            status=MigrationStatus.PENDING,
            metadata={"include_all_commits": True},
        )
    )
    save_session(cli_harness.state_manager, state)
    cli_harness.migrators.prompt.migrate_prompt.return_value = "team/prompt-a"

    result = cli_harness.invoke(["--non-interactive", "resume"])

    assert result.exit_code == 0
    cli_harness.migrators.prompt.migrate_prompt.assert_called_once_with(
        "team/prompt-a",
        include_all_commits=True,
    )
    assert "Resume processing completed" in cli_harness.console.text


def test_resume_command_interactive_can_select_non_latest_session(cli_harness):
    """Interactive resume should honor the user's chosen session, not always pick the latest."""

    latest = build_state("migration_resume_latest")
    latest.updated_at = 20.0
    latest.add_item(
        MigrationItem(
            id="prompt_default_latest",
            type="prompt",
            name="team/latest",
            source_id="team/latest",
            status=MigrationStatus.PENDING,
            metadata={"include_all_commits": False},
        )
    )
    latest.updated_at = 20.0
    older = build_state("migration_resume_older")
    older.add_item(
        MigrationItem(
            id="prompt_default_older",
            type="prompt",
            name="team/older",
            source_id="team/older",
            status=MigrationStatus.PENDING,
            metadata={"include_all_commits": True},
        )
    )
    older.updated_at = 10.0
    save_session(cli_harness.state_manager, latest)
    save_session(cli_harness.state_manager, older)
    cli_harness.console.inputs = ["2"]
    cli_harness.controls.confirm_answers = [True]
    cli_harness.migrators.prompt.migrate_prompt.return_value = "team/older"

    result = cli_harness.invoke(["resume"])

    assert result.exit_code == 0
    orchestrator = cli_harness.orchestrator_factory.instances[0]
    assert orchestrator.state.session_id == "migration_resume_older"
    cli_harness.migrators.prompt.migrate_prompt.assert_called_once_with(
        "team/older",
        include_all_commits=True,
    )


def test_queues_command_non_interactive_exits_with_code_2_for_blocked_items(cli_harness):
    """Non-interactive runs should exit with code 2 when the session requires remediation."""

    cli_harness.migrators.queue.list_queues.return_value = [
        {"id": "queue-1", "name": "Queue One"},
    ]
    cli_harness.migrators.queue.create_queue.side_effect = Exception("queue create failed")

    result = cli_harness.invoke(["--non-interactive", "queues"])

    assert result.exit_code == 2
    assert "Resolution Summary" in cli_harness.console.text
    assert "Remediation bundle:" in cli_harness.console.text


def test_clean_command_deletes_saved_sessions(cli_harness):
    """Cleaning sessions should remove persisted state files after confirmation."""

    save_session(cli_harness.state_manager, build_state("migration_one"))
    save_session(cli_harness.state_manager, build_state("migration_two"))
    cli_harness.controls.confirm_answers = [True]

    result = cli_harness.invoke(["clean"], add_base_args=False)

    assert result.exit_code == 0
    assert cli_harness.state_manager.list_sessions() == []
    assert "All sessions deleted" in cli_harness.console.text
