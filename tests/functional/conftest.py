"""Fixtures and fakes for CLI functional tests."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest.mock import Mock

import pytest
from click.testing import CliRunner

from langsmith_migrator.cli import main as cli_main
from langsmith_migrator.core import migrators as migrators_module
from langsmith_migrator.utils.state import MigrationState, StateManager


class FakeConsole:
    """Small console shim for deterministic CLI tests."""

    def __init__(self) -> None:
        self.buffer = ""
        self.inputs: list[str] = []

    def print(self, *args: Any, end: str = "\n", **_: Any) -> None:
        self.buffer += "".join(self._stringify(arg) for arg in args) + end

    def input(self, prompt: str = "") -> str:
        self.buffer += prompt
        if not self.inputs:
            raise AssertionError(f"No console input queued for prompt: {prompt!r}")
        return self.inputs.pop(0)

    def clear(self) -> None:
        self.buffer = ""

    @property
    def text(self) -> str:
        return self.buffer

    @staticmethod
    def _stringify(value: Any) -> str:
        if value.__class__.__name__ == "Table":
            title = getattr(value, "title", None)
            return f"<Table title={title!r}>"
        return str(value)


class FakeProgress:
    """Progress stub that behaves like rich.progress.Progress."""

    def __init__(self, *_: Any, **__: Any) -> None:
        self.tasks: dict[int, dict[str, Any]] = {}
        self._next_task_id = 1

    def __enter__(self) -> "FakeProgress":
        return self

    def __exit__(self, *_: Any) -> bool:
        return False

    def add_task(self, description: str, total: int) -> int:
        task_id = self._next_task_id
        self._next_task_id += 1
        self.tasks[task_id] = {
            "description": description,
            "total": total,
            "advanced": 0,
        }
        return task_id

    def advance(self, task_id: int, advance: int = 1) -> None:
        self.tasks[task_id]["advanced"] += advance


class FakeClient:
    """Client stub with enough surface for CLI tests."""

    def __init__(self, label: str) -> None:
        self.label = label
        self.session = SimpleNamespace(headers={})
        self.get_responses: dict[str, Any] = {}
        self.paginated_results: list[dict[str, Any]] = []
        self.get_calls: list[str] = []
        self.get_paginated_calls: list[tuple[str, int]] = []
        self.set_workspace_calls: list[str | None] = []
        self.closed = False

    def get(self, endpoint: str) -> Any:
        self.get_calls.append(endpoint)
        response = self.get_responses.get(endpoint, [])
        if isinstance(response, Exception):
            raise response
        return response

    def get_paginated(self, endpoint: str, page_size: int = 100) -> list[dict[str, Any]]:
        self.get_paginated_calls.append((endpoint, page_size))
        return list(self.paginated_results)

    def set_workspace(self, workspace_id: str | None) -> None:
        self.set_workspace_calls.append(workspace_id)
        if workspace_id is None:
            self.session.headers.pop("X-Tenant-Id", None)
        else:
            self.session.headers["X-Tenant-Id"] = workspace_id

    def close(self) -> None:
        self.closed = True


def make_state(
    *,
    session_id: str = "session-1",
    id_mappings: dict[str, dict[str, str]] | None = None,
) -> MigrationState:
    """Build a migration state for orchestrator fakes."""

    return MigrationState(
        session_id=session_id,
        started_at=1.0,
        updated_at=2.0,
        source_url="https://source.example",
        destination_url="https://dest.example",
        id_mappings=id_mappings or {},
    )


@dataclass
class FakeOrchestratorInstance:
    """Concrete fake orchestrator captured for assertions."""

    config: Any
    state_manager: StateManager
    source_client: FakeClient
    dest_client: FakeClient
    state: MigrationState | None
    test_connections_value: bool
    test_connections_detailed_value: tuple[bool, bool, str | None, str | None]
    migrate_datasets_return: dict[str, str]
    migrate_datasets_side_effect: Exception | None
    migrators: Any | None = None
    migrate_dataset_calls: list[dict[str, Any]] = field(default_factory=list)
    workspace_calls: list[tuple[str, str]] = field(default_factory=list)
    clear_workspace_called: bool = False
    cleanup_called: bool = False

    def test_connections(self) -> bool:
        return self.test_connections_value

    def test_connections_detailed(self) -> tuple[bool, bool, str | None, str | None]:
        return self.test_connections_detailed_value

    def migrate_datasets_parallel(
        self,
        dataset_ids: list[str],
        include_examples: bool = True,
        include_experiments: bool = False,
    ) -> dict[str, str]:
        self.migrate_dataset_calls.append(
            {
                "dataset_ids": list(dataset_ids),
                "include_examples": include_examples,
                "include_experiments": include_experiments,
            }
        )
        if self.migrate_datasets_side_effect is not None:
            raise self.migrate_datasets_side_effect
        return dict(self.migrate_datasets_return)

    def set_workspace_context(self, source_ws_id: str, dest_ws_id: str) -> None:
        self.workspace_calls.append((source_ws_id, dest_ws_id))
        self.source_client.set_workspace(source_ws_id)
        self.dest_client.set_workspace(dest_ws_id)

    def clear_workspace_context(self) -> None:
        self.clear_workspace_called = True
        self.source_client.set_workspace(None)
        self.dest_client.set_workspace(None)

    def resume_items(self, items_to_process: list[Any]) -> dict[str, list[str]]:
        results = {"resumed": [], "blocked": []}
        for item in items_to_process:
            if item.type == "dataset":
                self.migrate_datasets_parallel(
                    [item.source_id],
                    include_examples=True,
                    include_experiments=False,
                )
                results["resumed"].append(f"dataset:{item.source_id}")
            elif item.type == "prompt":
                self.migrators.prompt.migrate_prompt(
                    item.source_id,
                    include_all_commits=item.metadata.get("include_all_commits", False),
                )
                results["resumed"].append(f"prompt:{item.source_id}")
            elif item.type == "queue":
                self.migrators.queue.create_queue(item.metadata.get("queue"))
                results["resumed"].append(f"queue:{item.source_id}")
            elif item.type == "rule":
                self.migrators.rules._project_id_map = dict(item.metadata.get("project_id_map") or {})
                self.migrators.rules.create_rule(
                    item.metadata.get("rule"),
                    strip_project_reference=item.metadata.get("strip_projects", False),
                    ensure_project=item.metadata.get("ensure_project", False),
                    create_disabled=item.metadata.get("create_disabled", False),
                )
                results["resumed"].append(f"rule:{item.source_id}")
            elif item.type == "chart":
                self.migrators.chart.migrate_chart(
                    item.metadata.get("chart"),
                    item.metadata.get("dest_session_id"),
                    same_instance=item.metadata.get("same_instance", False),
                )
                results["resumed"].append(f"chart:{item.source_id}")
            else:
                results["blocked"].append(f"{item.type}:{item.source_id}")
        return results

    def cleanup(self) -> None:
        self.cleanup_called = True
        self.source_client.close()
        self.dest_client.close()


class FakeOrchestratorFactory:
    """Configurable orchestrator factory for CLI tests."""

    def __init__(self) -> None:
        self.instances: list[FakeOrchestratorInstance] = []
        self.source_client = FakeClient("source")
        self.dest_client = FakeClient("destination")
        self.state: MigrationState | None = make_state()
        self.test_connections_value = True
        self.test_connections_detailed_value = (True, True, None, None)
        self.migrate_datasets_return: dict[str, str] = {}
        self.migrate_datasets_side_effect: Exception | None = None
        self.migrators: Any | None = None

    def __call__(self, config: Any, state_manager: StateManager) -> FakeOrchestratorInstance:
        instance = FakeOrchestratorInstance(
            config=config,
            state_manager=state_manager,
            source_client=self.source_client,
            dest_client=self.dest_client,
            state=self.state,
            test_connections_value=self.test_connections_value,
            test_connections_detailed_value=self.test_connections_detailed_value,
            migrate_datasets_return=self.migrate_datasets_return,
            migrate_datasets_side_effect=self.migrate_datasets_side_effect,
            migrators=self.migrators,
        )
        self.instances.append(instance)
        return instance


@dataclass
class CliControls:
    """Interactive behavior controls for CLI tests."""

    console: FakeConsole
    confirm_answers: list[bool] = field(default_factory=list)
    select_results: list[Any] = field(default_factory=list)
    workspace_result: Any = None
    project_mapping_result: Any = None

    def confirm(self, prompt: str, default: bool = False, **_: Any) -> bool:
        if self.confirm_answers:
            return self.confirm_answers.pop(0)
        return default

    def select(self, items: list[dict[str, Any]], **_: Any) -> Any:
        if self.select_results:
            selection = self.select_results.pop(0)
            if callable(selection):
                return selection(items)
            return selection
        return items

    def resolve_workspaces(self, *_: Any, **__: Any) -> Any:
        return self.workspace_result

    def build_project_mapping(self, *_: Any, **__: Any) -> Any:
        return self.project_mapping_result


@dataclass
class MigratorRegistry:
    """Injected migrator instances for CLI tests."""

    dataset: Mock = field(default_factory=lambda: Mock(name="dataset_migrator"))
    queue: Mock = field(default_factory=lambda: Mock(name="queue_migrator"))
    prompt: Mock = field(default_factory=lambda: Mock(name="prompt_migrator"))
    rules: Mock = field(default_factory=lambda: Mock(name="rules_migrator"))
    chart: Mock = field(default_factory=lambda: Mock(name="chart_migrator"))

    def __post_init__(self) -> None:
        self.dataset.list_datasets.return_value = []
        self.queue.list_queues.return_value = []
        self.prompt.check_prompts_api_available.return_value = (True, None)
        self.prompt.probe_capabilities.return_value = {}
        self.prompt.list_prompts.return_value = []
        self.rules.probe_capabilities.return_value = {}
        self.rules.list_rules.return_value = []
        self.chart._project_id_map = None
        self.chart.probe_capabilities.return_value = {}
        self.chart.list_charts.return_value = []
        self.chart.list_sessions.return_value = []
        self.chart.migrate_all_charts.return_value = {}
        self.chart.migrate_session_charts.return_value = {}


@dataclass
class CliHarness:
    """Convenience wrapper around the patched CLI."""

    runner: CliRunner
    console: FakeConsole
    controls: CliControls
    orchestrator_factory: FakeOrchestratorFactory
    migrators: MigratorRegistry
    state_manager: StateManager

    base_args: tuple[str, ...] = (
        "--source-key",
        "src-key",
        "--dest-key",
        "dest-key",
        "--source-url",
        "https://source.example",
        "--dest-url",
        "https://dest.example",
    )

    def invoke(self, args: list[str], *, add_base_args: bool = True):
        self.console.clear()
        command = [*self.base_args, *args] if add_base_args else list(args)
        return self.runner.invoke(cli_main.cli, command, catch_exceptions=False)


@pytest.fixture
def cli_harness(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> CliHarness:
    """Patch the CLI module with deterministic fakes."""

    console = FakeConsole()
    controls = CliControls(console=console)
    orchestrator_factory = FakeOrchestratorFactory()
    migrators = MigratorRegistry()
    orchestrator_factory.migrators = migrators
    state_manager = StateManager(tmp_path / "state")

    monkeypatch.setattr(cli_main, "console", console)
    monkeypatch.setattr(cli_main, "display_banner", lambda: None)
    monkeypatch.setattr(cli_main, "Progress", FakeProgress)
    monkeypatch.setattr(cli_main.Confirm, "ask", lambda *args, **kwargs: controls.confirm(*args, **kwargs))
    monkeypatch.setattr(cli_main, "select_items", lambda *args, **kwargs: controls.select(*args, **kwargs))
    monkeypatch.setattr(cli_main, "_resolve_workspaces", controls.resolve_workspaces)
    monkeypatch.setattr(cli_main, "build_project_mapping_tui", controls.build_project_mapping)
    monkeypatch.setattr(cli_main, "StateManager", lambda: state_manager)
    monkeypatch.setattr(cli_main, "MigrationOrchestrator", orchestrator_factory)

    def _resolve_destination_session_id(source_session_id: str | None, *, same_instance: bool = False):
        if not source_session_id:
            return None
        if same_instance:
            return source_session_id

        project_id_map = getattr(migrators.chart, "_project_id_map", None) or {}
        if source_session_id in project_id_map:
            return project_id_map[source_session_id]

        if orchestrator_factory.state is not None:
            return orchestrator_factory.state.get_mapped_id("project", source_session_id)
        return None

    migrators.chart.resolve_destination_session_id.side_effect = _resolve_destination_session_id

    def patch_migrator(name: str, instance: Mock) -> None:
        factory = lambda *args, **kwargs: instance
        monkeypatch.setattr(cli_main, name, factory)
        monkeypatch.setattr(migrators_module, name, factory)

    patch_migrator("DatasetMigrator", migrators.dataset)
    patch_migrator("AnnotationQueueMigrator", migrators.queue)
    patch_migrator("PromptMigrator", migrators.prompt)
    patch_migrator("RulesMigrator", migrators.rules)
    patch_migrator("ChartMigrator", migrators.chart)

    return CliHarness(
        runner=CliRunner(),
        console=console,
        controls=controls,
        orchestrator_factory=orchestrator_factory,
        migrators=migrators,
        state_manager=state_manager,
    )
