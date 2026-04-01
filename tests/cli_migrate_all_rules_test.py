"""Tests for migrate-all rule enable/disable behavior."""

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


class _FakeRulesMigrator:
    create_rule_calls = []

    def __init__(self, source_client, dest_client, state, config):
        self._dataset_id_map = {}
        self._project_id_map = {}

    def list_rules(self):
        return [{"display_name": "Rule 1", "dataset_id": "dataset-1"}]

    def create_rule(self, rule, **kwargs):
        self.__class__.create_rule_calls.append(kwargs)
        return "new-rule-id"


class _FakeChartMigrator:
    def __init__(self, source_client, dest_client, state, config):
        self._project_id_map = None

    def list_charts(self):
        return []


class _FakeState:
    def __init__(self):
        self.session_id = "test-session"
        self.remediation_bundle_path = None

    def ensure_item(self, *args, **kwargs):
        return None

    def update_item_status(self, *args, **kwargs):
        return None

    def update_item_checkpoint(self, *args, **kwargs):
        return None

    def get_item(self, *args, **kwargs):
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
        migration=SimpleNamespace(verbose=False, non_interactive=False),
        source=SimpleNamespace(base_url="https://source.example", api_key="src-key"),
        destination=SimpleNamespace(base_url="https://dest.example", api_key="dest-key"),
    )


def _make_orchestrator():
    return SimpleNamespace(
        source_client=SimpleNamespace(session=SimpleNamespace(headers={})),
        dest_client=SimpleNamespace(session=SimpleNamespace(headers={})),
        state=None,
        state_manager=_FakeStateManager(),
    )


def test_migrate_all_rules_create_enabled_flag_sets_create_disabled_false():
    _FakeRulesMigrator.create_rule_calls = []

    with patch("langsmith_migrator.core.migrators.RulesMigrator", _FakeRulesMigrator), patch(
        "langsmith_migrator.core.migrators.ChartMigrator", _FakeChartMigrator
    ), patch(
        "langsmith_migrator.cli.main.Progress", _DummyProgress
    ), patch("langsmith_migrator.cli.main.Confirm.ask", side_effect=[True]):
        cli_main._migrate_all_for_workspace(
            ctx=None,
            orchestrator=_make_orchestrator(),
            config=_make_config(),
            skip_datasets=True,
            skip_experiments=True,
            skip_prompts=True,
            skip_queues=True,
            skip_rules=False,
            skip_charts=True,
            include_all_commits=False,
            strip_projects=False,
            map_projects=False,
            rules_create_enabled=True,
        )

    assert _FakeRulesMigrator.create_rule_calls
    assert _FakeRulesMigrator.create_rule_calls[0]["create_disabled"] is False


def test_migrate_all_rules_prompt_defaults_to_disabled_when_flag_omitted():
    _FakeRulesMigrator.create_rule_calls = []

    with patch("langsmith_migrator.core.migrators.RulesMigrator", _FakeRulesMigrator), patch(
        "langsmith_migrator.core.migrators.ChartMigrator", _FakeChartMigrator
    ), patch(
        "langsmith_migrator.cli.main.Progress", _DummyProgress
    ), patch("langsmith_migrator.cli.main.Confirm.ask", side_effect=[True, False]):
        cli_main._migrate_all_for_workspace(
            ctx=None,
            orchestrator=_make_orchestrator(),
            config=_make_config(),
            skip_datasets=True,
            skip_experiments=True,
            skip_prompts=True,
            skip_queues=True,
            skip_rules=False,
            skip_charts=True,
            include_all_commits=False,
            strip_projects=False,
            map_projects=False,
            rules_create_enabled=None,
        )

    assert _FakeRulesMigrator.create_rule_calls
    assert _FakeRulesMigrator.create_rule_calls[0]["create_disabled"] is True
