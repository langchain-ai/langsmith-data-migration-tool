"""Tests for ExperimentMigrator."""

from datetime import timedelta
from pathlib import Path
from unittest.mock import Mock

from langsmith_migrator.core.migrators.experiment import ExperimentMigrator
from langsmith_migrator.core.migrators.orchestrator import MigrationOrchestrator
from langsmith_migrator.utils.config import Config
from langsmith_migrator.utils.state import StateManager


def _make_migrator():
    config = Config(
        source_api_key="s",
        dest_api_key="d",
        source_url="https://s.test",
        dest_url="https://d.test",
    )
    config.migration.skip_existing = False
    source = Mock()
    dest = Mock()
    return ExperimentMigrator(source, dest, None, config)


class TestCreateExperimentTimeShift:
    def test_applies_delta_to_payload(self):
        migrator = _make_migrator()
        migrator.find_existing_experiment = Mock(return_value=None)
        migrator.dest.post = Mock(return_value={"id": "new-exp-id"})

        experiment = {
            "id": "src-exp",
            "name": "exp",
            "description": "",
            "start_time": "2026-02-03T00:00:00+00:00",
            "end_time": "2026-02-03T01:00:00+00:00",
            "extra": None,
            "trace_tier": None,
        }
        migrator.create_experiment(
            experiment, "dest-dataset-id", time_delta=timedelta(days=1)
        )

        sent_payload = migrator.dest.post.call_args[0][1]
        assert sent_payload["start_time"] == "2026-02-04T00:00:00.000000+00:00"
        assert sent_payload["end_time"] == "2026-02-04T01:00:00.000000+00:00"

    def test_no_delta_preserves_original_times(self):
        migrator = _make_migrator()
        migrator.find_existing_experiment = Mock(return_value=None)
        migrator.dest.post = Mock(return_value={"id": "new-exp-id"})

        experiment = {
            "id": "src-exp",
            "name": "exp",
            "start_time": "2026-02-03T00:00:00+00:00",
            "end_time": "2026-02-03T01:00:00+00:00",
        }
        migrator.create_experiment(experiment, "dest-dataset-id", time_delta=None)

        sent_payload = migrator.dest.post.call_args[0][1]
        assert sent_payload["start_time"] == "2026-02-03T00:00:00+00:00"
        assert sent_payload["end_time"] == "2026-02-03T01:00:00+00:00"


class TestMigrateRunsStreamingTimeShift:
    def test_runs_get_shifted_timestamps(self):
        migrator = _make_migrator()

        source_runs = [
            {
                "id": "run-a",
                "name": "a",
                "run_type": "chain",
                "session_id": "src-exp",
                "start_time": "2026-02-03T00:00:00+00:00",
                "end_time": "2026-02-03T00:00:05+00:00",
                "dotted_order": (
                    "20260203T000000000000Zaaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
                ),
                "trace_id": "run-a",
                "events": [{"name": "begin", "time": "2026-02-03T00:00:00+00:00"}],
            }
        ]

        migrator.source.post = Mock(
            return_value={"runs": source_runs, "cursors": {"next": None}}
        )

        captured_batches = []

        def fake_batch_post(path, payload):
            captured_batches.append(payload)
            return {"errors": []}

        migrator.dest.post = Mock(side_effect=fake_batch_post)

        migrator.migrate_runs_streaming(
            ["src-exp"],
            {
                "experiments": {"src-exp": "dest-exp"},
                "examples": {},
            },
            time_deltas={"src-exp": timedelta(days=1)},
        )

        batch_payload = captured_batches[0]
        sent_run = batch_payload["post"][0]
        assert sent_run["start_time"] == "2026-02-04T00:00:00.000000+00:00"
        assert sent_run["end_time"] == "2026-02-04T00:00:05.000000+00:00"
        assert sent_run["dotted_order"].startswith("20260204T000000000000Z")
        assert sent_run["events"][0]["time"] == "2026-02-04T00:00:00.000000+00:00"

    def test_no_delta_passes_original_times(self):
        migrator = _make_migrator()
        source_runs = [
            {
                "id": "run-a",
                "name": "a",
                "run_type": "chain",
                "session_id": "src-exp",
                "start_time": "2026-02-03T00:00:00+00:00",
                "end_time": "2026-02-03T00:00:05+00:00",
                "dotted_order": (
                    "20260203T000000000000Zaaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
                ),
                "trace_id": "run-a",
            }
        ]
        migrator.source.post = Mock(
            return_value={"runs": source_runs, "cursors": {"next": None}}
        )
        captured = []

        def fake_post(path, payload):
            captured.append(payload)
            return {"errors": []}

        migrator.dest.post = Mock(side_effect=fake_post)

        migrator.migrate_runs_streaming(
            ["src-exp"],
            {
                "experiments": {"src-exp": "dest-exp"},
                "examples": {},
            },
        )
        sent = captured[0]["post"][0]
        assert sent["start_time"] == "2026-02-03T00:00:00+00:00"


def _orchestrator(tmp_path: Path):
    config = Config(
        source_api_key="s", dest_api_key="d",
        source_url="https://s.test", dest_url="https://d.test",
    )
    state_manager = StateManager(state_dir=tmp_path / "state")
    return MigrationOrchestrator(config, state_manager), state_manager


class TestOrchestratorDeltaPersistence:
    def test_computes_and_persists_delta_on_first_attempt(self, tmp_path):
        orchestrator, _ = _orchestrator(tmp_path)
        state = orchestrator.ensure_state()
        item = state.ensure_item(
            "experiment_src-exp", "experiment", "exp", "src-exp",
            stage="create_experiment",
            workspace_pair={"source": None, "dest": None},
            metadata={"source_dataset_id": "src-ds", "dest_dataset_id": "dst-ds"},
        )

        experiment_payload = {
            "id": "src-exp", "name": "exp",
            "start_time": "2026-02-03T00:00:00+00:00",
            "end_time": "2026-02-03T01:00:00+00:00",
        }

        exp_mig = Mock()
        exp_mig.create_experiment = Mock(return_value="dest-exp")
        exp_mig.migrate_runs_streaming = Mock(return_value=(0, {}, 0))
        fb_mig = Mock()
        fb_mig.migrate_feedback_for_experiments = Mock(return_value=(0, 0))

        ok, _ = orchestrator._resolve_experiment_item(
            experiment_payload, "src-ds", "dst-ds", exp_mig, fb_mig,
        )
        assert ok is True

        passed_to_create = exp_mig.create_experiment.call_args.kwargs["time_delta"]
        passed_to_runs = exp_mig.migrate_runs_streaming.call_args.kwargs["time_deltas"]
        assert passed_to_create is not None
        assert passed_to_runs["src-exp"] == passed_to_create

        stored = state.get_item(item.id).metadata["time_shift_seconds"]
        assert isinstance(stored, (int, float))
        assert stored > 0

    def test_resume_reuses_persisted_delta(self, tmp_path):
        orchestrator, _ = _orchestrator(tmp_path)
        state = orchestrator.ensure_state()
        state.ensure_item(
            "experiment_src-exp", "experiment", "exp", "src-exp",
            stage="migrate_runs",
            workspace_pair={"source": None, "dest": None},
            metadata={
                "source_dataset_id": "src-ds",
                "dest_dataset_id": "dst-ds",
                "destination_experiment_id": "dest-exp",
                "time_shift_seconds": 86400.0,
            },
        )

        experiment_payload = {
            "id": "src-exp", "name": "exp",
            "start_time": "2026-02-03T00:00:00+00:00",
        }
        exp_mig = Mock()
        exp_mig.create_experiment = Mock(return_value="dest-exp")
        exp_mig.migrate_runs_streaming = Mock(return_value=(0, {}, 0))
        fb_mig = Mock()
        fb_mig.migrate_feedback_for_experiments = Mock(return_value=(0, 0))

        orchestrator._resolve_experiment_item(
            experiment_payload, "src-ds", "dst-ds", exp_mig, fb_mig,
        )

        passed = exp_mig.migrate_runs_streaming.call_args.kwargs["time_deltas"]
        assert passed["src-exp"] == timedelta(seconds=86400.0)

    def test_missing_anchor_skips_shift(self, tmp_path):
        orchestrator, _ = _orchestrator(tmp_path)
        state = orchestrator.ensure_state()
        state.ensure_item(
            "experiment_src-exp", "experiment", "exp", "src-exp",
            stage="create_experiment",
            workspace_pair={"source": None, "dest": None},
            metadata={"source_dataset_id": "src-ds", "dest_dataset_id": "dst-ds"},
        )

        experiment_payload = {"id": "src-exp", "name": "exp"}
        exp_mig = Mock()
        exp_mig.create_experiment = Mock(return_value="dest-exp")
        exp_mig.migrate_runs_streaming = Mock(return_value=(0, {}, 0))
        fb_mig = Mock()
        fb_mig.migrate_feedback_for_experiments = Mock(return_value=(0, 0))

        orchestrator._resolve_experiment_item(
            experiment_payload, "src-ds", "dst-ds", exp_mig, fb_mig,
        )

        assert exp_mig.create_experiment.call_args.kwargs["time_delta"] is None
        assert exp_mig.migrate_runs_streaming.call_args.kwargs["time_deltas"] is None
