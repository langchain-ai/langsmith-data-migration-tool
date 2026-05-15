"""Tests for ExperimentMigrator."""

from datetime import timedelta
from unittest.mock import Mock

from langsmith_migrator.core.migrators.experiment import ExperimentMigrator
from langsmith_migrator.utils.config import Config


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
