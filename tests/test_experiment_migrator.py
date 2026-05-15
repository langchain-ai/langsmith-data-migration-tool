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
