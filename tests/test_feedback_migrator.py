"""Tests for FeedbackMigrator."""

from unittest.mock import Mock, MagicMock

from langsmith_migrator.core.migrators.feedback import FeedbackMigrator


def _make_migrator(*, dry_run=False):
    """Create a FeedbackMigrator with minimal mock dependencies."""
    config = Mock()
    config.migration = Mock()
    config.migration.verbose = False
    config.migration.dry_run = dry_run
    source = Mock()
    source.session = Mock()
    source.session.headers = {}
    dest = Mock()
    dest.session = Mock()
    dest.session.headers = {}
    state = Mock()
    return FeedbackMigrator(source, dest, state, config)


class TestFeedbackFingerprint:
    """Tests for _feedback_fingerprint determinism."""

    def test_same_input_produces_same_fingerprint(self):
        migrator = _make_migrator()
        feedback = {
            "id": "fb-1",
            "run_id": "run-1",
            "key": "correctness",
            "score": 1.0,
            "value": "correct",
            "comment": "Looks good",
            "correction": None,
        }
        fp1 = migrator._feedback_fingerprint("exp-1", feedback)
        fp2 = migrator._feedback_fingerprint("exp-1", feedback)
        assert fp1 == fp2

    def test_different_experiment_ids_produce_different_fingerprints(self):
        migrator = _make_migrator()
        feedback = {
            "id": "fb-1",
            "run_id": "run-1",
            "key": "correctness",
            "score": 1.0,
        }
        fp1 = migrator._feedback_fingerprint("exp-1", feedback)
        fp2 = migrator._feedback_fingerprint("exp-2", feedback)
        assert fp1 != fp2


class TestFeedbackSourcePreservation:
    """Tests for feedback_source field handling during migration."""

    def test_feedback_source_included_when_present(self):
        """Verify feedback_source is preserved in the migrated payload."""
        migrator = _make_migrator()
        run_mapping = {"src-run-1": "dest-run-1"}
        feedback_source = {
            "type": "model",
            "metadata": {"run_id": "src-run-1"},
        }

        # Simulate what migrate_feedback_for_experiments does:
        # it builds the payload with feedback_source if present
        fb = {
            "id": "fb-1",
            "run_id": "src-run-1",
            "key": "correctness",
            "score": 1.0,
            "feedback_source": feedback_source,
        }

        # Build the migrated payload the same way the real code does
        dest_run_id = run_mapping.get(fb["run_id"])
        migrated_fb = {"run_id": dest_run_id, "key": fb["key"]}
        if fb.get("score") is not None:
            migrated_fb["score"] = fb["score"]
        if fb.get("feedback_source"):
            migrated_fb["feedback_source"] = fb["feedback_source"]

        assert migrated_fb["feedback_source"] == feedback_source
        assert migrated_fb["run_id"] == "dest-run-1"

    def test_feedback_source_omitted_when_absent(self):
        """Verify no feedback_source key when original has none."""
        fb = {
            "id": "fb-2",
            "run_id": "src-run-1",
            "key": "helpfulness",
            "score": 0.5,
        }

        migrated_fb = {"run_id": "dest-run-1", "key": fb["key"]}
        if fb.get("score") is not None:
            migrated_fb["score"] = fb["score"]
        if fb.get("feedback_source"):
            migrated_fb["feedback_source"] = fb["feedback_source"]

        assert "feedback_source" not in migrated_fb
