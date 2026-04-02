"""Tests for MigrationState status transitions and attempt counting."""

import json
import time
import pytest

from langsmith_migrator.utils.state import (
    MigrationItem,
    MigrationState,
    MigrationStatus,
    ResolutionOutcome,
    StateManager,
    VerificationState,
)


@pytest.fixture
def state():
    return MigrationState(
        session_id="test-session",
        started_at=time.time(),
        updated_at=time.time(),
        source_url="https://source.test.com",
        destination_url="https://dest.test.com",
    )


class TestUpdateItemStatus:
    """Tests for MigrationState.update_item_status."""

    def test_increments_attempts_only_on_failed(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        assert state.items["item_1"].attempts == 0

        # Non-failure transitions must NOT inflate attempts
        state.update_item_status("item_1", MigrationStatus.IN_PROGRESS)
        assert state.items["item_1"].attempts == 0

        state.update_item_status("item_1", MigrationStatus.COMPLETED)
        assert state.items["item_1"].attempts == 0

        # Only FAILED transitions increment attempts
        state.update_item_status("item_1", MigrationStatus.FAILED, error="boom")
        assert state.items["item_1"].attempts == 1

        state.update_item_status("item_1", MigrationStatus.FAILED, error="boom again")
        assert state.items["item_1"].attempts == 2

    def test_sets_last_attempt(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        before = time.time()
        state.update_item_status("item_1", MigrationStatus.IN_PROGRESS)
        after = time.time()
        assert before <= state.items["item_1"].last_attempt <= after

    def test_sets_destination_id_and_mapping(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.update_item_status("item_1", MigrationStatus.COMPLETED, destination_id="dest-1")
        assert state.items["item_1"].destination_id == "dest-1"
        assert state.id_mappings.get("dataset", {}).get("src-1") == "dest-1"

    def test_sets_error(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.update_item_status("item_1", MigrationStatus.FAILED, error="something broke")
        assert state.items["item_1"].error == "something broke"

    def test_sets_stage(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.update_item_status("item_1", MigrationStatus.IN_PROGRESS, stage="upload")
        assert state.items["item_1"].stage == "upload"

    def test_noop_for_missing_item(self, state):
        state.update_item_status("nonexistent", MigrationStatus.COMPLETED)


class TestMarkTerminal:
    """Tests for MigrationState.mark_terminal."""

    def test_does_not_increment_attempts(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.update_item_status("item_1", MigrationStatus.IN_PROGRESS)
        attempts_before = state.items["item_1"].attempts

        state.mark_terminal(
            "item_1",
            ResolutionOutcome.MIGRATED,
            "dataset_migrated",
            verification_state=VerificationState.VERIFIED,
        )
        assert state.items["item_1"].attempts == attempts_before

    def test_migrated_sets_completed(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.mark_terminal(
            "item_1",
            ResolutionOutcome.MIGRATED,
            "ok",
            verification_state=VerificationState.VERIFIED,
        )
        assert state.items["item_1"].status == MigrationStatus.COMPLETED


class TestStateManagerLoad:
    """Tests for StateManager save/load roundtrip."""

    def test_save_and_load_roundtrip(self, tmp_path):
        """State saved to disk can be loaded back with all fields intact."""
        sm = StateManager(state_dir=tmp_path)
        state = sm.create_session("https://source.test", "https://dest.test")
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.update_item_status("item_1", MigrationStatus.FAILED, error="test error")
        sm.save()

        # Load it back
        sessions = sm.list_sessions()
        assert len(sessions) >= 1
        loaded = sm.load_session(state.session_id)
        assert loaded is not None
        assert "item_1" in loaded.items
        assert loaded.items["item_1"].status == MigrationStatus.FAILED
        assert loaded.items["item_1"].error == "test error"
        assert loaded.items["item_1"].attempts == 1


class TestIssueTracking:
    """Tests for MigrationState.add_issue and queue_remediation."""

    def test_add_issue_creates_issue(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        issue = state.add_issue(
            "transient", "migration_failed", "Test issue",
            item_id="item_1", next_action="Retry"
        )
        assert issue.id
        assert issue.issue_class == "transient"
        assert issue.code == "migration_failed"
        assert len(state.issue_log) == 1

    def test_queue_remediation(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        issue = state.add_issue("transient", "fail", "Test", item_id="item_1")
        state.queue_remediation(
            issue_id=issue.id, item_id="item_1",
            next_action="Retry migration", command="langsmith-migrator resume"
        )
        assert len(state.remediation_queue) == 1
        task = state.remediation_queue[0]
        assert task.issue_id == issue.id
        assert task.command == "langsmith-migrator resume"


class TestGetCheckpointItems:
    """Tests for MigrationState.get_checkpoint_items."""

    def test_returns_blocked_items(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.mark_terminal(
            "item_1", ResolutionOutcome.BLOCKED_WITH_CHECKPOINT,
            "missing_dep", verification_state=VerificationState.BLOCKED
        )
        items = state.get_checkpoint_items()
        assert len(items) == 1
        assert items[0].id == "item_1"

    def test_excludes_migrated_items(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.mark_terminal(
            "item_1", ResolutionOutcome.MIGRATED,
            "ok", verification_state=VerificationState.VERIFIED
        )
        items = state.get_checkpoint_items()
        assert len(items) == 0
        assert state.items["item_1"].terminal_state == "migrated"

    def test_blocked_sets_skipped(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.mark_terminal(
            "item_1",
            ResolutionOutcome.BLOCKED_WITH_CHECKPOINT,
            "missing_dep",
            verification_state=VerificationState.BLOCKED,
        )
        assert state.items["item_1"].status == MigrationStatus.SKIPPED

    def test_sets_evidence(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.mark_terminal(
            "item_1",
            ResolutionOutcome.MIGRATED,
            "ok",
            verification_state=VerificationState.VERIFIED,
            evidence={"rows": 42},
        )
        assert state.items["item_1"].evidence["rows"] == 42

    def test_sets_error(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.mark_terminal(
            "item_1",
            ResolutionOutcome.BLOCKED_WITH_CHECKPOINT,
            "fail",
            verification_state=VerificationState.BLOCKED,
            error="dataset missing",
        )
        assert state.items["item_1"].error == "dataset missing"

    def test_noop_for_missing_item(self, state):
        state.mark_terminal(
            "nonexistent",
            ResolutionOutcome.MIGRATED,
            "ok",
            verification_state=VerificationState.VERIFIED,
        )

    def test_does_not_overwrite_migrated_state(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.mark_terminal(
            "item_1",
            ResolutionOutcome.MIGRATED,
            "ok",
            verification_state=VerificationState.VERIFIED,
        )
        assert state.items["item_1"].terminal_state == "migrated"
        assert state.items["item_1"].status == MigrationStatus.COMPLETED

        # Attempt to overwrite with BLOCKED — should be silently ignored
        state.mark_terminal(
            "item_1",
            ResolutionOutcome.BLOCKED_WITH_CHECKPOINT,
            "missing_dep",
            verification_state=VerificationState.BLOCKED,
        )
        assert state.items["item_1"].terminal_state == "migrated"
        assert state.items["item_1"].status == MigrationStatus.COMPLETED

    def test_allows_marking_non_terminal_item(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        assert state.items["item_1"].terminal_state is None

        state.mark_terminal(
            "item_1",
            ResolutionOutcome.BLOCKED_WITH_CHECKPOINT,
            "missing_dep",
            verification_state=VerificationState.BLOCKED,
        )
        assert state.items["item_1"].terminal_state == "blocked_with_checkpoint"
        assert state.items["item_1"].status == MigrationStatus.SKIPPED

    def test_allows_marking_blocked_to_migrated(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.mark_terminal(
            "item_1",
            ResolutionOutcome.BLOCKED_WITH_CHECKPOINT,
            "missing_dep",
            verification_state=VerificationState.BLOCKED,
        )
        assert state.items["item_1"].terminal_state == "blocked_with_checkpoint"
        assert state.items["item_1"].status == MigrationStatus.SKIPPED

        # Upgrading from BLOCKED to MIGRATED should be allowed
        state.mark_terminal(
            "item_1",
            ResolutionOutcome.MIGRATED,
            "ok",
            verification_state=VerificationState.VERIFIED,
        )
        assert state.items["item_1"].terminal_state == "migrated"
        assert state.items["item_1"].status == MigrationStatus.COMPLETED


class TestGetFailedItems:
    """Tests for max_attempts boundary in get_failed_items."""

    def test_includes_under_max(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.update_item_status("item_1", MigrationStatus.FAILED)
        state.update_item_status("item_1", MigrationStatus.FAILED)
        assert state.items["item_1"].attempts == 2
        assert len(state.get_failed_items(max_attempts=3)) == 1

    def test_excludes_at_max(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.update_item_status("item_1", MigrationStatus.FAILED)
        state.update_item_status("item_1", MigrationStatus.FAILED)
        state.update_item_status("item_1", MigrationStatus.FAILED)
        assert state.items["item_1"].attempts == 3
        assert len(state.get_failed_items(max_attempts=3)) == 0

    def test_excludes_completed_items(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.update_item_status("item_1", MigrationStatus.COMPLETED)
        assert len(state.get_failed_items(max_attempts=3)) == 0


class TestGetResumeItems:
    """Tests for get_resume_items combining pending + failed."""

    def test_includes_pending(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        items = state.get_resume_items()
        assert len(items) == 1

    def test_includes_in_progress(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.update_item_status("item_1", MigrationStatus.IN_PROGRESS)
        items = state.get_resume_items()
        assert len(items) == 1

    def test_includes_failed_under_max(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.update_item_status("item_1", MigrationStatus.FAILED)
        items = state.get_resume_items(max_attempts=3)
        assert len(items) == 1

    def test_excludes_completed(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.update_item_status("item_1", MigrationStatus.COMPLETED)
        items = state.get_resume_items()
        assert len(items) == 0

    def test_excludes_items_with_terminal_state(self, state):
        """Items marked BLOCKED_WITH_CHECKPOINT should not appear in resume."""
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.update_item_status("item_1", MigrationStatus.IN_PROGRESS)
        state.mark_terminal(
            "item_1",
            ResolutionOutcome.BLOCKED_WITH_CHECKPOINT,
            "missing_dep",
            verification_state=VerificationState.BLOCKED,
        )
        items = state.get_resume_items()
        assert len(items) == 0

    def test_excludes_completed_terminal_items(self, state):
        """Items marked MIGRATED should not appear in resume."""
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.update_item_status("item_1", MigrationStatus.IN_PROGRESS)
        state.mark_terminal(
            "item_1",
            ResolutionOutcome.MIGRATED,
            "ok",
            verification_state=VerificationState.VERIFIED,
        )
        items = state.get_resume_items()
        assert len(items) == 0


class TestUpdateItemCheckpoint:
    """Tests for update_item_checkpoint metadata merge."""

    def test_merges_metadata(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.update_item_checkpoint("item_1", metadata={"key1": "val1"})
        state.update_item_checkpoint("item_1", metadata={"key2": "val2"})
        item = state.items["item_1"]
        assert item.metadata["key1"] == "val1"
        assert item.metadata["key2"] == "val2"

    def test_overwrites_same_metadata_key(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.update_item_checkpoint("item_1", metadata={"key1": "old"})
        state.update_item_checkpoint("item_1", metadata={"key1": "new"})
        assert state.items["item_1"].metadata["key1"] == "new"

    def test_sets_destination_id(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.update_item_checkpoint("item_1", destination_id="dest-1")
        assert state.items["item_1"].destination_id == "dest-1"

    def test_does_not_increment_attempts(self, state):
        state.ensure_item("item_1", "dataset", "ds1", "src-1")
        state.update_item_checkpoint("item_1", metadata={"key": "val"})
        assert state.items["item_1"].attempts == 0


class TestEnsureItem:
    """Tests for ensure_item creation and idempotency."""

    def test_creates_item(self, state):
        item = state.ensure_item("item_1", "dataset", "ds1", "src-1")
        assert item.id == "item_1"
        assert item.type == "dataset"
        assert item.status == MigrationStatus.PENDING

    def test_idempotent(self, state):
        item1 = state.ensure_item("item_1", "dataset", "ds1", "src-1")
        item2 = state.ensure_item("item_1", "dataset", "ds1", "src-1")
        assert item1 is item2

    def test_stores_workspace_pair(self, state):
        wp = {"source": "ws-1", "dest": "ws-2"}
        item = state.ensure_item("item_1", "dataset", "ds1", "src-1", workspace_pair=wp)
        assert item.workspace_pair == wp


class TestAtomicSave:
    """Tests that StateManager.save() writes atomically and produces valid JSON."""

    def test_state_file_exists_after_save(self, tmp_path):
        mgr = StateManager(state_dir=tmp_path / "state", remediation_dir=tmp_path / "remediation")
        mgr.create_session("https://src.test", "https://dst.test")
        mgr.save()
        assert mgr.state_file.exists()

    def test_state_file_is_valid_json_after_save(self, tmp_path):
        mgr = StateManager(state_dir=tmp_path / "state", remediation_dir=tmp_path / "remediation")
        mgr.create_session("https://src.test", "https://dst.test")
        mgr.current_state.ensure_item("item_1", "dataset", "ds1", "src-1")
        mgr.save()

        raw = mgr.state_file.read_text(encoding="utf-8")
        data = json.loads(raw)
        assert data["session_id"] == mgr.current_state.session_id
        assert "items" in data

    def test_save_overwrites_previous_state(self, tmp_path):
        mgr = StateManager(state_dir=tmp_path / "state", remediation_dir=tmp_path / "remediation")
        mgr.create_session("https://src.test", "https://dst.test")
        mgr.save()

        mgr.current_state.ensure_item("item_1", "dataset", "ds1", "src-1")
        mgr.save()

        data = json.loads(mgr.state_file.read_text(encoding="utf-8"))
        assert "item_1" in data["items"]

    def test_no_temp_file_left_after_save(self, tmp_path):
        mgr = StateManager(state_dir=tmp_path / "state", remediation_dir=tmp_path / "remediation")
        mgr.create_session("https://src.test", "https://dst.test")
        mgr.save()

        tmp_files = list((tmp_path / "state").glob("*.tmp"))
        assert tmp_files == []
