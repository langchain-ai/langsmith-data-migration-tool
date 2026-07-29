"""Retry-budget accounting on migration items."""

from __future__ import annotations

import time

from langsmith_migrator.utils.state import (
    MigrationState,
    MigrationStatus,
    ResolutionOutcome,
    VerificationState,
)


def _state() -> MigrationState:
    return MigrationState(
        session_id="test-session",
        started_at=time.time(),
        updated_at=time.time(),
        source_url="https://source.example",
        destination_url="https://dest.example",
    )


def _item(state: MigrationState, item_id: str = "experiment_exp-1"):
    return state.ensure_item(item_id, "experiment", "exp-1", "exp-1", stage="create_experiment")


def test_non_failure_transitions_do_not_consume_the_budget():
    """A single pass moves an item through several states; only failures should count."""
    state = _state()
    item = _item(state)

    state.update_item_status(item.id, MigrationStatus.IN_PROGRESS, stage="create_experiment")
    state.update_item_status(item.id, MigrationStatus.IN_PROGRESS, stage="migrate_runs")
    state.update_item_status(item.id, MigrationStatus.IN_PROGRESS, stage="migrate_feedback")

    assert item.attempts == 0

    state.update_item_status(item.id, MigrationStatus.FAILED, error="boom")

    assert item.attempts == 1


def test_an_item_that_failed_once_is_still_resumable():
    """The bug this guards: one failing pass used to exhaust a budget of three."""
    state = _state()
    item = _item(state)

    # A realistic failing pass: create, run replay, feedback, then fail.
    state.update_item_status(item.id, MigrationStatus.IN_PROGRESS, stage="create_experiment")
    state.update_item_status(item.id, MigrationStatus.IN_PROGRESS, stage="migrate_runs")
    state.update_item_status(item.id, MigrationStatus.IN_PROGRESS, stage="migrate_feedback")
    state.update_item_status(item.id, MigrationStatus.FAILED, error="feedback replay incomplete")

    assert [i.id for i in state.get_resume_items()] == [item.id]


def test_budget_is_exhausted_after_three_real_failures():
    state = _state()
    item = _item(state)

    for _ in range(3):
        state.update_item_status(item.id, MigrationStatus.FAILED, error="boom")

    assert item.attempts == 3
    assert state.get_resume_items() == []


def test_retry_exhausted_override_includes_spent_items():
    """`resume --retry-exhausted` must work without hand-editing state."""
    state = _state()
    item = _item(state)

    for _ in range(5):
        state.update_item_status(item.id, MigrationStatus.FAILED, error="boom")

    assert state.get_resume_items() == []
    assert [i.id for i in state.get_resume_items(max_attempts=None)] == [item.id]


def test_completed_items_are_never_resumed():
    state = _state()
    item = _item(state)

    state.update_item_status(item.id, MigrationStatus.FAILED, error="boom")
    state.update_item_status(item.id, MigrationStatus.COMPLETED, destination_id="dest-1")

    assert item.attempts == 1
    assert state.get_resume_items() == []


def test_attempts_survives_a_round_trip():
    state = _state()
    item = _item(state)
    state.update_item_status(item.id, MigrationStatus.FAILED, error="boom")

    restored = MigrationState.from_dict(state.to_dict())

    assert restored.items[item.id].attempts == 1


def test_blocked_items_are_not_resumed_but_are_actionable():
    """Blocked items stay out of the automatic retry set by design."""
    state = _state()
    item = _item(state)
    state.mark_terminal(
        item.id,
        ResolutionOutcome.BLOCKED_WITH_CHECKPOINT,
        "missing_dataset_dependency",
        verification_state=VerificationState.BLOCKED,
        next_action="Migrate the referenced dataset, then resume.",
    )

    assert state.get_resume_items() == []
    assert [i.id for i in state.get_checkpoint_items()] == [item.id]
