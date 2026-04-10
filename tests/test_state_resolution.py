"""Resolver-state and remediation bundle tests."""

from __future__ import annotations

import json
import time

from langsmith_migrator.utils.state import (
    MigrationState,
    ResolutionOutcome,
    StateManager,
    VerificationState,
)


def test_state_manager_writes_remediation_bundle_and_round_trips(tmp_path):
    """Saving resolver state should emit bundle files and preserve remediation metadata."""

    state_manager = StateManager(tmp_path / "state")
    state = state_manager.create_session("https://source.example", "https://dest.example")
    item = state.ensure_item(
        "prompt_team_prompt-a",
        "prompt",
        "team/prompt-a",
        "team/prompt-a",
        stage="manual_apply",
    )
    export_path = state.export_artifact(item.id, "manual_apply", {"prompt": "payload"})
    issue = state.add_issue(
        "capability",
        "prompt_write_unsupported",
        "Prompt writes are unavailable on the destination instance",
        item_id=item.id,
        next_action="Apply the exported prompt manually, then run resume.",
        export_path=export_path,
    )
    state.queue_remediation(
        issue_id=issue.id,
        item_id=item.id,
        next_action="Apply the exported prompt manually, then run resume.",
        export_path=export_path,
        command="langsmith-migrator resume",
    )
    state.mark_terminal(
        item.id,
        ResolutionOutcome.EXPORTED_WITH_MANUAL_APPLY,
        "prompt_write_unsupported",
        verification_state=VerificationState.EXPORTED,
        next_action="Apply the exported prompt manually, then run resume.",
        export_path=export_path,
    )
    state_manager.save()

    bundle_dir = tmp_path / "remediation" / state.session_id
    assert (bundle_dir / "summary.md").exists()
    assert (bundle_dir / "issues.json").exists()
    assert (bundle_dir / "items.json").exists()

    loaded = state_manager.load_session(state.session_id)
    assert loaded is not None
    assert loaded.schema_version == 2
    assert loaded.get_terminal_counts()[ResolutionOutcome.EXPORTED_WITH_MANUAL_APPLY.value] == 1
    assert loaded.remediation_queue[0].command == "langsmith-migrator resume"


def test_loading_v1_state_upgrades_to_schema_v2(tmp_path):
    """Legacy state payloads should load with schema v2 defaults and verification summary."""

    state_dir = tmp_path / "state"
    state_dir.mkdir(parents=True)
    state_payload = {
        "session_id": "migration_legacy",
        "started_at": time.time(),
        "updated_at": time.time(),
        "source_url": "https://source.example",
        "destination_url": "https://dest.example",
        "items": {
            "dataset_1": {
                "id": "dataset_1",
                "type": "dataset",
                "name": "Dataset One",
                "source_id": "dataset-1",
                "status": "pending",
                "metadata": {},
            }
        },
        "id_mappings": {},
        "statistics": {},
    }
    (state_dir / "migration_legacy.json").write_text(json.dumps(state_payload), encoding="utf-8")

    state_manager = StateManager(state_dir)
    loaded = state_manager.load_session("migration_legacy")

    assert loaded is not None
    assert loaded.schema_version == 2
    assert loaded.verification_summary["total"] == 1


def test_remediation_summary_groups_duplicate_actionable_items(tmp_path):
    """Remediation summaries should collapse repeated user-role failures into one actionable step."""

    state = MigrationState(
        session_id="migration_grouped_actions",
        started_at=time.time(),
        updated_at=time.time(),
        source_url="https://source.example",
        destination_url="https://dest.example",
        remediation_bundle_path=str(
            (tmp_path / "remediation" / "migration_grouped_actions").resolve()
        ),
    )

    for email in ("alice@example.com", "bob@example.com"):
        item = state.ensure_item(
            f"ws_member_ws-1_{email}",
            "ws_member",
            email,
            email,
        )
        state.mark_terminal(
            item.id,
            ResolutionOutcome.BLOCKED_WITH_CHECKPOINT,
            "ws_member_add_failed",
            verification_state=VerificationState.BLOCKED,
            next_action="Re-run `langsmith-migrator users`.",
            evidence={"email": email},
        )

    bundle_dir = state.write_remediation_bundle()

    assert bundle_dir is not None
    summary = (bundle_dir / "summary.md").read_text(encoding="utf-8")
    assert "Workspace memberships failed to add (2 items)" in summary
    assert "Affected: alice@example.com, bob@example.com" in summary
    assert (
        "Next: Review the workspace membership create error in the remediation "
        "bundle, then re-run `langsmith-migrator users`."
    ) in summary
