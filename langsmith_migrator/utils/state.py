"""Migration state management for resume capability and remediation workflows."""

from __future__ import annotations

import json
import shutil
import re
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4


class MigrationStatus(Enum):
    """Status of a migration item."""

    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"


class ResolutionOutcome(Enum):
    """Terminal resolution state for a migration item."""

    MIGRATED = "migrated"
    MIGRATED_WITH_VERIFIED_DOWNGRADE = "migrated_with_verified_downgrade"
    BLOCKED_WITH_CHECKPOINT = "blocked_with_checkpoint"
    EXPORTED_WITH_MANUAL_APPLY = "exported_with_manual_apply"


class VerificationState(Enum):
    """Verification status for a migration item."""

    PENDING = "pending"
    VERIFIED = "verified"
    DEGRADED = "degraded"
    BLOCKED = "blocked"
    EXPORTED = "exported"


MANUAL_FOLLOW_UP_STATES = {
    ResolutionOutcome.BLOCKED_WITH_CHECKPOINT.value,
    ResolutionOutcome.EXPORTED_WITH_MANUAL_APPLY.value,
}


class IssueClass(Enum):
    """Typed incident classes for resolver workflows."""

    TRANSIENT = "transient"
    CAPABILITY = "capability"
    DEPENDENCY = "dependency"
    AMBIGUITY = "ambiguity"
    SOURCE_DATA_GAP = "source_data_gap"
    POST_WRITE_VERIFICATION = "post_write_verification"


REPAIR_POLICIES = {
    IssueClass.TRANSIENT.value: "auto_retry_with_backoff",
    IssueClass.CAPABILITY.value: "switch_strategy_or_export_manual_apply",
    IssueClass.DEPENDENCY.value: "auto_repair_if_deterministic_otherwise_guided_remediation",
    IssueClass.AMBIGUITY.value: "require_explicit_operator_choice",
    IssueClass.SOURCE_DATA_GAP.value: "attempt_enrichment_once_then_export_missing_fields",
    IssueClass.POST_WRITE_VERIFICATION.value: "retry_verification_once_then_mark_degraded_or_blocked",
}


def _now() -> float:
    return time.time()


def _safe_name(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", value).strip("._")
    return cleaned or "artifact"


@dataclass
class MigrationIssue:
    """A typed issue captured during migration resolution."""

    id: str
    issue_class: str
    code: str
    summary: str
    repair_policy: str
    created_at: float
    item_id: Optional[str] = None
    next_action: Optional[str] = None
    evidence: Dict[str, Any] = field(default_factory=dict)
    workspace_pair: Dict[str, Optional[str]] = field(default_factory=dict)
    export_path: Optional[str] = None
    resolved: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "issue_class": self.issue_class,
            "code": self.code,
            "summary": self.summary,
            "repair_policy": self.repair_policy,
            "created_at": self.created_at,
            "item_id": self.item_id,
            "next_action": self.next_action,
            "evidence": self.evidence,
            "workspace_pair": self.workspace_pair,
            "export_path": self.export_path,
            "resolved": self.resolved,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MigrationIssue":
        return cls(
            id=data["id"],
            issue_class=data["issue_class"],
            code=data["code"],
            summary=data["summary"],
            repair_policy=data.get(
                "repair_policy",
                REPAIR_POLICIES.get(data.get("issue_class", ""), "manual_review"),
            ),
            created_at=data.get("created_at", _now()),
            item_id=data.get("item_id"),
            next_action=data.get("next_action"),
            evidence=data.get("evidence", {}),
            workspace_pair=data.get("workspace_pair", {}),
            export_path=data.get("export_path"),
            resolved=data.get("resolved", False),
        )


@dataclass
class RemediationTask:
    """A resumable remediation task derived from a migration issue."""

    id: str
    item_id: Optional[str]
    issue_id: str
    next_action: str
    created_at: float
    status: str = "pending"
    export_path: Optional[str] = None
    command: Optional[str] = None
    requires_interaction: bool = False

    def to_dict(self) -> Dict[str, Any]:
        return {
            "id": self.id,
            "item_id": self.item_id,
            "issue_id": self.issue_id,
            "next_action": self.next_action,
            "created_at": self.created_at,
            "status": self.status,
            "export_path": self.export_path,
            "command": self.command,
            "requires_interaction": self.requires_interaction,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "RemediationTask":
        return cls(
            id=data["id"],
            item_id=data.get("item_id"),
            issue_id=data["issue_id"],
            next_action=data["next_action"],
            created_at=data.get("created_at", _now()),
            status=data.get("status", "pending"),
            export_path=data.get("export_path"),
            command=data.get("command"),
            requires_interaction=data.get("requires_interaction", False),
        )


@dataclass
class MigrationItem:
    """Represents an item being migrated."""

    id: str
    type: str  # dataset, experiment, queue, prompt, etc.
    name: str
    source_id: str
    destination_id: Optional[str] = None
    status: MigrationStatus = MigrationStatus.PENDING
    error: Optional[str] = None
    attempts: int = 0
    last_attempt: Optional[float] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    resource_type: Optional[str] = None
    stage: str = "pending"
    workspace_pair: Dict[str, Optional[str]] = field(default_factory=dict)
    dependencies: List[str] = field(default_factory=list)
    strategy: Optional[str] = None
    outcome_code: Optional[str] = None
    terminal_state: Optional[str] = None
    next_action: Optional[str] = None
    evidence: Dict[str, Any] = field(default_factory=dict)
    export_path: Optional[str] = None
    verification_state: str = VerificationState.PENDING.value

    def __post_init__(self) -> None:
        if self.resource_type is None:
            self.resource_type = self.type

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        return {
            "id": self.id,
            "type": self.type,
            "name": self.name,
            "source_id": self.source_id,
            "destination_id": self.destination_id,
            "status": self.status.value,
            "error": self.error,
            "attempts": self.attempts,
            "last_attempt": self.last_attempt,
            "metadata": self.metadata,
            "resource_type": self.resource_type,
            "stage": self.stage,
            "workspace_pair": self.workspace_pair,
            "dependencies": self.dependencies,
            "strategy": self.strategy,
            "outcome_code": self.outcome_code,
            "terminal_state": self.terminal_state,
            "next_action": self.next_action,
            "evidence": self.evidence,
            "export_path": self.export_path,
            "verification_state": self.verification_state,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MigrationItem":
        """Create from dictionary."""
        return cls(
            id=data["id"],
            type=data["type"],
            name=data["name"],
            source_id=data["source_id"],
            destination_id=data.get("destination_id"),
            status=MigrationStatus(data["status"]),
            error=data.get("error"),
            attempts=data.get("attempts", 0),
            last_attempt=data.get("last_attempt"),
            metadata=data.get("metadata", {}),
            resource_type=data.get("resource_type"),
            stage=data.get("stage", "pending"),
            workspace_pair=data.get("workspace_pair", {}),
            dependencies=data.get("dependencies", []),
            strategy=data.get("strategy"),
            outcome_code=data.get("outcome_code"),
            terminal_state=data.get("terminal_state"),
            next_action=data.get("next_action"),
            evidence=data.get("evidence", {}),
            export_path=data.get("export_path"),
            verification_state=data.get(
                "verification_state",
                VerificationState.PENDING.value,
            ),
        )


@dataclass
class MigrationState:
    """Tracks the state of an entire migration session."""

    session_id: str
    started_at: float
    updated_at: float
    source_url: str
    destination_url: str
    items: Dict[str, MigrationItem] = field(default_factory=dict)
    id_mappings: Dict[str, Dict[str, str]] = field(default_factory=dict)
    statistics: Dict[str, int] = field(default_factory=dict)
    source_workspace_id: Optional[str] = None
    dest_workspace_id: Optional[str] = None
    workspace_mapping: Dict[str, str] = field(default_factory=dict)
    schema_version: int = 2
    capability_matrix: Dict[str, Any] = field(default_factory=dict)
    inventory_snapshot: Dict[str, Any] = field(default_factory=dict)
    dependency_graph: Dict[str, List[str]] = field(default_factory=dict)
    issue_log: List[MigrationIssue] = field(default_factory=list)
    remediation_queue: List[RemediationTask] = field(default_factory=list)
    verification_summary: Dict[str, Any] = field(default_factory=dict)
    remediation_bundle_path: Optional[str] = None
    resolution_decisions: Dict[str, Any] = field(default_factory=dict)
    resolution_provenance: Dict[str, Any] = field(default_factory=dict)

    def touch(self) -> None:
        self.updated_at = _now()

    def add_item(self, item: MigrationItem) -> MigrationItem:
        """Add or replace an item to track."""
        self.items[item.id] = item
        self.touch()
        return item

    def ensure_item(
        self,
        item_id: str,
        item_type: str,
        name: str,
        source_id: str,
        **kwargs: Any,
    ) -> MigrationItem:
        """Get or create a tracked item."""
        if item_id in self.items:
            item = self.items[item_id]
            if kwargs.get("stage"):
                item.stage = kwargs["stage"]
            if kwargs.get("workspace_pair"):
                item.workspace_pair = kwargs["workspace_pair"]
            if kwargs.get("strategy"):
                item.strategy = kwargs["strategy"]
            if kwargs.get("dependencies"):
                item.dependencies = kwargs["dependencies"]
            if kwargs.get("metadata"):
                item.metadata.update(kwargs["metadata"])
            self.touch()
            return item

        item = MigrationItem(
            id=item_id,
            type=item_type,
            name=name,
            source_id=source_id,
            resource_type=kwargs.get("resource_type", item_type),
            stage=kwargs.get("stage", "pending"),
            workspace_pair=kwargs.get("workspace_pair", {}),
            dependencies=kwargs.get("dependencies", []),
            strategy=kwargs.get("strategy"),
            metadata=kwargs.get("metadata", {}),
        )
        return self.add_item(item)

    def get_item(self, item_id: str) -> Optional[MigrationItem]:
        return self.items.get(item_id)

    def get_mapped_id(self, item_type: str, source_id: str) -> Optional[str]:
        """Return the destination ID for a previously migrated item."""
        return self.id_mappings.get(item_type, {}).get(source_id)

    def set_mapped_id(self, item_type: str, source_id: str, destination_id: str) -> None:
        """Store a source to destination ID mapping."""
        if item_type not in self.id_mappings:
            self.id_mappings[item_type] = {}
        self.id_mappings[item_type][source_id] = destination_id
        self.touch()

    def update_item_status(
        self,
        item_id: str,
        status: MigrationStatus,
        destination_id: Optional[str] = None,
        error: Optional[str] = None,
        stage: Optional[str] = None,
    ) -> None:
        """Update the status of an item."""
        if item_id not in self.items:
            return

        item = self.items[item_id]
        item.status = status
        item.last_attempt = _now()
        item.attempts += 1

        if destination_id:
            item.destination_id = destination_id
            self.set_mapped_id(item.type, item.source_id, destination_id)

        if error is not None:
            item.error = error

        if stage is not None:
            item.stage = stage

        self.touch()

    def update_item_checkpoint(self, item_id: str, **kwargs: Any) -> None:
        """Persist non-status checkpoint details for an item."""
        item = self.items.get(item_id)
        if not item:
            return

        for field_name in (
            "destination_id",
            "stage",
            "strategy",
            "next_action",
            "outcome_code",
            "terminal_state",
            "export_path",
            "verification_state",
            "error",
        ):
            if field_name in kwargs and kwargs[field_name] is not None:
                setattr(item, field_name, kwargs[field_name])

        if "workspace_pair" in kwargs and kwargs["workspace_pair"] is not None:
            item.workspace_pair = kwargs["workspace_pair"]
        if "dependencies" in kwargs and kwargs["dependencies"] is not None:
            item.dependencies = list(kwargs["dependencies"])
        if "metadata" in kwargs and kwargs["metadata"] is not None:
            item.metadata.update(kwargs["metadata"])
        if "evidence" in kwargs and kwargs["evidence"] is not None:
            item.evidence.update(kwargs["evidence"])

        if item.destination_id:
            self.set_mapped_id(item.type, item.source_id, item.destination_id)
        self.touch()

    def mark_terminal(
        self,
        item_id: str,
        terminal_state: ResolutionOutcome,
        outcome_code: str,
        *,
        verification_state: VerificationState,
        next_action: Optional[str] = None,
        evidence: Optional[Dict[str, Any]] = None,
        export_path: Optional[str] = None,
        error: Optional[str] = None,
    ) -> None:
        """Mark an item with a terminal resolution state."""
        item = self.items.get(item_id)
        if not item:
            return

        item.terminal_state = terminal_state.value
        item.outcome_code = outcome_code
        item.verification_state = verification_state.value
        item.next_action = next_action
        item.export_path = export_path
        if evidence:
            item.evidence.update(evidence)
        if error is not None:
            item.error = error

        if terminal_state in (
            ResolutionOutcome.MIGRATED,
            ResolutionOutcome.MIGRATED_WITH_VERIFIED_DOWNGRADE,
        ):
            item.status = MigrationStatus.COMPLETED
        else:
            item.status = MigrationStatus.SKIPPED

        item.last_attempt = _now()
        self.refresh_verification_summary()
        self.touch()

    def get_pending_items(
        self,
        item_type: Optional[str] = None,
        include_in_progress: bool = False,
    ) -> List[MigrationItem]:
        """Get all pending items, optionally filtered by type."""
        statuses = {MigrationStatus.PENDING}
        if include_in_progress:
            statuses.add(MigrationStatus.IN_PROGRESS)

        items = []
        for item in self.items.values():
            if item.status in statuses:
                if item_type is None or item.type == item_type:
                    items.append(item)
        return items

    def get_failed_items(self, max_attempts: int = 3) -> List[MigrationItem]:
        """Get failed items that haven't exceeded max attempts."""
        items = []
        for item in self.items.values():
            if item.status == MigrationStatus.FAILED and item.attempts < max_attempts:
                items.append(item)
        return items

    def get_resume_items(self, max_attempts: int = 3) -> List[MigrationItem]:
        """Return items that should be reconsidered by resume."""
        resumable: Dict[str, MigrationItem] = {}
        for item in self.get_pending_items(include_in_progress=True):
            resumable[item.id] = item
        for item in self.get_failed_items(max_attempts=max_attempts):
            resumable[item.id] = item
        for item in self.get_checkpoint_items():
            resumable[item.id] = item
        return list(resumable.values())

    def get_active_remediation_tasks(self) -> List[RemediationTask]:
        """Return remediation tasks that still require operator follow-up."""
        active_tasks: List[RemediationTask] = []
        for task in self.remediation_queue:
            if task.status not in {"pending", "in_progress"}:
                continue
            if not task.item_id:
                active_tasks.append(task)
                continue
            item = self.items.get(task.item_id)
            if item is None:
                active_tasks.append(task)
                continue
            if (
                item.status == MigrationStatus.COMPLETED
                or item.terminal_state
                in (
                    ResolutionOutcome.MIGRATED.value,
                    ResolutionOutcome.MIGRATED_WITH_VERIFIED_DOWNGRADE.value,
                )
            ):
                continue
            active_tasks.append(task)
        return active_tasks

    def get_checkpoint_items(self) -> List[MigrationItem]:
        """Return items that landed in a checkpoint/export terminal state."""
        items = []
        for item in self.items.values():
            if item.terminal_state in MANUAL_FOLLOW_UP_STATES:
                items.append(item)
        return items

    def record_capability(
        self,
        scope: str,
        capability: str,
        *,
        supported: Optional[bool],
        detail: Optional[str] = None,
        evidence: Optional[Dict[str, Any]] = None,
        probe: Optional[str] = None,
    ) -> None:
        """Record the result of a capability probe."""
        scope_entry = self.capability_matrix.setdefault(scope, {})
        scope_entry[capability] = {
            "supported": supported,
            "detail": detail,
            "evidence": evidence or {},
            "probe": probe,
            "recorded_at": _now(),
        }
        self.touch()

    def record_inventory(self, key: str, value: Any) -> None:
        """Store an inventory snapshot entry."""
        self.inventory_snapshot[key] = value
        self.touch()

    def add_dependency(self, node: str, depends_on: str) -> None:
        """Add a directed dependency edge."""
        deps = self.dependency_graph.setdefault(node, [])
        if depends_on not in deps:
            deps.append(depends_on)
            self.touch()

    def set_dependency_graph(self, graph: Dict[str, List[str]]) -> None:
        self.dependency_graph = graph
        self.touch()

    def record_resolution_decision(self, key: str, value: Any) -> None:
        self.resolution_decisions[key] = value
        self.touch()

    def record_resolution_provenance(self, key: str, value: Any) -> None:
        self.resolution_provenance[key] = value
        self.touch()

    def add_issue(
        self,
        issue_class: str,
        code: str,
        summary: str,
        *,
        item_id: Optional[str] = None,
        next_action: Optional[str] = None,
        evidence: Optional[Dict[str, Any]] = None,
        workspace_pair: Optional[Dict[str, Optional[str]]] = None,
        export_path: Optional[str] = None,
        repair_policy: Optional[str] = None,
    ) -> MigrationIssue:
        """Record a typed issue."""
        issue = MigrationIssue(
            id=f"issue_{uuid4().hex}",
            issue_class=issue_class,
            code=code,
            summary=summary,
            repair_policy=repair_policy
            or REPAIR_POLICIES.get(issue_class, "manual_review"),
            created_at=_now(),
            item_id=item_id,
            next_action=next_action,
            evidence=evidence or {},
            workspace_pair=workspace_pair or {},
            export_path=export_path,
        )
        self.issue_log.append(issue)
        self.touch()
        return issue

    def queue_remediation(
        self,
        *,
        issue_id: str,
        next_action: str,
        item_id: Optional[str] = None,
        export_path: Optional[str] = None,
        command: Optional[str] = None,
        requires_interaction: bool = False,
    ) -> RemediationTask:
        """Queue a resumable remediation task."""
        task = RemediationTask(
            id=f"remediation_{uuid4().hex}",
            item_id=item_id,
            issue_id=issue_id,
            next_action=next_action,
            created_at=_now(),
            export_path=export_path,
            command=command,
            requires_interaction=requires_interaction,
        )
        self.remediation_queue.append(task)
        self.touch()
        return task

    def get_terminal_counts(self) -> Dict[str, int]:
        counts = {outcome.value: 0 for outcome in ResolutionOutcome}
        for item in self.items.values():
            if item.terminal_state in counts:
                counts[item.terminal_state] += 1
        return counts

    def refresh_verification_summary(self) -> None:
        """Recompute verification counts."""
        summary = {state.value: 0 for state in VerificationState}
        for item in self.items.values():
            if item.verification_state in summary:
                summary[item.verification_state] += 1
        summary["total"] = len(self.items)
        self.verification_summary = summary

    def get_statistics(self) -> Dict[str, Any]:
        """Get migration statistics."""
        stats = {
            "total": len(self.items),
            "completed": 0,
            "failed": 0,
            "pending": 0,
            "in_progress": 0,
            "skipped": 0,
            "by_type": {},
            "terminal": self.get_terminal_counts(),
            "issues": len(self.issue_log),
            "remediation_tasks": len(self.remediation_queue),
        }

        for item in self.items.values():
            stats[item.status.value.lower()] += 1

            if item.type not in stats["by_type"]:
                stats["by_type"][item.type] = {
                    "total": 0,
                    "completed": 0,
                    "failed": 0,
                    "pending": 0,
                    "in_progress": 0,
                    "skipped": 0,
                }

            stats["by_type"][item.type]["total"] += 1
            stats["by_type"][item.type][item.status.value.lower()] += 1

        if stats["total"] > 0:
            stats["completion_percentage"] = (stats["completed"] / stats["total"]) * 100
        else:
            stats["completion_percentage"] = 0

        stats["elapsed_time"] = self.updated_at - self.started_at
        self.refresh_verification_summary()
        stats["verification"] = self.verification_summary
        return stats

    def _bundle_dir(self) -> Optional[Path]:
        if not self.remediation_bundle_path:
            return None
        return Path(self.remediation_bundle_path)

    def ensure_bundle_dir(self) -> Optional[Path]:
        """Ensure the remediation bundle directory exists."""
        bundle_dir = self._bundle_dir()
        if bundle_dir is None:
            return None
        bundle_dir.mkdir(parents=True, exist_ok=True)
        (bundle_dir / "artifacts").mkdir(parents=True, exist_ok=True)
        return bundle_dir

    def export_artifact(
        self,
        item_id: str,
        name: str,
        payload: Any,
        *,
        extension: Optional[str] = None,
    ) -> Optional[str]:
        """Write a remediation artifact and return its absolute path."""
        bundle_dir = self.ensure_bundle_dir()
        if bundle_dir is None:
            return None

        item_safe = _safe_name(item_id)
        name_safe = _safe_name(name)
        if extension is None:
            extension = "md" if isinstance(payload, str) else "json"

        path = bundle_dir / "artifacts" / f"{item_safe}_{name_safe}.{extension}"
        if isinstance(payload, str):
            path.write_text(payload, encoding="utf-8")
        else:
            path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        return str(path.resolve())

    def write_remediation_bundle(self) -> Optional[Path]:
        """Refresh the remediation bundle files on disk."""
        bundle_dir = self.ensure_bundle_dir()
        if bundle_dir is None:
            return None

        issues_path = bundle_dir / "issues.json"
        issues_path.write_text(
            json.dumps([issue.to_dict() for issue in self.issue_log], indent=2, sort_keys=True),
            encoding="utf-8",
        )

        state_items_path = bundle_dir / "items.json"
        state_items_path.write_text(
            json.dumps({key: item.to_dict() for key, item in self.items.items()}, indent=2, sort_keys=True),
            encoding="utf-8",
        )

        stats = self.get_statistics()
        checkpoint_items = self.get_checkpoint_items()
        resume_command = "langsmith-migrator resume"
        lines = [
            f"# Remediation Summary: {self.session_id}",
            "",
            f"- Session ID: `{self.session_id}`",
            f"- Source: `{self.source_url}`",
            f"- Destination: `{self.destination_url}`",
            f"- Migrated: {stats['terminal'][ResolutionOutcome.MIGRATED.value]}",
            f"- Verified downgrade: {stats['terminal'][ResolutionOutcome.MIGRATED_WITH_VERIFIED_DOWNGRADE.value]}",
            f"- Blocked: {stats['terminal'][ResolutionOutcome.BLOCKED_WITH_CHECKPOINT.value]}",
            f"- Exported/manual apply: {stats['terminal'][ResolutionOutcome.EXPORTED_WITH_MANUAL_APPLY.value]}",
            f"- Issues: {len(self.issue_log)}",
            f"- Remediation tasks: {len(self.remediation_queue)}",
            f"- Resume command: `{resume_command}`",
            "",
        ]

        if checkpoint_items:
            lines.append("## Actionable Items")
            lines.append("")
            for item in checkpoint_items:
                lines.append(f"- `{item.id}` ({item.type}): {item.outcome_code or 'needs_attention'}")
                if item.next_action:
                    lines.append(f"  Next: {item.next_action}")
                if item.export_path:
                    lines.append(f"  Artifact: `{item.export_path}`")
            lines.append("")

        if self.remediation_queue:
            lines.append("## Remediation Queue")
            lines.append("")
            for task in self.remediation_queue:
                lines.append(f"- `{task.id}`: {task.next_action}")
                if task.command:
                    lines.append(f"  Command: `{task.command}`")
            lines.append("")

        (bundle_dir / "summary.md").write_text("\n".join(lines), encoding="utf-8")
        return bundle_dir

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary."""
        d = {
            "schema_version": self.schema_version,
            "session_id": self.session_id,
            "started_at": self.started_at,
            "updated_at": self.updated_at,
            "source_url": self.source_url,
            "destination_url": self.destination_url,
            "items": {k: v.to_dict() for k, v in self.items.items()},
            "id_mappings": self.id_mappings,
            "statistics": self.get_statistics(),
            "capability_matrix": self.capability_matrix,
            "inventory_snapshot": self.inventory_snapshot,
            "dependency_graph": self.dependency_graph,
            "issue_log": [issue.to_dict() for issue in self.issue_log],
            "remediation_queue": [task.to_dict() for task in self.remediation_queue],
            "verification_summary": self.verification_summary,
            "resolution_decisions": self.resolution_decisions,
            "resolution_provenance": self.resolution_provenance,
        }
        if self.source_workspace_id:
            d["source_workspace_id"] = self.source_workspace_id
        if self.dest_workspace_id:
            d["dest_workspace_id"] = self.dest_workspace_id
        if self.workspace_mapping:
            d["workspace_mapping"] = self.workspace_mapping
        if self.remediation_bundle_path:
            d["remediation_bundle_path"] = self.remediation_bundle_path
        return d

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "MigrationState":
        """Create from dictionary."""
        state = cls(
            session_id=data["session_id"],
            started_at=data["started_at"],
            updated_at=data["updated_at"],
            source_url=data["source_url"],
            destination_url=data["destination_url"],
            id_mappings=data.get("id_mappings", {}),
            source_workspace_id=data.get("source_workspace_id"),
            dest_workspace_id=data.get("dest_workspace_id"),
            workspace_mapping=data.get("workspace_mapping", {}),
            schema_version=max(data.get("schema_version", 1), 2),
            capability_matrix=data.get("capability_matrix", {}),
            inventory_snapshot=data.get("inventory_snapshot", {}),
            dependency_graph=data.get("dependency_graph", {}),
            verification_summary=data.get("verification_summary", {}),
            remediation_bundle_path=data.get("remediation_bundle_path"),
            resolution_decisions=data.get("resolution_decisions", {}),
            resolution_provenance=data.get("resolution_provenance", {}),
        )

        for item_id, item_data in data.get("items", {}).items():
            state.items[item_id] = MigrationItem.from_dict(item_data)

        state.issue_log = [
            MigrationIssue.from_dict(issue) for issue in data.get("issue_log", [])
        ]
        state.remediation_queue = [
            RemediationTask.from_dict(task)
            for task in data.get("remediation_queue", [])
        ]
        if not state.verification_summary:
            state.refresh_verification_summary()

        return state


class StateManager:
    """Manages migration state persistence."""

    def __init__(
        self,
        state_dir: Optional[Path] = None,
        remediation_dir: Optional[Path] = None,
    ):
        """
        Initialize state manager.

        Args:
            state_dir: Directory for state files
            remediation_dir: Directory for remediation bundles
        """
        self.state_dir = state_dir or Path.home() / ".langsmith-migrator" / "state"
        self.state_dir.mkdir(parents=True, exist_ok=True)
        if remediation_dir is not None:
            self.remediation_dir = remediation_dir
        elif state_dir is not None:
            self.remediation_dir = self.state_dir.parent / "remediation"
        else:
            self.remediation_dir = Path.cwd() / ".langsmith-migrator" / "remediation"
        self.remediation_dir.mkdir(parents=True, exist_ok=True)
        self.current_state: Optional[MigrationState] = None
        self.state_file: Optional[Path] = None

    def _default_bundle_path(self, session_id: str) -> Path:
        return self.remediation_dir / session_id

    def create_session(self, source_url: str, destination_url: str) -> MigrationState:
        """Create a new migration session."""
        session_id = f"migration_{int(_now())}"
        self.current_state = MigrationState(
            session_id=session_id,
            started_at=_now(),
            updated_at=_now(),
            source_url=source_url,
            destination_url=destination_url,
            remediation_bundle_path=str(self._default_bundle_path(session_id).resolve()),
        )

        self.state_file = self.state_dir / f"{session_id}.json"
        self.save()

        return self.current_state

    def load_session(self, session_id: str) -> Optional[MigrationState]:
        """Load an existing migration session."""
        state_file = self.state_dir / f"{session_id}.json"

        if not state_file.exists():
            return None

        with open(state_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        self.current_state = MigrationState.from_dict(data)
        if not self.current_state.remediation_bundle_path:
            self.current_state.remediation_bundle_path = str(
                self._default_bundle_path(session_id).resolve()
            )
        self.state_file = state_file

        return self.current_state

    def list_sessions(self) -> List[Dict[str, Any]]:
        """List all available migration sessions."""
        sessions = []

        for state_file in self.state_dir.glob("migration_*.json"):
            try:
                with open(state_file, "r", encoding="utf-8") as f:
                    data = json.load(f)

                stats = data.get("statistics", {})
                sessions.append(
                    {
                        "session_id": data["session_id"],
                        "started_at": data["started_at"],
                        "updated_at": data["updated_at"],
                        "source_url": data["source_url"],
                        "destination_url": data["destination_url"],
                        "statistics": stats,
                        "schema_version": data.get("schema_version", 1),
                        "remediation_bundle_path": data.get("remediation_bundle_path"),
                    }
                )
            except Exception:
                continue

        sessions.sort(key=lambda x: x["updated_at"], reverse=True)
        return sessions

    def save(self) -> None:
        """Save current state to disk."""
        if not self.current_state or not self.state_file:
            return

        self.current_state.touch()
        with open(self.state_file, "w", encoding="utf-8") as f:
            json.dump(self.current_state.to_dict(), f, indent=2)

        self.current_state.write_remediation_bundle()

    def delete_session(self, session_id: str) -> bool:
        """Delete a migration session."""
        state_file = self.state_dir / f"{session_id}.json"
        bundle_dir = self._default_bundle_path(session_id)
        deleted = False

        if state_file.exists():
            state_file.unlink()
            deleted = True

        if bundle_dir.exists():
            shutil.rmtree(bundle_dir, ignore_errors=True)
            deleted = True

        return deleted

    def get_resume_info(self, state: MigrationState) -> Dict[str, Any]:
        """Get information about what can be resumed."""
        stats = state.get_statistics()
        blocked = stats["terminal"].get(
            ResolutionOutcome.BLOCKED_WITH_CHECKPOINT.value, 0
        )
        exported = stats["terminal"].get(
            ResolutionOutcome.EXPORTED_WITH_MANUAL_APPLY.value, 0
        )
        resumable_items = state.get_resume_items()
        active_remediation_tasks = state.get_active_remediation_tasks()
        return {
            "session_id": state.session_id,
            "total_items": stats["total"],
            "completed": stats["completed"],
            "failed": stats["failed"],
            "pending": stats["pending"],
            "blocked": blocked,
            "exported": exported,
            "can_resume": len(resumable_items) > 0,
            "elapsed_time": stats["elapsed_time"],
            "by_type": stats["by_type"],
            "remediation_bundle_path": state.remediation_bundle_path,
            "remediation_tasks": len(active_remediation_tasks),
        }
