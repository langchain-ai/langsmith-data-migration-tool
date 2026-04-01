"""Base migrator class with shared functionality."""

from typing import Any, Dict, Optional
from rich.console import Console

from ..api_client import EnhancedAPIClient
from ...utils.state import (
    MigrationState,
    ResolutionOutcome,
    VerificationState,
)


class BaseMigrator:
    """Base class for all migrators."""

    def __init__(
        self,
        source_client: EnhancedAPIClient,
        dest_client: EnhancedAPIClient,
        state: MigrationState,
        config: Any
    ):
        """Initialize base migrator."""
        self.source = source_client
        self.dest = dest_client
        self.state = state
        self.config = config
        self.console = Console()

    def log(self, message: str, level: str = "info"):
        """Log a message if verbose mode is enabled."""
        if not self.config.migration.verbose:
            return

        style = {
            "info": "dim",
            "success": "green",
            "warning": "yellow",
            "error": "red"
        }.get(level, "")

        self.console.print(f"[{style}]{message}[/{style}]")

    def workspace_pair(self) -> Dict[str, Optional[str]]:
        """Return the active workspace pair from request headers."""
        return {
            "source": self.source.session.headers.get("X-Tenant-Id"),
            "dest": self.dest.session.headers.get("X-Tenant-Id"),
        }

    def persist_state(self) -> None:
        """Persist state if a state manager is attached to the config."""
        if not self.state:
            return
        state_manager = getattr(self.config, "state_manager", None)
        if state_manager is None:
            return
        state_manager.current_state = self.state
        state_manager.save()

    def ensure_item(
        self,
        item_id: str,
        item_type: str,
        name: str,
        source_id: str,
        *,
        stage: str = "pending",
        strategy: Optional[str] = None,
        dependencies: Optional[list[str]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ):
        """Create a tracked item if state support is active."""
        if not self.state:
            return None
        item = self.state.ensure_item(
            item_id,
            item_type,
            name,
            source_id,
            stage=stage,
            strategy=strategy,
            dependencies=dependencies or [],
            metadata=metadata or {},
            workspace_pair=self.workspace_pair(),
        )
        self.persist_state()
        return item

    def checkpoint_item(self, item_id: str, **kwargs: Any) -> None:
        """Persist checkpoint details for a tracked item."""
        if not self.state:
            return
        if "workspace_pair" not in kwargs:
            kwargs["workspace_pair"] = self.workspace_pair()
        self.state.update_item_checkpoint(item_id, **kwargs)
        self.persist_state()

    def record_issue(
        self,
        issue_class: str,
        code: str,
        summary: str,
        *,
        item_id: Optional[str] = None,
        next_action: Optional[str] = None,
        evidence: Optional[Dict[str, Any]] = None,
        export_path: Optional[str] = None,
    ):
        """Record a typed issue into the current session state."""
        if not self.state:
            return None
        issue = self.state.add_issue(
            issue_class,
            code,
            summary,
            item_id=item_id,
            next_action=next_action,
            evidence=evidence,
            workspace_pair=self.workspace_pair(),
            export_path=export_path,
        )
        self.persist_state()
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
    ):
        """Queue a remediation action in state."""
        if not self.state:
            return None
        task = self.state.queue_remediation(
            issue_id=issue_id,
            next_action=next_action,
            item_id=item_id,
            export_path=export_path,
            command=command,
            requires_interaction=requires_interaction,
        )
        self.persist_state()
        return task

    def export_payload(
        self,
        item_id: str,
        name: str,
        payload: Any,
        *,
        extension: Optional[str] = None,
    ) -> Optional[str]:
        """Write a payload into the remediation bundle."""
        if not self.state:
            return None
        path = self.state.export_artifact(item_id, name, payload, extension=extension)
        self.persist_state()
        return path

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
        """Persist a terminal resolution state for an item."""
        if not self.state:
            return
        self.state.mark_terminal(
            item_id,
            terminal_state,
            outcome_code,
            verification_state=verification_state,
            next_action=next_action,
            evidence=evidence,
            export_path=export_path,
            error=error,
        )
        self.persist_state()

    def mark_migrated(
        self,
        item_id: str,
        *,
        outcome_code: str = "migrated",
        evidence: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.mark_terminal(
            item_id,
            ResolutionOutcome.MIGRATED,
            outcome_code,
            verification_state=VerificationState.VERIFIED,
            evidence=evidence,
        )

    def mark_degraded(
        self,
        item_id: str,
        outcome_code: str,
        *,
        next_action: Optional[str] = None,
        evidence: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.mark_terminal(
            item_id,
            ResolutionOutcome.MIGRATED_WITH_VERIFIED_DOWNGRADE,
            outcome_code,
            verification_state=VerificationState.DEGRADED,
            next_action=next_action,
            evidence=evidence,
        )

    def mark_blocked(
        self,
        item_id: str,
        outcome_code: str,
        *,
        next_action: str,
        evidence: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.mark_terminal(
            item_id,
            ResolutionOutcome.BLOCKED_WITH_CHECKPOINT,
            outcome_code,
            verification_state=VerificationState.BLOCKED,
            next_action=next_action,
            evidence=evidence,
            error=outcome_code,
        )

    def mark_exported(
        self,
        item_id: str,
        outcome_code: str,
        *,
        next_action: str,
        export_path: Optional[str],
        evidence: Optional[Dict[str, Any]] = None,
    ) -> None:
        self.mark_terminal(
            item_id,
            ResolutionOutcome.EXPORTED_WITH_MANUAL_APPLY,
            outcome_code,
            verification_state=VerificationState.EXPORTED,
            next_action=next_action,
            export_path=export_path,
            evidence=evidence,
            error=outcome_code,
        )

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
        """Persist a capability probe result."""
        if not self.state:
            return
        self.state.record_capability(
            scope,
            capability,
            supported=supported,
            detail=detail,
            evidence=evidence,
            probe=probe,
        )
        self.persist_state()

    def record_provenance(self, key: str, value: Any) -> None:
        """Persist resolution provenance."""
        if not self.state:
            return
        self.state.record_resolution_provenance(key, value)
        self.persist_state()
