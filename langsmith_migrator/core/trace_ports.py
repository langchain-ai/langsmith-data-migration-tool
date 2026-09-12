"""The seam between the trace migrator and each side it talks to."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional, Protocol, Set, Tuple

from .trace_domain import SlicePlan, Window
from .trace_frames import CompiledFrame

Session = Dict[str, Any]


@dataclass
class SlicePrepared:
    """One slice's work, built in a worker and committed on the main thread."""

    window: Window
    src_id: str
    dst_id: str
    plan: SlicePlan
    population: List[Dict[str, Any]]
    target: Optional[Dict[str, Any]] = None
    staged: Any = None
    batches: List[Tuple[List[Dict[str, Any]], Optional[CompiledFrame]]] = field(
        default_factory=list
    )
    digests: Dict[str, Dict[str, str]] = field(default_factory=dict)
    degraded: Dict[str, Set[str]] = field(default_factory=dict)
    fidelity_notes: Set[str] = field(default_factory=set)
    upgrade_rows: List[Tuple[str, str, str]] = field(default_factory=list)
    issues: List[Tuple[str, str, str, Dict[str, Any]]] = field(default_factory=list)


class RunSource(Protocol):
    """Where runs are read from: a deployment, or an archive directory."""

    def sessions(self) -> List[Session]:
        """Every project this source can offer."""

    def windows(self, session: Session) -> Iterator[Window]:
        """The project's windows, oldest first."""

    def prepare(
        self,
        session: Session,
        target: Session,
        window: Window,
        existing_ids: Callable[[], Set[str]],
    ) -> "SlicePrepared":
        """One window's work, ready to commit. Worker thread; no shared state."""


class RunSink(Protocol):
    """Where runs are written to: a deployment, or an archive directory."""

    def open_target(self, session: Session) -> Optional[Session]:
        """Resolve (or create) the target for one source project."""

    def batch_limits(self) -> Tuple[int, int]:
        """Runs-per-batch and bytes-per-batch this sink wants."""

    def field_ceiling(self) -> int:
        """Largest single field this sink will take, in bytes."""

    def oversized_fields(self, payload: Dict[str, Any]) -> List[str]:
        """Fields this sink would not store whole. Empty when it has no cap."""

    def has_window(self, target: Session, window: Window) -> bool:
        """Whether this window is already fully written."""

    def existing_ids(self, target: Session, window: Window) -> Set[str]:
        """Run IDs the target already holds for this window."""

    def stage(self, target: Session, window: Window, prepared) -> Any:
        """Worker thread: everything the write needs, short of publishing it."""

    def commit(self, target: Session, window: Window, prepared, staged) -> List[Tuple[str, str]]:
        """Main thread, in window order. Returns ``[(run_id, error)]``."""

    def close_target(self, target: Session, recon: Any = None) -> None:
        """Release the target after its last window, told how it went."""
