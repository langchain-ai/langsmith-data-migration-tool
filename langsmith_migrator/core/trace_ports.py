"""The seam between the trace migrator and each side it talks to.

Two protocols, each with two implementations: ``TraceMigrator`` itself is the
LangSmith source and sink, and ``core/trace_archive.py`` supplies the file ones.
The method split is not arbitrary - it mirrors the concurrency the migrator
already has, and the pairing is load-bearing:

* ``prepare`` and ``stage`` run in a prefetch worker, so they must touch no
  shared state. That is where the expensive work belongs: the source fetch, and
  the archive's tar+zstd encode.
* ``commit`` runs on the main thread in strict window order. For the LangSmith
  sink that is the non-idempotent POST; for the file sink it is the rename that
  publishes a finished window. Ordering makes an interrupted run leave a
  contiguous prefix rather than an arbitrary subset with an interior hole.

A sink that answers ``has_window`` truthfully is what makes a re-run skip work
instead of redoing it: for a destination project it is always ``False`` (a
project has no window-level record and can always gain runs), for an archive it
is "the file exists and is complete".
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, Iterator, List, Optional, Protocol, Set, Tuple

from .trace_domain import SlicePlan, Window
from .trace_frames import CompiledFrame

# A tracing project as both sides pass it around: the API's own session dict.
Session = Dict[str, Any]


@dataclass
class SlicePrepared:
    """One slice's work, built in a worker and committed on the main thread.

    Findings accumulate here rather than on the migrator so a worker never
    writes shared state; the commit stage merges them in window order.
    """

    window: Window
    src_id: str
    dst_id: str
    plan: SlicePlan
    population: List[Dict[str, Any]]
    # The sink's target, and whatever ``stage`` produced for it (a ``.partial``
    # path for the archive, nothing for a live destination). Both ride along so
    # the ordered commit needs no side table keyed by window.
    target: Optional[Dict[str, Any]] = None
    staged: Any = None
    # (payloads, pre-compiled frame). The payloads are kept so a rejected batch
    # can still be split to isolate one bad run; that is what bounds peak
    # memory to prefetch x window, and why --window is the knob for it.
    batches: List[Tuple[List[Dict[str, Any]], Optional[CompiledFrame]]] = field(default_factory=list)
    digests: Dict[str, Dict[str, str]] = field(default_factory=dict)
    degraded: Dict[str, Set[str]] = field(default_factory=dict)
    fidelity_notes: Set[str] = field(default_factory=set)
    upgrade_rows: List[Tuple[str, str, str]] = field(default_factory=list)
    # (issue_class, code, summary, evidence), replayed through record_issue.
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
        """One window's work, ready to commit. Worker thread; no shared state.

        ``existing_ids`` is the sink's answer to "which of these do you already
        hold", subtracted before anything is fetched so a re-run of a finished
        window costs the identity scan and nothing else. It is a callable rather
        than a set because a source window that turns out to be empty must not
        cost a destination request at all.
        """


class RunSink(Protocol):
    """Where runs are written to: a deployment, or an archive directory."""

    def open_target(self, session: Session) -> Optional[Session]:
        """Resolve (or create) the target for one source project."""

    def batch_limits(self) -> Tuple[int, int]:
        """Runs-per-batch and bytes-per-batch this sink wants."""

    def has_window(self, target: Session, window: Window) -> bool:
        """Whether this window is already fully written."""

    def existing_ids(self, target: Session, window: Window) -> Set[str]:
        """Run IDs the target already holds for this window."""

    def stage(self, target: Session, window: Window, prepared) -> Any:
        """Worker thread: everything the write needs, short of publishing it."""

    def commit(self, target: Session, window: Window, prepared, staged) -> List[Tuple[str, str]]:
        """Main thread, in window order. Returns ``[(run_id, error)]``."""

    def close_target(self, target: Session, recon: Any = None) -> None:
        """Release the target after its last window, told how it went.

        The LangSmith sink uses ``recon`` to decide whether a tier it raised may
        be put back; an archive has nothing to settle.
        """
