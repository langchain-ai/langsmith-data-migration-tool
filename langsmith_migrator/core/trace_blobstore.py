"""Where an archive's window files live: a directory, or an S3 prefix."""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any, BinaryIO, List, Protocol, Set, Tuple

FREE_SPACE_FLOOR = 1024**3


class ArchiveError(RuntimeError):
    """An archive could not be written, or could not be trusted to be read."""


class BlobWriter(Protocol):
    """One window in flight. Publishes on ``finish``, and not before."""

    def write(self, data: bytes) -> int:
        """Worker thread. May transfer."""

    def flush_tail(self) -> None:
        """Worker thread. Push whatever is left, still without publishing."""

    def finish(self) -> None:
        """Main thread, in window order. Publishes; must not transfer."""

    def abort(self) -> None:
        """Discard everything written. Must leave no trace and no cost."""


class BlobStore(Protocol):
    """A place to put ``(container, name)`` blobs and list them back."""

    def container(self, *parts: str) -> str:
        """Address a container from already-slugged components."""

    def ensure_container(self, container: str, marker: str, body: bytes) -> None:
        """Create it if absent, and (re)write ``marker`` so it never goes stale."""

    def names(self, container: str) -> Set[str]:
        """Every blob name in it. Backs both resumption and enumeration."""

    def begin(self, container: str, name: str) -> BlobWriter:
        """Start a blob. Raises rather than start one that cannot finish."""

    def read(self, container: str, name: str) -> BinaryIO:
        """Open a blob for sequential reading."""

    def find_containers(self) -> List[Tuple[str, bytes]]:
        """``(container, marker bytes)`` for every container holding one."""

    def describe(self, container: str, name: str = "") -> Any:
        """This store's natural handle for a location."""


class CountingWriter:
    """A ``BlobWriter`` that measures instead of storing: the dry run's sink."""

    def __init__(self) -> None:
        self.written = 0

    def write(self, data: bytes) -> int:
        self.written += len(data)
        return len(data)

    def flush_tail(self) -> None:
        return None

    def finish(self) -> None:
        return None

    def abort(self) -> None:
        return None


MARKER = "PROJECT.json"
PARTIAL = ".partial"


class _LocalWriter:
    """``<name>.partial`` until ``finish`` renames it over the real name."""

    def __init__(self, path: Path):
        self.final = path
        self.partial = Path(str(path) + PARTIAL)
        self._handle = _open_private(self.partial)

    def write(self, data: bytes) -> int:
        return self._handle.write(data)

    def flush_tail(self) -> None:
        self._handle.flush()
        os.fsync(self._handle.fileno())

    def finish(self) -> None:
        self._handle.close()
        os.replace(self.partial, self.final)

    def abort(self) -> None:
        try:
            self._handle.close()
        finally:
            self.partial.unlink(missing_ok=True)


def _open_private(path: Path):
    return os.fdopen(os.open(str(path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600), "wb")


class LocalStore:
    """``BlobStore`` over a directory tree. Files 0o600, directories 0o700."""

    def __init__(self, root: str | Path):
        self.root = Path(root)

    def container(self, *parts: str) -> str:
        return str(self._contained(self.root.joinpath(*parts)))

    def ensure_container(self, container: str, marker: str, body: bytes) -> None:
        directory = Path(container)
        directory.mkdir(mode=0o700, parents=True, exist_ok=True)
        staging = directory / f"{marker}.new"
        try:
            with _open_private(staging) as handle:
                handle.write(body)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(staging, directory / marker)
        except BaseException:
            staging.unlink(missing_ok=True)
            raise

    def names(self, container: str) -> Set[str]:
        directory = Path(container)
        if not directory.is_dir():
            return set()
        return {entry.name for entry in directory.iterdir() if entry.is_file()}

    def begin(self, container: str, name: str) -> BlobWriter:
        directory = Path(container)
        free = shutil.disk_usage(directory).free
        if free < FREE_SPACE_FLOOR:
            raise ArchiveError(
                f"only {free:,} bytes free under {directory}; refusing to start a window"
            )
        return _LocalWriter(directory / name)

    def read(self, container: str, name: str) -> BinaryIO:
        return open(Path(container) / name, "rb")

    def find_containers(self) -> List[Tuple[str, bytes]]:
        found = []
        for marker in sorted(self.root.rglob(MARKER)):
            found.append((str(marker.parent), marker.read_bytes()))
        return found

    def describe(self, container: str, name: str = "") -> Path:
        return Path(container) / name if name else Path(container)

    def _contained(self, child: Path) -> Path:
        """Reject any path that resolves outside the archive directory."""
        resolved, base = child.resolve(), self.root.resolve()
        if resolved != base and base not in resolved.parents:
            raise ArchiveError(f"refusing a path outside the archive directory: {child}")
        return resolved


def open_store(target: str | Path, **s3_kwargs) -> BlobStore:
    """``s3://bucket/prefix`` gets the S3 store; anything else is a directory."""
    if is_s3(target):
        from .trace_s3 import S3Store  # noqa: PLC0415 - optional dependency

        return S3Store(str(target), **s3_kwargs)
    return LocalStore(target)


def is_s3(target: str | Path | None) -> bool:
    return str(target or "").startswith("s3://")


__all__ = [
    "ArchiveError",
    "BlobStore",
    "BlobWriter",
    "CountingWriter",
    "FREE_SPACE_FLOOR",
    "LocalStore",
    "MARKER",
    "PARTIAL",
    "is_s3",
    "open_store",
]
