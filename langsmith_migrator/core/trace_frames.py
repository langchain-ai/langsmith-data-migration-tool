"""Compile a multipart ingest body, optionally zstd-compressed.

The SDK's ``multipart_ingest`` builds and sends in one call, so its CPU lands
wherever the send happens. Splitting the two lets the body be built off the
serial ingest path (see ``TraceMigrator.prepare_slice``) and lets us choose a
compression level: the SDK hardcodes zstd level 1, which measures at 5.9x on
real trace payloads against 68.5x at level 3 with long-distance matching.

Everything here is reachable only through SDK private API. The imports are
guarded and reported as one reason string so an SDK bump degrades to the
uncompressed public path instead of crashing.
"""

from __future__ import annotations

import io
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

# Level 3 + long-distance matching, measured per assembled frame on real dev
# payloads: 30.6x -> 68.5x at the same 1755 MB/s. window_log 24 (16 MB) covers
# the ~20 MB batch cap; 32 MB and 128 MB windows add 1% for 8x the memory.
DEFAULT_COMPRESS_LEVEL = 3
_WINDOW_LOG = 24

_UNAVAILABLE: Optional[str] = None
try:  # pragma: no cover - import shape, exercised by test_frames_unavailable
    import zstandard as _zstd
    from langsmith.client import _BOUNDARY
    from langsmith._internal._multipart import join_multipart_parts_and_context
    from langsmith._internal._operations import (
        combine_serialized_queue_operations,
        serialize_run_dict,
        serialized_run_operation_to_multipart_parts_and_context,
    )
    from requests_toolbelt import multipart as _rqtb
except ImportError as exc:
    _UNAVAILABLE = f"{exc}"


def unavailable_reason(client: Any) -> Optional[str]:
    """Why frames cannot be compiled here, or None when they can."""
    if _UNAVAILABLE:
        return f"langsmith SDK internals moved ({_UNAVAILABLE})"
    if not hasattr(client, "_send_compressed_multipart_req"):
        return "langsmith Client has no _send_compressed_multipart_req"
    return None


@dataclass(frozen=True)
class CompiledFrame:
    """A ready-to-POST ingest body and the run IDs it carries."""

    run_ids: Tuple[str, ...]
    stream: io.BytesIO  # the zstd frame
    sizes: Tuple[int, int]  # (raw, compressed), for the ingest log


def _params(level: int):
    return _zstd.ZstdCompressionParameters.from_level(
        level, enable_ldm=True, window_log=_WINDOW_LOG
    )


def compile_frame(
    client: Any, payloads: Sequence[Dict[str, Any]], level: int
) -> CompiledFrame:
    """Build one compressed ingest body. Safe to call from a worker thread.

    Mirrors ``Client.multipart_ingest`` minus the parts that cannot apply here:
    the 404 fallback to ``/runs/batch`` (this migrator requires multipart), the
    filesystem-attachment check (attachments are already in-memory bytes), and
    the create/update merge (this path only creates).

    NB: ``_run_transform`` mutates each payload in place - ``id`` becomes a
    ``UUID`` - so digests must be taken before compiling.
    """
    run_ids = tuple(str(p["id"]) for p in payloads)
    # multipart_ingest validates this before serializing; this path skips it, so
    # the check has to live here rather than in an assert that -O would strip.
    if not all(p.get("trace_id") and p.get("dotted_order") for p in payloads):
        raise ValueError("multipart ingest requires trace_id and dotted_order on every run")
    if client.tracing_sample_rate is not None:
        # Sampling keeps per-trace state on the client, which a worker thread
        # must not touch. Nothing sets it here; fail loudly if that changes.
        raise RuntimeError("frame compilation requires tracing_sample_rate to be unset")

    transformed = [client._run_transform(p) for p in payloads]
    client._insert_runtime_env(transformed)  # no-op under omit_traced_runtime_info
    ops = combine_serialized_queue_operations(
        [serialize_run_dict("post", run) for run in transformed]
    )
    parts: List[Any] = []
    for op in ops:
        part, opened = serialized_run_operation_to_multipart_parts_and_context(op)
        assert not opened, "attachments must be in-memory, not filesystem paths"
        parts.append(part)
    acc = join_multipart_parts_and_context(parts)

    raw = _rqtb.MultipartEncoder(acc.parts, boundary=_BOUNDARY).to_string()
    comp = _zstd.ZstdCompressor(compression_params=_params(level)).compress(raw)
    stream = io.BytesIO(comp)
    # The SDK's sender joins this into the request's log context.
    stream.context = getattr(acc, "context", [])
    return CompiledFrame(run_ids=run_ids, stream=stream, sizes=(len(raw), len(comp)))
