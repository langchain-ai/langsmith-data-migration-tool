"""Compile a multipart ingest body, optionally zstd-compressed."""

from __future__ import annotations

import io
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Sequence, Tuple

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


def compile_frame(client: Any, payloads: Sequence[Dict[str, Any]], level: int) -> CompiledFrame:
    """Build one compressed ingest body. Safe to call from a worker thread."""
    run_ids = tuple(str(p["id"]) for p in payloads)
    if not all(p.get("trace_id") and p.get("dotted_order") for p in payloads):
        raise ValueError("multipart ingest requires trace_id and dotted_order on every run")
    if client.tracing_sample_rate is not None:
        raise RuntimeError("frame compilation requires tracing_sample_rate to be unset")

    transformed = [client._run_transform(dict(p)) for p in payloads]
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
    stream.context = getattr(acc, "context", [])
    return CompiledFrame(run_ids=run_ids, stream=stream, sizes=(len(raw), len(comp)))
