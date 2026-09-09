"""Frame compilation: the multipart body must be identical to the SDK's own.

These tests use the real langsmith Client so a moved SDK internal fails here
rather than in production.
"""

import io
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import patch

import pytest
import zstandard
from langsmith import Client
from langsmith.client import _BOUNDARY

from langsmith_migrator.core import trace_frames
from langsmith_migrator.core.trace_frames import (
    DEFAULT_COMPRESS_LEVEL,
    compile_frame,
    unavailable_reason,
)


@pytest.fixture
def client():
    return Client(
        api_url="https://example.invalid",
        api_key="k",
        omit_traced_runtime_info=True,
        auto_batch_tracing=False,
    )


def _payload(i=0, blob=200, attachments=None):
    rid = str(uuid.uuid4())
    run = {
        "id": rid,
        "trace_id": rid,
        "dotted_order": f"20260826T120000000000Z{rid}",
        "session_id": "11111111-1111-1111-1111-111111111111",
        "name": f"r{i}",
        "run_type": "chain",
        "start_time": "2026-08-26T12:00:00+00:00",
        "inputs": {"q": "repeatable text " * blob, "i": i},
        "outputs": {"a": "answer text " * blob},
    }
    if attachments:
        run["attachments"] = attachments
    return run


def _body(frame):
    return zstandard.ZstdDecompressor().decompress(frame.stream.getvalue())


def test_the_compiled_body_is_well_formed_multipart(client):
    runs = [_payload(i) for i in range(4)]
    frame = compile_frame(client, runs, DEFAULT_COMPRESS_LEVEL)
    body = _body(frame)
    assert body.startswith(f"--{_BOUNDARY}".encode())
    # one part per run at minimum; inputs/outputs may be split out
    assert body.count(f"--{_BOUNDARY}".encode()) - 1 >= len(runs)
    assert frame.run_ids == tuple(str(r["id"]) for r in runs)
    for rid in frame.run_ids:
        assert rid.encode() in body


def test_attachments_survive_into_the_body(client):
    blob = bytes(range(256))
    frame = compile_frame(
        client, [_payload(attachments={"shot": ("application/octet-stream", blob)})],
        DEFAULT_COMPRESS_LEVEL,
    )
    body = _body(frame)
    assert b"shot" in body and blob in body


def test_compiling_off_the_main_thread_gives_the_same_bytes(client):
    runs = [_payload(i) for i in range(3)]
    import copy

    main = compile_frame(client, copy.deepcopy(runs), DEFAULT_COMPRESS_LEVEL)
    with ThreadPoolExecutor(2) as pool:
        assert pool.submit(threading.get_ident).result() != threading.get_ident()
        worker = pool.submit(compile_frame, client, copy.deepcopy(runs), DEFAULT_COMPRESS_LEVEL).result()
    assert _body(worker) == _body(main)


def test_a_higher_level_compresses_harder(client):
    import copy

    runs = [_payload(i) for i in range(6)]
    low = compile_frame(client, copy.deepcopy(runs), 1)
    high = compile_frame(client, copy.deepcopy(runs), 19)
    assert high.sizes[1] < low.sizes[1]
    assert low.sizes[0] == high.sizes[0]  # same body, different framing


def test_the_callers_payloads_survive_compilation(client):
    """serialize_run_dict pops inputs/outputs/extra/... out of the dict it is
    given. If that reached the caller's copy, a retry would re-send skeletons
    and any size measured afterwards would be of the skeleton."""
    import copy

    pay = _payload(attachments={"blob": ("application/octet-stream", b"\x00" * 512)})
    pay["extra"] = {"m": 1}
    pay["events"] = [{"e": 1}]
    pay["serialized"] = {"s": 1}
    before = copy.deepcopy(pay)
    compile_frame(client, [pay], DEFAULT_COMPRESS_LEVEL)
    assert pay == before, f"stripped: {[k for k in before if k not in pay]}"


def test_recompiling_the_same_payload_gives_the_same_frame(client):
    """The binary split recompiles its halves; it must not get a lesser body."""
    pay = _payload(blob=400)
    first = compile_frame(client, [pay], DEFAULT_COMPRESS_LEVEL)
    second = compile_frame(client, [pay], DEFAULT_COMPRESS_LEVEL)
    assert first.sizes == second.sizes
    assert _body(first) == _body(second)


def test_a_run_without_dotted_order_is_refused(client):
    bad = _payload()
    del bad["dotted_order"]
    with pytest.raises(ValueError, match="dotted_order"):
        compile_frame(client, [bad], DEFAULT_COMPRESS_LEVEL)


def test_sampling_is_refused_because_it_keeps_client_state(client):
    client.tracing_sample_rate = 0.5
    with pytest.raises(RuntimeError, match="tracing_sample_rate"):
        compile_frame(client, [_payload()], DEFAULT_COMPRESS_LEVEL)


def test_unavailable_reason_is_none_when_the_sdk_cooperates(client):
    assert unavailable_reason(client) is None


def test_a_moved_sdk_internal_is_reported_not_raised(client):
    with patch.object(trace_frames, "_UNAVAILABLE", "no module named x"):
        reason = unavailable_reason(client)
    assert reason and "SDK internals moved" in reason


def test_a_client_without_the_compressed_sender_is_reported():
    class Old:
        pass

    assert "no _send_compressed_multipart_req" in unavailable_reason(Old())


def test_the_stream_carries_the_log_context(client):
    frame = compile_frame(client, [_payload()], DEFAULT_COMPRESS_LEVEL)
    assert isinstance(frame.stream, io.BytesIO)
    assert hasattr(frame.stream, "context")
