"""The S3 store's contracts, none of which a live bucket would tell you cheaply.

The load-bearing one is the phase split: every byte, tail included, must leave
in the worker so the ordered commit is a single payload-free request. That is
not a performance nicety - it is what keeps "an interrupted export leaves a
contiguous prefix" affordable at thousands of windows, and it is invisible
unless asserted, because getting it wrong only makes things slower.
"""

import io
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import pytest

from langsmith_migrator.core.trace_blobstore import ArchiveError
from langsmith_migrator.core.trace_s3 import (
    MAX_PARTS,
    MIN_PART_BYTES,
    MultipartWriter,
    S3Store,
    check_part_ceiling,
)

BUCKET = "archive-bucket"


class FakeS3:
    """Records every call and models the two behaviours that matter:
    an object is invisible until the upload completes, and a zero-part
    complete is refused the way S3 refuses it."""

    def __init__(self):
        self.calls = Counter()
        self.objects = {}
        self.uploads = {}  # live multipart uploads only: the billing hazard
        self.put_extra = {}
        self._next = 0

    def _log(self, name):
        self.calls[name] += 1

    def head_bucket(self, Bucket):
        self._log("HeadBucket")
        return {}

    def create_multipart_upload(self, Bucket, Key, **extra):
        self._log("CreateMultipartUpload")
        self._next += 1
        uid = f"upload-{self._next}"
        self.uploads[uid] = {"key": Key, "parts": {}, "extra": extra}
        return {"UploadId": uid}

    def upload_part(self, Bucket, Key, PartNumber, UploadId, Body):
        self._log("UploadPart")
        if UploadId not in self.uploads:
            raise RuntimeError("NoSuchUpload")
        self.uploads[UploadId]["parts"][PartNumber] = bytes(Body)
        return {"ETag": f'"etag-{PartNumber}"'}

    def complete_multipart_upload(self, Bucket, Key, UploadId, MultipartUpload):
        self._log("CompleteMultipartUpload")
        upload = self.uploads.pop(UploadId)
        numbers = [p["PartNumber"] for p in MultipartUpload["Parts"]]
        assert numbers == sorted(numbers), "parts must be sent in order"
        assert numbers, "S3 refuses a complete with no parts"
        self.objects[Key] = b"".join(upload["parts"][n] for n in numbers)
        return {}

    def abort_multipart_upload(self, Bucket, Key, UploadId):
        self._log("AbortMultipartUpload")
        self.uploads.pop(UploadId, None)
        return {}

    def put_object(self, Bucket, Key, Body, **extra):
        self._log("PutObject")
        self.objects[Key] = bytes(Body)
        self.put_extra[Key] = extra
        return {}

    def head_object(self, Bucket, Key):
        self._log("HeadObject")
        if Key not in self.objects:
            raise RuntimeError("404")
        return {"ContentLength": len(self.objects[Key])}

    def get_object(self, Bucket, Key):
        self._log("GetObject")
        return {"Body": io.BytesIO(self.objects[Key])}

    def list_objects_v2(self, Bucket, Prefix="", **kw):
        self._log("ListObjectsV2")
        hits = sorted(k for k in self.objects if k.startswith(Prefix))
        return {"Contents": [{"Key": k} for k in hits], "IsTruncated": False}

    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        outer = self

        class _Pager:
            def paginate(self, **kwargs):
                return [outer.list_objects_v2(**kwargs)]

        return _Pager()

    def download_fileobj(self, Bucket, Key, fileobj, Config=None):
        self._log("DownloadFileobj")
        fileobj.write(self.objects[Key])


def store(**kw):
    client = FakeS3()
    return S3Store(f"s3://{BUCKET}/arch", client=client, **kw), client


def writer(client, *, part_bytes=MIN_PART_BYTES, concurrency=1):
    return MultipartWriter(
        client,
        BUCKET,
        "arch/ws/proj/w.tar.zst",
        part_bytes=part_bytes,
        concurrency=concurrency,
    )


# --------------------------------------------------------------------------
# The phase split
# --------------------------------------------------------------------------
def test_the_object_is_invisible_until_finish_publishes_it():
    """The whole ordered-commit guarantee rests on this."""
    _, client = store()
    w = writer(client)
    w.write(b"x" * (MIN_PART_BYTES + 1000))
    w.flush_tail()
    assert client.objects == {}, "a window must not appear before it is whole"
    w.finish()
    assert len(client.objects["arch/ws/proj/w.tar.zst"]) == MIN_PART_BYTES + 1000


def test_every_byte_including_the_tail_leaves_before_the_ordered_commit():
    """The regression guard for the three-phase split.

    If the tail ever moves back into ``finish``, this fails - and nothing else
    would notice, because the only symptom is a slower serial path.
    """
    _, client = store()
    w = writer(client)
    w.write(b"x" * (MIN_PART_BYTES * 2 + 7))  # two full parts and a remainder
    w.flush_tail()
    assert client.calls["UploadPart"] == 3, "the tail must be uploaded by flush_tail"
    before = client.calls["UploadPart"]
    w.finish()
    assert client.calls["UploadPart"] == before, "finish must not transfer"
    assert client.calls["CompleteMultipartUpload"] == 1


def test_finish_refuses_to_run_with_parts_still_in_flight():
    _, client = store()
    w = writer(client, concurrency=2)
    w.write(b"x" * (MIN_PART_BYTES * 2))
    w._pending.append((99, ThreadPoolExecutor(1).submit(lambda: {"ETag": "x"})))
    with pytest.raises(AssertionError, match="drain"):
        w.finish()
    w._pending.clear()
    w.abort()


# --------------------------------------------------------------------------
# Concurrency
# --------------------------------------------------------------------------
def test_part_numbers_survive_out_of_order_completion():
    _, client = store()
    w = writer(client, concurrency=4)
    w.write(b"a" * MIN_PART_BYTES + b"b" * MIN_PART_BYTES + b"c" * MIN_PART_BYTES)
    w.flush_tail()
    w.finish()
    body = client.objects["arch/ws/proj/w.tar.zst"]
    assert body == b"a" * MIN_PART_BYTES + b"b" * MIN_PART_BYTES + b"c" * MIN_PART_BYTES


# --------------------------------------------------------------------------
# Abort
# --------------------------------------------------------------------------
def test_abort_leaves_no_object_and_no_live_upload():
    """Orphaned parts are invisible to a listing and billed until aborted."""
    _, client = store()
    w = writer(client)
    w.write(b"x" * MIN_PART_BYTES)
    w.flush_tail()
    w.abort()
    assert client.objects == {} and client.uploads == {}
    assert client.calls["AbortMultipartUpload"] == 1


def test_an_empty_object_is_written_the_only_way_s3_allows():
    """A zero-part complete is refused by S3; the fake refuses it too."""
    _, client = store()
    w = writer(client)
    w.finish()
    assert client.objects["arch/ws/proj/w.tar.zst"] == b""
    assert client.calls["CompleteMultipartUpload"] == 0


# --------------------------------------------------------------------------
# Keys built from untrusted names
# --------------------------------------------------------------------------
@pytest.mark.parametrize("part", ["../../escape", "..", ".", "", "a/b", "x\x00y", "a" * 300])
def test_a_hostile_segment_cannot_reach_a_key(part):
    s, _ = store()
    with pytest.raises(ArchiveError, match="unsafe key segment"):
        s.container(part)


def test_a_key_outside_the_configured_prefix_is_refused():
    s, _ = store()
    with pytest.raises(ArchiveError, match="outside the archive prefix"):
        s._key("elsewhere/ws/proj", "w.tar.zst")


# --------------------------------------------------------------------------
# Listing, resumption, enumeration
# --------------------------------------------------------------------------
def test_resumption_costs_one_listing_per_project_not_one_head_per_window():
    """A request per window is thousands of round-trips before a byte moves."""
    s, client = store()
    container = s.container("ws", "proj")
    for i in range(5):
        client.objects[f"{container}/w{i}.tar.zst"] = b"x"
    client.calls.clear()
    names = s.names(container)
    assert names == {f"w{i}.tar.zst" for i in range(5)}
    assert client.calls["ListObjectsV2"] == 1
    assert client.calls["HeadObject"] == 0


def test_containers_are_found_by_their_marker():
    s, client = store()
    a, b = s.container("ws", "p1"), s.container("ws", "p2")
    client.objects[f"{a}/PROJECT.json"] = b'{"id": "1"}'
    client.objects[f"{b}/PROJECT.json"] = b'{"id": "2"}'
    client.objects[f"{a}/w.tar.zst"] = b"x"
    assert s.find_containers() == [(a, b'{"id": "1"}'), (b, b'{"id": "2"}')]


def test_the_marker_is_rewritten_so_it_cannot_go_stale():
    """Write-once left an existing archive without fields added later - which
    is what stopped an older archive being selected by workspace."""
    s, client = store()
    container = s.container("ws", "proj")
    s.ensure_container(container, "PROJECT.json", b'{"id": "1"}')
    s.ensure_container(container, "PROJECT.json", b'{"id": "1", "workspace_id": "ws"}')
    assert client.calls["PutObject"] == 2
    assert client.calls["HeadObject"] == 0, "one PUT, not a HEAD and a PUT"
    assert b"workspace_id" in client.objects[f"{container}/PROJECT.json"]


def test_a_blob_round_trips_through_the_store():
    s, client = store()
    container = s.container("ws", "proj")
    w = s.begin(container, "w.tar.zst")
    w.write(b"payload-bytes")
    w.flush_tail()
    w.finish()
    with s.read(container, "w.tar.zst") as handle:
        assert handle.read() == b"payload-bytes"


# --------------------------------------------------------------------------
# Settings
# --------------------------------------------------------------------------


def test_a_part_size_that_cannot_span_the_largest_window_is_refused_up_front():
    with pytest.raises(ArchiveError, match=f"over S3's {MAX_PARTS:,} limit"):
        check_part_ceiling(MIN_PART_BYTES, 8 * 1024**4)
    check_part_ceiling(8 * 1024**2, 8 * 1024**3)  # the real defaults are fine


def test_encryption_and_storage_class_reach_the_write_calls():
    s, client = store(sse="aws:kms", sse_kms_key_id="key-1", storage_class="STANDARD_IA")
    container = s.container("ws", "proj")
    w = s.begin(container, "w.tar.zst")
    w.write(b"x")
    w.flush_tail()
    w.finish()
    extra = next(iter(client.uploads.values()))["extra"] if client.uploads else None
    assert extra is None  # the upload completed, so only the recorded args remain
    assert s.extra_args == {
        "ServerSideEncryption": "aws:kms",
        "SSEKMSKeyId": "key-1",
        "StorageClass": "STANDARD_IA",
    }


def test_preflight_reads_the_bucket_and_writes_nothing():
    _, client = store()
    assert client.calls["HeadBucket"] == 1
    assert not any(client.calls[op] for op in ("PutObject", "CreateMultipartUpload", "UploadPart"))


def test_an_unreachable_bucket_fails_with_the_bucket_named():
    class Broken(FakeS3):
        def head_bucket(self, Bucket):
            raise RuntimeError("AccessDenied")

    with pytest.raises(ArchiveError, match="cannot reach s3://archive-bucket"):
        S3Store(f"s3://{BUCKET}/arch", client=Broken())


# --------------------------------------------------------------------------
# End to end through the archive layer, still without a network
# --------------------------------------------------------------------------
from datetime import datetime, timedelta, timezone  # noqa: E402

from langsmith_migrator.core.trace_archive import ArchiveSink, ArchiveSource  # noqa: E402
from langsmith_migrator.core.trace_domain import Window, plan_slice  # noqa: E402
from langsmith_migrator.core.trace_ports import SlicePrepared  # noqa: E402

NOW = datetime(2026, 9, 1, tzinfo=timezone.utc)
WINDOW = Window(NOW - timedelta(days=1), NOW)
PROJECT = {"id": "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", "name": "gtm-agent"}


def _run(n, **extra):
    run_id = f"{n:08d}-1111-1111-1111-111111111111"
    return {
        "id": run_id,
        "trace_id": run_id,
        "name": f"run-{n}",
        "start_time": (NOW - timedelta(hours=2)).isoformat(),
        "dotted_order": f"20260831T220000000000Z{run_id}",
        "inputs": {"q": "x" * 200},
        "outputs": {"a": "y" * 200},
        **extra,
    }


def _prepared(runs):
    ids = {str(r["id"]) for r in runs}
    prepared = SlicePrepared(WINDOW, "src", PROJECT["id"], plan_slice(ids, set()), list(runs))
    prepared.batches = [(list(runs), None)] if runs else []
    return prepared


def _sink(client, **kw):
    sink = ArchiveSink(
        f"s3://{BUCKET}/arch",
        compress_level=3,
        store=S3Store(f"s3://{BUCKET}/arch", client=client, **kw),
        workspace={"id": "ws-1", "name": "LangChain Team"},
    )
    sink.intent = {"range_start": "s", "range_end": "e", "window_hours": 24.0, "projects": ["p"]}
    return sink


def test_a_window_round_trips_through_s3_byte_for_byte():
    client = FakeS3()
    runs = [_run(1, attachments={"a.bin": ("application/octet-stream", b"\x00\xff" * 64)})]
    sink = _sink(client)
    target = sink.open_target(PROJECT)
    prepared = _prepared(runs)
    sink.commit(target, WINDOW, prepared, sink.stage(target, WINDOW, prepared))

    source = ArchiveSource(
        f"s3://{BUCKET}/arch", store=S3Store(f"s3://{BUCKET}/arch", client=client)
    )
    sessions = source.sessions()
    assert [s["name"] for s in sessions] == ["gtm-agent"]
    decoded_windows = list(source.windows(sessions[0]))
    assert decoded_windows == [WINDOW]
    out = source.prepare(sessions[0], {"id": "dst"}, WINDOW, lambda: set())
    payload = out.batches[0][0][0]
    assert payload["attachments"]["a.bin"][1] == b"\x00\xff" * 64
    assert payload["session_id"] == "dst"


def test_an_interrupted_window_leaves_nothing_visible_and_no_live_upload():
    client = FakeS3()
    sink = _sink(client)
    target = sink.open_target(PROJECT)
    prepared = _prepared([_run(1)])
    sink.stage(target, WINDOW, prepared)  # staged, never committed
    assert sink.has_window(target, WINDOW) is False
    assert not any(k.endswith(".tar.zst") for k in client.objects)
    sink.discard_staged()
    assert client.uploads == {}, "an abandoned upload must be aborted, not left billing"


def test_a_dry_run_against_s3_issues_no_mutating_call():
    """The contract of the mode: reads to prove reachability, nothing else."""
    client = FakeS3()
    sink = _sink(client)
    sink.dry_run = True
    target = sink.open_target(PROJECT)
    prepared = _prepared([_run(1)])
    sink.commit(target, WINDOW, prepared, sink.stage(target, WINDOW, prepared))

    assert client.calls["HeadBucket"] == 1
    assert client.calls["ListObjectsV2"] >= 1
    for op in ("PutObject", "CreateMultipartUpload", "UploadPart", "CompleteMultipartUpload"):
        assert client.calls[op] == 0, f"{op} escaped a dry run"
    assert client.objects == {}
    assert sink.projected_bytes > 0, "the encode still has to run"


def test_the_writer_shuts_its_own_pool_down_on_both_exits():
    """The store used to keep every pool in a list and a close() nobody called."""
    _, client = store()
    done = writer(client, concurrency=4)
    done.write(b"x" * MIN_PART_BYTES)
    done.flush_tail()
    done.finish()
    assert done._pool is None

    dropped = writer(client, concurrency=4)
    dropped.write(b"x" * MIN_PART_BYTES)
    dropped.flush_tail()
    dropped.abort()
    assert dropped._pool is None


# --------------------------------------------------------------------------
# Failure messages
# --------------------------------------------------------------------------


def test_abort_waits_for_a_part_already_in_flight():
    """cancel() does not stop a running part, so it would land after the abort."""
    import threading

    class Blocking(FakeS3):
        def __init__(self):
            super().__init__()
            self.entered = threading.Event()
            self.release = threading.Event()
            self.after_abort = []

        def upload_part(self, Bucket, Key, PartNumber, UploadId, Body):
            self.entered.set()
            self.release.wait(5)
            if "AbortMultipartUpload" in self.calls:
                self.after_abort.append(PartNumber)
            return super().upload_part(Bucket, Key, PartNumber, UploadId, Body)

    client = Blocking()
    w = writer(client, concurrency=4)
    w.write(b"x" * MIN_PART_BYTES)
    client.entered.wait(5)

    done = threading.Event()
    threading.Thread(target=lambda: (w.abort(), done.set())).start()
    assert not done.wait(0.3), "abort must not return while a part is in flight"
    client.release.set()
    assert done.wait(5)
    assert client.after_abort == [], "a part landed after the upload was aborted"


def test_a_failed_marker_rewrite_leaves_the_old_one_readable(tmp_path):
    """It is rewritten every run, and an empty one hides the whole project."""
    import json
    from unittest.mock import patch

    import langsmith_migrator.core.trace_blobstore as blobstore

    local = blobstore.LocalStore(tmp_path)
    container = local.container("ws-1", "proj-1")
    good = json.dumps({"id": "p", "workspace_id": "w"}).encode()
    local.ensure_container(container, "PROJECT.json", good)

    real_open = blobstore._open_private

    def half_then_fail(path):
        handle = real_open(path)
        write = handle.write

        def boom(data):
            write(data[: len(data) // 2])
            raise OSError("disk full")

        handle.write = boom
        return handle

    with patch.object(blobstore, "_open_private", half_then_fail):
        with pytest.raises(OSError):
            local.ensure_container(container, "PROJECT.json", b'{"id": "other"}')

    marker = tmp_path / "ws-1" / "proj-1" / "PROJECT.json"
    assert json.loads(marker.read_bytes()) == json.loads(good)
    assert sorted(p.name for p in marker.parent.iterdir()) == ["PROJECT.json"]
