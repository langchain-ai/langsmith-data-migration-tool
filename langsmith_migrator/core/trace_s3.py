"""``BlobStore`` over an S3 prefix, with a writer that publishes on demand."""

from __future__ import annotations

import io
import re
from concurrent.futures import ThreadPoolExecutor, wait
from operator import itemgetter
from typing import Any, BinaryIO, Dict, List, Optional, Set, Tuple
from urllib.parse import urlparse

from .trace_blobstore import MARKER, ArchiveError

MIN_PART_BYTES = 5 * 1024**2
MAX_PART_BYTES = 5 * 1024**3
MAX_PARTS = 10_000

DEFAULT_PART_BYTES = 8 * 1024**2
DEFAULT_UPLOAD_CONCURRENCY = 4
DEFAULT_DOWNLOAD_CONCURRENCY = 4
DEFAULT_DOWNLOAD_CHUNK_BYTES = 2 * 1024**2

_SEGMENT = re.compile(r"^(?!\.\.?$)[A-Za-z0-9._-]{1,200}$")
_MAX_KEY_BYTES = 1024


def clamp_part_bytes(value: int) -> Tuple[int, Optional[str]]:
    """Clamp to S3's part limits, and say so."""
    if value < MIN_PART_BYTES:
        return MIN_PART_BYTES, f"part size raised to S3's {MIN_PART_BYTES // 1024**2} MiB minimum"
    if value > MAX_PART_BYTES:
        return MAX_PART_BYTES, f"part size lowered to S3's {MAX_PART_BYTES // 1024**3} GiB maximum"
    return value, None


def check_part_ceiling(part_bytes: int, max_window_bytes: int) -> None:
    """Refuse a part size that cannot span the largest window we would accept."""
    needed = -(-max_window_bytes // part_bytes)  # ceil
    if needed > MAX_PARTS:
        raise ArchiveError(
            f"a {part_bytes:,}-byte part needs {needed:,} parts to cover a "
            f"{max_window_bytes:,}-byte window, over S3's {MAX_PARTS:,} limit; "
            f"raise --s3-part-bytes to at least {-(-max_window_bytes // MAX_PARTS):,}"
        )


_CREDENTIAL_CODES = frozenset(
    {
        "ExpiredToken",
        "ExpiredTokenException",
        "InvalidToken",
        "TokenRefreshRequired",
        "InvalidAccessKeyId",
        "SignatureDoesNotMatch",
        "RequestTimeTooSkewed",
    }
)


def _explain(exc: Exception) -> str:
    """Say what a bucket error most likely means."""
    code = str((((getattr(exc, "response", None) or {}).get("Error")) or {}).get("Code") or "")
    if code in _CREDENTIAL_CODES:
        return f"{code} - refresh your AWS credentials and re-run"
    if code in ("400", "403", "AccessDenied"):
        return (
            f"{exc}; HeadBucket returns no detail, so the usual causes are expired or "
            "invalid credentials (refresh and re-run), a bucket in another region, or "
            "a key without s3:ListBucket"
        )
    return str(exc)


def _transfer_config(chunk_bytes: int, concurrency: int) -> Any:
    try:
        from boto3.s3.transfer import TransferConfig  # noqa: PLC0415
    except ImportError:
        return None
    return TransferConfig(
        multipart_threshold=chunk_bytes,
        multipart_chunksize=chunk_bytes,
        max_concurrency=concurrency,
        use_threads=True,
    )


class MultipartWriter:
    """Three phases: parts, then the tail, then a publish that moves no bytes."""

    def __init__(
        self,
        client: Any,
        bucket: str,
        key: str,
        *,
        part_bytes: int,
        concurrency: int,
        extra_args: Optional[Dict[str, Any]] = None,
    ):
        self._client = client
        self.bucket, self.key = bucket, key
        self._part_bytes = part_bytes
        self._pool = (
            ThreadPoolExecutor(concurrency, thread_name_prefix="s3-part")
            if concurrency > 1
            else None
        )
        self._buf = bytearray()
        self._parts: List[Dict[str, Any]] = []
        self._pending: List[Tuple[int, Any]] = []
        self._count = 0
        self.upload_id = client.create_multipart_upload(
            Bucket=bucket, Key=key, **(extra_args or {})
        )["UploadId"]

    def write(self, data: bytes) -> int:
        self._buf += data
        while len(self._buf) >= self._part_bytes:
            self._send(bytes(self._buf[: self._part_bytes]))
            del self._buf[: self._part_bytes]
        return len(data)

    def flush_tail(self) -> None:
        """The last of the bytes, still on the worker thread."""
        if self._buf:
            self._send(bytes(self._buf))
            self._buf.clear()
        self._drain()

    def finish(self) -> None:
        """Main thread, in window order. One request, no payload."""
        assert not self._pending, "flush_tail must drain before the ordered commit"
        try:
            self._complete()
        finally:
            self._shutdown()

    def _complete(self) -> None:
        if not self._parts:
            self._client.abort_multipart_upload(
                Bucket=self.bucket, Key=self.key, UploadId=self.upload_id
            )
            self.upload_id = ""
            self._client.put_object(Bucket=self.bucket, Key=self.key, Body=b"")
            return
        self._client.complete_multipart_upload(
            Bucket=self.bucket,
            Key=self.key,
            UploadId=self.upload_id,
            MultipartUpload={"Parts": sorted(self._parts, key=itemgetter("PartNumber"))},
        )
        self.upload_id = ""

    def abort(self) -> None:
        """Uploaded parts are invisible to a listing and billed until aborted."""
        wait([future for _, future in self._pending if not future.cancel()])
        self._pending.clear()
        if not self.upload_id:
            self._shutdown()
            return
        try:
            self._client.abort_multipart_upload(
                Bucket=self.bucket, Key=self.key, UploadId=self.upload_id
            )
        finally:
            self.upload_id = ""
            self._shutdown()

    def _shutdown(self) -> None:
        if self._pool is not None:
            self._pool.shutdown(wait=False)
            self._pool = None

    def _send(self, body: bytes) -> None:
        self._count += 1
        number = self._count
        if number > MAX_PARTS:
            raise ArchiveError(f"{self.key}: over S3's {MAX_PARTS:,}-part limit")

        def call() -> Any:
            return self._client.upload_part(
                Bucket=self.bucket,
                Key=self.key,
                PartNumber=number,
                UploadId=self.upload_id,
                Body=body,
            )

        if self._pool is None:
            self._parts.append({"ETag": call()["ETag"], "PartNumber": number})
            return
        self._pending.append((number, self._pool.submit(call)))
        for num, future in [p for p in self._pending if p[1].done()]:
            self._collect(num, future)

    def _collect(self, number: int, future: Any) -> None:
        self._parts.append({"ETag": future.result()["ETag"], "PartNumber": number})
        self._pending = [p for p in self._pending if p[0] != number]

    def _drain(self) -> None:
        for number, future in list(self._pending):
            self._collect(number, future)


class S3Store:
    """``BlobStore`` over ``s3://bucket/prefix``."""

    def __init__(
        self,
        url: str,
        *,
        part_bytes: int = DEFAULT_PART_BYTES,
        upload_concurrency: int = DEFAULT_UPLOAD_CONCURRENCY,
        download_concurrency: int = DEFAULT_DOWNLOAD_CONCURRENCY,
        download_chunk_bytes: int = DEFAULT_DOWNLOAD_CHUNK_BYTES,
        max_pool_connections: int = 64,
        connect_timeout: int = 10,
        read_timeout: int = 120,
        max_attempts: int = 6,
        retry_mode: str = "adaptive",
        endpoint_url: Optional[str] = None,
        addressing_style: str = "auto",
        sse: Optional[str] = None,
        sse_kms_key_id: Optional[str] = None,
        storage_class: Optional[str] = None,
        client: Any = None,
    ):
        parsed = urlparse(str(url))
        if parsed.scheme != "s3" or not parsed.netloc:
            raise ArchiveError(f"not an s3:// archive location: {url}")
        self.bucket = parsed.netloc
        self.prefix = parsed.path.strip("/")
        self.part_bytes, _ = clamp_part_bytes(part_bytes)
        self.upload_concurrency = max(1, min(32, upload_concurrency))
        self.download_concurrency = max(1, min(32, download_concurrency))
        self.download_chunk_bytes = max(1, download_chunk_bytes)

        self.extra_args: Dict[str, Any] = {}
        if sse:
            self.extra_args["ServerSideEncryption"] = sse
            if sse_kms_key_id:
                self.extra_args["SSEKMSKeyId"] = sse_kms_key_id
        if storage_class:
            self.extra_args["StorageClass"] = storage_class

        if client is not None:
            self.client = client
        else:
            try:
                import boto3  # noqa: PLC0415 - optional dependency
                from botocore.config import Config  # noqa: PLC0415
            except ImportError as exc:  # pragma: no cover - exercised by hand
                raise ArchiveError(
                    "s3:// archives need the s3 extra: "
                    "uv tool install 'langsmith-data-migration-tool[s3]'"
                ) from exc
            self.client = boto3.client(
                "s3",
                endpoint_url=endpoint_url,
                config=Config(
                    connect_timeout=connect_timeout,
                    read_timeout=read_timeout,
                    retries={"max_attempts": max_attempts, "mode": retry_mode},
                    max_pool_connections=max_pool_connections,
                    s3={"addressing_style": addressing_style},
                ),
            )
        self._transfer_config = _transfer_config(
            self.download_chunk_bytes, self.download_concurrency
        )
        self.preflight()

    def preflight(self) -> None:
        """One read that proves bucket, region and credentials before any work."""
        try:
            self.client.head_bucket(Bucket=self.bucket)
        except Exception as exc:
            raise ArchiveError(f"cannot reach s3://{self.bucket}: {_explain(exc)}") from exc

    def container(self, *parts: str) -> str:
        for part in parts:
            if not _SEGMENT.match(part):
                raise ArchiveError(f"refusing an unsafe key segment: {part!r}")
        return "/".join(filter(None, (self.prefix, *parts)))

    def ensure_container(self, container: str, marker: str, body: bytes) -> None:
        self.client.put_object(
            Bucket=self.bucket, Key=self._key(container, marker), Body=body, **self.extra_args
        )

    def names(self, container: str) -> Set[str]:
        """One paginated listing, not one existence check per window."""
        found = set()
        for key in self._list(container.rstrip("/") + "/"):
            tail = key[len(container) + 1 :]
            if tail and "/" not in tail:
                found.add(tail)
        return found

    def begin(self, container: str, name: str):
        return MultipartWriter(
            self.client,
            self.bucket,
            self._key(container, name),
            part_bytes=self.part_bytes,
            concurrency=self.upload_concurrency,
            extra_args=self.extra_args,
        )

    def read(self, container: str, name: str) -> BinaryIO:
        """Parallel ranged GETs into memory, each range retried on its own."""
        buf = io.BytesIO()
        self.client.download_fileobj(
            self.bucket, self._key(container, name), buf, Config=self._transfer_config
        )
        buf.seek(0)
        return buf

    def find_containers(self) -> List[Tuple[str, bytes]]:
        found = []
        for key in self._list(f"{self.prefix}/" if self.prefix else ""):
            if not key.endswith("/" + MARKER):
                continue
            container = key[: -len(MARKER) - 1]
            body = self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
            found.append((container, body))
        return sorted(found)

    def describe(self, container: str, name: str = "") -> str:
        return f"s3://{self.bucket}/{'/'.join(filter(None, (container, name)))}"

    def _key(self, container: str, name: str) -> str:
        if not _SEGMENT.match(name):
            raise ArchiveError(f"refusing an unsafe blob name: {name!r}")
        key = f"{container}/{name}" if container else name
        if self.prefix and not key.startswith(self.prefix + "/"):
            raise ArchiveError(f"refusing a key outside the archive prefix: {key}")
        segments = key.split("/")
        if any(s in ("", ".", "..") for s in segments) or len(key.encode()) > _MAX_KEY_BYTES:
            raise ArchiveError(f"refusing an unsafe key: {key!r}")
        return key

    def _exists(self, key: str) -> bool:
        try:
            self.client.head_object(Bucket=self.bucket, Key=key)
            return True
        except Exception:
            return False

    def _list(self, prefix: str) -> List[str]:
        pages = self.client.get_paginator("list_objects_v2").paginate(
            Bucket=self.bucket, Prefix=prefix
        )
        return [item["Key"] for page in pages for item in page.get("Contents", ())]
