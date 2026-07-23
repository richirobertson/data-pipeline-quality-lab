"""Immutable local and S3-compatible object storage adapters."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from botocore.exceptions import ClientError

from pipeline_quality.exceptions import ObjectConflictError
from pipeline_quality.manifest import sha256_bytes


class ObjectStore(Protocol):
    """Minimum behavior required by ingestion, independent of storage vendor."""

    def put_immutable(self, key: str, content: bytes) -> None: ...

    def get(self, key: str) -> bytes: ...


class FileObjectStore:
    """Filesystem implementation used by deterministic tests and local fixtures."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def put_immutable(self, key: str, content: bytes) -> None:
        """Create an object or prove an existing object contains identical bytes."""
        target = self.root / key
        if target.exists():
            existing = target.read_bytes()
            if sha256_bytes(existing) != sha256_bytes(content):
                raise ObjectConflictError(
                    f"immutable key already contains different content: {key}"
                )
            return
        target.parent.mkdir(parents=True, exist_ok=True)
        # Write-then-rename prevents readers observing a partially written file.
        temporary = target.with_suffix(target.suffix + ".partial")
        temporary.write_bytes(content)
        temporary.replace(target)

    def get(self, key: str) -> bytes:
        """Read an object exactly as it was stored."""
        return (self.root / key).read_bytes()


class S3ObjectStore:
    """S3/MinIO implementation with checksum-based overwrite protection."""

    def __init__(self, client: object, bucket: str) -> None:
        self.client = client
        self.bucket = bucket

    def put_immutable(self, key: str, content: bytes) -> None:
        """Apply the filesystem adapter's immutability rule through the S3 API."""
        try:
            existing = self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        except ClientError as exc:
            # A missing object is expected on first write; authorization and
            # service errors must still propagate rather than look like absence.
            if exc.response["Error"]["Code"] not in {"NoSuchKey", "404"}:
                raise
            existing = None
        if existing is not None:
            if sha256_bytes(existing) != sha256_bytes(content):
                raise ObjectConflictError(
                    f"immutable key already contains different content: {key}"
                )
            return
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=content,
            # Metadata makes integrity inspectable without downloading the body.
            Metadata={"sha256": sha256_bytes(content)},
        )

    def get(self, key: str) -> bytes:
        """Return the S3 response body as bytes to match the storage protocol."""
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
