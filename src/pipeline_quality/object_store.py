"""Immutable local and S3-compatible object storage adapters."""

from __future__ import annotations

from pathlib import Path
from typing import Protocol

from botocore.exceptions import ClientError

from pipeline_quality.exceptions import ObjectConflictError
from pipeline_quality.manifest import sha256_bytes


class ObjectStore(Protocol):
    def put_immutable(self, key: str, content: bytes) -> None: ...

    def get(self, key: str) -> bytes: ...


class FileObjectStore:
    """Filesystem implementation used by deterministic tests and local fixtures."""

    def __init__(self, root: Path) -> None:
        self.root = root

    def put_immutable(self, key: str, content: bytes) -> None:
        target = self.root / key
        if target.exists():
            existing = target.read_bytes()
            if sha256_bytes(existing) != sha256_bytes(content):
                raise ObjectConflictError(
                    f"immutable key already contains different content: {key}"
                )
            return
        target.parent.mkdir(parents=True, exist_ok=True)
        temporary = target.with_suffix(target.suffix + ".partial")
        temporary.write_bytes(content)
        temporary.replace(target)

    def get(self, key: str) -> bytes:
        return (self.root / key).read_bytes()


class S3ObjectStore:
    """S3/MinIO implementation with checksum-based overwrite protection."""

    def __init__(self, client: object, bucket: str) -> None:
        self.client = client
        self.bucket = bucket

    def put_immutable(self, key: str, content: bytes) -> None:
        try:
            existing = self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
        except ClientError as exc:
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
            Metadata={"sha256": sha256_bytes(content)},
        )

    def get(self, key: str) -> bytes:
        return self.client.get_object(Bucket=self.bucket, Key=key)["Body"].read()
