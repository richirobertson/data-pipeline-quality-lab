from __future__ import annotations

import pytest
from botocore.exceptions import ClientError

from pipeline_quality.exceptions import ObjectConflictError
from pipeline_quality.object_store import FileObjectStore, S3ObjectStore


def test_file_store_is_idempotent_for_the_same_content(tmp_path) -> None:
    store = FileObjectStore(tmp_path)

    store.put_immutable("landing/example", b"same")
    store.put_immutable("landing/example", b"same")

    assert store.get("landing/example") == b"same"


def test_file_store_rejects_different_content_for_an_existing_key(tmp_path) -> None:
    store = FileObjectStore(tmp_path)
    store.put_immutable("landing/example", b"first")

    with pytest.raises(ObjectConflictError, match="immutable key"):
        store.put_immutable("landing/example", b"second")


class FakeS3:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}

    def get_object(self, *, Bucket: str, Key: str):
        try:
            content = self.objects[(Bucket, Key)]
        except KeyError as exc:
            raise ClientError(
                {"Error": {"Code": "NoSuchKey", "Message": "missing"}},
                "GetObject",
            ) from exc
        return {"Body": FakeBody(content)}

    def put_object(self, *, Bucket: str, Key: str, Body: bytes, Metadata: dict):
        assert Metadata["sha256"]
        self.objects[(Bucket, Key)] = Body


class FakeBody:
    def __init__(self, content: bytes) -> None:
        self.content = content

    def read(self) -> bytes:
        return self.content


def test_s3_store_creates_reads_and_reuses_immutable_object() -> None:
    client = FakeS3()
    store = S3ObjectStore(client, "quality")

    store.put_immutable("landing/key", b"value")
    store.put_immutable("landing/key", b"value")

    assert store.get("landing/key") == b"value"


def test_s3_store_rejects_content_conflict() -> None:
    client = FakeS3()
    store = S3ObjectStore(client, "quality")
    store.put_immutable("landing/key", b"first")

    with pytest.raises(ObjectConflictError, match="immutable key"):
        store.put_immutable("landing/key", b"second")
