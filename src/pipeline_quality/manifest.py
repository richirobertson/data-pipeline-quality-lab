"""Canonical hashing and immutable ingestion manifests."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path

from pipeline_quality.models import ArtifactRef, DatasetRef, FilterDefinition, IngestionManifest


def sha256_bytes(content: bytes) -> str:
    """Return a stable content fingerprint used for identity and integrity."""
    return hashlib.sha256(content).hexdigest()


def canonical_filter_hash(definition: FilterDefinition) -> str:
    """Hash meaning rather than JSON formatting or dictionary insertion order."""
    payload = definition.model_dump(mode="json")
    # Sorted keys and compact separators make logically identical JSON serialize
    # to the same bytes before hashing.
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return sha256_bytes(canonical)


def artifact_key(
    source: DatasetRef,
    filter_hash: str,
    kind: str,
    checksum: str,
) -> str:
    """Build an inspectable, content-addressed landing-zone key."""
    # Source version and filter identity remain visible for operators, while the
    # final checksum ensures different bytes can never share an object identity.
    return (
        f"landing/ons/{source.id}/edition={source.edition}/version={source.version}/"
        f"filter={filter_hash}/{kind}/{checksum}"
    )


def build_artifact(
    *,
    source: DatasetRef,
    filter_hash: str,
    kind: str,
    source_url: str,
    content: bytes,
    content_type: str | None = None,
    etag: str | None = None,
) -> ArtifactRef:
    """Describe downloaded bytes without altering the source content."""
    checksum = sha256_bytes(content)
    return ArtifactRef(
        kind=kind,
        source_url=source_url,
        object_key=artifact_key(source, filter_hash, kind, checksum),
        sha256=checksum,
        size_bytes=len(content),
        content_type=content_type,
        etag=etag,
    )


def write_manifest(manifest: IngestionManifest, destination: Path) -> None:
    """Persist reproducible run evidence as readable JSON."""
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_text(manifest.model_dump_json(indent=2) + "\n", encoding="utf-8")
