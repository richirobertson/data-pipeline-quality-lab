"""Typed source and provenance models."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, HttpUrl


class DatasetRef(BaseModel):
    """The ONS identity required to reproduce an extract."""

    model_config = ConfigDict(extra="forbid")

    id: str
    edition: str
    version: int = Field(ge=1)


class FilterDefinition(BaseModel):
    """A stable representation of the requested multidimensional extract."""

    model_config = ConfigDict(extra="forbid")

    dataset: DatasetRef
    population_type: str
    dimensions: list[dict[str, Any]]


class ArtifactRef(BaseModel):
    """Metadata for an immutable downloaded artifact."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["csv", "csvw", "json"]
    source_url: HttpUrl
    object_key: str
    sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    size_bytes: int = Field(ge=0)
    content_type: str | None = None
    etag: str | None = None


class IngestionManifest(BaseModel):
    """Evidence that binds one run to one ONS request and its artifacts."""

    model_config = ConfigDict(extra="forbid")

    manifest_version: Literal[1] = 1
    run_id: str
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    filter_hash: str = Field(pattern=r"^[a-f0-9]{64}$")
    filter_id: str
    filter_output_id: str
    source: DatasetRef
    artifacts: list[ArtifactRef]
