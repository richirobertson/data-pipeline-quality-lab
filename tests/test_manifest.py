from __future__ import annotations

import re

from hypothesis import given
from hypothesis import strategies as st

from pipeline_quality.manifest import (
    artifact_key,
    build_artifact,
    canonical_filter_hash,
    sha256_bytes,
)
from pipeline_quality.models import DatasetRef, FilterDefinition


def test_filter_hash_is_stable_when_json_key_order_changes(load_fixture) -> None:
    payload = load_fixture("filter-definition.json")
    first = canonical_filter_hash(FilterDefinition.model_validate(payload))
    reordered = {
        "dimensions": payload["dimensions"],
        "population_type": payload["population_type"],
        "dataset": {
            "version": 1,
            "edition": "2021",
            "id": "TS009",
        },
    }
    second = canonical_filter_hash(FilterDefinition.model_validate(reordered))

    assert first == second
    assert re.fullmatch(r"[a-f0-9]{64}", first)


@given(st.binary())
def test_sha256_is_always_a_lowercase_hex_digest(content: bytes) -> None:
    assert re.fullmatch(r"[a-f0-9]{64}", sha256_bytes(content))


def test_artifact_contains_content_addressed_key() -> None:
    source = DatasetRef(id="TS009", edition="2021", version=1)
    artifact = build_artifact(
        source=source,
        filter_hash="a" * 64,
        kind="csv",
        source_url="https://static.ons.gov.uk/example.csv",
        content=b"example",
        content_type="text/csv",
    )

    assert artifact.sha256 in artifact.object_key
    assert artifact.object_key == artifact_key(source, "a" * 64, "csv", artifact.sha256)
    assert artifact.size_bytes == 7
