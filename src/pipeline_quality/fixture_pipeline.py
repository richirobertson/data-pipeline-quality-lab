"""Create reproducible landing artifacts and provenance from repository fixtures."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipeline_quality.manifest import (
    build_artifact,
    canonical_filter_hash,
    write_manifest,
)
from pipeline_quality.models import DatasetRef, FilterDefinition, IngestionManifest
from pipeline_quality.object_store import FileObjectStore


def build_parser() -> argparse.ArgumentParser:
    """Expose fixture locations so tests can isolate output in temporary folders."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--fixtures", type=Path, default=Path("tests/fixtures"))
    parser.add_argument("--landing", type=Path, default=Path("data/landing"))
    parser.add_argument(
        "--manifest", type=Path, default=Path("evidence/generated/fixture-manifest.json")
    )
    parser.add_argument("--run-id", default="fixture-run")
    return parser


def run_fixture_pipeline(
    *,
    fixtures: Path,
    landing: Path,
    manifest_path: Path,
    run_id: str,
) -> IngestionManifest:
    """Land deterministic ONS-shaped fixtures and describe them in a manifest."""
    # Pydantic validates the repository fixture just as it would validate a live
    # request definition, preventing test-only data from bypassing the contract.
    definition = FilterDefinition.model_validate_json(
        (fixtures / "filter-definition.json").read_text(encoding="utf-8")
    )
    filter_hash = canonical_filter_hash(definition)
    source = DatasetRef.model_validate(definition.dataset.model_dump())
    store = FileObjectStore(landing)

    # CSV carries the observations; CSVW carries the schema metadata used to
    # interpret them. Keeping both mirrors the real ONS download boundary.
    artifact_specs = [
        (
            "csv",
            "https://static.ons.gov.uk/datasets/example/TS009.csv",
            fixtures / "ons-population.csv",
            "text/csv",
        ),
        (
            "csvw",
            "https://static.ons.gov.uk/datasets/example/TS009.csvw",
            fixtures / "ons-population.csvw",
            "application/csvm+json",
        ),
    ]
    artifacts = []
    for kind, url, path, content_type in artifact_specs:
        content = path.read_bytes()
        # The artifact descriptor is calculated before storage so its object key
        # is derived from the exact bytes that will be written.
        artifact = build_artifact(
            source=source,
            filter_hash=filter_hash,
            kind=kind,
            source_url=url,
            content=content,
            content_type=content_type,
        )
        store.put_immutable(artifact.object_key, content)
        artifacts.append(artifact)

    manifest = IngestionManifest(
        run_id=run_id,
        filter_hash=filter_hash,
        filter_id="fixture-filter",
        filter_output_id="fixture-output",
        source=source,
        artifacts=artifacts,
    )
    write_manifest(manifest, manifest_path)
    return manifest


def main(argv: list[str] | None = None) -> int:
    """Run fixture ingestion as a shell-friendly pipeline stage."""
    args = build_parser().parse_args(argv)
    manifest = run_fixture_pipeline(
        fixtures=args.fixtures,
        landing=args.landing,
        manifest_path=args.manifest,
        run_id=args.run_id,
    )
    print(json.dumps({"run_id": manifest.run_id, "artifacts": len(manifest.artifacts)}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
