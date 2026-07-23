"""Generate a compact, human-readable pipeline quality evidence report."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


def load_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload if isinstance(payload, dict) else {}


def dbt_summary(run_results: dict[str, Any]) -> tuple[int, int, int]:
    results = run_results.get("results", [])
    passed = sum(result.get("status") in {"success", "pass"} for result in results)
    failed = sum(result.get("status") in {"error", "fail", "runtime error"} for result in results)
    return len(results), passed, failed


def render_report(
    *,
    manifest: dict[str, Any],
    spark: dict[str, Any],
    dbt_results: dict[str, Any],
) -> str:
    dbt_total, dbt_passed, dbt_failed = dbt_summary(dbt_results)
    artifacts = manifest.get("artifacts", [])
    artifact_lines = (
        "\n".join(
            f"- `{item.get('kind', 'unknown')}`: `{item.get('sha256', 'missing')}`"
            for item in artifacts
        )
        or "- No manifest artifacts found"
    )
    return f"""# Pipeline quality evidence

## Run identity

- Pipeline run: `{spark.get("pipeline_run_id", manifest.get("run_id", "not generated"))}`
- Dataset: `{spark.get("dataset_id", manifest.get("source", {}).get("id", "unknown"))}`
- Edition: `{spark.get("edition", manifest.get("source", {}).get("edition", "unknown"))}`
- Version: `{spark.get("version", manifest.get("source", {}).get("version", "unknown"))}`
- Filter hash: `{manifest.get("filter_hash", "not generated")}`

## Source provenance

{artifact_lines}

## Spark reconciliation

| Measure | Count |
|---|---:|
| Input | {spark.get("input_rows", "not generated")} |
| Accepted | {spark.get("accepted_rows", "not generated")} |
| Quarantined | {spark.get("quarantined_rows", "not generated")} |

## dbt results

| Measure | Count |
|---|---:|
| Executed | {dbt_total} |
| Passed | {dbt_passed} |
| Failed | {dbt_failed} |

## Interpretation

A release is acceptable when source artifacts have immutable checksums, Spark input equals
accepted plus quarantined records, dbt layer counts reconcile, and no dbt test fails.
"""


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--manifest", type=Path, default=Path("evidence/generated/fixture-manifest.json")
    )
    parser.add_argument("--spark", type=Path, default=Path("evidence/generated/spark-summary.json"))
    parser.add_argument(
        "--dbt-results", type=Path, default=Path("warehouse/target/run_results.json")
    )
    parser.add_argument("--output", type=Path, default=Path("evidence/generated/quality-report.md"))
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = render_report(
        manifest=load_json(args.manifest),
        spark=load_json(args.spark),
        dbt_results=load_json(args.dbt_results),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(report, encoding="utf-8")
    print(args.output)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
