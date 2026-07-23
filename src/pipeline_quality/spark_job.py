"""Executable PySpark fixture job used locally and in CI."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from pipeline_quality.manifest import sha256_bytes
from pipeline_quality.spark_transform import (
    SourceMetadata,
    ensure_postgres_schema,
    load_postgres,
    read_source,
    transform_observations,
    validate_csvw_contract,
    write_results,
)


def build_parser() -> argparse.ArgumentParser:
    """Define file, evidence, and optional JDBC boundaries for the Spark stage."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--csv", type=Path, required=True)
    parser.add_argument("--csvw", type=Path, required=True)
    parser.add_argument("--curated", type=Path, required=True)
    parser.add_argument("--quarantine", type=Path, required=True)
    parser.add_argument("--run-id", default="fixture-run")
    parser.add_argument(
        "--summary", type=Path, default=Path("evidence/generated/spark-summary.json")
    )
    parser.add_argument("--jdbc-url")
    parser.add_argument("--db-table", default="raw.raw_ons_observations")
    parser.add_argument("--db-host", default="postgres")
    parser.add_argument("--db-port", type=int, default=5432)
    parser.add_argument("--db-name", default="quality_lab")
    parser.add_argument("--db-user", default="pipeline")
    parser.add_argument("--db-password", default="pipeline")
    return parser


def main(argv: list[str] | None = None) -> int:
    """Validate, transform, persist, optionally load, and summarize one extract."""
    # Importing Spark lazily keeps fast Python unit tests lightweight.
    from pyspark.sql import SparkSession

    args = build_parser().parse_args(argv)
    # Fail on metadata drift before starting the more expensive Spark session.
    validate_csvw_contract(args.csvw)
    spark = SparkSession.builder.appName("data-pipeline-quality-lab").getOrCreate()
    try:
        frame = read_source(spark, str(args.csv))
        result = transform_observations(
            frame,
            SourceMetadata(
                dataset_id="TS009",
                edition="2021",
                version=1,
                source_checksum=sha256_bytes(args.csv.read_bytes()),
                pipeline_run_id=args.run_id,
            ),
        )
        write_results(
            result,
            curated_path=str(args.curated),
            quarantine_path=str(args.quarantine),
        )
        if args.jdbc_url:
            # Separating schema creation from Spark's JDBC writer makes the first
            # run work against a completely empty PostgreSQL database.
            schema = args.db_table.split(".", maxsplit=1)[0]
            ensure_postgres_schema(
                host=args.db_host,
                port=args.db_port,
                database=args.db_name,
                user=args.db_user,
                password=args.db_password,
                schema=schema,
            )
            load_postgres(
                result.accepted,
                jdbc_url=args.jdbc_url,
                table=args.db_table,
                user=args.db_user,
                password=args.db_password,
                mode="overwrite",
            )
        # Counts are materialized deliberately: they form the reconciliation
        # evidence that input equals accepted plus quarantined records.
        summary = {
            "pipeline_run_id": args.run_id,
            "dataset_id": "TS009",
            "edition": "2021",
            "version": 1,
            "input_rows": frame.count(),
            "accepted_rows": result.accepted.count(),
            "quarantined_rows": result.quarantined.count(),
        }
        args.summary.parent.mkdir(parents=True, exist_ok=True)
        args.summary.write_text(json.dumps(summary, indent=2) + "\n", encoding="utf-8")
        print(json.dumps(summary))
    finally:
        # Spark owns JVM resources that must be released even after a failed job.
        spark.stop()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
