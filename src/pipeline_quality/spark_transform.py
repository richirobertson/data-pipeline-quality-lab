"""PySpark source-contract and bronze-to-silver transformations."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

from pipeline_quality.exceptions import SchemaContractError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

RAW_HEADERS = [
    "Lower Tier Local Authorities Code",
    "Lower Tier Local Authorities",
    "Sex (2 categories) Code",
    "Sex (2 categories)",
    "Age (91 categories) Code",
    "Age (91 categories)",
    "Observation",
]
SOURCE_COLUMNS = [
    "geography_code",
    "geography_label",
    "sex_code",
    "sex_label",
    "age_code",
    "age_label",
    "observation",
]
BUSINESS_KEY = ["geography_code", "age_code", "sex_code"]


@dataclass(frozen=True)
class SourceMetadata:
    dataset_id: str
    edition: str
    version: int
    source_checksum: str
    pipeline_run_id: str
    loaded_at: str = "2026-01-01 00:00:00+00:00"


@dataclass(frozen=True)
class TransformationResult:
    accepted: DataFrame
    quarantined: DataFrame


def validate_csvw_contract(path: Path) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    columns = payload.get("tableSchema", {}).get("columns", [])
    names = [column.get("name") for column in columns]
    if names != RAW_HEADERS:
        raise SchemaContractError(
            f"CSVW columns do not match the source contract: expected {RAW_HEADERS}, got {names}"
        )
    datatypes = {column["name"]: column.get("datatype") for column in columns}
    if datatypes["Observation"] not in {"integer", "long"}:
        raise SchemaContractError("CSVW observation must be an integer")


def source_schema():
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    return StructType(
        [
            StructField("geography_code", StringType(), True),
            StructField("geography_label", StringType(), True),
            StructField("sex_code", StringType(), True),
            StructField("sex_label", StringType(), True),
            StructField("age_code", StringType(), True),
            StructField("age_label", StringType(), True),
            StructField("observation", LongType(), True),
        ]
    )


def raw_source_schema():
    from pyspark.sql.types import LongType, StringType, StructField, StructType

    return StructType(
        [
            StructField(RAW_HEADERS[0], StringType(), True),
            StructField(RAW_HEADERS[1], StringType(), True),
            StructField(RAW_HEADERS[2], StringType(), True),
            StructField(RAW_HEADERS[3], StringType(), True),
            StructField(RAW_HEADERS[4], StringType(), True),
            StructField(RAW_HEADERS[5], StringType(), True),
            StructField(RAW_HEADERS[6], LongType(), True),
        ]
    )


def read_source(spark: SparkSession, csv_path: str) -> DataFrame:
    from pyspark.sql import functions as f

    observed_headers = spark.read.option("header", True).csv(csv_path).columns
    if observed_headers != RAW_HEADERS:
        raise SchemaContractError(
            f"CSV columns do not match the source contract: expected {RAW_HEADERS}, "
            f"got {observed_headers}"
        )
    frame = spark.read.option("header", True).schema(raw_source_schema()).csv(csv_path)
    return frame.select(
        *[
            f.col(raw_name).alias(internal_name)
            for raw_name, internal_name in zip(RAW_HEADERS, SOURCE_COLUMNS, strict=True)
        ]
    )


def transform_observations(
    frame: DataFrame,
    metadata: SourceMetadata,
) -> TransformationResult:
    from pyspark.sql import Window
    from pyspark.sql import functions as f

    cleaned = frame
    for column in SOURCE_COLUMNS[:-1]:
        cleaned = cleaned.withColumn(column, f.trim(f.col(column)))

    missing_dimension = None
    for column in SOURCE_COLUMNS[:-1]:
        condition = f.col(column).isNull() | (f.col(column) == "")
        missing_dimension = (
            condition if missing_dimension is None else missing_dimension | condition
        )

    invalid = cleaned.withColumn(
        "quality_rule",
        f.when(missing_dimension, f.lit("missing_dimension"))
        .when(f.col("observation").isNull(), f.lit("invalid_observation"))
        .when(f.col("observation") < 0, f.lit("negative_observation")),
    )
    structurally_valid = invalid.filter(f.col("quality_rule").isNull()).drop("quality_rule")
    invalid_rows = invalid.filter(f.col("quality_rule").isNotNull())

    conflicting_keys = (
        structurally_valid.groupBy(*BUSINESS_KEY)
        .agg(f.countDistinct("observation").alias("distinct_observations"))
        .filter(f.col("distinct_observations") > 1)
        .select(*BUSINESS_KEY)
    )
    conflicting_rows = structurally_valid.join(conflicting_keys, BUSINESS_KEY, "inner").withColumn(
        "quality_rule", f.lit("conflicting_duplicate")
    )
    non_conflicting = structurally_valid.join(conflicting_keys, BUSINESS_KEY, "left_anti")

    exact_window = Window.partitionBy(*BUSINESS_KEY, "observation").orderBy(
        f.col("geography_label"), f.col("age_label"), f.col("sex_label")
    )
    ranked = non_conflicting.withColumn("_duplicate_rank", f.row_number().over(exact_window))
    exact_duplicates = (
        ranked.filter(f.col("_duplicate_rank") > 1)
        .drop("_duplicate_rank")
        .withColumn("quality_rule", f.lit("exact_duplicate"))
    )
    accepted = ranked.filter(f.col("_duplicate_rank") == 1).drop("_duplicate_rank")

    quarantined = invalid_rows.unionByName(conflicting_rows).unionByName(exact_duplicates)
    accepted = _add_provenance(accepted, metadata)
    quarantined = _add_provenance(quarantined, metadata)

    return TransformationResult(
        accepted=accepted.select(
            *SOURCE_COLUMNS,
            "dataset_id",
            "edition",
            "version",
            "source_checksum",
            "pipeline_run_id",
            "source_record_hash",
            "loaded_at",
        ),
        quarantined=quarantined.select(
            *SOURCE_COLUMNS,
            "quality_rule",
            "dataset_id",
            "edition",
            "version",
            "source_checksum",
            "pipeline_run_id",
            "source_record_hash",
            "loaded_at",
        ),
    )


def _add_provenance(frame: DataFrame, metadata: SourceMetadata) -> DataFrame:
    from pyspark.sql import functions as f

    return (
        frame.withColumn("dataset_id", f.lit(metadata.dataset_id))
        .withColumn("edition", f.lit(metadata.edition))
        .withColumn("version", f.lit(metadata.version))
        .withColumn("source_checksum", f.lit(metadata.source_checksum))
        .withColumn("pipeline_run_id", f.lit(metadata.pipeline_run_id))
        .withColumn("loaded_at", f.to_timestamp(f.lit(metadata.loaded_at)))
        .withColumn(
            "source_record_hash",
            f.sha2(
                f.concat_ws(
                    "||",
                    *[
                        f.coalesce(f.col(column).cast("string"), f.lit("<null>"))
                        for column in SOURCE_COLUMNS
                    ],
                ),
                256,
            ),
        )
    )


def write_results(
    result: TransformationResult,
    *,
    curated_path: str,
    quarantine_path: str,
) -> None:
    (
        result.accepted.write.mode("overwrite")
        .partitionBy("edition", "version")
        .parquet(curated_path)
    )
    (
        result.quarantined.write.mode("overwrite")
        .partitionBy("edition", "version", "quality_rule")
        .parquet(quarantine_path)
    )


def load_postgres(
    frame: DataFrame,
    *,
    jdbc_url: str,
    table: str,
    user: str,
    password: str,
    mode: str = "append",
) -> None:
    (
        frame.write.format("jdbc")
        .option("url", jdbc_url)
        .option("dbtable", table)
        .option("user", user)
        .option("password", password)
        .option("driver", "org.postgresql.Driver")
        .option("truncate", "true")
        .mode(mode)
        .save()
    )


def ensure_postgres_schema(
    *,
    host: str,
    port: int,
    database: str,
    user: str,
    password: str,
    schema: str,
) -> None:
    import psycopg2
    from psycopg2 import sql

    with (
        psycopg2.connect(
            host=host,
            port=port,
            dbname=database,
            user=user,
            password=password,
        ) as connection,
        connection.cursor() as cursor,
    ):
        cursor.execute(sql.SQL("create schema if not exists {}").format(sql.Identifier(schema)))
