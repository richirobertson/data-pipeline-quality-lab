from __future__ import annotations

import pytest

pytestmark = pytest.mark.spark


@pytest.fixture(scope="session")
def spark():
    pytest.importorskip("pyspark")
    from pyspark.sql import SparkSession

    session = (
        SparkSession.builder.master("local[2]")
        .appName("pipeline-quality-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    yield session
    session.stop()


def metadata():
    from pipeline_quality.spark_transform import SourceMetadata

    return SourceMetadata(
        dataset_id="TS009",
        edition="2021",
        version=1,
        source_checksum="a" * 64,
        pipeline_run_id="test-run",
    )


def test_transform_accepts_fixture_at_declared_grain(spark, fixture_dir) -> None:
    from pipeline_quality.spark_transform import read_source, transform_observations

    result = transform_observations(
        read_source(spark, str(fixture_dir / "ons-population.csv")),
        metadata(),
    )

    assert result.accepted.count() == 8
    assert result.quarantined.count() == 0
    assert result.accepted.select("source_record_hash").distinct().count() == 8


def test_invalid_and_duplicate_rows_are_explainable(spark) -> None:
    from pipeline_quality.spark_transform import source_schema, transform_observations

    rows = [
        ("E1", "Area", "2", "Male", "Y", "Young", 10),
        ("E1", "Area", "2", "Male", "Y", "Young", 10),
        ("E1", "Area", "1", "Female", "A", "Adult", 20),
        ("E1", "Area", "1", "Female", "A", "Adult", 21),
        ("", "Area", "1", "Female", "O", "Older", 30),
        ("E1", "Area", "2", "Male", "O", "Older", -1),
    ]
    frame = spark.createDataFrame(rows, source_schema()).repartition(3)

    result = transform_observations(frame, metadata())
    rules = {
        row["quality_rule"]: row["count"]
        for row in result.quarantined.groupBy("quality_rule").count().collect()
    }

    assert result.accepted.count() == 1
    assert rules == {
        "exact_duplicate": 1,
        "conflicting_duplicate": 2,
        "missing_dimension": 1,
        "negative_observation": 1,
    }


def test_results_do_not_depend_on_input_partition_count(spark, fixture_dir) -> None:
    from pipeline_quality.spark_transform import read_source, transform_observations

    frame = read_source(spark, str(fixture_dir / "ons-population.csv"))
    one_partition = transform_observations(frame.repartition(1), metadata())
    four_partitions = transform_observations(frame.repartition(4), metadata())

    first = {
        tuple(row[column] for column in ["geography_code", "age_code", "sex_code", "observation"])
        for row in one_partition.accepted.collect()
    }
    second = {
        tuple(row[column] for column in ["geography_code", "age_code", "sex_code", "observation"])
        for row in four_partitions.accepted.collect()
    }

    assert first == second
