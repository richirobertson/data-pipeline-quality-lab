# ADR 0001: dbt owns business transformations

Status: accepted

PySpark owns technical parsing, validation, deduplication, and conformance. dbt owns dimensional modelling, aggregations, contracts, reconciliation, and documentation.

This avoids duplicated business definitions across Python and SQL while retaining Spark-specific tests for partition and distributed-processing behaviour.

