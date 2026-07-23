# ADR 0003: defer an orchestration framework

Status: accepted

The vertical slice uses explicit commands and run identifiers. Airflow, Dagster, or Prefect will be evaluated only after stage contracts, idempotency, and recovery semantics are proven.

Adding orchestration earlier would increase infrastructure without improving confidence in the core data boundaries.

