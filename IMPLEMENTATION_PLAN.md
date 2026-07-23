# Implementation plan

## Goal

Build a small but production-minded batch pipeline using Department for Transport Road Safety Data discovered through data.gov.uk. The repository should demonstrate senior-level testing decisions across ingestion, storage, transformation, orchestration, and release confidence.

The first useful release will process one annual snapshot of collision, vehicle, and casualty data. Later increments can introduce multiple years, backfills, and deliberately faulty fixtures.

## Guiding constraints

- Do not use DuckDB.
- Keep external calls out of pull-request-critical tests.
- Preserve every downloaded source file unchanged and record its checksum.
- Make every pipeline stage repeatable and idempotent.
- Treat data quality failures as first-class outputs, not only log messages.
- Prefer focused contracts and reconciliations over indiscriminate column assertions.
- Make local execution possible with Docker Compose and one documented command.

## Phase 1 — establish the repository

1. Add Python project metadata, dependency locking, formatting, linting, type checking, and pre-commit hooks.
2. Add a Docker Compose environment containing PostgreSQL.
3. Define the repository layout for source adapters, ingestion, contracts, orchestration, dbt, fixtures, and tests.
4. Add GitHub Actions for deterministic checks.
5. Record architecture and testing decisions in short ADRs.

Exit criteria:

- A clean checkout can run formatting, static analysis, and an empty test suite.
- PostgreSQL starts locally and is exercised by a container smoke test.
- Pull requests do not require data.gov.uk availability.

## Phase 2 — source discovery and immutable ingestion

1. Implement a data.gov.uk CKAN catalogue client using the documented `package_search` and `package_show` actions.
2. Represent catalogue responses with typed models.
3. Resolve the selected road-safety resources without hard-coding transient download URLs where possible.
4. Download files using streaming I/O, bounded retries, timeouts, and a descriptive user agent.
5. Store immutable raw files with retrieval timestamp, source URL, HTTP metadata, size, and SHA-256 checksum.
6. Write a manifest describing every ingestion attempt.

Tests:

- Recorded catalogue fixtures for deterministic contract tests.
- Timeout, `429`, `5xx`, malformed JSON, HTML error page, truncated download, and checksum mismatch cases.
- Property tests for filename and manifest generation.
- Idempotency test proving the same resource is not silently duplicated.
- Optional scheduled live contract check against data.gov.uk.

Exit criteria:

- One annual dataset can be discovered and downloaded reproducibly.
- An upstream outage produces actionable evidence without corrupting local state.

## Phase 3 — raw-data contracts and quarantine

1. Define Pandera contracts for collision, vehicle, and casualty files.
2. Validate headers, encodings, types, nullability, enumerations, identifiers, date/time fields, and geographic ranges.
3. Separate structural failures from row-level quality failures.
4. Send invalid rows to quarantine with dataset, source file, row identifier, rule, observed value, and timestamp.
5. Publish a validation summary in JSON and Markdown.

Tests:

- Golden examples representing known-good source rows.
- Focused fixtures for missing columns, unexpected columns, invalid codes, corrupt dates, impossible coordinates, and duplicate identifiers.
- Mutation testing of the most important validation rules.
- Property tests for boundary values and null representations.

Exit criteria:

- Invalid data is explainable and traceable to its source.
- Validation results are deterministic and machine-readable.

## Phase 4 — PostgreSQL loading

1. Create raw and staging schemas with explicit migration tooling.
2. Load valid records in bounded batches.
3. Track file-level and row-level lineage.
4. Use natural source identifiers plus load metadata to make reruns safe.
5. Implement transactional failure handling and resumable loads.

Tests:

- Testcontainers integration tests against the supported PostgreSQL version.
- Duplicate ingestion and replay tests.
- Transaction rollback tests for partial batch failure.
- Row-count and checksum reconciliation between validated files and staging tables.
- Migration tests from an empty and previous-version database.

Exit criteria:

- Reprocessing the same source produces no duplicate business records.
- Partial failure cannot leave an apparently successful load.

## Phase 5 — dbt transformations and analytical marts

1. Create staging models that preserve source meaning while normalising names and types.
2. Create intermediate models joining collisions, vehicles, and casualties.
3. Create a small mart for annual and geographic safety measures.
4. Document models, columns, ownership, and lineage.
5. Add dbt source freshness and schema contracts.

Tests:

- Uniqueness, non-null, accepted-value, and relationship tests.
- Custom tests for collision/vehicle/casualty cardinality.
- Reconciliation of source totals through every layer.
- Singular SQL tests for impossible domain combinations.
- Unit tests for high-risk transformation logic.

Exit criteria:

- Every published measure can be traced and reconciled to source data.
- Broken joins and silent row multiplication are detected.

## Phase 6 — orchestration and operational behaviour

1. Build a Prefect flow for discovery, download, validation, load, dbt build, and evidence publication.
2. Define retry policies by failure class rather than applying retries indiscriminately.
3. Add backfill parameters and safe concurrency controls.
4. Emit structured logs, run identifiers, metrics, and OpenTelemetry traces.
5. Expose a clear terminal state: passed, passed with quarantined rows, source unavailable, or failed quality gate.

Tests:

- Flow-level tests with task boundaries mocked appropriately.
- Recovery from interrupted runs.
- Retry classification tests.
- Concurrent-run and lock tests.
- Backfill tests spanning multiple source snapshots.

Exit criteria:

- A failed run is diagnosable without rerunning it.
- Reruns and backfills do not change previously accepted results unexpectedly.

## Phase 7 — deliberate fault laboratory

Create versioned faulty datasets that introduce:

1. Duplicate collisions.
2. Orphan casualties and vehicles.
3. Missing files.
4. Schema drift.
5. Changed code lists.
6. Invalid geographic coordinates.
7. Late corrections to historic records.
8. Row multiplication during a join.
9. A unit or semantic change that remains type-valid.
10. A partial publication that looks superficially successful.

For each fault, document:

- The business risk.
- The earliest useful detection point.
- The test or monitor that detects it.
- The evidence shown to an operator.
- The recovery procedure.

Exit criteria:

- CI proves each fault is caught by the intended control.
- The repository contains a risk-to-test coverage matrix.

## Phase 8 — release evidence and portfolio presentation

1. Generate a static Markdown/HTML quality report for a sample run.
2. Include source provenance, checksums, row counts, rejected rows, reconciliations, freshness, test results, and pipeline timing.
3. Publish CI artifacts without committing bulky government datasets.
4. Add an architecture diagram, demonstration recording, quick start, and guided reviewer path.
5. Add security scanning, Dependabot, licence, contribution guidance, and tagged releases.

Exit criteria:

- A reviewer can understand the system and inspect credible evidence in under ten minutes.
- The repository demonstrates why the data product is trustworthy, not merely that the code has tests.

## Suggested milestones

1. `v0.1-foundation` — tooling, PostgreSQL, CI, and architecture.
2. `v0.2-ingestion` — catalogue discovery, immutable downloads, and manifests.
3. `v0.3-contracts` — validation, quarantine, and reports.
4. `v0.4-warehouse` — PostgreSQL loading and dbt transformations.
5. `v0.5-operations` — Prefect, backfills, telemetry, and recovery.
6. `v1.0-quality-lab` — fault catalogue and published release evidence.

## Initial backlog

The first implementation slice should be deliberately narrow:

1. Confirm the exact current data.gov.uk road-safety package identifier and resource URLs once the catalogue is available.
2. Add the Python project and Docker Compose PostgreSQL service.
3. Save one representative catalogue response and small, licence-compatible data fixtures.
4. Implement catalogue discovery and raw-file manifest generation.
5. Add deterministic success and upstream-failure tests.
6. Run those checks in GitHub Actions.

This slice establishes the boundaries and testing model before committing to the full transformation design.
