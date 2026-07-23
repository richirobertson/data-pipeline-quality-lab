# Implementation plan

## Current status

The first vertical slice is implemented. It covers the platform foundation, typed ONS
client and fixtures, immutable manifests and object storage, PySpark validation and
quarantine, JDBC loading, a contracted dbt warehouse, deterministic CI, and generated
quality evidence. Later increments in this plan—revision snapshots, backfill controls,
broader fault injection, orchestration, and telemetry—remain intentionally out of scope
for the initial portfolio release.

## Goal

Build a production-minded analytical pipeline using the ONS Beta API and Census 2021 data. The system will request a bounded population extract by age, sex, and geography; preserve its provenance; process it with PySpark; load it into PostgreSQL; and use dbt to create tested analytical models.

The repository should demonstrate how a senior test engineer establishes confidence across an API, distributed data processing, a warehouse, dbt transformations, and operational reruns.

## Why this use case

The ONS API provides several useful real-world behaviours:

- datasets are divided into editions and versions;
- new versions may represent corrections, revisions, or newly available data;
- multidimensional observations must retain their dimension context;
- Census filter requests create asynchronous outputs before CSV and CSVW artifacts become available;
- the API is currently beta and may introduce breaking changes;
- clients must handle rate limiting and respect `Retry-After`.

These behaviours make the source suitable for testing provenance, schema evolution, asynchronous workflows, revisions, dimensional modelling, and reconciliation.

## Architectural boundaries

### Python ingestion client

Owns:

- ONS API requests and typed response models;
- dataset, edition, version, and dimension discovery;
- filter creation, submission, polling, and artifact download;
- timeouts, retries, `Retry-After`, checksums, and ingestion manifests.

It does not own analytical transformation logic.

### MinIO landing and curated zones

Own:

- immutable ONS responses and downloaded CSV/CSVW artifacts;
- content-addressed source manifests;
- partitioned Parquet produced by Spark;
- reproducible inputs for reruns and tests.

The design uses MinIO locally to exercise S3-style object-storage behaviour without requiring a cloud account.

### PySpark

Owns:

- explicit source schemas;
- CSV/CSVW parsing and agreement checks;
- technical validation and rejected-record routing;
- normalisation to a stable observation shape;
- deterministic deduplication;
- partitioned Parquet output;
- loading conformed records into PostgreSQL through JDBC.

PySpark is used where its distributed semantics matter. Business measures and warehouse joins remain in dbt so ownership is clear.

### PostgreSQL and dbt

dbt is the centre of the analytical implementation. It owns:

- source declarations and freshness;
- staging, intermediate, dimension, fact, and mart models;
- model contracts;
- generic, singular, unit, and relationship tests;
- incremental-model and full-refresh equivalence;
- snapshots of revision-sensitive metadata;
- reconciliation and audit models;
- documentation, exposures, and lineage.

PostgreSQL provides a realistic warehouse target without adding a paid cloud dependency.

### Pipeline control

The first release uses a small Python command that executes the stages and records a run identifier. An orchestration framework will only be added after the stage contracts and recovery semantics are proven; orchestration should not obscure the data-quality design.

## Guiding constraints

- Do not use DuckDB.
- Make dbt the primary transformation and data-testing layer.
- Use PySpark for technical normalisation, not for business logic duplicated from dbt.
- Keep external ONS calls out of pull-request-critical tests.
- Preserve every source response and artifact unchanged with a checksum.
- Make ingestion, Spark processing, JDBC loading, and dbt models idempotent.
- Treat quarantined data and failed quality gates as queryable outputs.
- Never silently overwrite a previously processed ONS version.
- Make the complete platform runnable through Docker Compose.

## Phase 1 — platform foundation

1. Add Python project metadata, locked dependencies, formatting, linting, type checking, and pre-commit hooks.
2. Add Docker Compose services for MinIO and PostgreSQL.
3. Add a local PySpark runtime with the pinned Java, Spark, Hadoop, S3, and PostgreSQL JDBC versions documented.
4. Scaffold the dbt project and validate its PostgreSQL profile.
5. Define folders for ingestion, Spark jobs, dbt models, fixtures, contracts, evidence, and ADRs.
6. Add deterministic GitHub Actions checks.

Tests:

- Container health checks.
- PostgreSQL and MinIO connectivity tests.
- A Spark session smoke test with the production configuration.
- `dbt debug`, parsing, and an empty build.
- Dependency compatibility checks for Spark/JDBC/S3 packages.

Exit criteria:

- One command starts the local platform.
- CI can lint, type-check, run unit tests, initialise Spark, and parse the dbt project.
- No CI check requires live ONS availability.

## Phase 2 — ONS discovery and filter workflow

1. Implement a typed ONS client against `https://api.beta.ons.gov.uk/v1`.
2. Discover and persist the selected Census dataset, edition, version, dimensions, options, and geography codes.
3. Create a bounded filter for population by age, sex, and geography.
4. Submit the filter and poll its output with a finite state machine.
5. Download CSV and CSVW artifacts once ready.
6. Store request/response metadata, source identifiers, URLs, timestamps, sizes, ETags where available, and SHA-256 checksums in a manifest.
7. Send a simple, non-identifying versioned user agent and respect ONS rate limits.

Tests:

- Recorded fixtures for discovery, filter creation, submission, pending, ready, and failed states.
- State-machine tests for valid and invalid filter-output transitions.
- `429` tests that verify `Retry-After` is honoured.
- Timeout, `5xx`, malformed JSON, unexpected content type, truncated download, and checksum mismatch tests.
- Property tests for manifests and polling limits.
- A scheduled, non-blocking live contract test.

Exit criteria:

- A filter request can be replayed from its manifest.
- A source outage or unfinished filter cannot create a successful ingestion record.

## Phase 3 — immutable object storage and provenance

1. Define landing-zone keys using dataset, edition, version, filter definition hash, and artifact checksum.
2. Store raw API responses, CSV, CSVW, and manifests in MinIO without mutation.
3. Prevent a key from being overwritten by different content.
4. Add a run ledger in PostgreSQL linking pipeline runs to immutable source artifacts.
5. Define retention rules for small repository fixtures versus downloaded runtime data.

Tests:

- Content-addressability and overwrite-protection tests.
- Repeated-download idempotency.
- Interrupted upload cleanup.
- Manifest-to-object checksum reconciliation.
- Missing-object and stale-manifest behaviour.

Exit criteria:

- Every downstream row can be associated with an immutable ONS version and artifact.
- Rerunning ingestion cannot silently replace accepted inputs.

## Phase 4 — PySpark bronze-to-silver processing

1. Read the CSV and CSVW metadata from MinIO.
2. Define an explicit Spark schema instead of relying on inference.
3. Verify CSV columns and types agree with CSVW metadata.
4. Normalise the source into a stable long-form observation model containing:
   - dataset, edition, and version;
   - geography code and label;
   - age code and label;
   - sex code and label;
   - observation value;
   - source checksum and pipeline run identifier.
5. Route invalid rows to a quarantine Parquet dataset with rule identifiers and observed values.
6. Deduplicate deterministically using the full dimensional business key.
7. Write curated Parquet partitioned by edition, version, and geography type.
8. Load conformed observations and source metadata into PostgreSQL using JDBC.

Tests:

- Local Spark unit tests over small fixtures.
- Explicit schema drift tests for missing, added, reordered, and retyped fields.
- CSV/CSVW disagreement tests.
- Null, invalid numeric, negative observation, and unexpected category tests.
- Duplicate records spread across partitions.
- Partition-count invariance and deterministic-output tests.
- Property tests comparing PySpark results with a small pure-Python reference implementation.
- JDBC retry, transaction, and partial-write tests.

Exit criteria:

- Equivalent input produces equivalent curated data regardless of Spark partition count.
- Rejected data is traceable and does not disappear from reconciliation totals.

## Phase 5 — dbt staging and contracts

1. Declare raw PostgreSQL sources with freshness and loaded-at metadata.
2. Build staging models that rename, type, and document source columns without changing grain.
3. Enable dbt contracts on public models.
4. Define the observation business key and enforce uniqueness.
5. Add reusable generic tests for codes, labels, non-negative observations, and provenance fields.
6. Add audit models comparing Spark load manifests with dbt source counts.

Tests:

- dbt source freshness.
- `not_null`, `unique`, `accepted_values`, and relationship tests.
- Singular tests for incomplete business keys and invalid measures.
- dbt unit tests for high-risk staging rules.
- Contract-failure fixtures proving breaking changes stop the build.

Exit criteria:

- dbt cannot build on an unrecognised source schema.
- Staging preserves source grain and reconciles exactly with accepted Spark output.

## Phase 6 — dimensional warehouse and marts

1. Build conformed geography, age, sex, dataset-version, and pipeline-run dimensions.
2. Build a population-observation fact table at the declared dimensional grain.
3. Build marts for:
   - population totals by geography;
   - age distribution by geography;
   - sex distribution by geography;
   - cross-version revision comparisons.
4. Add dbt exposures for the generated quality report.
5. Generate and publish dbt documentation and lineage.

Tests:

- Referential integrity from facts to every dimension.
- Fact-grain uniqueness.
- Totals equal the sum of disjoint age and sex categories where the source definition supports it.
- No row multiplication across joins.
- Geographic roll-up reconciliation with documented tolerances.
- dbt unit tests for banding and aggregation logic.

Exit criteria:

- Every published measure can be traced to its source artifact and ONS version.
- Mart totals reconcile to the accepted source observations.

## Phase 7 — incremental models, revisions, and backfills

1. Implement incremental loading keyed by ONS dataset, edition, version, and dimensional business key.
2. Snapshot dataset metadata and dimension labels to expose revisions.
3. Define behaviour for a new version of an existing edition.
4. Add a parameterised backfill command.
5. Compare incremental and full-refresh results.
6. Prevent concurrent runs from publishing the same version twice.

Tests:

- First load, no-change rerun, new-version, corrected-observation, and removed-observation scenarios.
- Incremental versus full-refresh equivalence.
- Out-of-order version arrival.
- Backfill across multiple versions.
- Concurrent-run locking and recovery after interruption.
- Snapshot history and validity-window tests.

Exit criteria:

- A revised ONS version is visible as a revision, not an unexplained overwrite.
- Full refresh and incremental processing produce equivalent current-state marts.

## Phase 8 — deliberate quality-failure laboratory

Create versioned fixtures that introduce:

1. Breaking ONS response-schema drift.
2. A filter output that never becomes ready.
3. CSV and CSVW disagreement.
4. Duplicate observations split across Spark partitions.
5. Missing geography dimension members.
6. A changed label for an existing code.
7. Negative or non-numeric observations.
8. Spark partition skew.
9. A dbt join that multiplies rows.
10. A faulty incremental predicate.
11. A historic revision arriving after a newer version.
12. A partial JDBC load followed by retry.

For each fault, document:

- the business risk;
- the earliest useful detection point;
- the responsible control;
- the operator-facing evidence;
- the recovery procedure.

Exit criteria:

- CI proves each fault is caught at the intended architectural boundary.
- A risk-to-test matrix explains why every control exists.

## Phase 9 — operational evidence

1. Add a run command covering ingestion, Spark processing, JDBC load, `dbt build`, and evidence generation.
2. Emit structured logs with source version, filter ID, artifact checksum, Spark application ID, dbt invocation ID, and pipeline run ID.
3. Produce a static Markdown/HTML evidence report containing:
   - source provenance and checksums;
   - Spark input, accepted, quarantined, and output counts;
   - dbt build and test results;
   - layer-by-layer reconciliations;
   - revision comparison;
   - freshness and duration.
4. Upload dbt artifacts, Spark summaries, and the evidence report from CI.
5. Add OpenTelemetry-compatible traces after the run identifiers and stage boundaries are stable.

Exit criteria:

- A failed run is diagnosable from retained evidence without reproducing it.
- A reviewer can understand the architecture and inspect a credible sample run in under ten minutes.

## Phase 10 — portfolio release

1. Add a concise quick start and reviewer walkthrough.
2. Add architecture decision records explaining PySpark, dbt/PostgreSQL, MinIO, and orchestration choices.
3. Publish dbt docs and a sample evidence report.
4. Add a short demonstration recording.
5. Add CodeQL, Dependabot, licence, contribution guidance, and release notes.
6. Tag the first complete release.

Exit criteria:

- The repository demonstrates testing maturity across API, Spark, storage, warehouse, and dbt boundaries.
- Technology choices are justified by failure modes rather than included as a tool checklist.

## Suggested milestones

1. `v0.1-platform` — Docker Compose, Spark, MinIO, PostgreSQL, dbt, and CI.
2. `v0.2-ons-ingestion` — discovery, filter workflow, manifests, and immutable artifacts.
3. `v0.3-spark-processing` — explicit schemas, quarantine, Parquet, and JDBC load.
4. `v0.4-dbt-warehouse` — staging, contracts, dimensions, facts, marts, and reconciliation.
5. `v0.5-revisions` — incremental models, snapshots, backfills, and recovery.
6. `v1.0-quality-lab` — fault laboratory, evidence report, documentation, and demonstration.

## First implementation slice

The first slice should prove the architecture without attempting the full platform:

1. Pin the exact Census 2021 dataset, edition, version, dimensions, and a small geography set.
2. Add Docker Compose services for PostgreSQL and MinIO.
3. Scaffold Python, PySpark, and dbt projects with compatible pinned versions.
4. Store representative ONS API, CSV, and CSVW fixtures.
5. Implement filter-output polling and immutable manifest generation.
6. Implement one PySpark job that converts the fixture into partitioned Parquet.
7. Load it into PostgreSQL and build one contracted dbt staging model plus one population mart.
8. Add reconciliation from source rows to Spark output to dbt mart.
9. Run the complete deterministic slice in GitHub Actions.

This vertical slice validates the component boundaries before adding more datasets, orchestration, or observability.
