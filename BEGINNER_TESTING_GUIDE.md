# Beginner's guide: what this pipeline proves

This guide is for someone who is new to data engineering or data-pipeline
testing. It explains what the repository does, why each type of test exists,
how to run it, and what confidence a passing result provides.

## The short version

The project takes population-shaped data from the Office for National
Statistics (ONS), checks it at several boundaries, transforms it, loads it into
a PostgreSQL warehouse, and produces totals that an analyst could use.

The deterministic test pipeline uses eight small, repository-owned records
rather than downloading a large live dataset. The records use the real ONS
column names and API response shapes, but their values are test data and must
not be presented as official statistics.

The pipeline proves four main things:

1. The ONS API client handles successful responses and predictable failures.
2. Source files and their provenance cannot be silently changed.
3. Spark accepts valid records, explains rejected records, and behaves the same
   regardless of how the data is partitioned.
4. dbt preserves the intended data grain, builds valid relationships, and
   produces totals that reconcile with the accepted input.

No single test proves the whole system is correct. Confidence comes from
testing each boundary and then running the boundaries together.

## The pipeline in plain English

```text
ONS-shaped API responses and CSV files
                  |
                  v
Python client: download safely and record provenance
                  |
                  v
MinIO landing area: keep immutable source artifacts
                  |
                  v
PySpark: validate, normalize, deduplicate, and quarantine
             |                         |
             v                         v
      accepted Parquet          rejected Parquet
             |
             v
PostgreSQL raw table
             |
             v
dbt: staging -> dimensions and fact -> reporting mart
             |
             v
Quality evidence report
```

### Important terms

- **Pipeline**: a sequence of steps that moves and transforms data.
- **Artifact**: a file produced or consumed by the pipeline, such as CSV,
  CSVW, Parquet, JSON, or a report.
- **Provenance**: evidence of where data came from, including source version,
  checksum, and pipeline run identifier.
- **Checksum**: a fingerprint of file content. If the content changes, its
  SHA-256 checksum changes.
- **Schema**: the expected columns and data types.
- **Contract**: an explicit promise about a schema or interface that causes a
  failure when broken.
- **Grain**: what one row represents. Here, one fact row represents one
  dataset version, geography, age, and sex combination.
- **Dimension**: descriptive data such as geography, age, or sex.
- **Fact**: a measurable event or observation linked to dimensions.
- **Mart**: a table prepared for a particular reporting or analytical use.
- **Quarantine**: a separate output for invalid records, including the reason
  they were rejected.
- **Idempotent**: safe to run repeatedly without creating different or
  duplicated results.
- **Reconciliation**: proving that counts or totals agree across pipeline
  stages.
- **Fixture**: small test data stored in the repository.

## Why the tests are split into layers

A final total can look reasonable even when the pipeline is wrong. For
example, a retry could download the same data twice, a join could multiply
rows, or an invalid record could silently disappear.

The tests therefore fail as close as possible to the problem:

| Problem | Test boundary |
|---|---|
| ONS sends an error or changes its response | Python API-client tests |
| A downloaded file is replaced | Manifest and object-store tests |
| CSV columns or types change | CSVW contract tests |
| A row is invalid or duplicated | Spark transformation tests |
| A fact points to a missing dimension | dbt relationship tests |
| Rows disappear or multiply | dbt reconciliation tests |
| Evidence cannot explain a run | Evidence-report tests |

This makes failures easier to understand and assigns clear ownership.

## Before running anything

You need:

- Docker with Docker Compose
- GNU Make

Build the project image and start PostgreSQL and MinIO:

```bash
make build
make up
```

Stop the local services when finished:

```bash
make down
```

Generated data is placed under `data/`, generated evidence under
`evidence/generated/`, and dbt artifacts under `warehouse/target/`. These
runtime outputs are ignored by Git.

## The main commands

The four learning commands below are guided walkthroughs. Before invoking a
tool, the Make target prints its purpose, the risks it is checking, and how to
interpret the underlying output. A final `OUTCOME: SUCCESS` message is printed
only if the tool exits successfully; Make stops immediately on the first
failure, so a success banner cannot hide an earlier error.

### Run the complete deterministic verification

```bash
make verify
```

This runs linting, all deterministic Python and Spark tests, fixture
generation, the standalone Spark job, the seed-backed dbt build, and evidence
generation.

It proves that each component works with controlled test data. It does not
call the live ONS service.

### Run the real cross-component path

```bash
make pipeline
```

This performs:

1. fixture landing and manifest creation;
2. Spark validation and normalization;
3. Spark JDBC loading into PostgreSQL;
4. dbt models and tests over Spark-loaded data;
5. quality-evidence generation.

This is the strongest local demonstration because data crosses the real
component boundaries. A successful fixture run currently reports:

```text
input_rows: 8
accepted_rows: 8
quarantined_rows: 0
```

The dbt pipeline build should then pass all 63 selected nodes. The exact node
count may increase when models or tests are added; the important result is
zero failed nodes.

### Run only fast Python tests

```bash
make test-unit
```

Use this while changing the API client, manifests, storage adapters, CLI, or
evidence generator.

This fast subset uses `--no-cov` because it intentionally excludes Spark tests
and therefore cannot satisfy a whole-package coverage threshold. `make test`
and CI still enforce the 85% coverage gate over the complete deterministic
suite.

### Run only Spark tests

```bash
make test-spark
```

Spark has higher startup cost, so it is separated for quicker development.
Like `make test-unit`, this focused subset uses `--no-cov`; only the complete
`make test` and CI runs enforce whole-package coverage.

### Run every deterministic pytest test

```bash
make test
```

The live ONS test is collected but skipped unless explicitly enabled.

### Run dbt using its repository seed

```bash
make dbt
```

This loads `warehouse/seeds/raw_ons_observations.csv` and builds the warehouse.
It isolates dbt from Spark, which helps determine whether a failure belongs to
the warehouse logic or an earlier pipeline stage.

### Run and inspect individual stages

```bash
make fixture
make spark
make spark-load
make dbt-build
make evidence
```

- `fixture` creates immutable local input artifacts and a manifest.
- `spark` creates curated and quarantine Parquet without loading PostgreSQL.
- `spark-load` also writes accepted records to PostgreSQL through JDBC.
- `dbt-build` builds and tests dbt over the Spark-loaded raw table.
- `evidence` combines source, Spark, and dbt results into Markdown.

## What every Python test validates

### `tests/test_cli.py`

#### `test_filter_hash_command_is_stable`

Runs the CLI against equivalent filter definitions and checks that it produces
the same hash.

**Risk caught:** JSON formatting or key order changes the identity of a filter
even though the requested data is unchanged.

#### `test_cli_requires_a_command`

Checks that invoking the CLI without an operation fails clearly rather than
doing something surprising.

**Risk caught:** ambiguous or accidental command-line use.

### `tests/test_manifest.py`

#### `test_filter_hash_is_stable_when_json_key_order_changes`

Proves that logically identical JSON creates the same canonical filter hash.

**Risk caught:** unnecessary duplicate ingestion runs for the same request.

#### `test_sha256_is_always_a_lowercase_hex_digest`

Uses Hypothesis to try many automatically generated byte sequences and proves
that every checksum has the expected 64-character lowercase hexadecimal form.

**Risk caught:** malformed checksums that cannot reliably identify content.

This is a **property test**: it tests a rule over many generated examples
rather than checking only one hand-written example.

#### `test_artifact_contains_content_addressed_key`

Checks that an artifact's storage key includes its content identity and source
context.

**Risk caught:** two different files being stored under an ambiguous key.

### `tests/test_object_store.py`

#### `test_file_store_is_idempotent_for_the_same_content`

Writes the same bytes to the same local key twice and expects success.

**Risk caught:** safe retries being treated as conflicts.

#### `test_file_store_rejects_different_content_for_an_existing_key`

Attempts to replace an existing key with different bytes and expects failure.

**Risk caught:** silent mutation of a supposedly immutable source artifact.

#### `test_s3_store_creates_reads_and_reuses_immutable_object`

Exercises the same behavior through the S3-compatible adapter used for MinIO.

**Risk caught:** the local implementation and object-storage implementation
having different immutability behavior.

#### `test_s3_store_rejects_content_conflict`

Checks that the S3-compatible adapter rejects different content at an existing
key.

**Risk caught:** source data being silently overwritten in object storage.

### `tests/test_ons_client.py`

These tests use controlled HTTP responses. They do not depend on ONS being
online.

#### `test_create_and_submit_filter`

Checks that the client creates a filter and submits it using the identifiers
returned by the API.

**Risk caught:** constructing the asynchronous ONS workflow incorrectly.

#### `test_polling_waits_for_csv_and_csvw`

Returns pending states before a ready state and checks that the client waits
until both the CSV data and CSVW metadata are available.

**Risk caught:** treating an incomplete filter output as successful.

#### `test_polling_times_out`

Keeps the output pending and checks that polling stops after a finite limit.

**Risk caught:** a pipeline hanging forever.

#### `test_polling_stops_on_source_error`

Returns an ONS failure state and checks that the client stops with a
descriptive error.

**Risk caught:** recording a failed upstream request as successful.

#### `test_rate_limit_honours_retry_after`

Returns HTTP `429` and checks that the delay specified by `Retry-After` is
used before retrying.

**Risk caught:** ignoring provider rate limits or retrying too aggressively.

#### `test_html_error_page_is_not_treated_as_json`

Returns HTML where JSON is expected and checks that content-type validation
rejects it.

**Risk caught:** confusing a proxy or service error page with valid API data.

#### `test_transport_failure_is_bounded`

Simulates network failures and proves that retries stop at the configured
limit.

**Risk caught:** infinite retry loops during network outages.

#### `test_non_retryable_http_error_is_descriptive`

Returns an HTTP error that should not be retried and checks that useful
diagnostic context is preserved.

**Risk caught:** hiding the true upstream failure behind a generic exception.

#### `test_download_rejects_empty_artifact`

Returns an empty download and expects rejection.

**Risk caught:** passing an incomplete or corrupt source file downstream.

### `tests/test_fixture_pipeline.py`

#### `test_fixture_pipeline_is_repeatable`

Runs fixture ingestion more than once and checks that its artifacts and
manifest remain stable.

**Risk caught:** identical input producing different provenance or duplicate
outputs.

#### `test_fixture_pipeline_command`

Runs the fixture pipeline through its command entry point and confirms that
the expected files are created.

**Risk caught:** unit-tested functions working while the actual executable
command is wired incorrectly.

### `tests/test_spark_contract.py`

CSVW is metadata describing the CSV's columns and types.

#### `test_csvw_fixture_matches_source_contract`

Checks that the repository CSV and CSVW fixtures agree with the expected ONS
provider contract.

**Risk caught:** stale or internally inconsistent fixtures giving false
confidence.

#### `test_csvw_rejects_reordered_columns`

Changes the declared column order and expects contract validation to fail.

**Risk caught:** values being interpreted as the wrong fields after schema
drift.

#### `test_csvw_rejects_non_integer_observation`

Changes the observation type in metadata and expects rejection.

**Risk caught:** measures arriving as an incompatible type.

### `tests/test_spark_transform.py`

#### `test_transform_accepts_fixture_at_declared_grain`

Transforms the valid fixture and checks that all eight rows are accepted at
the intended dataset/version/geography/age/sex grain.

**Risk caught:** valid data being lost or the row meaning changing.

#### `test_invalid_and_duplicate_rows_are_explainable`

Supplies missing values, negative observations, exact duplicates, and
conflicting duplicates. It checks which rows are accepted or quarantined and
that stable rule identifiers explain the decision.

**Risk caught:** invalid data silently disappearing, invalid measures reaching
the warehouse, or arbitrary duplicate selection.

An exact duplicate can be safely reduced to one record. Conflicting records
for the same business key are quarantined because the pipeline cannot know
which value is correct.

#### `test_results_do_not_depend_on_input_partition_count`

Processes the same data with different Spark partition counts and compares the
results.

**Risk caught:** distributed execution order changing the business result.

This matters because Spark may divide production data differently depending
on volume and cluster resources.

### `tests/test_evidence.py`

#### `test_dbt_summary_classifies_results`

Checks that successful and failed dbt nodes are counted correctly.

**Risk caught:** an evidence report incorrectly presenting failures as passes.

#### `test_report_connects_source_spark_and_dbt_evidence`

Builds a report from source checksums, Spark counts, and dbt results and checks
that the stages are connected in one readable output.

**Risk caught:** a report that cannot trace a warehouse result back to its
source and processing run.

#### `test_load_json_handles_missing_and_non_object_files`

Checks that missing or wrongly shaped evidence files are handled
predictably.

**Risk caught:** obscure crashes while gathering diagnostics for an already
failed pipeline.

#### `test_evidence_command_writes_report`

Runs the evidence generator through its command entry point and checks that a
report is written.

**Risk caught:** tested rendering logic that is not correctly connected to the
operator command.

### `tests/test_ons_live.py`

#### `test_ts009_version_contract_is_still_usable`

Calls the real ONS API and checks that dataset `TS009`, edition `2021`,
version `1`, its required dimensions, and CSV/CSVW downloads are still
available.

**Risk caught:** the external beta API changing after repository fixtures were
recorded.

It is intentionally excluded from normal pull-request tests because external
availability is outside the repository's control. It runs on a schedule and
can be invoked manually:

```bash
docker compose run --rm \
  -e RUN_LIVE_ONS=1 \
  quality pytest tests/test_ons_live.py
```

A live failure is a signal to investigate the provider contract. It does not
automatically mean application code is defective.

## What the dbt tests validate

dbt tests are SQL queries. A test passes when it returns no invalid rows.

Run the isolated, seed-backed dbt suite with:

```bash
make dbt
```

### Raw-source tests

`warehouse/models/sources.yml` checks that geography, age, sex, observation,
and source-record hash are not null. It also checks that each source-record
hash is unique.

**What this proves:** Spark supplied complete dimensional keys and did not load
the same accepted source record more than once.

### Staging contract and tests

`warehouse/models/staging/schema.yml` enforces the exact names and data types
of the public staging model. All columns must be non-null, and the
source-record hash must be unique.

**What this proves:** dbt receives the schema it expects, preserves provenance,
and does not alter the source grain.

If a column is renamed or changes from an integer to text unexpectedly, the
contract stops the build before downstream models use it.

### Dimension tests

`warehouse/models/dimensions/schema.yml` tests the geography, age, and sex
dimensions:

- surrogate keys must be present and unique;
- source codes must be present and unique;
- human-readable labels must be present.

**What this proves:** every code has one usable dimension member and warehouse
joins have stable targets.

### Fact tests

`warehouse/models/facts/schema.yml` checks:

- the observation key is present and unique;
- every geography key exists in `dim_geography`;
- every age key exists in `dim_age`;
- every sex key exists in `dim_sex`;
- the observation is present;
- the source-record hash is present and unique.

**What this proves:** the fact table has one row per declared business grain,
has no orphaned dimension references, and remains traceable to accepted input.

The `relationships` tests are data engineering's equivalent of referential
integrity checks: they prevent a fact from pointing to a dimension member that
does not exist.

### Mart tests

`warehouse/models/marts/schema.yml` checks that the reporting mart has complete
geography, dataset-version, and population fields.

**What this proves:** published reporting rows contain the minimum context
needed to interpret each total.

### Singular SQL tests

These tests express project-specific rules that are clearer as SQL.

#### `assert_no_negative_observations.sql`

Returns fact rows with observations below zero.

**What this proves:** an invalid negative population measure cannot be
published even if it bypasses an earlier control.

#### `assert_layer_counts_reconcile.sql`

Compares row counts across raw, staging, and fact layers and fails if they
differ.

**What this proves:** rows were not silently lost or multiplied during dbt
transformations.

#### `assert_fixture_totals.sql`

Checks the exact expected totals for the two fixture geographies and rejects
unexpected geographies.

**What this proves:** the final aggregation produces a known correct answer,
not merely a plausible shape.

This test is appropriate for the controlled fixture. Production totals would
normally reconcile against authoritative source totals rather than
hard-coded sample values.

## Linting and coverage

Run style and static checks:

```bash
make lint
```

Linting does not prove data correctness. It catches inconsistent formatting
and suspicious Python patterns so reviews can focus on behavior.

The pytest suite enforces at least 85% branch-aware coverage. Coverage answers
"which code executed during tests?" It does not answer "were the assertions
correct?" High coverage without meaningful risk-based assertions can still
provide weak confidence.

The Spark command entry point is excluded from coverage because its behavior
is exercised through transformation tests and end-to-end commands; counting
the argument-wiring module would distort the useful coverage signal.

## Continuous integration

`.github/workflows/quality.yml` separates the checks into jobs:

- **python-and-spark** installs Python and Java, checks formatting, and runs
  pytest with coverage;
- **dbt** starts a real PostgreSQL service, loads the deterministic seed, runs
  dbt models and tests, generates documentation, and uploads evidence;
- **docker** validates the Compose configuration and builds the project image.

`.github/workflows/live-ons-contract.yml` runs the external ONS contract check
on a schedule or by manual request.

Separating jobs makes ownership clearer. A dbt failure does not hide a passing
Python suite, and an external ONS outage does not block an otherwise safe pull
request.

## How to read a failure

Start with the earliest failing boundary:

1. **API-client failure:** inspect status, content type, retry, or filter state.
2. **Manifest/storage failure:** compare keys and SHA-256 checksums.
3. **Spark contract failure:** compare CSV headers with CSVW metadata.
4. **Spark quarantine increase:** inspect `quality_rule` and the rejected row.
5. **dbt source/staging failure:** check JDBC-loaded schema and null keys.
6. **dbt relationship failure:** find the fact key missing from its dimension.
7. **dbt reconciliation failure:** compare raw, staging, and fact row counts.
8. **fixture-total failure:** inspect joins and aggregation grain.
9. **evidence failure:** inspect the retained Spark summary and dbt
   `run_results.json`.

Do not immediately change an expected value to make a test pass. First decide
whether the source legitimately changed, the implementation is defective, or
the test's assumption is obsolete.

## What a green build does and does not prove

A green build gives evidence that:

- known ONS response shapes are handled;
- retries and failure states are bounded;
- artifacts are immutable and traceable;
- known schema changes are rejected;
- invalid and conflicting records are explainable;
- Spark results are partition-invariant for the tested cases;
- warehouse keys and relationships are valid;
- controlled rows and totals reconcile end to end.

It does not prove that:

- ONS will always be available;
- every possible future schema change is handled;
- the eight fixture rows represent the full production-data distribution;
- the pipeline will meet an unspecified production performance target;
- the analytical definition of every future measure is correct;
- high test coverage alone guarantees quality.

Those limitations are deliberate. Mature testing states what the evidence
supports and where uncertainty remains.

## A useful learning path

If you are exploring the repository for the first time:

1. Run `make test-unit` and read one API-client test.
2. Compare `ons-population.csv` with `ons-population.csvw`.
3. Run `make test-spark` and inspect the quarantine assertions.
4. Run `make dbt` and look at the generated dbt test names.
5. Run `make pipeline`.
6. Open `evidence/generated/quality-report.md`.
7. Change a fixture value or schema in a temporary branch and observe which
   boundary fails first.

That last exercise is especially valuable: a test suite is easiest to
understand when you see the defect each test prevents.
