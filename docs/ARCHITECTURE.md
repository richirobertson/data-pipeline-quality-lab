# Architecture

## System context

The pipeline turns a bounded ONS Census 2021 extract into a tested population mart. Its primary output is not only data; it is evidence connecting each published value to an ONS dataset version and immutable artifact.

## Component boundaries

### ONS client

The client implements only the ONS behaviours the pipeline needs:

- retrieve a dataset version;
- create and submit a filter;
- poll a filter output;
- download completed artifacts.

HTTP is injected through `httpx.Client`, making provider failures deterministic in tests. Retries are intentionally restricted to transport errors, rate limits, and server errors.

### Object storage

Landing objects are content addressed. A logical key includes dataset, edition, version, filter hash, artifact type, and artifact checksum. Writing different bytes to an existing key fails rather than overwriting evidence.

`FileObjectStore` is the deterministic implementation. `S3ObjectStore` uses the same semantics for MinIO and S3-compatible systems.

### Spark

Spark reads exact ONS headers into an explicit internal schema. It:

1. normalises whitespace;
2. separates structurally invalid records;
3. identifies conflicting observations at the dimensional grain;
4. removes exact duplicates deterministically;
5. appends source and run provenance;
6. writes accepted and quarantined Parquet separately.

Business aggregations are not implemented in Spark. Keeping them in dbt prevents two transformation layers from owning competing definitions.

### Warehouse and dbt

PySpark's accepted output lands in PostgreSQL `raw`. dbt then:

- creates a contracted grain-preserving staging model;
- builds geography, age, and sex dimensions;
- builds a population observation fact;
- builds a population-by-geography mart;
- reconciles raw, staging, and fact row counts;
- publishes documentation and an exposure for quality evidence.

### Evidence

The evidence generator combines ingestion manifests, Spark reconciliation counts, and dbt run results into a compact Markdown report. CI retains the detailed machine-readable artifacts as well.

## Data grain

The fact business key is:

```text
dataset + edition + version + geography + age + sex
```

Any two different observations at that grain are a conflict. The pipeline does not guess which one is correct.

## Local versus production concerns

MinIO and PostgreSQL keep local development reproducible. A cloud deployment could replace them with managed object storage and a supported warehouse without changing the quality boundaries. Orchestration is intentionally deferred until stage contracts, rerun semantics, and evidence are stable.

