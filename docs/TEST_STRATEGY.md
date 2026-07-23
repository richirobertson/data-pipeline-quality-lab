# Test strategy

## Quality objective

The test system must answer four questions:

1. Did we retrieve the intended ONS data?
2. Did distributed processing preserve or explicitly reject every source record?
3. Do warehouse models preserve grain and dimensional integrity?
4. Can an operator explain a failed or revised run from retained evidence?

## Test layers

### Python unit and property tests

Fast tests cover canonical hashing, manifests, immutable storage, API retry policy, filter states, error content, and evidence rendering. Hypothesis verifies digest properties across arbitrary bytes.

### Provider contract fixtures

Fixtures reflect the published TS009 headers and ONS filter response shapes. They are repository-owned, deterministic, and intentionally small.

A live provider check is separate from pull-request gating because ONS availability and beta changes are not repository-controlled.

### Spark tests

Spark runs locally with multiple workers. Tests cover:

- exact schema enforcement;
- CSV/CSVW agreement;
- missing dimensions;
- invalid and negative observations;
- exact and conflicting duplicates;
- duplicates spanning partitions;
- result invariance across partition counts.

### dbt tests

dbt provides:

- source tests;
- enforced staging contracts;
- uniqueness and non-null tests;
- dimension relationship tests;
- singular tests for negative measures;
- layer-count reconciliation;
- known fixture totals.

### Platform checks

CI validates the Compose graph and builds the authoritative image. PostgreSQL-backed dbt tests run against a real service rather than an in-memory substitute.

## Failure ownership

| Failure | Owning boundary |
|---|---|
| HTTP protocol, rate limit or filter state | ONS client |
| Artifact mutation or missing provenance | Object store/manifest |
| Technical schema or invalid record | Spark |
| Dimensional relationship or business aggregate | dbt |
| Missing cross-stage evidence | Evidence generator |

## Deliberate exclusions

- Live ONS availability does not block pull requests.
- Coverage percentage is not treated as proof of data correctness.
- Snapshot testing is not used for large payloads.
- Spark and dbt do not duplicate business transformations.

