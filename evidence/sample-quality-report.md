# Sample pipeline quality evidence

This report is produced by the deterministic fixture pipeline. The source rows are
provider-shaped test data, not published population statistics.

## Run identity

- Pipeline run: `fixture-run`
- Dataset: `TS009`
- Edition: `2021`
- Version: `1`
- Filter hash: `ebc7f88e898584a421133febe90560e3937fe781e3ce70056f09eae92ccc354a`

## Source provenance

- `csv`: `560865db222afa70340f27e3576b3c3150f4e62b06914f753abe0494f6293e1b`
- `csvw`: `aeda38df4400841cd2b1d1b7c2abb31971390c203e1c11296c35182252dc2eef`

## Spark reconciliation

| Measure | Count |
|---|---:|
| Input | 8 |
| Accepted | 8 |
| Quarantined | 0 |

## dbt results

| Measure | Count |
|---|---:|
| Executed | 63 |
| Passed | 63 |
| Failed | 0 |

## Release interpretation

The source artifacts have immutable checksums, Spark input equals accepted plus
quarantined records, warehouse layers reconcile, and no dbt test failed.
