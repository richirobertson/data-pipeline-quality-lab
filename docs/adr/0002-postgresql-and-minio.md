# ADR 0002: PostgreSQL and MinIO for the local platform

Status: accepted

PostgreSQL provides a realistic SQL warehouse target for dbt without requiring a paid account. MinIO exercises S3-compatible immutable object-storage behaviour locally.

The interfaces are intentionally portable: managed object storage or a cloud warehouse can replace either component without moving quality ownership between stages.

