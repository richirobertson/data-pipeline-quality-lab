.PHONY: build up down lint test test-unit test-spark fixture spark spark-load dbt dbt-build evidence pipeline verify

# Build the shared Python/Java/Spark/dbt runtime.
build:
	docker compose build quality

# Start only the stateful dependencies; commands use short-lived quality containers.
up:
	docker compose up -d postgres minio minio-init

# Stop services while retaining named volumes for the next run.
down:
	docker compose down

# Static checks are separate from behavioral tests for clearer failures.
lint:
	docker compose run --rm quality ruff check src tests
	docker compose run --rm quality ruff format --check src tests

# Run every deterministic pytest test; the live ONS test remains opt-in.
test:
	docker compose run --rm quality pytest

# Fast feedback loop without Spark, integrations, or external services.
test-unit:
	docker compose run --rm quality pytest -m "not spark and not integration and not live"

# Exercise distributed transformation behavior in isolation.
test-spark:
	docker compose run --rm quality pytest -m spark

# Materialize provider-shaped fixtures and immutable provenance.
fixture:
	docker compose run --rm quality python -m pipeline_quality.fixture_pipeline

# Transform to Parquet without crossing the JDBC boundary.
spark:
	docker compose run --rm quality python -m pipeline_quality.spark_job \
		--csv tests/fixtures/ons-population.csv \
		--csvw tests/fixtures/ons-population.csvw \
		--curated data/curated/ons_population \
		--quarantine data/quarantine/ons_population \
		--summary evidence/generated/spark-summary.json

# Transform to Parquet and load accepted records into PostgreSQL.
spark-load:
	docker compose run --rm quality python -m pipeline_quality.spark_job \
		--csv tests/fixtures/ons-population.csv \
		--csvw tests/fixtures/ons-population.csvw \
		--curated data/curated/ons_population \
		--quarantine data/quarantine/ons_population \
		--summary evidence/generated/spark-summary.json \
		--jdbc-url jdbc:postgresql://postgres:5432/quality_lab

# Seed raw data so dbt can be developed independently of upstream stages.
dbt:
	docker compose run --rm quality sh -c \
		"cd warehouse && dbt seed --profiles-dir . && dbt build --profiles-dir ."

# Build over an already loaded raw table; do not replace Spark data with the seed.
dbt-build:
	docker compose run --rm quality sh -c \
		"cd warehouse && dbt build --exclude resource_type:seed --profiles-dir . && dbt docs generate --profiles-dir ."

# Combine retained stage artifacts into reviewer-friendly Markdown.
evidence:
	docker compose run --rm quality python -m pipeline_quality.evidence

# Exercise the true cross-component route from landing through evidence.
pipeline: fixture spark-load dbt-build evidence

# Broad component verification using deterministic, repository-owned inputs.
verify: lint test fixture spark dbt evidence
