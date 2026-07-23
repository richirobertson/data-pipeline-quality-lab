.PHONY: build up down lint test test-unit test-spark fixture spark spark-load dbt dbt-build evidence pipeline verify

build:
	docker compose build quality

up:
	docker compose up -d postgres minio minio-init

down:
	docker compose down

lint:
	docker compose run --rm quality ruff check src tests
	docker compose run --rm quality ruff format --check src tests

test:
	docker compose run --rm quality pytest

test-unit:
	docker compose run --rm quality pytest -m "not spark and not integration and not live"

test-spark:
	docker compose run --rm quality pytest -m spark

fixture:
	docker compose run --rm quality python -m pipeline_quality.fixture_pipeline

spark:
	docker compose run --rm quality python -m pipeline_quality.spark_job \
		--csv tests/fixtures/ons-population.csv \
		--csvw tests/fixtures/ons-population.csvw \
		--curated data/curated/ons_population \
		--quarantine data/quarantine/ons_population \
		--summary evidence/generated/spark-summary.json

spark-load:
	docker compose run --rm quality python -m pipeline_quality.spark_job \
		--csv tests/fixtures/ons-population.csv \
		--csvw tests/fixtures/ons-population.csvw \
		--curated data/curated/ons_population \
		--quarantine data/quarantine/ons_population \
		--summary evidence/generated/spark-summary.json \
		--jdbc-url jdbc:postgresql://postgres:5432/quality_lab

dbt:
	docker compose run --rm quality sh -c \
		"cd warehouse && dbt seed --profiles-dir . && dbt build --profiles-dir ."

dbt-build:
	docker compose run --rm quality sh -c \
		"cd warehouse && dbt build --exclude resource_type:seed --profiles-dir . && dbt docs generate --profiles-dir ."

evidence:
	docker compose run --rm quality python -m pipeline_quality.evidence

pipeline: fixture spark-load dbt-build evidence

verify: lint test fixture spark dbt evidence
