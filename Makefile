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
	@printf '\nUNIT TEST WALKTHROUGH\n'
	@printf 'Purpose: test the fast Python boundaries without Java, Spark, infrastructure, or the live ONS API.\n'
	@printf 'Validating:\n'
	@printf '  - ONS API retries, rate limits, polling states, and error handling\n'
	@printf '  - stable filter hashes, checksums, manifests, and immutable storage\n'
	@printf '  - fixture repeatability, CLI behavior, CSVW contracts, and evidence reporting\n'
	@printf 'Output: pytest lists every test by name; PASSED means its stated risk was controlled.\n'
	@printf 'Coverage: intentionally disabled for this subset; make test and CI enforce whole-package coverage.\n\n'
	docker compose run --rm quality pytest -vv --tb=short --no-cov -m "not spark and not integration and not live"
	@printf '\nUNIT TEST OUTCOME: SUCCESS\n'
	@printf 'All selected fast tests passed. No Spark, infrastructure, or live-provider behavior was exercised.\n\n'

# Exercise distributed transformation behavior in isolation.
test-spark:
	@printf '\nSPARK TEST WALKTHROUGH\n'
	@printf 'Purpose: prove that distributed processing makes deterministic and explainable data-quality decisions.\n'
	@printf 'Validating:\n'
	@printf '  - valid ONS-shaped rows retain the declared geography/age/sex grain\n'
	@printf '  - invalid, negative, exact-duplicate, and conflicting rows receive stable quarantine reasons\n'
	@printf '  - changing Spark partition counts does not change accepted business results\n'
	@printf 'Output: pytest lists each Spark scenario; PASSED means its assertions held.\n'
	@printf 'Coverage: intentionally disabled for this subset; make test and CI enforce whole-package coverage.\n\n'
	docker compose run --rm quality pytest -vv --tb=short --no-cov -m spark
	@printf '\nSPARK TEST OUTCOME: SUCCESS\n'
	@printf 'All Spark scenarios passed. Accepted and quarantined behavior was deterministic for the tested data.\n\n'

# Materialize provider-shaped fixtures and immutable provenance.
fixture:
	@printf '\nPIPELINE STAGE 1/4 - LAND IMMUTABLE SOURCE FIXTURES\n'
	@printf 'Creating provider-shaped CSV/CSVW artifacts, checksums, content-addressed keys, and a run manifest.\n'
	docker compose run --rm quality python -m pipeline_quality.fixture_pipeline
	@printf 'STAGE 1 OUTCOME: source artifacts and provenance manifest created successfully.\n\n'

# Transform to Parquet without crossing the JDBC boundary.
spark:
	@printf '\nSPARK TRANSFORMATION - FILE OUTPUT ONLY\n'
	@printf 'Checking the CSV/CSVW contract, normalizing rows, and writing accepted and quarantine Parquet.\n'
	docker compose run --rm quality python -m pipeline_quality.spark_job \
		--csv tests/fixtures/ons-population.csv \
		--csvw tests/fixtures/ons-population.csvw \
		--curated data/curated/ons_population \
		--quarantine data/quarantine/ons_population \
		--summary evidence/generated/spark-summary.json
	@printf 'SPARK OUTCOME: review the printed input, accepted, and quarantined counts above.\n\n'

# Transform to Parquet and load accepted records into PostgreSQL.
spark-load:
	@printf '\nPIPELINE STAGE 2/4 - VALIDATE, QUARANTINE, AND LOAD WITH SPARK\n'
	@printf 'Checking the source contract and row rules, then writing Parquet and loading accepted rows to PostgreSQL.\n'
	@printf 'Reconciliation rule: input rows must equal accepted rows plus quarantined rows.\n'
	docker compose run --rm quality python -m pipeline_quality.spark_job \
		--csv tests/fixtures/ons-population.csv \
		--csvw tests/fixtures/ons-population.csvw \
		--curated data/curated/ons_population \
		--quarantine data/quarantine/ons_population \
		--summary evidence/generated/spark-summary.json \
		--jdbc-url jdbc:postgresql://postgres:5432/quality_lab
	@printf 'STAGE 2 OUTCOME: Spark completed and printed reconciliation counts above; accepted rows were loaded by JDBC.\n\n'

# Seed raw data so dbt can be developed independently of upstream stages.
dbt:
	@printf '\nDBT WAREHOUSE TEST WALKTHROUGH\n'
	@printf 'Purpose: test warehouse logic independently using the deterministic raw seed.\n'
	@printf 'Validating:\n'
	@printf '  - staging column names, types, non-null fields, uniqueness, and grain\n'
	@printf '  - dimension keys and fact-to-dimension relationships\n'
	@printf '  - no negative measures, no row loss/multiplication, and exact fixture totals\n'
	@printf 'How to read output: each START line names a model or test; PASS/OK is healthy, ERROR identifies the boundary.\n\n'
	docker compose run --rm quality sh -c \
		"cd warehouse && dbt seed --profiles-dir . && dbt build --profiles-dir ."
	@printf '\nDBT OUTCOME: SUCCESS\n'
	@printf 'The seed loaded and every selected warehouse model, contract, relationship, and data test passed.\n\n'

# Build over an already loaded raw table; do not replace Spark data with the seed.
dbt-build:
	@printf '\nPIPELINE STAGE 3/4 - BUILD AND TEST THE DBT WAREHOUSE\n'
	@printf 'Using Spark-loaded rows (not the dbt seed) to build staging, dimensions, fact, mart, and audit layers.\n'
	@printf 'dbt will print every model and test name so failures can be traced to a specific warehouse rule.\n'
	docker compose run --rm quality sh -c \
		"cd warehouse && dbt build --exclude resource_type:seed --profiles-dir . && dbt docs generate --profiles-dir ."
	@printf 'STAGE 3 OUTCOME: all dbt nodes passed and the lineage/documentation catalog was generated.\n\n'

# Combine retained stage artifacts into reviewer-friendly Markdown.
evidence:
	@printf '\nPIPELINE STAGE 4/4 - GENERATE QUALITY EVIDENCE\n'
	@printf 'Combining source checksums, Spark reconciliation counts, and dbt results into one Markdown report.\n'
	docker compose run --rm quality python -m pipeline_quality.evidence
	@printf 'STAGE 4 OUTCOME: evidence/generated/quality-report.md was created successfully.\n\n'

# Exercise the true cross-component route from landing through evidence.
pipeline:
	@printf '\nEND-TO-END DATA PIPELINE WALKTHROUGH\n'
	@printf 'This run crosses the real local boundaries: files -> Spark -> PostgreSQL -> dbt -> evidence.\n'
	@printf 'It uses eight controlled ONS-shaped records and does not call the live ONS API.\n'
	@printf 'The run stops immediately if any stage fails.\n\n'
	@$(MAKE) --no-print-directory fixture
	@$(MAKE) --no-print-directory spark-load
	@$(MAKE) --no-print-directory dbt-build
	@$(MAKE) --no-print-directory evidence
	@printf '\nPIPELINE OUTCOME: SUCCESS\n'
	@printf 'All four stages completed. Source identity, Spark decisions, warehouse integrity, and retained evidence agree.\n'
	@printf 'Next: open evidence/generated/quality-report.md and warehouse/target/index.html.\n\n'

# Broad component verification using deterministic, repository-owned inputs.
verify: lint test fixture spark dbt evidence
