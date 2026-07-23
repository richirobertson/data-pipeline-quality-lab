FROM python:3.14-slim-bookworm

# Pin the database driver independently from PySpark so upgrades are deliberate.
ARG SPARK_POSTGRES_JDBC_VERSION=42.7.5

# These settings make container logs immediate, avoid bytecode noise in mounted
# source folders, and point dbt at the repository-owned profile.
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python3 \
    DBT_PROFILES_DIR=/workspace/warehouse

# Java runs Spark's JVM; procps is used by Spark's startup scripts.
RUN apt-get update \
    && apt-get install --yes --no-install-recommends curl git openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY requirements-dev.txt ./

# Installing dependencies before source code preserves this expensive Docker
# layer when application files change but dependency pins do not.
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir --requirement requirements-dev.txt \
    && SPARK_JARS="$(python -c "import pathlib, pyspark; print(pathlib.Path(pyspark.__file__).parent / 'jars')")" \
    && curl --fail --location --silent --show-error \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${SPARK_POSTGRES_JDBC_VERSION}/postgresql-${SPARK_POSTGRES_JDBC_VERSION}.jar" \
      --output "${SPARK_JARS}/postgresql-${SPARK_POSTGRES_JDBC_VERSION}.jar"

COPY pyproject.toml README.md ./
COPY src ./src
# Editable installation keeps imports consistent when Compose mounts local code.
RUN pip install --no-cache-dir --no-deps --editable .

COPY . .

# A bare container run performs the safest default operation: deterministic tests.
CMD ["pytest"]
