FROM python:3.11-slim-bookworm

ARG SPARK_POSTGRES_JDBC_VERSION=42.7.5

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYSPARK_PYTHON=python3 \
    DBT_PROFILES_DIR=/workspace/warehouse

RUN apt-get update \
    && apt-get install --yes --no-install-recommends curl git openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

COPY requirements-dev.txt ./

RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir --requirement requirements-dev.txt \
    && SPARK_JARS="$(python -c "import pathlib, pyspark; print(pathlib.Path(pyspark.__file__).parent / 'jars')")" \
    && curl --fail --location --silent --show-error \
      "https://repo1.maven.org/maven2/org/postgresql/postgresql/${SPARK_POSTGRES_JDBC_VERSION}/postgresql-${SPARK_POSTGRES_JDBC_VERSION}.jar" \
      --output "${SPARK_JARS}/postgresql-${SPARK_POSTGRES_JDBC_VERSION}.jar"

COPY pyproject.toml README.md ./
COPY src ./src
RUN pip install --no-cache-dir --no-deps --editable .

COPY . .

CMD ["pytest"]
