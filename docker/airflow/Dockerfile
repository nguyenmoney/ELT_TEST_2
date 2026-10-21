# Image Airflow + Java + Spark/Iceberg + dbt.
# Build tu THU MUC GOC project:  docker build -f docker/airflow/Dockerfile -t nyc-elt-airflow .
ARG AIRFLOW_VERSION=3.3.0
FROM apache/airflow:${AIRFLOW_VERSION}-python3.12

# ---- 1. Java (Spark can) + uv ----
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless curl \
    && rm -rf /var/lib/apt/lists/* \
    && ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java
ENV JAVA_HOME=/opt/java

COPY --from=ghcr.io/astral-sh/uv:0.11.7 /uv /usr/local/bin/uv

# ---- 2. Jar Iceberg (phai khop pyspark 3.5.1) ----
ARG ICEBERG_JAR=iceberg-spark-runtime-3.5_2.12-1.5.2.jar
RUN mkdir -p /opt/jars \
    && curl -fsSL -o /opt/jars/${ICEBERG_JAR} \
       https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/${ICEBERG_JAR}

# ---- 2b. Jar JDBC Postgres (Spark doc Postgres nguon o luong B) ----
ARG PG_JAR=postgresql-42.7.3.jar
RUN curl -fsSL -o /opt/jars/${PG_JAR} \
       https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.3/${PG_JAR}

# ---- 3. Thu muc lam viec ----
RUN mkdir -p /opt/venv /opt/project /opt/airflow/data /opt/airflow/warehouse \
    && chown -R airflow /opt/venv /opt/project /opt/airflow/data /opt/airflow/warehouse

# ---- 4. Thu vien project (pyspark, dbt-spark...) trong venv rieng ----
# Tach rieng khoi Python cua Airflow de tranh xung dot phien ban.
USER airflow
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON_DOWNLOADS=never \
    UV_LINK_MODE=copy
WORKDIR /opt/project
COPY --chown=airflow pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project --python /usr/local/bin/python

# ---- 5. Code ----
COPY --chown=airflow src ./src
COPY --chown=airflow dbt_project ./dbt_project

# Duong dan cho code Python va cau hinh Spark/dbt (khop spark_session.py va profiles.yml)
ENV PYTHONPATH=/opt/project/src \
    JARS_DIR=/opt/jars \
    ICEBERG_JAR=/opt/jars/${ICEBERG_JAR} \
    PG_JAR=/opt/jars/${PG_JAR} \
    DATA_DIR=/opt/airflow/data \
    WAREHOUSE_DIR=/opt/airflow/warehouse
# Luu y: KHONG them /opt/venv/bin vao PATH (se de len python cua Airflow).
# Goi truc tiep: /opt/venv/bin/python -m nyc_elt.download ... va /opt/venv/bin/dbt ...