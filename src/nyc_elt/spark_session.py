"""Tao SparkSession co Iceberg (Hadoop catalog ten 'lake').

Dung chung cho cac buoc dung Spark (load_bronze, ...).
"""
import os
import sys
from pathlib import Path

import requests
from pyspark.sql import SparkSession

ICEBERG_JAR_NAME = "iceberg-spark-runtime-3.5_2.12-1.5.2.jar"
ICEBERG_JAR_URL = (
    "https://repo1.maven.org/maven2/org/apache/iceberg/"
    f"iceberg-spark-runtime-3.5_2.12/1.5.2/{ICEBERG_JAR_NAME}"
)

JARS_DIR = Path(os.getenv("JARS_DIR", "./jars")).resolve()
WAREHOUSE_DIR = Path(os.getenv("WAREHOUSE_DIR", "./warehouse")).resolve()


def ensure_iceberg_jar() -> Path:
    """Tra ve duong dan jar Iceberg; tu tai ve neu chua co."""
    jar = JARS_DIR / ICEBERG_JAR_NAME
    if jar.exists():
        return jar

    JARS_DIR.mkdir(parents=True, exist_ok=True)
    tmp = jar.with_suffix(".part")
    print(f"Dang tai {ICEBERG_JAR_URL}")
    try:
        with requests.get(ICEBERG_JAR_URL, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(tmp, "wb") as f:
                for chunk in r.iter_content(chunk_size=1024 * 1024):
                    f.write(chunk)
        tmp.replace(jar)
    finally:
        tmp.unlink(missing_ok=True)
    return jar


def build_spark(app_name: str) -> SparkSession:
    jar = ensure_iceberg_jar()
    WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

    # Dung dung Python cua moi truong hien tai cho worker (quan trong tren Windows)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)

    return (
        SparkSession.builder.appName(app_name)
        .master("local[2]")
        .config("spark.jars", jar.as_uri())
        .config("spark.driver.memory", os.getenv("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.sql.session.timeZone", "UTC")
        .config(
            "spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions",
        )
        .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.lake.type", "hadoop")
        .config("spark.sql.catalog.lake.warehouse", WAREHOUSE_DIR.as_uri())
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )