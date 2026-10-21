"""Buoc L (Load): Spark doc parquet va ghi vao bang bronze Iceberg.

Schema bronze co dinh: moi thang deu duoc ep ve cung 1 schema
(cot thieu -> NULL, cot la khong co trong schema -> bo qua va canh bao).

Chay:  uv run python -m nyc_elt.load_bronze 2024-06
"""
import os
import re
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession, functions as F

from nyc_elt.spark_session import build_spark

DATA_DIR = Path(os.getenv("DATA_DIR", "./data"))
BRONZE_TABLE = "lake.bronze.yellow_trips"

# (ten cot viet thuong, kieu Spark SQL)
BRONZE_COLUMNS = [
    ("vendorid", "int"),
    ("tpep_pickup_datetime", "timestamp"),
    ("tpep_dropoff_datetime", "timestamp"),
    ("passenger_count", "bigint"),
    ("trip_distance", "double"),
    ("ratecodeid", "bigint"),
    ("store_and_fwd_flag", "string"),
    ("pulocationid", "int"),
    ("dolocationid", "int"),
    ("payment_type", "bigint"),
    ("fare_amount", "double"),
    ("extra", "double"),
    ("mta_tax", "double"),
    ("tip_amount", "double"),
    ("tolls_amount", "double"),
    ("improvement_surcharge", "double"),
    ("total_amount", "double"),
    ("congestion_surcharge", "double"),
    ("airport_fee", "double"),
    ("cbd_congestion_fee", "double"),
]
META_COLUMNS = [
    ("source_month", "string"),
    ("_source_file", "string"),
    ("_ingested_at", "timestamp"),
]


def validate_month(month: str) -> str:
    """Chi nhan dang YYYY-MM (vd 2024-06)."""
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month):
        raise ValueError(f"month phai co dang YYYY-MM, nhan duoc: {month!r}")
    return month


def conform(df: DataFrame, month: str, source_file: str) -> DataFrame:
    """Ep DataFrame ve schema bronze co dinh va them cot metadata."""
    df = df.toDF(*[c.lower() for c in df.columns])

    known = {name for name, _ in BRONZE_COLUMNS}
    extra = [c for c in df.columns if c not in known]
    if extra:
        print(f"CANH BAO: bo qua cot khong co trong schema bronze: {extra}")

    cols = [
        (F.col(name).cast(typ) if name in df.columns else F.lit(None).cast(typ)).alias(name)
        for name, typ in BRONZE_COLUMNS
    ]
    return df.select(*cols).select(
        "*",
        F.lit(month).alias("source_month"),
        F.lit(source_file).alias("_source_file"),
        F.current_timestamp().alias("_ingested_at"),
    )


def create_table_if_missing(spark: SparkSession, table: str = BRONZE_TABLE) -> None:
    col_defs = ", ".join(f"{n} {t}" for n, t in BRONZE_COLUMNS + META_COLUMNS)
    spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.bronze")
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} ({col_defs}) "
        "USING iceberg PARTITIONED BY (source_month)"
    )


def load_bronze(month: str) -> int:
    month = validate_month(month)
    path = (DATA_DIR / "raw" / f"yellow_tripdata_{month}.parquet").resolve()
    if not path.exists():
        raise FileNotFoundError(f"Chua co file {path}, hay chay download truoc")

    spark = build_spark("load_bronze")
    try:
        df = conform(spark.read.parquet(path.as_uri()), month, path.name)
        rows_in_file = df.count()
        if rows_in_file == 0:
            raise RuntimeError(f"File {path.name} khong co dong nao")
        print(f"File {path.name} co {rows_in_file:,} dong")

        create_table_if_missing(spark)
        # Ghi de dung phan vung cua thang nay -> chay lai khong bi trung
        df.writeTo(BRONZE_TABLE).overwritePartitions()

        rows_bronze = (
            spark.table(BRONZE_TABLE).where(F.col("source_month") == month).count()
        )
    finally:
        spark.stop()

    if rows_bronze != rows_in_file:
        raise RuntimeError(
            f"LECH: file {rows_in_file:,} dong, bronze {rows_bronze:,} dong"
        )
    print(f"Xong: bronze co {rows_bronze:,} dong cho thang {month}, khop voi file")
    return rows_bronze


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Dung: python -m nyc_elt.load_bronze YYYY-MM")
    load_bronze(sys.argv[1])