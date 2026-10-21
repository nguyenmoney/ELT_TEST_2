"""Doi soat sau khi chay dbt: bronze = silver + quarantine cho 1 thang.

Neu lech thi bao loi (exit code khac 0) de Airflow danh dau task that bai.

Chay:  uv run python -m nyc_elt.reconcile 2024-06
"""
import re
import sys

from pyspark.sql import SparkSession, functions as F

from nyc_elt.spark_session import build_spark

BRONZE_TABLE = "lake.bronze.yellow_trips"
SILVER_TABLE = "lake.silver.silver_yellow_trips"
QUARANTINE_TABLE = "lake.silver.quarantine_yellow_trips"


def validate_month(month: str) -> str:
    """Chi nhan dang YYYY-MM (vd 2024-06)."""
    if not re.fullmatch(r"\d{4}-(0[1-9]|1[0-2])", month):
        raise ValueError(f"month phai co dang YYYY-MM, nhan duoc: {month!r}")
    return month


def count_month(spark: SparkSession, table: str, month: str) -> int:
    return spark.table(table).where(F.col("source_month") == month).count()


def check(bronze: int, silver: int, quarantine: int) -> None:
    """Nem loi neu so dong khong khop hoac bronze rong."""
    if bronze == 0:
        raise RuntimeError("Bronze khong co dong nao cho thang nay")
    if bronze != silver + quarantine:
        raise RuntimeError(
            f"LECH: bronze={bronze:,} nhung silver+quarantine={silver + quarantine:,} "
            f"(silver={silver:,}, quarantine={quarantine:,}, chenh={bronze - silver - quarantine:,})"
        )


def reconcile(month: str) -> dict:
    month = validate_month(month)
    spark = build_spark("reconcile")
    try:
        counts = {
            "bronze": count_month(spark, BRONZE_TABLE, month),
            "silver": count_month(spark, SILVER_TABLE, month),
            "quarantine": count_month(spark, QUARANTINE_TABLE, month),
        }
    finally:
        spark.stop()

    print(
        f"Thang {month}: bronze={counts['bronze']:,} | "
        f"silver={counts['silver']:,} | quarantine={counts['quarantine']:,}"
    )
    check(**counts)
    print("KHOP: bronze = silver + quarantine")
    return counts


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Dung: python -m nyc_elt.reconcile YYYY-MM")
    reconcile(sys.argv[1])