"""Doi soat sau khi chay dbt: bronze = silver + quarantine cho 1 thang.

Neu lech thi bao loi (exit code khac 0) de Airflow danh dau task that bai.

Hai luong:
  file (mac dinh) : bronze.yellow_trips    -> silver_yellow_trips    + quarantine_yellow_trips
  pg              : bronze.yellow_trips_pg -> silver_yellow_trips_pg + quarantine_yellow_trips_pg

Chay:  uv run python -m nyc_elt.reconcile 2024-06          (luong file)
       uv run python -m nyc_elt.reconcile 2026-01 pg       (luong Postgres)
"""
import re
import sys

from pyspark.sql import SparkSession, functions as F

from nyc_elt.spark_session import build_spark

# luong -> (bronze, silver, quarantine)
FLOWS = {
    "file": (
        "lake.bronze.yellow_trips",
        "lake.silver.silver_yellow_trips",
        "lake.silver.quarantine_yellow_trips",
    ),
    "pg": (
        "lake.bronze.yellow_trips_pg",
        "lake.silver.silver_yellow_trips_pg",
        "lake.silver.quarantine_yellow_trips_pg",
    ),
}


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


def reconcile(month: str, flow: str = "file") -> dict:
    month = validate_month(month)
    if flow not in FLOWS:
        raise ValueError(f"flow phai la mot trong {list(FLOWS)}, nhan duoc: {flow!r}")
    bronze_table, silver_table, quarantine_table = FLOWS[flow]
    spark = build_spark("reconcile")
    try:
        counts = {
            "bronze": count_month(spark, bronze_table, month),
            "silver": count_month(spark, silver_table, month),
            "quarantine": count_month(spark, quarantine_table, month),
        }
    finally:
        spark.stop()

    print(
        f"[{flow}] Thang {month}: bronze={counts['bronze']:,} | "
        f"silver={counts['silver']:,} | quarantine={counts['quarantine']:,}"
    )
    check(**counts)
    print("KHOP: bronze = silver + quarantine")
    return counts


if __name__ == "__main__":
    if len(sys.argv) not in (2, 3):
        sys.exit("Dung: python -m nyc_elt.reconcile YYYY-MM [file|pg]")
    reconcile(sys.argv[1], sys.argv[2] if len(sys.argv) == 3 else "file")