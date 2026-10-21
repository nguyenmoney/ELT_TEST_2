"""Luong B - buoc L (Load): Spark doc Postgres nguon qua JDBC, ghi vao bronze Iceberg.

Ket qua nam o bang rieng lake.bronze.yellow_trips_pg, cung schema voi bang cua luong A
(tai su dung conform() cua load_bronze) nen cac buoc phia sau giong het.

Chay (trong Docker):
  docker compose exec airflow /opt/venv/bin/python -m nyc_elt.extract_postgres 2026-01
"""
import sys

from pyspark.sql import functions as F

from nyc_elt.load_bronze import conform, create_table_if_missing, validate_month
from nyc_elt.pg_source import SOURCE_TABLE, jdbc_options
from nyc_elt.spark_session import build_spark

BRONZE_TABLE_PG = "lake.bronze.yellow_trips_pg"


def extract_postgres(month: str) -> int:
    month = validate_month(month)

    spark = build_spark("extract_postgres")
    try:
        reader = (
            spark.read.format("jdbc")
            .options(**jdbc_options())
            .option("dbtable", SOURCE_TABLE)
            .option("fetchsize", 10000)
            # Doc song song: chia theo pulocationid (so nguyen 1..265) thanh 2 phan
            # (khop master local[2]). Neu khong khai bao, Spark chi dung 1 ket noi.
            .option("partitionColumn", "pulocationid")
            .option("lowerBound", 1)
            .option("upperBound", 265)
            .option("numPartitions", 2)
        )
        # filter nay duoc day xuong Postgres (WHERE source_month = ...), khong keo het ve Spark
        src = reader.load().where(F.col("source_month") == month).drop("source_month")

        df = conform(src, month, f"postgres:{SOURCE_TABLE}")
        rows_in_pg = df.count()
        if rows_in_pg == 0:
            raise RuntimeError(
                f"Postgres khong co dong nao cho thang {month} "
                f"(bang {SOURCE_TABLE} dang chua thang khac? hay chay seed_postgres)"
            )
        print(f"Postgres co {rows_in_pg:,} dong cho thang {month}")

        create_table_if_missing(spark, BRONZE_TABLE_PG)
        # Ghi de dung phan vung cua thang nay -> chay lai khong bi trung
        df.writeTo(BRONZE_TABLE_PG).overwritePartitions()

        rows_bronze = (
            spark.table(BRONZE_TABLE_PG).where(F.col("source_month") == month).count()
        )
    finally:
        spark.stop()

    if rows_bronze != rows_in_pg:
        raise RuntimeError(
            f"LECH: Postgres {rows_in_pg:,} dong, bronze {rows_bronze:,} dong"
        )
    print(f"Xong: {BRONZE_TABLE_PG} co {rows_bronze:,} dong cho thang {month}, khop voi Postgres")
    return rows_bronze


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Dung: python -m nyc_elt.extract_postgres YYYY-MM")
    extract_postgres(sys.argv[1])