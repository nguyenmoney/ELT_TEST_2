"""Dua du lieu 1 thang vao Postgres NGUON (chuan bi cho luong B).

Day chi la buoc DUNG NGUON GIA LAP: ngoai doi thuc, Postgres da co san du lieu.
Doc file parquet cua thang do, ghi vao bang public.yellow_trips bang Spark JDBC.
Moi lan chay GHI DE toan bo bang (bang chi chua 1 thang), nen chay lai khong bi trung.

Chay (trong Docker):
  docker compose exec airflow /opt/venv/bin/python -m nyc_elt.seed_postgres 2026-01
"""
import sys

from pyspark.sql import functions as F

from nyc_elt.download import download, validate_month
from nyc_elt.pg_source import SOURCE_TABLE, jdbc_options
from nyc_elt.spark_session import build_spark


def seed(month: str) -> int:
    month = validate_month(month)
    path = download(month)  # da co file thi bo qua, chua co thi tai

    spark = build_spark("seed_postgres")
    try:
        df = spark.read.parquet(path.resolve().as_uri())
        # Postgres + Spark JDBC bao ten cot trong dau ngoac kep -> phai viet thuong tu dau
        df = df.toDF(*[c.lower() for c in df.columns])
        df = df.withColumn("source_month", F.lit(month))

        rows_in_file = df.count()
        print(f"File {path.name} co {rows_in_file:,} dong, dang ghi vao Postgres...")

        (
            df.write.format("jdbc")
            .options(**jdbc_options())
            .option("dbtable", SOURCE_TABLE)
            .option("batchsize", 10000)
            .mode("overwrite")
            .option("truncate", "true")  # giu nguyen bang, chi xoa du lieu cu
            .save()
        )

        rows_pg = (
            spark.read.format("jdbc")
            .options(**jdbc_options())
            .option("dbtable", SOURCE_TABLE)
            .load()
            .count()
        )
    finally:
        spark.stop()

    if rows_pg != rows_in_file:
        raise RuntimeError(f"LECH: file {rows_in_file:,} dong, Postgres {rows_pg:,} dong")
    print(f"Xong: {SOURCE_TABLE} co {rows_pg:,} dong cho thang {month}, khop voi file")
    return rows_pg


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Dung: python -m nyc_elt.seed_postgres YYYY-MM")
    seed(sys.argv[1])