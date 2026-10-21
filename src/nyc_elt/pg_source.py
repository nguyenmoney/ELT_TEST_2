"""Cau hinh ket noi toi Postgres NGUON (luong B), dung chung cho seed va extract.

Trong Docker, cac bien PG_SOURCE_* duoc docker-compose dat san.
Chay tay tren may: mac dinh tro toi localhost:5433 (cong ma compose mo ra).
"""
import os

# Bang nguon trong Postgres (Spark JDBC tu tao bang nay khi seed)
SOURCE_TABLE = "public.yellow_trips"


def jdbc_options() -> dict:
    host = os.getenv("PG_SOURCE_HOST", "localhost")
    port = os.getenv("PG_SOURCE_PORT", "5433")
    db = os.getenv("PG_SOURCE_DB", "source_db")
    return {
        "url": f"jdbc:postgresql://{host}:{port}/{db}",
        "user": os.getenv("PG_SOURCE_USER", "source"),
        "password": os.getenv("PG_SOURCE_PASSWORD", "source"),
        "driver": "org.postgresql.Driver",
    }