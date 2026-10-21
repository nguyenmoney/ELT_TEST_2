"""DAG luong B (POSTGRES): seed Postgres -> JDBC -> bronze_pg (Iceberg) -> silver_pg + quarantine_pg (dbt) -> test -> doi soat.

Trigger thu cong, nhap tham so `month` (dang YYYY-MM). Chay lai cung thang an toan (idempotent).
Task `seed_postgres` chi de GIA LAP nguon: ngoai doi thuc Postgres da co san du lieu.
"""
from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

# Python va dbt cua project nam trong venv rieng (xem Dockerfile)
PY = "/opt/venv/bin/python"

# Chi chay cac model cua luong B (luong A co model rieng, khong duoc chay o day)
SELECT_B = "flagged_yellow_trips_pg silver_yellow_trips_pg quarantine_yellow_trips_pg"


def dbt_cmd(cmd: str) -> str:
    """Lenh dbt (run/test) cho thang trong params.month, chi cac model luong B."""
    return (
        f"cd /opt/project && /opt/venv/bin/dbt {cmd} --select {SELECT_B} "
        "--project-dir dbt_project --profiles-dir dbt_project "
        """--vars '{"batch_month": "{{ params.month }}"}'"""
    )


with DAG(
    dag_id="nyc_taxi_elt_pg",
    description="NYC Yellow Taxi (Postgres): seed -> extract JDBC -> silver_pg/quarantine_pg -> test -> reconcile",
    schedule=None,  # chi chay khi trigger thu cong
    catchup=False,
    max_active_runs=1,  # Spark + Iceberg local: chi 1 lan chay cung luc
    params={
        "month": Param(
            "2026-01",
            type="string",
            pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
            description="Thang can xu ly, dang YYYY-MM",
        )
    },
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["nyc_taxi", "elt", "postgres"],
) as dag:

    seed_postgres = BashOperator(
        task_id="seed_postgres",
        bash_command=f"{PY} -m nyc_elt.seed_postgres {{{{ params.month }}}}",
    )

    extract_postgres = BashOperator(
        task_id="extract_postgres",
        bash_command=f"{PY} -m nyc_elt.extract_postgres {{{{ params.month }}}}",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=dbt_cmd("run"),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=dbt_cmd("test"),
    )

    reconcile = BashOperator(
        task_id="reconcile",
        bash_command=f"{PY} -m nyc_elt.reconcile {{{{ params.month }}}} pg",
    )

    seed_postgres >> extract_postgres >> dbt_run >> dbt_test >> reconcile