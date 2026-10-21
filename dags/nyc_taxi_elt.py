"""DAG luong A (FILE): tai file -> bronze (Iceberg) -> silver + quarantine (dbt) -> test -> doi soat.
Luong B (Postgres) nam o DAG rieng: nyc_taxi_elt_pg.py

Trigger thu cong, nhap tham so `month` (dang YYYY-MM). Chay lai cung thang an toan (idempotent).
"""
from datetime import timedelta

from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG, Param

# Python va dbt cua project nam trong venv rieng (xem Dockerfile)
PY = "/opt/venv/bin/python"


# Chi chay cac model cua luong A (luong B co model *_pg rieng, khong duoc chay o day)
SELECT_A = "flagged_yellow_trips silver_yellow_trips quarantine_yellow_trips"


def dbt_cmd(cmd: str) -> str:
    """Lenh dbt (run/test) cho thang trong params.month, chi cac model luong A."""
    return (
        f"cd /opt/project && /opt/venv/bin/dbt {cmd} --select {SELECT_A} "
        "--project-dir dbt_project --profiles-dir dbt_project "
        """--vars '{"batch_month": "{{ params.month }}"}'"""
    )


with DAG(
    dag_id="nyc_taxi_elt",
    description="NYC Yellow Taxi: download -> bronze -> silver/quarantine -> test -> reconcile",
    schedule=None,  # chi chay khi trigger thu cong
    catchup=False,
    max_active_runs=1,  # Spark + Iceberg local: chi 1 lan chay cung luc
    params={
        "month": Param(
            "2024-06",
            type="string",
            pattern=r"^\d{4}-(0[1-9]|1[0-2])$",
            description="Thang can xu ly, dang YYYY-MM",
        )
    },
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["nyc_taxi", "elt"],
) as dag:

    download = BashOperator(
        task_id="download",
        bash_command=f"{PY} -m nyc_elt.download {{{{ params.month }}}}",
    )

    load_bronze = BashOperator(
        task_id="load_bronze",
        bash_command=f"{PY} -m nyc_elt.load_bronze {{{{ params.month }}}}",
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
        bash_command=f"{PY} -m nyc_elt.reconcile {{{{ params.month }}}}",
    )

    download >> load_bronze >> dbt_run >> dbt_test >> reconcile