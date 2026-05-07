"""DAG Airflow — orchestration du job silver_credit_encours.

Exécuté quotidiennement à 06:00 UTC. Le job Spark est soumis sur le
cluster YARN via `spark-submit` (le code Python est packagé en zip
disponible sous `/opt/datalake/`).
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-team",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
    "email": ["data-alerts@banque.ma"],
}


with DAG(
    dag_id="silver_credit_encours",
    default_args=default_args,
    schedule_interval="0 6 * * *",  # tous les jours à 06:00
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["silver", "credit"],
) as dag:

    run_silver = BashOperator(
        task_id="run_silver_credit_encours",
        bash_command=(
            "spark-submit "
            "--master yarn "
            "--deploy-mode cluster "
            "--py-files /opt/datalake/datalake_lib.zip "
            "/opt/datalake/jobs/silver/silver_credit_encours.py "
            "--date-arrete {{ ds }} "
            "--env prod"
        ),
    )
