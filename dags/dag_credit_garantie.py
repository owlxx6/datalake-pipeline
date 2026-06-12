"""DAG Crédit-Garantie — orchestration du pipeline médaillon par arrêté.

Chaîne : vérification Landing → Bronze/Silver (13 tables, moteur DQ + historisation)
→ Gold (modèle déclaratif : dims, faits, OBT, vues KPI) → recette (sanity check).

Déclenchement :
  - manuel (UI/CLI) avec la date d'arrêté en conf : {"date_arrete": "2026-05-31"}
  - sinon, par défaut : dernier jour du mois précédent la date d'exécution.

Les runs sont idempotents (snapshot overwritePartitions + SCD2 par hash) :
rejouer un arrêté ne crée ni doublon ni version parasite.

Exécution Spark en mode local sur la VM (POC) — passer --master yarn en cible.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

PROJECT = "/media/sf_datalake-pipeline"
CONFIG = f"{PROJECT}/configs/bronze/credit_garantie.yaml"
GOLD_CONFIG = f"{PROJECT}/configs/gold/credit_garantie.yaml"
# Le scheduler tourne dans airflow-venv (sans pydantic/yaml) : on force le python
# SYSTÈME pour le driver PySpark — la tâche devient indépendante du venv Airflow.
SPARK_SUBMIT = (
    "export PYSPARK_PYTHON=/usr/bin/python3 PYSPARK_DRIVER_PYTHON=/usr/bin/python3 && "
    "spark-submit --driver-memory 4g"
)

# Les 13 tables alimentées en Landing (les autres entrées du YAML n'ont pas de donnée)
TABLES = [
    "TIERS", "IMAGE_CONTRAT", "GRC_PORTEFEUILLE", "TYPOLOGIE", "ASAL", "POPS", "ORG",
    "CREDIT_AMORT", "IMPAYES_CDL", "DEPASSEMENT",
    "GARANTIE_DETAIL", "GARANTIE_CONTRAT", "IDCE_AUTORISATION",
]

default_args = {
    "owner": "data-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="credit_garantie_medaillon",
    description="Pipeline médaillon Crédit-Garantie : Bronze/Silver -> Gold -> recette",
    default_args=default_args,
    schedule=None,                 # POC : déclenchement manuel par arrêté
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_tasks=1,            # VM 9,5 Go : un driver Spark (4g) à la fois
    tags=["credit_garantie", "medaillon", "bronze", "silver", "gold"],
    params={"date_arrete": "2026-05-31"},
) as dag:

    verifier_landing = BashOperator(
        task_id="verifier_landing",
        bash_command=(
            f"hdfs dfs -ls /data/landing/pops/date_arrete="
            f"{{{{ (dag_run.conf.get('date_arrete') or params.date_arrete) | replace('-', '') }}}}"
        ),
    )

    bronze_silver_tasks = []
    for table in TABLES:
        bronze_silver_tasks.append(BashOperator(
            task_id=f"bronze_silver_{table.lower()}",
            bash_command=(
                f"cd {PROJECT} && {SPARK_SUBMIT} jobs/bronze/engine.py "
                f"--config {CONFIG} "
                f"--date-arrete {{{{ dag_run.conf.get('date_arrete') or params.date_arrete }}}} "
                f"--table {table}"
            ),
        ))

    gold = BashOperator(
        task_id="gold_modele_declaratif",
        bash_command=(
            f"cd {PROJECT} && {SPARK_SUBMIT} jobs/gold/engine.py "
            f"--config {GOLD_CONFIG} "
            f"--date-arrete {{{{ dag_run.conf.get('date_arrete') or params.date_arrete }}}}"
        ),
    )

    recette = BashOperator(
        task_id="recette_sanity_check",
        bash_command=(
            f"cd {PROJECT} && {SPARK_SUBMIT} scripts/check_lake_sanity.py "
            f"2>&1 | tee /tmp/sanity_dag.log | grep -E '\\[OK|\\[KO|VERDICT' ; "
            f"grep -q 'TOUT EST PROPRE' /tmp/sanity_dag.log"
        ),
    )

    # Chaîne : landing → tables en séquence (RAM) → gold → recette
    verifier_landing >> bronze_silver_tasks[0]
    for prev, nxt in zip(bronze_silver_tasks, bronze_silver_tasks[1:]):
        prev >> nxt
    bronze_silver_tasks[-1] >> gold >> recette
