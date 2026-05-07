"""Classe abstraite BaseJob — point d'entrée commun à tous les jobs Spark."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from datalake_lib.log.audit import AuditLogger

if TYPE_CHECKING:
    from pyspark.sql import SparkSession


class BaseJob(ABC):
    """Classe de base pour tous les jobs du pipeline médaillon.

    Sous-classes attendues :
    - `jobs/bronze/bronze_*.py`  → ingestion landing → Bronze
    - `jobs/silver/silver_*.py`  → Bronze → Silver (typage, DQ, merge)
    - `jobs/gold/gold_*.py`      → Silver → Gold (agrégats KPI)

    Chaque sous-classe doit :
    1. Définir `JOB_NAME`, `DOMAIN`, `TABLE` comme attributs de classe.
    2. Implémenter `run()` qui orchestre le pipeline.
    """

    JOB_NAME: str = ""
    DOMAIN: str = ""
    TABLE: str = ""

    def __init__(self, spark: SparkSession, env: str = "dev") -> None:
        self.spark = spark
        self.env = env
        self.logger = AuditLogger()
        self._apply_spark_conf()

    # ── API publique ──────────────────────────────────────────────────────

    @abstractmethod
    def run(self) -> None:
        """Point d'entrée principal du job."""

    # ── Configuration Spark commune ──────────────────────────────────────

    def _get_spark_conf(self) -> dict[str, str]:
        """Configuration Spark commune à tous les jobs."""
        return {
            "spark.sql.sources.partitionOverwriteMode": "dynamic",
            "spark.sql.adaptive.enabled": "true",
        }

    def _apply_spark_conf(self) -> None:
        """Applique la config Spark commune sur la session active."""
        for key, value in self._get_spark_conf().items():
            self.spark.conf.set(key, value)
