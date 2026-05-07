"""Job Silver : credit_encours — pipeline complet Bronze → Silver.

Sert de **template** pour tous les autres jobs Silver. Connecte les 5
modules de la lib :
- `read.BronzeReader`     : extraction Bronze
- `quality.SparkDQRunner` : règles DQ déclaratives
- `write.SilverWriter`    : écriture Silver
- `log.AuditLogger`       : audit JSON
- `log.MetricsCollector`  : métriques de run

Exécution :
    python jobs/silver/silver_credit_encours.py \\
        --date-arrete 2025-01-01 --env dev
"""

from __future__ import annotations

import argparse
import logging
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

import yaml

from datalake_lib.log import MetricsCollector
from datalake_lib.quality import (
    DataQualityError,
    DQConfig,
    DQRule,
    SparkDQRunner,
)
from datalake_lib.read import BronzeReader, JobConfig, ReadOptions
from datalake_lib.write import SilverWriter, WriteOptions

from jobs.base_job import BaseJob

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession

    from datalake_lib.write import WriteResult

logger = logging.getLogger(__name__)


# Chemin par défaut de la config YAML — relatif à la racine du repo.
_DEFAULT_CONFIG_PATH = (
    Path(__file__).resolve().parents[2]
    / "configs"
    / "silver"
    / "credit"
    / "credit_encours.yaml"
)


class SilverCreditEncours(BaseJob):
    """Pipeline Bronze → Silver pour la table `credit_encours`.

    Étapes :
    1. `_extract`  : lecture `bronze_credit_credit_encours` filtrée sur
                     la `date_arrete`.
    2. `_transform`: cast types, calcul `DUREE_RESTANTE_JOURS`, filtre
                     `MONTANT_RESTANT_DU < 0`, standardisation `CODE_STATUT`,
                     application des règles DQ.
    3. `_load`     : écriture `silver_credit_credit_encours` (mode full).
    """

    JOB_NAME = "silver_credit_encours"
    DOMAIN = "credit"
    TABLE = "credit_encours"

    # Seuil au-dessus duquel le job échoue (lignes bloquantes / total).
    DEFAULT_BLOCKING_THRESHOLD = 0.10

    def __init__(
        self,
        spark: SparkSession,
        date_arrete: date,
        env: str = "dev",
        config_path: Path | None = None,
    ) -> None:
        super().__init__(spark, env)
        self.date_arrete = date_arrete
        self.config_path = config_path or _DEFAULT_CONFIG_PATH

        self._yaml = self._load_yaml()
        self.config = self._build_job_config()
        self.dq_threshold = self._yaml.get("dq", {}).get(
            "blocking_threshold", self.DEFAULT_BLOCKING_THRESHOLD
        )
        self.business_rules = self._yaml.get("business_rules", {})

        self.reader = BronzeReader(spark, self.config)
        self.writer = SilverWriter(spark, self.config)
        self.dq_runner = SparkDQRunner(spark)

    # ── Pipeline principal ────────────────────────────────────────────────

    def run(self) -> None:
        """Orchestration end-to-end avec audit."""
        log = self.logger.start(
            job_name=self.JOB_NAME,
            table_name=f"silver_{self.DOMAIN}_{self.TABLE}",
            layer="silver",
            domain=self.DOMAIN,
        )
        collector = MetricsCollector()
        collector.add_partition("date_arrete", self.date_arrete.isoformat())

        try:
            df_raw = self._extract()
            collector.set_records_read(df_raw.count())

            df_clean = self._transform(df_raw)
            collector.set_records_written(df_clean.count())

            result = self._load(df_clean)
            collector.set_output_table(result.table)

            metrics = collector.build()
            self.logger.success(log, metrics)
            logger.info(
                "Job %s terminé : %d lignes écrites dans %s",
                self.JOB_NAME,
                result.rows_written,
                result.table,
            )
        except Exception as exc:
            metrics = collector.build()
            self.logger.fail(log, error=str(exc), metrics=metrics)
            raise

    # ── Étapes ────────────────────────────────────────────────────────────

    def _extract(self) -> DataFrame:
        """Lit la table Bronze filtrée sur `date_arrete`."""
        logger.info(
            "Extract : bronze_%s_%s WHERE date_arrete=%s",
            self.DOMAIN,
            self.TABLE,
            self.date_arrete,
        )
        return self.reader.read(ReadOptions(filter_date=self.date_arrete))

    def _transform(self, df: DataFrame) -> DataFrame:
        """Applique les transformations métier + filtres + DQ."""
        from pyspark.sql import functions as F
        from pyspark.sql.types import DateType, DoubleType

        date_format = self.business_rules.get("date_format", "dd/MM/yyyy")

        # 1. Casts numériques.
        for col in ("MONTANT_ACCORDE", "MONTANT_RESTANT_DU", "TAUX_INTERET"):
            if col in df.columns:
                df = df.withColumn(col, F.col(col).cast(DoubleType()))

        # 2. Casts dates (depuis chaîne format dd/MM/yyyy).
        for col in ("DATE_DEBUT", "DATE_ECHEANCE"):
            if col in df.columns:
                df = df.withColumn(
                    col,
                    F.to_date(F.col(col).cast("string"), date_format).cast(DateType()),
                )

        # 3. Calcul DUREE_RESTANTE_JOURS.
        if "DATE_ECHEANCE" in df.columns:
            df = df.withColumn(
                "DUREE_RESTANTE_JOURS",
                F.when(
                    F.col("DATE_ECHEANCE").isNull(), None
                ).otherwise(
                    F.datediff(F.col("DATE_ECHEANCE"), F.current_date())
                ),
            )

        # 4. Standardisation CODE_STATUT.
        if "CODE_STATUT" in df.columns:
            allowed = self.business_rules.get(
                "allowed_statuts", ["ACTIF", "SOLDE", "CONTENTIEUX", "ECHU"]
            )
            default = self.business_rules.get("default_statut", "INCONNU")
            df = df.withColumn(
                "CODE_STATUT",
                F.upper(F.trim(F.col("CODE_STATUT"))),
            ).withColumn(
                "CODE_STATUT",
                F.when(F.col("CODE_STATUT").isin(allowed), F.col("CODE_STATUT"))
                .otherwise(F.lit(default)),
            )

        # 5. Filtrage MONTANT_RESTANT_DU < 0 (rejets).
        df, n_rejected = self._reject_invalid_rows(df)

        # 6. DQ déclarative + seuil bloquant.
        df = self._apply_quality_checks(df)

        return df

    def _load(self, df: DataFrame) -> WriteResult:
        """Écrit la table Silver en mode full."""
        return self.writer.write(df, WriteOptions(mode="full", coalesce=1))

    # ── Helpers métier ────────────────────────────────────────────────────

    def _reject_invalid_rows(
        self,
        df: DataFrame,
    ) -> tuple[DataFrame, int]:
        """Rejette les lignes `MONTANT_RESTANT_DU < 0`.

        Les rejets sont écrits dans `bronze_credit_encours_rejects` (POC :
        nom de table simple, pas de DB Hive séparée).

        Returns:
            (df_valid, nb_rejets)
        """
        from pyspark.sql import functions as F

        if "MONTANT_RESTANT_DU" not in df.columns:
            return df, 0

        invalid_cond = F.col("MONTANT_RESTANT_DU") < 0
        df_invalid = df.filter(invalid_cond)
        n_rejected = df_invalid.count()

        if n_rejected > 0:
            logger.warning(
                "%d ligne(s) rejetée(s) : MONTANT_RESTANT_DU < 0", n_rejected
            )
            try:
                (
                    df_invalid.withColumn(
                        "_reject_reason", F.lit("MONTANT_RESTANT_DU < 0")
                    )
                    .write.mode("append")
                    .saveAsTable("bronze_credit_encours_rejects")
                )
            except Exception as exc:
                # En POC local, l'écriture peut échouer sans warehouse.
                logger.warning(
                    "Impossible d'écrire la table de rejets : %s", exc
                )

        df_valid = df.filter(~invalid_cond | F.col("MONTANT_RESTANT_DU").isNull())
        return df_valid, n_rejected

    def _apply_quality_checks(self, df: DataFrame) -> DataFrame:
        """Applique les règles DQ et lève DataQualityError au-delà du seuil."""
        dq_config = self._build_dq_config(df)
        df_valid, df_rejected, report = self.dq_runner.run(df, dq_config)

        total = report.results[0].records_checked if report.results else df.count()
        blocking_failed = sum(
            r.records_failed
            for r in report.results
            if r.severity == "blocking"
        )
        rate = (blocking_failed / total) if total > 0 else 0.0

        if rate > self.dq_threshold:
            raise DataQualityError(
                f"Taux d'erreur bloquant : {rate * 100:.1f}% > "
                f"seuil {self.dq_threshold * 100:.0f}%",
                failure_rate=rate,
                threshold=self.dq_threshold,
            )
        if rate > 0:
            logger.warning(
                "Taux d'erreur bloquant : %.2f%% (sous le seuil %.0f%%)",
                rate * 100,
                self.dq_threshold * 100,
            )

        return df_valid

    # ── Config helpers ────────────────────────────────────────────────────

    def _load_yaml(self) -> dict:
        """Charge le YAML de config du job (clés runtime + business rules)."""
        if not self.config_path.exists():
            raise FileNotFoundError(
                f"Config introuvable : '{self.config_path}'"
            )
        return yaml.safe_load(self.config_path.read_text(encoding="utf-8"))

    def _build_job_config(self) -> JobConfig:
        """Construit le JobConfig pour BronzeReader / SilverWriter."""
        return JobConfig(
            domain=self._yaml.get("domain", self.DOMAIN),
            table=self._yaml.get("table", self.TABLE),
            layer=self._yaml.get("layer", "silver"),
            source_system=self._yaml.get("source_system", ""),
        )

    @staticmethod
    def _build_dq_config(df: DataFrame) -> DQConfig:
        """Règles DQ minimales appliquées dans Silver (template).

        Ces règles complètent les filtres métier explicites — elles sont
        définies en code ici pour la POC. En prod, les règles vivent dans
        `dq_configs/credit_encours_dq.yaml` et sont chargées via
        `parse_dq_config`.
        """
        rules = [
            DQRule(
                rule_id="DQ_NUM_CONTRAT_NOT_NULL",
                rule_type="not_null",
                columns=["NUM_CONTRAT"],
                severity="blocking",
                description="NUM_CONTRAT obligatoire",
            ),
            DQRule(
                rule_id="DQ_NUM_CONTRAT_UNIQUE",
                rule_type="unique",
                columns=["NUM_CONTRAT"],
                severity="blocking",
                description="NUM_CONTRAT unique",
            ),
            DQRule(
                rule_id="DQ_MONTANT_ACCORDE_RANGE",
                rule_type="range",
                column="MONTANT_ACCORDE",
                min_value=0,
                severity="warning",
                description="MONTANT_ACCORDE >= 0",
            ),
        ]
        # On filtre les règles dont les colonnes sont absentes du df pour
        # éviter une DQExecutionError sur un schéma incomplet en test.
        df_columns = set(df.columns)

        def _rule_applicable(rule: DQRule) -> bool:
            cols = rule.columns or ([rule.column] if rule.column else [])
            return all(c in df_columns for c in cols if c)

        return DQConfig(
            table_name="silver_credit_credit_encours",
            domain="credit",
            primary_key=["NUM_CONTRAT"],
            rules=[r for r in rules if _rule_applicable(r)] or rules,
        )


# ── Entry point CLI ───────────────────────────────────────────────────────


def main() -> None:
    """Entry point pour `spark-submit`."""
    parser = argparse.ArgumentParser(description="Job Silver credit_encours")
    parser.add_argument("--date-arrete", required=True, help="Format YYYY-MM-DD")
    parser.add_argument("--env", default="dev", choices=["dev", "preprod", "prod"])
    args = parser.parse_args()

    from pyspark.sql import SparkSession

    spark = (
        SparkSession.builder
        .appName("silver_credit_encours")
        .getOrCreate()
    )

    job = SilverCreditEncours(
        spark=spark,
        date_arrete=date.fromisoformat(args.date_arrete),
        env=args.env,
    )
    job.run()


if __name__ == "__main__":
    main()
