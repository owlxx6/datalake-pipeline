"""Moteur d'exécution PySpark des règles DQ.

Ce module utilise PySpark — il n'est PAS exécuté en local.
L'exécution réelle a lieu sur la VM (Spark 3.x sur YARN).
"""

from __future__ import annotations

import logging
from datetime import datetime
from functools import reduce
from typing import TYPE_CHECKING

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

from ..exceptions import DQExecutionError
from ..models.rule_models import (
    DQConfig,
    DQReport,
    DQResult,
    DQRule,
    OverallStatus,
    RuleStatus,
)

if TYPE_CHECKING:
    from collections.abc import Callable

logger = logging.getLogger(__name__)


# Colonnes de métadonnées ajoutées aux DataFrames de rejets.
META_RULE_ID = "_dq_rule_id"
META_REASON = "_dq_reason"


class SparkDQRunner:
    """Exécute toutes les règles d'une DQConfig sur un DataFrame Spark.

    Le résultat est un triplet :
    - `df_valid`    : lignes qui passent toutes les règles bloquantes
    - `df_rejected` : lignes rejetées (avec colonnes `_dq_rule_id`, `_dq_reason`)
    - `report`      : DQReport avec un DQResult par règle
    """

    def __init__(self, spark: SparkSession) -> None:
        self.spark = spark

    # ── API publique ──────────────────────────────────────────────────────

    def run(
        self,
        df: DataFrame,
        config: DQConfig,
    ) -> tuple[DataFrame, DataFrame, DQReport]:
        """Exécute toutes les règles de `config` sur `df`.

        Args:
            df: DataFrame d'entrée à valider.
            config: DQConfig (règles + PK optionnelle).

        Returns:
            (df_valid, df_rejected, report)
        """
        total_records = df.count()
        results: list[DQResult] = []
        blocking_rejects: list[DataFrame] = []

        dispatch: dict[str, Callable[[DataFrame, DQRule], DataFrame]] = {
            "not_null": self._check_not_null,
            "unique": self._check_unique,
            "referential": self._check_referential,
            "regex": self._check_regex,
            "range": self._check_range,
            "in_set": self._check_in_set,
            "custom_sql": self._check_custom_sql,
        }

        for rule in config.rules:
            checker = dispatch.get(rule.rule_type)
            if checker is None:
                raise DQExecutionError(
                    f"rule_type inconnu : '{rule.rule_type}'",
                    rule_id=rule.rule_id,
                )

            try:
                df_rejects_rule = checker(df, rule)
            except Exception as exc:
                raise DQExecutionError(
                    f"Échec de l'exécution de la règle '{rule.rule_id}' "
                    f"({rule.rule_type}) : {exc}",
                    rule_id=rule.rule_id,
                ) from exc

            records_failed = df_rejects_rule.count()
            failure_rate = (
                records_failed / total_records if total_records > 0 else 0.0
            )
            status: RuleStatus = self._compute_status(records_failed, rule)

            results.append(
                DQResult(
                    rule_id=rule.rule_id,
                    rule_type=rule.rule_type,
                    status=status,
                    records_checked=total_records,
                    records_failed=records_failed,
                    failure_rate=failure_rate,
                    severity=rule.severity,
                    details=rule.description,
                )
            )

            # Seules les règles bloquantes alimentent df_rejected.
            if rule.severity == "blocking" and records_failed > 0:
                blocking_rejects.append(df_rejects_rule)

        df_rejected = self._union_rejects(blocking_rejects, df.schema)
        df_valid = self._compute_valid(df, df_rejected, config.primary_key)

        report = self._build_report(config.table_name, results)

        return df_valid, df_rejected, report

    # ── Méthodes de checks (une par rule_type) ────────────────────────────

    def _check_not_null(self, df: DataFrame, rule: DQRule) -> DataFrame:
        """Rejette les lignes où une colonne `columns` est nulle."""
        cond = reduce(
            lambda a, b: a | b,
            (F.col(c).isNull() for c in rule.columns),
        )
        rejects = df.filter(cond)
        reason = f"Champ(s) null : {','.join(rule.columns)}"
        return self._tag(rejects, rule, reason)

    def _check_unique(self, df: DataFrame, rule: DQRule) -> DataFrame:
        """Rejette les lignes en doublon sur les colonnes `columns`."""
        dupes = (
            df.groupBy(*rule.columns)
            .count()
            .filter(F.col("count") > 1)
            .drop("count")
        )
        rejects = df.join(dupes, on=rule.columns, how="inner")
        reason = f"Doublon sur : {','.join(rule.columns)}"
        return self._tag(rejects, rule, reason)

    def _check_referential(self, df: DataFrame, rule: DQRule) -> DataFrame:
        """Rejette les lignes dont `column` n'existe pas dans la table de référence."""
        ref_df = self.spark.read.table(rule.reference_table).select(
            F.col(rule.reference_column).alias("__ref_key")
        ).distinct()

        rejects = (
            df.join(
                ref_df,
                df[rule.column] == ref_df["__ref_key"],
                "left_anti",
            )
        )
        reason = (
            f"FK '{rule.column}' absente de "
            f"{rule.reference_table}.{rule.reference_column}"
        )
        return self._tag(rejects, rule, reason)

    def _check_regex(self, df: DataFrame, rule: DQRule) -> DataFrame:
        """Rejette les lignes où `column` ne matche pas `pattern`."""
        rejects = df.filter(
            F.col(rule.column).isNotNull()
            & ~F.col(rule.column).rlike(rule.pattern)
        )
        reason = f"Format invalide '{rule.column}' (pattern={rule.pattern})"
        return self._tag(rejects, rule, reason)

    def _check_range(self, df: DataFrame, rule: DQRule) -> DataFrame:
        """Rejette les lignes hors bornes [min_value, max_value]."""
        col = F.col(rule.column)
        conditions = []
        if rule.min_value is not None:
            conditions.append(col < F.lit(rule.min_value))
        if rule.max_value is not None:
            conditions.append(col > F.lit(rule.max_value))

        cond = reduce(lambda a, b: a | b, conditions)
        rejects = df.filter(col.isNotNull() & cond)

        bounds = []
        if rule.min_value is not None:
            bounds.append(f"min={rule.min_value}")
        if rule.max_value is not None:
            bounds.append(f"max={rule.max_value}")
        reason = f"Hors bornes '{rule.column}' ({', '.join(bounds)})"
        return self._tag(rejects, rule, reason)

    def _check_in_set(self, df: DataFrame, rule: DQRule) -> DataFrame:
        """Rejette les lignes dont une des `columns` est hors de `allowed_values`."""
        cond = reduce(
            lambda a, b: a | b,
            (
                F.col(c).isNotNull() & ~F.col(c).isin(rule.allowed_values)
                for c in rule.columns
            ),
        )
        rejects = df.filter(cond)
        cols_str = ",".join(rule.columns)
        reason = f"Valeur hors typologie sur '{cols_str}'"
        return self._tag(rejects, rule, reason)

    def _check_custom_sql(self, df: DataFrame, rule: DQRule) -> DataFrame:
        """Exécute un SQL custom — l'expression doit retourner les rejets.

        L'expression peut faire référence à la vue temporaire `__dq_input`
        qui pointe sur le DataFrame d'entrée.
        """
        df.createOrReplaceTempView("__dq_input")
        rejects = self.spark.sql(rule.sql_expr)
        reason = f"Échec custom_sql ({rule.rule_id})"
        return self._tag(rejects, rule, reason)

    # ── Helpers internes ──────────────────────────────────────────────────

    @staticmethod
    def _tag(df_rejects: DataFrame, rule: DQRule, reason: str) -> DataFrame:
        """Ajoute les colonnes de métadonnées DQ aux rejets."""
        return df_rejects.withColumn(
            META_RULE_ID, F.lit(rule.rule_id).cast(StringType())
        ).withColumn(META_REASON, F.lit(reason).cast(StringType()))

    @staticmethod
    def _compute_status(records_failed: int, rule: DQRule) -> RuleStatus:
        """Calcule le statut d'une règle après exécution."""
        if records_failed == 0:
            return "passed"
        # Une règle warning qui détecte des rejets reste en status 'warning'.
        if rule.severity == "warning":
            return "warning"
        return "failed"

    def _union_rejects(
        self,
        rejects_list: list[DataFrame],
        original_schema,
    ) -> DataFrame:
        """Union de tous les DataFrames de rejets bloquants."""
        if not rejects_list:
            # DataFrame vide avec le schéma attendu (colonnes d'origine + meta).
            empty_schema = (
                original_schema.add(META_RULE_ID, StringType(), True)
                .add(META_REASON, StringType(), True)
            )
            return self.spark.createDataFrame([], empty_schema)

        return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), rejects_list)

    @staticmethod
    def _compute_valid(
        df: DataFrame,
        df_rejected: DataFrame,
        primary_key: list[str],
    ) -> DataFrame:
        """Calcule le DataFrame valide = df - df_rejected.

        Si une PK est définie : LEFT ANTI JOIN sur la PK (rapide et propre).
        Sinon : `exceptAll` sur la ligne complète (fallback plus lent).
        """
        if df_rejected.rdd.isEmpty():
            return df

        if primary_key:
            rejected_keys = df_rejected.select(*primary_key).distinct()
            return df.join(rejected_keys, on=primary_key, how="left_anti")

        # Fallback : soustraction sans PK.
        return df.exceptAll(df_rejected.drop(META_RULE_ID, META_REASON))

    @staticmethod
    def _build_report(table_name: str, results: list[DQResult]) -> DQReport:
        """Construit le rapport global à partir des résultats par règle."""
        passed = sum(1 for r in results if r.status == "passed")
        failed = sum(1 for r in results if r.status == "failed")
        warnings = sum(1 for r in results if r.status == "warning")

        has_blocking_failure = any(
            r.status == "failed" and r.severity == "blocking" for r in results
        )
        overall: OverallStatus = "failed" if has_blocking_failure else "passed"

        return DQReport(
            table_name=table_name,
            execution_date=datetime.utcnow(),
            total_rules=len(results),
            passed=passed,
            failed=failed,
            warnings=warnings,
            results=results,
            overall_status=overall,
        )
