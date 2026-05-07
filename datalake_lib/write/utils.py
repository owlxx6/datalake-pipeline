"""Utilitaires PySpark partagés par le module write.

`build_merge_sql` est pure Python — testable sans Spark.
Les autres fonctions importent pyspark de façon lazy.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from .exceptions import MergeKeyError

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


# ── SQL pur (testable sans Spark) ─────────────────────────────────────────


def build_merge_sql(
    target: str,
    source_alias: str,
    merge_keys: list[str],
    all_columns: list[str],
) -> str:
    """Génère le SQL `MERGE INTO` Spark sur Iceberg.

    Args:
        target: Nom de la table cible (ex: `silver_credit_credit_amort`).
        source_alias: Alias de la table source dans le SQL (ex: `src`).
        merge_keys: Liste des colonnes formant la condition de jointure.
        all_columns: Toutes les colonnes du DataFrame source — utilisées
            pour la clause UPDATE SET et l'INSERT.

    Returns:
        SQL `MERGE INTO ... USING ... ON ... WHEN MATCHED ... WHEN NOT MATCHED`.

    Raises:
        MergeKeyError: Si `merge_keys` est vide.
    """
    if not merge_keys:
        raise MergeKeyError("merge_keys ne peut pas être vide pour build_merge_sql")

    join_cond = " AND ".join(
        f"tgt.{k} = {source_alias}.{k}" for k in merge_keys
    )
    update_cols = [c for c in all_columns if c not in merge_keys]
    set_clause = ",\n    ".join(
        f"tgt.{c} = {source_alias}.{c}" for c in update_cols
    )
    insert_cols = ", ".join(all_columns)
    insert_vals = ", ".join(f"{source_alias}.{c}" for c in all_columns)

    return (
        f"MERGE INTO {target} AS tgt\n"
        f"USING {source_alias} AS {source_alias}\n"
        f"ON {join_cond}\n"
        f"WHEN MATCHED THEN UPDATE SET\n"
        f"    {set_clause}\n"
        f"WHEN NOT MATCHED THEN INSERT ({insert_cols})\n"
        f"VALUES ({insert_vals})"
    )


# ── Fonctions Spark (imports lazy) ────────────────────────────────────────


def validate_merge_keys(df: DataFrame, merge_keys: list[str]) -> None:
    """Vérifie que `merge_keys` est non-vide et que toutes les clés
    existent dans le schéma du DataFrame.

    Raises:
        MergeKeyError: Si la liste est vide ou si une clé est absente.
    """
    if not merge_keys:
        raise MergeKeyError(
            "merge_keys ne peut pas être vide en mode incremental",
            missing_keys=[],
        )

    df_columns = set(df.columns)
    missing = [k for k in merge_keys if k not in df_columns]
    if missing:
        raise MergeKeyError(
            f"Clés de merge absentes du DataFrame : {missing}",
            missing_keys=missing,
        )


def add_technical_cols(
    df: DataFrame,
    layer: str,
    job_name: str,
) -> DataFrame:
    """Ajoute les colonnes techniques d'audit selon la couche cible.

    Silver : `_updated_ts` + `_job_name`
    Gold   : `_gold_ts`    + `_job_name`

    Args:
        df: DataFrame d'entrée.
        layer: "silver" ou "gold".
        job_name: Nom du job Spark.

    Returns:
        Nouveau DataFrame avec les colonnes ajoutées en fin.
    """
    from pyspark.sql import functions as F

    ts_col = "_updated_ts" if layer == "silver" else "_gold_ts"
    return (
        df.withColumn(ts_col, F.current_timestamp())
        .withColumn("_job_name", F.lit(job_name))
    )


def count_partitions_written(
    df: DataFrame,
    partition_cols: list[str],
) -> int:
    """Compte le nombre de combinaisons distinctes des colonnes de partition.

    Utilisé pour remplir `WriteResult.partitions_written`.
    Si `partition_cols` est vide ou si aucune colonne ne matche, retourne 1.
    """
    if not partition_cols:
        return 1

    existing = [c for c in partition_cols if c in df.columns]
    if not existing:
        return 1

    return df.select(*existing).distinct().count()
