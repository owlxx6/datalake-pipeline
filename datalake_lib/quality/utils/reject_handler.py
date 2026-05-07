"""Gestion des lignes rejetées par le runner DQ."""

from __future__ import annotations

import logging

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

logger = logging.getLogger(__name__)


def write_rejects(
    df_rejected: DataFrame,
    table_name: str,
    output_path: str,
    partition_by: str = "date_arrete",
) -> None:
    """Écrit les lignes rejetées dans une table de rejects.

    Chemin cible : `{output_path}/{table_name}_rejects/`
    Format : Parquet, partitionné par `partition_by` (défaut `date_arrete`).
    Mode : append.

    Args:
        df_rejected: DataFrame de rejets avec colonnes `_dq_rule_id`, `_dq_reason`.
        table_name: Nom logique de la table source (ex: 'credit_amort').
        output_path: Préfixe HDFS / chemin de sortie.
        partition_by: Colonne de partitionnement (défaut `date_arrete`).
    """
    target = f"{output_path.rstrip('/')}/{table_name}_rejects"

    writer = df_rejected.write.mode("append").format("parquet")
    if partition_by in df_rejected.columns:
        writer = writer.partitionBy(partition_by)
    else:
        logger.warning(
            "Colonne de partitionnement '%s' absente des rejets — "
            "écriture sans partitioning.",
            partition_by,
        )

    writer.save(target)
    logger.info("Rejets écrits dans '%s'", target)


def count_rejects_by_rule(df_rejected: DataFrame) -> dict[str, int]:
    """Retourne le nombre de rejets par `rule_id`.

    Args:
        df_rejected: DataFrame de rejets avec colonne `_dq_rule_id`.

    Returns:
        Dictionnaire {rule_id: nombre_de_rejets}.
    """
    if df_rejected.rdd.isEmpty():
        return {}

    rows = (
        df_rejected.groupBy("_dq_rule_id")
        .agg(F.count("*").alias("nb"))
        .collect()
    )
    return {row["_dq_rule_id"]: int(row["nb"]) for row in rows}
