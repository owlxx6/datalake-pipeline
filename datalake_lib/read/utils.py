"""Utilitaires PySpark partagés par le module read.

Aucun import top-level de pyspark : les imports sont locaux aux fonctions
pour permettre `import datalake_lib.read.utils` même sans cluster Spark
installé (utile pour test_models.py).
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)


def add_audit_cols(
    df: DataFrame,
    source_system: str,
    layer: str,
) -> DataFrame:
    """Ajoute les colonnes d'audit en fin de DataFrame.

    Colonnes ajoutées :
    - `_ingestion_ts`  : `current_timestamp()` (timestamp UTC côté Spark)
    - `_source_system` : littéral (ex: "evolan", "garweb")
    - `_layer`         : "bronze" | "silver" | "gold"

    Args:
        df: DataFrame d'entrée.
        source_system: Système source (sera stocké en littéral).
        layer: Couche cible.

    Returns:
        Nouveau DataFrame avec les 3 colonnes ajoutées en fin.
    """
    from pyspark.sql import functions as F

    return (
        df.withColumn("_ingestion_ts", F.current_timestamp())
        .withColumn("_source_system", F.lit(source_system))
        .withColumn("_layer", F.lit(layer))
    )


def cast_columns(
    df: DataFrame,
    schema: dict[str, str],
) -> DataFrame:
    """Cast les colonnes d'un DataFrame selon un mapping `{col: type_spark}`.

    Comportement :
    - Une colonne absente du DataFrame est **ignorée** (pas d'erreur).
    - Si le cast échoue (type inconnu, incompatible), la colonne reste
      inchangée et un warning est loggé.

    Args:
        df: DataFrame source.
        schema: Mapping `{nom_colonne: type_spark_string}` — ex.
            `{"montant": "double", "date_echeance": "date"}`.

    Returns:
        DataFrame avec colonnes castées.
    """
    df_columns = set(df.columns)
    out = df

    for col_name, type_str in schema.items():
        if col_name not in df_columns:
            logger.debug(
                "cast_columns : colonne '%s' absente, skip silencieux", col_name
            )
            continue

        try:
            out = out.withColumn(col_name, out[col_name].cast(type_str))
        except Exception as exc:
            logger.warning(
                "cast_columns : échec cast '%s' → %s (%s) — colonne inchangée",
                col_name,
                type_str,
                exc,
            )

    return out


def infer_schema(df: DataFrame) -> dict[str, str]:
    """Retourne un mapping `{col: dtype_string}` depuis le schéma Spark.

    Exemple de sortie :
        {"NUM_CONTRAT": "string", "MONTANT": "double", "DATE_ECHEANCE": "date"}
    """
    return {field.name: field.dataType.simpleString() for field in df.schema.fields}
