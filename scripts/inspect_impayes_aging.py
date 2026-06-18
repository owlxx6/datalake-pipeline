#!/usr/bin/env python3
"""Diagnostic aging IMPAYES_CDL (lecture seule).

Aide à trancher quelle date sert de référence pour l'ancienneté des impayés, et si
COD_STATUT_INCIDENT encode déjà une classe d'ancienneté.

  spark-submit --driver-memory 2g scripts/inspect_impayes_aging.py
"""
from __future__ import annotations

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

TABLE = "silver_credit_garantie.impayes_cdl"
DATE_COLS = ["date_mise_impaye", "date_echeance", "date_creance",
             "date_statut", "date_effet", "date_remise"]
STATUS_COLS = ["cod_statut_incident", "nat_declassement", "priorite_statut"]


def main() -> None:
    spark = (
        SparkSession.builder.appName("INSPECT_impayes_aging")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.table(TABLE)
    total = df.count()
    colmap = {c.lower(): c for c in df.columns}  # résolution insensible à la casse
    print(f"\n===== {TABLE} : {total} lignes =====")
    print("Colonnes réelles :", df.columns)

    # 1) Remplissage des dates candidates (non null ET non vide) + min/max
    print("\n--- DATES candidates (remplissage + min/max) ---")
    for c in DATE_COLS:
        col = colmap.get(c)
        if not col:
            print(f"  {c:20s} : (absente)")
            continue
        nn = df.filter(F.col(col).isNotNull() & (F.trim(F.col(col).cast("string")) != "")).count()
        pct = 100.0 * nn / total if total else 0.0
        mn, mx = df.select(F.min(col), F.max(col)).first()
        print(f"  {c:20s} : {nn:>9d} remplis ({pct:5.1f}%)  min={mn}  max={mx}")

    # 2) Distribution des champs de statut (classes d'ancienneté ?)
    for sc in STATUS_COLS:
        col = colmap.get(sc)
        if not col:
            print(f"\n--- {sc} : (absente) ---")
            continue
        print(f"\n--- {col} : valeurs distinctes (top 40) ---")
        (df.groupBy(col).count().orderBy(F.desc("count")).limit(40)).show(40, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
