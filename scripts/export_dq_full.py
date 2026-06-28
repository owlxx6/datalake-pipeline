#!/usr/bin/env python3
"""Dump COMPLET de la table d'audit DQ (toutes les colonnes, dernier arrêté).

Écrit un CSV unique avec TOUS les champs de BR_CREDIT_GARANTIE_QUALITY_RESULTS
dans docs/dq_extract/quality_results_full.csv (lisible / réutilisable pour le rapport).

  export PYSPARK_PYTHON=/usr/bin/python3 PYSPARK_DRIVER_PYTHON=/usr/bin/python3
  spark-submit --driver-memory 4g scripts/export_dq_full.py
"""
from __future__ import annotations

import csv
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

QR = "silver_credit_garantie.BR_CREDIT_GARANTIE_QUALITY_RESULTS"
OUT = Path("/media/sf_datalake-pipeline/docs/dq_extract")


def main() -> None:
    spark = (
        SparkSession.builder.appName("EXPORT_dq_full")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = spark.table(QR)
    cols = {c.lower(): c for c in df.columns}
    datecol = cols.get("date_situation") or cols.get("date_arrete")
    if datecol:
        last = df.agg(F.max(datecol)).collect()[0][0]
        df = df.where(F.col(datecol) == last)
        print(f"Dernier arrêté : {last}")

    columns = df.columns
    rows = df.collect()
    print(f"Colonnes ({len(columns)}) : {columns}")
    print(f"Lignes : {len(rows)}")

    OUT.mkdir(parents=True, exist_ok=True)
    target = OUT / "quality_results_full.csv"
    with open(target, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(columns)
        for r in rows:
            w.writerow(["" if v is None else v for v in r])

    spark.stop()
    print(f"[DONE] {target}")


if __name__ == "__main__":
    main()
