#!/usr/bin/env python3
"""Export des tables Gold (+ QUALITY_RESULTS) en CSV, pour import dans Power BI.

Lit les noms de cibles depuis le YAML Gold (déclaratif), exporte chaque table/vue en
CSV avec en-tête (un fichier par table). Pour une démo Power BI en mode Import : pas
besoin de connexion live à la VM.

  spark-submit --driver-memory 4g scripts/export_gold_powerbi.py
  # puis récupérer le dossier (voir runbook) :  cp -r /home/hadoop/exports_powerbi /media/sf_datalake-pipeline/exports
"""
from __future__ import annotations

import argparse
from pathlib import Path

import yaml
from pyspark.sql import SparkSession

ROOT = Path(__file__).resolve().parents[1]
# Table d'audit DQ (pour le dashboard Data Quality)
EXTRA = ["silver_credit_garantie.BR_CREDIT_GARANTIE_QUALITY_RESULTS"]


def main() -> None:
    ap = argparse.ArgumentParser(description="Export Gold -> CSV pour Power BI.")
    ap.add_argument("--config", default=str(ROOT / "configs/gold/credit_garantie.yaml"))
    ap.add_argument("--out", default="/home/hadoop/exports_powerbi")
    args = ap.parse_args()

    cfg = yaml.safe_load(open(args.config, encoding="utf-8"))
    db = cfg.get("database", "gold_credit_garantie")
    tables = [f"{db}.{t['name']}" for t in cfg["targets"]] + EXTRA

    spark = (
        SparkSession.builder.appName("EXPORT_gold_powerbi")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    out = args.out.rstrip("/")
    print(f"\n===== Export Gold -> {out} (CSV, en-tête) =====")
    for fq in tables:
        name = fq.split(".")[-1].lower()
        try:
            df = spark.table(fq)
        except Exception as e:                       # table absente -> on continue
            print(f"  [SKIP] {fq} : {e}")
            continue
        n = df.count()
        (df.coalesce(1).write
           .option("header", True)
           .option("encoding", "UTF-8")
           .mode("overwrite")
           .csv(f"file://{out}/{name}"))
        print(f"  ✓ {fq:55s} -> {name}/  ({n} lignes)")

    spark.stop()
    print(f"\n[DONE] CSV dans {out}/ (un sous-dossier par table, fichier part-*.csv)")


if __name__ == "__main__":
    main()
