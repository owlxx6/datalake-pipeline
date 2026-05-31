#!/usr/bin/env python3
"""One-shot : affiche le schéma réel d'une partition Landing (colonnes + types)
et un aperçu de quelques lignes. Sert à réconcilier le YAML (issu de l'EDD)
avec la donnée réellement ingérée par NiFi.

Usage:
    spark-submit scripts/show_landing_schema.py \\
        --path hdfs://ubuntu3:9000/data/landing/credit_amort/date_arrete=20260531
"""
from __future__ import annotations

import argparse

from pyspark.sql import SparkSession


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", required=True, help="Chemin HDFS de la partition Landing")
    parser.add_argument("--rows", type=int, default=3, help="Nb de lignes d'aperçu")
    args = parser.parse_args()

    spark = SparkSession.builder.appName("SCHEMA").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(args.path)
    cols = df.columns
    print(f"\n{'=' * 70}")
    print(f"  SCHEMA  |  {args.path}")
    print(f"  Colonnes : {len(cols)}")
    print(f"{'=' * 70}\n")
    for f in df.schema.fields:
        print(f"  {f.name:<40} {f.dataType.simpleString()}")

    print(f"\n  Lignes totales : {df.count()}\n")
    df.show(args.rows, truncate=40, vertical=True)

    spark.stop()


if __name__ == "__main__":
    main()
