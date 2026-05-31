#!/usr/bin/env python3
"""One-shot : inspecte les valeurs réelles des colonnes codifiables d'une table.

Pour chaque champ qui a une règle `in_list` dans le YAML, lit la donnée Landing
et affiche les valeurs distinctes effectives + leur fréquence. Permet de raffiner
les `allowed_values` du YAML sur la base de la donnée réelle, pas de l'EDD partiel.

Usage:
    spark-submit scripts/inspect_codifications.py \\
        --config configs/bronze/credit_garantie.yaml \\
        --date-arrete 2026-05-14 \\
        --table TIERS
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from jobs.bronze.config_loader import load_config


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", required=True)
    parser.add_argument("--date-arrete", required=True)
    parser.add_argument("--table", required=True)
    parser.add_argument("--top-n", type=int, default=30,
                        help="Nombre max de valeurs distinctes à afficher par champ")
    args = parser.parse_args()

    config = load_config(args.config)
    table = config.get_table(args.table)
    if not table:
        print(f"Table {args.table} introuvable dans {args.config}")
        sys.exit(1)

    # Construction du chemin Landing
    source_path = config.resolve(
        table.source.path_template,
        table_name=table.name,
        date_arrete=args.date_arrete.replace("-", ""),
    )

    # Récupère les champs avec une règle in_list + leurs allowed_values actuels
    inlist_fields = []
    for rule in table.quality_rules:
        for check in rule.checks:
            if check.type == "in_list" and check.allowed_values:
                inlist_fields.append((rule.source_column, check.allowed_values))

    if not inlist_fields:
        print(f"Aucune règle in_list trouvée pour {args.table}")
        sys.exit(0)

    spark = (SparkSession.builder
             .appName(f"INSPECT_{args.table}")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    df = spark.read.parquet(source_path)
    # Aligne sur la convention YAML : NiFi écrit le Parquet en minuscules, le YAML
    # référence les champs en MAJUSCULES. On retire aussi les colonnes techniques
    # NiFi (préfixe "_").
    df = df.select(*[c for c in df.columns if not c.startswith("_")])
    df = df.toDF(*[c.upper() for c in df.columns])
    total = df.count()
    print(f"\n{'=' * 70}")
    print(f"  INSPECTION {args.table}  |  {source_path}")
    print(f"  Lignes totales : {total}")
    print(f"  Champs codifiables à inspecter : {len(inlist_fields)}")
    print(f"{'=' * 70}\n")

    for col_name, current_allowed in inlist_fields:
        if col_name not in df.columns:
            print(f"[{col_name}] COLONNE ABSENTE du DataFrame\n")
            continue

        # Cast en string pour gérer types mixtes, puis comptage
        agg = (df.withColumn(col_name, F.col(col_name).cast("string"))
                 .groupBy(col_name)
                 .agg(F.count("*").alias("nb"))
                 .orderBy(F.desc("nb"))
                 .limit(args.top_n)
                 .collect())

        actual_values = [r[col_name] for r in agg if r[col_name] is not None]
        nulls = sum(r["nb"] for r in agg if r[col_name] is None)
        stripped_allowed = {v.strip() if v else v for v in current_allowed}

        in_allowed = [v for v in actual_values if v and v.strip() in stripped_allowed]
        missing = [v for v in actual_values if v and v.strip() not in stripped_allowed]

        print(f"── {col_name} ──")
        print(f"  YAML allowed_values : {current_allowed}")
        print(f"  Top valeurs réelles ({len(agg)}) :")
        for r in agg:
            val = r[col_name]
            nb = r["nb"]
            pct = nb / total * 100
            marker = ("✓" if val is not None and val.strip() in stripped_allowed
                      else ("∅" if val is None else "✗"))
            print(f"    {marker} {repr(val):<20} {nb:>6}  ({pct:5.2f}%)")
        if missing:
            print(f"  → manquantes dans YAML : {missing}")
        print()

    spark.stop()


if __name__ == "__main__":
    main()
