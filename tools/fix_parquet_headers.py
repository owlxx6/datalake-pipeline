"""fix_parquet_headers.py — Corrige un Parquet dont la ligne 0 est le header.

Problème : la conversion Excel → Parquet a stocké les noms de colonnes
comme première ligne de données (column_0, column_1...) au lieu du schéma.
Certaines colonnes numériques nullable sont encodées en structs {long, string}.

Usage :
    spark-submit tools/fix_parquet_headers.py \
        --input  hdfs://ubuntu3:9000/data/landing/TIERS/date_arrete=20260513 \
        --output hdfs://ubuntu3:9000/data/landing/TIERS/date_arrete=20260513_clean \
        --marker-col column_3 \
        --marker-val COD_TYP_TIERS
"""
from __future__ import annotations

import argparse
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


def fix(input_path: str, output_path: str, marker_col: str, marker_val: str) -> None:
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("fix_parquet_headers")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    df = spark.read.parquet(input_path)
    schema = df.schema
    header = df.first()

    # ── 1. Extraire les noms de colonnes depuis la ligne 0 ────────────────────
    col_map: dict[str, str] = {}
    for field in schema.fields:
        cn = field.name
        val = header[cn]
        if isinstance(field.dataType, StructType):
            # Header : {null, "NOM_COL"} — le champ string contient le nom
            name = None
            for sf in field.dataType.fields:
                v = val[sf.name] if val is not None else None
                if isinstance(v, str) and v.strip():
                    name = v.strip()
                    break
            col_map[cn] = name or cn
        else:
            col_map[cn] = str(val).strip() if val is not None else cn

    print(f"\n=== MAPPING ({len(col_map)} colonnes) ===")
    for k, v in list(col_map.items())[:15]:
        print(f"  {k:12} → {v}")
    print("  ...")

    # ── 2. Supprimer la ligne header ──────────────────────────────────────────
    df_data = df.filter(F.col(marker_col) != marker_val)
    total = df_data.count()
    print(f"\n=== LIGNES DE DONNÉES : {total} ===")

    # ── 3. Construire les expressions de sélection ────────────────────────────
    exprs = []
    for field in schema.fields:
        cn = field.name
        new_name = col_map.get(cn, cn)
        if isinstance(field.dataType, StructType):
            # Colonnes struct {valeur_num, valeur_str} : coalesce en string
            sub_exprs = [
                F.col(cn).getField(sf.name).cast("string")
                for sf in field.dataType.fields
            ]
            exprs.append(F.coalesce(*sub_exprs).alias(new_name))
        else:
            exprs.append(F.col(cn).alias(new_name))

    df_clean = df_data.select(*exprs)

    print("\n=== APERÇU ===")
    print("Colonnes :", df_clean.columns[:10], "...")
    df_clean.show(2, vertical=True, truncate=False)

    # ── 4. Écriture ───────────────────────────────────────────────────────────
    (
        df_clean
        .coalesce(1)
        .write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(output_path)
    )
    print(f"\n=== ÉCRIT → {output_path} ===")
    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input",      required=True)
    parser.add_argument("--output",     required=True)
    parser.add_argument("--marker-col", default="column_3",
                        help="Colonne string connue dans la ligne header")
    parser.add_argument("--marker-val", default="COD_TYP_TIERS",
                        help="Valeur de cette colonne dans la ligne header")
    args = parser.parse_args()
    fix(args.input, args.output, args.marker_col, args.marker_val)
