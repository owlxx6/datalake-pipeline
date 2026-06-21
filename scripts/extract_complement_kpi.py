#!/usr/bin/env python3
"""Extrait 2 KPI complémentaires depuis le Silver (agrégats seulement, pas de détail client).

Produit 2 petits CSV (totaux) transférables par Git (aucune donnée nominative) :
  - powerbi/funnel_octroi.csv  : Montant Accordé -> Montant Décaissé (funnel D4)
  - powerbi/recouvrement.csv   : nb impayés / non réglés / régularisés (taux régularisation D2)

Source : silver_credit_garantie.credit_amort (arrêté --date-arrete).
À lancer sur la VM (HDFS + Metastore requis) :

  export PYSPARK_PYTHON=/usr/bin/python3 PYSPARK_DRIVER_PYTHON=/usr/bin/python3
  spark-submit --driver-memory 4g scripts/extract_complement_kpi.py --date-arrete 2026-05-31
"""
from __future__ import annotations

import argparse
import csv
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    ap = argparse.ArgumentParser(description="Extrait funnel octroi + recouvrement (agrégats).")
    ap.add_argument("--date-arrete", default="2026-05-31")
    ap.add_argument("--out", default=str(ROOT / "powerbi"))
    args = ap.parse_args()

    spark = (
        SparkSession.builder.appName("EXTRACT_complement_kpi")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    df = (spark.table("silver_credit_garantie.credit_amort")
          .where(F.col("date_arrete") == args.date_arrete))

    agg = df.agg(
        F.sum("mnt_credit").alias("mnt_accorde"),
        F.sum("mnt_deblocage").alias("mnt_decaisse"),
        F.sum("nbr_impayes").alias("nbr_impayes"),
        F.sum("nbr_imp_non_reg").alias("nbr_imp_non_reg"),
    ).collect()[0]

    accorde = float(agg["mnt_accorde"] or 0)
    decaisse = float(agg["mnt_decaisse"] or 0)
    nbr_imp = float(agg["nbr_impayes"] or 0)
    nbr_nreg = float(agg["nbr_imp_non_reg"] or 0)
    nbr_reg = nbr_imp - nbr_nreg

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)

    with open(out / "funnel_octroi.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ordre", "etape", "montant"])
        w.writerow([1, "Montant Accordé", round(accorde)])
        w.writerow([2, "Montant Décaissé", round(decaisse)])

    with open(out / "recouvrement.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["nbr_impayes", "nbr_imp_non_reg", "nbr_regularises"])
        w.writerow([round(nbr_imp), round(nbr_nreg), round(nbr_reg)])

    spark.stop()
    print("[DONE] funnel_octroi.csv + recouvrement.csv ->", out)
    print(f"  Accordé={accorde:,.0f}  Décaissé={decaisse:,.0f}  "
          f"(taux décaissement={decaisse/accorde*100:.1f}%)" if accorde else "  Accordé=0")
    print(f"  Impayés={nbr_imp:,.0f}  Non réglés={nbr_nreg:,.0f}  Régularisés={nbr_reg:,.0f}  "
          f"(taux régul={nbr_reg/nbr_imp*100:.1f}%)" if nbr_imp else "  Impayés=0")


if __name__ == "__main__":
    main()
