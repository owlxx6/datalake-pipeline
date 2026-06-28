#!/usr/bin/env python3
"""Extrait des éléments de la qualité des données pour le rapport (figures/tableaux).

Produit, depuis l'environnement de validation :
  1. Résumé règle-niveau par table × statut (table d'audit QUALITY_RESULTS).
  2. Échantillon de règles (CREDIT_AMORT) — colonnes lisibles pour une figure.
  3. Résumé ligne-niveau PASSED / WARNING / REJECTED (flag dq_status du Silver).

Affiche tout en console (à copier) et écrit de petits CSV dans docs/dq_extract/.
À lancer sur la VM (HDFS + Metastore requis) :

  export PYSPARK_PYTHON=/usr/bin/python3 PYSPARK_DRIVER_PYTHON=/usr/bin/python3
  spark-submit --driver-memory 4g scripts/extract_dq_sample.py
"""
from __future__ import annotations

import csv
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

OUT = Path("/media/sf_datalake-pipeline/docs/dq_extract")
QR = "silver_credit_garantie.BR_CREDIT_GARANTIE_QUALITY_RESULTS"
SILVER_FACT = "silver_credit_garantie.credit_amort"


def write_csv(name, header, rows):
    OUT.mkdir(parents=True, exist_ok=True)
    with open(OUT / name, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        w.writerows(rows)
    print(f"  -> {OUT / name}")


def main() -> None:
    spark = (
        SparkSession.builder.appName("EXTRACT_dq_sample")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    qr = spark.table(QR)
    cols = {c.lower(): c for c in qr.columns}
    # dernier arrêté / situation
    datecol = cols.get("date_situation") or cols.get("date_arrete")
    if datecol:
        last = qr.agg(F.max(datecol)).collect()[0][0]
        qr = qr.where(F.col(datecol) == last)
        print(f"\n=== QUALITY_RESULTS — arrêté {last} ===")

    # 1. résumé règle-niveau par table x statut
    print("\n--- (1) Règles par table x statut ---")
    summ = (qr.groupBy(cols["table_name"], cols["status"]).count()
              .orderBy(cols["table_name"], cols["status"]).collect())
    rows1 = [[r[0], r[1], r[2]] for r in summ]
    for r in rows1:
        print(f"  {r[0]:<22} {r[1]:<10} {r[2]}")
    write_csv("dq_resume_par_table.csv", ["table", "statut_regle", "nb_regles"], rows1)

    # 2. échantillon de règles CREDIT_AMORT
    print("\n--- (2) Échantillon de règles — CREDIT_AMORT ---")
    sel = [cols[c] for c in ["field_name", "rule_type", "severity", "status",
                             "records_checked", "records_failed", "passed_percentage"] if c in cols]
    samp = (qr.where(F.lower(F.col(cols["table_name"])) == "credit_amort")
              .select(*sel).orderBy(cols["status"], cols["rule_type"]).limit(18).collect())
    hdr = [c for c in ["field_name", "rule_type", "severity", "status",
                       "records_checked", "records_failed", "passed_percentage"] if c in cols]
    rows2 = [[("" if v is None else v) for v in r] for r in samp]
    print("  " + " | ".join(hdr))
    for r in rows2:
        print("  " + " | ".join(str(v) for v in r))
    write_csv("dq_echantillon_credit_amort.csv", hdr, rows2)

    # 3. résumé ligne-niveau (dq_status du Silver)
    print("\n--- (3) Lignes par état DQ — CREDIT_AMORT ---")
    try:
        f = spark.table(SILVER_FACT)
        fcols = {c.lower(): c for c in f.columns}
        st = fcols.get("dq_status")
        if "date_arrete" in fcols:
            la = f.agg(F.max(fcols["date_arrete"])).collect()[0][0]
            f = f.where(F.col(fcols["date_arrete"]) == la)
        if st:
            line = f.groupBy(st).count().orderBy(st).collect()
            rows3 = [[r[0], r[1]] for r in line]
            for r in rows3:
                print(f"  {str(r[0]):<12} {r[1]}")
            write_csv("dq_lignes_par_etat.csv", ["dq_status", "nb_lignes"], rows3)
        else:
            print("  (colonne dq_status absente du Silver credit_amort)")
    except Exception as e:  # pragma: no cover
        print(f"  [SKIP] {e}")

    spark.stop()
    print("\n[DONE]")


if __name__ == "__main__":
    main()
