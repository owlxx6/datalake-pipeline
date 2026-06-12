"""Sanity check Silver + Gold — détecte tables manquantes, doublons, versions parasites.

Vérifie, contre les attendus du re-baseline 2026-05-31 :
  Silver dims SCD2 : volumétrie, uniquement v1/is_current (pas de versions parasites),
                     zéro doublon de PK parmi les lignes courantes.
  Silver faits     : volumétrie par partition date_arrete (pas de partition doublée).
  Gold             : volumétrie des cibles + grain OBT (pas de fan-out) + vue KPI.

Usage (sur la VM) :
    spark-submit --driver-memory 4g scripts/check_lake_sanity.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("CHECK_LAKE_SANITY")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hive")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

SILVER = "silver_credit_garantie"
GOLD = "gold_credit_garantie"
ARRETE = "2026-05-31"

# (table, pk_cols, lignes attendues)
DIMS = [
    ("pops", ["idf_code_produit"], 1226),
    ("asal", ["idf_personnel"], 4464),
    ("org", ["idf_num_gui", "idf_num_bur"], 762),
    ("typologie", ["idf_num_tiers"], 154249),
    ("tiers", ["idf_num_tiers"], 154245),
    ("grc_portefeuille", ["idf_num_gui", "idf_num_tiers", "idf_code_cc"], 151528),
    ("image_contrat", ["idf_num_tiers", "idf_ref_ext_ctr", "idf_typ_prd", "idf_ref_prd"], 158322),
]
FACTS = [
    ("credit_amort", 200000),
    ("impayes_cdl", 1597242),
    ("depassement", 75870),
    ("garantie_detail", 253900),
    ("garantie_contrat", 258473),
    ("idce_autorisation", 368567),
]
GOLD_TABLES = [
    ("dim_temps", 4018),
    ("dim_produit", 1226),
    ("dim_organisation", 762),
    ("gdw_encours", 200000),
    ("gdw_impayes", 1597242),
    ("gdw_synthese_credit", 200000),
]

ko = 0

def status(ok, msg):
    global ko
    if not ok:
        ko += 1
    print(f"  [{'OK' if ok else 'KO'}] {msg}")


print("\n" + "=" * 95)
print(f"  SANITY CHECK — {SILVER} + {GOLD} (arrêté {ARRETE})")
print("=" * 95)

print("\n-- Inventaire des tables --")
silver_tables = {r.tableName for r in spark.sql(f"SHOW TABLES IN {SILVER}").collect()}
gold_tables = {r.tableName for r in spark.sql(f"SHOW TABLES IN {GOLD}").collect()}
expected_silver = {t for t, _, _ in DIMS} | {t for t, _ in FACTS} | {
    "br_credit_garantie_quality_results", "br_credit_garantie_log_pipeline"}
extra_s = silver_tables - expected_silver
missing_s = expected_silver - silver_tables
status(not missing_s, f"Silver : {len(silver_tables)} tables (manquantes : {sorted(missing_s) or 'aucune'})")
if extra_s:
    print(f"       tables en plus (à examiner) : {sorted(extra_s)}")
expected_gold = {t for t, _ in GOLD_TABLES} | {"v_kpi_credit"}
missing_g = expected_gold - gold_tables
status(not missing_g, f"Gold : {len(gold_tables)} tables/vues (manquantes : {sorted(missing_g) or 'aucune'})")
extra_g = gold_tables - expected_gold
if extra_g:
    print(f"       tables en plus (à examiner) : {sorted(extra_g)}")

print("\n-- Dimensions SCD2 (Silver) --")
for t, pk, expected in DIMS:
    df = spark.table(f"{SILVER}.{t}")
    total = df.count()
    current = df.where("is_current = true")
    n_cur = current.count()
    n_old = total - n_cur
    vmax = df.agg(F.max("version")).collect()[0][0]
    dup = n_cur - current.select(*pk).distinct().count()
    ok = (n_cur == expected and n_old == 0 and vmax == 1 and dup == 0)
    status(ok, f"{t:<18} total={total:<8} courantes={n_cur:<8} fermées={n_old:<6} "
               f"vmax={vmax}  dupPK={dup}  (attendu {expected})")

print("\n-- Faits (Silver, partition date_arrete) --")
for t, expected in FACTS:
    parts = (spark.table(f"{SILVER}.{t}")
             .groupBy("date_arrete").count().orderBy("date_arrete").collect())
    desc = ", ".join(f"{r['date_arrete']}={r['count']}" for r in parts)
    ok = (len(parts) == 1 and str(parts[0]["date_arrete"]) == ARRETE
          and parts[0]["count"] == expected)
    status(ok, f"{t:<18} partitions: {desc}  (attendu {ARRETE}={expected})")

print("\n-- Gold --")
for t, expected in GOLD_TABLES:
    n = spark.table(f"{GOLD}.{t}").count()
    status(n == expected, f"{t:<22} {n} lignes (attendu {expected})")
# Grain OBT : 1 ligne par dossier (pas de fan-out)
obt = spark.table(f"{GOLD}.gdw_synthese_credit")
n_obt = obt.count()
n_doss = obt.select("idf_num_doss").distinct().count()
status(n_obt == n_doss, f"OBT grain dossier : {n_obt} lignes / {n_doss} dossiers distincts")
# Vue KPI opérationnelle
try:
    n_kpi = spark.table(f"{GOLD}.v_kpi_credit").where("flag_cdl = true").count()
    status(n_kpi == 96474, f"v_kpi_credit : {n_kpi} crédits CDL via la vue (attendu 96474)")
except Exception as e:
    status(False, f"v_kpi_credit : ERREUR {e}")

print("\n-- Audit (info) --")
for t in ("br_credit_garantie_quality_results", "br_credit_garantie_log_pipeline"):
    n = spark.table(f"{SILVER}.{t}").count()
    print(f"  [--] {t:<40} {n} lignes (append-only, croît à chaque run)")

print("\n" + "=" * 95)
print(f"  VERDICT : {'TOUT EST PROPRE ✓' if ko == 0 else f'{ko} ANOMALIE(S) À CORRIGER ✗'}")
print("=" * 95)
spark.stop()
