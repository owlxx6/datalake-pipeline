"""Validation empirique de TOUTES les jointures du diagramme (docs/diagram jointure.jpeg).

Pour chaque jointure : nb de lignes côté source (clé non nulle), nb matchées dans la
cible, taux de match. TRIM appliqué des deux côtés (padding position fixe). Les
dimensions SCD2 sont filtrées sur is_current = true.

Usage (sur la VM) :
    spark-submit --driver-memory 4g scripts/test_jointures_diagramme.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("TEST_JOINTURES_DIAGRAMME")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hive")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")
DB = "silver_credit_garantie"

SCD2_TABLES = {"pops", "asal", "image_contrat", "grc_portefeuille", "tiers", "typologie"}


def keyset(table: str, expr: str):
    """Ensemble distinct des clés (trimées) de la table cible."""
    df = spark.table(f"{DB}.{table}")
    if table in SCD2_TABLES:
        df = df.where("is_current = true")
    return df.select(F.trim(F.expr(expr)).alias("k")).where("k IS NOT NULL AND k <> ''").distinct()


def test(name: str, src_table: str, src_expr: str, dst_table: str, dst_expr: str):
    src = spark.table(f"{DB}.{src_table}")
    if src_table in SCD2_TABLES:
        src = src.where("is_current = true")
    src_keys = src.select(F.trim(F.expr(src_expr)).alias("k")).where("k IS NOT NULL AND k <> ''")
    total = src_keys.count()
    matched = src_keys.join(keyset(dst_table, dst_expr), "k", "left_semi").count()
    pct = (100.0 * matched / total) if total else 0.0
    flag = "OK " if pct >= 95 else ("~  " if pct >= 50 else "KO ")
    print(f"  [{flag}] {name:<62} {matched:>9}/{total:<9} = {pct:6.2f}%")


print("\n" + "=" * 100)
print("  VALIDATION DES JOINTURES DU DIAGRAMME — donnée réelle (arrêté 2026-05-31)")
print("=" * 100)

print("\n-- Faits → hub IMAGE_CONTRAT (IDF_REF_EXT_CTR) --")
test("CREDIT_AMORT.NUM_COMPTE_EMPRUNTEUR = IMAGE_CONTRAT.IDF_REF_EXT_CTR",
     "credit_amort", "NUM_COMPTE_EMPRUNTEUR", "image_contrat", "IDF_REF_EXT_CTR")
test("DEPASSEMENT.NUM_COMPTE = IMAGE_CONTRAT.IDF_REF_EXT_CTR",
     "depassement", "NUM_COMPTE", "image_contrat", "IDF_REF_EXT_CTR")
test("IDCE_AUTORISATION.IDF_REF_AUTO = IMAGE_CONTRAT.IDF_REF_EXT_CTR",
     "idce_autorisation", "IDF_REF_AUTO", "image_contrat", "IDF_REF_EXT_CTR")
test("GARANTIE_CONTRAT.IDF_REF_EXT_CREDIT = IMAGE_CONTRAT.IDF_REF_EXT_CTR  (ambig. n6)",
     "garantie_contrat", "IDF_REF_EXT_CREDIT", "image_contrat", "IDF_REF_EXT_CTR")

print("\n-- Hub → dimensions tiers --")
test("IMAGE_CONTRAT.IDF_NUM_TIERS = TIERS.IDF_NUM_TIERS",
     "image_contrat", "IDF_NUM_TIERS", "tiers", "IDF_NUM_TIERS")
test("IMAGE_CONTRAT.IDF_NUM_TIERS = TYPOLOGIE.IDF_NUM_TIERS",
     "image_contrat", "IDF_NUM_TIERS", "typologie", "IDF_NUM_TIERS")
test("GRC_PORTEFEUILLE.IDF_NUM_TIERS = TIERS.IDF_NUM_TIERS",
     "grc_portefeuille", "IDF_NUM_TIERS", "tiers", "IDF_NUM_TIERS")
test("TYPOLOGIE.IDF_NUM_TIERS = TIERS.IDF_NUM_TIERS  (outrigger 1:1)",
     "typologie", "IDF_NUM_TIERS", "tiers", "IDF_NUM_TIERS")

print("\n-- Produit (3 hypothèses) --")
test("CREDIT_AMORT.COD_PRODUIT = POPS.IDF_CODE_PRODUIT  (diagramme)",
     "credit_amort", "COD_PRODUIT", "pops", "IDF_CODE_PRODUIT")
test("CREDIT_AMORT.COD_PRODUIT = POPS.COD_SOP  (découverte donnée)",
     "credit_amort", "COD_PRODUIT", "pops", "COD_SOP")
test("CONCAT(IMAGE_CONTRAT.typ, ref) = POPS.IDF_CODE_PRODUIT  (encadrant)",
     "image_contrat", "concat(IDF_TYP_PRD, IDF_REF_PRD)", "pops", "IDF_CODE_PRODUIT")
test("CONCAT sans espaces (robuste post-normalisation)",
     "image_contrat", "regexp_replace(concat(IDF_TYP_PRD, IDF_REF_PRD), '\\\\s', '')",
     "pops", "regexp_replace(IDF_CODE_PRODUIT, '\\\\s', '')")

print("\n-- Faits entre eux / chaîne garantie --")
test("IMPAYES_CDL.IDF_NUM_DOSSIER = CREDIT_AMORT.IDF_NUM_DOSS",
     "impayes_cdl", "IDF_NUM_DOSSIER", "credit_amort", "IDF_NUM_DOSS")
test("GARANTIE_CONTRAT.IDF_NUM_CTR_GARANTIE = GARANTIE_DETAIL.IDF_NUM_CTR_GARANTIE",
     "garantie_contrat", "IDF_NUM_CTR_GARANTIE", "garantie_detail", "IDF_NUM_CTR_GARANTIE")

print("\n-- Non testables aujourd'hui --")
print("  [--] GRC_PORTEFEUILLE.IDF_NUM_GUI = ORG.IDF_NUM_GUI            (table ORG non ingérée)")
print("  [--] TIERS <-> GARANTIE_TIERS (IDF_NUM_TIERS)                  (table non industrialisée)")
print("  [--] GARANTIE_TIERS <-> GARANTIE_DETAIL                        (table non industrialisée)")

print("\nLégende : OK >= 95% · ~ 50-95% · KO < 50% — TRIM des deux côtés, dimensions SCD2 sur is_current.")
print("=" * 100)
spark.stop()
