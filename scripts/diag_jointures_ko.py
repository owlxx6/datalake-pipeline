"""Diagnostic des 2 jointures KO : IDF_REF_AUTO et IDF_REF_EXT_CREDIT.

1) Affiche les formats réels (échantillons + longueurs) des clés concernées.
2) Teste toutes les hypothèses de correspondance (options b/c/d de l'ambiguïté n°6
   + colonnes candidates des deux côtés).

Usage (sur la VM) :
    spark-submit --driver-memory 4g scripts/diag_jointures_ko.py
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("DIAG_JOINTURES_KO")
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
SCD2 = {"pops", "asal", "image_contrat", "grc_portefeuille", "tiers", "typologie"}


def t(table):
    df = spark.table(f"{DB}.{table}")
    return df.where("is_current = true") if table in SCD2 else df


def sample(table, col, n=5):
    rows = (t(table).select(F.expr(col).alias("v")).where("v IS NOT NULL")
            .groupBy("v").count().orderBy(F.desc("count")).limit(n).collect())
    print(f"  {table}.{col}:")
    for r in rows:
        print(f"      [{r['v']}]  (len={len(str(r['v']))}, n={r['count']})")


def match(name, src_t, src_e, dst_t, dst_e):
    src = (t(src_t).select(F.trim(F.expr(src_e)).alias("k"))
           .where("k IS NOT NULL AND k <> ''"))
    dst = (t(dst_t).select(F.trim(F.expr(dst_e)).alias("k"))
           .where("k IS NOT NULL AND k <> ''").distinct())
    total = src.count()
    m = src.join(dst, "k", "left_semi").count()
    pct = (100.0 * m / total) if total else 0.0
    flag = "OK " if pct >= 95 else ("~  " if pct >= 50 else "KO ")
    print(f"  [{flag}] {name:<70} {m:>9}/{total:<9} = {pct:6.2f}%")


print("\n" + "=" * 105)
print("  1. FORMATS RÉELS DES CLÉS")
print("=" * 105)
print("\n-- côté hub / crédit --")
sample("image_contrat", "IDF_REF_EXT_CTR")
sample("credit_amort", "IDF_NUM_DOSS")
sample("credit_amort", "NUM_DOSS_CDL")
print("\n-- côté autorisation --")
sample("idce_autorisation", "IDF_REF_AUTO")
sample("idce_autorisation", "NUM_COMPTE")
sample("idce_autorisation", "NUM_COMPTE_ENGAGEMENT")
print("\n-- côté garantie --")
sample("garantie_contrat", "IDF_REF_EXT_CREDIT")
sample("garantie_contrat", "NUM_CTR_CREDIT")
sample("garantie_contrat", "COD_FILIERE_GES_CREDIT")

print("\n" + "=" * 105)
print("  2. HYPOTHÈSES — IDCE_AUTORISATION → ?")
print("=" * 105)
match("IDF_REF_AUTO = IMAGE_CONTRAT.IDF_REF_EXT_CTR (sans espaces)",
      "idce_autorisation", "regexp_replace(IDF_REF_AUTO, '\\\\s', '')",
      "image_contrat", "regexp_replace(IDF_REF_EXT_CTR, '\\\\s', '')")
match("NUM_COMPTE = IMAGE_CONTRAT.IDF_REF_EXT_CTR",
      "idce_autorisation", "NUM_COMPTE", "image_contrat", "IDF_REF_EXT_CTR")
match("NUM_COMPTE_ENGAGEMENT = IMAGE_CONTRAT.IDF_REF_EXT_CTR",
      "idce_autorisation", "NUM_COMPTE_ENGAGEMENT", "image_contrat", "IDF_REF_EXT_CTR")
match("NUM_TIERS = TIERS.IDF_NUM_TIERS",
      "idce_autorisation", "NUM_TIERS", "tiers", "IDF_NUM_TIERS")

print("\n" + "=" * 105)
print("  3. HYPOTHÈSES — GARANTIE_CONTRAT.IDF_REF_EXT_CREDIT → ?  (ambiguïté n°6)")
print("=" * 105)
match("b) = CREDIT_AMORT.IDF_NUM_DOSS",
      "garantie_contrat", "IDF_REF_EXT_CREDIT", "credit_amort", "IDF_NUM_DOSS")
match("b') = CREDIT_AMORT.NUM_DOSS_CDL",
      "garantie_contrat", "IDF_REF_EXT_CREDIT", "credit_amort", "NUM_DOSS_CDL")
match("a') = CREDIT_AMORT.NUM_COMPTE_EMPRUNTEUR",
      "garantie_contrat", "IDF_REF_EXT_CREDIT", "credit_amort", "NUM_COMPTE_EMPRUNTEUR")
match("c) = IDCE_AUTORISATION.IDF_REF_AUTO",
      "garantie_contrat", "IDF_REF_EXT_CREDIT", "idce_autorisation", "IDF_REF_AUTO")
match("c') = IDCE_AUTORISATION.NUM_COMPTE",
      "garantie_contrat", "IDF_REF_EXT_CREDIT", "idce_autorisation", "NUM_COMPTE")
match("a'') = IMAGE_CONTRAT.IDF_REF_EXT_CTR (sans espaces)",
      "garantie_contrat", "regexp_replace(IDF_REF_EXT_CREDIT, '\\\\s', '')",
      "image_contrat", "regexp_replace(IDF_REF_EXT_CTR, '\\\\s', '')")
match("NUM_CTR_CREDIT = IMAGE_CONTRAT.IDF_REF_EXT_CTR",
      "garantie_contrat", "NUM_CTR_CREDIT", "image_contrat", "IDF_REF_EXT_CTR")
match("NUM_CTR_CREDIT = CREDIT_AMORT.IDF_NUM_DOSS",
      "garantie_contrat", "NUM_CTR_CREDIT", "credit_amort", "IDF_NUM_DOSS")

print("\n-- d) répartition COD_FILIERE_GES_CREDIT (la cible dépend-elle de la filière ?) --")
t("garantie_contrat").groupBy("COD_FILIERE_GES_CREDIT").count().orderBy(F.desc("count")).show(10, False)

print("=" * 105)
spark.stop()
