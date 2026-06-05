#!/usr/bin/env python3
"""Simule un 2e arrêté POPS (date_arrete=20260630) pour DÉMONTRER le SCD2.

Fabrique une variante de la photo du 2026-05-31 :
  - 3 produits MODIFIÉS  (cod_usage basculé F<->P : garantit un changement, reste EDD-valide)
  - 1 produit DISPARU    (retiré de la photo)
  - 1 produit NOUVEAU    (IDF_CODE_PRODUIT = ZZZNEW001)

Puis on lance l'engine sur 2026-06-30 → le MERGE SCD2 doit :
  modifiés → v1 clôturée + v2 ouverte · disparu → clôturé · nouveau → v1 · autres → inchangés

Usage (sur la VM) :
    spark-submit scripts/simulate_pops_arrete2.py
"""
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window

NEW_DATE = "20260630"
SRC = "hdfs://ubuntu3:9000/data/landing/pops/date_arrete=20260531"
DST = f"hdfs://ubuntu3:9000/data/landing/pops/date_arrete={NEW_DATE}"

spark = SparkSession.builder.appName("SIM_POPS_ARRETE2").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(SRC).withColumn("__rn", F.row_number().over(Window.orderBy("idf_code_produit")))

# Clés impactées (pour vérification ensuite)
impacted = df.filter(F.col("__rn").isin(1, 2, 3, 4, 5)) \
             .select("__rn", "idf_code_produit", "cod_usage").orderBy("__rn").collect()
print("\n=== Clés impactées ===")
for r in impacted:
    role = {1: "MODIF", 2: "MODIF", 3: "MODIF", 4: "DISPARU", 5: "(source du nouveau)"}[r["__rn"]]
    print(f"  rn={r['__rn']:<2} {role:<20} IDF_CODE_PRODUIT={r['idf_code_produit']}  cod_usage={r['cod_usage']}")
print("  NOUVEAU              IDF_CODE_PRODUIT=ZZZNEW001\n")

# 3 modifiés : bascule cod_usage F<->P (changement garanti, valeur EDD-valide)
df_mod = df.withColumn(
    "cod_usage",
    F.when(F.col("__rn").isin(1, 2, 3),
           F.when(F.col("cod_usage") == "F", F.lit("P")).otherwise(F.lit("F")))
     .otherwise(F.col("cod_usage"))
)
# 1 disparu : retirer rn == 4
df_mod = df_mod.filter(F.col("__rn") != 4)

# 1 nouveau : cloner rn == 5 avec un nouvel IDF_CODE_PRODUIT
new = df.filter(F.col("__rn") == 5).drop("__rn").withColumn("idf_code_produit", F.lit("ZZZNEW001"))

df_out = df_mod.drop("__rn").unionByName(new)
df_out.write.mode("overwrite").parquet(DST)
print(f"Écrit {df_out.count()} lignes → {DST}")
spark.stop()
