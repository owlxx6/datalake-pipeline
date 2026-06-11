"""Charge l'échantillon ORG (CSV, converti depuis ORG.xlsx) en Landing Parquet.

Usage (sur la VM) :
    spark-submit scripts/load_org_landing.py
"""
from pyspark.sql import SparkSession

SRC = "file:///media/sf_datalake-pipeline/echantillon/org.csv"
DST = "hdfs://ubuntu3:9000/data/landing/org/date_arrete=20260531"

spark = SparkSession.builder.appName("LOAD_ORG_LANDING").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

df = (spark.read
      .option("header", True)
      .option("delimiter", ";")
      .option("encoding", "UTF-8")
      .csv(SRC))
print(f"Lu : {df.count()} lignes, {len(df.columns)} colonnes")
df.coalesce(1).write.mode("overwrite").parquet(DST)
print(f"Écrit → {DST}")
spark.stop()
