"""Diagnostic SCD2 POPS — identifie la colonne required qui reçoit un null.

À lancer sur la VM :
  cd /media/sf_datalake-pipeline && spark-submit --driver-memory 4g scripts/diag_scd2_pops.py

Affiche :
  1. Le schéma Iceberg de silver_credit_garantie.pops avec le flag nullable
  2. Pour chaque colonne NON nullable (required), le nombre de null réellement stockés
     → la/les colonne(s) avec count > 0 sont la cause du NullPointerException.
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder.appName("diag_scd2_pops")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.iceberg.spark.SparkSessionCatalog")
    .config("spark.sql.catalog.spark_catalog.type", "hive")
    .enableHiveSupport()
    .getOrCreate()
)

table = "spark_catalog.silver_credit_garantie.pops"
df = spark.table(table)

print("\n=== SCHÉMA (nullable=False => required Iceberg) ===")
required_cols = []
for f in df.schema.fields:
    flag = "NULLABLE" if f.nullable else "REQUIRED"
    print(f"  {f.name:30s} {str(f.dataType):20s} {flag}")
    if not f.nullable:
        required_cols.append(f.name)

print(f"\n=== Colonnes REQUIRED : {required_cols} ===")
print("=== Comptage des null stockés dans les colonnes required ===")
if required_cols:
    counts = df.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in required_cols]
    ).collect()[0].asDict()
    for c, n in counts.items():
        marker = "  <<< CAUSE NPE" if (n or 0) > 0 else ""
        print(f"  {c:30s} null={n}{marker}")
else:
    print("  (aucune colonne required — la cause est ailleurs)")

print("\n=== Répartition is_current / version ===")
df.groupBy("is_current").count().show()
spark.stop()
