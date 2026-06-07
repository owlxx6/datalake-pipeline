"""Gold Engine — moteur générique déclaratif (config-driven).

Lit la couche Silver et construit la couche Gold (Iceberg, base `gold_*`) à partir
d'un YAML décrivant chaque cible. 4 types de cibles :

  - **date**      : dimension calendrier générée (dim_temps)
  - **dimension** : depuis une dim SCD2 Silver (état courant ou versionné)
  - **fact**      : depuis un fait snapshot Silver, écrit en snapshot par date_arrete
  - **obt**       : table large = fait de base + dims aplaties + faits 1:N agrégés

Le modèle (faits/dims/jointures) est de la donnée de config : un changement de modèle
n'impacte que le YAML, jamais ce moteur.

Usage:
    spark-submit jobs/gold/engine.py --config configs/gold/credit_garantie.yaml \\
        --date-arrete 2026-05-31 [--target gdw_encours]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from jobs.gold.config_loader import load_gold_config, GoldTarget


# ── Helpers ─────────────────────────────────────────────────────────────────────
def _norm_columns(columns) -> list[tuple[str, str]]:
    """Normalise les colonnes en (src, alias). Une entrée = str ou {src, as, ...}."""
    out: list[tuple[str, str]] = []
    for c in columns or []:
        if isinstance(c, str):
            out.append((c, c))
        elif isinstance(c, dict):
            src = c["src"]
            out.append((src, c.get("as", src)))
    return out


def _resolve(name: str, db: str) -> str:
    """Table pleinement qualifiée si elle contient un point, sinon préfixée par la base Gold."""
    return name if "." in name else f"{db}.{name}"


def _write(df: DataFrame, full_table: str, spark: SparkSession, partition: str | None = None) -> None:
    """Écriture Iceberg. partition → snapshot (overwritePartitions), sinon refresh complet."""
    if partition and spark.catalog.tableExists(full_table):
        df.writeTo(full_table).overwritePartitions()
    elif partition:
        df.writeTo(full_table).using("iceberg").partitionedBy(F.col(partition)).createOrReplace()
    else:
        df.writeTo(full_table).using("iceberg").createOrReplace()


# ── Builders par type ────────────────────────────────────────────────────────────
def build_date(spark: SparkSession, tgt: GoldTarget, db: str) -> None:
    r = tgt.range or {"from": "2020-01-01", "to": "2030-12-31"}
    df = spark.sql(
        f"SELECT explode(sequence(to_date('{r['from']}'), to_date('{r['to']}'), "
        f"interval 1 day)) AS date"
    )
    df = (
        df.withColumn("date_key", F.date_format("date", "yyyyMMdd").cast("int"))
          .withColumn("jour", F.dayofmonth("date"))
          .withColumn("mois", F.month("date"))
          .withColumn("trimestre", F.quarter("date"))
          .withColumn("annee", F.year("date"))
          .withColumn("lib_mois", F.date_format("date", "MMMM"))
          .select("date_key", "date", "jour", "mois", "trimestre", "annee", "lib_mois")
    )
    _write(df, f"{db}.{tgt.name}", spark)
    print(f"  [GOLD] ✓ dim date {db}.{tgt.name} : {df.count()} jours")


def build_dimension(spark: SparkSession, tgt: GoldTarget, db: str) -> None:
    df = spark.table(tgt.source)
    if tgt.mode == "scd2_current":
        df = df.where("is_current = true")
    # scd2_versioned / snapshot : on garde tel quel (versions incluses)
    cols = _norm_columns(tgt.columns)
    if cols:
        df = df.select([F.col(s).alias(a) for s, a in cols])
    _write(df, f"{db}.{tgt.name}", spark)
    print(f"  [GOLD] ✓ dimension {db}.{tgt.name} ({tgt.mode}) : {df.count()} lignes")


def build_fact(spark: SparkSession, tgt: GoldTarget, db: str, date_arrete: str) -> None:
    df = spark.table(tgt.source)
    if date_arrete:
        avail = [str(r[0]) for r in df.select("date_arrete").distinct().collect()]
        if date_arrete not in avail:
            print(f"  [WARN] date_arrete '{date_arrete}' absent de {tgt.source}. Dispo: {avail}")
        df = df.where(F.col("date_arrete") == F.lit(date_arrete))

    # Jointures as-of : gèle des attributs de dimension dans la version valide à l'arrêté
    if tgt.joins:
        df = df.alias("f")
        for j in tgt.joins:
            dim = spark.table(j["with"]).alias("d")
            cond = None
            for fc, dc in j["on"].items():
                c = F.col(f"f.{fc}") == F.col(f"d.{dc}")
                cond = c if cond is None else (cond & c)
            if j.get("as_of"):
                a = j["as_of"]
                cond = cond & (F.col("d.valid_from") <= F.col(f"f.{a}")) & (
                    F.col("d.valid_to").isNull() | (F.col("d.valid_to") > F.col(f"f.{a}"))
                )
            sel = j.get("select", [])
            df = df.join(dim, cond, "left").select("f.*", *[F.col(f"d.{c}").alias(c) for c in sel])
            df = df.alias("f")

    cols = _norm_columns(tgt.columns)
    aliases = [a for _, a in cols]
    selected = [F.col(s).alias(a) for s, a in cols]
    if "date_arrete" not in aliases:
        selected.append(F.col("date_arrete"))
    df_out = df.select(*selected)
    _write(df_out, f"{db}.{tgt.name}", spark, partition="date_arrete")
    print(f"  [GOLD] ✓ fait {db}.{tgt.name} (snapshot {date_arrete}) : {df_out.count()} lignes")


def build_obt(spark: SparkSession, tgt: GoldTarget, db: str, date_arrete: str) -> None:
    df = spark.table(_resolve(tgt.base, db))
    if date_arrete:
        df = df.where(F.col("date_arrete") == F.lit(date_arrete))

    # Aplatir des attributs de dimensions
    for j in tgt.joins or []:
        dim = spark.table(_resolve(j["with"], db))
        on = j["on"]                      # {col_base: col_dim}
        sel = j.get("select", [])
        keys = list(on.keys())
        dim_sel = (
            dim.select(
                *[F.col(dc).alias(bc) for bc, dc in on.items()],
                *[F.col(c) for c in sel],
            ).dropDuplicates(keys)
        )
        df = df.join(dim_sel, keys, "left")

    # Agréger les faits 1:N au grain de la base
    for agg in tgt.aggregates or []:
        sub = spark.table(_resolve(agg["from"], db))
        if date_arrete:
            sub = sub.where(F.col("date_arrete") == F.lit(date_arrete))
        gb = agg["group_by"]
        exprs = [F.expr(v).alias(k) for k, v in agg["measures"].items()]
        df = df.join(sub.groupBy(*gb).agg(*exprs), gb, "left")

    # Colonnes dérivées (KPI / flags)
    for name, expr in (tgt.derived or {}).items():
        df = df.withColumn(name, F.expr(expr))

    _write(df, f"{db}.{tgt.name}", spark, partition="date_arrete")
    print(f"  [GOLD] ✓ OBT {db}.{tgt.name} : {df.count()} lignes")


_BUILDERS = {
    "date": lambda s, t, db, d: build_date(s, t, db),
    "dimension": lambda s, t, db, d: build_dimension(s, t, db),
    "fact": lambda s, t, db, d: build_fact(s, t, db, d),
    "obt": lambda s, t, db, d: build_obt(s, t, db, d),
}


def main() -> None:
    parser = argparse.ArgumentParser(description="Gold Engine — moteur déclaratif")
    parser.add_argument("--config", required=True, help="YAML modèle Gold")
    parser.add_argument("--date-arrete", required=True, help="YYYY-MM-DD")
    parser.add_argument("--target", default=None, help="Une cible spécifique (optionnel)")
    args = parser.parse_args()

    config = load_gold_config(args.config)
    db = config.database

    targets = config.targets
    if args.target:
        targets = [t for t in targets if t.name == args.target]
        if not targets:
            raise ValueError(
                f"Cible '{args.target}' introuvable. "
                f"Disponibles : {[t.name for t in config.targets]}"
            )

    spark = (
        SparkSession.builder.appName("GOLD_credit_garantie")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.spark_catalog",
                "org.apache.iceberg.spark.SparkSessionCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hive")
        .enableHiveSupport()
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {db}")

    print(f"\n{'=' * 65}")
    print(f"  GOLD ENGINE  |  {db}")
    print(f"  Date arrêté  :  {args.date_arrete}")
    print(f"  Cibles       :  {[t.name for t in targets]}")
    print(f"{'=' * 65}")

    for tgt in targets:
        builder = _BUILDERS.get(tgt.type)
        if builder is None:
            raise ValueError(f"Type inconnu pour '{tgt.name}' : {tgt.type}")
        builder(spark, tgt, db, args.date_arrete)

    spark.stop()
    print("  [DONE] GOLD — SUCCESS")


if __name__ == "__main__":
    main()
