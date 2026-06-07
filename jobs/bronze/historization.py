"""historization.py — Framework d'historisation de la couche Silver.

Point d'entrée unique : ``historize()``. Selon la config de la table, applique la
bonne stratégie d'historisation :

  - **snapshot** (flux transactionnel : faits, encours, montants) :
    écriture Iceberg partitionnée par ``date_arrete``. Chaque arrêté = un état figé ;
    ``overwritePartitions`` ne remplace que la partition du jour, les autres restent.

  - **SCD2** (flux référentiel & contractuel : dimensions) :
    table versionnée (valid_from / valid_to / is_current / version / row_hash).
    Le changement est détecté par empreinte ``row_hash`` sur les colonnes à historiser
    (``scd2_signal.columns`` ou, par défaut, toutes les colonnes métier du schéma).

Activation par table (YAML) ::

    silver_write:
      scd2: true                     # absent/false => snapshot
      scd2_signal:
        type: hash
        columns: [DUREE, TYPE_TAUX]  # optionnel : sous-ensemble contractuel

NB Iceberg 1.3.1 : le SCD2 n'utilise PAS MERGE/UPDATE copy-on-write (NullPointerException
sur la recopie des lignes inchangées). On reconstruit l'état complet en DataFrame puis on
réécrit via ``createOrReplace`` (écriture bulk, déterministe et portable).
"""
from __future__ import annotations

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from jobs.bronze.config_loader import BronzeConfig, TableEntry


def historize(
    df_silver: DataFrame, table: TableEntry, config: BronzeConfig,
    spark: SparkSession, date_arrete: str,
) -> int:
    """Dispatch unique : SCD2 (dimensions) ou snapshot (faits).

    Retourne le nombre de lignes écrites (snapshot) ou de nouvelles versions (SCD2).
    """
    sw = table.silver_write
    if sw is None:
        return 0
    if sw.scd2:
        return _write_scd2(df_silver, table, config, spark, date_arrete)
    return _write_snapshot(df_silver, table, config, spark, date_arrete)


# ── Stratégie snapshot (faits) ──────────────────────────────────────────────────
def _write_snapshot(
    df_flagged: DataFrame, table: TableEntry, config: BronzeConfig,
    spark: SparkSession, date_arrete: str,
) -> int:
    """Silver Iceberg snapshot — toutes les lignes + flags DQ, partitionné date_arrete.

    - Table existante : ``overwritePartitions()`` — remplace uniquement la partition
      ``date_arrete`` du jour ; les autres arrêtés restent intacts.
    - Sinon : création Iceberg partitionnée par ``date_arrete``.
    """
    sw = table.silver_write
    full_table = f"{sw.database}.{sw.table}"

    if config.execution.dry_run:
        count = df_flagged.count()
        print(f"  [DRY RUN] Silver snapshot (Iceberg) → {full_table} ({count} lignes)")
        return count

    df_to_write = df_flagged.withColumn("date_arrete", F.lit(date_arrete))

    if spark.catalog.tableExists(full_table):
        df_to_write.writeTo(full_table).overwritePartitions()
    else:
        (
            df_to_write.writeTo(full_table)
            .using("iceberg")
            .partitionedBy(F.col("date_arrete"))
            .createOrReplace()
        )

    count = df_flagged.count()
    print(f"  [SILVER] ✓ {count} lignes (Iceberg snapshot, avec flags DQ) → {full_table}")
    return count


# ── Stratégie SCD2 (dimensions) ─────────────────────────────────────────────────
def _compare_columns(table: TableEntry, df_silver: DataFrame) -> list[str]:
    """Colonnes utilisées pour détecter un changement (empreinte row_hash).

    ``scd2_signal.columns`` si fourni (sous-ensemble contractuel), sinon toutes les
    colonnes métier du schéma présentes dans le DataFrame.
    """
    signal = table.silver_write.scd2_signal
    business_cols = [f.name for f in table.schema.fields if f.name in df_silver.columns]
    if signal and signal.columns:
        cols = [c for c in signal.columns if c in df_silver.columns]
        return cols or business_cols
    return business_cols


def _write_scd2(
    df_silver: DataFrame, table: TableEntry, config: BronzeConfig,
    spark: SparkSession, date_arrete: str,
) -> int:
    """Silver SCD2 (dimensions) — table versionnée.

    Colonnes ajoutées : row_hash, valid_from, valid_to, is_current, version, load_ts.
    À chaque arrêté, comparaison photo du jour vs état courant (is_current=true) :
      - clé nouvelle              → INSERT version 1
      - clé connue, hash identique → rien
      - clé connue, hash différent → CLÔTURE l'ancienne + INSERT nouvelle version
      - clé disparue de la photo   → CLÔTURE (soft delete)
    """
    sw = table.silver_write
    pk = table.primary_key
    full_table = f"{sw.database}.{sw.table}"
    compare_cols = _compare_columns(table, df_silver)

    # Empreinte des attributs historisés (NULL-safe)
    row_hash = F.sha2(
        F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("§")) for c in compare_cols]),
        256,
    )
    base = (
        df_silver
        .withColumn("row_hash", row_hash)
        .withColumn("valid_to", F.lit(None).cast("string"))
        .withColumn("is_current", F.lit(True))
        .withColumn("load_ts", F.col("ingestion_ts"))
    )

    if config.execution.dry_run:
        n = base.count()
        print(f"  [DRY RUN] Silver SCD2 → {full_table} ({n} lignes en entrée)")
        return n

    # 1er chargement : création, tout en version 1
    if not spark.catalog.tableExists(full_table):
        df_init = base.withColumn("valid_from", F.lit(date_arrete)).withColumn("version", F.lit(1))
        df_init.writeTo(full_table).using("iceberg").createOrReplace()
        n = df_init.count()
        print(f"  [SCD2] ✓ création {full_table} : {n} lignes (version 1)")
        return n

    # Chargements suivants : réécriture DataFrame déterministe (pas de MERGE/UPDATE
    # copy-on-write : NullPointerException sur Iceberg 1.3.1). On reconstruit l'état
    # complet puis on réécrit via createOrReplace (écriture bulk, gère les null).
    existing = spark.table(full_table)
    current = existing.where("is_current = true")
    history = existing.where("is_current = false OR is_current IS NULL")

    # Comparaison clé par clé entre l'état courant et la photo du jour
    inc_hash = base.select(*pk, F.col("row_hash").alias("__new_hash"))
    cur_cmp = current.join(inc_hash, pk, "left")

    # Lignes courantes à CLÔTURER : clé disparue (__new_hash null) ou hash changé
    closed = (
        cur_cmp.where("__new_hash IS NULL OR __new_hash <> row_hash")
               .withColumn("is_current", F.lit(False))
               .withColumn("valid_to", F.lit(date_arrete))
               .drop("__new_hash")
    )
    # Lignes courantes INCHANGÉES : conservées telles quelles
    kept = cur_cmp.where("__new_hash IS NOT NULL AND __new_hash = row_hash").drop("__new_hash")

    # NOUVELLES versions : clés nouvelles + clés au hash changé (version = max + 1)
    cur_hash = current.select(*pk, F.col("row_hash").alias("__cur_hash"))
    max_ver = existing.groupBy(*pk).agg(F.max("version").alias("__maxv"))
    new_versions = (
        base.join(cur_hash, pk, "left")
            .where("__cur_hash IS NULL OR __cur_hash <> row_hash")
            .drop("__cur_hash")
            .join(max_ver, pk, "left")
            .withColumn("version", F.coalesce(F.col("__maxv"), F.lit(0)) + F.lit(1))
            .withColumn("valid_from", F.lit(date_arrete))
            .drop("__maxv")
    )

    # Table finale = historique + clôturées + inchangées + nouvelles versions
    final = (
        history
        .unionByName(closed)
        .unionByName(kept)
        .unionByName(new_versions)
    )
    # Matérialiser AVANT d'écraser la table (on lit et on réécrit la même table).
    final = final.persist()
    n_total = final.count()
    n_new = final.where(
        (F.col("valid_from") == F.lit(date_arrete)) & (F.col("is_current") == F.lit(True))
    ).count()
    final.writeTo(full_table).using("iceberg").createOrReplace()
    final.unpersist()
    print(f"  [SCD2] ✓ {n_new} nouvelle(s) version(s) — {n_total} lignes au total → {full_table}")
    return n_new
