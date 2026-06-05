"""Bronze Engine — moteur générique couche Bronze (YAML groupé multi-tables).

Usage:
    # Toutes les tables du domaine
    spark-submit jobs/bronze/engine.py \\
        --config configs/bronze/credit_garantie.yaml \\
        --date-arrete 2026-05-04

    # Une seule table
    spark-submit jobs/bronze/engine.py \\
        --config configs/bronze/credit_garantie.yaml \\
        --date-arrete 2026-05-04 \\
        --table TIERS

Architecture des sorties (post 2026-05-21) :
  - Bronze HDFS  : miroir typé (Parquet), toutes les lignes, sans flag DQ.
  - Silver Hive  : toutes les lignes + 4 colonnes flag DQ. Aucune ligne n'est
                   filtrée — on flagge, on n'enlève pas.
  - Silver Iceberg (append-only) : 2 tables d'audit
      * QUALITY_RESULTS — 1 ligne par règle par exécution
      * LOG_PIPELINE    — 1 ligne par exécution
"""
from __future__ import annotations

import argparse
import re
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

# Permet de lancer le script directement via spark-submit sans configurer PYTHONPATH.
# Ajoute la racine du projet (parent de `jobs/`) au sys.path pour que l'import
# `from jobs.bronze.config_loader` fonctionne.
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, LongType, FloatType,
    DoubleType, DateType, TimestampType, DecimalType, BooleanType,
)
from pyspark.sql.window import Window

from jobs.bronze.config_loader import (
    load_config, BronzeConfig, TableEntry, DQCheck, CsvOptions,
)


# Mapping sévérité → poids (1 = plus sévère)
SEVERITY_WEIGHT = {"CRITICAL": 1, "HIGH": 2, "MEDIUM": 3, "LOW": 4}


# ── Type resolver ─────────────────────────────────────────────────────────────

def _spark_type(type_str: str):
    type_str = type_str.strip()
    if type_str.startswith("DecimalType"):
        m = re.match(r"DecimalType\((\d+)(?:,\s*(\d+))?\)", type_str)
        p = int(m.group(1)) if m else 18
        s = int(m.group(2)) if m and m.group(2) else 0
        return DecimalType(p, s)
    return {
        "StringType": StringType(), "IntegerType": IntegerType(),
        "LongType": LongType(), "FloatType": FloatType(),
        "DoubleType": DoubleType(), "DateType": DateType(),
        "TimestampType": TimestampType(), "BooleanType": BooleanType(),
    }.get(type_str, StringType())


# ── Smart cast (normalisation formats fournisseurs) ──────────────────────────

# Patterns de dates rencontrés dans les extracts (CSV/Parquet) :
#   - yyyy-MM-dd HH:mm:ss.SSSSSS  (la plupart des CSV : tiers, garantie, idce…)
#   - yyyy-MM-dd HH:mm:ss
#   - yyyy-MM-dd
#   - dd/MM/yyyy HH:mm:ss          (DATE_CHARGEMENT, DATE_FICHIER : asal, pops, image_contrat)
#   - dd/MM/yyyy
_DATE_PATTERNS = [
    "yyyy-MM-dd HH:mm:ss.SSSSSS",
    "yyyy-MM-dd HH:mm:ss",
    "yyyy-MM-dd",
    "dd/MM/yyyy HH:mm:ss",
    "dd/MM/yyyy",
]


def _smart_cast(col_expr, target_type):
    """Cast tolérant aux formats fournisseurs (décimal européen, dates multi-format).

    - DecimalType / FloatType / DoubleType : nettoie le format européen
      ``+000000000700000,000`` → ``700000.000`` avant le cast.
    - DateType / TimestampType : essaie plusieurs patterns ; ``0001-01-01`` est
      traité comme NULL (sentinelle source pour "date absente").
      ``9999-12-31`` est conservé tel quel (sentinelle "date ouverte").
    - Autres types : cast standard.
    """
    if isinstance(target_type, (DecimalType, FloatType, DoubleType)):
        s = col_expr.cast(StringType())
        s = F.regexp_replace(s, r"^\s*\+", "")   # +123 → 123
        s = F.regexp_replace(s, r"\s", "")       # retire espaces
        s = F.regexp_replace(s, r",", ".")       # virgule décimale → point
        return s.cast(target_type)

    if isinstance(target_type, (DateType, TimestampType)):
        s = col_expr.cast(StringType())
        # Sentinelle "0001-01-01..." (souvent suivie d'une heure) → NULL logique
        s = F.when(s.startswith("0001-01-01"), F.lit(None).cast(StringType())).otherwise(s)
        parsed = F.coalesce(*[F.to_timestamp(s, p) for p in _DATE_PATTERNS])
        return F.to_date(parsed) if isinstance(target_type, DateType) else parsed

    return col_expr.cast(target_type)


# ── Schema Enforcer ───────────────────────────────────────────────────────────

def enforce_schema(df: DataFrame, table: TableEntry) -> DataFrame:
    """Cast chaque colonne vers son type déclaré dans le YAML.

    Utilise ``_smart_cast`` pour absorber les formats fournisseurs (décimal européen,
    dates multi-format). Aucune ligne n'est filtrée — un cast en échec produit NULL,
    qui sera flaggé ensuite par les règles DQ (not_null sur les champs critiques).
    """
    df_typed = df
    for field in table.schema.fields:
        if field.name not in df.columns:
            continue
        target_type = _spark_type(field.spark_type)
        df_typed = df_typed.withColumn(field.name, _smart_cast(F.col(field.name), target_type))
    return df_typed


# ── Reader (Parquet / CSV avec encodage) ──────────────────────────────────────

def _normalize_columns(df: DataFrame) -> DataFrame:
    """Aligne les noms de colonnes source sur la convention du YAML.

    - Supprime les colonnes techniques internes NiFi (préfixe ``_`` :
      ``_source_file``, ``_ingestion_ts``, ``_date_arrete``). La traçabilité est
      reconstruite côté engine par ``add_ingestion_metadata``.
    - Met tous les noms restants en MAJUSCULES : NiFi écrit le Parquet en
      minuscules (``idf_num_doss``) alors que le YAML (schema, DQ, PK,
      consistency) référence les champs en MAJUSCULES (``IDF_NUM_DOSS``).
      Normaliser ici aligne tout le pipeline en un seul point.
    """
    keep = [c for c in df.columns if not c.startswith("_")]
    df = df.select(*keep)
    return df.toDF(*[c.upper() for c in df.columns])


def read_source(spark: SparkSession, table: TableEntry, source_path: str) -> DataFrame:
    """Lit la source Landing selon le format déclaré.

    CSV : applique encoding, délimiteur, quote, escape depuis ``csv_options``.
    Parquet : lecture standard.

    Dans les deux cas, les noms de colonnes sont normalisés en MAJUSCULES et les
    colonnes techniques NiFi (préfixe ``_``) sont retirées (cf. ``_normalize_columns``).
    """
    fmt = table.source.format.lower()
    if fmt == "csv":
        opts = table.source.csv_options or CsvOptions()
        df = (
            spark.read
            .option("header", str(opts.header).lower())
            .option("delimiter", opts.delimiter)
            .option("encoding", opts.encoding)
            .option("quote", opts.quote)
            .option("escape", opts.escape)
            .option("multiLine", str(opts.multiline).lower())
            .csv(source_path)
        )
    else:
        df = spark.read.parquet(source_path)
    return _normalize_columns(df)


# ── Traçabilité d'ingestion (colonnes ajoutées sur chaque ligne) ─────────────

def add_ingestion_metadata(
    df: DataFrame, source_path: str, execution_id: str,
) -> DataFrame:
    """Ajoute 3 colonnes de traçabilité technique sur chaque ligne :
      - execution_id  : identifiant unique du run (lien vers LOG_PIPELINE)
      - source_file   : chemin HDFS du fichier source
      - ingestion_ts  : timestamp Spark au moment du traitement

    Appliquées en amont de write_bronze : Bronze et Silver portent ces colonnes,
    indépendamment du fait que la source les fournisse (cf. inconsistance entre
    asal/pops/image_contrat qui ont COD_ENTITE/DATE_CHARGEMENT et tiers/credit_amort
    qui ne les ont pas).
    """
    return (
        df
        .withColumn("execution_id", F.lit(execution_id))
        .withColumn("source_file", F.lit(source_path))
        .withColumn("ingestion_ts", F.current_timestamp())
    )


# ── Résolution date_arrete depuis la donnée ──────────────────────────────────

def resolve_date_arrete(df_raw: DataFrame, table: TableEntry, cli_date_arrete: str) -> str:
    """Si ``source.date_arrete_column`` est configurée, lit la date dans la donnée.

    Permet de dériver la partition ``date_arrete`` de ``DATE_FICHIER`` (entête du
    fichier porté sur chaque ligne) plutôt que de la passer en CLI. Fallback sur
    l'argument CLI si la colonne est absente / vide / non parseable.
    """
    col = table.source.date_arrete_column
    if not col or col not in df_raw.columns:
        return cli_date_arrete

    row = df_raw.select(col).limit(1).collect()
    if not row or row[0][0] is None:
        return cli_date_arrete

    raw = str(row[0][0]).strip()
    date_part = raw.split(" ")[0]  # ignore la partie heure si présente

    for in_fmt in ("%Y-%m-%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(date_part, in_fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    print(f"  [WARN] {col}='{raw}' non parseable, fallback CLI date_arrete={cli_date_arrete}")
    return cli_date_arrete


# ── DQ check condition ────────────────────────────────────────────────────────

def _check_condition(col_name: str, check: DQCheck):
    """Retourne une expression Column qui vaut TRUE si le check est en échec."""
    col = F.col(col_name)

    if check.type == "not_null":
        # Une chaîne vide (ou whitespace) est sémantiquement "non renseigné" — on la
        # traite comme NULL. Permet de flagger les valeurs manquantes par not_null,
        # indépendamment de la forme (NULL strict vs '' vs '   ').
        return col.isNull() | (F.trim(col.cast(StringType())) == "")
    elif check.type == "unique":
        w = Window.partitionBy(col_name)
        return (F.count(col_name).over(w) > 1) & col.isNotNull()
    elif check.type == "in_list" and check.allowed_values:
        stripped = [v.strip() for v in check.allowed_values]
        # Les chaînes vides sont sémantiquement "non renseigné" (équivalent NULL).
        # On les exclut du check in_list — c'est le rôle de not_null de les flagger.
        return (
            ~F.trim(col).isin(stripped)
            & col.isNotNull()
            & (F.trim(col) != "")
        )
    elif check.type == "max_length" and check.max_length:
        return F.length(col.cast(StringType())) > check.max_length
    elif check.type == "regex" and check.pattern:
        return ~col.rlike(check.pattern) & col.isNotNull()
    elif check.type == "date_valid":
        return F.to_date(col.cast(StringType())).isNull() & col.isNotNull()
    elif check.type == "date_range":
        conds = []
        if check.min_date:
            conds.append(col < F.lit(check.min_date).cast(DateType()))
        if check.max_date_expr:
            conds.append(col > F.expr(check.max_date_expr))
        if conds:
            result = conds[0]
            for c in conds[1:]:
                result = result | c
            return result & col.isNotNull()
    return None


# ── DQ statistiques par règle (pour QUALITY_RESULTS) ─────────────────────────

def run_dq(
    df: DataFrame, table: TableEntry, config: BronzeConfig,
    execution_id: str, date_situation: str,
) -> list[dict]:
    """Calcule les agrégats DQ par règle (records_failed, taux, statut).

    Ne filtre AUCUNE ligne. Retourne uniquement la liste des résultats pour
    QUALITY_RESULTS. Les flags ligne par ligne sont calculés séparément par
    compute_dq_flags.
    """
    results: list[dict] = []

    # quality_rules
    for rule in table.quality_rules:
        col_name = rule.source_column
        if col_name not in df.columns:
            continue

        df_scope = df.filter(rule.filter_condition) if rule.filter_condition else df
        scope_count = df_scope.count()

        for check in rule.checks:
            rule_id = f"DQ_{rule.field}_{check.type}".upper()

            if check.type == "unique":
                _tmp = "__dup_count__"
                failed = (
                    df_scope
                    .withColumn(_tmp, F.count(col_name).over(Window.partitionBy(col_name)))
                    .filter(F.col(_tmp) > 1)
                    .count()
                )
            else:
                condition = _check_condition(col_name, check)
                if condition is None:
                    continue
                failed = df_scope.filter(condition).count()

            rate = (failed / scope_count * 100) if scope_count > 0 else 0.0
            results.append(_qr_row(
                execution_id, date_situation, rule_id, check.type,
                col_name, check.severity, scope_count, failed, rate,
                check.alert_threshold, check.control_type, config, table,
            ))

    # consistency_rules
    for rule in table.consistency_rules:
        rule_id = f"DQ_CONSISTENCY_{rule.name}".upper()
        df_scope = df.filter(rule.condition)
        scope_count = df_scope.count()

        null_conds = [F.col(f).isNull() for f in rule.required_fields if f in df.columns]
        if not null_conds:
            continue

        condition = null_conds[0]
        for c in null_conds[1:]:
            condition = condition | c

        fields_str = ", ".join(rule.required_fields)
        failed = df_scope.filter(condition).count()
        rate = (failed / scope_count * 100) if scope_count > 0 else 0.0

        results.append(_qr_row(
            execution_id, date_situation, rule_id, "consistency",
            fields_str, rule.severity, scope_count, failed, rate,
            rule.alert_threshold, rule.control_type, config, table,
        ))

    return results


def _qr_row(
    execution_id, date_situation, rule_id, rule_type, field_name,
    severity, scope_count, failed, rate, threshold, control_type,
    config: BronzeConfig, table: TableEntry,
) -> dict:
    return {
        "execution_id": execution_id,
        "date_situation": date_situation,
        "rule_id": rule_id,
        "rule_type": rule_type,
        "field_name": field_name,
        "severity": severity,
        "records_checked": scope_count,
        "records_failed": failed,
        "failure_rate_pct": round(rate, 4),
        "alert_threshold_pct": threshold,
        "status": "PASSED" if rate <= threshold else "FAILED",
        "control_type": control_type,
        "layer": "BRONZE",
        "table_name": table.name,
        "domain": config.project.domain,
        "environment": config.project.environment,
    }


# ── Flag DQ par ligne (pour Silver) ───────────────────────────────────────────

def compute_dq_flags(
    df_typed: DataFrame, table: TableEntry, execution_id: str,
) -> DataFrame:
    """Ajoute 4 colonnes flag DQ à chaque ligne de df_typed.

      - dq_failed_checks  : array<string> des règles échouées (ex. ["COD_CYC_VIE:in_list"])
      - dq_status         : "PASSED" (aucun défaut) / "WARNING" (défaut HIGH/MEDIUM/LOW,
                            ligne utilisable) / "REJECTED" (défaut CRITICAL sur PK, ligne
                            inutilisable). Piloté par la gravité, pas la présence d'un défaut.
      - dq_severity_max   : sévérité max parmi les règles échouées (NULL si PASSED)
      - dq_execution_id   : lien vers LOG_PIPELINE
    """
    fail_exprs: list = []
    weight_exprs: list = []

    # Note : la dédup Silver ne retire que les doublons PARFAITS (cf. deduplicate_silver),
    # et elle s'exécute AVANT ce calcul. Le check `unique` ne voit donc plus que les
    # vraies collisions de PK (même PK, valeurs différentes) → il les flague REJECTED.

    # quality_rules
    for rule in table.quality_rules:
        col_name = rule.source_column
        if col_name not in df_typed.columns:
            continue
        scope_expr = F.expr(rule.filter_condition) if rule.filter_condition else F.lit(True)
        for check in rule.checks:
            # Les checks informational sont mesurés dans QUALITY_RESULTS (run_dq) mais
            # n'influencent PAS le statut de la ligne — on les saute ici.
            if check.informational:
                continue
            check_id = f"{rule.field}:{check.type}"
            if check.type == "unique":
                cond = (F.count(col_name).over(Window.partitionBy(col_name)) > 1) & F.col(col_name).isNotNull()
            else:
                cond = _check_condition(col_name, check)
                if cond is None:
                    continue
            failed = cond & scope_expr
            fail_exprs.append(F.when(failed, F.lit(check_id)))
            weight_exprs.append(F.when(failed, F.lit(SEVERITY_WEIGHT.get(check.severity, 4))))

    # consistency_rules
    for rule in table.consistency_rules:
        scope_expr = F.expr(rule.condition)
        null_conds = [F.col(f).isNull() for f in rule.required_fields if f in df_typed.columns]
        if not null_conds:
            continue
        any_null = null_conds[0]
        for c in null_conds[1:]:
            any_null = any_null | c
        check_id = f"CONSISTENCY:{rule.name}"
        failed = any_null & scope_expr
        fail_exprs.append(F.when(failed, F.lit(check_id)))
        weight_exprs.append(F.when(failed, F.lit(SEVERITY_WEIGHT.get(rule.severity, 4))))

    # Aucune règle DQ — flags neutres
    if not fail_exprs:
        return (
            df_typed
            .withColumn("dq_failed_checks", F.array().cast("array<string>"))
            .withColumn("dq_status", F.lit("PASSED"))
            .withColumn("dq_severity_max", F.lit(None).cast(StringType()))
            .withColumn("dq_execution_id", F.lit(execution_id))
        )

    # Construire les arrays bruts (avec NULL pour les checks non-échoués), puis filtrer
    df_out = (
        df_typed
        .withColumn("__dq_failed_raw", F.array(*fail_exprs))
        .withColumn("__dq_weights_raw", F.array(*weight_exprs))
    )
    df_out = (
        df_out
        .withColumn("dq_failed_checks", F.expr("filter(__dq_failed_raw, x -> x is not null)"))
        .withColumn("__dq_weights", F.expr("filter(__dq_weights_raw, x -> x is not null)"))
        .drop("__dq_failed_raw", "__dq_weights_raw")
    )
    df_out = (
        df_out
        .withColumn("__dq_min_weight", F.array_min("__dq_weights"))
        # dq_status piloté par la GRAVITÉ, pas par la simple présence d'un défaut :
        #   - aucun défaut             → PASSED   (ligne parfaite)
        #   - défaut CRITICAL (PK)     → REJECTED (ligne inutilisable : PK nulle/dupliquée)
        #   - défaut HIGH/MEDIUM/LOW   → WARNING  (ligne utilisable, défauts qualité tracés)
        .withColumn(
            "dq_status",
            F.when(F.size("dq_failed_checks") == 0, F.lit("PASSED"))
             .when(F.col("__dq_min_weight") == 1, F.lit("REJECTED"))
             .otherwise(F.lit("WARNING"))
        )
        .withColumn(
            "dq_severity_max",
            F.when(F.col("__dq_min_weight") == 1, F.lit("CRITICAL"))
             .when(F.col("__dq_min_weight") == 2, F.lit("HIGH"))
             .when(F.col("__dq_min_weight") == 3, F.lit("MEDIUM"))
             .when(F.col("__dq_min_weight") == 4, F.lit("LOW"))
             .otherwise(F.lit(None).cast(StringType()))
        )
        .withColumn("dq_execution_id", F.lit(execution_id))
        .drop("__dq_weights", "__dq_min_weight")
    )
    return df_out


# ── Writers ───────────────────────────────────────────────────────────────────

def write_bronze(
    df: DataFrame, table: TableEntry, config: BronzeConfig,
    date_arrete: str,
) -> int:
    """Bronze HDFS — miroir typé (Parquet), aucune colonne flag.

    Écriture path-based : chaque date_arrete a son propre répertoire,
    donc `mode=overwrite` n'efface que la partition du jour.
    """
    path = config.resolve(
        table.write.path_template,
        table_name=table.name,
        domain=table.domain,
        date_arrete=date_arrete.replace("-", ""),
    )

    if config.execution.dry_run:
        count = df.count()
        print(f"  [DRY RUN] Bronze → {path} ({count} lignes)")
        return count

    (
        df.withColumn("date_arrete", F.lit(date_arrete))
        .coalesce(table.write.coalesce)
        .write
        .format("parquet")
        .mode(table.write.mode)
        .option("compression", table.write.compression)
        .save(path)
    )
    count = df.count()
    print(f"  [BRONZE] ✓ {count} lignes → {path}")
    return count


def deduplicate_silver(df_typed: DataFrame, table: TableEntry) -> tuple[DataFrame, int]:
    """Dédup Silver — retire UNIQUEMENT les doublons PARFAITS (opt-in deduplicate).

    Un doublon parfait = deux lignes identiques sur **toutes les colonnes métier**
    (les colonnes du schéma). On en garde une seule : aucune information n'est perdue.

    Les **collisions de PK** (même PK mais valeurs différentes ailleurs) ne sont PAS
    des doublons → elles sont **conservées** et seront flaguées par le check `unique`
    (REJECTED) au lieu d'être jetées en silence.

    Bronze garde toutes les lignes (doublons parfaits inclus) → historisation.
    À appeler AVANT le calcul des flags DQ : ainsi `unique` ne voit plus que les
    vraies collisions.

    Retourne (df_dédupliqué, nb_doublons_parfaits_retirés).
    """
    sw = table.silver_write
    if sw is None or not sw.deduplicate:
        return df_typed, 0

    # Colonnes métier = schéma source (on ignore les colonnes techniques constantes
    # execution_id / source_file / ingestion_ts, identiques au sein d'un run).
    business_cols = [f.name for f in table.schema.fields if f.name in df_typed.columns]
    if not business_cols:
        return df_typed, 0

    before = df_typed.count()
    df_dedup = df_typed.dropDuplicates(business_cols)
    removed = before - df_dedup.count()
    return df_dedup, removed


def write_silver(
    df_flagged: DataFrame, table: TableEntry, config: BronzeConfig,
    spark: SparkSession, date_arrete: str,
) -> int:
    """Silver Iceberg — toutes les lignes + colonnes flag DQ.

    Format Apache Iceberg (données Parquet sur HDFS, métadonnées Hive Metastore) :
    active le time-travel (``... TIMESTAMP AS OF``) et prépare le MERGE INTO du SCD2.

    - Table existante : ``overwritePartitions()`` (Iceberg dynamique) — remplace
      uniquement la/les partition(s) ``date_arrete`` présentes dans la donnée du jour ;
      les autres arrêtés restent intacts (snapshot par arrêté).
    - Sinon : création Iceberg partitionnée par ``date_arrete``.

    NB migration : les anciennes tables Silver en Hive Parquet doivent être
    supprimées (DROP) une fois avant le premier run Iceberg.
    """
    sw = table.silver_write
    if sw is None:
        return 0

    full_table = f"{sw.database}.{sw.table}"

    if config.execution.dry_run:
        count = df_flagged.count()
        print(f"  [DRY RUN] Silver (Iceberg) → {full_table} ({count} lignes)")
        return count

    df_to_write = df_flagged.withColumn("date_arrete", F.lit(date_arrete))

    if spark.catalog.tableExists(full_table):
        # Remplace uniquement la partition de l'arrêté du jour (les autres dates restent)
        df_to_write.writeTo(full_table).overwritePartitions()
    else:
        (
            df_to_write.writeTo(full_table)
            .using("iceberg")
            .partitionedBy(F.col("date_arrete"))
            .createOrReplace()
        )

    count = df_flagged.count()
    print(f"  [SILVER] ✓ {count} lignes (Iceberg, avec flags DQ) → {full_table}")
    return count


def write_silver_scd2(
    df_silver: DataFrame, table: TableEntry, config: BronzeConfig,
    spark: SparkSession, date_arrete: str,
) -> int:
    """Silver SCD2 (dimensions) — la table devient versionnée.

    Colonnes ajoutées : row_hash, valid_from, valid_to, is_current, version, load_ts.
    À chaque arrêté, on compare la photo du jour à l'état courant (is_current=true) :
      - clé nouvelle              → INSERT version 1
      - clé connue, hash identique → rien
      - clé connue, hash différent → CLÔTURE l'ancienne + INSERT nouvelle version
      - clé disparue de la photo   → CLÔTURE (soft delete)

    Famille hash (POPS, ASAL…) : row_hash = sha2 des attributs métier. Les familles
    date_maj / interval réutilisent la même mécanique (signal de comparaison différent).
    Toutes les versions sont conservées (Iceberg + time-travel en plus).
    """
    sw = table.silver_write
    pk = table.primary_key
    full_table = f"{sw.database}.{sw.table}"
    business_cols = [f.name for f in table.schema.fields if f.name in df_silver.columns]

    # Empreinte des attributs métier (NULL-safe)
    row_hash = F.sha2(
        F.concat_ws("|", *[F.coalesce(F.col(c).cast("string"), F.lit("§")) for c in business_cols]),
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

    # Chargements suivants : SCD2 par réécriture DataFrame déterministe.
    # On évite MERGE/UPDATE copy-on-write : sur Iceberg 1.3.1 ils déclenchent un
    # NullPointerException (UTF8String null) lors de la recopie des lignes inchangées.
    # On reconstruit l'état complet de la table puis on l'écrit via createOrReplace
    # (écriture bulk, qui gère correctement les null — comme à la création).
    existing = spark.table(full_table)
    current = existing.where("is_current = true")
    history = existing.where("is_current = false OR is_current IS NULL")

    # Comparaison clé par clé entre l'état courant et la photo du jour (base)
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

    # Table finale = historique + clôturées + inchangées + nouvelles versions.
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


def write_logs(
    spark: SparkSession, quality_results: list[dict], config: BronzeConfig,
    execution_id: str, date_situation: str,
    job_status: str, rows_read: int, rows_written: int,
    duration_seconds: float, table_name: str,
    source_file: str = "", cli_date_arrete: str = "",
    rows_flagged_failed: int = 0,
) -> None:
    if config.execution.dry_run:
        print(f"  [DRY RUN] write_logs skipped ({len(quality_results)} résultats DQ + 1 entrée LOG_PIPELINE)")
        return

    """Écrit QUALITY_RESULTS + LOG_PIPELINE en Iceberg (append-only).

    LOGS_REJECTED supprimée (2026-05-21) — l'info ligne par ligne vit
    dans Silver via les colonnes dq_*.
    """
    db = config.output.database
    env = config.project.environment

    # QUALITY_RESULTS
    if quality_results:
        qr_table = f"{db}.{config.resolve(config.output.quality_results.name_template, table_name=table_name)}"
        (
            spark.createDataFrame(quality_results)
            .withColumn("environment", F.lit(env))
            .write.format("iceberg").mode("append")
            .partitionBy("environment", "date_situation")
            .saveAsTable(qr_table)
        )
        print(f"  [SINK] {len(quality_results)} résultats DQ → {qr_table}")

    # LOG_PIPELINE
    lp_table = f"{db}.{config.resolve(config.output.log_pipeline.name_template, table_name=table_name)}"
    (
        spark.createDataFrame([{
            "execution_id": execution_id,
            "layer": "BRONZE",
            "table_name": table_name,
            "domain": config.project.domain,
            "environment": env,
            "date_situation": date_situation,
            "cli_date_arrete": cli_date_arrete,
            "source_file": source_file,
            "job_status": job_status,
            "rows_read": rows_read,
            "rows_written": rows_written,
            "rows_flagged_failed": rows_flagged_failed,
            "duration_seconds": round(duration_seconds, 2),
            "execution_ts": datetime.now().isoformat(),
        }])
        .withColumn("environment", F.lit(env))
        .write.format("iceberg").mode("append")
        .partitionBy("environment")
        .saveAsTable(lp_table)
    )
    print(f"  [SINK] Pipeline log → {lp_table}")


# ── DQ Report (stdout) ────────────────────────────────────────────────────────

def print_report(results: list[dict], table_name: str) -> None:
    print(f"\n  {'─' * 60}")
    print(f"  RAPPORT DQ — {table_name}")
    print(f"  {'─' * 60}")
    for r in results:
        icon = "✓" if r["status"] == "PASSED" else "✗"
        print(
            f"  [{icon}] {r['rule_id']:<40} "
            f"{r['records_failed']:>6} échecs "
            f"({r['failure_rate_pct']:.2f}%) [{r['severity']}]"
        )
    failed = sum(1 for r in results if r["status"] == "FAILED")
    print(f"  {'─' * 60}")
    print(f"  {len(results)} règles | {failed} en échec\n")


# ── Pipeline par table ────────────────────────────────────────────────────────

def run_table(
    table: TableEntry, config: BronzeConfig,
    date_arrete: str, spark: SparkSession,
) -> dict:
    """Exécute le pipeline Bronze pour une table.

    Flux (post 2026-05-21) :
      1. Lecture Landing (Parquet ou CSV avec encodage)
      1bis. Résolution date_arrete depuis la donnée (DATE_FICHIER) si configurée
      2. enforce_schema     (cast tolérant : échecs → NULL, aucune ligne perdue)
      3. run_dq             → stats par règle pour QUALITY_RESULTS
      4. compute_dq_flags   → 4 colonnes flag par ligne
      5. write_bronze       (df typé, sans flag)
      6. write_silver       (df flaggé, toutes les lignes)
      7. write_logs         (QUALITY_RESULTS + LOG_PIPELINE)
    """
    start = time.monotonic()
    cli_date_arrete = date_arrete   # mémorisé pour le chemin source + fallback
    execution_id = config.resolve(
        config.project.execution_id_template,
        table_name=table.name,
        date_arrete=cli_date_arrete.replace("-", ""),
    )

    print(f"\n{'─' * 65}")
    print(f"  TABLE : {table.name}  |  {execution_id}")
    print(f"{'─' * 65}")

    job_status = "FAILED"
    rows_read = rows_written = rows_silver = rows_flagged_failed = 0
    quality_results: list[dict] = []
    date_situation = cli_date_arrete

    try:
        # 1. Lecture (Parquet par défaut, CSV avec encodage si configuré)
        source_path = config.resolve(
            table.source.path_template,
            table_name=table.name,
            date_arrete=cli_date_arrete.replace("-", ""),
        )
        print(f"  [SOURCE] {source_path}  (format={table.source.format})")
        df_raw = read_source(spark, table, source_path)
        rows_read = df_raw.count()
        print(f"  [SOURCE] {rows_read} lignes lues")

        if config.performance.cache_source_data:
            df_raw.cache()

        # 1bis. date_arrete dérivée de la donnée si une colonne business est configurée
        date_arrete = resolve_date_arrete(df_raw, table, cli_date_arrete)
        date_situation = date_arrete
        if date_arrete != cli_date_arrete:
            print(f"  [DATE] {table.source.date_arrete_column} → date_arrete={date_arrete} "
                  f"(CLI={cli_date_arrete})")

        # 2. Schema enforcement (cast tolérant — aucune ligne perdue)
        df_typed = enforce_schema(df_raw, table)

        # 2bis. Traçabilité d'ingestion (3 colonnes ajoutées avant Bronze)
        df_typed = add_ingestion_metadata(df_typed, source_path, execution_id)

        # 3. Bronze (typé, sans flag) — TOUTES les lignes (doublons parfaits inclus)
        rows_written = write_bronze(df_typed, table, config, date_arrete)

        # 3bis. Dédup Silver (opt-in) : retire les doublons PARFAITS uniquement.
        #       Les collisions de PK sont conservées → flaguées par `unique`.
        df_silver_base, rows_deduped = deduplicate_silver(df_typed, table)
        if rows_deduped:
            print(f"  [DEDUP] {rows_deduped} doublon(s) parfait(s) retiré(s) du Silver "
                  f"(Bronze garde tout)")

        # 4. DQ aggregates (QUALITY_RESULTS) — sur la donnée Silver (dédupliquée)
        quality_results = run_dq(
            df_silver_base, table, config, execution_id, date_situation,
        )
        print_report(quality_results, table.name)

        # 5. Per-row DQ flags — `unique` flague les vraies collisions de PK restantes
        df_silver = compute_dq_flags(df_silver_base, table, execution_id)

        # 6. Silver (Iceberg) — dimensions SCD2 (versionné) ou faits snapshot
        if table.silver_write and table.silver_write.scd2:
            rows_silver = write_silver_scd2(df_silver, table, config, spark, date_arrete)
        else:
            rows_silver = write_silver(df_silver, table, config, spark, date_arrete)

        # Répartition des lignes par statut DQ (PASSED / WARNING / REJECTED)
        # — calculée sur le df réellement écrit en Silver (post-dédup)
        status_counts = {
            r["dq_status"]: r["n"]
            for r in df_silver.groupBy("dq_status").agg(F.count("*").alias("n")).collect()
        }
        rows_passed = status_counts.get("PASSED", 0)
        rows_warning = status_counts.get("WARNING", 0)
        rows_flagged_failed = status_counts.get("REJECTED", 0)  # REJECTED = ligne inutilisable (PK)
        print(f"  [INFO] DQ lignes : PASSED={rows_passed}  WARNING={rows_warning}  "
              f"REJECTED={rows_flagged_failed}  / {rows_silver} en Silver")

        job_status = "SUCCESS"

    except Exception:
        traceback.print_exc()
        job_status = "FAILED"

    finally:
        duration = time.monotonic() - start
        try:
            write_logs(
                spark=spark, quality_results=quality_results, config=config,
                execution_id=execution_id, date_situation=date_situation,
                job_status=job_status, rows_read=rows_read,
                rows_written=rows_written, duration_seconds=duration,
                table_name=table.name,
                source_file=source_path if "source_path" in dir() else "",
                cli_date_arrete=cli_date_arrete,
                rows_flagged_failed=rows_flagged_failed,
            )
        except Exception as e:
            print(f"  [WARN] Erreur logs : {e}")

        print(f"  [DONE] {table.name} — {job_status} | {duration:.1f}s")

    return {
        "table": table.name, "status": job_status,
        "rows_read": rows_read, "rows_written": rows_written,
        "rows_silver": rows_silver, "rows_flagged_failed": rows_flagged_failed,
    }


# ── Main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(description="Bronze Engine — moteur générique")
    parser.add_argument("--config", required=True, help="Chemin YAML groupé")
    parser.add_argument("--date-arrete", required=True, help="YYYY-MM-DD")
    parser.add_argument("--table", default=None,
                        help="Nom d'une table spécifique (optionnel)")
    parser.add_argument("--env", default="dev",
                        choices=["dev", "preprod", "prod"])
    parser.add_argument("--dry-run", action="store_true",
                        help="Mode dry-run : exécute lecture + DQ mais n'écrit rien")
    args = parser.parse_args()

    config = load_config(args.config)
    config.project.environment = args.env.upper()
    if args.dry_run:
        config.execution.dry_run = True
        print("  [MODE] DRY-RUN activé — aucune écriture HDFS / Hive / Iceberg")

    print(f"\n{'=' * 65}")
    print(f"  BRONZE ENGINE  |  {config.project.domain}")
    print(f"  Date arreté    :  {args.date_arrete}")
    print(f"  Environment    :  {args.env.upper()}")
    print(f"  Config         :  {args.config}")

    # Sélection des tables à traiter
    if args.table:
        table_entry = config.get_table(args.table)
        if not table_entry:
            raise ValueError(
                f"Table '{args.table}' introuvable dans le YAML. "
                f"Tables disponibles : {[t.name for t in config.tables]}"
            )
        tables_to_run = [table_entry]
    else:
        tables_to_run = config.tables

    print(f"  Tables         :  {[t.name for t in tables_to_run]}")
    print(f"{'=' * 65}")

    # SparkSession
    builder = SparkSession.builder.appName(f"BRONZE_{config.project.domain}")
    for entry in config.spark.configs:
        builder = builder.config(entry.key, entry.value)
    spark = builder.enableHiveSupport().getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Exécution
    global_start = time.monotonic()
    summary = []
    try:
        for table in tables_to_run:
            result = run_table(table, config, args.date_arrete, spark)
            summary.append(result)
    finally:
        spark.stop()

    # Résumé global
    total = time.monotonic() - global_start
    print(f"\n{'=' * 65}")
    print(f"  RÉSUMÉ FINAL — {config.project.domain}")
    print(f"{'─' * 65}")
    for r in summary:
        icon = "✓" if r["status"] == "SUCCESS" else "✗"
        print(f"  [{icon}] {r['table']:<25} "
              f"lu={r['rows_read']:>8}  "
              f"bronze={r['rows_written']:>8}  "
              f"silver={r.get('rows_silver', 0):>8}  "
              f"rejected={r.get('rows_flagged_failed', 0):>6}")
    print(f"{'─' * 65}")
    print(f"  Durée totale : {total:.1f}s")
    print(f"{'=' * 65}\n")


if __name__ == "__main__":
    main()
