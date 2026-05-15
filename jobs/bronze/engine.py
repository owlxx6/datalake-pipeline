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
"""
from __future__ import annotations

import argparse
import re
import time
import traceback
from datetime import datetime
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, IntegerType, LongType, FloatType,
    DoubleType, DateType, TimestampType, DecimalType, BooleanType,
)
from pyspark.sql.window import Window

from jobs.bronze.config_loader import (
    load_config, BronzeConfig, TableEntry, DQCheck, QualityRule, ConsistencyRule,
    SilverWriteConfig,
)


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


# ── Reject metadata ───────────────────────────────────────────────────────────

def _add_reject_meta(
    df: DataFrame, rule_id: str, rule_type: str, field_name: str,
    severity: str, stage: str, message: str,
    execution_id: str, date_situation: str, table_name: str,
) -> DataFrame:
    actual = (
        F.col(field_name).cast(StringType())
        if field_name in df.columns
        else F.lit(None).cast(StringType())
    )
    return (
        df
        .withColumn("_rule_id", F.lit(rule_id))
        .withColumn("_rule_type", F.lit(rule_type))
        .withColumn("_field_name", F.lit(field_name))
        .withColumn("_severity", F.lit(severity))
        .withColumn("_actual_value", actual)
        .withColumn("_error_message", F.lit(message))
        .withColumn("_reject_stage", F.lit(stage))
        .withColumn("_execution_id", F.lit(execution_id))
        .withColumn("_date_situation", F.lit(date_situation))
        .withColumn("_table_name", F.lit(table_name))
        .withColumn("_layer", F.lit("BRONZE"))
        .withColumn("_ingestion_ts", F.lit(datetime.now().isoformat()))
    )


# ── Schema Enforcer ───────────────────────────────────────────────────────────

def enforce_schema(
    df: DataFrame, table: TableEntry,
    execution_id: str, date_situation: str, spark: SparkSession,
) -> tuple[DataFrame, DataFrame]:
    df_typed = df
    reject_dfs: list[DataFrame] = []

    for field in table.schema.fields:
        if field.name not in df.columns:
            continue

        target_type = _spark_type(field.spark_type)
        casted = F.col(field.name).cast(target_type)
        cast_failed = F.col(field.name).isNotNull() & casted.isNull()

        if table.schema.on_cast_error == "reject" and not field.nullable:
            rejects = _add_reject_meta(
                df_typed.filter(cast_failed),
                rule_id=f"SCHEMA_CAST_{field.name}",
                rule_type="cast", field_name=field.name,
                severity="CRITICAL", stage="CAST",
                message=f"Cast vers {field.spark_type} échoué",
                execution_id=execution_id, date_situation=date_situation,
                table_name=table.name,
            )
            reject_dfs.append(rejects)
            df_typed = df_typed.filter(~cast_failed)

        df_typed = df_typed.withColumn(field.name, casted)

    if reject_dfs:
        df_cast_rejects = reject_dfs[0]
        for r in reject_dfs[1:]:
            df_cast_rejects = df_cast_rejects.unionByName(r, allowMissingColumns=True)
    else:
        df_cast_rejects = spark.createDataFrame([], df_typed.schema)

    return df_typed, df_cast_rejects


# ── DQ check condition ────────────────────────────────────────────────────────

def _check_condition(col_name: str, check: DQCheck):
    col = F.col(col_name)

    if check.type == "not_null":
        return col.isNull()
    elif check.type == "unique":
        w = Window.partitionBy(col_name)
        return (F.count(col_name).over(w) > 1) & col.isNotNull()
    elif check.type == "in_list" and check.allowed_values:
        stripped = [v.strip() for v in check.allowed_values]
        return ~F.trim(col).isin(stripped) & col.isNotNull()
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


# ── DQ Runner ─────────────────────────────────────────────────────────────────

def run_dq(
    df: DataFrame, table: TableEntry, config: BronzeConfig,
    execution_id: str, date_situation: str, spark: SparkSession,
) -> tuple[DataFrame, list[dict]]:
    reject_dfs: list[DataFrame] = []
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
                df_failing = (
                    df_scope
                    .withColumn(_tmp, F.count(col_name).over(Window.partitionBy(col_name)))
                    .filter(F.col(_tmp) > 1)
                    .drop(_tmp)
                )
            else:
                condition = _check_condition(col_name, check)
                if condition is None:
                    continue
                df_failing = df_scope.filter(condition)

            df_rej = _add_reject_meta(
                df_failing,
                rule_id=rule_id, rule_type=check.type, field_name=col_name,
                severity=check.severity, stage="DQ",
                message=f"{check.type} failed on {col_name}",
                execution_id=execution_id, date_situation=date_situation,
                table_name=table.name,
            )
            failed = df_rej.count()
            rate = (failed / scope_count * 100) if scope_count > 0 else 0.0

            results.append(_qr_row(
                execution_id, date_situation, rule_id, check.type,
                col_name, check.severity, scope_count, failed, rate,
                check.alert_threshold, check.control_type, config, table,
            ))
            if failed > 0:
                reject_dfs.append(df_rej)

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
        df_rej = _add_reject_meta(
            df_scope.filter(condition),
            rule_id=rule_id, rule_type="consistency", field_name=fields_str,
            severity=rule.severity, stage="CONSISTENCY",
            message=rule.description,
            execution_id=execution_id, date_situation=date_situation,
            table_name=table.name,
        )
        failed = df_rej.count()
        rate = (failed / scope_count * 100) if scope_count > 0 else 0.0

        results.append(_qr_row(
            execution_id, date_situation, rule_id, "consistency",
            fields_str, rule.severity, scope_count, failed, rate,
            rule.alert_threshold, rule.control_type, config, table,
        ))
        if failed > 0:
            reject_dfs.append(df_rej)

    if reject_dfs:
        df_rejects = reject_dfs[0]
        for r in reject_dfs[1:]:
            df_rejects = df_rejects.unionByName(r, allowMissingColumns=True)
    else:
        df_rejects = spark.createDataFrame([], df.schema)

    return df_rejects, results


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


# ── Clean DF (Silver input) ───────────────────────────────────────────────────

def get_clean_df(
    df_typed: DataFrame, df_dq_rejects: DataFrame, table: TableEntry,
) -> DataFrame:
    """Soustrait les lignes rejetées par le DQ de df_typed (anti-join sur PK).

    Bronze HDFS garde TOUTES les lignes (brut = fidèle à la source).
    Silver Hive ne reçoit que les lignes ayant passé toutes les règles DQ.
    """
    if df_dq_rejects.count() == 0:
        return df_typed

    pk_cols = table.primary_key
    # On ne sélectionne que les PK depuis les rejects (évite les colonnes audit)
    available_pks = [c for c in pk_cols if c in df_dq_rejects.columns]
    if not available_pks:
        return df_typed

    df_reject_pks = df_dq_rejects.select(available_pks).distinct()
    return df_typed.join(df_reject_pks, on=available_pks, how="left_anti")


# ── Layer Router ──────────────────────────────────────────────────────────────

def write_bronze(
    df: DataFrame, table: TableEntry, config: BronzeConfig,
    date_arrete: str,
) -> int:
    path = config.resolve(
        table.write.path_template,
        table_name=table.name,
        domain=table.domain,
        date_arrete=date_arrete.replace("-", ""),
    )

    if config.execution.dry_run:
        count = df.count()
        print(f"  [DRY RUN] → {path} ({count} lignes)")
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
    print(f"  [WRITE] ✓ {count} lignes → {path}")
    return count


def write_silver(
    df_clean: DataFrame, table: TableEntry, config: BronzeConfig,
    date_arrete: str,
) -> int:
    """Écrit les lignes propres dans la table Hive Silver (format ORC via Hive).

    N'est appelé que si `silver_write` est défini dans la config de la table.
    Mode overwrite par défaut — rejouer le job remplace la partition du jour.
    """
    sw = table.silver_write
    if sw is None:
        return 0

    full_table = f"{sw.database}.{sw.table}"

    if config.execution.dry_run:
        count = df_clean.count()
        print(f"  [DRY RUN] Silver → {full_table} ({count} lignes)")
        return count

    (
        df_clean
        .withColumn("date_arrete", F.lit(date_arrete))
        .write
        .mode(sw.mode)
        .partitionBy(*sw.partition_by)
        .saveAsTable(full_table)          # Hive géré table — format ORC par défaut
    )
    count = df_clean.count()
    print(f"  [SILVER] ✓ {count} lignes propres → {full_table}")
    return count


# ── Sink Writer ───────────────────────────────────────────────────────────────

def write_logs(
    spark: SparkSession, df_rejects: DataFrame,
    quality_results: list[dict], config: BronzeConfig,
    execution_id: str, date_situation: str,
    job_status: str, rows_read: int, rows_written: int,
    duration_seconds: float, table_name: str,
) -> None:
    db = config.output.database
    env = config.project.environment

    # LOGS_REJECTED
    if config.output.logs_rejected.enabled and df_rejects.count() > 0:
        table = f"{db}.{config.resolve(config.output.logs_rejected.name_template, table_name=table_name)}"
        (
            df_rejects
            .withColumn("environment", F.lit(env))
            .withColumn("date_situation", F.lit(date_situation))
            .write.format("iceberg").mode("append")
            .partitionBy("environment", "date_situation")
            .saveAsTable(table)
        )
        print(f"  [SINK] {df_rejects.count()} rejects → {table}")

    # QUALITY_RESULTS
    if quality_results:
        table = f"{db}.{config.resolve(config.output.quality_results.name_template, table_name=table_name)}"
        (
            spark.createDataFrame(quality_results)
            .withColumn("environment", F.lit(env))
            .write.format("iceberg").mode("append")
            .partitionBy("environment", "date_situation")
            .saveAsTable(table)
        )
        print(f"  [SINK] {len(quality_results)} résultats DQ → {table}")

    # LOG_PIPELINE
    table = f"{db}.{config.resolve(config.output.log_pipeline.name_template, table_name=table_name)}"
    (
        spark.createDataFrame([{
            "execution_id": execution_id,
            "layer": "BRONZE",
            "table_name": table_name,
            "domain": config.project.domain,
            "environment": env,
            "date_situation": date_situation,
            "job_status": job_status,
            "rows_read": rows_read,
            "rows_written": rows_written,
            "rows_rejected": rows_read - rows_written,
            "duration_seconds": round(duration_seconds, 2),
            "execution_ts": datetime.now().isoformat(),
        }])
        .withColumn("environment", F.lit(env))
        .write.format("iceberg").mode("append")
        .partitionBy("environment")
        .saveAsTable(table)
    )
    print(f"  [SINK] Pipeline log → {table}")


# ── DQ Report ─────────────────────────────────────────────────────────────────

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
    """Exécute le pipeline Bronze complet pour une table."""
    start = time.monotonic()
    execution_id = config.resolve(
        config.project.execution_id_template,
        table_name=table.name,
        date_arrete=date_arrete.replace("-", ""),
    )
    date_situation = date_arrete

    print(f"\n{'─' * 65}")
    print(f"  TABLE : {table.name}  |  {execution_id}")
    print(f"{'─' * 65}")

    job_status = "FAILED"
    rows_read = rows_written = 0
    df_rejects = spark.createDataFrame([], spark.range(0).schema)
    quality_results: list[dict] = []

    try:
        # 1. Lecture
        source_path = config.resolve(
            table.source.path_template,
            table_name=table.name,
            date_arrete=date_arrete.replace("-", ""),
        )
        print(f"  [SOURCE] {source_path}")
        df_raw = spark.read.parquet(source_path)
        rows_read = df_raw.count()
        print(f"  [SOURCE] {rows_read} lignes lues")

        if config.performance.cache_source_data:
            df_raw.cache()

        # 2. Schema enforcement
        df_typed, df_cast_rejects = enforce_schema(
            df_raw, table, execution_id, date_situation, spark
        )
        cast_count = df_cast_rejects.count()
        if cast_count:
            print(f"  [SCHEMA] {cast_count} lignes rejetées (cast errors)")
            df_rejects = df_cast_rejects

        # 3. DQ checks
        df_dq_rejects, quality_results = run_dq(
            df_typed, table, config, execution_id, date_situation, spark
        )
        print_report(quality_results, table.name)

        if df_dq_rejects.count() > 0:
            df_rejects = (
                df_rejects.unionByName(df_dq_rejects, allowMissingColumns=True)
                if df_rejects.count() > 0 else df_dq_rejects
            )

        # 4. Écriture Bronze Parquet (toutes les lignes typées — fidèle à la source)
        rows_written = write_bronze(df_typed, table, config, date_arrete)

        # 5. Écriture Silver Hive (lignes propres — DQ passé uniquement)
        df_clean = get_clean_df(df_typed, df_dq_rejects, table)
        rows_silver = write_silver(df_clean, table, config, date_arrete)
        if rows_silver:
            print(f"  [SILVER] {rows_silver} lignes propres / {rows_written} lues")

        job_status = "SUCCESS"

    except Exception:
        traceback.print_exc()
        job_status = "FAILED"

    finally:
        duration = time.monotonic() - start
        try:
            write_logs(
                spark=spark, df_rejects=df_rejects,
                quality_results=quality_results, config=config,
                execution_id=execution_id, date_situation=date_situation,
                job_status=job_status, rows_read=rows_read,
                rows_written=rows_written, duration_seconds=duration,
                table_name=table.name,
            )
        except Exception as e:
            print(f"  [WARN] Erreur logs : {e}")

        print(f"  [DONE] {table.name} — {job_status} | {duration:.1f}s")

    return {
        "table": table.name, "status": job_status,
        "rows_read": rows_read, "rows_written": rows_written,
        "rows_silver": rows_silver if "rows_silver" in dir() else 0,
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
    args = parser.parse_args()

    config = load_config(args.config)
    config.project.environment = args.env.upper()

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

    # Exécution — une table après l'autre
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
        rejected = r["rows_read"] - r["rows_written"]
        print(f"  [{icon}] {r['table']:<25} "
              f"lu={r['rows_read']:>8}  "
              f"bronze={r['rows_written']:>8}  "
              f"silver={r.get('rows_silver', 0):>8}  "
              f"rejeté={rejected:>6}")
    print(f"{'─' * 65}")
    print(f"  Durée totale : {total:.1f}s")
    print(f"{'=' * 65}\n")


if __name__ == "__main__":
    main()
