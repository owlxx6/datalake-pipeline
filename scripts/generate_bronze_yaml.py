#!/usr/bin/env python3
"""One-shot Bronze YAML generator depuis canevas EDD.

Parse les 13 fichiers `canevas/EDD_MODEL_*.xlsx` et produit
`configs/bronze/credit_garantie_generated.yaml` au format Bronze groupé.

EDD étant stables, ce script est jetable après usage.

Usage:
    python3 scripts/generate_bronze_yaml.py
"""

from __future__ import annotations

from pathlib import Path

import openpyxl
import yaml


PK_MARKERS = {"O", "OUI", "X", "PK", "1", "TRUE"}
PARAM_SHEETS = {"CODE_EVNT", "CODE_TRANSA_GEST"}
BODY_MARKERS = {"CORPS FICHIER", "DETAIL", "CORPS", "BODY"}

FORMAT_TO_SPARK = {
    "VARCHAR": "StringType",
    "VARCHAR2": "StringType",
    "AN": "StringType",
    "CHAR": "StringType",
    "NUMBER": "DecimalType",
    "N": "DecimalType",
    "DATE": "TimestampType",
    "D": "DateType",
    "TIMESTAMP": "TimestampType",
}


def load_rows(ws, max_col: int = 40):
    return [list(row) for row in ws.iter_rows(max_col=max_col, values_only=True)]


def find_body_start(rows):
    for i, row in enumerate(rows):
        first = next((v for v in row if v is not None), None)
        if first is not None and str(first).strip().upper() in BODY_MARKERS:
            return i
    return None


def parse_metadata(rows, body_idx):
    meta = {}
    key_map = {
        "table": "table_name",
        "description": "description",
        "fréquence d'alimentation": "frequence",
        "type stockage": "type_stockage",
    }
    for row in rows[body_idx + 1:]:
        vals = [v for v in row if v is not None]
        if len(vals) >= 2:
            key_raw = str(vals[0]).strip().lower()
            if key_raw in key_map:
                meta[key_map[key_raw]] = str(vals[1]).strip()
        if len(vals) >= 1 and str(vals[0]).strip().upper() == "CHAMP EDD":
            break
    return meta


def find_header_row(rows):
    for i, row in enumerate(rows):
        for j, val in enumerate(row):
            if val is not None and str(val).strip().upper() == "CHAMP EDD":
                return i, j
    return None


def parse_fields(rows, header_idx, col_offset):
    fields = []
    for row in rows[header_idx + 1:]:
        sliced = row[col_offset:]
        vals = sliced[:8]
        if all(v is None for v in vals):
            if fields:
                break
            continue
        if not vals or vals[0] is None:
            if fields:
                break
            continue
        champ = str(vals[0]).strip()
        if not champ:
            if fields:
                break
            continue
        fmt_raw = str(vals[1]).strip() if len(vals) > 1 and vals[1] is not None else ""
        longueur = vals[2] if len(vals) > 2 and vals[2] is not None else ""
        description = str(vals[3]).strip() if len(vals) > 3 and vals[3] is not None else ""
        pk_raw = str(vals[4]).strip().upper() if len(vals) > 4 and vals[4] is not None else ""
        oblig_raw = str(vals[5]).strip().upper() if len(vals) > 5 and vals[5] is not None else ""

        is_pk = pk_raw in PK_MARKERS
        is_oblig = oblig_raw in PK_MARKERS

        fields.append({
            "name": champ,
            "format_raw": fmt_raw,
            "longueur": longueur,
            "description": description,
            "is_pk": is_pk,
            "nullable": not (is_pk or is_oblig),
        })
    return fields


def parse_codifications(rows):
    codif = {}
    in_param = False
    current_field = None
    for row in rows:
        first_idx = next((i for i, v in enumerate(row) if v is not None), None)
        if first_idx is None:
            continue
        first = str(row[first_idx]).strip()
        if "PARAMETRAGE" in first.upper():
            in_param = True
            current_field = None
            continue
        if not in_param:
            continue
        if first.upper() in {"CHAMP", "CODE"}:
            continue
        champ = str(row[1]).strip() if len(row) > 1 and row[1] is not None else ""
        code = str(row[2]).strip() if len(row) > 2 and row[2] is not None else ""
        if champ:
            current_field = champ
            codif.setdefault(current_field, [])
        if code and current_field:
            codif[current_field].append(code)
    return codif


def map_spark_type(fmt_raw, longueur):
    base = fmt_raw.split("(")[0].strip().upper()
    if base in FORMAT_TO_SPARK:
        spark = FORMAT_TO_SPARK[base]
        if spark == "DecimalType" and longueur:
            lon_str = str(longueur).strip()
            try:
                lon_int = int(float(lon_str))
                scale = 0 if lon_int <= 13 else 2
                spark = f"DecimalType({lon_int}, {scale})"
            except ValueError:
                spark = "DecimalType(18, 2)"
        return spark
    return "StringType"


def build_table_entry(table_name, fields, codif):
    table_lower = table_name.lower()
    pk_cols = [f["name"] for f in fields if f["is_pk"]]

    schema_fields = []
    for f in fields:
        entry = {
            "name": f["name"],
            "spark_type": map_spark_type(f["format_raw"], f["longueur"]),
            "nullable": f["nullable"],
        }
        if f["description"]:
            entry["description"] = f["description"][:100]
        schema_fields.append(entry)

    quality_rules = []
    field_names = {f["name"] for f in fields}
    is_composite_pk = len(pk_cols) > 1

    for col in pk_cols:
        checks = [{"type": "not_null", "severity": "CRITICAL",
                   "alert_threshold": 0.0, "control_type": "BASIC"}]
        if not is_composite_pk:
            checks.append({"type": "unique", "severity": "CRITICAL",
                           "alert_threshold": 0.0, "control_type": "BASIC"})
        quality_rules.append({
            "field": col,
            "source_column": col,
            "data_type": "StringType",
            "description": f"PK {col} — clé primaire",
            "checks": checks,
        })

    for field_name, codes in codif.items():
        if not codes or field_name in pk_cols:
            continue
        if field_name not in field_names:
            continue
        quality_rules.append({
            "field": field_name,
            "source_column": field_name,
            "data_type": "StringType",
            "description": f"{field_name} — codification EDD",
            "checks": [{
                "type": "in_list",
                "severity": "HIGH",
                "allowed_values": codes,
                "alert_threshold": 1.0,
                "control_type": "BASIC",
            }],
        })

    consistency_rules = []
    if is_composite_pk:
        consistency_rules.append({
            "name": "pk_composite_complete",
            "description": f"PK composite ({len(pk_cols)} colonnes) toutes non-nulles",
            "condition": "1=1",
            "required_fields": pk_cols,
            "severity": "CRITICAL",
            "alert_threshold": 0.0,
            "control_type": "BASIC",
        })

    entry = {
        "name": table_name,
        "domain": table_lower,
        "primary_key": pk_cols,
        "is_partitioned": False,
        "source": {
            "path_template": "hdfs://ubuntu3:9000/data/landing/{table_name}/date_arrete={date_arrete}",
            "format": "parquet",
            "loading": {"mode": "FULL"},
        },
        "schema": {
            "on_cast_error": "reject",
            "fields": schema_fields,
        },
        "write": {
            "format": "parquet",
            "path_template": "hdfs://ubuntu3:9000/data/bronze/{domain}/{table_name}/date_arrete={date_arrete}",
            "mode": "overwrite",
            "partition_by": ["date_arrete"],
            "compression": "snappy",
            "coalesce": 4,
        },
        "silver_write": {
            "database": "silver_credit_garantie",
            "table": table_lower,
            "mode": "overwrite",
            "partition_by": ["date_arrete"],
        },
        "quality_rules": quality_rules,
        "consistency_rules": consistency_rules,
    }

    if "DATE_FICHIER" in field_names:
        entry["source"]["date_arrete_column"] = "DATE_FICHIER"

    return entry


def main():
    canevas_dir = Path("canevas")
    edd_files = sorted(canevas_dir.glob("EDD_MODEL_*.xlsx"))

    all_tables = []
    skipped = []
    for path in edd_files:
        wb = openpyxl.load_workbook(path, data_only=True, read_only=True)
        for ws in wb.worksheets:
            if ws.title.upper() in PARAM_SHEETS:
                skipped.append(f"{path.name}/{ws.title} (param sheet)")
                continue
            rows = load_rows(ws)
            body_idx = find_body_start(rows)
            if body_idx is None:
                skipped.append(f"{path.name}/{ws.title} (no body)")
                continue
            meta = parse_metadata(rows, body_idx)
            header = find_header_row(rows)
            if header is None:
                skipped.append(f"{path.name}/{ws.title} (no header)")
                continue
            fields = parse_fields(rows, header[0], header[1])
            if not fields:
                skipped.append(f"{path.name}/{ws.title} (no fields)")
                continue
            codif = parse_codifications(rows)
            table_name = meta.get("table_name", ws.title)
            entry = build_table_entry(table_name, fields, codif)
            all_tables.append(entry)
            print(f"  ✓ {table_name:<28} {len(fields):>3} champs · "
                  f"{len(entry['quality_rules']):>2} DQ rules · "
                  f"{len(codif)} codif")
        wb.close()

    yaml_obj = {
        "globals": {
            "layer_abbrev": "BR",
            "severity_weights": {"CRITICAL": 1, "HIGH": 2, "MEDIUM": 3, "LOW": 4},
        },
        "project": {
            "layer": "BRONZE",
            "domain": "CREDIT_GARANTIE",
            "environment": "DEV",
            "execution_id_template": "{layer_abbrev}_{domain}_{table_name}_{timestamp}",
        },
        "spark": {
            "configs": [
                {"key": "spark.sql.extensions",
                 "value": "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"},
                {"key": "spark.sql.catalog.spark_catalog",
                 "value": "org.apache.iceberg.spark.SparkSessionCatalog"},
                {"key": "spark.sql.catalog.spark_catalog.type", "value": "hive"},
                {"key": "spark.sql.catalog.spark_catalog.uri", "value": "thrift://ubuntu3:9083"},
                {"key": "spark.sql.parquet.datetimeRebaseModeInWrite", "value": "CORRECTED"},
                {"key": "spark.sql.parquet.int96RebaseModeInWrite", "value": "CORRECTED"},
                {"key": "spark.sql.sources.partitionOverwriteMode", "value": "DYNAMIC"},
            ],
        },
        "output": {
            "database": "silver_credit_garantie",
            "quality_results": {
                "name_template": "{layer_abbrev}_{domain}_QUALITY_RESULTS",
                "format": "iceberg",
                "mode": "append",
                "partition_by": ["environment", "date_situation"],
            },
            "log_pipeline": {
                "name_template": "{layer_abbrev}_{domain}_LOG_PIPELINE",
                "format": "iceberg",
                "mode": "append",
                "partition_by": ["environment"],
            },
        },
        "performance": {
            "cache_source_data": True,
            "repartition": {"enabled": False, "num_partitions": 8},
        },
        "retry": {
            "max_retries": 3,
            "backoff_seconds": 2,
            "retryable_exceptions": [
                "java.io.IOException",
                "org.apache.spark.SparkException",
                "java.net.ConnectException",
            ],
        },
        "execution": {
            "dry_run": False,
            "date_situation": {"source": "today", "format": "yyyy-MM-dd"},
        },
        "tables": all_tables,
    }

    out_path = Path("configs/bronze/credit_garantie_generated.yaml")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        yaml.dump(yaml_obj, f, sort_keys=False, allow_unicode=True,
                  width=120, default_flow_style=False)

    total_fields = sum(len(t["schema"]["fields"]) for t in all_tables)
    total_rules = sum(len(t["quality_rules"]) for t in all_tables)
    total_consist = sum(len(t["consistency_rules"]) for t in all_tables)
    print(f"\n{'=' * 60}")
    print(f"RÉSUMÉ")
    print(f"{'─' * 60}")
    print(f"  Tables générées       : {len(all_tables)}")
    print(f"  Total champs schéma   : {total_fields}")
    print(f"  Total DQ rules        : {total_rules}")
    print(f"  Total consistency     : {total_consist}")
    print(f"  Skip                  : {len(skipped)}")
    for s in skipped:
        print(f"    - {s}")
    print(f"\n  Output : {out_path}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    main()
