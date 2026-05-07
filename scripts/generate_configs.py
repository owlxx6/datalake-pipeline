#!/usr/bin/env python3
"""Parse les 12 canevas Excel EDD et génère un fichier YAML par table dans dags/configs/."""

import argparse
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

import openpyxl
import yaml

# ── Constantes ──────────────────────────────────────────────────────────────

PK_MARKERS = {"O", "OUI", "X", "PK", "1", "TRUE"}

FORMAT_TO_SPARK = {
    "VARCHAR": "StringType",
    "VARCHAR2": "StringType",
    "AN": "StringType",
    "CHAR": "StringType",
    "NUMBER": "DecimalType",
    "N": "LongType",
    "DATE": "DateType",
    "D": "DateType",
    "TIMESTAMP": "TimestampType",
}

BODY_MARKERS = {"CORPS FICHIER", "DETAIL", "CORPS", "BODY"}

FREQ_TO_CRON = {
    "quotidien": "0 6 * * *",
    "mensuel": "0 6 1 * *",
    "hebdomadaire": "0 6 * * 1",
}


# ── Parsing ─────────────────────────────────────────────────────────────────

def load_rows(ws, max_col: int = 40) -> list[list]:
    """Charge toutes les rows d'une feuille (read-only, max_col limité)."""
    return [list(row) for row in ws.iter_rows(max_col=max_col, values_only=True)]


def find_body_start(rows: list[list]) -> int | None:
    """Trouve l'index de la row marquant le début du CORPS."""
    for i, row in enumerate(rows):
        first = next((v for v in row if v is not None), None)
        if first is not None and str(first).strip().upper() in BODY_MARKERS:
            return i
    return None


def parse_metadata(rows: list[list], body_idx: int) -> dict:
    """Extrait les métadonnées (Table, SOP source, Fréquence, etc.) après le marqueur CORPS."""
    meta = {}
    key_map = {
        "table": "table_name",
        "nom fichier": "nom_fichier",
        "sop source": "sop_source",
        "description": "description",
        "mode d'alimentation": "mode_alimentation",
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


def find_header_row(rows: list[list]) -> tuple[int, int] | None:
    """Trouve la row 'Champ EDD' et retourne (row_index, col_offset)."""
    for i, row in enumerate(rows):
        for j, val in enumerate(row):
            if val is not None and str(val).strip().upper() == "CHAMP EDD":
                return i, j
    return None


def parse_fields(rows: list[list], header_idx: int, col_offset: int) -> list[dict]:
    """Parse les champs métier à partir de la row après le header.

    S'arrête à la première ligne vide (tous None après slice) — les sections
    PARAMETRAGE / tables de codes qui suivent ne sont pas des champs.
    """
    fields = []
    for row in rows[header_idx + 1:]:
        sliced = row[col_offset:]
        vals = sliced[:8]  # champ, format, longueur, description, pk, oblig, codifiable, regles

        # Ligne vide après col_offset → fin de la section champs
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


# ── Mapping types ───────────────────────────────────────────────────────────

def map_spark_type(fmt_raw: str, longueur) -> tuple[str, bool]:
    """Retourne (spark_type, is_unknown_format)."""
    base = fmt_raw.split("(")[0].strip().upper()
    if base in FORMAT_TO_SPARK:
        spark = FORMAT_TO_SPARK[base]
        if spark == "DecimalType" and longueur:
            lon_str = str(longueur).strip()
            if lon_str.startswith("("):
                spark = f"DecimalType{lon_str}"
            else:
                spark = f"DecimalType({lon_str})"
        return spark, False
    if fmt_raw:
        return "StringType", True
    return "StringType", False


# ── Construction config ─────────────────────────────────────────────────────

def derive_domain(excel_filename: str) -> str:
    """EDD_MODEL_CREDIT.xlsx → credit"""
    name = Path(excel_filename).stem
    parts = name.upper().replace("EDD_MODEL_", "").replace("EDD_", "")
    return parts.lower()


def derive_schedule(frequence: str) -> str:
    freq_lower = frequence.lower().split("(")[0].strip() if frequence else ""
    for key, cron in FREQ_TO_CRON.items():
        if key in freq_lower:
            return cron
    return "0 6 * * *  # TODO: fréquence non reconnue, défaut quotidien"


def derive_write_mode(type_stockage: str) -> str:
    if type_stockage and type_stockage.upper() == "STOCK":
        return "merge"
    return "append"


def build_config(
    meta: dict,
    fields: list[dict],
    excel_path: str,
    sheet_name: str,
    domain: str,
    timestamp: str,
) -> tuple[dict, list[str]]:
    """Construit le dict de config YAML. Retourne (config, unknown_formats)."""
    table_name = meta.get("table_name", sheet_name)
    table_lower = table_name.lower()
    description = meta.get("description", "")
    type_stockage = meta.get("type_stockage", "STOCK")
    frequence = meta.get("frequence", "")
    write_mode = derive_write_mode(type_stockage)
    schedule = derive_schedule(frequence)

    pk_cols = [f["name"] for f in fields if f["is_pk"]]
    oblig_cols = [f["name"] for f in fields if not f["nullable"]]

    schema = []
    unknown_formats = []
    for f in fields:
        spark_type, is_unknown = map_spark_type(f["format_raw"], f["longueur"])
        if is_unknown:
            unknown_formats.append(f"{table_name}.{f['name']} (format: {f['format_raw']})")
        entry = {
            "name": f["name"],
            "type": spark_type,
            "nullable": f["nullable"],
        }
        if f["description"]:
            entry["description"] = f["description"]
        schema.append(entry)

    quality_rules = []
    if oblig_cols:
        quality_rules.append({"rule": "not_null", "columns": oblig_cols})
    if pk_cols:
        quality_rules.append({"rule": "unique", "columns": pk_cols})

    pk_comment = None
    if not pk_cols:
        pk_comment = "WARNING: aucune PK détectée"

    config = {
        "metadata": {
            "generated_from": excel_path,
            "sheet_name": sheet_name,
            "generated_at": timestamp,
        },
        "table_name": table_name,
        "domain": domain,
        "source": "sftp",
        "description": description,
        "bronze": {
            "table": f"bronze_sftp_{table_lower}",
            "partition_by": "date_arrete",
            "mode": "append",
        },
        "silver": {
            "table": f"silver_{table_lower}",
            "primary_key": pk_cols,
            "write_mode": write_mode,
            "partition_by": "date_arrete",
            "schema": schema,
            "quality_rules": quality_rules,
        },
        "gold": {
            "feeds": [],
        },
        "schedule": schedule,
        "owner": "data-team",
        "frequency": frequence if frequence else "Quotidien",
        "storage_type": type_stockage if type_stockage else "STOCK",
    }

    return config, unknown_formats, pk_comment


# ── Construction config DQ ─────────────────────────────────────────────────

def build_dq_config(meta: dict, fields: list[dict], domain: str, table_name: str) -> dict:
    """Construit le dict DQConfig (format datalake_lib.quality) pour une table.

    Génère automatiquement les règles minimales :
      - not_null sur les colonnes PK
      - unique sur les colonnes PK
      - not_null sur les colonnes obligatoires non-PK

    Retourne un dict avec `rules: []` si aucune règle ne peut être déduite
    (ni PK, ni colonnes obligatoires) — le caller doit gérer ce cas.
    """
    table_name = table_name.lower()
    pk_cols = [f["name"] for f in fields if f["is_pk"]]
    oblig_non_pk = [f["name"] for f in fields if not f["nullable"] and not f["is_pk"]]

    rules: list[dict] = []
    counter = 1

    def _next_id() -> str:
        nonlocal counter
        rid = f"RUL_{counter:03d}"
        counter += 1
        return rid

    if pk_cols:
        rules.append({
            "rule_id": _next_id(),
            "rule_type": "not_null",
            "columns": list(pk_cols),
            "severity": "blocking",
            "description": "PK ne doit pas être nulle",
        })
        rules.append({
            "rule_id": _next_id(),
            "rule_type": "unique",
            "columns": list(pk_cols),
            "severity": "blocking",
            "description": "PK doit être unique",
        })

    if oblig_non_pk:
        rules.append({
            "rule_id": _next_id(),
            "rule_type": "not_null",
            "columns": list(oblig_non_pk),
            "severity": "blocking",
            "description": "Champs obligatoires non nullables",
        })

    return {
        "table_name": table_name,
        "domain": domain,
        "version": "1.0",
        "primary_key": list(pk_cols),
        "rules": rules,
    }


# ── Sérialisation YAML ─────────────────────────────────────────────────────

class CommentedDumper(yaml.SafeDumper):
    pass


# Désactive les anchors/alias YAML (&idXXX, *idXXX) — peu lisibles dans nos configs.
CommentedDumper.ignore_aliases = lambda self, data: True


def str_representer(dumper, data):
    if "\n" in data:
        return dumper.represent_scalar("tag:yaml.org,2002:str", data, style="|")
    return dumper.represent_scalar("tag:yaml.org,2002:str", data)


CommentedDumper.add_representer(str, str_representer)


def dump_yaml_with_comments(config: dict, pk_comment: str | None) -> str:
    """Sérialise en YAML et injecte les commentaires nécessaires."""
    raw = yaml.dump(config, Dumper=CommentedDumper, default_flow_style=False,
                    allow_unicode=True, sort_keys=False, width=120)

    lines = raw.split("\n")
    result = []
    for line in lines:
        result.append(line)
        if pk_comment and line.strip() == "primary_key: []":
            indent = len(line) - len(line.lstrip())
            result[-1] = line + f"  # {pk_comment}"
        if "source: sftp" in line:
            result[-1] = line + "  # TODO: à confirmer — défaut SFTP car Fichiers PLATS"
        if "owner: data-team" in line:
            result[-1] = line + "  # TODO: à confirmer"
        if "feeds: []" in line:
            result[-1] = line + "  # TODO: à définir (modèle en étoile)"

    return "\n".join(result)


def dump_dq_yaml(dq_config: dict) -> str:
    """Sérialise un DQConfig en YAML, suivi d'un bloc TODO de guidage métier."""
    raw = yaml.dump(dq_config, Dumper=CommentedDumper, default_flow_style=False,
                    allow_unicode=True, sort_keys=False, width=120)

    todo_block = (
        "\n"
        "# ─────────────────────────────────────────────\n"
        "# TODO Métier DQ — compléter les règles ci-dessous\n"
        "# ─────────────────────────────────────────────\n"
        "# Exemples :\n"
        "#   - rule_id: RUL_XXX\n"
        "#     rule_type: referential\n"
        "#     column: MON_CHAMP_FK\n"
        "#     reference_table: silver_table_parent\n"
        "#     reference_column: IDF_CLE_PARENT\n"
        "#     severity: warning\n"
        "#     description: \"...\"\n"
        "#\n"
        "#   - rule_id: RUL_XXX\n"
        "#     rule_type: range\n"
        "#     column: MNT_MONTANT\n"
        "#     min_value: 0\n"
        "#     severity: blocking\n"
        "#     description: \"...\"\n"
    )

    return raw + todo_block


# ── Main ────────────────────────────────────────────────────────────────────

def process_sheet(ws, excel_path: str, sheet_name: str, domain: str, timestamp: str):
    """Parse une feuille et retourne (config_dict, yaml_str, dq_config, warnings).

    `dq_config` est None si aucune règle DQ n'a pu être déduite (ni PK
    ni colonnes obligatoires).
    """
    rows = load_rows(ws)

    body_idx = find_body_start(rows)
    if body_idx is None:
        return None, None, None, {"skip": f"  {sheet_name}: pas de marqueur CORPS trouvé"}

    meta = parse_metadata(rows, body_idx)

    header = find_header_row(rows)
    if header is None:
        return None, None, None, {"skip": f"  {sheet_name}: pas de ligne 'Champ EDD' trouvée"}

    header_idx, col_offset = header
    fields = parse_fields(rows, header_idx, col_offset)

    if not fields:
        return None, None, None, {"skip": f"  {sheet_name}: aucun champ détecté"}

    config, unknown_formats, pk_comment = build_config(
        meta, fields, excel_path, sheet_name, domain, timestamp
    )

    yaml_str = dump_yaml_with_comments(config, pk_comment)

    dq_config = build_dq_config(meta, fields, domain, config["table_name"])

    warnings = {}
    if pk_comment:
        warnings["no_pk"] = sheet_name
    if unknown_formats:
        warnings["unknown_formats"] = unknown_formats
    if not dq_config["rules"]:
        warnings["no_dq_rules"] = sheet_name
        dq_config = None

    return config, yaml_str, dq_config, warnings


def count_todos(yaml_str: str) -> int:
    return yaml_str.count("TODO")


def main():
    parser = argparse.ArgumentParser(description="Génère les configs YAML depuis les canevas Excel EDD.")
    parser.add_argument("--canevas-dir", default="canevas", help="Dossier contenant les .xlsx")
    parser.add_argument("--output-dir", default="dags/configs", help="Dossier de sortie YAML pipeline")
    parser.add_argument(
        "--dq-output-dir",
        default="datalake_lib/quality/dq_configs",
        help="Dossier de sortie YAML DQ (datalake_lib.quality)",
    )
    parser.add_argument("--dry-run", action="store_true", help="Affiche sans écrire")
    args = parser.parse_args()

    canevas_dir = Path(args.canevas_dir)
    output_dir = Path(args.output_dir)
    dq_output_dir = Path(args.dq_output_dir)
    timestamp = datetime.now(timezone.utc).isoformat()

    if not canevas_dir.exists():
        print(f"ERREUR: dossier {canevas_dir} introuvable", file=sys.stderr)
        sys.exit(1)

    xlsx_files = sorted(canevas_dir.glob("*.xlsx"))
    if not xlsx_files:
        print(f"ERREUR: aucun .xlsx dans {canevas_dir}", file=sys.stderr)
        sys.exit(1)

    if not args.dry_run:
        output_dir.mkdir(parents=True, exist_ok=True)
        dq_output_dir.mkdir(parents=True, exist_ok=True)

    # Compteurs pour le rapport
    nb_files = 0
    nb_sheets = 0
    nb_yaml = 0
    nb_dq_yaml = 0
    nb_todos_total = 0
    all_no_pk = []
    all_no_dq_rules = []
    all_unknown_formats = []
    all_skipped = []

    for xlsx_path in xlsx_files:
        nb_files += 1
        domain = derive_domain(xlsx_path.name)
        rel_path = str(xlsx_path)

        print(f"\n{'─'*60}")
        print(f"Fichier: {xlsx_path.name}")

        wb = openpyxl.load_workbook(str(xlsx_path), read_only=True, data_only=True)

        for sheet_name in wb.sheetnames:
            nb_sheets += 1
            ws = wb[sheet_name]
            config, yaml_str, dq_config, warnings = process_sheet(
                ws, rel_path, sheet_name, domain, timestamp
            )

            if config is None:
                skip_msg = warnings.get("skip", f"  {sheet_name}: ignoré")
                all_skipped.append(skip_msg)
                print(f"  ⏭ {skip_msg}")
                continue

            table_name = config["table_name"].lower()
            out_file = output_dir / f"{table_name}.yaml"
            nb_yaml += 1
            nb_todos_total += count_todos(yaml_str)

            if "no_pk" in warnings:
                all_no_pk.append(f"{xlsx_path.name} → {warnings['no_pk']}")
                print(f"  ⚠ WARNING: {sheet_name} — aucune PK détectée")

            if "unknown_formats" in warnings:
                all_unknown_formats.extend(warnings["unknown_formats"])

            if args.dry_run:
                print(f"  ✓ {sheet_name} → {out_file} (dry-run)")
            else:
                out_file.write_text(yaml_str, encoding="utf-8")
                print(f"  ✓ {sheet_name} → {out_file}")

            # Génération du fichier DQ associé
            if dq_config is None:
                all_no_dq_rules.append(f"{xlsx_path.name} → {sheet_name}")
                print(f"  ⚠ WARNING: {sheet_name} — aucune règle DQ déductible (pas de PK ni d'obligatoires)")
            else:
                dq_out_file = dq_output_dir / f"{table_name}_dq.yaml"
                dq_yaml_str = dump_dq_yaml(dq_config)
                nb_dq_yaml += 1
                if args.dry_run:
                    print(f"  ✓ DQ {sheet_name} → {dq_out_file} (dry-run)")
                else:
                    dq_out_file.write_text(dq_yaml_str, encoding="utf-8")
                    print(f"  ✓ DQ {sheet_name} → {dq_out_file}")

        wb.close()

    # ── Rapport de synthèse ─────────────────────────────────────────────
    print(f"\n{'═'*60}")
    print("RAPPORT DE SYNTHÈSE")
    print(f"{'═'*60}")
    print(f"Fichiers Excel parsés : {nb_files}")
    print(f"Feuilles traitées     : {nb_sheets}")
    print(f"YAML pipeline générés : {nb_yaml}")
    print(f"YAML DQ générés       : {nb_dq_yaml}")
    print(f"TODO dans les YAML    : {nb_todos_total}")

    if all_no_pk:
        print(f"\n⚠ Feuilles SANS PK ({len(all_no_pk)}) :")
        for entry in all_no_pk:
            print(f"  - {entry}")

    if all_no_dq_rules:
        print(f"\n⚠ Feuilles SANS règles DQ déductibles ({len(all_no_dq_rules)}) :")
        for entry in all_no_dq_rules:
            print(f"  - {entry}")

    if all_unknown_formats:
        print(f"\n⚠ Formats inconnus mappés en StringType ({len(all_unknown_formats)}) :")
        for entry in all_unknown_formats:
            print(f"  - {entry}")

    if all_skipped:
        print(f"\n⏭ Feuilles ignorées ({len(all_skipped)}) :")
        for entry in all_skipped:
            print(f"  - {entry}")

    if args.dry_run:
        print("\n(mode --dry-run : aucun fichier écrit)")


if __name__ == "__main__":
    main()
