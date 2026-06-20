#!/usr/bin/env python3
"""Génère un projet Power BI (.pbip / TMDL) pré-câblé pour les dashboards Crédit-Garantie.

Produit un dossier `CreditGarantie/` contenant :
  - CreditGarantie.pbip
  - CreditGarantie.SemanticModel/  (modèle TMDL : 11 tables + relations + mesures DAX + colonnes calculées)
  - CreditGarantie.Report/         (rapport vide : 5 pages D1..D5 prêtes à recevoir les visuels)

Les tables pointent vers les CSV exportés (mode Import). À ouvrir dans Power BI Desktop (Windows).
Le format TMDL est sensible aux TABULATIONS : ce générateur les écrit explicitement.

  python3 scripts/gen_powerbi_project.py --csv-path "C:\\datalake\\exports" --out powerbi

NB : non testable sur Mac (Power BI = Windows). Si l'ouverture échoue, voir le guide (montage UI).
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

T = "\t"

# ── Spécification des tables ───────────────────────────────────────────────────
# kind : text | int | num | numfr | date | bool   (num = décimale point ; numfr = décimale virgule)
TABLES: dict[str, list[tuple[str, str]]] = {
    "dim_temps": [
        ("date_key", "int"), ("date", "date"), ("jour", "int"), ("mois", "int"),
        ("trimestre", "int"), ("annee", "int"), ("lib_mois", "text"),
    ],
    "dim_produit": [
        ("idf_code_produit", "text"), ("cod_sop", "text"), ("cod_activite", "text"),
        ("lib_activite", "text"), ("cod_famille", "text"), ("lib_famille", "text"),
        ("prod_commercial", "text"), ("lib_produit", "text"), ("cod_usage", "text"),
        ("cod_typ_produit", "text"),
    ],
    "dim_organisation": [
        ("idf_num_gui", "text"), ("idf_num_bur", "text"), ("cod_fed", "text"),
        ("lib_fed_court", "text"), ("cod_bqe", "text"), ("lib_bqe", "text"),
        ("lib_typ_dir", "text"), ("num_dir", "text"), ("lib_dir_court", "text"),
        ("lib_typ_grp", "text"), ("num_grp", "text"), ("lib_grp_court", "text"),
        ("lib_court_gui", "text"), ("lib_long_gui", "text"), ("lib_bur", "text"),
        ("typ_bur", "text"), ("cod_ecv_bur", "text"), ("nom_vil_bur", "text"),
        ("code_post_bur", "text"), ("code_reg_eco", "text"),
    ],
    "dim_tiers": [
        ("idf_num_tiers", "text"), ("raison_sociale", "text"), ("nom_usg_tiers", "text"),
        ("prenom_tiers", "text"), ("cod_catg_jur", "text"), ("cod_activite", "text"),
        ("cod_typ_tiers", "text"), ("cod_statut_tiers", "text"),
    ],
    "dim_portefeuille": [
        ("idf_num_tiers", "text"), ("idf_num_gui", "text"),
        ("idf_code_cc", "text"), ("cod_typ_rattach", "text"),
    ],
    "gdw_synthese_credit": [
        ("idf_num_doss", "text"), ("date_arrete", "date"), ("cod_produit", "text"),
        ("num_compte_emprunteur", "text"), ("idf_num_tiers", "text"), ("date_accord", "date"),
        ("encours", "num"), ("mnt_credit", "num"), ("tx_credit", "num"), ("duree", "int"),
        ("cod_sit_doss", "text"), ("cod_famille", "text"), ("lib_famille", "text"),
        ("mnt_impaye_total", "num"), ("nb_creances_impayees", "int"), ("flag_cdl", "bool"),
    ],
    "gdw_impayes": [
        ("idf_num_doss", "text"), ("idf_num_creance", "text"), ("num_compte", "text"),
        ("mnt_impayes_cdl", "num"), ("mnt_creance", "num"), ("cod_statut_incident", "text"),
        ("date_mise_impaye", "date"), ("date_echeance", "date"), ("date_arrete", "date"),
    ],
    "fait_garanties": [
        ("idf_num_ctr_garantie", "text"), ("idf_ref_ext_credit", "text"),
        ("cod_typ_garant", "text"), ("cod_nat_garantie", "text"), ("cod_statut_garantie", "text"),
        ("mnt_total_garantie", "num"), ("mnt_actualise", "num"),
        ("pourcentage_garantie", "num"), ("date_arrete", "date"),
    ],
    "fait_autorisations": [
        ("idf_ref_auto", "text"), ("num_compte", "text"), ("num_tiers", "text"),
        ("cod_typ_auto", "text"), ("cod_etat_auto", "text"),
        ("montant_autorise", "numfr"), ("montant_utilise", "numfr"), ("date_arrete", "date"),
    ],
    "fait_depassement": [
        ("num_compte", "text"), ("compte_general", "text"), ("date_entree", "date"),
        ("date_sortie", "date"), ("mnt_depassement", "numfr"), ("date_arrete", "date"),
    ],
    "br_credit_garantie_quality_results": [
        ("alert_threshold_pct", "num"), ("control_type", "text"), ("date_situation", "date"),
        ("description", "text"), ("domain", "text"), ("duration_seconds", "num"),
        ("environment", "text"), ("error_message", "text"), ("execution_date", "date"),
        ("execution_id", "text"), ("failure_rate_pct", "num"), ("field_name", "text"),
        ("layer", "text"), ("passed", "bool"), ("passed_percentage", "num"),
        ("records_checked", "int"), ("records_failed", "int"), ("rule_code", "text"),
        ("rule_id", "text"), ("rule_type", "text"), ("severity", "text"), ("status", "text"),
        ("table_name", "text"), ("table_partition_exists", "bool"),
    ],
}

# Fichier CSV source par table (la table d'audit DQ vient du Silver)
CSV_FILE = {t: f"{t}.csv" for t in TABLES}

# Tables dédoublonnées sur une clé (pour rendre la relation côté « 1 » valide)
DEDUP_ON = {"dim_portefeuille": "idf_num_tiers"}

# ── Relations (côté « 1 » vérifié unique sur les données) ──────────────────────
RELATIONSHIPS = [
    ("rel_synth_tiers",        "gdw_synthese_credit", "idf_num_tiers", "dim_tiers", "idf_num_tiers"),
    ("rel_synth_temps",        "gdw_synthese_credit", "date_accord",   "dim_temps", "date"),
    ("rel_impayes_synth",      "gdw_impayes",         "idf_num_doss",  "gdw_synthese_credit", "idf_num_doss"),
    ("rel_garanties_synth",    "fait_garanties",      "idf_ref_ext_credit", "gdw_synthese_credit", "idf_num_doss"),
    ("rel_auto_tiers",         "fait_autorisations",  "num_tiers",     "dim_tiers", "idf_num_tiers"),
    ("rel_synth_portefeuille", "gdw_synthese_credit", "idf_num_tiers", "dim_portefeuille", "idf_num_tiers"),
    ("rel_portef_org",         "dim_portefeuille",    "idf_num_gui",   "dim_organisation", "idf_num_gui"),
]

# ── Mesures DAX (toutes portées par gdw_synthese_credit) ───────────────────────
MEASURES = [
    # NB : nom != colonne 'encours' (Power BI casse-insensible -> conflit mesure/colonne)
    ("Encours Total", "SUM(gdw_synthese_credit[encours])", "#,0"),
    ("Montant Crédit", "SUM(gdw_synthese_credit[mnt_credit])", "#,0"),
    ("Nombre Crédits", "DISTINCTCOUNT(gdw_synthese_credit[idf_num_doss])", "#,0"),
    ("Ticket Moyen", "DIVIDE([Montant Crédit], [Nombre Crédits])", "#,0"),
    ("Montant Impayés", "SUM(gdw_impayes[mnt_impayes_cdl])", "#,0"),
    ("Montant Créances", "SUM(gdw_impayes[mnt_creance])", "#,0"),
    ("Taux Impayé", "DIVIDE([Montant Impayés], [Encours Total])", "0.0%"),
    ("Montant en Défaut", "CALCULATE([Encours Total], gdw_synthese_credit[flag_cdl] = TRUE())", "#,0"),
    ("NPL", "DIVIDE([Montant en Défaut], [Encours Total])", "0.0%"),
    ("Valeur Garanties", "SUM(fait_garanties[mnt_total_garantie])", "#,0"),
    ("Taux Couverture", "DIVIDE([Valeur Garanties], [Encours Total])", "0.0%"),
    ("Risque Net", "[Encours Total] - [Valeur Garanties]", "#,0"),
    ("Montant Autorisé", "SUM(fait_autorisations[montant_autorise])", "#,0"),
    ("Montant Utilisé", "SUM(fait_autorisations[montant_utilise])", "#,0"),
    ("Taux Utilisation", "DIVIDE([Montant Utilisé], [Montant Autorisé])", "0.0%"),
    ("Montant Dépassement", "SUM(fait_depassement[mnt_depassement])", "#,0"),
]

# ── Colonnes calculées (sur gdw_impayes) ───────────────────────────────────────
CALC_COLUMNS = {
    "gdw_impayes": [
        ("Classe CDL",
         'SWITCH(gdw_impayes[cod_statut_incident], "PD","1-Pré-douteux", "DT","2-Douteux", '
         '"CP","3-Compromis", "IM","0-Impayé", "RB","Sain", BLANK())', "string"),
        ("Aging Jours",
         "DATEDIFF(gdw_impayes[date_mise_impaye], gdw_impayes[date_arrete], DAY)", "int64"),
        ("Tranche Aging",
         'SWITCH(TRUE(), ISBLANK(gdw_impayes[date_mise_impaye]), "Non daté", '
         'gdw_impayes[Aging Jours] <= 30, "0-30j", gdw_impayes[Aging Jours] <= 60, "30-60j", '
         'gdw_impayes[Aging Jours] <= 90, "60-90j", "90j+")', "string"),
    ],
}

TMDL_TYPE = {"text": "string", "int": "int64", "num": "double", "numfr": "double",
             "date": "dateTime", "bool": "boolean"}


def m_convert(col: str, kind: str) -> str | None:
    """Fonction M de conversion de valeur (None si la colonne reste en texte)."""
    if kind == "int":
        return f'{{"{col}", each try Int64.From(_) otherwise null}}'
    if kind == "num":
        return f'{{"{col}", each try Number.FromText(_, "en-US") otherwise null}}'
    if kind == "numfr":
        return f'{{"{col}", each try Number.FromText(Text.Replace(_, ",", "."), "en-US") otherwise null}}'
    if kind == "date":
        return ('{"%s", each try #date(Number.FromText(Text.Start(_,4)), '
                'Number.FromText(Text.Middle(_,5,2)), Number.FromText(Text.Middle(_,8,2))) '
                'otherwise null}') % col
    if kind == "bool":
        return f'{{"{col}", each if _ = null then null else Text.Lower(_) = "true"}}'
    return None


def m_type(col: str, kind: str) -> str | None:
    mp = {"int": "Int64.Type", "num": "type number", "numfr": "type number",
          "date": "type date", "bool": "type logical"}
    return f'{{"{col}", {mp[kind]}}}' if kind in mp else None


def build_m(table: str, csv_path: str) -> list[str]:
    """Construit l'expression M (Power Query) pour une table, ligne par ligne."""
    cols = TABLES[table]
    ncols = len(cols)
    full = f'{csv_path}\\{CSV_FILE[table]}'
    conv = [m_convert(c, k) for c, k in cols if m_convert(c, k)]
    typ = [m_type(c, k) for c, k in cols if m_type(c, k)]

    lines = ["let"]
    lines.append(f'    Source = Csv.Document(File.Contents("{full}"), '
                 f'[Delimiter=",", Columns={ncols}, Encoding=65001, QuoteStyle=QuoteStyle.Csv]),')
    last = "Promoted"
    lines.append("    Promoted = Table.PromoteHeaders(Source, [PromoteAllScalars=true])"
                 + ("," if (table in DEDUP_ON or conv) else ""))
    if table in DEDUP_ON:
        lines.append(f'    Deduped = Table.Distinct(Promoted, {{"{DEDUP_ON[table]}"}})'
                     + ("," if conv else ""))
        last = "Deduped"
    if conv:
        src = "Promoted" if table not in DEDUP_ON else "Deduped"
        lines.append(f"    Converted = Table.TransformColumns({src}, {{")
        for i, c in enumerate(conv):
            lines.append(f"        {c}" + ("," if i < len(conv) - 1 else ""))
        lines.append("    }),")
        lines.append("    Typed = Table.TransformColumnTypes(Converted, {")
        for i, c in enumerate(typ):
            lines.append(f"        {c}" + ("," if i < len(typ) - 1 else ""))
        lines.append("    })")
        last = "Typed"
    lines.append("in")
    lines.append(f"    {last}")
    return lines


def col_block(col: str, kind: str) -> list[str]:
    return [
        f"{T}column {col}",
        f"{T}{T}dataType: {TMDL_TYPE[kind]}",
        f"{T}{T}summarizeBy: none",
        f"{T}{T}sourceColumn: {col}",
        "",
        f"{T}{T}annotation SummarizationSetBy = Automatic",
        "",
    ]


def calc_col_block(name: str, expr: str, dtype: str) -> list[str]:
    return [
        f"{T}column '{name}' = {expr}",
        f"{T}{T}dataType: {dtype}",
        f"{T}{T}summarizeBy: none",
        "",
    ]


def measure_block(name: str, dax: str, fmt: str) -> list[str]:
    return [
        f"{T}measure '{name}' = {dax}",
        f"{T}{T}formatString: {fmt}",
        "",
    ]


def table_tmdl(table: str, csv_path: str) -> str:
    out = [f"table {table}", ""]
    for c, k in TABLES[table]:
        out += col_block(c, k)
    for name, expr, dtype in CALC_COLUMNS.get(table, []):
        out += calc_col_block(name, expr, dtype)
    if table == "gdw_synthese_credit":
        for name, dax, fmt in MEASURES:
            out += measure_block(name, dax, fmt)
    out += [f"{T}partition {table} = m", f"{T}{T}mode: import", f"{T}{T}source ="]
    for ln in build_m(table, csv_path):
        out.append(f"{T}{T}{T}{T}{ln}")
    out.append("")
    return "\n".join(out)


def relationships_tmdl() -> str:
    out = []
    for name, ft, fc, tt, tc in RELATIONSHIPS:
        out += [f"relationship {name}", f"{T}fromColumn: {ft}.{fc}", f"{T}toColumn: {tt}.{tc}", ""]
    return "\n".join(out)


def model_tmdl() -> str:
    order = json.dumps(list(TABLES.keys()), ensure_ascii=False)
    return "\n".join([
        "model Model",
        f"{T}culture: en-US",
        f"{T}defaultPowerBIDataSourceVersion: powerBI_V3",
        f"{T}sourceQueryCulture: en-US",
        "",
        f"{T}annotation PBI_QueryOrder = {order}",
        "",
    ])


def platform(item_type: str, name: str, logical_id: str) -> str:
    return json.dumps({
        "$schema": "https://developer.microsoft.com/json-schemas/fabric/gitIntegration/"
                   "platformProperties/2.0.0/schema.json",
        "metadata": {"type": item_type, "displayName": name},
        "config": {"version": "2.0", "logicalId": logical_id},
    }, indent=2)


def report_json() -> str:
    pages = [("d1", "D1 - Pilotage"), ("d2", "D2 - Risque & Impayés"),
             ("d3", "D3 - Production"), ("d4", "D4 - Autorisations"),
             ("d5", "D5 - Garanties")]
    sections = [{
        "name": n, "displayName": d, "filters": "[]", "ordinal": i,
        "visualContainers": [], "config": "{}", "displayOption": 1,
        "width": 1280, "height": 720,
    } for i, (n, d) in enumerate(pages)]
    cfg = {"version": "5.43", "activeSectionIndex": 0, "settings": {}}
    return json.dumps({
        "config": json.dumps(cfg),
        "layoutOptimization": 0,
        "sections": sections,
    }, indent=2, ensure_ascii=False)


def write(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    # Power BI EXIGE de l'UTF-8 SANS BOM pour TMDL *et* JSON du projet
    # (« Only text with UTF8 encoding without BOM is supported »). On écrit donc tout
    # en utf-8 strict (jamais utf-8-sig). NB : transférer par Git (sans perte), pas par
    # clé USB/disque qui peut réencoder les fichiers.
    path.write_text(content, encoding="utf-8")
    print(f"  ✓ {path}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Génère le projet Power BI .pbip Crédit-Garantie.")
    ap.add_argument("--csv-path", default=r"C:\datalake\exports",
                    help=r"Dossier des CSV côté Windows (def: C:\datalake\exports)")
    ap.add_argument("--out", default="powerbi", help="Dossier de sortie (def: powerbi)")
    args = ap.parse_args()

    root = Path(args.out) / "CreditGarantie"
    sm = root.parent / "CreditGarantie.SemanticModel"
    rep = root.parent / "CreditGarantie.Report"
    print(f"Génération projet Power BI -> {root.parent} (CSV: {args.csv_path})")

    # .pbip
    write(root.parent / "CreditGarantie.pbip", json.dumps({
        "version": "1.0",
        "artifacts": [{"report": {"path": "CreditGarantie.Report"}}],
        "settings": {"enableAutoRecovery": True},
    }, indent=2))

    # Semantic model
    write(sm / ".platform", platform("SemanticModel", "CreditGarantie",
                                      "11111111-1111-1111-1111-111111111111"))
    write(sm / "definition.pbism", json.dumps({"version": "4.2", "settings": {}}, indent=2))
    write(sm / "definition" / "database.tmdl", "database\n\tcompatibilityLevel: 1567\n")
    write(sm / "definition" / "model.tmdl", model_tmdl())
    for t in TABLES:
        write(sm / "definition" / "tables" / f"{t}.tmdl", table_tmdl(t, args.csv_path))
    write(sm / "definition" / "relationships.tmdl", relationships_tmdl())

    # Report (vide, 5 pages)
    write(rep / ".platform", platform("Report", "CreditGarantie",
                                      "22222222-2222-2222-2222-222222222222"))
    write(rep / "definition.pbir", json.dumps({
        "version": "1.0",
        "datasetReference": {"byPath": {"path": "../CreditGarantie.SemanticModel"}},
    }, indent=2))
    write(rep / "report.json", report_json())

    print("\n[DONE] Ouvre CreditGarantie.pbip dans Power BI Desktop (Windows).")


if __name__ == "__main__":
    main()
