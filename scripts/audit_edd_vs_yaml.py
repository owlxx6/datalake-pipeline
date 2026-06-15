#!/usr/bin/env python3
"""Audit de gouvernance : compare les canevas EDD (canevas/EDD_MODEL_*.xlsx) avec
les règles DQ du YAML Bronze. Pour chaque table, signale les écarts :

  - [CODIF-MANQ]  champ Codifiable=O dans l'EDD mais sans règle in_list dans le YAML
  - [CODIF-TROP]  in_list dans le YAML alors que l'EDD ne marque pas le champ codifiable
  - [VAL-MANQ]    valeurs présentes dans l'EDD mais absentes des allowed_values
  - [VAL-TROP]    allowed_values du YAML absentes de l'EDD
  - [OBLIG-MANQ]  champ Oblig=O dans l'EDD sans not_null (et hors PK) dans le YAML
  - [PK-DIFF]     PK déclarée (EDD PK=O) ≠ primary_key du YAML

Lecture seule. N'écrit rien. Usage : python3 scripts/audit_edd_vs_yaml.py [TABLE ...]
"""
from __future__ import annotations
import sys
from pathlib import Path

import openpyxl
import yaml

ROOT = Path(__file__).resolve().parents[1]
YAML_PATH = ROOT / "configs/bronze/credit_garantie.yaml"
CANEVAS = ROOT / "canevas"

# Fichiers EDD partagés par plusieurs onglets/tables (constantes — évite la duplication).
EDD_CREDIT = "EDD_MODEL_CREDIT.xlsx"
EDD_GARWEB = "EDD_MODEL_GARWEB.xlsx"
EDD_IDCE = "EDD_MODEL_IDCE.xlsx"
EDD_TIERS = "EDD_MODEL_TIERS.xlsx"
EDD_AUTORISATIONS = "EDD_MODEL_AUTORISATIONS.xlsx"

# table YAML -> (fichier EDD, onglet) — gère les typos d'onglets
MAP = {
    "ASAL": ("EDD_MODEL_ASAL.xlsx", "ASAL"),
    "CREDIT_NON_AMORT": (EDD_AUTORISATIONS, "CREDIT_NON_AMORT"),
    "CREDIT_AMORT": (EDD_CREDIT, "CREDIT_AMORT"),
    "CREDIT_RH": (EDD_CREDIT, "CREDIT_RH"),
    "NEVOLAN_CONTRAT": (EDD_CREDIT, "NEVOLAN_CONTRAT"),
    "NEVOLAN_TA": (EDD_CREDIT, "NEVOLAN_TA"),
    "CREDIT_AMORT_TA": (EDD_CREDIT, "CREDIT_AMORT_TA"),
    "DEPASSEMENT": ("EDD_MODEL_DEPASSEMENT.xlsx", "DEPASSEMENT"),
    "GARANTIE_DETAIL": (EDD_GARWEB, "GARANTIE_DETAIL"),
    "GARANTIE_TIERS": (EDD_GARWEB, "GARANTIE_TIERS"),
    "GARANTIE_CONTRAT": (EDD_GARWEB, "GARANTIE_CONTRAT"),
    "GARANTIE_INSCRIPTION": (EDD_GARWEB, "GARANTIE_INSCRIPTION"),
    "GARANTIE_BIEN": (EDD_GARWEB, "GARANTIE_BIEN"),
    "GARANTIE_RENOUVELLEMENT": (EDD_GARWEB, "GARANTIE_RENOUVELLEMENT"),
    "IDCE_DOSSIER": (EDD_IDCE, "IDCE_DOSSIER"),
    # NB : la donnée physique 'idce_autorisation' correspond au canevas AUTORISATIONS
    # (onglet mal nommé 'CREDIT_NON_AMORT'), pas à l'onglet IDCE_AUTORISATION du fichier IDCE.
    "IDCE_AUTORISATION": (EDD_AUTORISATIONS, "CREDIT_NON_AMORT"),
    "IDCE_TIERS_DOSSIER": (EDD_IDCE, "ICDE_TIERS_DOSSIER"),
    "IDCE_EVENT_DOSSIER": (EDD_IDCE, "IDCE_EVENT_DOSSIER"),
    "IDCE_EVENT_AUTO": (EDD_IDCE, "IDCE_EVENT_AUTO"),
    "IDCE_TIERS_AUTO": (EDD_IDCE, "IDCE_TIERS_AUTO"),
    "IDCE_LIENS_AUTO": (EDD_IDCE, "IDCE_LIEN_AUTO"),
    "IMAGE_CONTRAT": ("EDD_MODEL_IMAGE_CONTRAT.xlsx", "IMAGE_CONTRAT"),
    "IMPAYES_CDL": ("EDD_MODEL_IMPAYES_CDL.xlsx", "IMPAYES_CDL"),
    "ORG": ("EDD_MODEL_ORG.xlsx", "ORG"),
    "POPS": ("EDD_MODEL_POPS.xlsx", "POPS"),
    "GRC_PORTEFEUILLE": ("EDD_MODEL_PORTEFEUILLE.xlsx", "GRC_PORTFEUILLE"),
    "TIERS": (EDD_TIERS, "TIERS"),
    "CONTACTS": (EDD_TIERS, "CONTACTS"),
    "REPRESENTANT_LEG": (EDD_TIERS, "REPRESENTANT_LEG"),
    "TIERS_EI": (EDD_TIERS, "TIERS_EI"),
    "TYPOLOGIE": ("EDD_MODEL_TYPOLOGIE.xlsx", "TYPOLOGIE"),
}

TRUE_FLAGS = {"O", "OUI", "X", "1", "TRUE"}


def _cell(row, off: int, k: int) -> str:
    idx = off + k
    return str(row[idx]).strip() if len(row) > idx and row[idx] not in (None, "") else ""


def _header_offset(rows) -> tuple[int, int]:
    """(ligne d'en-tête, offset de colonne) repérés via 'Champ EDD'."""
    hi = next(i for i, r in enumerate(rows) if any("Champ EDD" in str(v) for v in r if v))
    off = [j for j, v in enumerate(rows[hi]) if str(v).strip() == "Champ EDD"][0]
    return hi, off


def _codif_start(rows, hi: int, off: int):
    """Index de la 1ʳᵉ ligne du bloc codif (CHAMP / CODE), ou None."""
    for i in range(hi + 1, len(rows)):
        if _cell(rows[i], off, 0) == "CHAMP" and _cell(rows[i], off, 1) == "CODE":
            return i
    return None


def _parse_fields(rows, hi: int, field_end: int, off: int) -> dict:
    fields = {}
    for i in range(hi + 1, field_end):
        champ = _cell(rows[i], off, 0)
        if not champ or champ.startswith("VARCHAR") or champ == "CHAMP":
            continue
        fields[champ] = {
            "pk": _cell(rows[i], off, 4).upper() in TRUE_FLAGS,
            "oblig": _cell(rows[i], off, 5).upper() in TRUE_FLAGS,
            "codif": _cell(rows[i], off, 6).upper() in TRUE_FLAGS,
        }
    return fields


def _parse_codif(rows, ci, off: int) -> dict:
    codif = {}
    if ci is None:
        return codif
    cur = None
    for i in range(ci + 1, len(rows)):
        champ = _cell(rows[i], off, 0)
        code = _cell(rows[i], off, 1)
        if champ:
            cur = champ
        if cur and code:
            codif.setdefault(cur, set()).add(code)
    return codif


def parse_edd(path: Path, sheet: str):
    """Retourne (fields, codif) :
      fields : {champ: {'pk':bool,'oblig':bool,'codif':bool}}
      codif  : {champ: set(codes)}
    """
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    rows = [list(r) for r in wb[sheet].iter_rows(values_only=True)]
    wb.close()
    hi, off = _header_offset(rows)
    ci = _codif_start(rows, hi, off)
    field_end = ci if ci is not None else len(rows)
    return _parse_fields(rows, hi, field_end, off), _parse_codif(rows, ci, off)


def yaml_rules(table: dict):
    """Retourne {champ: {'not_null':bool,'unique':bool,'in_list':set|None}} + pk list."""
    out = {}
    for r in table.get("quality_rules", []):
        f = r["field"]
        d = out.setdefault(f, {"not_null": False, "unique": False, "in_list": None})
        for c in r["checks"]:
            if c["type"] == "not_null":
                d["not_null"] = True
            elif c["type"] == "unique":
                d["unique"] = True
            elif c["type"] == "in_list":
                d["in_list"] = {str(v).strip() for v in (c.get("allowed_values") or [])}
    return out, list(table.get("primary_key") or [])


_DEFAULT_YR = {"not_null": False, "unique": False, "in_list": None}


def _field_issues(f: str, v: dict, yr: dict, ecodif: dict, ypk: list) -> list[str]:
    """Écarts EDD↔YAML pour un champ (codif manquante, oblig, valeurs)."""
    out = []
    if v["codif"] and yr["in_list"] is None:
        extra = f" (EDD: {sorted(ecodif.get(f, []))})" if f in ecodif else ""
        out.append(f"  [CODIF-MANQ] {f} codifiable EDD, pas d'in_list YAML{extra}")
    if v["oblig"] and not yr["not_null"] and f not in ypk:
        out.append(f"  [OBLIG-MANQ] {f} Oblig=O EDD, pas de not_null YAML")
    if yr["in_list"] is not None and f in ecodif:
        miss = ecodif[f] - yr["in_list"]
        trop = yr["in_list"] - ecodif[f]
        if miss:
            out.append(f"  [VAL-MANQ]   {f} : EDD non listés YAML → {sorted(miss)}")
        if trop:
            out.append(f"  [VAL-TROP]   {f} : YAML hors EDD → {sorted(trop)}")
    return out


def audit(name: str, table: dict) -> list[str]:
    if name not in MAP:
        return ["  (pas de canevas EDD mappé)"]
    fname, sheet = MAP[name]
    path = CANEVAS / fname
    if not path.exists():
        return [f"  (fichier EDD introuvable : {fname})"]
    try:
        efields, ecodif = parse_edd(path, sheet)
    except Exception as e:
        return [f"  (erreur lecture EDD : {e})"]
    yrules, ypk = yaml_rules(table)

    issues = []
    edd_pk = sorted([f for f, v in efields.items() if v["pk"]])
    if edd_pk and set(edd_pk) != set(ypk):
        issues.append(f"  [PK-DIFF]    EDD={edd_pk}  YAML={ypk}")

    for f, v in efields.items():
        issues += _field_issues(f, v, yrules.get(f, _DEFAULT_YR), ecodif, ypk)

    # in_list YAML alors que pas codifiable EDD
    for f, yr in yrules.items():
        if yr["in_list"] is not None and f in efields and not efields[f]["codif"]:
            issues.append(f"  [CODIF-TROP] {f} in_list YAML mais EDD non-codifiable")

    return issues or ["  ✓ conforme"]


def main():
    cfg = yaml.safe_load(open(YAML_PATH, encoding="utf-8"))
    tables = {t["name"]: t for t in cfg["tables"]}
    wanted = sys.argv[1:] or list(tables.keys())
    n_issues = 0
    for name in wanted:
        if name not in tables:
            print(f"\n### {name} : absente du YAML")
            continue
        res = audit(name, tables[name])
        ok = res == ["  ✓ conforme"]
        print(f"\n### {name}" + ("  ✓" if ok else ""))
        for line in res:
            print(line)
        if not ok and res and not res[0].startswith("  ("):
            n_issues += sum(1 for l in res if l.strip().startswith("["))
    print(f"\n{'='*60}\nTotal écarts signalés : {n_issues}")


if __name__ == "__main__":
    main()
