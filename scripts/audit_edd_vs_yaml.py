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

# table YAML -> (fichier EDD, onglet) — gère les typos d'onglets
MAP = {
    "ASAL": ("EDD_MODEL_ASAL.xlsx", "ASAL"),
    "CREDIT_NON_AMORT": ("EDD_MODEL_AUTORISATIONS.xlsx", "CREDIT_NON_AMORT"),
    "CREDIT_AMORT": ("EDD_MODEL_CREDIT.xlsx", "CREDIT_AMORT"),
    "CREDIT_RH": ("EDD_MODEL_CREDIT.xlsx", "CREDIT_RH"),
    "NEVOLAN_CONTRAT": ("EDD_MODEL_CREDIT.xlsx", "NEVOLAN_CONTRAT"),
    "NEVOLAN_TA": ("EDD_MODEL_CREDIT.xlsx", "NEVOLAN_TA"),
    "CREDIT_AMORT_TA": ("EDD_MODEL_CREDIT.xlsx", "CREDIT_AMORT_TA"),
    "DEPASSEMENT": ("EDD_MODEL_DEPASSEMENT.xlsx", "DEPASSEMENT"),
    "GARANTIE_DETAIL": ("EDD_MODEL_GARWEB.xlsx", "GARANTIE_DETAIL"),
    "GARANTIE_TIERS": ("EDD_MODEL_GARWEB.xlsx", "GARANTIE_TIERS"),
    "GARANTIE_CONTRAT": ("EDD_MODEL_GARWEB.xlsx", "GARANTIE_CONTRAT"),
    "GARANTIE_INSCRIPTION": ("EDD_MODEL_GARWEB.xlsx", "GARANTIE_INSCRIPTION"),
    "GARANTIE_BIEN": ("EDD_MODEL_GARWEB.xlsx", "GARANTIE_BIEN"),
    "GARANTIE_RENOUVELLEMENT": ("EDD_MODEL_GARWEB.xlsx", "GARANTIE_RENOUVELLEMENT"),
    "IDCE_DOSSIER": ("EDD_MODEL_IDCE.xlsx", "IDCE_DOSSIER"),
    # NB : la donnée physique 'idce_autorisation' correspond au canevas AUTORISATIONS
    # (onglet mal nommé 'CREDIT_NON_AMORT'), pas à l'onglet IDCE_AUTORISATION du fichier IDCE.
    "IDCE_AUTORISATION": ("EDD_MODEL_AUTORISATIONS.xlsx", "CREDIT_NON_AMORT"),
    "IDCE_TIERS_DOSSIER": ("EDD_MODEL_IDCE.xlsx", "ICDE_TIERS_DOSSIER"),
    "IDCE_EVENT_DOSSIER": ("EDD_MODEL_IDCE.xlsx", "IDCE_EVENT_DOSSIER"),
    "IDCE_EVENT_AUTO": ("EDD_MODEL_IDCE.xlsx", "IDCE_EVENT_AUTO"),
    "IDCE_TIERS_AUTO": ("EDD_MODEL_IDCE.xlsx", "IDCE_TIERS_AUTO"),
    "IDCE_LIENS_AUTO": ("EDD_MODEL_IDCE.xlsx", "IDCE_LIEN_AUTO"),
    "IMAGE_CONTRAT": ("EDD_MODEL_IMAGE_CONTRAT.xlsx", "IMAGE_CONTRAT"),
    "IMPAYES_CDL": ("EDD_MODEL_IMPAYES_CDL.xlsx", "IMPAYES_CDL"),
    "ORG": ("EDD_MODEL_ORG.xlsx", "ORG"),
    "POPS": ("EDD_MODEL_POPS.xlsx", "POPS"),
    "GRC_PORTEFEUILLE": ("EDD_MODEL_PORTEFEUILLE.xlsx", "GRC_PORTFEUILLE"),
    "TIERS": ("EDD_MODEL_TIERS.xlsx", "TIERS"),
    "CONTACTS": ("EDD_MODEL_TIERS.xlsx", "CONTACTS"),
    "REPRESENTANT_LEG": ("EDD_MODEL_TIERS.xlsx", "REPRESENTANT_LEG"),
    "TIERS_EI": ("EDD_MODEL_TIERS.xlsx", "TIERS_EI"),
    "TYPOLOGIE": ("EDD_MODEL_TYPOLOGIE.xlsx", "TYPOLOGIE"),
}

TRUE_FLAGS = {"O", "OUI", "X", "1", "TRUE"}


def parse_edd(path: Path, sheet: str):
    """Retourne (fields, codif) :
      fields : {champ: {'pk':bool,'oblig':bool,'codif':bool}}
      codif  : {champ: set(codes)}
    """
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb[sheet]
    rows = [list(r) for r in ws.iter_rows(values_only=True)]
    wb.close()
    # ligne d'en-tête
    hi = next(i for i, r in enumerate(rows) if any("Champ EDD" in str(v) for v in r if v))
    off = [j for j, v in enumerate(rows[hi]) if str(v).strip() == "Champ EDD"][0]

    def cell(r, k):
        return str(r[off + k]).strip() if len(r) > off + k and r[off + k] not in (None, "") else ""

    # début du bloc codif (CHAMP / CODE / LIBELLE)
    ci = None
    for i in range(hi + 1, len(rows)):
        if cell(rows[i], 0) == "CHAMP" and cell(rows[i], 1) == "CODE":
            ci = i
            break
    field_end = ci if ci is not None else len(rows)

    fields = {}
    for i in range(hi + 1, field_end):
        champ = cell(rows[i], 0)
        if not champ or champ.startswith("VARCHAR") or champ == "CHAMP":
            continue
        fields[champ] = {
            "pk": cell(rows[i], 4).upper() in TRUE_FLAGS,
            "oblig": cell(rows[i], 5).upper() in TRUE_FLAGS,
            "codif": cell(rows[i], 6).upper() in TRUE_FLAGS,
        }

    codif = {}
    if ci is not None:
        cur = None
        for i in range(ci + 1, len(rows)):
            champ = cell(rows[i], 0)
            code = cell(rows[i], 1)
            if champ:
                cur = champ
            if cur and code:
                codif.setdefault(cur, set()).add(code)
    return fields, codif


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


def audit(name: str, table: dict) -> list[str]:
    if name not in MAP:
        return [f"  (pas de canevas EDD mappé)"]
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

    # PK
    edd_pk = sorted([f for f, v in efields.items() if v["pk"]])
    if edd_pk and set(edd_pk) != set(ypk):
        issues.append(f"  [PK-DIFF]    EDD={edd_pk}  YAML={ypk}")

    for f, v in efields.items():
        yr = yrules.get(f, {"not_null": False, "unique": False, "in_list": None})
        # codifiable EDD sans in_list YAML
        if v["codif"] and yr["in_list"] is None:
            extra = f" (EDD: {sorted(ecodif.get(f, []))})" if f in ecodif else ""
            issues.append(f"  [CODIF-MANQ] {f} codifiable EDD, pas d'in_list YAML{extra}")
        # oblig EDD sans not_null YAML (hors PK)
        if v["oblig"] and not yr["not_null"] and f not in ypk:
            issues.append(f"  [OBLIG-MANQ] {f} Oblig=O EDD, pas de not_null YAML")
        # comparaison valeurs si in_list des deux côtés
        if yr["in_list"] is not None and f in ecodif:
            miss = ecodif[f] - yr["in_list"]
            trop = yr["in_list"] - ecodif[f]
            if miss:
                issues.append(f"  [VAL-MANQ]   {f} : EDD non listés YAML → {sorted(miss)}")
            if trop:
                issues.append(f"  [VAL-TROP]   {f} : YAML hors EDD → {sorted(trop)}")

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
