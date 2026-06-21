#!/usr/bin/env python3
"""Audit des KPI Power BI — recalcule chaque indicateur depuis les CSV exportés du Gold.

But : prouver que les chiffres affichés dans les dashboards Power BI = la donnée source
(contrôle de cohérence pour le rapport PFE). Gère les décimales virgule (autorisations /
dépassements) et le point (reste). Arrêté de référence : 2026-05-31.

  python3 scripts/audit_kpi.py            # lit ./exports/*.csv
  python3 scripts/audit_kpi.py --exports /chemin/exports
"""
from __future__ import annotations

import argparse
import csv
from pathlib import Path


def col(path: Path, c: str, comma: bool = False, cond=None) -> float:
    tot = 0.0
    for r in csv.DictReader(open(path, encoding="utf-8")):
        if cond and not cond(r):
            continue
        v = (r.get(c) or "").strip()
        if not v:
            continue
        if comma:
            v = v.replace(",", ".")
        try:
            tot += float(v)
        except ValueError:
            pass
    return tot


def cnt_distinct(path: Path, c: str) -> int:
    return len({(r.get(c) or "") for r in csv.DictReader(open(path, encoding="utf-8"))
                if (r.get(c) or "")})


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit KPI Power BI vs source Gold.")
    ap.add_argument("--exports", default="exports", help="Dossier des CSV (def: exports)")
    args = ap.parse_args()
    e = Path(args.exports)

    enc = col(e / "gdw_synthese_credit.csv", "encours")
    cred = col(e / "gdw_synthese_credit.csv", "mnt_credit")
    nbcr = cnt_distinct(e / "gdw_synthese_credit.csv", "idf_num_doss")
    defaut = col(e / "gdw_synthese_credit.csv", "encours",
                 cond=lambda r: (r.get("flag_cdl") or "").lower() == "true")
    imp = col(e / "gdw_impayes.csv", "mnt_impayes_cdl")
    crea = col(e / "gdw_impayes.csv", "mnt_creance")
    gar = col(e / "fait_garanties.csv", "mnt_total_garantie")
    auto = col(e / "fait_autorisations.csv", "montant_autorise", comma=True)
    util = col(e / "fait_autorisations.csv", "montant_utilise", comma=True)
    dep = col(e / "fait_depassement.csv", "mnt_depassement", comma=True)

    rows = [
        ("Encours Total", enc), ("Montant Crédit", cred), ("Nombre Crédits", nbcr),
        ("Ticket Moyen", cred / nbcr), ("Montant en Défaut", defaut),
        ("Montant Impayés", imp), ("Montant Créances", crea), ("Valeur Garanties", gar),
        ("Montant Autorisé", auto), ("Montant Utilisé", util), ("Montant Dépassement", dep),
    ]
    print("=== KPI (arrêté 2026-05-31) ===")
    for name, val in rows:
        print(f"{name:<20}: {val:>18,.0f}")
    print("--- Taux ---")
    print(f"Taux Impayé     = Impayés/Encours    = {imp/enc*100:5.1f}%")
    print(f"NPL             = Défaut/Encours     = {defaut/enc*100:5.1f}%")
    print(f"Taux Couverture = Garanties/Encours  = {gar/enc*100:5.1f}%")
    print(f"Taux Utilisation= Utilisé/Autorisé   = {util/auto*100:5.1f}%")
    print(f"Risque Net      = Encours - Garanties = {(enc-gar):,.0f}")


if __name__ == "__main__":
    main()
