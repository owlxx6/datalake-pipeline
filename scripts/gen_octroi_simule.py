#!/usr/bin/env python3
"""Ajoute la partie SIMULÉE par-dessus les agrégats RÉELS d'octroi.

Lit powerbi/funnel_octroi.csv (produit par extract_complement_kpi.py : Accordé + Décaissé
RÉELS) et :
  - réécrit funnel_octroi.csv en y ajoutant l'étape « Demande » SIMULÉE
    (Demande = Accordé / TAUX_ACCEPTATION) + une colonne `type` (simulé/réel) ;
  - écrit powerbi/kpi_simules.csv : Taux d'Acceptation et Taux de Recouvrement
    SIMULÉS (illustratifs — non calculables sur la donnée : pas de demandes refusées,
    et NBR_IMPAYES == NBR_IMP_NON_REG donc taux de régularisation réel = 0%).

⚠️ Hypothèses illustratives, À ÉTIQUETER « simulé » sur les dashboards.

  python3 scripts/gen_octroi_simule.py
"""
from __future__ import annotations

import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PB = ROOT / "powerbi"

# Hypothèses illustratives (alignées sur le mockup). Non issues de la donnée.
TAUX_ACCEPTATION = 0.72
TAUX_RECOUVREMENT = 0.65


def main() -> None:
    src = PB / "funnel_octroi.csv"
    rows = list(csv.DictReader(open(src, encoding="utf-8")))
    montants = {r["etape"]: float(r["montant"]) for r in rows}
    accorde = montants["Montant Accordé"]
    decaisse = montants["Montant Décaissé"]
    demande = round(accorde / TAUX_ACCEPTATION)  # étape simulée

    # Funnel 3 étapes : Demande (simulé) -> Accordé (réel) -> Décaissé (réel)
    with open(src, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["ordre", "etape", "montant", "type"])
        w.writerow([1, "Demande (simulé)", demande, "simulé"])
        w.writerow([2, "Montant Accordé", round(accorde), "réel"])
        w.writerow([3, "Montant Décaissé", round(decaisse), "réel"])

    # KPI simulés (cartes) — valeurs illustratives
    with open(PB / "kpi_simules.csv", "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["taux_acceptation", "taux_recouvrement"])
        w.writerow([TAUX_ACCEPTATION, TAUX_RECOUVREMENT])

    print("[DONE] funnel_octroi.csv (3 étapes) + kpi_simules.csv")
    print(f"  Demande (simulé) = {demande:,.0f}  (Accordé / {TAUX_ACCEPTATION})")
    print(f"  Accordé (réel)   = {accorde:,.0f}")
    print(f"  Décaissé (réel)  = {decaisse:,.0f}  (taux décaissement {decaisse/accorde*100:.1f}%)")
    print(f"  Taux Acceptation (simulé)  = {TAUX_ACCEPTATION:.0%}")
    print(f"  Taux Recouvrement (simulé) = {TAUX_RECOUVREMENT:.0%}")


if __name__ == "__main__":
    main()
