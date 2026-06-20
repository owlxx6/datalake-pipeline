#!/usr/bin/env python3
"""Génère une petite table de TENDANCE (totaux mensuels) pour les courbes Power BI.

⚠️ DÉMO UNIQUEMENT — un seul arrêté réel est chargé (2026-05-31). Les courbes
« Encours vs Impayés » (D1) et « Évolution des impayés » (D2) ont besoin de plusieurs
arrêtés. On simule donc janvier→avril 2026 ; **mai 2026 = valeurs RÉELLES**, les mois
antérieurs sont marqués `simulé`. À étiqueter clairement sur le dashboard.

Le point mai est recalculé depuis exports/ si présents (sinon ancres figées).
Sortie : powerbi/tendance_simulee.csv  (1 ligne/mois : date_arrete, encours, impayes, type)

  python3 scripts/gen_tendance_simulee.py
"""
from __future__ import annotations

import csv
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
EXPORTS = ROOT / "exports"
OUT = ROOT / "powerbi" / "tendance_simulee.csv"

# Ancres RÉELLES mai 2026 (recalculées depuis exports/ si dispo)
ENCOURS_REEL = 23_188_113_696
IMPAYES_REEL = 20_885_678_858


def _sum_col(path: Path, col: str) -> float | None:
    if not path.exists():
        return None
    with open(path, encoding="utf-8") as f:
        r = csv.DictReader(f)
        return sum(float(row[col]) for row in r if row.get(col))


def main() -> None:
    encours = _sum_col(EXPORTS / "gdw_synthese_credit.csv", "encours") or ENCOURS_REEL
    impayes = _sum_col(EXPORTS / "gdw_impayes.csv", "mnt_impayes_cdl") or IMPAYES_REEL

    # Facteurs mensuels — encours en hausse lente, impayés en hausse plus rapide
    # (scénario de dégradation pour une démo de dashboard risque). Mai = 1.0 = réel.
    plan = [
        ("2026-01-31", 0.930, 0.820, "simulé"),
        ("2026-02-28", 0.950, 0.870, "simulé"),
        ("2026-03-31", 0.965, 0.910, "simulé"),
        ("2026-04-30", 0.985, 0.960, "simulé"),
        ("2026-05-31", 1.000, 1.000, "réel"),
    ]

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["date_arrete", "encours", "impayes", "type"])
        for date_arrete, fe, fi, typ in plan:
            w.writerow([date_arrete, round(encours * fe), round(impayes * fi), typ])

    print(f"[DONE] {OUT}")
    print(f"  mai 2026 (réel) : encours={encours:,.0f}  impayes={impayes:,.0f}")


if __name__ == "__main__":
    main()
