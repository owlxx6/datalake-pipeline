# Audit des KPI — Dashboards Power BI Crédit-Garantie

**Objectif** : prouver que chaque chiffre affiché dans les 5 dashboards Power BI correspond
exactement à la donnée source (couche Gold). Contrôle de cohérence pour le rapport PFE.

- **Arrêté de référence** : 2026-05-31 (seul arrêté chargé en POC).
- **Source** : CSV exportés du Gold (`exports/`), eux-mêmes issus des tables Iceberg.
- **Méthode** : recalcul indépendant via [`scripts/audit_kpi.py`](../scripts/audit_kpi.py)
  (gère les décimales virgule des autorisations/dépassements et le point ailleurs).
- **Reproductible** : `python3 scripts/audit_kpi.py`

## Résultat — tous les KPI concordent ✅

| KPI | Formule | Source (table.colonne) | Valeur recalculée | Affiché dashboard |
|--|--|--|--|--|
| Encours Total | `SUM(encours)` | gdw_synthese_credit.encours | 23 188 113 696 (23,19 Md) | 23 Md ✅ |
| Montant Crédit | `SUM(mnt_credit)` | gdw_synthese_credit.mnt_credit | 113 243 749 787 (113,24 Md) | 113 Md ✅ |
| Nombre Crédits | `DISTINCTCOUNT(idf_num_doss)` | gdw_synthese_credit.idf_num_doss | 200 000 | 200 K ✅ |
| Ticket Moyen | `Montant Crédit / Nombre Crédits` | — | 566 219 | 566 K ✅ |
| Montant en Défaut | `SUM(encours) WHERE flag_cdl=true` | gdw_synthese_credit | 7 039 396 680 (7,04 Md) | 7 Md ✅ |
| Montant Impayés | `SUM(mnt_impayes_cdl)` | gdw_impayes.mnt_impayes_cdl | 20 885 678 858 (20,89 Md) | 21 Md ✅ |
| Montant Créances | `SUM(mnt_creance)` | gdw_impayes.mnt_creance | 19 915 284 878 (19,92 Md) | 19,9 Md ✅ |
| Valeur Garanties | `SUM(mnt_total_garantie)` | fait_garanties.mnt_total_garantie | 2 410 997 666 (2,41 Md) | 2 Md ✅ |
| Montant Autorisé | `SUM(montant_autorise)` | fait_autorisations.montant_autorise | 249 736 060 669 (249,74 Md) | 250 Md ✅ |
| Montant Utilisé | `SUM(montant_utilise)` | fait_autorisations.montant_utilise | 109 487 264 186 (109,49 Md) | 109 Md ✅ |
| Montant Dépassement | `SUM(mnt_depassement)` | fait_depassement.mnt_depassement | 1 724 646 290 (1,72 Md) | 1,72 Md ✅ |
| **Taux Impayé** | `Impayés / Encours` | — | **90,1 %** | 90,1 % ✅ |
| **NPL** | `Défaut / Encours` | — | **30,4 %** | 30,4 % ✅ |
| **Taux Couverture** | `Garanties / Encours` | — | **10,4 %** | 10,4 % ✅ |
| **Taux Utilisation** | `Utilisé / Autorisé` | — | **43,8 %** | 43,8 % ✅ |
| **Risque Net** | `Encours − Garanties` | — | 20,78 Md | 21 Md (arrondi) ✅ |

## Conclusion
Les KPI sont **mécaniquement justes** : la chaîne Silver → Gold → export → modèle Power BI →
mesures DAX ne dénature aucun chiffre. La cohérence est vérifiée de bout en bout.

## Limite à assumer (honnêteté)
Les **données sont synthétiques** (environnement de test). Certains ratios sont donc
irréalistes pour une vraie banque — notamment **Taux Impayé 90,1 %** et **NPL 30,4 %**,
car `gdw_impayes` additionne tout le détail historique des créances (1,6 M lignes), pas le
seul encours impayé courant. À présenter comme **validation du framework**, pas comme un
chiffre métier réel.

## Notes d'interprétation
- Les cartes « Montant Dépassement » peuvent afficher une valeur partielle si une ligne du
  tableau associé est sélectionnée (filtrage croisé Power BI) — désélectionner pour le total.
- Volumes Gold sous-jacents : gdw_synthese_credit 200 000 · gdw_impayes 1 597 242 ·
  fait_garanties 253 900 · fait_autorisations 368 567 · fait_depassement 75 870.
