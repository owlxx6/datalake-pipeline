# Rapport — Modélisation Gold pour les 5 dashboards Crédit-Garantie

But : couvrir les 5 tableaux de bord cibles (Pilotage, Risque & Impayés, Production,
Autorisations, Garanties) par le modèle dimensionnel Gold. Pour chaque KPI : la formule,
la source, et le(s) dashboard(s) concerné(s). Tout est fondé sur des champs source
**vérifiés** dans le Silver (aucun champ inventé).

---

## 1. Réponse directe : ces faits sont-ils dans le modèle en étoile ?

**Oui.** Notre Gold est un **schéma en constellation** (galaxy schema) = plusieurs tables de
faits qui partagent les **mêmes dimensions conformes**. Il l'était déjà (`gdw_encours` +
`gdw_impayes` partagent `dim_temps` / `dim_produit` / `dim_organisation`).

Les 3 nouveaux faits (`fait_garanties`, `fait_autorisations`, `fait_depassement`) sont des
**tables de faits supplémentaires de la MÊME constellation** : ils se branchent sur les
dimensions conformes existantes (+ la nouvelle `dim_tiers`). Aucun modèle parallèle.

- **Étoile** = 1 fait au centre + dimensions autour.
- **Constellation** = plusieurs étoiles qui mutualisent les dimensions. ← notre cas.
- L'OBT `gdw_synthese_credit` n'est PAS l'étoile : c'est une couche **dénormalisée**
  (one-big-table) posée par-dessus pour le dashboard consolidé. L'étoile reste la
  référence ; l'OBT est un confort de restitution.

---

## 2. Dimensions conformes (partagées par tous les faits)

| Dimension | Source Silver | Mode | Attributs clés | Statut |
|--|--|--|--|--|
| `dim_temps` | générée | — | date, mois, trimestre, année | ✅ existe |
| `dim_produit` | pops (SCD2) | scd2_current | cod_sop (clé jointure), cod_famille, lib_famille, cod_activite | ✅ existe |
| `dim_organisation` | org (SCD2) | scd2_current | idf_num_gui, agence, dir, groupe, région | ✅ existe |
| `dim_tiers` | tiers (SCD2) + typologie | scd2_current | idf_num_tiers, raison sociale, **segment** (typologie), catégorie marché | 🔴 à construire |

Jointure segment : `tiers ↔ typologie` via `IDF_NUM_TIERS` (validée diagramme).

---

## 3. Tables de faits (constellation)

### Existantes
| Fait | Grain | Mesures | Source |
|--|--|--|--|
| `gdw_encours` | dossier × date_arrete | encours (capital restant dû), mnt_credit, tx_credit, duree | credit_amort |
| `gdw_impayes` | créance × date_arrete | mnt_impayes_cdl, mnt_creance | impayes_cdl |

### À construire (donnée source confirmée)
| Fait | Grain | Mesures | Source + champ | Jointure vers l'étoile |
|--|--|--|--|--|
| `fait_garanties` | contrat garantie × date_arrete | `MNT_TOTAL_GARANTIE`, `POURCENTAGE_GARANTIE` ; dim dégénérée `COD_TYP_GARANT` | garantie_detail (+ garantie_contrat) | garantie_contrat.`IDF_REF_EXT_CREDIT` = credit.`IDF_NUM_DOSS` → dim_produit, dim_tiers ; date → dim_temps |
| `fait_autorisations` | autorisation × date_arrete | `MNT_AUTORISATION` ; dégénérée `COD_TYP_AUTO` | idce_autorisation | `NUM_COMPTE` = image_contrat.`IDF_REF_EXT_CTR` → crédit/tiers/produit |
| `fait_depassement` | dépassement × date_arrete | `MNT_DEPASSEMENT` | depassement | `NUM_COMPTE` = image_contrat.`IDF_REF_EXT_CTR` → crédit |

Toutes les jointures sont celles **validées par le diagramme** (cf. CLAUDE.md). Faits =
snapshots par `date_arrete` (historisation par accumulation, comme l'existant).

---

## 4. Catalogue des KPIs & taux

Légende statut : ✅ dispo · 🟢 dispo après construction des faits §3 · ⛔ donnée absente.

### Mesures de base (additives)
| KPI | Formule | Source | Dashboards | Statut |
|--|--|--|--|--|
| Encours | `SUM(gdw_encours.encours)` | gdw_encours | D1, D2, D3 | ✅ |
| Montant crédit | `SUM(mnt_credit)` | gdw_encours | D1, D3 | ✅ |
| Nombre de crédits | `COUNT(DISTINCT idf_num_doss)` | gdw_encours | D3 | ✅ |
| Montant impayés | `SUM(mnt_impayes_cdl)` | gdw_impayes | D1, D2 | ✅ |
| Nb créances impayées | `COUNT(*)` | gdw_impayes | D2 | ✅ |
| Montant en défaut | `SUM(encours) WHERE flag_cdl` | gdw_encours/OBT | D1, D2 | ✅ |
| Valeur garanties | `SUM(mnt_total_garantie)` | fait_garanties | D1, D5 | 🟢 |
| Montant autorisé | `SUM(mnt_autorisation)` | fait_autorisations | D4 | 🟢 |
| Montant dépassement | `SUM(mnt_depassement)` | fait_depassement | D4 | 🟢 |

### Taux & ratios (calculés dans une vue, comme `v_kpi_credit`)
| Taux | Formule | Briques requises | Dashboards | Statut |
|--|--|--|--|--|
| Ticket moyen | `SUM(mnt_credit) / COUNT(DISTINCT idf_num_doss)` | gdw_encours | D3 | ✅ |
| Taux d'impayé | `SUM(impayés) / SUM(encours)` | gdw_encours + gdw_impayes | D1, D2 | ✅ |
| Poids impayé / créance | `SUM(impayés) / SUM(mnt_credit)` | idem | D2 | ✅ |
| NPL | `SUM(encours WHERE flag_cdl) / SUM(encours)` | gdw_encours (flag_cdl) | D2 | ✅ |
| Taux de couverture | `SUM(garanties) / SUM(encours)` | fait_garanties + gdw_encours | D5, D1 | 🟢 |
| Exposition nette / Risque net | `SUM(encours) − SUM(min(garanties, encours))` | idem | D1, D5 | 🟢 |
| Taux d'utilisation | `SUM(encours utilisé) / SUM(montant autorisé)` | gdw_encours + fait_autorisations | D4 | 🟢 |

### Agrégations / répartitions
| Vue | Calcul | Dashboards | Statut |
|--|--|--|--|
| Production mensuelle | `SUM(mnt_credit)` group by `month(date_accord)` | D3 | ✅ |
| Répartition par type de crédit | `SUM(encours)` group by `dim_produit.cod_famille` | D1 | ✅ |
| Répartition garanties par type | `SUM(garanties)` group by `cod_typ_garant` | D5 | 🟢 |
| Garanties par produit | `SUM(garanties)` group by `dim_produit` | D5 | 🟢 |
| Par agence | `SUM(encours/mnt_credit)` group by `dim_organisation` | D3 | 🟢 (pont crédit→agence) |
| Top clients à risque | `TOP N dim_tiers` by `SUM(encours WHERE flag_cdl)` | D1 | 🟢 (dim_tiers) |
| Top créances en défaut | `TOP N` by `SUM(mnt_creance)` filtré CDL | D2 | ✅ |
| Matrice segment × produit | `SUM(encours)` ou taux d'impayé group by `segment × famille` | D2 | 🟢 (dim_tiers) |
| Aging des impayés | `SUM(impayés)` bucket by `datediff(date_arrete, DATE_ECHEANCE)` : 0-30 / 31-60 / 61-90 / 90j+ | D2 | 🟢 (à confirmer DATE_ECHEANCE sur impayes_cdl) |

---

## 5. Détail par dashboard

### D1 — Pilotage Crédit Consolidé
Faits : gdw_encours, gdw_impayes, fait_garanties · Dims : dim_produit, dim_tiers, dim_temps.
KPIs : Encours · Impayés · Valeur garanties · Exposition nette · Taux d'impayé ·
Répartition par type de crédit · Top clients à risque · Encours vs Impayés (tendance).

### D2 — Suivi Risque & Impayés
Faits : gdw_encours, gdw_impayes · Dims : dim_tiers (segment), dim_produit, dim_temps.
KPIs : Taux d'impayé · NPL · Montant en défaut · Aging des impayés · Top créances en défaut ·
Matrice segment × produit · Évolution des impayés. **⛔ Taux de recouvrement (absent).**

### D3 — Suivi Production Crédit
Faits : gdw_encours · Dims : dim_produit, dim_organisation, dim_temps.
KPIs : Nombre crédits · Montant total · Ticket moyen · Production mensuelle (date_accord) ·
Par produit · Par agence. **⛔ Taux d'acceptation (absent).**

### D4 — Suivi des Autorisations
Faits : fait_autorisations, fait_depassement, gdw_encours · Dims : dim_temps, dim_tiers.
KPIs : Montant autorisé · Montant utilisé (= encours rattaché) · Taux d'utilisation ·
Dépassements autorisés. **⛔ Funnel Demande→Accepté→Décaissé (absent).**

### D5 — Suivi des Garanties
Faits : fait_garanties, gdw_encours · Dims : dim_produit, dim_temps.
KPIs : Valeur garanties · Taux de couverture · Risque net · Répartition par type
(hypothèque/caution/autres via `cod_typ_garant`) · Couverture vs Encours · Garanties par produit.

---

## 6. KPIs NON réalisables (donnée absente — à signaler, pas à inventer)

| KPI | Dashboard | Raison |
|--|--|--|
| Taux de recouvrement | D2 | aucun champ « montant recouvré » dans les sources |
| Taux d'acceptation | D3 | pas de statut demande/refus dans les 13 tables actives |
| Funnel Demande→Accepté→Décaissé | D4 | cycle de vie absent (nécessiterait les tables IDCE *event*, hors périmètre) |

→ Options : les retirer des dashboards, ou les positionner en **perspective** dans le rapport
(« nécessite l'intégration des tables événementielles IDCE »).

---

## 7. Schéma constellation (vue d'ensemble)

```
                    dim_temps
                        |
   dim_tiers ── gdw_encours ── dim_produit
        |        /   |   \           |
        |  fait_garanties \     fait_autorisations
        |        |         \         |
   dim_organisation     gdw_impayes  fait_depassement
                        (toutes branchées sur les dims conformes)

   OBT gdw_synthese_credit  = couche dénormalisée par-dessus (restitution D1)
```

Cinq faits, quatre dimensions conformes. Chaque fait partage les dimensions → constellation.

---

## 8. Plan d'implémentation (déclaratif — cibles YAML à ajouter)

Aucun code à écrire : on étend `configs/gold/credit_garantie.yaml`.
1. `dim_tiers` (type dimension, source silver tiers, scd2_current, + jointure typologie pour le segment)
2. `fait_garanties` (type fact, source garantie_detail, jointure garantie_contrat → crédit)
3. `fait_autorisations` (type fact, source idce_autorisation, jointure image_contrat)
4. `fait_depassement` (type fact, source depassement, jointure image_contrat)
5. Étendre la vue `v_kpi_credit` (ou nouvelles vues) avec les taux : couverture, risque net,
   utilisation, NPL, aging.

⚠️ À confirmer avant build : champ `DATE_ECHEANCE` exact sur impayes_cdl (pour l'aging).
