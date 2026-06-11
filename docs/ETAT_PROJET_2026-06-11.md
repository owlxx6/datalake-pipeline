# État du projet — résultats session 2026-06-10/11

Suite de `ETAT_PROJET_2026-06-07.md`. Tout est committé sur `main` (HEAD `e3ae481`).

## Résumé exécutif

1. **Framework de normalisation déclaratif** (pilier 3) — réalisé, actif sur 13/13 tables.
2. **Validation empirique du diagramme de jointures** — 13/13 testables : 10 confirmées,
   **3 corrigées** (0% → 99,5-100%).
3. **Dimension ORG** ingérée **via NiFi** — 7e dimension SCD2.
4. **Re-baseline complet** du Silver normalisé + Gold recalculé — non-régression exacte.
5. **2 fixes de performance moteur** (plan Catalyst + lignage DQ).

## 1. Normalisation (Phase N — réalisée)

- `jobs/bronze/normalization.py` : ops déclaratives (trim/ltrim/rtrim, upper/lower,
  collapse_spaces, remove_spaces, nullify_empty, replace regex, lpad/rpad).
- Config : `normalization.defaults.string: [trim, nullify_empty]` (global) + override par
  champ. Placement : Bronze (brut) → NORMALISATION → dédup → DQ → Silver (normalisé).
- Conforme aux **RG EDD** (position fixe → trim ; MONTANT/TAUX/DATE déjà gérés par
  `_smart_cast` au typage — alignement constaté avec le docx RG des flux EDD).

## 2. Jointures — validation contre la donnée réelle

Scripts : `test_jointures_diagramme.py` (13 jointures, taux de match) +
`diag_jointures_ko.py` (formats + hypothèses). Corrections :

| Diagramme (faux) | Réel (validé) | Taux |
|---|---|---|
| COD_PRODUIT = IDF_CODE_PRODUIT (0%) | COD_PRODUIT = **POPS.COD_SOP** | 99,49% |
| IDF_REF_AUTO = IDF_REF_EXT_CTR (0%) | **NUM_COMPTE** (IDCE) = IDF_REF_EXT_CTR | 100% |
| IDF_REF_EXT_CREDIT = IDF_REF_EXT_CTR (0%) | = **CREDIT_AMORT.IDF_NUM_DOSS** (lien direct) | 100% |

Cause racine des 3 : rapprochement par **similarité de noms**. La sémantique EDD l'avait
signalé (« code activité », « référence autorisation », « référence BTA », NUM_CTR_CREDIT
« non exploitée »). Chemin produit officiel encadrant confirmé : CONCAT(typ, ref) = 100%.
Confirmées aussi : hub crédit 99,99% / dépassement 100% / tiers-typologie-portefeuille 100% /
impayés→crédit 100% / garantie_contrat→detail 99,91% / portefeuille→ORG 100%.
À valider par l'encadrant : la jointure COD_SOP. Non testable : chaîne garantie_tiers.

## 3. ORG (7e dimension)

Ingestion **NiFi** (GetFile → UpdateAttribute → ExecuteScript csv→parquet → PutHDFS),
762 lignes, 40 colonnes, PK composite GUI+BUR. Partition renommée à l'arrêté réel
(20260611→20260531 ; le `now()` du flow = limite connue, cible : date depuis le nom de
fichier, standard RG EDD). Pipeline complet : Bronze + norm (39 col.) + DQ (4 règles,
100% PASSED) + SCD2 v1. Jointure portefeuille→ORG : 100%.

## 4. Re-baseline normalisé (13/13) + Gold

- 7 dims SCD2 v1 normalisées : tiers 154245 · image_contrat 158322 · grc 151528 ·
  typologie 154249 · asal 4464 · pops 1226 · org 762.
- 6 faits réécrits : credit_amort 200000 (DQ identique : 176281/23719/0) ·
  impayes_cdl 1597242 · depassement 75870 · garantie_detail 253900 ·
  garantie_contrat 258473 · idce_autorisation 368567.
- **Découverte qualité** : les 103 526 ex-REJECTED d'IMPAYES_CDL (PK vide) = une même
  ligne de remplissage répétée (padding) → dédup parfaite en garde 1. Bronze conserve
  les 1 700 767 brutes. WARNING 44023 (vides→NULL vus par not_null — effet voulu).
- **Gold recalculé** : dim_temps 4018 · dim_produit 1226 · gdw_encours 200000 ·
  gdw_impayes 1597242 · OBT 200000. Vérif : famille_renseignee **198 981** (la jointure
  COD_SOP exécutée — était 0 avant correction), **flag_cdl 96 474** (identique).
- KPI métier démontré sans jointure (OBT) : répartition crédits/CDL par famille
  (CREDIT IMMEDIAT 128738/47% · FOGARIME 81,8% CDL · …).

## 5. Fixes performance moteur

1. `normalize()` en **projection unique** — N `withColumn` empilés faisaient exploser
   l'optimisation Catalyst (tables larges figées).
2. **`localCheckpoint` post-normalisation** — la DQ relançait tout le plan PAR règle
   (~40 recalculs sur CREDIT_AMORT, figé >15 min → 392 s, non-régression DQ exacte).
- Levier futur : DQ en une passe d'agrégation (1 job au lieu de N).

## Commits de la session

`88ca047` plan DQ+ · `bcc3927` framework normalisation · `41dade0` corrections jointures
· `7f4f7d9` ORG via NiFi (13/13) · `e3ae481` perf + re-baseline.

## Reste à faire (inchangé + nouveaux)

1. Modèle Gold final après validation métier (+ `dim_organisation` en config Gold).
2. Validation encadrant : jointure COD_SOP + les 2 autres corrections.
3. Phase DQ+ (alignement modèle banque : colonnes opérationnelles + advanced_controls).
4. DQ une passe (perf) · revoir REJECTED sur PK nulle (demande Taha : flag sans rejet —
   à cadrer : REJECTED ne supprime rien déjà, question de sémantique du statut).
5. Airflow, Power BI, CI/CD, Atlas/Ranger (perspectives à réaliser avant soutenance).
