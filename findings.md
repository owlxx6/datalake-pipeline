# Findings — Analyse canevas (EDD) + échantillons

## Confirmation historisation
Les 13 EDD (≈30 tables) déclarent tous `Type stockage = STOCK`, fréquence quotidienne.
→ Le Full Snapshot partitionné est la méthode imposée par le mode de livraison contractuel.

## Inventaire des 30 tables (use case credit_garantie)

| Domaine EDD | Tables |
|-------------|--------|
| ASAL | ASAL |
| AUTORISATIONS | CREDIT_NON_AMORT |
| CREDIT | CREDIT_AMORT, CREDIT_RH, NEVOLAN_CONTRAT, NEVOLAN_TA, CREDIT_AMORT_TA |
| DEPASSEMENT | DEPASSEMENT |
| GARWEB | GARANTIE_DETAIL, _TIERS, _CONTRAT, _INSCRIPTION, _BIEN, _RENOUVELLEMENT |
| IDCE | IDCE_DOSSIER, _AUTORISATION, _TIERS_DOSSIER, _EVENT_DOSSIER, _EVENT_AUTO, _TIERS_AUTO, _LIENS_AUTO |
| IMAGE_CONTRAT | IMAGE_CONTRAT |
| IMPAYES_CDL | IMPAYES_CDL |
| ORG | ORG |
| POPS | POPS |
| PORTEFEUILLE | GRC_PORTEFEUILLE |
| TIERS | TIERS, CONTACTS, REPRESENTANT_LEG, TIERS_EI |
| TYPOLOGIE | TYPOLOGIE |

Param sheets (non-tables) : CODE_EVNT, CODE_TRANSA_GEST.

## 3 familles selon le signal de changement
- **Famille 1 — signal MAJ natif** : IMAGE_CONTRAT (`DATE_MODIF`, `COD_MOD_MODIF`),
  garantie_contrat / garantie_details (`DATE_MAJ_OCCURENCE`), idce_autorisation (`DATE_MAJ`).
- **Famille 2 — intervalles de validité intégrés** : TYPOLOGIE (`DATE_EFFET_*`/`DATE_FIN_*`),
  TIERS (`DATE_DEB_ADRESSE`/`DATE_FIN_ADRESSE`). SCD2 déjà dans la donnée — à préserver.
- **Famille 3 — aucun signal, montants mouvants** : credit_amort (`MNT_CAPITAL_REST_DU`),
  impayes_cdl (`MNT_INTERET_RETARD`, `MNT_PENALITE_RETARD`), depassement. Comparaison par hash.

## Findings techniques (à traiter Phase 1)
1. **Traçabilité incohérente** — `asal`, `pops`, `image_contrat` portent `COD_ENTITE`/`DATE_CHARGEMENT` ;
   `tiers`, `credit_amort`, `garantie_*`, `idce_*`, `impayes_cdl` ne les ont pas dans le CSV.
2. **Encodage** — `garantie_details.csv` en ISO-8859-1 (`Hypoth�que`), pas UTF-8.
3. **Format décimal européen** — montants en `+000000000700000,000` (virgule, signe, padding).
4. **Dates hétérogènes** — `dd/MM/yyyy HH:mm:ss` vs `yyyy-MM-dd HH:mm:ss.SSSSSS` ;
   `0001-01-01`=null, `9999-12-31`=date ouverte.
5. **PK manquante** — NEVOLAN_TA et DEPASSEMENT sans PK déclarée dans l'EDD.

## Bugs identifiés dans le code
- **Silver `partitionOverwriteMode`** — SparkSession ne configure pas ce paramètre → défaut STATIC.
  `write_silver` fait `.mode("overwrite").saveAsTable()` → écrase TOUTE la table à chaque run.
  Bronze est sûr (écriture path-based, 1 répertoire par date).
- **DQ COD_CYC_VIE** — `credit_garantie.yaml` a `allowed_values: ["T","A","S","R","E"]`,
  l'EDD codifie B/C/I/T. Cause des rejets massifs IMAGE_CONTRAT (225/250).

## Schémas YAML actuels tronqués
- TIERS configuré : 10 champs / EDD : ~120
- IMAGE_CONTRAT configuré : 11 champs / EDD : ~50

## Fichiers de référence
- EDD : `canevas/EDD_MODEL_*.xlsx` (13 fichiers)
- Échantillons : `echantillon/*.csv` (12) + `TIERS.xlsx`, `IMAGE_CONTRAT.xlsx`
- Code : `jobs/bronze/engine.py`, `jobs/bronze/config_loader.py`
- Config : `configs/bronze/credit_garantie.yaml`
- Outil d'inspection : `scripts/inspect_codifications.py`

## Anomalies à discuter avec le métier (run 2026-05-23, date_arrete=2026-05-14)

### TIERS — 46 lignes flaggées / 1000

**Codes inconnus** (probablement à étendre dans le référentiel ou erreurs de saisie)
- COD_PCS : 14 lignes avec '0000' (placeholder ?)
- COD_SEC_ACTIVITE : 13 lignes avec '011', '050' (codes hors EDD)
- COD_TRIBUNAL : 14 lignes avec '010', '090', '070', '050' (codes hors EDD)
- TOP_EMAILING : 3 lignes avec '1','2','3' (EDD dit N/O — codification a changé ?)
- TOP_REP_LEGAL : 5 lignes avec '2' (EDD dit 0/1 — extension ?)

**Champs avec valeur fixe à 100%**
- COD_NPAI : 1000 lignes (100%) avec 'N' — pourquoi systématiquement N ?

**Champs presque jamais renseignés** (pas de règle not_null, donc non flaggées — à voir si on doit les flagger)
- COD_ETAT_PROF (99.6% empty), COD_INDICATEUR_TVA (99.1%), COD_RELA_ENTITE (99.5%)
- COD_PCS (96.6%), TYP_CLASSIFICATION (98.4%), COD_CYC_VIE_CLASSIF (98.4%)
- TOP_VIP, TOP_MAILING, TOP_SMS (100% empty)

### IMAGE_CONTRAT — 248 lignes flaggées / 250

**Champ presque jamais renseigné — flaggé**
- COD_MTF_CYC_VIE : 248/250 lignes vides (99.2%) — champ déprécié, optionnel, ou défaut de saisie ?

**Exception isolée**
- COD_TYP_ADRESSE : 1 ligne vide / 250 (anomalie ponctuelle)

## Conventions retenues
- Méthode d'historisation : **Full Snapshot STOCK partitionné** par `date_arrete` (confirmé EDD pour les 31 tables)
- Empty string sémantiquement = NULL pour `not_null` et `in_list`
- `dq_failed_checks` format : `["FIELD:check_type", ...]` (array string)
- `dq_severity_max` : NULL si PASSED, sinon CRITICAL > HIGH > MEDIUM > LOW
- Iceberg DQ tables : **append-only strict**, audit cumulatif (filtrer par `execution_id` pour le dernier run)
- 2 tables DQ Iceberg : LOG_PIPELINE + QUALITY_RESULTS (LOGS_REJECTED supprimée)
