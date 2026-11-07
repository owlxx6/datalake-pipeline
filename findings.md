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

## 2026-06-08 — Architecture réelle du projet (fournie par Taha, diagramme "Architecture DATALAKE")
Image : `docs/rapport/figures/architecture_datalake.png` → figure clé du Chapitre 3 (Conception).
Plateforme **Cloudera** (Data Lake on-prem), traitement **batch** (pas de temps réel).

### Sources (gauche)
- Sources non-structurées + semi-structurées.
- Bloc **SOP** (systèmes sources opérationnels) : IDCHAB, IDCE, Evolan, BTA,
  Système comptable, CDL, Dépassements.
- Acheminement legacy : **SAP Data Services** + dépôt **SFTP "SOP"** (systèmes sources/consommateurs).

### Ingestion
- **Apache NiFi** (connecteurs Java + ingestion des nouvelles sources) — **SEUL outil d'ingestion utilisé**.
- (Le 2ᵉ bloc d'ingestion du schéma n'a PAS été utilisé — correction Taha.)

### Orchestration
- **Apache Airflow**.

### Traitement
- **Apache Spark (PySpark)** : "Transformation with Spark", étapes clean & normalize (Bronze→Silver)
  et aggregate (Silver→Gold).
- **JupyterLab + Spark** pour l'équipe Data Science.

### Stockage (HDFS, médaillon en Apache Iceberg)
- **Bronze (Landing Zone)** : brut/Parquet, Iceberg.
- **Silver** : Hive + Iceberg (données nettoyées/normalisées).
- **Gold** : Hive + Iceberg (agrégats).
- **Datamart** : **Apache Impala** + Iceberg.
- Couche **Data Quality** entre Spark et le stockage.

### Gouvernance & sécurité (Cloudera SDX)
- **Apache Ranger** (administration & sécurité) ↔ TagSync ↔ **Apache Atlas** (gouvernance,
  documentation, metadata store).
- **BoA Active Directory + Kerberos (KDC)** → sync user group → Ranger.
- **Cloudera Manager** (monitoring + logs applicatifs).
- **Grafana** → Alertes (Email ou SNMP).

### CI/CD
- **Nexus Repo**, **GitLab** (repository), **Jenkins**.

### Visualisation / BI
- Schéma montre **Tableau** MAIS Taha utilise **Power BI** (correction) → Data Quality Dashboard
  + Data Visualisation.

### Corrections Taha (à respecter dans tout le rapport)
1. Ingestion = **NiFi uniquement**.
2. Visualisation = **Power BI** (pas Tableau).

### Existant (avant le Data Lake) — confirmé par Taha (2026-06-08)
- L'ancien existant Crédit-Garantie reposait sur **SAP** (extraction/ETL via **SAP Data Services**).
- Le projet vise à moderniser/standardiser cette exploitation vers un Data Lake Cloudera (médaillon).
- Systèmes sources (bloc SOP) : IDCHAB, IDCE, Evolan, BTA, Système comptable, CDL, Dépassements.
- Base du Chapitre 2 (Existant) : présentation SAP + limites documentées (pas de Golden Source,
  pas de Data Lineage, doublons/incohérences, traitements manuels, pas d'archi unifiée médaillon).

## 2026-06-08 — Périmètre RÉALISÉ vs CIBLE pour le Chapitre 4 (confirmé Taha)
Source chiffres : docs/ETAT_PROJET_2026-06-07.md (arrêté réel 2026-05-31). NE PAS utiliser
les pilotes de mai (TIERS 1000/46, IMAGE_CONTRAT 250/248 = périmés).

### RÉALISÉ (validé sur la VM)
- **Ingestion → Bronze → Silver : 12 tables industrialisées** (run 2026-05-31). Exemples :
  credit_amort 200 000 ; impayes_cdl 1 700 767 ; tiers 154 245 ; image_contrat 158 322 ;
  grc_portefeuille 151 528 ; typologie 154 249 ; pops 1 226 ; asal 4 464 (+ depassement,
  garantie_detail, idce_autorisation, garantie_contrat).
- **Moteur DQ 3 états** PASSED/WARNING/REJECTED + audit Iceberg LOG_PIPELINE / QUALITY_RESULTS.
- **Historisation** : point d'entrée historize() → snapshot (faits, date_arrete) /
  SCD2 (dimensions). **6 dimensions SCD2 hash** (valid_from/valid_to/is_current/version/row_hash),
  activées par flag YAML : pops, asal, image_contrat, grc_portefeuille, tiers, typologie.
  Difficulté résolue : NPE Iceberg 1.3.1 (copy-on-write) → write_silver_scd2 reconstruit l'état
  100% en DataFrame puis createOrReplace.
- **Couche Gold (moteur déclaratif config-driven, 4 cibles : date/dimension/fact/obt)** —
  prouvée arrêté 2026-05-31 : dim_temps (4 018 j), dim_produit (1 226, SCD2 état courant),
  gdw_encours (200 000, **jointure as-of** tiers gelé 199 981/200 000 sans fan-out),
  gdw_impayes (1 700 767), **OBT gdw_synthese_credit** (200 000, agrégat 1:N + dérivé
  flag_cdl = 96 474 CDL).

### CIBLE / PERSPECTIVES (PAS réalisé)
- Airflow (DAG squelette d'avril non aligné, jamais exécuté ; runs = spark-submit manuels).
- Atlas / Ranger (aucune implémentation, seulement dans les docs de plan).
- Datamart Impala. Power BI (Gold prêt à brancher, rien fait). CI/CD.
- Reste : fix padding codes (cod_produit paddé ≠ idf_code_produit → cod_famille NULL en OBT),
  détection de variation inter-arrêtés (N vs N-1), dimension ORG (table à venir).
