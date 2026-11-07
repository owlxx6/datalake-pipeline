# REPORT_DATA_PACK — données factuelles pour le rapport PFE

> Généré le 2026-06-10 depuis le repo `datalake-pipeline` (branche main, HEAD 88ca047).
> Règle : tout ce qui suit vient du repo, des logs de runs ou des docs de session.
> Ce qui n'existe pas est marqué **NON DISPONIBLE**.

## 1. Contexte entreprise

- **Eurafric Information (EAI)** : rôle, partenaires, Data Center, chiffres clés → **NON DISPONIBLE dans le repo** (le rapport en cours, chapitre 1, contient déjà des figures EAI réutilisées — cf. task_plan Phase R). À sourcer depuis les documents internes EAI.
- **Bank of Africa (BOA)** : présentation institutionnelle → **NON DISPONIBLE dans le repo**. Élément repo : le projet porte sur l'axe **Crédit-Garantie de BOA Entreprise** (README, CLAUDE.md).
- **Équipe d'accueil** : projet réalisé chez **Eurafric Information / BOA**.
  - Auteur : **HABBOU Taha Yassine** · Encadrant EMSI : **Mr Mohammed Redouane** · Encadrant entreprise : **Mr Reda Kilani** (Eurafric Information / BOA) — source : task_plan.md.
  - Projet jumeau : **EL FATIHI Salma** (même sujet, même entreprise, même encadrant) — source : progress.md.
  - Stage : **05/02/2026 → 30/06/2026** (source : task_plan, Ch.1 SCRUM).
- Référentiels internes fournis par l'équipe : RG de normalisation des flux EDD (docx), présentation technique EDD (pptx), framework DQ équipe DATA (`ACTIVITYCOMM.yaml`, export `quality_dq_engine_results.csv`).

## 2. Déroulé du projet (chronologie réelle, depuis git log)

| Période | Phase | Contenu |
|---|---|---|
| 2026-05-07 | Initialisation | squelette framework médaillon (`datalake_lib`, dags, configs) |
| 2026-05-15 | Moteur Bronze | Bronze DQ engine + config YAML groupé + runbook opérationnel |
| 2026-05-18 → 23 | Pilotes TIERS / IMAGE_CONTRAT | refactor Silver flags DQ (« on flague, on n'enlève pas »), générateur YAML 30 tables depuis les EDD, dry-run CLI, raffinement codifications (in_list/not_null vs chaînes vides), validation pipeline end-to-end sur les 2 pilotes |
| 2026-05-31 → 06-02 | Industrialisation 12 tables | statut DQ 3-états (PASSED/WARNING/REJECTED), checks informational, dédup Silver opt-in, politique EDD stricte + outil d'audit `audit_edd_vs_yaml.py`, reconstruction schéma IDCE_AUTORISATION ; 12 tables Landing industrialisées (arrêté réel 2026-05-31) |
| 2026-06-03 | H1 — Iceberg | migration `write_silver` en Apache Iceberg (12 tables), fix dédup (doublons PARFAITS uniquement), design H3 SCD2, rapport d'historisation .docx |
| 2026-06-05 | H3 — pilote SCD2 | SCD2 sur POPS (réécriture DataFrame anti-NPE), modèle dimensionnel figé (5 faits / 6 dims / 1 pont, diagramme jointures), résolution ambiguïté n°6 |
| 2026-06-07 | H3 complet + Gold | framework d'historisation unique (`historization.py`), 6/6 dimensions SCD2, framework Gold déclaratif (4 builders) + mécaniques as-of/OBT prouvées |
| 2026-06-08 → 10 | Normalisation + DQ+ | framework de normalisation déclaratif (module + config), alignement DQ banque mis au plan |

Commits significatifs (sélection, ordre chronologique) :
`61b8c64` Bronze DQ engine + runbook · `cefe4d4` Silver flags DQ + générateur YAML 30 tables ·
`741a946` CREDIT_AMORT + DQ 3-états · `40c6422` dédup Silver opt-in + TIERS · `589ab3d` DEPASSEMENT + audit EDD ·
`86cc62e` reconstruction IDCE_AUTORISATION · `f596aa7` H1 migration Iceberg · `c2cd1f6` dédup doublons parfaits ·
`68f0c77` SCD2 pilote POPS (anti-NPE) · `1a9e59c` modèle dimensionnel · `d934b02` framework historisation unique ·
`f862856`/`2a0f996`/`c041a2e`/`fe75d48`/`a4b1643` SCD2 ASAL/IMAGE_CONTRAT/GRC/TIERS/TYPOLOGIE ·
`2adaf35` framework Gold déclaratif · `756a3ca` as-of + OBT prouvés.

## 3. Stack technique — versions exactes

**RÉALISÉ (environnement de validation)** — sources : logs de runs + runbook + CLAUDE.md :
- VM **Ubuntu**, hostname `ubuntu3`, user `hadoop`, **9,5 Go RAM** (VirtualBox, dossier partagé `/media/sf_datalake-pipeline`).
- **Spark 3.4.0** (PySpark) — exécution réelle en **mode local** (`Spark master: local[*]` dans tous les logs de runs). Un cluster **Spark Standalone** (Master+Worker, UI :8080) est installé sur la VM (runbook) mais les runs validés sont en local. Driver memory : **4 g** (obligatoire, cf. OOM).
- **Apache Iceberg 1.3.1** (logs : `Apache Iceberg 1.3.1 (commit 62c34711...)`) — catalogue **Hive Metastore** (thrift://ubuntu3:9083).
- **HDFS** (hdfs://ubuntu3:9000) — réplication 1, NameNode UI :9870. Version Hadoop exacte : **NON DISPONIBLE** (à relever : `hadoop version`).
- **py4j 0.10.9.7** (livré avec Spark 3.4.0). Python VM : **NON DISPONIBLE** (à relever : `python3 --version`). Python local dev : 3.9 (macOS).
- **Apache NiFi** : installé sur la VM, flow `GetFile → UpdateAttribute → ExecuteScript → PutHDFS` (ingestion XLSX→HDFS) — partiellement utilisé ; le Landing Parquet est la source des runs.
- Validation pydantic v2 (config YAML), openpyxl (parsing canevas).

**CIBLE (architecture conçue, non réalisée)** : Spark sur YARN, distribution Cloudera, Airflow, Atlas, Ranger, Impala, Power BI, CI/CD GitLab/Jenkins/Nexus/SonarQube, Grafana.

## 4. Framework — arborescence et modules

### Arborescence réelle (fichiers clés)
```
jobs/
  bronze/
    config_loader.py     # modèles pydantic + loader YAML (Bronze/Silver)
    engine.py            # moteur générique Bronze→Silver (~850 lignes)
    historization.py     # framework historisation : historize() → snapshot | SCD2
    normalization.py     # framework normalisation déclarative (trim, replace, lpad…)
  gold/
    config_loader.py     # contrat pydantic du modèle Gold déclaratif
    engine.py            # moteur Gold : 4 builders (date/dimension/fact/obt)
configs/
  bronze/credit_garantie.yaml   # 31 tables : schémas, DQ, silver_write, scd2 (~9700 lignes)
  gold/credit_garantie.yaml     # modèle Gold déclaratif (dims, faits, OBT)
scripts/
  audit_edd_vs_yaml.py · inspect_codifications.py · generate_configs.py
  simulate_pops_arrete2.py · diag_scd2_pops.py · show_landing_schema.py
docs/   (runbook, modèles, états projet)   ·   canevas/   (12 EDD Excel)
```
NB : le package `datalake_lib/` (read/quality/write/log/config) est le squelette initial du
projet ; les moteurs opérationnels sont dans `jobs/`. Ne pas présenter `datalake_lib` comme
le framework qui tourne.

### Modules (rôle + points d'entrée)
- **`jobs/bronze/config_loader.py`** — modèles pydantic (TableEntry, SchemaField, SilverWriteConfig, Scd2Signal, NormalizationConfig, DQCheck…) + `load_config(path)`. Tout le pipeline d'une table est décrit en YAML, validé au chargement.
- **`jobs/bronze/engine.py`** — moteur générique : `run_table()` orchestre lecture Landing (Parquet/CSV) → `enforce_schema` (cast tolérant `_smart_cast` : virgule décimale, signe +, multi-formats dates, sentinelles 0001-01-01/9999-12-31) → `normalize` → `deduplicate_silver` (doublons parfaits) → `run_dq` (agrégats) → `compute_dq_flags` (par ligne) → `write_bronze` (Parquet brut) → `historize` (Silver Iceberg) → `write_logs` (audit). CLI : `--config --date-arrete [--table] [--dry-run]`.
- **`jobs/bronze/historization.py`** — `historize(df, table, config, spark, date_arrete)` : dispatch **snapshot** (faits, partition date_arrete, overwritePartitions) ou **SCD2** (dimensions, valid_from/valid_to/is_current/version/row_hash sha2, mouvement 100% DataFrame + createOrReplace). Activation : `silver_write.scd2: true` + `scd2_signal: {type: hash, columns: [...]}`.
- **`jobs/bronze/normalization.py`** — `normalize(df, table, config)` : ops déclaratives (trim/ltrim/rtrim, upper/lower, collapse_spaces, remove_spaces, nullify_empty, replace regex, lpad/rpad). Défauts globaux `normalization.defaults.string` + override par champ `schema.fields[].normalize`. Placé entre Bronze (brut) et Silver (normalisé). Conforme aux RG EDD (position fixe → trim ; MONTANT/TAUX/DATE déjà gérés par `_smart_cast`).
- **`jobs/gold/engine.py`** — moteur Gold config-driven : builders `date` (calendrier généré), `dimension` (depuis dim SCD2 Silver, mode scd2_current/scd2_versioned), `fact` (snapshot date_arrete + jointures as-of point-in-time sans fan-out), `obt` (base + dims aplaties + agrégats 1:N + colonnes dérivées). CLI : `--config --date-arrete [--target]`.

### Règles DQ : 7 types implémentés
`not_null` · `unique` (PK simple/composite) · `in_list` · `max_length` · `regex` · `date_valid` · `date_range`.
Sévérités CRITICAL/HIGH/MEDIUM/LOW + `alert_threshold` (taux de tolérance) + `informational` (hors statut ligne).
**3 états par ligne** : PASSED / WARNING / REJECTED — aucune ligne supprimée (flags `dq_status`, `dq_failed_checks`, `dq_severity_max`, `dq_execution_id`).
Audit Iceberg append-only : **LOG_PIPELINE** (1 ligne/exécution) + **QUALITY_RESULTS** (1 ligne/règle/exécution — colonnes : execution_id, date_situation, rule_id, rule_type, field_name, severity, records_checked, records_failed, failure_rate_pct, alert_threshold_pct, status, control_type, layer, table_name, domain, environment).

## 5. Modèle de données Gold (exact)

### 5 faits (snapshot, grain = clé × date_arrete)
| Fait | Source Silver | Mesure(s) | Grain |
|---|---|---|---|
| `gdw_encours` | credit_amort | mnt_capital_rest_du (encours), mnt_credit | dossier × arrêté |
| `gdw_impayes` | impayes_cdl | mnt_impayes_cdl, mnt_creance, intérêts/pénalités retard | créance × arrêté |
| `gdw_depassement` | depassement | mnt_depassement | dépassement × arrêté |
| `gdw_garantie` | garantie_detail | mnt_initial, mnt_actualise, mnt_total_garantie | garantie × arrêté |
| `gdw_autorisation` | idce_autorisation | mnt_auto_tot_dev, mnt_auto_deb_dev | autorisation × arrêté |

### 6 dimensions + hub (SCD2 en Silver)
| Dimension | Table Silver | PK | Famille SCD2 |
|---|---|---|---|
| `dim_temps` | (générée) | date_key | — (calendrier) |
| `dim_produit` | pops | IDF_CODE_PRODUIT | hash |
| `dim_tiers` | tiers | IDF_NUM_TIERS | hash (111 colonnes) |
| `dim_typologie` | typologie | IDF_NUM_TIERS | hash |
| `dim_agent` | asal | IDF_PERSONNEL | hash |
| `dim_portefeuille` | grc_portefeuille | IDF_NUM_GUI+IDF_NUM_TIERS+IDF_CODE_CC | hash |
| `bridge_image_contrat` (hub) | image_contrat | IDF_NUM_TIERS+IDF_REF_EXT_CTR+IDF_TYP_PRD+IDF_REF_PRD | hash |
+ 1 pont : `garantie_contrat` (PK IDF_REF_EXT_CREDIT+IDF_NUM_CTR_GARANTIE).
Jointure clé (diagramme validé) : tous les faits → hub via `IDF_REF_EXT_CTR` ; hub → tiers/produit.

### Colonnes SCD2
`row_hash` (sha2-256 des attributs historisés, NULL-safe) · `valid_from` · `valid_to` (NULL si courant) · `is_current` · `version` (incrémental par clé) · `load_ts`. Mouvement : clé nouvelle → v1 ; hash identique → rien ; hash différent → clôture + nouvelle version ; clé disparue → clôture (soft delete).

### OBT `gdw_synthese_credit`
Grain : **dossier crédit × date_arrete** (200 000 lignes à l'arrêté 2026-05-31). Construction : `gdw_encours` (base) + dims aplaties (`cod_famille`, `lib_famille` via dim_produit) + agrégats 1:N (`mnt_impaye_total = sum(mnt_impayes_cdl)`, `nb_creances_impayees = count(*)`) + dérivés (`flag_cdl = nb_creances_impayees > 0`, `taux_impaye = mnt_impaye_total / nullif(encours,0)`). Principe : un nouveau KPI = une formule SQL sur l'OBT, zéro nouvelle jointure, zéro refonte du modèle.

## 6. Chiffres réels (arrêté 2026-05-31)

### 12 tables Silver
| Table | Type | Lignes Silver | DQ notable |
|---|---|---|---|
| impayes_cdl | fait | 1 700 767 | 103 526 REJECTED |
| idce_autorisation | fait | 368 567 | schéma reconstruit (370k source) |
| garantie_contrat | pont | 258 473 | 100% PASSED |
| garantie_detail | fait | 253 900 | dédup 4 328 |
| credit_amort | fait | 200 000 | 23 719 WARNING |
| image_contrat | dim (hub) | 158 322 | 1 REJECTED |
| typologie | dim | 154 249 | 1 068 WARNING |
| tiers | dim | 154 245 | 14 069 WARNING |
| grc_portefeuille | dim | 151 528 | 100% PASSED |
| depassement | fait | 75 870 | dédup 445 130 (521k source) |
| asal | dim | 4 464 | 100% PASSED |
| pops | dim | 1 226 | 70 WARNING (PASSED 1 156) |

### Volumes Gold (validés VM)
- `dim_temps` : 4 018 jours · `dim_produit` : 1 226 (état courant SCD2)
- `gdw_encours` : 200 000 — jointure **as-of** : tiers gelé sur 199 981/200 000 (99,99%), zéro fan-out
- `gdw_impayes` : 1 700 767
- `gdw_synthese_credit` (OBT) : 200 000 — **flag_cdl = 96 474** crédits en CDL
- SCD2 démontré sur POPS (2 arrêtés simulés) : 3 modifiés → v1 clôturée + v2 ; 1 disparu → clôturé ; 1 nouveau → v1 ; total 1 230 lignes (puis nettoyé, datalake remis à l'état réel).

## 7. Difficultés rencontrées (réelles)

1. **NPE Iceberg 1.3.1 (MERGE copy-on-write)** — `NullPointerException: UTF8String.getBaseObject() because "input" is null` à la recopie des lignes inchangées (ReplaceDataExec), y compris codegen désactivé. **Solution** : mouvement SCD2 calculé 100% en DataFrame (clôturées/inchangées/nouvelles) + réécriture bulk `createOrReplace` — déterministe et portable.
2. **Padding des codes (RG « position fixe »)** — `cod_produit` (CREDIT_AMORT) ≠ `idf_code_produit` (POPS, stocké paddé ex. `40000      M`) → jointure Gold sans correspondance (`cod_famille` NULL). **Réponse** : framework de normalisation déclaratif (trim/nullify_empty globaux + ops par champ) ; inspection des clés en cours.
3. **Dédup : doublons parfaits vs collisions PK** — première version de la dédup perdait des lignes sur collision de PK. **Correction** : dédup restreinte aux doublons 100% identiques ; les collisions de PK sont conservées et flaguées REJECTED par le check `unique` (audit-only sous dédup).
4. **Parsing des canevas Excel (EDD)** — la ligne « Champ EDD » marque le DÉBUT de la section champs (pas la fin), offset de colonnes variable, PK paddée, sections DETAIL/CORPS/BODY — règles établies par essais successifs ; openpyxl read-only.
5. **EDD partiellement faux/corrompus** — ex. IDCE_AUTORISATION : EDD ne correspondait pas à la donnée réelle → schéma reconstruit depuis la donnée + le bon onglet EDD ; valeurs `allowed_values` EDD non paddées vs données paddées.
6. **Contraintes VM 9,5 Go** — OOM driver (saveAsTable 1,7M lignes) → `--driver-memory 4g` systématique ; HDFS safe mode après crash VM (blocs manquants, réplication 1) → procédure runbook.
7. **Dépassement de contexte de session** (outillage IA) — session de 3 semaines perdue « Prompt is too long », récupérée par extraction du transcript (749 messages) + synthèse → `docs/recap_session_c57cf119.md`, `docs/ETAT_PROJET_2026-06-03.md`.

## 8. EXTRAITS DE DONNÉES pour figures

### QUALITY_RESULTS — extrait réel
**NON DISPONIBLE en local** (la table vit sur HDFS/VM). Commande d'export à lancer sur la VM :
```bash
spark-sql --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  -e "SELECT table_name, rule_id, rule_type, severity, records_checked, records_failed, failure_rate_pct, status
      FROM silver_credit_garantie.br_credit_garantie_quality_results
      ORDER BY records_failed DESC LIMIT 15;" > /tmp/quality_extract.txt
```
Récap 3-états par table : utiliser le tableau §6 (colonne « DQ notable », chiffres des runs).

### gdw_synthese_credit — extrait réel (~10 lignes)
**NON DISPONIBLE en local.** Commande d'export :
```bash
spark-sql [mêmes --conf] -e "SELECT idf_num_doss, encours, mnt_credit, tx_credit,
  mnt_impaye_total, nb_creances_impayees, flag_cdl, taux_impaye
  FROM gold_credit_garantie.gdw_synthese_credit WHERE flag_cdl = true LIMIT 10;"
```
(5 lignes réelles déjà observées en session : dossiers 00000667 / 00000710 / 00002335 / 00002407 / 00010412 — encours 0.00, mnt_impaye_total 27 762,51 / 201 661,53 / 6 706,14 / 8 641,73 / 30 338,05, nb créances 7/19/2/5/4, flag_cdl true.)

### Descripteur YAML complet d'une table (POPS — réel, 204 lignes)
```yaml
    description: COD_ECV_BUR — codification EDD
    checks:
    - type: in_list
      severity: HIGH
      allowed_values:
      - A
      - C
      alert_threshold: 1.0
      control_type: BASIC
  consistency_rules:
  - name: pk_composite_complete
    description: PK composite (2 colonnes) toutes non-nulles
    condition: 1=1
    required_fields: *id015
    severity: CRITICAL
    alert_threshold: 0.0
    control_type: BASIC
- name: POPS
  domain: pops
  primary_key:
  - IDF_CODE_PRODUIT
  is_partitioned: false
  source:
    path_template: hdfs://ubuntu3:9000/data/landing/{table_name_lower}/date_arrete={date_arrete}
    format: parquet
    loading:
      mode: FULL
  schema:
    on_cast_error: reject
    fields:
    - name: IDF_CODE_PRODUIT
      spark_type: StringType
      nullable: false
      description: Code activité
    - name: COD_ACTIVITE
      spark_type: StringType
      nullable: true
      description: Libellé de l'activité
    - name: LIB_ACTIVITE
      spark_type: StringType
      nullable: true
      description: Code sous activité
    - name: COD_SS_ACTIVITE
      spark_type: StringType
      nullable: true
      description: Libellé de la sous-activité
    - name: LIB_SS_ACTIVITE
      spark_type: StringType
      nullable: true
      description: Code famille
    - name: COD_FAMILLE
      spark_type: StringType
      nullable: true
      description: Libellé de la famille
    - name: LIB_FAMILLE
      spark_type: StringType
      nullable: true
      description: Produit commercial
    - name: PROD_COMMERCIAL
      spark_type: StringType
      nullable: true
      description: Code du produit
    - name: LIB_PRODUIT
      spark_type: StringType
      nullable: true
      description: Libellé du produit
    - name: NOM_SOP
      spark_type: StringType
      nullable: true
      description: Nom du système opérant
    - name: COD_SOP
      spark_type: StringType
      nullable: true
      description: Code du système opérant
    - name: COD_USAGE
      spark_type: StringType
      nullable: true
      description: Usage
    - name: COD_RIBABLE
      spark_type: StringType
      nullable: true
      description: Ribable
    - name: LIB_COURT
      spark_type: StringType
      nullable: true
      description: Libelle produit court
    - name: COD_EDITION_RIB
      spark_type: StringType
      nullable: true
      description: Edition du RIB autorisée (O/N)
    - name: COD_TYP_PRODUIT
      spark_type: StringType
      nullable: true
      description: Type du produit
    - name: COD_TRANSAC_GESTION
      spark_type: StringType
      nullable: true
      description: transaction de gestion
    - name: COD_DECLARATIF_OSCAR
      spark_type: StringType
      nullable: true
      description: Déclaratif Oscar
    - name: LIB_CLIENT_LONG
      spark_type: StringType
      nullable: true
      description: libellé du client long
    - name: LIB_CLIENT_COURT
      spark_type: StringType
      nullable: true
      description: libellé du client court
  write:
    format: parquet
    path_template: hdfs://ubuntu3:9000/data/bronze/{domain}/{table_name}/date_arrete={date_arrete}
    mode: overwrite
    partition_by:
    - date_arrete
    compression: snappy
    coalesce: 4
  silver_write:
    database: silver_credit_garantie
    table: pops
    mode: overwrite
    partition_by:
    - date_arrete
    deduplicate: true
    scd2: true
    scd2_signal:
      type: hash
  quality_rules:
  - field: IDF_CODE_PRODUIT
    source_column: IDF_CODE_PRODUIT
    data_type: StringType
    description: PK IDF_CODE_PRODUIT — clé primaire
    checks:
    - type: not_null
      severity: CRITICAL
      alert_threshold: 0.0
      control_type: BASIC
    - type: unique
      severity: CRITICAL
      alert_threshold: 0.0
      control_type: BASIC
  - field: COD_DECLARATIF_OSCAR
    source_column: COD_DECLARATIF_OSCAR
    data_type: StringType
    description: COD_DECLARATIF_OSCAR — codification EDD
    checks:
    - type: not_null   # flag 'manquant' — distinct du in_list (ajouté 2026-05-31)
      severity: MEDIUM
      alert_threshold: 0.0
      control_type: BASIC
      informational: true   # systématique sur colonne optionnelle → hors statut ligne
    - type: in_list
      severity: HIGH
      allowed_values:
      - D
      - N
      - O
      alert_threshold: 1.0
      control_type: BASIC
  - field: COD_EDITION_RIB
    source_column: COD_EDITION_RIB
    data_type: StringType
    description: COD_EDITION_RIB — codification EDD
    checks:
    - type: not_null   # flag 'manquant' — distinct du in_list (ajouté 2026-05-31)
      severity: MEDIUM
      alert_threshold: 0.0
      control_type: BASIC
      informational: true   # systématique sur colonne optionnelle → hors statut ligne
    - type: in_list
      severity: HIGH
      allowed_values:
      - N
      - O
      alert_threshold: 1.0
      control_type: BASIC
  - field: COD_RIBABLE
    source_column: COD_RIBABLE
    data_type: StringType
    description: COD_RIBABLE — codification EDD
    checks:
    - type: not_null   # flag 'manquant' — distinct du in_list (ajouté 2026-05-31)
      severity: MEDIUM
      alert_threshold: 0.0
      control_type: BASIC
      informational: true   # systématique sur colonne optionnelle → hors statut ligne
    - type: in_list
      severity: HIGH
      allowed_values:
      - D
      - R
      - S
      alert_threshold: 1.0
      control_type: BASIC
  - field: COD_USAGE
    source_column: COD_USAGE
    data_type: StringType
    description: COD_USAGE — codification EDD
    checks:
    - type: not_null   # flag 'manquant' — distinct du in_list (ajouté 2026-05-31)
      severity: MEDIUM
      alert_threshold: 0.0
      control_type: BASIC
```

### Modèle Gold déclaratif (configs/gold/credit_garantie.yaml — réel)
```yaml
# Modèle Gold — axe Crédit-Garantie (déclaratif).
# PILOTE : dim_temps + dim_produit + gdw_encours. On étend après validation métier.
database: gold_credit_garantie

targets:

  # ── Dimension date (générée) ──────────────────────────────
  - name: dim_temps
    type: date
    range: {from: "2020-01-01", to: "2030-12-31"}

  # ── Dimension produit (depuis Silver SCD2, état courant) ──
  - name: dim_produit
    type: dimension
    source: silver_credit_garantie.pops
    mode: scd2_current
    key: [idf_code_produit]
    columns:
      - idf_code_produit
      - cod_activite
      - lib_activite
      - cod_famille
      - lib_famille
      - prod_commercial
      - lib_produit
      - cod_usage
      - cod_typ_produit

  # ── Fait encours (snapshot par date_arrete) ───────────────
  - name: gdw_encours
    type: fact
    source: silver_credit_garantie.credit_amort
    historisation: snapshot
    grain: [idf_num_doss, date_arrete]
    joins:
      # as-of : gèle le tiers tel qu'il était à l'arrêté (via le hub contrat)
      - with: silver_credit_garantie.image_contrat
        match: {num_compte_emprunteur: idf_ref_ext_ctr}
        as_of: date_arrete
        select: [idf_num_tiers]
    columns:
      - {src: idf_num_doss, role: pk}
      - {src: num_compte_emprunteur, role: fk, ref: bridge_image_contrat}
      - {src: idf_num_tiers, role: fk, ref: dim_tiers}
      - {src: cod_produit, role: fk, ref: dim_produit}
      - {src: date_accord, role: fk, ref: dim_temps}
      - {src: mnt_capital_rest_du, as: encours, role: measure}
      - {src: mnt_credit, role: measure}
      - {src: tx_credit, role: attribute}
      - {src: duree, role: attribute}
      - {src: cod_sit_doss, role: attribute}

  # ── Fait impayés (détail créance, snapshot) ───────────────
  - name: gdw_impayes
    type: fact
    source: silver_credit_garantie.impayes_cdl
    historisation: snapshot
    grain: [idf_num_dossier, idf_num_creance, date_arrete]
    columns:
      - {src: idf_num_dossier, as: idf_num_doss, role: fk, ref: gdw_encours}
      - {src: idf_num_creance, role: pk}
      - {src: num_compte, role: fk, ref: bridge_image_contrat}
      - {src: mnt_impayes_cdl, role: measure}
      - {src: mnt_creance, role: measure}
      - {src: cod_statut_incident, role: attribute}

  # ── OBT : synthèse crédit (agrégats 1:N + dims aplaties) ───
  - name: gdw_synthese_credit
    type: obt
    base: gdw_encours
    joins:
      - with: dim_produit
        match: {cod_produit: idf_code_produit}
        select: [cod_famille, lib_famille]
    aggregates:
      - from: gdw_impayes
        group_by: [idf_num_doss, date_arrete]
        measures:
          mnt_impaye_total: "sum(mnt_impayes_cdl)"
          nb_creances_impayees: "count(*)"
    derived:
      flag_cdl: "coalesce(nb_creances_impayees, 0) > 0"
      taux_impaye: "coalesce(mnt_impaye_total, 0) / nullif(encours, 0)"
```

## 9. Power BI

**Cible / perspectives — confirmé.** Aucun dashboard construit, aucune connexion établie (aucune trace dans le repo ; décision de session : « Power BI remplace Tableau » au niveau conception). La couche Gold Iceberg est prête à être consommée. Si des dashboards sont construits avant la soutenance, fournir les captures à ce moment-là.

## 10. Glossaire

| Terme | Définition |
|---|---|
| Architecture médaillon | Organisation du lac en couches de raffinage Bronze (brut) → Silver (validé) → Gold (décisionnel) |
| Bronze / Silver / Gold | Brut 1:1 source / typé+normalisé+flagué / modèle dimensionnel pour la BI |
| Lakehouse | Lac de données avec garanties d'entrepôt (ACID, schéma, time-travel) via format de table (Iceberg) |
| Apache Iceberg | Format de table ouvert : transactions ACID, évolution de schéma, time-travel, partitionnement caché |
| Hive Metastore | Catalogue central des métadonnées de tables (schémas, emplacements) |
| HDFS | Système de fichiers distribué Hadoop (stockage du lac) |
| YARN | Gestionnaire de ressources Hadoop (exécution distribuée Spark — cible) |
| Apache Spark / PySpark | Moteur de calcul distribué en mémoire / son API Python |
| Apache NiFi | Outil d'ingestion par flux visuels (sources → Landing) |
| Apache Airflow | Orchestrateur de traitements par DAG (planification, dépendances, reprise) |
| Apache Atlas | Catalogue de métadonnées + lineage (traçabilité bout-en-bout) |
| Apache Ranger | Sécurité centralisée : politiques d'accès par rôle (intégrable AD/Kerberos) |
| Apache Impala | Moteur SQL interactif à faible latence (datamart — cible) |
| DQ (Data Quality) | Contrôles de qualité déclaratifs ; ici 7 types, 3 états, « on flague, on n'enlève pas » |
| Snapshot (historisation) | Photo complète par arrêté, partitionnée par date_arrete, jamais écrasée |
| SCD2 | Slowly Changing Dimension type 2 : chaque changement d'attribut crée une version datée ; tout l'historique est conservé |
| row_hash | Empreinte SHA-256 des attributs : signal de détection de changement du SCD2 |
| as-of (jointure) | Jointure point-in-time : version de la dimension valide à la date du fait |
| OBT (One Big Table) | Table large dénormalisée réunissant les champs calculables ; nouveau KPI = une formule |
| Fait / Dimension | Table de mesures (grain daté) / référentiel descriptif (axes d'analyse) |
| Constellation | Schéma multi-étoiles : plusieurs faits partagent des dimensions conformes |
| Hub (bridge) | Table pivot reliant les faits aux dimensions (ici IMAGE_CONTRAT) |
| date_arrete | Date d'arrêté comptable : clé d'historisation métier (exigence Bâle III) |
| Bâle III | Cadre réglementaire bancaire imposant traçabilité et reconstitution des états passés |
| EDD | Entrepôt De Données du groupe (existant) ; aussi : dictionnaires de données (canevas Excel) |
| ODS | Operational Data Store — étage d'intégration de l'EDD historique |
| Canevas | Dictionnaire de données Excel par table (champs, formats, longueurs, PK) |
| POPS | Référentiel produits de la banque |
| CDL | Créances Douteuses et Litigieuses (impayés déclassés) |
| Golden Source | Source de référence unique d'une donnée |
| Landing | Zone d'atterrissage des fichiers ingérés (amont Bronze) |
| Lineage | Traçabilité du parcours d'une donnée de la source à la restitution |
| Dry-run | Exécution à blanc (lecture + contrôles, aucune écriture) |
| Time-travel | Interrogation d'une table à un instant/une version passée (Iceberg AS OF) |
| Idempotence | Relancer le même traitement produit le même état (pas de doublon) |

## CAPTURES À FAIRE MANUELLEMENT

Sur la **VM** (terminal) :
1. `spark-submit ... engine.py --table POPS` — sortie console complète d'un run (résumé final ✓ bronze/silver/rejected).
2. `spark-sql` — résultat du mouvement SCD2 POPS (8 lignes versions/clôtures) si re-démontré, sinon l'état `is_current` d'une dimension.
3. `spark-sql` — extrait QUALITY_RESULTS (commande §8) et LOG_PIPELINE (dernières exécutions).
4. `SHOW TABLES IN silver_credit_garantie;` et `SHOW TABLES IN gold_credit_garantie;`.
5. `hdfs dfs -ls /data/landing/ /data/bronze/ /data/silver/` — arborescence du lac.
6. NameNode UI (http://ubuntu3:9870) — vue d'ensemble HDFS.
7. Spark UI (http://ubuntu3:4040 pendant un run) — DAG d'exécution d'un job.
8. **NiFi** (http://ubuntu3:8443/nifi) — le flow GetFile→PutHDFS.
9. Hive Metastore : `SHOW DATABASES;` via spark-sql.

Sur le **Mac** :
10. VS Code — arborescence `jobs/` + un YAML ouvert (descripteur de table).
11. GitLab local (http://localhost:8929) — historique des commits.
12. `git log --oneline --graph` — chronologie du projet.

**Power BI / Airflow / Atlas / Ranger : rien à capturer (non réalisés).** Si construits avant la soutenance, ajouter : dashboard Power BI (KPI OBT), Data Quality Dashboard, DAG Airflow.
