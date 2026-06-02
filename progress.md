# Progress Log

## 2026-06-02 — Marathon industrialisation 12 tables + cadrage historisation
**Tables industrialisées (moteur 3-états + dédup)** : CREDIT_AMORT, IMPAYES_CDL, GARANTIE_CONTRAT,
GARANTIE_DETAIL, IMAGE_CONTRAT, TIERS, TYPOLOGIE, GRC_PORTEFEUILLE, POPS, ASAL, DEPASSEMENT,
IDCE_AUTORISATION. Toutes en Silver, SUCCESS, vérifiées spark-sql.

**Features moteur livrées** :
- Statut DQ 3-états : PASSED / WARNING (HIGH/MEDIUM) / REJECTED (CRITICAL/PK). Piloté par sévérité.
- Checks `informational` : mesurés en audit (QUALITY_RESULTS) mais hors statut ligne (complétude, EDD cassés).
- **Dédup Silver opt-in** (`deduplicate: true`) : 1 ligne/PK, survie = meilleur dq_status puis récence.
  Bronze garde les doublons. PK incomplète jamais fusionnée. DEPASSEMENT : 445130 doublons retirés !
- **Unicité audit-only sous dédup** : le check unique ne marque plus REJECTED le survivant (sinon exclu du Gold).
- Normalisation colonnes à la lecture (drop `_*` NiFi, uppercase).

**Décisions actées** :
- Politique codifs = **EDD strict** : flaguer tout hors-EDD (CREDIT_AMORT harmonisé : WARN 100→23719).
- DEPASSEMENT : PK composite 4-col posée (était vide) d'après diagramme jointures.
- IDCE_AUTORISATION : schéma **reconstruit** (le YAML était faux ; vraie donnée = canevas AUTORISATIONS/CREDIT_NON_AMORT).
- Outil `scripts/audit_edd_vs_yaml.py` créé (gouvernance EDD↔YAML, réutilisable).
- Runbook enrichi (--driver-memory 4g, troubleshooting HDFS).

**Historisation cadrée (plan H1-H4 dans task_plan.md)** :
- Source = STOCK → snapshot par date_arrete (déjà fait Bronze/Silver) + append-only.
- **SCD2 COMPLET requis** sur dimensions (garder TOUTES les versions) — révise la décision « aucun SCD ».
- Cible stockage : **Iceberg sur HDFS, métadonnées Hive Metastore**, pour Silver ET Gold.
- H1 (Silver→Iceberg) = prochain chantier, prérequis du MERGE/SCD2 + time-travel.

**Backlog** : checks référentiels (devise/pays/produit), IDCE grain (IDF_REF_AUTO+NUM_DOSS?),
anomalies source à remonter métier, canevas EDD corrompus (110, NPAI, noms onglets).

**~18 commits.** Prochaine session = H1 migration Iceberg.

## 2026-05-18 — Cadrage
- Lecture des 13 EDD (canevas) + 12 CSV + 2 xlsx (échantillons).
- Confirmé : les 30 tables sont `Type stockage = STOCK` → méthode Full Snapshot validée.
- Identifié 2 bugs bloquants (Silver partitionOverwriteMode, DQ COD_CYC_VIE) et
  4 findings de normalisation (encodage, décimal, dates, traçabilité).
- Plan en 6 phases créé dans `task_plan.md`.
- Périmètre confirmé : 30 tables, use case credit_garantie.
- Prochaine étape : Phase 0 — corrections bloquantes.

## 2026-05-21 — Refonte architecture Silver
- Décision : Silver conserve **toutes** les lignes, anomalies flaggées via colonnes DQ.
- Suppression de la table `LOGS_REJECTED` (info redondante avec les flags Silver).
- Architecture finale : Bronze brut + Silver enrichi flag + 2 tables Iceberg DQ (LOG_PIPELINE + QUALITY_RESULTS).
- Phase 0 élargie : ajoute le refactor `write_silver` + retrait `write_logs.LOGS_REJECTED`.

## 2026-05-21 — Phase 0 terminée
Modifications appliquées :
- `configs/bronze/credit_garantie.yaml` :
  - ajout `spark.sql.sources.partitionOverwriteMode: DYNAMIC`
  - retrait du bloc `output.logs_rejected`
  - `COD_CYC_VIE` → `allowed_values: [B, C, I, T]`
- `jobs/bronze/config_loader.py` : `OutputConfig.logs_rejected` devient `Optional`
- `jobs/bronze/engine.py` (réécrit ~430 → ~430 lignes) :
  - `enforce_schema` simplifié (cast tolérant, plus de cast_rejects)
  - `_add_reject_meta` et `get_clean_df` supprimés
  - `run_dq` retourne uniquement `list[dict]` (stats par règle)
  - **nouvelle fonction `compute_dq_flags`** : 4 colonnes `dq_status`, `dq_severity_max`, `dq_failed_checks`, `dq_execution_id`
  - `write_silver` : `insertInto(overwrite=True)` si table existe, sinon `saveAsTable`
  - `write_logs` : branche `LOGS_REJECTED` retirée
  - `run_table` : nouveau flux à 7 étapes
- Vérifié : syntaxe OK, YAML parse correctement, allowed_values valides.

Prochaine étape : Phase 1 — normalisation à l'ingestion.

## 2026-05-21 — Phase 1 terminée
Modifications appliquées :
- `jobs/bronze/config_loader.py` :
  - nouvelle classe `CsvOptions` (delimiter, encoding ISO-8859-1, header, quote, escape, multiline)
  - `TableSourceConfig` : ajout `csv_options` et `date_arrete_column` (tous optionnels)
- `jobs/bronze/engine.py` :
  - nouvelle fonction `_smart_cast` — gère le décimal européen (`+000...,000`) et les dates multi-pattern (5 formats)
    - sentinelle `0001-01-01` → NULL ; `9999-12-31` conservé tel quel
  - nouvelle fonction `read_source` — Parquet ou CSV avec encoding configurable
  - nouvelle fonction `resolve_date_arrete` — dérive la date partition depuis une colonne de la donnée (DATE_FICHIER, etc.), fallback CLI
  - `enforce_schema` utilise désormais `_smart_cast` au lieu de `cast()` direct
  - `run_table` orchestre la nouvelle lecture + résolution de date
- Les options Phase 1 sont disponibles dans le YAML mais pas encore appliquées à TIERS / IMAGE_CONTRAT (qui restent en Parquet) — utilisables dès l'ajout des autres tables (Phase 3).

Prochaine étape : Phase 2 — traçabilité.

## 2026-05-21 — Phase 2 terminée
- Nouvelle fonction `add_ingestion_metadata` → ajoute 3 colonnes sur Bronze + Silver :
  - `execution_id` (lien LOG_PIPELINE)
  - `source_file` (chemin HDFS)
  - `ingestion_ts` (timestamp Spark)
- `write_logs` / LOG_PIPELINE enrichi avec `source_file`, `cli_date_arrete`, `rows_flagged_failed`.
- Le `finally` du `run_table` garantit qu'un log est écrit pour chaque exécution (SUCCESS ou FAILED).
- engine.py passe à ~760 lignes.

Prochaine étape : Phase 3 — compléter les schémas YAML.

## 2026-05-21 — Phase 3 terminée
- Générateur one-shot écrit : `scripts/generate_bronze_yaml.py` (parsing EDD canevas).
- Exécuté avec succès :
  - 31 tables générées (toutes en `STOCK`, validé par EDD)
  - 753 champs au total
  - 175 DQ rules (`not_null` + `unique` sur PK simple, `in_list` depuis PARAMETRAGE)
  - 18 consistency_rules (PK composites)
- `configs/bronze/credit_garantie.yaml` remplacé par la version générée (8891 lignes vs 461 avant).
- Toutes les 30 tables du use case credit_garantie sont configurées (TIERS, CONTACTS, IMAGE_CONTRAT, CREDIT_AMORT, GARANTIE_*, IDCE_*, IMPAYES_CDL, ORG, POPS, GRC_PORTEFEUILLE, TYPOLOGIE, ASAL, DEPASSEMENT, etc.).
- 2 feuilles ignorées (paramétrage, pas de données) : `CODE_EVNT`, `CODE_TRANSA_GEST`.
- Script supprimé après usage (l'EDD ne changera plus).

## 2026-05-23 — Raffinement + validation end-to-end
**Raffinement TIERS** :
- Script `scripts/inspect_codifications.py` créé pour inspecter les valeurs réelles
- Engine : `_check_condition` patché — `not_null` attrape NULL + chaîne vide ; `in_list` skippe chaîne vide
- YAML : COD_NPAI ['*']→['N'] (correction EDD), COD_CYC_VIE [T,A,S,R,E]→[B,C,I,T] (déjà fait Phase 0)
- TOP_REP_LEGAL gardé strict à [0,1] — la valeur '2' reste flaggée comme anomalie à discuter
- Hand-tuned réintégrés : NOM_USG_TIERS, RAISON_SOCIALE, DATE_NAISSANCE avec filter PP/PM + consistency PP_identite_requise, PM_raison_sociale_requise

**Raffinement IMAGE_CONTRAT** :
- IDF_REF_EXT_CTR max_length 20→50 (EDD officiel VARCHAR2(50))
- Ajout not_null MEDIUM sur COD_MTF_CYC_VIE (99.2% empty) et COD_TYP_ADRESSE (1 empty)
- Hand-tuned restaurés : date_valid DATE_OUV_CTR + consistency contrat_actif_produit_complet

**Flag CLI ajouté** : `--dry-run` (exécute lecture + DQ sans écriture HDFS/Hive/Iceberg).
**Fix sys.path** : engine.py auto-injecte la racine projet (plus besoin de PYTHONPATH).

**Run réel validé sur 2026-05-14** :
- TIERS : 1000 lignes, 46 flaggées FAILED (codes hors EDD COD_PCS/SEC_ACTIVITE/TRIBUNAL + valeurs TOP_EMAILING/REP_LEGAL)
- IMAGE_CONTRAT : 250 lignes, 248 flaggées FAILED (COD_MTF_CYC_VIE empty quasi-universel)
- Bronze HDFS, Silver Hive (7 colonnes meta), Iceberg QUALITY_RESULTS + LOG_PIPELINE tous écrits
- Vérifications spark-sql OK : flags lisibles, dq_failed_checks accessible, LOG_PIPELINE enrichi

**Anomalies à discuter avec métier** : voir `findings.md` section dédiée.

**Prochaine session — Phase A** :
Étendre à d'autres tables — méthode rodée : inspect → patch YAML → dry-run → run réel.
Priorités : CREDIT_AMORT, IMPAYES_CDL, GARANTIE_DETAIL, IDCE_AUTORISATION, TYPOLOGIE.
- Note : TIERS et IMAGE_CONTRAT existants ont été écrasés ; les règles métier finement tunées (filter_condition PP/PM) ne sont plus présentes — à réajouter manuellement si besoin.

Prochaine étape : Phase 4 — couche Gold (détection de variation par famille).
