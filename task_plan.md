# Task Plan — Historisation & Normalisation Bronze (use case credit_garantie)

## Objectif
Mettre en place l'historisation et la normalisation de la donnée depuis son ingestion,
pour les 30 tables du use case credit_garantie. Tout conserver, tracer chaque exécution,
flaguer les anomalies sans jamais modifier la donnée source.

## Périmètre
30 tables, toutes rattachées au use case **credit_garantie**.
TIERS et IMAGE_CONTRAT déjà traités comme pilote — à compléter/corriger.

## Méthode retenue
Full Snapshot **STOCK** partitionné par `date_arrete`.
Confirmé par les EDD : les 30 tables sont `Type stockage = STOCK`, fréquence quotidienne.
Aucun SCD manuel. Détection de variation à Gold, jamais d'écriture dans la source.

## Architecture des sorties (révisée)
- **Bronze HDFS** : miroir brut de la source (Parquet, toutes les lignes, fidélité 100%)
- **Silver Hive** : **toutes les lignes** + colonnes DQ flag (pas de filtrage — on flagge, on n'enlève pas)
- **Silver Iceberg DQ** : 2 tables d'audit append-only
  - `LOG_PIPELINE` (1 ligne / exécution)
  - `QUALITY_RESULTS` (1 ligne / règle / exécution)
- `LOGS_REJECTED` **supprimée** — l'info ligne par ligne vit désormais dans Silver via les flags

Total par domaine : 30 tables Silver + 2 tables DQ = **32 tables**.

## Phases

### Phase 0 — Corrections bloquantes + refactor Silver ✅
- [x] Bug Silver `partitionOverwriteMode` → `DYNAMIC` ajouté au YAML + `insertInto(overwrite=True)` dans `write_silver`
- [x] DQ `COD_CYC_VIE` → `allowed_values: [B, C, I, T]` (EDD officiel)
- [x] Refactor `write_silver` : écrit **toutes** les lignes + 4 colonnes flag DQ
- [x] Refactor `write_logs` : branche `LOGS_REJECTED` supprimée
- [x] `credit_garantie.yaml` : bloc `output.logs_rejected` supprimé
- [x] `enforce_schema` simplifié : cast tolérant (échecs → NULL, aucune ligne perdue)
- [x] Nouvelle fonction `compute_dq_flags` : produit les 4 colonnes `dq_*`
- [x] `config_loader.OutputConfig.logs_rejected` : `Optional` (rétrocompatibilité)

### Phase 1 — Normalisation à l'ingestion ✅
- [x] Encodage : `read_source` lit Parquet ou CSV ; `CsvOptions` (encoding ISO-8859-1 par défaut)
- [x] Format décimal européen : `_smart_cast` nettoie `+`, espaces et virgule décimale avant cast
- [x] Dates hétérogènes : `_smart_cast` essaie 5 patterns ; `0001-01-01`→NULL, `9999-12-31` conservé
- [x] Partition `date_arrete` dérivée de la donnée : `source.date_arrete_column` + `resolve_date_arrete`

### Phase 2 — Traçabilité ✅
- [x] Colonnes techniques `execution_id` + `source_file` + `ingestion_ts` ajoutées sur chaque ligne Bronze + Silver via `add_ingestion_metadata`
- [x] `LOG_PIPELINE` enrichi : `source_file`, `cli_date_arrete`, `rows_flagged_failed` ; appel garanti dans `finally` (chaque exécution, succès comme échec)

### Phase 3 — Schémas YAML complets ✅
- [x] Générateur one-shot écrit (`scripts/generate_bronze_yaml.py`), exécuté, **supprimé**
- [x] `configs/bronze/credit_garantie.yaml` régénéré depuis les 13 EDD : 31 tables, 753 champs, 175 DQ rules, 18 consistency rules
- [x] `in_list` auto depuis la section PARAMETRAGE de chaque EDD (ex. COD_CYC_VIE → [B,C,I,T])
- [x] `consistency_rule pk_composite_complete` auto pour chaque PK composite
- [x] `date_arrete_column` détecté si DATE_FICHIER présent dans le corps

### Phase 4 — Couche Gold (détection de variation)
- [ ] Logique de comparaison inter-snapshots par famille (date MAJ / intervalles validité / hash)
- [ ] Flag d'anomalie calculé à Gold

### Phase 5 — Livrable
- [ ] Finaliser le document avec cartographie des 30 tables et choix d'historisation

## Chemin critique
Phase 0 → 1 → 3 séquentielles. Phases 2 et 4 parallélisables après la Phase 1.

## Décisions
- 2026-05-18 — Méthode d'historisation : Full Snapshot STOCK. Justification : EDD `Type stockage=STOCK` sur les 30 tables.
- 2026-05-18 — Périmètre : 30 tables, toutes dans le use case credit_garantie.
- 2026-05-18 — Règle projet : append-only sur les tables Iceberg DQ, jamais de suppression.
- 2026-05-21 — **Silver garde toutes les lignes**, anomalies flaggées (pas filtrées). `LOGS_REJECTED` supprimée. 2 tables DQ au lieu de 3.

## Statut
Phase courante : **Étendre à d'autres tables** (CREDIT_AMORT, IMPAYES_CDL, GARANTIE_DETAIL…).
Phases 0/1/2/3 terminées + validation end-to-end TIERS + IMAGE_CONTRAT le 2026-05-23.

## État validé (run réel 2026-05-23)
- TIERS : 1000 lignes lues → Bronze + Silver + Iceberg ✓ — 46 lignes flaggées FAILED
- IMAGE_CONTRAT : 250 lignes lues → Bronze + Silver + Iceberg ✓ — 248 flaggées
- Architecture finale : 2 tables DQ Iceberg + Silver avec 7 colonnes meta + flags
- date_arrete validée en Landing : 2026-05-14

## Méthode de raffinement par table (rodée sur TIERS + IMAGE_CONTRAT)
1. `spark-submit scripts/inspect_codifications.py --table X` → voir valeurs réelles vs YAML
2. Identifier : empties suspects, codes hors EDD, valeurs systématiques
3. Patcher YAML : ajouter not_null sur empties à discuter, corriger allowed_values
4. Dry-run : `spark-submit jobs/bronze/engine.py ... --table X --dry-run`
5. Run réel quand le ratio anomalies/total est cohérent métier

## Tables prioritaires à intégrer (suite)
- **CREDIT_AMORT** (105 colonnes — encours, montants mouvants)
- **IMPAYES_CDL** (41 colonnes — montants qui s'accumulent)
- **GARANTIE_DETAIL** (54 colonnes — DATE_MAJ_OCCURENCE pour change detection)
- **IDCE_AUTORISATION** (43 colonnes)
- **TYPOLOGIE** (intervalles validité DATE_EFFET/DATE_FIN — famille 2)

## Commandes services VM (ubuntu3)
```
# Démarrer (ordre)
start-dfs.sh
$SPARK_HOME/sbin/start-all.sh
nohup hive --service metastore > ~/logs/metastore.log 2>&1 &

# Vérifier
jps    # NameNode + DataNode + Master + Worker + RunJar

# Lancer un run
spark-submit --master spark://ubuntu3:7077 \
  jobs/bronze/engine.py \
  --config configs/bronze/credit_garantie.yaml \
  --date-arrete 2026-05-14 \
  --table NOM_TABLE \
  [--dry-run]
```
