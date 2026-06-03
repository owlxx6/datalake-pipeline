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

## Historisation — cible & techniques (planifié 2026-06-02)

Fondé sur la classification EDD `Type stockage = STOCK` (full snapshot quotidien).
La technique de base est donc **snapshot + append-only**, PAS de delta/SCD imposé à la source.

| Technique | Couche | Rôle | Statut |
|---|---|---|---|
| **Snapshot full (STOCK)** | Bronze + Silver, partition `date_arrete` | chaque arrêté = état figé, conservé | ✅ fait (12 tables) |
| **Append-only** | partitions `date_arrete` cumulées + audit Iceberg | rien n'est écrasé entre arrêtés | ✅ fait |
| **Bi-temporel** | `date_arrete` (métier) + `ingestion_ts` (technique) par ligne | distinguer « vrai le » vs « connu le » | ⏳ colonnes présentes, à formaliser valid_from/valid_to |
| **Time-travel** | Iceberg `AS OF` | rejouer une table à un snapshot passé | ⚠️ seulement audit ; **Silver = Hive → migrer Iceberg** |
| **CDC par comparaison** | Gold | diff entre 2 arrêtés (ce qui a changé) | ⏳ Phase 4 |
| **SCD2 complet (dérivé)** | Silver dimensions | garder TOUTES les versions (valid_from/valid_to/is_current) | ⏳ **REQUIS** (décision 2026-06-02) |
| **Data Vault** | — | hors scope (source STOCK, pas delta) | ❌ |

### Résultats attendus
- Reconstituer l'état exact de n'importe quel arrêté (Bâle III) — déjà possible via partitions.
- Time-travel SQL (`AS OF`) — après migration Silver→Iceberg.
- Détecter ce qui change entre 2 arrêtés (nouveaux crédits, impayés en hausse, garanties tombées).
- Vue as-of d'une dimension (segment client à une date passée).
- Gérer corrections tardives (bi-temporel).

### Phase H1 — Migration Silver → Iceberg
Décision 2026-06-03 : **Bronze reste Parquet brut (path-based HDFS)** ; seuls
**Silver + Gold** en Iceberg, catalogue Hive Metastore. write_bronze inchangé.
- [x] `write_silver` : format Iceberg via DataFrameWriterV2 (createOrReplace +
      overwritePartitions), partitionné par date_arrete — fait dans engine.py
- [x] valider Iceberg sur POPS (provider=iceberg, .snapshots/.history OK)
- [x] DROP des 12 tables Silver Hive + re-run → **12 tables Silver en Iceberg** ✓
- [x] **Correction dédup** : retire uniquement les doublons PARFAITS (pas sur PK) ;
      collisions de PK conservées + flaguées REJECTED par `unique`. Ordre run_table
      revu (Bronze → dédup → DQ → flags). Validé : DEPASSEMENT/GARANTIE_DETAIL/IDCE
      = doublons 100% parfaits, résultats identiques. commit c2cd1f6.
- [ ] reste H1 (optionnel) : démonstration time-travel `VERSION AS OF` sur 2 snapshots

### Phase H2 — Bi-temporel formalisé
- [ ] colonnes `valid_from` (date_arrete) / `valid_to` / `is_current` sur Silver dimensions
- [ ] `load_ts` technique (= ingestion_ts)

### Phase H3 — SCD2 complet sur dimensions Silver (REQUIS)
Décision 2026-06-02 : garder TOUT l'historique. Design cadré 2026-06-03.

**Décisions design (2026-06-03)** :
- Architecture : la **table Silver de la dimension DEVIENT SCD2** (versionnée, plus de
  partition date_arrete ; à la place valid_from/valid_to/is_current). Pilote = **POPS** (hash).
- Colonnes SCD2 : `valid_from`, `valid_to` (null/9999 si courant), `is_current`, `version`,
  `row_hash` (empreinte attributs), `load_ts`. Clé technique = (PK métier, valid_from).
- 3 familles de détection : F1 date_maj · F2 intervalles source (DATE_DEB/FIN) · hash (POPS/ASAL).
- Config : `silver_write.scd2: true` + `scd2_signal: {type: hash|date_maj|interval, column: …}`.

**Logique MERGE (par arrêté, comparer photo du jour vs is_current=true)** :
- clé nouvelle → INSERT version 1 (valid_from=arrêté, is_current=true)
- clé connue, row_hash identique → rien
- clé connue, row_hash différent → CLÔTURE ancienne (valid_to, is_current=false) + INSERT nouvelle
- clé disparue → CLÔTURE (soft delete)
→ MERGE INTO Iceberg (clôture) + INSERT (nouvelles versions).

**À faire** :
- [ ] config_loader : silver_write.scd2 + scd2_signal
- [ ] write_silver_scd2() : create (1er) / merge (suivants), row_hash sha2
- [ ] pilote POPS, valider avec un 2e arrêté simulé (qq COD_USAGE modifiés)
- [ ] étendre : TIERS, TYPOLOGIE (F2), GRC_PORTEFEUILLE, ASAL, IMAGE_CONTRAT
- [ ] faits (credit/impayes/garantie/depassement) : restent snapshot (pas de SCD2)

### Phase H4 — Gold : détection de variation inter-arrêtés
- [ ] diff snapshot N vs N-1 (créé / modifié / disparu) pour les KPIs de variation

## Phases (Bronze/Silver — historique)

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
- 2026-06-02 — **Dédup Silver opt-in** (deduplicate: true) + unicité audit-only sous dédup. 12 tables industrialisées.
- 2026-06-02 — **Politique EDD stricte** : flaguer tout hors-EDD (CREDIT_AMORT harmonisé). Outil `audit_edd_vs_yaml.py`.
- 2026-06-02 — **Historisation = snapshot STOCK (Bronze/Silver) + SCD2 COMPLET dérivé sur dimensions Silver**.
  Révise la décision 2026-05-18 « aucun SCD » : on garde toutes les versions des attributs de dimensions.
  Prérequis = migration Silver→Iceberg (MERGE). Faits restent snapshot.

## Statut
**2026-06-02 — Bronze/Silver TERMINÉ : 12 tables Landing industrialisées** sur le moteur
DQ 3-états (PASSED/WARNING/REJECTED) + dédup Silver opt-in + audit EDD.
Prochaine session : **Phase H1 — migration Silver → Iceberg** (prérequis SCD2 + time-travel),
puis H2/H3/H4. Cible stockage confirmée : Iceberg sur HDFS, métadonnées Hive Metastore.

### 12 tables Silver validées (run réel 2026-05-31)
credit_amort (200k, WARN 23719), impayes_cdl (1.7M, REJ 103526), garantie_contrat (258k),
garantie_detail (258k→253900, 4328 dédup), image_contrat (158k, 1 REJ), tiers (154k, WARN 14069),
typologie (154k, WARN 1068), grc_portefeuille (151k), pops (1226), asal (4464),
depassement (521k→75870, 445130 dédup), idce_autorisation (370k→368567, schéma reconstruit).

### Phase courante (ancienne, terminée)
Phases 0/1/2/3 + extension 12 tables : DONE.

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
