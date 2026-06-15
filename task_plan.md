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
- [x] config_loader : silver_write.scd2 + scd2_signal
- [x] write_silver_scd2() : create (1er) / merge (suivants), row_hash sha2
- [x] pilote POPS, validé avec un 2e arrêté simulé (COD_USAGE modifiés) — commit 68f0c77
      NB : MERGE/UPDATE copy-on-write Iceberg 1.3.1 = NPE → réécriture DataFrame déterministe
- [x] **framework unique** `jobs/bronze/historization.py` (historize() dispatch
      snapshot/SCD2) — commit d934b02
- [x] scd2_signal.columns : sous-ensemble de colonnes comparées (codé)
- [x] **6/6 dimensions en SCD2 hash** : POPS, ASAL, IMAGE_CONTRAT, GRC_PORTEFEUILLE,
      TIERS, TYPOLOGIE — toutes validées VM (création v1)
- [~] famille interval (F2) : **reportée** — TYPOLOGIE faite en hash (règle « historiser
      tout » : tout changement de DATE_EFFET/FIN = nouvelle version). Interval = raffinement futur
- [x] faits restent snapshot (pas de SCD2) — confirmé
- [~] dim_credit (scission contractuel) : **abandonné** — l'OBT Gold (snapshot) couvre le
      besoin « tous champs calculables historisés » sans SCD2 sur du transactionnel

### Phase N — Framework de normalisation (déclaratif YAML) — À FAIRE
Objectif : normaliser les valeurs de façon déclarative (config), pour que les clés de
jointure matchent (padding) et que les codes collent aux `allowed_values` EDD.

- Placement : `Landing → Bronze (brut) → [NORMALISATION] → dédup → DQ → Silver`
  (Bronze garde la fidélité source ; Silver est normalisé).
- Config : `normalization.defaults.string: [trim, nullify_empty]` (global) + override
  par champ `schema.fields[].normalize: [...]`.
- Ops : trim/ltrim/rtrim, upper/lower, collapse_spaces, remove_spaces, nullify_empty,
  `{op: replace, pattern, with}`, `{op: lpad/rpad, width, char}`.
- Module : `jobs/bronze/normalization.py` + appel dans `run_table`.
- Scope retenu (décision 2026-06-07) : **complet** (toutes les ops + per-field + args),
  à faire plus tard (pas en cours).
- Charge estimée : ~½ journée code, ~1 journée avec test + re-baseline SCD2.
- ⚠️ Re-baseline : normaliser change le `row_hash` → re-run des 6 dims (nouvelle version, une fois).
- Motivation immédiate : corrige le padding `cod_produit ↔ idf_code_produit` (cf. Phase H4).

### Re-baseline normalisation — FAIT (2026-06-11)
Tout le Silver est désormais normalisé (trim + nullify_empty), 13/13 tables :
- 7 dims SCD2 recréées v1 normalisées (drop+re-run) : TIERS 154245 · IMAGE_CONTRAT 158322 ·
  GRC_PORTEFEUILLE 151528 · TYPOLOGIE 154249 · ASAL 4464 · POPS 1226 · ORG 762
- 6 faits/pont re-runs (snapshot réécrit) : CREDIT_AMORT 200000 (non-régression DQ exacte :
  PASSED 176281/WARN 23719/REJ 0) · IMPAYES_CDL 1597242 · DEPASSEMENT 75870 ·
  GARANTIE_DETAIL 253900 · GARANTIE_CONTRAT 258473 · IDCE_AUTORISATION 368567
- **Découverte qualité (IMPAYES_CDL)** : les 103526 lignes ex-REJECTED (PK vide) étaient
  une même ligne de remplissage répétée (différant par le padding). Normalisées, elles
  deviennent des doublons parfaits → dédup en garde 1 (REJECTED=1, Bronze garde 1700767).
  WARNING 44023 (vides→NULL maintenant vus par not_null MEDIUM — effet voulu).
- **Fixes perf** (révélés par la normalisation, profitables à tout le moteur) :
  1) normalize() en projection unique (42 withColumn empilés = explosion du plan Catalyst)
  2) localCheckpoint après normalisation (la DQ relançait tout le plan PAR règle →
     ~40 recalculs complets ; désormais 1 matérialisation, règles sur du matérialisé)
- Levier futur noté : DQ en une seule passe d'agrégation (1 job au lieu de N).

### Sprint S1 — Orchestration Airflow — FAIT (2026-06-12)
- [x] DAG `credit_garantie_medaillon` (dags/dag_credit_garantie.py) : 16 tâches —
      verifier_landing → 13 tables Bronze/Silver → Gold déclaratif → recette sanity.
- [x] **RUN COMPLET VERT sur la VM** (16/16 success, arrêté 2026-05-31) — la recette
      « TOUT EST PROPRE » s'exécute depuis l'orchestrateur. Airflow = RÉALISÉ.
- [x] Difficultés résolues (matière rapport) : venv Airflow sans deps jobs
      (→ PYSPARK_PYTHON système), standalone tout-ou-rien (→ scheduler/webserver
      séparés), schedulers en doublon (SIGTERM des tâches), Metastore absent
      post-reboot (retry Thrift silencieux), port 8080 occupé (→ 8090), cwd
      vboxuser (Permission denied gunicorn). Runbook section Airflow ajoutée.
- [ ] Captures UI (Graph vert, recette, Grid) — webserver à relancer post-run.
- [ ] Supprimer dags/dag_silver_credit.py (obsolète) — accord Taha requis.
- [ ] Option : schedule mensuel (actuellement trigger manuel par arrêté).

### Ordre des sprints restants (décision Taha 2026-06-12)
**S3 CI/CD → S4 DQ finitions → S2 Power BI → S-CLEAN nettoyage repo** (+ S5 métier, S6 rapport).

### Sprint S3 — CI/CD (Jenkins + SonarQube + GitLab) — BOUCLÉ (2026-06-14)
- [x] Stack CI migrée du repo VM séparé vers le repo canonique (commit bddf745) :
      docker-compose.yml, Dockerfile.jenkins, Jenkinsfile, sonar-project.properties, .gitignore.
- [x] Jenkinsfile adapté à la vraie structure (jobs/, dags/, scripts/) — 5 stages :
      checkout -> compile (py_compile) -> validation config YAML -> SonarQube -> deploy DAG.
      Contrôles compile/validate exécutés sur la VM via SSH (deps Python y vivent).
- [x] Code poussé vers le GitLab local (force, histoires indépendantes) → Jenkins build
      le vrai projet. **Build Jenkins SUCCESS** : 5/5 stages verts, Sonar 59 fichiers analysés.
- [x] Topologie : 2 repos étaient indépendants (GitHub owlxx6 = code / GitLab taha-group4 =
      scaffolding CI). Désormais unifiés dans le repo code.
- [x] Qualité SonarQube : 29 issues → traitées sur dag_credit_garantie.py (f-strings,
      littéraux), engine.py (6 fonctions : _check_condition, run_dq+_quality/_consistency_results,
      compute_dq_flags, _check_description, _qr_row), audit_edd_vs_yaml.py (constantes + complexité).
      Build Jenkins SUCCESS ; sécurité A, 0% duplication.
- [ ] RELIQUATS S3 (non bloquants, le pipeline tourne) :
      · commit fa0e558 (run_dq scindé) à pousser + 1 dernier rebuild pour confirmer le compte d'issues
      · fichiers pas encore re-scannés en détail : historization.py, gold/engine.py, config_loader.py,
        scripts diag/inspect/generate/simulate — corriger si Sonar en signale
      · ⚠️ NON-RÉGRESSION À FAIRE : après les refactors DQ d'engine.py, relancer CREDIT_AMORT
        et vérifier PASSED=176281/WARNING=23719/REJECTED=0 (preuve comportement inchangé) —
        nécessite HDFS+Metastore+Spark (session dédiée)
      · pousser tous les commits vers GitHub origin (owlxx6) — actuellement local + GitLab
- [ ] SÉCURITÉ : régénérer le token GitLab exposé (glpat-IVYf…) + le mettre en credential
      (pas en clair dans l'URL du remote ni dans jobs/.../config.xml).
- [ ] Pousser bddf745 vers GitHub origin (owlxx6) — actuellement seulement en local + GitLab.
- [ ] (option) Jenkinsfile : la stage deploy copie vers le dossier partagé (= source) ;
      en cible, viser un emplacement de déploiement distinct.

### Sprint S-CLEAN — Nettoyage du repo — À FAIRE
- [x] dags/dag_silver_credit.py supprimé (squelette avril, chemins morts) — accord Taha.
- [ ] Statuer sur `datalake_lib/` : squelette non utilisé par les moteurs (jobs/). Options :
      supprimer, ou y migrer proprement les moteurs (gros refactor), ou README "héritage".
- [ ] dags/configs/*.yaml (34 fichiers ancien format, avril) : obsolètes vs configs/bronze.
- [ ] jobs/silver/silver_credit_encours.py + jobs/base_job.py : ancien pipeline, non utilisés.
- [ ] scripts/ : trier one-shot (generate_configs…) vs outils pérennes (sanity, jointures).
- [ ] tests/ : alignés sur datalake_lib obsolète — réécrire sur jobs/ ou retirer.
- [ ] echantillon/ : vérifier .gitignore (données bancaires, jamais commitées).
⚠️ Règle : tout passage en revue AVEC validation Taha avant suppression.

### Phase DQ+ — Alignement sur le modèle DQ de la banque — À FAIRE
Référence : `quality_dq_engine_results.csv` + `ACTIVITYCOMM.yaml` (framework DQ équipe DATA).
Constat : notre QUALITY_RESULTS est ~75% iso (mêmes grain/nomenclature) ; on est plus riches
sur alert_threshold_pct + flags 3-états par ligne ; eux ont des colonnes opérationnelles en plus.

- [x] Colonnes opérationnelles dans `_qr_row` — commit d64a631 : rule_code composite,
      description par type, error_message, duration_seconds (chrono/règle), passed,
      passed_percentage, execution_date, table_partition_exists. QUALITY_RESULTS recréée.
- [x] **advanced_controls** — commit d64a631 : SQL métier déclaratif en YAML
      ({{silver_db}}/{{start_date}}), exécutés post-Silver, control_type=ADVANCED.
      3 livrés : CG_IMP_1 (impayé orphelin, 0 anomalie — recoupe la jointure 100%),
      CG_CRD_1 (encours négatif), CG_CRD_2 (actif mais soldé).
- [x] Types basic ajoutés (lot 3) : `range` (is_between), `email`, `referential`
      (anti-join, agrégé), `distinct_count` (niveau table) + 4 démos YAML — tous
      validés VM (COD_CYC_VIE = exactement 2 distinctes, conforme ACTIVITYCOMM).
- [ ] Contrôles techniques fichier/entête (RG EDD) : NBR_LIGNE entête = lignes reçues,
      fichier en double/vide, tolérance volumétrie vs j-1, seuil de rejets — RESTE À FAIRE.
Argument soutenance : table de résultats iso-format avec celle de l'équipe DATA.

### Phase H4 — Couche Gold (framework déclaratif)
- [x] **moteur Gold générique** `jobs/gold/engine.py` (config-driven) — commits 2adaf35, 756a3ca
      4 builders : date / dimension (SCD2 current|versioned) / fact (snapshot) / OBT
- [x] config `configs/gold/credit_garantie.yaml` + contrat pydantic `jobs/gold/config_loader.py`
- [x] mécaniques prouvées VM : dim_temps, dim_produit, gdw_encours (as-of tiers 199981/200000),
      gdw_impayes, OBT gdw_synthese_credit (agrégat + flag_cdl 96474)
- [ ] **APRÈS validation métier** : remplir le modèle final (dims tiers/typologie/agent/
      portefeuille/bridge_contrat/+ORG ; faits depassement/garantie/autorisation ; OBT complet)
- [ ] fix padding `cod_produit ↔ idf_code_produit` (normaliser les codes, jointures fait↔dim)
- [ ] détection de variation inter-arrêtés (diff N vs N-1) sur les snapshots

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

## Phase R — Rédaction du rapport PFE (EMSI) — EN COURS (2026-06-08)

### Contexte
- Auteur : **HABBOU Taha Yassine**. Encadrant EMSI : **Mr Mohammed Redouane**.
  Encadrant entreprise : **Mr Reda Kilani** (Eurafric Information / BOA).
- Base de travail : rapport d'une camarade (EL FATIHI Salma, même sujet) — `docs/rapport/`.
  On RÉUTILISE la mise en page + le front matter (swap des noms), PAS le contenu technique.
- Guide officiel : `GUIDE DE RÉDACTION DU RAPPORT DE PFE` (EMSI Rabat). Contraintes :
  A4, marges 2.5/2.5/3/2.5, Times New Roman 12, interligne 1.5, justifié,
  60-100 pages hors annexes. Front matter : page de garde → dédicace (opt) →
  remerciements → résumé FR/EN + ملخص AR → TDM → listes figures/tableaux/abréviations →
  intro générale → chapitres → conclusion → biblio → annexes.

### Règle de contenu (CRITIQUE — directive Taha)
NE JAMAIS INVENTER de donnée. Si une info manque → question directe à Taha, il fournit.
Les chapitres techniques s'appuient sur le VRAI projet (Iceberg, SCD2 6 dims, moteur Gold
déclaratif, framework DQ, Atlas/Ranger, GitLab/Jenkins/SonarQube) — différent du stack Salma.

### Structure cible — 4 chapitres (demande encadrant EMSI)
- **Ch.1 — Étude (contexte général)** : org d'accueil BOA/EAI, contexte, problématique,
  objectifs, méthodologie SCRUM. [réutilise Salma Ch1 — vérifier détails org]
- **Ch.2 — Étude de l'existant** : exploitation actuelle des données Crédit-Garantie +
  limites ; état de l'art Big Data / Data Lake / architecture Médaillon. [Salma Ch2 + existant]
- **Ch.3 — Conception de l'application** : architecture cible Bronze/Silver/Gold, choix
  technologiques (NiFi, Spark/YARN, Iceberg+Hive Metastore, Airflow, Power BI, Atlas/Ranger),
  modélisation Gold (étoile + framework déclaratif). [VRAI projet Taha]
- **Ch.4 — Réalisation** : implémentation pipelines (datalake_lib, DQ déclaratif,
  historisation snapshot + SCD2), orchestration, gouvernance, visualisation Power BI +
  résultats. [VRAI projet Taha — task_plan/findings/code]
- Ordre retenu (à confirmer) : Étude → Existant → Conception → Réalisation (flux académique).

### Mécanique d'édition
- Édition XML directe du DOCX Salma (unpack/edit/repack) pour préserver EXACTEMENT la mise
  en page (logos BOA/EAI embarqués, styles, text boxes page de garde).
- Page de garde + remerciements = text boxes dupliqués (mc:Choice + mc:Fallback) →
  chaque nom apparaît 2× → replace_all.
- Fichier de travail : `docs/rapport/RAPPORT_HABBOU_TAHA_base.docx`, unpacked dans
  `docs/rapport/unpacked/`.

### Décisions / points ouverts (Phase R)
- 2026-06-08 — Réutiliser mise en page Salma, réécrire chapitres techniques depuis le vrai projet.
- 2026-06-08 — Titre RETENU : « Conception et mise en œuvre d'une plateforme DATA standardisée :
  modélisation des couches en médaillons, standardisation des Frameworks de développement et
  gouvernance des données ».
- 2026-06-08 — Dédicace : version générique sans noms (Taha ajustera après).
- 2026-06-08 — Campus : **EMSI Rabat**.
- 2026-06-08 — Ordre des chapitres : **Étude → Existant → Conception → Réalisation** (académique standard).

### Avancement Phase R
- [x] Guide EMSI + rapport Salma analysés ; DOCX copié + unpacké ; noms localisés
- [x] Front matter : titre posé, HABBOU Taha Yassine, Mr Mohammed Redouane (EMSI),
      KILANI Rida conservé, EMSI Rabat. Validé via textutil (ouverture + contenu OK).
- [x] Dédicace rendue générique (sans noms) + accord masculin
- [x] Intro générale : liste des chapitres restructurée 5→4 + stack alignée
      (Iceberg via Hive Metastore, Atlas/Ranger — plus le « Hive » de Salma)
- [x] **Ch.1 Étude du projet** : titre renommé, SCRUM complété + dates stage
      (05/02→30/06/2026), accents « Top Employer » réparés. Figures EAI réutilisées.
      Validé textutil. Reste (option) : conclusion de chapitre.
- [ ] Résumé/Abstract/ملخص (depuis le vrai projet) — à rédiger
- [x] **Ch.2 Étude de l'existant** : inséré dans le DOCX (Titre1/2/3 pour TOC, num. manuelle
      2.1→2.4, puces, 2 placeholders [FIGURE ICI]). Existant SAP/SAP Data Services + état de
      l'art (Big Data, DW vs Data Lake, médaillon, Iceberg, gouvernance). Validé textutil.
      → en attente validation Taha + visuels (DW vs Data Lake ; médaillon).
- [x] **Ch.3 Conception** : inséré (3.1→3.7) + 2 tableaux (choix techno, faits Gold) + 3
      placeholders figures. Patchs Taha appliqués (architecture cible/POC, Impala cible,
      framework à moteurs + 7 contrôles DQ in_list/max_length/date_valid/date_range,
      silver_credit_garantie, SCD2 dimensions). Validé textutil.
      → §3.6 (noms Gold) à éventuellement vérifier côté Taha (vient de modele_gold_credit_garantie.md).
- [x] **Ch.4 Réalisation** : inséré (4.1→4.8) + 2 tableaux (12 tables Silver, 6 dims SCD2).
      Chiffres réels arrêté 2026-05-31. Patchs Taha : Spark **mode local** (pas YARN),
      normalisation = point d'extension/perspectives, Gold réalisé. Cible = Airflow/Atlas/
      Ranger/Impala/Power BI/CI-CD. Validé textutil.
- [x] **4 chapitres complets dans le DOCX** (Ch1→Ch4 + Conclusion). Reste le closing :
- [x] Conclusion générale (synthèse + perspectives) — injectée après le titre existant
- [x] Résumé FR + Abstract EN + ملخص AR (RTL) + mots-clés — injectés dans Abstract/Résumé
- [x] Bibliographie (9 sources officielles Apache/Databricks/Cloudera) — placeholders supprimés
- [x] Figures 6/8 insérées : Fig9 DW/DataLake, Fig10 médaillon, Fig11 archi (image),
      Fig12 DQ YAML, Fig13 modèle Gold (image diagram jointure), Fig14 descripteur YAML.
- [ ] RESTE figures/images (fournies par Taha) :
      · Fig15 §4.4 — capture QUALITY_RESULTS (PASSED/WARNING/REJECTED) [VM]
      · Fig16 §4.6 — capture gdw_synthese_credit / flag_cdl [VM]
      · Dashboards Power BI — captures à fournir quand construits (Power BI = cible ;
        si dashboards réalisés d'ici la soutenance → §4.6/4.7 + figures dédiées)
      · En option pendant l'expansion : nouvelles figures théoriques Ch.2 (archi HDFS/YARN,
        Spark, SCD2, étoile Kimball) — générables sans données.

## Phase R2 — Expansion 35 → 55-70 pages (demande encadrant, 2026-06-09)
Cible ~60 p. : Ch1 ~9p (BOA + SCRUM détaillé + phases réelles), Ch2 ~13p (état de l'art
approfondi : Hadoop/Spark/Iceberg/Lakehouse/Kimball/SCD/DQ/gouvernance), Ch3 ~13p (conception
détaillée : flux, modèle données, moteur DQ, historisation, arborescence framework), Ch4 ~14p
(environnement, déroulé, difficultés réelles, validation), Glossaire 1-2p.
Méthode inchangée : texte → validation Taha → injection. Théorie = fondamentaux sourcés ;
faits = docs/configs du repo.
- [ ] RESTE : dans Word, mettre à jour TOC + listes figures/tableaux (F9) ; vérifier page de garde
      (titre long), enrichir le Glossaire si besoin
- [x] RAPPORT TEXTUELLEMENT COMPLET : page de garde, dédicace, remerciements, résumés FR/EN/AR,
      4 chapitres (Étude/Existant/Conception/Réalisation), conclusion générale, bibliographie.
- [ ] Bibliographie GLOBALE en fin de rapport (regrouper les réf. de chaque chapitre)
- [ ] Workflow/chapitre : texte → [FIGURE ICI] → Taha fournit visuel/données → valider → suivant
- [ ] Conclusion + perspectives, bibliographie, listes auto, repack + validation

### Livrable courant
`docs/rapport/RAPPORT_HABBOU_TAHA.docx` (front matter OK). Source éditable :
`docs/rapport/unpacked/`. Repack = `python3 zipfile` (lxml absent → pack.py skip).
Rendu PDF impossible en local (pas de LibreOffice) ; validation via `textutil`.

### Points ouverts page de garde (non bloquants)
- Date soutenance = placeholder `../07/2026` (inconnue).
- Labels génériques « Nom de l'étudiant1/2 » restent (layout Salma) — à nettoyer si voulu.
- Titre non gras (comme placeholder Salma) — bold possible si souhaité.

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
**2026-06-07 — H1, H3 et le framework Gold TERMINÉS.**
- H1 : 12 tables Silver en Iceberg ✅
- H3 : 6 dimensions Silver en SCD2 (POPS/ASAL/IMAGE_CONTRAT/GRC_PORTEFEUILLE/TIERS/TYPOLOGIE),
  framework unique `historization.py` ✅
- Gold : moteur déclaratif `jobs/gold/engine.py` + config — mécaniques (date/dim/fait/OBT/
  as-of/agrégats) prouvées sur le pilote ✅
- **BI = Power BI** (remplace Tableau).

**Prochaine étape : remplir le modèle Gold final APRÈS validation métier** (le modèle
en étoile peut différer du pilote — d'où le choix d'un Gold config-driven). Puis : fix
padding codes, orchestration Airflow, Power BI, gouvernance, livrable.

### Décisions récentes
- 2026-06-07 — **Pas de SCD2 sur le transactionnel** : « historiser tout » sur un fait/OBT
  = snapshot append par date_arrete (chaque arrêté gardé), pas SCD2 (qui créerait une fausse
  version par ligne par arrêté). dim_credit abandonné au profit de l'OBT snapshot.
- 2026-06-07 — **Gold = framework déclaratif** : le modèle vit dans le YAML (robuste au
  changement de modèle métier). 4 types de cibles : date / dimension / fact / obt.
- 2026-06-07 — **BI = Power BI**.

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
