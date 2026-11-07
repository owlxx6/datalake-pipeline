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

## 2026-06-08 — Phase R : démarrage rédaction rapport PFE (EMSI)
- Analysé : guide de rédaction EMSI (A4, TNR 12, interligne 1.5, 60-100 p.) + rapport
  camarade EL FATIHI Salma (même sujet, même entreprise EAI/BOA, même encadrant Kilani).
- Décisions (Taha) : titre « Conception et mise en œuvre d'une plateforme DATA standardisée… »,
  dédicace générique sans noms, campus **EMSI Rabat**, ordre **Étude→Existant→Conception→Réalisation**.
- Méthode : édition XML directe du DOCX Salma (unpack/edit/repack zipfile) pour garder la
  mise en page. Front matter fait : titre, HABBOU Taha Yassine, Mr Mohammed Redouane (EMSI),
  KILANI Rida conservé, EMSI Rabat, dédicace générique. Validé via textutil.
- Règle de contenu : NE RIEN INVENTER ; chapitres techniques depuis le VRAI projet
  (Iceberg, SCD2 6 dims, moteur Gold déclaratif, DQ, Atlas/Ranger) — pas le stack Salma.
- Outils locaux : pas de LibreOffice (rendu PDF KO), pas de lxml (validateur pack.py KO),
  pas de npm docx global. Word installé. Repack manuel zipfile + contrôle textutil.
- Prochaine étape : corps 5→4 chapitres. Ch.1 Étude + Ch.2 Existant adaptés de Salma
  (même entreprise) ; Ch.3 Conception + Ch.4 Réalisation depuis task_plan/findings/code.

## 2026-06-08 (suite) — Chapitre 1 (Étude) adapté + intro restructurée
- Heading « Chapitre 1 : Contexte général » → « Chapitre 1 : Étude du projet » (heading + TOC).
- Section méthodologie SCRUM réécrite proprement (supprimé le bleed « 10 Chapitre 1… »,
  phrase complétée) + intégration des dates de stage : 05/02/2026 → 30/06/2026 (~5 mois).
- 1.2.4 « Top Employer » : accents cassés (PDF paste) réparés — été/certifiée/cinquième/
  année/consécutive/récompenser l'engagement/bien-être/développement des compétences.
- Intro générale : liste des chapitres 5→4 (Étude / Existant / Conception / Réalisation),
  entrée « Chapitre 5 » supprimée ; stack technique alignée sur le vrai projet
  (Apache Iceberg via Hive Metastore + Atlas/Ranger, au lieu du « Hive » de Salma).
- Repack zipfile + contrôle textutil : OK. Figures EAI (logos, organigramme, Top Employer)
  conservées et réutilisables (même entreprise).
- Reste Ch.1 (option) : ajouter une « Conclusion du chapitre ».
- Prochaine étape : Ch.2 Existant (existant EAI + état de l'art Big Data/médaillon).

## 2026-06-08 (suite) — Chapitre 2 (Existant) rédigé + injecté
- Contenu validé par Taha puis injecté dans le DOCX (38 paragraphes) avant la Conclusion.
- Titres en styles Titre1 (chapitre) / Titre2 / Titre3 → apparaîtront dans la TOC après
  mise à jour du champ (F9). Numérotation manuelle (2.1→2.4) pour ne pas perturber la
  numérotation auto du Ch.1.
- Existant = SAP / SAP Data Services (sources : IDCHAB, IDCE, Evolan, BTA, système comptable,
  CDL, Dépassements) + limites documentées. État de l'art : Big Data/Hadoop, DW vs Data Lake,
  médaillon, Apache Iceberg, gouvernance (Atlas/Ranger) + qualité.
- 2 placeholders [FIGURE ICI] : (1) DW vs Data Lake, (2) médaillon B/S/G.
- Références : à regrouper dans une bibliographie GLOBALE en fin de rapport (pas par chapitre).
- Reste avant Ch.3 : validation Taha + visuels. Ch.3 utilisera figures/architecture_datalake.png.

## 2026-06-08 (suite) — Chapitre 3 (Conception) rédigé + patché + injecté
- Brouillon Ch.3 proposé ; Taha a renvoyé 4 patchs (corrige 2 erreurs factuelles + lève
  l'ambiguïté cible/réalisé). Patchs appliqués puis injection dans le DOCX (Titre1/2/3 + 2
  tableaux Word : choix techno, faits Gold).
- Corrections clés : §3.5 framework à MOTEURS (Bronze→Silver engine, Gold engine) + capacités
  transverses (config_loader/quality/historization/normalization/audit) ; 7 contrôles DQ réels :
  not_null, unique, in_list, max_length, regex, date_valid, date_range ; statuts PASSED/WARNING/
  REJECTED ; tables audit LOG_PIPELINE + QUALITY_RESULTS. §3.2/§3.4 : Cloudera/Impala = cible,
  POC requête Spark SQL sur Iceberg. §3.3.3 : base silver_credit_garantie, dimensions SCD2 non
  partitionnées.
- Incident corrigé : 3 '&' bruts ('clean & normalize', 'Audit & journalisation', 'Gouvernance
  & sécurité') cassaient le XML → remplacés par &amp;. Leçon : échapper & dans le texte injecté.
- §3.6 (Gold) vient de modele_gold_credit_garantie.md (gdw_* / 6 dims / OBT) → à éventuellement
  vérifier côté Taha.
- Prochaine étape : Ch.4 Réalisation (runs réels, DQ, historisation, orchestration, restitution).

## 2026-06-08 (suite) — Chapitre 4 (Réalisation) patché + injecté → 4 chapitres complets
- Brouillon Ch.4 → Taha a renvoyé 2 corrections factuelles + 4 chiffres :
  (1) Spark en MODE LOCAL (pas YARN — les logs local[*] le prouvent) ; (2) normalisation
  CODÉE MAIS INERTE → présentée en point d'extension/perspectives (cohérent avec §4.7).
  Chiffres Silver complétés : idce_autorisation 368 567, garantie_contrat 258 473,
  garantie_detail 253 900, depassement 75 870.
- Injection OK (Titre1/2/3 + 2 tableaux). Ajout esc() dans les helpers → plus de risque de '&' brut.
- ÉTAT : les 4 chapitres (Étude / Existant / Conception / Réalisation) sont dans le DOCX,
  ordre correct, avant la Conclusion Générale. Réalisé vs cible clairement séparés.
- Reste : conclusion générale, résumé/abstract/ملخص, bibliographie globale, figures, MAJ TOC.

## 2026-06-08 (fin) — Front/back matter complétés → rapport textuellement complet
- Conclusion générale injectée (titre + 6 paragraphes), saut de page de Salma corrigé.
- Résumés : Abstract EN + Résumé FR (dans les placeholders existants) + ملخص AR (RTL, bidi)
  inséré avant le Glossaire. Mots-clés FR/EN/AR.
  Incident : placeholders avaient un espace insécable (\xa0) avant « : » → motifs regex robustes
  (\s*:[^<]*) au lieu d'espace normal. Leçon notée.
- Bibliographie : 9 sources officielles (Hadoop, Spark, Iceberg, NiFi, Airflow, Atlas, Ranger,
  Databricks médaillon, Cloudera) ; placeholders [1]/[2]/[3] + hints de mise en forme supprimés.
- ÉTAT : RAPPORT TEXTUELLEMENT COMPLET. Reste = figures (placeholders [FIGURE ICI]), MAJ champs
  Word (TOC + listes), vérif page de garde, glossaire optionnel.

## 2026-06-08 (correctifs) — « contenu illisible » + pages vierges + titres + TOC
- CAUSE « contenu illisible » Word : mon zip manuel incluait les fichiers macOS .DS_Store
  (unpacked/.DS_Store, unpacked/word/.DS_Store) — parties non déclarées → rejet. FIX : repack
  exclut .DS_Store + suppression dans l'arbre. (Leçon : toujours exclure .DS_Store du repack.)
- PAGES VIERGES 26-32 : Salma avait laissé **150 paragraphes vides** entre la fin du Ch.1
  (figure SCRUM) et le Ch.2. Supprimés par strip arrière sûr (régex sans imbrication de w:p).
  Bugs évités : (a) régex naïve <w:p>.*?</w:p> casse sur les <w:p> imbriqués des zones de texte
  (figure SCRUM) ; (b) test de vide via '<w:r' faux positif car <w:rPr> contient « <w:r ».
  → détecter le vide via présence de <w:t>/<w:drawing>/<mc:AlternateContent>, pas <w:r>.
- TITRES DE CHAPITRES : saut de page ajouté après les titres Ch.2/3/4 → chaque titre seul sur
  sa page, comme le Ch.1.
- SOMMAIRE : <w:updateFields w:val="true"/> ajouté à settings.xml → Word propose/actualise les
  champs (TOC + listes) à l'ouverture.
- Sécurité : restauration de document.xml depuis le DOCX intact après un write cassé ; désormais
  ET.fromstring(s) valide EN MÉMOIRE avant tout write.

## 2026-06-09 — Figures : 6/8 insérées
- Embarquement d'images dans le DOCX (rels rId100/rId101, media/image11.png + image12.jpeg,
  namespaces a/pic déclarés inline, EMU calculés depuis sips). Repack exclut .DS_Store.
- Figure 9 : DW vs Data Lake (tableau natif). Figure 10 : médaillon B/S/G (tableau coloré).
- Figure 11 : architecture DATALAKE (image fournie). Figure 12 : config DQ déclarative (YAML réel pops).
- Figure 13 : modèle Gold/constellation (image docs/diagram jointure.jpeg).
- Figure 14 : descripteur YAML de table (extrait réel pops.yaml).
- RESTE 2 placeholders (données VM requises) : #7 §4.4 QUALITY_RESULTS (PASSED/WARNING/REJECTED),
  #8 §4.6 gdw_synthese_credit / flag_cdl.
- Note : numérotation figures manuelle (9→14) ; la « Liste des figures » à vérifier après MAJ
  des champs dans Word.

## 2026-06-10 — Data pack reçu + figures 15/16 + glossaire
- docs/REPORT_DATA_PACK.md fourni par Taha (534 l.) : stack exacte (Spark 3.4.0 local, Iceberg
  1.3.1, HMS, HDFS ubuntu3, 9.5Go), framework dans jobs/ (PAS datalake_lib), 7 types DQ,
  modèle Gold exact (5 faits/6 dims/bridge/OBT), chiffres 12 tables + DQ par table, 5 lignes
  réelles OBT, YAML réels (POPS + gold), 7 difficultés, glossaire, Power BI = cible confirmé.
- Figure 15 (QUALITY_RESULTS par table) et Figure 16 (extrait gdw_synthese_credit) insérées
  depuis données réelles → **0 placeholder [FIGURE ICI] restant** (8/8 figures + 6 Ch1 = 16).
- Glossaire : 38 termes (table) injecté à la place des 3 entrées EAI/BI/BOA.
- RESTE : Phase R2 expansion 35→55-70 p. (Ch2 état de l'art, Ch3 conception détaillée, Ch4
  déroulé/difficultés, Ch1 BOA+SCRUM). Dashboards Power BI = à fournir SI construits (cible).

## 2026-06-11 — Ch.2 étoffé + 5 figures théoriques générées (SVG→PNG via qlmanage)
- Ch.2 réécrit/étoffé (état de l'art approfondi : Big Data 5V, Hadoop, Spark, Parquet,
  DW/DL/Lakehouse, Iceberg, médaillon, Kimball/SCD1-2-3, qualité/gouvernance). Injecté en
  remplaçant l'ancien Ch.2. Numérotation figures corrigée (Ch.2=9-15, Ch.3=16-18, Ch.4=19-21,
  0 doublon). Bug évité : renumérotage global +5 avait touché les légendes Ch.2 → corrigé par
  texte exact.
- **Capacité figure débloquée** : pas de LibreOffice/ImageMagick/cairosvg/PIL, MAIS `qlmanage`
  (Quick Look macOS) rasterise un SVG → PNG proprement (testé, rendu pro). Méthode : SVG carré
  1200x1200 (contenu plein) → qlmanage -t -s 1600 → PNG 1600x1600 → embarqué ~9,5 cm.
- 5 figures générées et embarquées : Fig9 Hadoop (HDFS/YARN), Fig10 Spark (driver/executors),
  Fig12 Iceberg (couches/snapshots), Fig14 étoile (Kimball), Fig15 SCD2 (avant/après).
- **0 placeholder [FIGURE ICI] restant.** Toutes les figures du rapport sont réelles.
- Reste expansion : Ch.3, Ch.4, Ch.1 (BOA+SCRUM). Mots ~7900 → viser ~15000 pour 55-70 p.

## 2026-06-11 (suite) — Ch.3 étoffé + 3 figures générées
- Ch.3 reconstruit/étoffé : 3.1 intro, 3.2 archi, 3.3 médaillon (4 sous-sect), 3.4 choix techno
  (tableau), 3.5 framework (principe/moteurs/pipeline run_table), 3.6 moteur DQ (7 types/3 états/
  traçabilité), 3.7 historisation (snapshot/SCD2/as-of), 3.8 modèle Gold (constellation/faits-dims/OBT),
  3.9 conclusion. Depuis le data pack.
- 3 nouvelles figures SVG→PNG (qlmanage) : Fig18 flux run_table, Fig19 cycle règle DQ + 3 états,
  Fig20 snapshot vs SCD2. + ré-embarquement Fig16 archi (rId100) et Fig21 modèle Gold (rId101) +
  Fig17 YAML (code table).
- Renumérotation : Ch.4 captions 19/20/21 → 22/23/24 (par texte exact, sans bug global). 0 doublon.
- Reste expansion : Ch.4 (déroulé, difficultés détaillées, environnement, validation), Ch.1 (BOA+SCRUM).

## 2026-06-11 (suite) — Ch.4 étoffé + Gantt généré
- Ch.4 reconstruit/étoffé : 4.1 intro, 4.2 environnement détaillé (Spark 3.4.0 local, Iceberg
  1.3.1, HMS, HDFS, 9.5Go, driver 4g), 4.3 déroulé+Gantt, 4.4 Bronze→Silver (descripteur YAML +
  12 tables), 4.5 qualité (QUALITY_RESULTS), 4.6 historisation (6 dims SCD2), 4.7 Gold (OBT),
  4.8 difficultés (7 réelles développées), 4.9 bilan réalisé/cible, 4.10 conclusion.
- Fig 22 Gantt généré (chronologie SCRUM en escalier). Fig 23 descripteur YAML, Fig 24 QUALITY,
  Fig 25 gdw_synthese (tables natives reconstruites). + Tableau 2 (12 tables) + Tableau 3 (6 dims).
- ÉTAT : 4 chapitres étoffés, 25 figures réelles, 0 placeholder. Estimation ~55-65 pages.
- Reste : Ch.1 (présentation BOA + SCRUM détaillé + bénéfices) pour finir l'expansion.
  Puis MAJ champs Word (TOC + Liste des figures via style Légende) + relecture.

## 2026-06-11 (suite) — Ch.1 étoffé (texte) + sources web EAI
- Recherche web site officiel EAI (eurafric-information.com) : faits sourcés (créée oct 2008,
  JV Bank of Africa BMCE Group + RMA + Crédit Mutuel/Euro-Information, 350+ ingénieurs, 15+
  filiales, Datacenter Tier III+ Uptime Institute, Casablanca Bouskoura, valeurs EAI =
  Engagement/trAnsparence/Innovation, Top Employer 2024).
- Ch.1 : insertion (paragraphes, sans nouveaux titres numérotés pour ne pas casser la num.
  Salma) après EAI = EAI officiel + groupe Bank of Africa + organisation Data Office ; après
  SCRUM = rôles/cérémonies/artefacts + application au projet.
- Pièges anchors résolus : "reconnu" (pas "reconnus"), SCRUM apostrophe = entité &#x2019;.
- Taha a fourni 3 slides EAI (ARCHITECTURE DWH existant SAP BO ; Data Office ; Gouvernance)
  → figures/slide_1/2/3.png. + 2 docs (EDD_TECHNIQUE.pptx, PETIT RAPPEL DES RG.docx) en réf.
- RESTE (passe figures) : embarquer slide_2 (Data Office, Ch.1), slide_1 (DWH existant, Ch.2
  §2.2.1) + enrichir texte existant, slide_3 (gouvernance, Ch.2 §2.3.9). PUIS renumérotation
  COMPLETE des figures (insertions décalent tout). PUIS MAJ champs Word.

## 2026-06-11 (fin) — 3 slides EAI intégrées + numérotation figures par champs SEQ
- DÉCOUVERTE : les légendes de Salma utilisent des champs SEQ (auto-num), et la « Liste des
  figures » est un champ Table des figures (TOC \c "Figure"). Mes légendes étaient en texte
  littéral → conflit/doublons.
- SOLUTION DÉFINITIVE : converti mes 17 légendes Figure + 3 Tableau en CHAMPS SEQ (même
  structure que Salma). Désormais toutes les figures/tableaux s'auto-numérotent dans l'ordre
  du document, et les listes (figures/tableaux) se peuplent seules à la MAJ des champs.
- 3 slides EAI embarquées : slide_2 Data Office (Ch.1 §1.2), slide_1 DWH existant SAP
  BusinessObjects (Ch.2 §2.2.1, + texte enrichi ODS/EDD/datamarts), slide_3 Gouvernance (Ch.2
  §2.3.9). rId111-113, image22-24.
- updateFields=true déjà actif → Word propose la MAJ à l'ouverture.
- ÉTAT FINAL : 4 chapitres étoffés, 28 figures réelles (8 Salma + 17 schémas/tables générés +
  3 slides EAI), 3 tableaux, glossaire 38 termes, conclusion, biblio. Pièges anchors: apostrophe
  entité vs littérale, parenthèse "Dépassements)", "reconnu".
- ACTION TAHA : ouvrir dans Word → accepter la MAJ des champs (ou tout sélectionner + F9) →
  TOC + Liste des figures + Liste des tableaux + numéros se mettent à jour.
