# Discours de soutenance — Plateforme DATA Crédit-Garantie (BOA / Eurafric)

**Durée cible : 17-18 min** (fourchette 15-20). 35 slides.
Rythme : ~30 s par slide de contenu, 5-10 s sur les séparateurs.

## Conseils de rythme
- **Ne lis pas les slides** : la slide montre, toi tu racontes.
- Une **phrase de transition** entre chaque partie (elles sont notées 🔄).
- Ralentis sur 3 moments forts : la **problématique**, le **framework déclaratif**, la **démo Bronze→Gold**.
- Garde 2 min de marge pour les questions.

---

## INTRODUCTION — ~1 min

### Slide 1 — Plan de la présentation
« Madame, Monsieur les membres du jury, bonjour. Je vais vous présenter mon projet de fin d'études réalisé chez Eurafric Information pour le compte de Bank of Africa : la conception d'une **plateforme DATA standardisée** pour l'axe Crédit-Garantie. »
« Je suivrai six parties : le contexte, la problématique, l'étude de l'existant et l'état de l'art, la conception, la réalisation, puis la conclusion. »
🔄 « Commençons par le cadre dans lequel s'inscrit ce projet. »

---

## PARTIE 1 — CONTEXTE ENTREPRISE — ~1 min 30

### Slide 3 — Organisme d'accueil
« Le projet réunit deux entités du même groupe. **Bank of Africa**, l'émetteur du besoin : un groupe panafricain présent dans 32 pays, plus de 15 000 collaborateurs et 6 millions de clients. Et **Eurafric Information**, l'entreprise d'accueil : le centre de services IT du groupe, 350 collaborateurs, plus de 3 000 serveurs, qui porte la transformation digitale. »

### Slide 4 — Département : Business Intelligence / Data Office
« J'ai été intégré au **Data Office**, organisé en squads agiles par domaine métier — une équipe centrale, des squads Build, une équipe Socle et une équipe Run. Mon projet s'inscrit dans le domaine Crédit-Garantie. »
🔄 « Voyons maintenant le contexte précis du projet. »

---

## PARTIE 2 — CONTEXTE & PROBLÉMATIQUE — ~2 min 30

### Slide 6 — Contexte du projet
« Le projet repose sur quatre axes : **standardiser** les formats de données via un référentiel commun, **industrialiser** les traitements, structurer le lac en **architecture médaillon**, et assurer la **gouvernance et la restitution**. »

### Slide 7 — Objectifs du projet
« L'activité Crédit-Garantie génère de gros volumes de données financières, qui exigent une traçabilité rigoureuse et une restitution conforme aux exigences réglementaires. Eurafric a donc été mandatée pour concevoir une plateforme DATA capable d'industrialiser ces traitements et de produire des tableaux de bord fiables. »

### Slide 8 — Problématique *(à dire lentement)*
« Le système actuel repose sur **SAP**, avec trois limites critiques. Un : une **architecture monolithique rigide**, difficile à faire évoluer. Deux : une **volumétrie** que le traitement non distribué ne suit plus. Trois — et c'est le cœur du problème — **aucune standardisation ni traçabilité** : des scripts hétérogènes, pas de Golden Source, pas de Data Lineage, ce qui engendre doublons et incohérences. »
🔄 « Pour répondre à cela, j'ai d'abord étudié l'existant et l'état de l'art. »

---

## PARTIE 3 — EXISTANT & ÉTAT DE L'ART — ~3 min 30

### Slide 10 — L'existant : architecture SAP
« L'existant, c'est une chaîne **SAP BusinessObjects** : extraction ETL via SAP Data Services, un entrepôt sources → ODS → EDD, puis des datamarts métier Risque, Finance, Comptabilité. Une organisation éprouvée, mais qui montre ses limites face aux volumes et aux exigences de traçabilité. »

### Slide 11 — Les limites de l'existant
« Ces limites se résument en sept points : pas de standardisation, pas de Golden Source, traçabilité limitée, faible industrialisation, qualité non maîtrisée, pas d'architecture médaillon, et une plateforme propriétaire coûteuse. »

### Slide 12 — État de l'art : le socle Big Data
« Face à ça, l'état de l'art Big Data. Les **5 V**, l'écosystème **Hadoop** — HDFS pour le stockage distribué, YARN pour les ressources —, **Apache Spark** comme moteur de calcul en mémoire, et le format colonnaire **Parquet**. »

### Slide 13 — Du Data Warehouse au Lakehouse
« Le débat classique : le Data Warehouse est fiable mais rigide et coûteux ; le Data Lake est souple mais devient vite un "marécage". Le **Lakehouse** réconcilie les deux — les garanties ACID d'un entrepôt sur un Data Lake. C'est le modèle retenu, via **Apache Iceberg**. »

### Slide 14 — L'architecture en médaillon
« Le raffinage se fait en trois couches : **Bronze**, données brutes fidèles à la source ; **Silver**, données nettoyées, typées, dédupliquées et contrôlées ; **Gold**, données agrégées en modèle en étoile, prêtes pour l'analyse. »

### Slide 15 — L'historisation SCD2
« Pour suivre l'évolution des données dans le temps, j'utilise le **Slowly Changing Dimension de type 2** : chaque changement crée une nouvelle version datée — valid_from, valid_to, is_current — et tout l'historique est conservé. Couplé à Iceberg, cela permet la **jointure point-in-time**, c'est-à-dire retrouver la valeur exacte d'un attribut à une date d'arrêté donnée. »

### Slide 16 — Qualité & gouvernance
« La qualité est mesurée sur cinq dimensions — complétude, unicité, validité, cohérence, intégrité référentielle — avec un principe clé : **on déclare des règles, on mesure, on trace, mais on ne supprime jamais la donnée fautive**. La gouvernance s'appuie sur **Apache Atlas** pour le lineage et **Apache Ranger** pour la sécurité. »

### Slide 17 — Solution
« En synthèse, ma solution combine trois piliers : une **architecture médaillon moderne** sur Lakehouse Iceberg ; un **traitement distribué** Hadoop/Spark avec ingestion NiFi ; et un **framework déclaratif en YAML** qui décrit chaque pipeline sans écrire de code, avec un moteur de qualité et l'historisation. »
🔄 « Passons à la conception de cette solution. »

---

## PARTIE 4 — CONCEPTION — ~3 min

### Slide 19 — Environnement & chaîne de traitement
« La chaîne technique : **NiFi** pour l'ingestion, **Spark 3.4** pour le traitement, **Iceberg** pour le stockage, **Power BI** pour la restitution. Et des briques transverses : Airflow pour l'orchestration, Ranger pour la sécurité, Atlas pour la gouvernance, GitLab/Jenkins pour le CI/CD. »

### Slide 20 — Architecture générale
« Voici l'architecture cible complète. À gauche, les sources ingérées par NiFi. Au centre, Airflow orchestre les traitements Spark qui alimentent les couches Bronze, Silver, Gold en Iceberg. En haut, la gouvernance Ranger/Atlas. À droite, la restitution Power BI. »
*(Si le jury pousse : préciser que le POC couvre le sous-ensemble Crédit-Garantie ; Impala, JupyterLab et Kerberos sont dans la cible de production.)*

### Slide 21 — Chronologie du pipeline *(à dire lentement)*
« Voici ce qui arrive concrètement à la donnée, dans l'ordre : lecture du Landing, typage tolérant, normalisation, déduplication, contrôles qualité, pose des flags ligne par ligne, historisation, et audit Iceberg. Entre Bronze et Silver : normalisation, DQ, écriture des tables QUALITY_RESULTS et LOG_PIPELINE. Entre Silver et Gold : jointures, agrégation, modèle en étoile, export Power BI. »

### Slide 22 — Modèle Gold
« Le modèle Gold s'organise autour de la table pivot **IMAGE_CONTRAT**, qui relie les faits — crédits, impayés, dépassements, garanties, autorisations — aux dimensions — tiers, produit, portefeuille, organisation. Le tout consolidé dans une table de synthèse, la One Big Table gdw_synthese_credit. »
🔄 « Voyons maintenant la réalisation concrète. »

---

## PARTIE 5 — RÉALISATION — ~5 min *(le cœur, prends ton temps)*

### Slide 24 — Ingestion NiFi
« L'ingestion est pilotée par NiFi : un flux GetFile → UpdateAttribute → ExecuteScript → PutHDFS dépose automatiquement les fichiers sources dans la zone Landing HDFS. »

### Slide 25 — Exécution Bronze→Silver (run réel) *(moment fort)*
« Voici une exécution réelle sur la table CREDIT_AMORT, 200 000 lignes. Le moteur applique **41 règles de qualité**, dont 14 en échec — par exemple des codifications absentes. Point clé : sur ces 200 000 lignes, **176 281 passent, 23 719 sont en avertissement, et zéro sont rejetées**. On flague, on ne supprime pas. Les résultats sont tracés dans QUALITY_RESULTS et LOG_PIPELINE. »

### Slide 26 — Exécution Gold (run réel)
« L'étage Gold construit ensuite tout le modèle en étoile : les dimensions historisées en SCD2 — tiers, produit, portefeuille — et les faits en snapshot — encours, impayés, garanties. Par exemple 1,6 million de lignes d'impayés, 150 000 tiers. Le tout en une exécution, terminée par un SUCCESS. »

### Slide 27 — Orchestration Airflow
« L'ensemble est orchestré par un DAG Airflow : vérification du Landing, puis les 13 tables Bronze/Silver en séquence, puis le Gold, puis une recette automatique. Un seul déclenchement par date d'arrêté, et le pipeline est rejouable sans créer de doublon. »

### Slide 28 — Lineage Apache Atlas
« La traçabilité bout-en-bout est assurée par Apache Atlas : on voit ici le parcours complet d'une donnée, du Landing jusqu'aux KPI Gold, en passant par Bronze et Silver. C'est la réponse directe au "pas de Data Lineage" de l'existant. »

### Slide 30 — CI/CD Jenkins
« Côté industrialisation, un pipeline Jenkins : récupération du code, compilation, **validation des configurations YAML**, analyse SonarQube, et déploiement des DAGs. La qualité logicielle est automatisée. »

### Slides 30-34 — Tableaux de bord Power BI *(enchaîne, ~1 min 30 pour les 5)*
« La restitution, enfin, se fait sur cinq tableaux de bord Power BI alimentés par la couche Gold : »
- « **D1 — Pilotage global** : la vue synthétique, 23 milliards d'encours, la répartition par famille, la tendance, et le top des clients à risque. »
- « **D2 — Risque & Impayés** : suivi du recouvrement, aging des impayés, classes CDL. »
- « **D3 — Production de Crédit** : 200 000 crédits, production par famille et par agence, taux d'acceptation. »
- « **D4 — Autorisations & Dépassements** : autorisé vs utilisé, entonnoir demande → décaissement. »
- « **D5 — Garanties & Couverture** : taux de couverture du portefeuille et répartition des garanties. »
🔄 « Pour conclure. »

---

## CONCLUSION — ~1 min 30

### Slide 35 — Conclusion
« En conclusion, j'ai livré un **framework de pipelines médaillon standardisé** pour l'axe Crédit-Garantie : 13 tables traitées par deux moteurs génériques pilotés par YAML, un moteur de qualité déclaratif, l'historisation SCD2, la gouvernance Atlas/Ranger, le tout industrialisé en CI/CD et restitué sur cinq tableaux de bord. »
« La valeur : **ajouter une table ou une règle ne demande plus de code, seulement de la configuration** — c'est réutilisable, maintenable et traçable. »
« En perspective : le passage à l'exécution distribuée YARN, l'intégration d'Impala pour le datamart, et l'extension du framework aux autres axes métier de la banque. »
« Je vous remercie pour votre attention et reste à votre disposition pour vos questions. »

---

## Questions probables du jury (prépare 1 phrase par point)
- **Pourquoi Iceberg et pas Delta/Hudi ?** → ACID + time-travel + intégration Hive/Spark native, format ouvert.
- **11 ou 7 types de règles DQ ?** → harmonise ta slide : le moteur en supporte **11** (not_null, unique, in_list, regex, range, referential, email, max_length, date_valid, distinct_count, consistency).
- **Pourquoi flaguer et ne pas rejeter ?** → exigence d'audit : on doit pouvoir tout expliquer ; le rejet silencieux perd de l'information.
- **SCD2 sans MERGE Iceberg ?** → reconstruction du DataFrame complet + createOrReplace (limite Iceberg 1.3.1).
- **Volumétrie réelle ?** → POC sur environnement de validation, Spark en mode local ; la cible est YARN distribué.

## Corrections avant le jour J *(pagination / cohérence)*
1. Slide Plan : `1/25` → `1/35`.
2. Slide D4 Autorisations : `23/25` → `33/35`.
3. Slide Solution (17) : « 7 types de règles » → **11** (aligne avec l'Annexe DQ).
4. Retirer les 2 captures parasites du Bureau (bandeau vide + interface Canva) si elles ont servi de base.
