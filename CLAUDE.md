# Projet — Plateforme DATA standardisée Crédit-Garantie (BOA Entreprise)

## Contexte
PFE / stage de fin d'études à BOA Entreprise (banque).
Objectif : construire un framework standardisé pour pipelines médaillon
(Bronze / Silver / Gold) sur l'axe **Crédit-Garantie**.

Périmètre actuel : **POC / environnement de test** destiné à valider le
pattern avant industrialisation en production. La priorité est la
**réutilisabilité** du framework, pas la volumétrie.

## Environnement de travail
- **Dev** : machine locale (édition de code uniquement, aucune exécution)
- **Exécution** : VM Ubuntu (hostname `ubuntu3`, user `hadoop`, 9.5 Go RAM)
- **Transfert** : dossier partagé VirtualBox — le code édité en local est
  immédiatement visible sur la VM

## Stack technique cible (sur la VM)
- **Ingestion** : Apache NiFi
- **Stockage** : HDFS + Apache Iceberg (via Hive Metastore) — PAS Hive storage pur
- **Traitement** : Spark 3.x (PySpark) sur YARN
- **Orchestration** : Apache Airflow
- **Gouvernance** : Apache Atlas (lineage), Apache Ranger (sécurité)
- **BI** : Tableau
- **CI/CD** : GitLab + Jenkins + SonarQube (tous en Docker)

## Architecture médaillon
- **Bronze** : données brutes, 1:1 avec la source, format Parquet/Iceberg
- **Silver** : données typées, dédupliquées, PK validées, format Iceberg
- **Gold** : tables de faits + dimensions, modèle en étoile, format Iceberg

## Conventions de nommage
- Tables Bronze : `bronze_<source>_<objet>` (ex: `bronze_sftp_credit_amort`)
- Tables Silver : `silver_<objet>` (ex: `silver_credit_amort`)
- Tables Gold : `gold_fact_<kpi>` / `gold_dim_<entite>`
- Jobs : `JOB_<LAYER>_<PROCESS>`
- Partitionnement : toujours par `date_arrete` (jamais par date d'ingestion)
  — exigence d'audit Bâle III

## Structure de datalake_lib
- `config/` : loader YAML avec validation (pydantic)
- `read/` : lecteurs paramétrés (SFTP, SAP, Iceberg, Parquet)
- `quality/` : moteur de règles DQ déclaratives
- `write/` : écriture Iceberg (append / merge / SCD2)
- `log/` : table audit_jobs + metrics
- `utils/` : helpers (spark session, etc.)

## Règles de qualité (DQ) à supporter
- `not_null` : complétude (PK, colonnes critiques)
- `unique` : unicité (PK simple ou composite)
- `referential` : existence de la FK dans table parente
- `regex` : conformité de format (dates, codes produits POPS)
- `range` : bornes numériques (montants, scores)
- `in_set` : valeurs dans une typologie

## Périmètre données — 12 canevas Excel
Fichiers dans `/canevas/` :
1. ModelTiers.xlsx — référentiel tiers / clients
2. Credit_.xlsx — crédits amortissables
3. Credit_non_amort.xlsx — crédits non amortissables
4. GARANTIE.xlsx — garanties (contrats, détails)
5. IDCE.xlsx — dossiers entreprise (cycle de vie)
6. IMPAYES.xlsx — historique d'impayés
7. DEPASSEMENT.xlsx — dépassements de plafonds
8. IMAGE_CONTRAT.xlsx — **table pivot centrale** tiers ↔ produits
9. POPS.xlsx — référentiel produits
10. Typologie.xlsx — référentiels métier
11. ASAL.xlsx — agents commerciaux
12. Feuille_de_calcul_dans_PFE_FRAMEWORK (ORG) — référentiel organisationnel

## Règles de parsing des canevas Excel
Critique — ces règles ont été établies par trial and error :
- La ligne contenant "Champ EDD" marque le **DÉBUT** de la section champs métier
  (PAS la fin — erreur classique)
- Les champs ne commencent PAS à la colonne A
- Détecter `col_offset` via `row.index("Champ EDD")`
- Slicer les données avec `vals[col_offset:]`
- Colonnes attendues après slice : champ, format, longueur, description, PK
- PK détectée si 5ᵉ valeur paddée ∈ `('O','OUI','X','PK','1','TRUE')`
- Filtrer les None : `[v for v in row if v is not None]`
- Détection de la section body : rows dont la 1ʳᵉ valeur contient
  `'DETAIL'`, `'CORPS'` ou `'BODY'`
- Utiliser **openpyxl en mode read-only** pour éviter les problèmes mémoire
- Limiter à ~40 colonnes par feuille pour éviter le débordement de contexte

## Ambiguïtés du modèle de données

### Source de vérité
Diagramme de jointures validé : `docs/diagram jointure.jpeg`
La règle de nommage dure (jointure uniquement si noms identiques)
est annulée. Les jointures confirmées par le diagramme sont valides
même si les noms de champs diffèrent.

### Ambiguïtés RÉSOLUES (par le diagramme)
1. ✅ NUM_COMPTE_EMPRUNTEUR (CREDIT_AMORT) = IDF_REF_EXT_CTR (IMAGE_CONTRAT)
2. ✅ NUM_COMPTE (DEPASSEMENT) = IDF_REF_EXT_CTR (IMAGE_CONTRAT)
3. ✅ PK DEPASSEMENT = NUM_COMPTE + DATE_ENTREE + COMPTE_GENERAL + MNT_DEPASSEMENT
4. ✅ IDF_NUM_DOSS (IMPAYES_CDL) → IDF_NUM_DOSS (CREDIT_AMORT) — lien direct
5. ✅ COD_PRODUIT (CREDIT_AMORT) = IDF_CODE_PRODUIT (POPS) — jointure directe
6. ✅ CONCAT(IDF_TYP_PRD, IDF_REF_PRD) = IDF_CODE_PRODUIT (POPS) — via IMAGE_CONTRAT
7. ✅ IDF_REF_AUTO (AUTORISATIONS) = IDF_REF_EXT_CTR (IMAGE_CONTRAT)
8. ✅ PORTEFEUILLE → ORG via IDF_NUM_GUI
9. ✅ TIERS ↔ IMAGE_CONTRAT via IDF_NUM_TIERS
10. ✅ TIERS ↔ TYPOLOGIE via IDF_NUM_TIERS
11. ✅ TIERS ↔ GARANTIE_TIERS via IDF_NUM_TIERS
12. ✅ GARANTIE_TIERS ↔ GARANTIE_DETAIL ↔ GARANTIE_CONTRAT via IDF_NUM_CTR_GARANTIE

### Ambiguïté N°6 — RÉSOLUE (par le diagramme)
13. ✅ GARANTIE_CONTRAT.IDF_REF_EXT_CREDIT = IMAGE_CONTRAT.IDF_REF_EXT_CTR (option a).
Tranché par `docs/diagram jointure.jpeg` (boîte de jointure
`IDF_REF_EXT_CTR = IDF_REF_EXT_CREDIT`). La garantie se relie au crédit via le hub
IMAGE_CONTRAT, pas en direct. Plus aucune ambiguïté ouverte sur le modèle de jointures.

## Préférences de travail
- Procéder **étape par étape**, ne pas sauter d'étapes
- **Montrer le plan** avant d'exécuter une grosse tâche, attendre validation
- **Demander avant de supprimer** quoi que ce soit
- **Documentation technique en français**
- **Tests en premier** (TDD) quand c'est possible
- Ne jamais inventer une structure de données non vérifiée dans les canevas

## Ce qu'on NE fait PAS en local
- Pas d'installation de dépendances (pas de venv, pas de pip install)
- Pas d'exécution de Spark, Airflow, ou tests pytest
- Aucune connexion réseau vers la VM
- Le code est juste écrit et sauvegardé, l'exécution se fait sur la VM

## Git
Repo GitLab sur la VM : `http://localhost:8929/root/datalake-pipeline`
Remote distant nommé `local` (sur la VM, pour le self-hosted GitLab).
