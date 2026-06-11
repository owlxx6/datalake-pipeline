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

### Source de vérité (révisée 2026-06-11)
1. **Donnée réelle** (validation empirique — `scripts/test_jointures_diagramme.py`
   et `scripts/diag_jointures_ko.py`, arrêté 2026-05-31)
2. **Canevas EDD** (sémantique des champs)
3. Diagramme `docs/diagram jointure.jpeg` — structure juste, mais 3 jointures
   fausses corrigées par la donnée (rapprochements par similarité de noms).
La règle de nommage dure (jointure uniquement si noms identiques) reste annulée.

### Jointures VALIDÉES par la donnée (taux de match réels)
1. ✅ NUM_COMPTE_EMPRUNTEUR (CREDIT_AMORT) = IDF_REF_EXT_CTR (IMAGE_CONTRAT) — 99,99%
2. ✅ NUM_COMPTE (DEPASSEMENT) = IDF_REF_EXT_CTR (IMAGE_CONTRAT) — 100%
3. ✅ PK DEPASSEMENT = NUM_COMPTE + DATE_ENTREE + COMPTE_GENERAL + MNT_DEPASSEMENT
4. ✅ IDF_NUM_DOSSIER (IMPAYES_CDL) = IDF_NUM_DOSS (CREDIT_AMORT) — 100%
5. ✅ **CORRIGÉE** : COD_PRODUIT (CREDIT_AMORT) = **COD_SOP** (POPS) — 99,49%.
   (Diagramme disait IDF_CODE_PRODUIT → 0% : c'est un « code activité » selon l'EDD,
   pas le code produit. COD_PRODUIT est le code du système opérant.)
6. ✅ CONCAT(IDF_TYP_PRD, IDF_REF_PRD) = IDF_CODE_PRODUIT (POPS) — 100%
   (chemin officiel encadrant : type du produit depuis un contrat du hub)
7. ✅ **CORRIGÉE** : **NUM_COMPTE** (IDCE_AUTORISATION) = IDF_REF_EXT_CTR — 100%.
   (Diagramme disait IDF_REF_AUTO → 0% : c'est la PK interne de l'autorisation,
   séquence 16 chiffres, « Référence autorisation » selon l'EDD.)
   Bonus : NUM_TIERS (IDCE_AUTORISATION) = IDF_NUM_TIERS (TIERS) — 99,96%.
8. ✅ GRC_PORTEFEUILLE.IDF_NUM_GUI = ORG.IDF_NUM_GUI — 100% (151 528/151 528 ;
   ORG ingérée via NiFi le 2026-06-11, 762 lignes, 7e dimension SCD2)
9. ✅ TIERS ↔ IMAGE_CONTRAT via IDF_NUM_TIERS — 100%
10. ✅ TIERS ↔ TYPOLOGIE via IDF_NUM_TIERS — 100%
11. ⏳ TIERS ↔ GARANTIE_TIERS via IDF_NUM_TIERS (table non industrialisée)
12. ✅ GARANTIE_CONTRAT ↔ GARANTIE_DETAIL via IDF_NUM_CTR_GARANTIE — 99,91%
13. ✅ **CORRIGÉE (ambiguïté n°6, option b confirmée)** :
   GARANTIE_CONTRAT.IDF_REF_EXT_CREDIT = **CREDIT_AMORT.IDF_NUM_DOSS** — 100%.
   La garantie se relie au crédit EN DIRECT (la note de réunion avait raison),
   pas via le hub (option a du diagramme → 0%). EDD : « Référence BTA ».
   NB : NUM_CTR_CREDIT = « Référence interne du crédit (Non exploitée) » (EDD) → 0% partout.

### Leçon (à retenir pour toute nouvelle jointure)
Les 3 erreurs du diagramme venaient de rapprochements par SIMILARITÉ DE NOMS
(REF_EXT↔REF_EXT, REF↔REF, PRODUIT↔PRODUIT). Toute jointure se valide contre
la donnée réelle (taux de match) + la sémantique EDD avant d'entrer dans le modèle.

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
