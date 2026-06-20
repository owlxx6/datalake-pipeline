# État session — reprise post-compact (2026-06-14)

HEAD main = `27c01c6`. Tout committé côté Mac (repo canonique). Dossier partagé VM = ce repo.

## Sprints — statut
- ✅ Silver (H1 Iceberg + H3 SCD2 7 dimensions) · normalisation 13/13 · recette `check_lake_sanity.py` 23/23
- ✅ Gold : moteur déclaratif `jobs/gold/engine.py` (date/dimension/fact/obt/view) + OBT `gdw_synthese_credit` + vue `v_kpi_credit`
- ✅ DQ+ : QUALITY_RESULTS iso-banque + advanced_controls + 11 types de checks
- ✅ S1 Airflow : DAG `credit_garantie_medaillon` 16/16 vert
- ✅ S3 CI/CD : GitLab+Jenkins+SonarQube, build 5 stages SUCCESS, Sonar A/0% dup, issues 29→traitées
- ✅ S4 : REJECTED documenté (flag, jamais suppression) ; Lot A entête fichier = perspective
- ✅ S7 : gouvernance Atlas — lineage médaillon poussé & vérifié (46 datasets / 32 process,
  OBT = 27 rel/28 nœuds) ; module `jobs/governance/atlas_lineage.py` + runbook §9. Capture UI faite.
- 🔄 S2 Power BI (data PRÊTE — 2026-06-20) : Gold étendu + reconstruit pour les 5 dashboards.
  Cibles buildées : dim_tiers, dim_portefeuille (pont tiers→agence), faits garanties/autorisations/
  depassement, gdw_impayes enrichi (date_mise_impaye + date_echeance pour aging calendaire).
  Export propre `exports/` = 11 CSV **sans doublon** (gdw_encours + v_kpi_credit exclus via EXCLUDE
  dans `scripts/export_gold_powerbi.py`). Guide `docs/guide_powerbi_credit_garantie.md` à jour
  (relations dim_portefeuille→dim_organisation, DAX aging calendaire + par classe CDL, visuels D1-D5).
  Reste (manuel, PC Windows) : monter les 5 dashboards en suivant le guide ; option segment COD_MARCHE.
- ⏳ Reste : S-CLEAN · S5 métier · S6 rapport

## RELIQUATS IMPORTANTS (à ne pas oublier)
1. ✅ **Non-régression FAITE (2026-06-17)** : CREDIT_AMORT rejoué après les refactors Sonar →
   PASSED=176281 / WARNING=23719 / REJECTED=0 exact. Comportement inchangé confirmé.
2. ✅ **Remotes à jour + CI verte (2026-06-17)** : GitHub origin + GitLab local poussés (f6ccef1).
   Build Jenkins **SUCCESS** (compile / YAML / SonarQube ANALYSIS SUCCESSFUL 24 fichiers / deploy DAG).
   NB : stage compile ne couvre pas encore `jobs/governance/*.py` (Sonar oui) — élargir le glob Jenkinsfile.
3. Sonar : fichiers pas encore re-scannés en détail (historization.py, gold/engine.py, config_loader.py,
   scripts diag/inspect/generate/simulate) — corriger si signalés.
4. SÉCURITÉ : régénérer le token GitLab exposé `glpat-IVYf…` (visible dans jobs/.../config.xml + remote).

## Baselines de vérification (ne pas perdre)
- CREDIT_AMORT DQ : PASSED 176281 / WARNING 23719 / REJECTED 0 (sur 200000)
- Lake sanity 23/23 OK · OBT gdw_synthese_credit : 200000, famille_renseignee 198981, flag_cdl 96474
- Silver dims SCD2 v1 : tiers 154245 · image_contrat 158322 · grc 151528 · typologie 154249 · asal 4464 · pops 1226 · org 762
- Faits : credit_amort 200000 · impayes_cdl 1597242 · depassement 75870 · garantie_detail 253900 · garantie_contrat 258473 · idce_autorisation 368567
- Gold buildé (2026-06-20, arrêté 2026-05-31) : gdw_synthese_credit 200000 · gdw_impayes 1597242 (+ aging) ·
  dim_tiers 154245 · dim_portefeuille 151528 · dim_produit 1226 · dim_organisation 762 · dim_temps 3653

## Topologie repos (IMPORTANT)
- Repo canonique = ce dossier (Mac, origin GitHub owlxx6) — contient code + CI (Jenkinsfile, docker-compose,
  Dockerfile.jenkins, sonar-project.properties).
- GitLab local (VM, `http://localhost:8929/root/datalake-pipeline`) = ce que Jenkins build ; on y pousse via
  remote `gitlab` (depuis /media/sf_datalake-pipeline après `git config --global --add safe.directory ...`).
- Clone VM `~/datalake-pipeline` = disque natif, héberge `jenkins_data/` + lance docker-compose (PAS le
  dossier partagé : docker n'aime pas vboxsf).

## CI/CD — exploitation (VM)
- docker v1 → commande `docker-compose` (avec tiret), pas `docker compose`.
- Stack lourde sur 9,5 Go : GitLab(~3-4G)+Jenkins(~1G)+Sonar(~2G). Pour pousser, couper Sonar+Jenkins
  (`docker stop sonarqube-server jenkins-server`) le temps que GitLab serve, puis rallumer.
- SonarQube sur **port 9001** (pas 9000). Attendre `status:UP` avant un build (sinon scan échoue « STARTING »).
- Build : Jenkins http://ubuntu3:8081 (user admin ; mot de passe = voir gestionnaire de secrets, non committé) → datalake-pipeline → Build Now.
- Token Sonar pour l'API issues : My Account → Security → Generate ; `curl -u "$TOKEN:" .../api/issues/search`.

## Checklist post-reboot VM (ordre strict — cf. runbook)
```
start-dfs.sh ; hdfs dfsadmin -safemode get (leave si ON)
nohup hive --service metastore > ~/logs/metastore.log 2>&1 &   # port 9083 (requis Iceberg)
# CI : cd ~/datalake-pipeline && docker-compose up -d   (puis docker start sonarqube-server si conflit)
# Airflow (si besoin) : 1 SEUL scheduler + webserver --port 8090 depuis /home/hadoop, venv activé
```

## S2 Power BI — approche retenue (mode Import, pas de live)
Power BI Desktop = Windows-only. Choix POC : **mode Import** (CSV statiques), pas de Thrift Server.
Pipeline : build Gold (VM) → `scripts/export_gold_powerbi.py` → CSV dans `exports/` → transfert PC
Windows → import + DAX (cf. `docs/guide_powerbi_credit_garantie.md`). `exports/` = dossier de
transfert (gitignoré, effacé/recréé à chaque export) ; emporter le guide séparément.

## VM — incident 2026-06-20 (résolu)
Crash après MAJ VirtualBox (saved-state VGA corrompu) → discardstate + cold boot. Boots suivants
figés (segfault `finit_module` au chargement module noyau). Disque SAIN (fstab propre, 0 unité en
échec). Résolu par un `reboot` propre depuis recovery ; Guest Additions OK (dossier partagé monté).

## Prochaine action au choix
Monter les dashboards (Windows) · S-CLEAN · S5 métier · S6 rapport.
