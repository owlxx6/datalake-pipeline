# État session courante — reprise S3 (2026-06-12)

## Où on en est
- **S1 Airflow : BOUCLÉ** — DAG credit_garantie_medaillon 16/16 vert (commit cfc5d59),
  runbook section Airflow, recette sanity exécutée par l'orchestrateur.
  Captures UI : à confirmer (Graph vert + recette + Grid).
- **Ordre acté : S3 CI/CD → S4 DQ finitions → S2 Power BI → S-CLEAN** (+ S5 métier, S6 rapport).
- Vieux dag_silver_credit.py supprimé (commit aa4b28b). Sprint S-CLEAN cadré au plan.

## S3 CI/CD — démarré, plan validé à confirmer
- Pipeline .gitlab-ci.yml proposé : compile (py_compile) → lint (ruff/flake8)
  → validate-config (YAML+pydantic bronze/gold — le contrat déclaratif en CI) → sonar.
- Taha dit : GitLab + runner + SonarQube DÉJÀ installés/configurés sur la VM.
- GitLab self-hosted : http://localhost:8929/root/datalake-pipeline (Docker), remote git « local ».
- SonarQube + jenkins.war + jdk17 dans /home/hadoop/apps/.
- EN ATTENTE (commandes envoyées, sorties pas encore reçues) :
  free -h · ps aux --sort=-%mem · docker ps · which gitlab-runner / gitlab-runner list
  · ls ~/apps/sonarqube/bin
- Allègement VM en cours : pkill airflow/gunicorn + kill metastore (11659) OK pour S3 ;
  garder HDFS + GitLab docker.

## Repères VM (post-reboot, ordre strict — cf. runbook)
start-dfs.sh → safemode get → metastore (nohup hive --service metastore) →
airflow scheduler (nohup, UN SEUL) → webserver --port 8090 --workers 1 (depuis /home/hadoop).

## Git
HEAD main = aa4b28b. Tout est committé (code + docs + plan). Non tracké volontairement :
docs/REPORT_DATA_PACK.md, docs/modele_gold_credit_garantie.md, docs/rapport/, echantillon/.
