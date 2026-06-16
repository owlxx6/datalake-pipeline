# Runbook — Datalake Pipeline (ubuntu3)

> Toutes les commandes sont à exécuter sur **ubuntu3** depuis la racine du projet.
> `$PROJECT` = chemin absolu du projet (ex : `/home/hadoop/datalake-pipeline`)

---

## 1. Démarrage des services

### 1.1 Ordre obligatoire

```
HDFS → Spark Standalone (Master + Worker) → Hive Metastore → NiFi
```

### 1.2 HDFS

```bash
start-dfs.sh
```

Vérifier que le Safe Mode est désactivé après le démarrage :

```bash
hdfs dfsadmin -safemode get
# Si "ON" → forcer la sortie :
hdfs dfsadmin -safemode leave
```

### 1.3 Spark Standalone

```bash
$SPARK_HOME/sbin/start-all.sh
```

Vérifier que Master et Worker sont actifs :

```bash
jps
# Attendu : Master + Worker
```

L'UI Spark Master est accessible sur `http://ubuntu3:8080`

### 1.4 Hive Metastore

```bash
nohup hive --service metastore > ~/logs/metastore.log 2>&1 &
```

Vérifier que le port 9083 est ouvert (attendre ~15s) :

```bash
netstat -tlnp | grep 9083
```

### 1.5 NiFi

```bash
$NIFI_HOME/bin/nifi.sh start
```

NiFi 2.x écoute sur HTTPS — vérifier qu'il est actif (attendre ~30s) :

```bash
curl -sk https://localhost:8443/nifi/ -o /dev/null -w "%{http_code}"
# Réponse attendue : 200
```

> Si `$NIFI_HOME` n'est pas défini : `find /opt /home /usr/local -name "nifi.sh" 2>/dev/null`

---

### 1.6 Vérification globale

```bash
jps
```

Processus attendus :

| Processus        | Rôle                        |
|------------------|-----------------------------|
| NameNode         | HDFS — coordonnateur        |
| DataNode         | HDFS — stockage             |
| Master           | Spark Standalone — master   |
| Worker           | Spark Standalone — worker   |
| RunJar           | Hive Metastore              |
| NiFi             | Ingestion XLSX → HDFS       |

---

## 2. Arrêt des services

### 2.1 Ordre obligatoire (inverse du démarrage)

```
NiFi → Hive Metastore → Spark Standalone → HDFS
```

### 2.2 NiFi

```bash
$NIFI_HOME/bin/nifi.sh stop
```

Vérifier l'arrêt :

```bash
curl -sk https://localhost:8443/nifi/ -o /dev/null -w "%{http_code}"
# Réponse attendue : 000 (connexion refusée = arrêté)
```

### 2.3 Hive Metastore

```bash
kill $(jps | grep RunJar | awk '{print $1}')
```

### 2.4 Spark Standalone

```bash
$SPARK_HOME/sbin/stop-all.sh
```

Ou individuellement :

```bash
$SPARK_HOME/sbin/stop-master.sh
$SPARK_HOME/sbin/stop-worker.sh
```

### 2.5 HDFS

```bash
stop-dfs.sh
```

---

## 3. Ingestion NiFi (XLSX → HDFS Landing)

### 3.1 Déposer un fichier à ingérer

Copier le fichier XLSX dans la drop zone de la table concernée :

```bash
# TIERS
cp /path/to/tiers.xlsx /opt/nifi/drop-zone/tiers/

# IMAGE_CONTRAT
cp /path/to/image_contrat.xlsx /opt/nifi/drop-zone/image_contrat/
```

NiFi surveille ces dossiers en continu — le fichier est traité automatiquement (GetFile → ConvertRecord → PutHDFS).

### 3.2 Vérifier l'arrivée en HDFS Landing

```bash
# TIERS — vérifier la partition du jour
hdfs dfs -ls hdfs://ubuntu3:9000/data/landing/TIERS/

# IMAGE_CONTRAT
hdfs dfs -ls hdfs://ubuntu3:9000/data/landing/IMAGE_CONTRAT/

# Compter les fichiers dans une partition (remplacer la date)
hdfs dfs -ls "hdfs://ubuntu3:9000/data/landing/TIERS/date_arrete=20260515/"
```

### 3.3 Statut NiFi

```bash
# Vérifier que NiFi tourne
$NIFI_HOME/bin/nifi.sh status

# Ou via HTTP
curl -sk https://localhost:8443/nifi/ -o /dev/null -w "%{http_code}"
```

### 3.4 Logs NiFi (en cas d'erreur)

```bash
tail -200 $NIFI_HOME/logs/nifi-app.log | grep -E "ERROR|WARN"
```

### 3.5 Drop zones disponibles

| Table          | Drop zone                           |
|----------------|-------------------------------------|
| TIERS          | `/opt/nifi/drop-zone/tiers/`        |
| IMAGE_CONTRAT  | `/opt/nifi/drop-zone/image_contrat/`|

---

## 4. Bronze DQ Engine

> **Mémoire driver** — pour les tables volumineuses (> 1 M lignes, ex. IMPAYES_CDL
> ≈ 1,7 M), ajouter `--driver-memory 4g` au `spark-submit`. Sans ça, l'écriture
> Silver (tri partitionné) tombe en `OutOfMemoryError: Java heap space` car le
> driver est à 1 Go par défaut. `spark.driver.memory` ne se règle PAS dans le YAML
> (la JVM driver est déjà lancée) — uniquement en CLI ou `spark-defaults.conf`.
> En cas de doute, mettre `--driver-memory 4g` par défaut (VM = 9,5 Go).

### 3.1 Toutes les tables d'un domaine

```bash
spark-submit \
  --master spark://ubuntu3:7077 \
  --driver-memory 4g \
  $PROJECT/jobs/bronze/engine.py \
  --config $PROJECT/configs/bronze/credit_garantie.yaml \
  --date-arrete 2026-05-15
```

### 3.2 Une table spécifique

```bash
spark-submit \
  --master spark://ubuntu3:7077 \
  $PROJECT/jobs/bronze/engine.py \
  --config $PROJECT/configs/bronze/credit_garantie.yaml \
  --date-arrete 2026-05-15 \
  --table TIERS
```

```bash
spark-submit \
  --master spark://ubuntu3:7077 \
  $PROJECT/jobs/bronze/engine.py \
  --config $PROJECT/configs/bronze/credit_garantie.yaml \
  --date-arrete 2026-05-15 \
  --table IMAGE_CONTRAT
```

### 3.3 Dry run (sans écriture)

Modifier temporairement `execution.dry_run: true` dans le YAML, ou lancer :

```bash
spark-submit \
  --master spark://ubuntu3:7077 \
  $PROJECT/jobs/bronze/engine.py \
  --config $PROJECT/configs/bronze/credit_garantie.yaml \
  --date-arrete 2026-05-15 \
  --dry-run
```

---

## 5. Vérification des résultats (spark-sql)

Lancer spark-sql :

```bash
spark-sql \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.spark_catalog=org.apache.iceberg.spark.SparkSessionCatalog \
  --conf spark.sql.catalog.spark_catalog.type=hive \
  --conf spark.sql.catalog.spark_catalog.uri=thrift://ubuntu3:9083
```

### 5.1 LOG_PIPELINE — historique des exécutions

```sql
SELECT execution_id, table_name, job_status, rows_read, rows_written,
       rows_rejected, start_time, end_time
FROM silver_credit_garantie.BR_CREDIT_GARANTIE_LOG_PIPELINE
ORDER BY start_time DESC
LIMIT 20;
```

### 5.2 QUALITY_RESULTS — détail des contrôles DQ

```sql
SELECT table_name, field_name, check_type, severity,
       records_checked, records_failed, failure_rate_pct, check_status
FROM silver_credit_garantie.BR_CREDIT_GARANTIE_QUALITY_RESULTS
WHERE date_situation = '2026-05-15'
ORDER BY table_name, severity, failure_rate_pct DESC;
```

Contrôles en échec uniquement :

```sql
SELECT table_name, field_name, check_type, severity,
       records_failed, failure_rate_pct
FROM silver_credit_garantie.BR_CREDIT_GARANTIE_QUALITY_RESULTS
WHERE date_situation = '2026-05-15'
  AND check_status = 'FAILED'
ORDER BY severity, failure_rate_pct DESC;
```

### 5.3 LOGS_REJECTED — lignes rejetées

```sql
SELECT table_name, rejection_reason, COUNT(*) AS nb_rejets
FROM silver_credit_garantie.BR_CREDIT_GARANTIE_LOGS_REJECTED
WHERE date_situation = '2026-05-15'
GROUP BY table_name, rejection_reason
ORDER BY nb_rejets DESC;
```

Aperçu des lignes rejetées pour une table :

```sql
SELECT *
FROM silver_credit_garantie.BR_CREDIT_GARANTIE_LOGS_REJECTED
WHERE date_situation = '2026-05-15'
  AND table_name = 'IMAGE_CONTRAT'
LIMIT 20;
```

### 5.4 Silver Hive — données clean

```sql
-- Compter les lignes clean par date
SELECT date_arrete, COUNT(*) AS nb_lignes
FROM silver_credit_garantie.tiers
GROUP BY date_arrete;

SELECT date_arrete, COUNT(*) AS nb_lignes
FROM silver_credit_garantie.image_contrat
GROUP BY date_arrete;
```

```sql
-- Aperçu données clean
SELECT * FROM silver_credit_garantie.tiers LIMIT 5;
SELECT * FROM silver_credit_garantie.image_contrat LIMIT 5;
```

### 5.5 Vérifier les tables disponibles dans le domaine

```sql
SHOW TABLES IN silver_credit_garantie;
```

---

## 6. Commandes HDFS utiles

```bash
# Lister les données Bronze d'une table
hdfs dfs -ls hdfs://ubuntu3:9000/data/bronze/tiers/TIERS/

# Lister les données Landing d'une table
hdfs dfs -ls hdfs://ubuntu3:9000/data/landing/TIERS/

# Vérifier l'espace disque HDFS
hdfs dfsadmin -report | head -20

# Voir le contenu d'un répertoire Bronze (parquet)
hdfs dfs -ls "hdfs://ubuntu3:9000/data/bronze/tiers/TIERS/date_arrete=2026-05-15/"
```

---

## 7. Résolution de problèmes fréquents

| Symptôme | Cause | Commande |
|----------|-------|----------|
| `SafeModeException: Name node is in safe mode` | HDFS démarre en safe mode (attend le report des blocs) | Attendre ~30 s puis `hdfs dfsadmin -safemode leave` (vérifier `jps` : NameNode + DataNode présents) |
| `ConnectException: ... to ubuntu3:9000 ... Connection refused` | HDFS pas démarré (NameNode down) | Respecter l'ordre §1 : `start-dfs.sh` AVANT Spark/engine |
| `OutOfMemoryError: Java heap space` à l'écriture Silver | Driver Spark à 1 Go par défaut, tri partitionné sur gros volume | Relancer avec `--driver-memory 4g` (cf. §4) |
| `Connection refused` sur port 9083 | Hive Metastore pas démarré | `nohup hive --service metastore > ~/logs/metastore.log 2>&1 &` |
| `AnalysisException: Table not found` | Base Silver pas créée | Lancer le engine une première fois, il crée les tables Iceberg automatiquement |
| `SparkUpgradeException: WRITE_ANCIENT_DATETIME` | Dates antérieures à 1582 | Vérifier `datetimeRebaseModeInWrite: CORRECTED` dans le YAML |
| `AnalysisException: Window functions inside WHERE` | Check `unique` mal géré | Bug déjà corrigé dans `engine.py` — vérifier la version du fichier |
| NiFi : fichier XLSX non traité | Processor arrêté ou drop zone vide | Vérifier UI NiFi `https://localhost:8443/nifi/` — démarrer les processors |
| NiFi : `000` au curl | NiFi pas démarré | `$NIFI_HOME/bin/nifi.sh start` puis attendre 30s |

---

## 8. Ajouter une nouvelle table

1. Ouvrir `configs/bronze/credit_garantie.yaml`
2. Ajouter une entrée dans la liste `tables:` en suivant le pattern TIERS ou IMAGE_CONTRAT
3. Lancer uniquement cette table pour valider :
   ```bash
   spark-submit ... --table NOM_TABLE
   ```
4. Vérifier les résultats dans spark-sql (sections 4.1 à 4.4)

Aucune modification de `engine.py` ou `config_loader.py` n'est nécessaire.

## Airflow (orchestration)

### Démarrage (APRÈS HDFS + Metastore — jamais avant)
```bash
su - hadoop                      # le tiret est important (bon HOME, bon env)
source ~/airflow-venv/bin/activate
cd /home/hadoop                  # JAMAIS depuis /home/vboxuser (Permission denied gunicorn)
nohup airflow scheduler  > ~/logs/airflow-scheduler.log 2>&1 &
nohup airflow webserver --port 8090 --workers 1 > ~/logs/airflow-webserver.log 2>&1 &
```
- UI : http://ubuntu3:8090 (admin / mot de passe réinitialisé via `airflow users reset-password`)
- Port 8080 = Spark Master UI → webserver Airflow sur **8090**.
- **UN SEUL scheduler à la fois** : un doublon SIGTERM-e les tâches de l'autre (~45 s).
- Éviter `airflow standalone` : couplé tout-ou-rien (webserver mort → scheduler emporté).
- VM chargée : webserver lent à démarrer → `AIRFLOW__WEBSERVER__WEB_SERVER_MASTER_TIMEOUT=300`.
- dags_folder = /media/sf_datalake-pipeline/dags (dossier partagé).

### Déclencher un arrêté
```bash
airflow dags trigger credit_garantie_medaillon --conf '{"date_arrete": "2026-05-31"}'
airflow tasks states-for-dag-run credit_garantie_medaillon "<run_id>" | tail -8
```

### Checklist post-reboot VM (ordre strict)
```bash
start-dfs.sh
hdfs dfsadmin -safemode get          # leave si ON
nohup hive --service metastore > ~/logs/metastore.log 2>&1 &   # ss -tlnp | grep 9083
# puis scheduler + webserver (ci-dessus)
```
⚠️ Metastore absent = les jobs Iceberg « tournent » indéfiniment en retry Thrift silencieux.

---

## 9. Apache Atlas (gouvernance — lineage)

Installé en `/home/hadoop/apache-atlas-2.4.0` (serveur déjà compilé). Backends **externes**
(`MANAGE_LOCAL_*=false`) : HBase standalone + SolrCloud, tous deux sur le **ZooKeeper d'HBase
(2181)**. Atlas REST/UI sur **21000** (admin/admin). Pas besoin d'HDFS : HBase est en
`file:///home/hadoop/hbase/data`.

⚠️ **RAM** : Atlas (HBase+Solr+JVM) ≈ 3-4 Go → ne coexiste PAS avec le stack CI
(GitLab+Jenkins+Sonar). Couper la CI avant : `docker stop gitlab jenkins-server sonarqube-server`.

### 9.1 Démarrage (ordre STRICT : HBase → Solr → Atlas)
```bash
export ATLAS_HOME=/home/hadoop/apache-atlas-2.4.0
# 1) HBase standalone (fournit aussi ZooKeeper 2181)
/home/hadoop/hbase/bin/start-hbase.sh        # jps -> HMaster ; ss -ltn | grep -E ':2181|:16010'
# 2) Solr en mode cloud, branché sur le ZK d'HBase
/home/hadoop/solr/bin/solr start -c -z localhost:2181   # port 8983
# 3) Atlas (démarrage lent : 3-8 min)
python3 $ATLAS_HOME/bin/atlas_start.py
# 4) attendre que l'API réponde
curl -s -u admin:admin http://localhost:21000/api/atlas/admin/version   # 200 + JSON = up
```
> Le log de démarrage est `logs/atlas.<timestamp>.out` (PAS `application.log`, absent ici).
> `grep -a` (fichier binaire).

### 9.2 Panne classique : collections Solr fantômes (500 sur toute lecture)
Symptôme : Atlas répond `version` 200 mais **toute lecture de type/entité = 500**. Log Atlas :
`no core retrieved for vertex_index ... code=404`. Cause : les 3 collections
(`vertex_index`, `edge_index`, `fulltext_index`) sont enregistrées dans ZK mais leur **core
n'a pas d'index** (métadonnée survit au reboot, pas les données). Réparation = recréer :
```bash
python3 $ATLAS_HOME/bin/atlas_stop.py
for c in vertex_index edge_index fulltext_index; do /home/hadoop/solr/bin/solr delete -c $c; done
for c in vertex_index edge_index fulltext_index; do
  /home/hadoop/solr/bin/solr create -c $c -d $ATLAS_HOME/conf/solr -shards 1 -replicationFactor 1
done
python3 $ATLAS_HOME/bin/atlas_start.py
# vérif : GET .../v2/types/typedefs doit renvoyer 200 (plus 500)
```

### 9.3 Pousser le lineage médaillon
Le module dérive le graphe Landing→Bronze→Silver→Gold des MÊMES YAML (stdlib, pas de dépendance) :
```bash
cd /media/sf_datalake-pipeline                 # en user hadoop (accès au partage)
python3 -u jobs/governance/atlas_lineage.py    # --dry-run pour voir sans pousser
```
Résultat attendu : 46 datasets + 32 processus. Vérif lineage :
```bash
curl -s -u admin:admin "http://localhost:21000/api/atlas/v2/lineage/uniqueAttribute/type/medallion_dataset?attr:qualifiedName=gold.gdw_synthese_credit@credit_garantie&direction=INPUT&depth=10"
```
UI : `http://ubuntu3:21000` → Search by type `medallion_dataset` → une entité → onglet **Lineage**.

### 9.4 Arrêt (inverse)
```bash
python3 $ATLAS_HOME/bin/atlas_stop.py
/home/hadoop/solr/bin/solr stop -all
/home/hadoop/hbase/bin/stop-hbase.sh
```

> Process Atlas figé qui prend le terminal en otage (tout ce qu'on tape disparaît) :
> `pkill -9 -f atlas_lineage` ou `pkill -9 -f atlas`, depuis un terminal **du même user**.
