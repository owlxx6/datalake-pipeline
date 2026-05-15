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

### 3.1 Toutes les tables d'un domaine

```bash
spark-submit \
  --master spark://ubuntu3:7077 \
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
| `SafeModeException: Name node is in safe mode` | HDFS démarre en safe mode | `hdfs dfsadmin -safemode leave` |
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
