pipeline {
  agent any

  // Stages : checkout -> compile -> validation config YAML -> analyse SonarQube -> déploiement.
  // Les contrôles compile/validate s'exécutent SUR la VM datalake (via SSH), là où Python +
  // pydantic/pyyaml/pyspark sont disponibles ; le conteneur Jenkins ne sert qu'à orchestrer
  // et à lancer sonar-scanner.
  environment {
    SONAR_HOST_URL   = 'http://sonarqube-server:9000'
    DATALAKE_HOST    = '172.17.0.1'          // hôte Docker = la VM
    DATALAKE_USER    = 'hadoop'
    SSH_KEY          = '/var/jenkins_home/.ssh/id_ed25519'
    PROJECT_DIR      = '/media/sf_datalake-pipeline'   // repo visible sur la VM (dossier partagé)
    AIRFLOW_DAGS_DIR = '/media/sf_datalake-pipeline/dags'
    SSH_OPTS         = '-o StrictHostKeyChecking=no'
  }

  stages {
    stage('Checkout') {
      steps { checkout scm }
    }

    stage('Compile (py_compile)') {
      steps {
        sh '''
          ssh ${SSH_OPTS} -i ${SSH_KEY} ${DATALAKE_USER}@${DATALAKE_HOST} \
            "cd ${PROJECT_DIR} && python3 -m py_compile \
               jobs/bronze/*.py jobs/gold/*.py dags/*.py scripts/*.py && echo 'compile OK'"
        '''
      }
    }

    stage('Validation config (YAML déclaratif)') {
      steps {
        sh '''
          ssh ${SSH_OPTS} -i ${SSH_KEY} ${DATALAKE_USER}@${DATALAKE_HOST} \
            "cd ${PROJECT_DIR} && python3 -c \\"import yaml; \
               yaml.safe_load(open('configs/bronze/credit_garantie.yaml')); \
               yaml.safe_load(open('configs/gold/credit_garantie.yaml')); \
               print('config YAML valide')\\""
        '''
      }
    }

    stage('SonarQube Analysis') {
      steps {
        withCredentials([string(credentialsId: 'SONAR_TOKEN', variable: 'SONAR_TOKEN')]) {
          sh '''
            sonar-scanner \
              -Dsonar.projectKey=datalake-pipeline \
              -Dsonar.sources=jobs,dags,scripts \
              -Dsonar.host.url=${SONAR_HOST_URL} \
              -Dsonar.login=${SONAR_TOKEN}
          '''
        }
      }
    }

    stage('Deploy DAGs Airflow') {
      steps {
        sh '''
          ssh ${SSH_OPTS} -i ${SSH_KEY} ${DATALAKE_USER}@${DATALAKE_HOST} "mkdir -p ${AIRFLOW_DAGS_DIR}"
          scp ${SSH_OPTS} -i ${SSH_KEY} dags/dag_credit_garantie.py ${DATALAKE_USER}@${DATALAKE_HOST}:${AIRFLOW_DAGS_DIR}/
        '''
      }
    }
  }

  post {
    success { echo 'Pipeline OK — code compilé, config validée, analyse Sonar, DAG déployé.' }
    failure { echo 'Pipeline en échec — voir les logs de l\'étape fautive.' }
  }
}
