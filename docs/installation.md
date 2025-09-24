# Guide d'installation et de déploiement

Ce document fournit les instructions détaillées pour l'installation et le déploiement de l'architecture Big Data pour le traitement des données météorologiques.

## Prérequis

Avant de commencer, assurez-vous d'avoir les éléments suivants installés :

- Docker Desktop (Windows/Mac) ou Docker Engine (Linux)
- Docker Compose
- Git
- Minimum 8 Go de RAM disponible
- 20 Go d'espace disque

## Étapes d'installation

### 1. Cloner le dépôt Git

```bash
git clone https://github.com/lara58/projet_architecture_big_data.git
cd projet_architecture_big_data
```

### 2. Configuration de l'environnement

#### Créer les répertoires nécessaires

```bash
mkdir -p data/raw data/processed data/output
mkdir -p logs
```

### 3. Démarrage des services

Vous pouvez démarrer tous les services en utilisant le script d'initialisation :

```bash
# Windows
.\init-environment.bat

# Linux/Mac
bash init-environment.sh
```

Ou démarrer manuellement les services :

```bash
# Infrastructure principale
cd docker
docker-compose up -d

# Attendre que les services démarrent
timeout /t 30

# Initialiser les topics Kafka
cd ../scripts
.\init-kafka.bat

# Démarrer Airflow
cd ../docker
docker-compose -f docker-compose-airflow.yml up -d
```

### 4. Vérification de l'installation

#### Vérifier que tous les services sont opérationnels

```bash
cd scripts
.\test-connectivity.bat
```

### 5. Accès aux interfaces Web

Une fois l'installation terminée, vous pouvez accéder aux interfaces suivantes :

- **Spark Master UI** : http://localhost:8080
- **Spark Worker UI** : http://localhost:8081
- **Airflow** : http://localhost:8080 (utilisateur: admin, mot de passe: admin)

## Structure des dossiers

```
projet_archi_big_data/
├── docker/
│   ├── docker-compose.yml          # Services principaux (Kafka, Spark)
│   ├── docker-compose-airflow.yml  # Services Airflow
│   └── data/                       # Volume partagé
├── scripts/
│   ├── init-kafka.bat              # Initialisation des topics Kafka
│   └── test-connectivity.bat       # Tests de connectivité
├── dags/
│   ├── weather_ingestion_dag.py    # DAG d'ingestion des données
│   ├── weather_batch_dag.py        # DAG de traitement batch
│   └── weather_alerts_dag.py       # DAG de détection d'alertes
├── src/
│   ├── spark/                      # Scripts Spark
│   │   ├── daily_weather_processor.py
│   │   └── monthly_weather_aggregator.py
├── data/
│   ├── raw/                        # Données brutes
│   ├── processed/                  # Données traitées
│   └── output/                     # Résultats finaux
└── docs/
    └── architecture.md             # Documentation de l'architecture
```

## Troubleshooting

### Problèmes courants

#### Les conteneurs ne démarrent pas

```bash
# Vérifier les logs
docker-compose logs

# Redémarrer les conteneurs
docker-compose down
docker-compose up -d
```

#### Kafka n'est pas accessible

```bash
# Vérifier que Kafka est en cours d'exécution
docker ps | grep kafka

# Vérifier les topics Kafka
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
```

#### Airflow ne démarre pas

```bash
# Vérifier les logs Airflow
docker-compose -f docker-compose-airflow.yml logs airflow

# Réinitialiser la base de données Airflow
docker-compose -f docker-compose-airflow.yml down
docker volume rm postgres_data
docker-compose -f docker-compose-airflow.yml up -d
```

#### Spark ne traite pas les données

```bash
# Vérifier la connectivité entre Spark Master et Worker
docker logs spark-worker | findstr -i master

# Vérifier l'accès au volume partagé
docker exec spark-master ls -la /data
```

## Support

Pour toute assistance supplémentaire, contactez :
- Narimane Tahir (Architecture Infrastructure)
- Lidia Khelifi (Pipeline de données)
- Mouna KHOLASSI (Développement Backend)