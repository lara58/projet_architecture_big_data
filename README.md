# Projet d'Architecture Big Data - Analyse des Tremblements de Terre

## Description du Projet

Ce projet consiste à développer une architecture Big Data pour analyser les données des tremblements de terre en temps réel en utilisant l'API USGS (United States Geological Survey). L'objectif est de créer un pipeline de données complet pour ingérer, traiter et analyser les données sismiques mondiales.

## API Utilisée

**USGS Earthquake Catalog API**
- URL : https://earthquake.usgs.gov/fdsnws/event/1/
- Documentation : https://earthquake.usgs.gov/fdsnws/event/1/
- Format de données : JSON, CSV, XML
- Fréquence de mise à jour : Temps réel

### Données Disponibles dans la réponse USGS

**Magnitude des tremblements de terre**
- Champ : `mag`
- Exemple : 6.4

**Localisation géographique (latitude, longitude, profondeur)**
- Champs : `geometry.coordinates` → [longitude, latitude, profondeur] (en km)

**Date et heure de l'événement**
- Champ : `time` (timestamp UNIX, à convertir en date "humaine")
- Champ : `updated` (dernière mise à jour de l'info)

**Type de tremblement de terre**
- Champ : `type` (dans properties, généralement "earthquake")

**Région / Pays affecté**
- Champ : `place` (description du lieu : ville, région, pays)

**Intensité ressentie**
- Champs :
  - `felt` (nombre de personnes ayant ressenti le séisme)
  - `cdi` (Community Determined Intensity, intensité ressentie)
  - `mmi` (Modified Mercalli Intensity, intensité instrumentale)

**Alertes tsunami**
- Champ : `tsunami` (0 = pas d'alerte, 1 = alerte tsunami)

## Architecture du Système

### Composants Principaux

1. **Ingestion de Données**
   - Apache Kafka pour le streaming en temps réel
   - Apache NiFi pour l'orchestration des flux de données
   - Connecteurs API USGS

2. **Stockage**
   - Apache Hadoop (HDFS) pour le stockage distribué
   - Apache HBase pour les données en temps réel
   - Apache Parquet pour l'optimisation des requêtes

3. **Traitement**
   - Apache Spark pour le traitement batch et streaming
   - Apache Storm pour le traitement en temps réel
   - Python/Scala pour les algorithmes d'analyse

4. **Analyse et Visualisation**
   - Apache Superset pour les dashboards
   - Grafana pour le monitoring en temps réel
   - Jupyter Notebooks pour l'analyse exploratoire

5. **Orchestration**
   - Apache Airflow pour la planification des tâches
   - Docker pour la containerisation
   - Kubernetes pour l'orchestration des conteneurs

## Cas d'Usage

### Analyses Prévues

1. **Détection de Patterns Sismiques**
   - Identification des zones à risque élevé
   - Prédiction de répliques
   - Analyse des séquences sismiques

2. **Alertes en Temps Réel**
   - Système d'alerte pour les tremblements de terre majeurs (M > 6.0)
   - Notifications géolocalisées
   - Intégration avec les systèmes d'urgence

3. **Analyses Statistiques**
   - Tendances sismiques par région
   - Corrélation magnitude/profondeur
   - Impact sur les populations

4. **Visualisations Interactives**
   - Cartes de chaleur en temps réel
   - Historique des événements sismiques
   - Dashboards de monitoring

## Exigences du Projet

### Frontend - Interface Utilisateur
- **Bouton de téléchargement** : Permettre aux utilisateurs de télécharger les données sismiques au format CSV/JSON
- **Affichage tabulaire** : Présentation des données sous forme de tableau interactif
- **Fonctionnalités d'agrégation** : 
  - Calcul de statistiques (moyenne, min, max des magnitudes)
  - Groupement par région, type de séisme, période
- **Tri dynamique** : Possibilité de trier selon toutes les colonnes (magnitude, date, lieu, profondeur)
- **Filtres** : Filtrage par magnitude, période, région

### Tests de Connectivité des Services
- **Test Spark** : Vérification de la connexion et fonctionnement du cluster Spark
- **Test HDFS** : Validation de l'accès au système de fichiers distribué Hadoop
- **Test Kafka** : Contrôle du streaming et des topics
- **Test API USGS** : Vérification de la disponibilité de l'API externe
- **Monitoring automatique** : Scripts de health check pour tous les services

### Documentation Technique Complète
- **Documentation d'architecture** : Diagrammes et explications détaillées
- **Guide d'installation** : Instructions pas à pas pour chaque composant
- **Documentation API** : Endpoints, paramètres, exemples de réponses
- **Guide de développement** : Standards de code, bonnes pratiques
- **Documentation des tests** : Procédures de test et validation
- **Troubleshooting** : Guide de résolution des problèmes courants

## Technologies Utilisées

### Big Data Stack
- **Apache Hadoop** - Stockage distribué
- **Apache Spark** - Traitement de données
- **Apache Kafka** - Streaming de données
- **Apache HBase** - Base de données NoSQL
- **Apache Airflow** - Orchestration des workflows

### Langages de Programmation
- **Python** - Scripts d'analyse et ML
- **Scala** - Applications Spark
- **SQL** - Requêtes de données
- **JavaScript** - Interface utilisateur

### Outils de Déploiement
- **Docker** - Containerisation
- **Kubernetes** - Orchestration
- **Terraform** - Infrastructure as Code

## Prérequis

### Logiciels Requis
- Java 8+
- Python 3.8+
- Docker & Docker Compose
- Apache Spark 3.x
- Apache Kafka 2.8+

### Ressources Système
- RAM : Minimum 16 GB (recommandé 32 GB)
- CPU : Minimum 8 cœurs
- Stockage : 500 GB disponible
- Réseau : Connexion internet stable pour l'API

## Installation et Configuration

### 1. Cloner le Repository
```bash
git clone <repository-url>
cd projet_archi_big_data
```

### 2. Configuration de l'Environnement
```bash
# Créer l'environnement virtuel Python
python -m venv venv
source venv/bin/activate  # Linux/Mac
venv\Scripts\activate     # Windows

# Installer les dépendances
pip install -r requirements.txt
```

### 3. Configuration Docker
```bash
# Lancer l'infrastructure
docker-compose up -d

# Vérifier les services
docker-compose ps
```

### 4. Configuration de l'API USGS
```bash
# Variables d'environnement
export USGS_API_URL="https://earthquake.usgs.gov/fdsnws/event/1/"
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export SPARK_MASTER_URL="spark://localhost:7077"
```

## Structure du Projet

```
projet_archi_big_data/
├── data/
│   ├── raw/                 # Données brutes de l'API
│   ├── processed/           # Données traitées
│   └── output/              # Résultats des analyses
├── src/
│   ├── ingestion/           # Scripts d'ingestion Kafka
│   ├── processing/          # Jobs Spark
│   ├── analysis/            # Scripts d'analyse
│   ├── visualization/       # Dashboards et graphiques
│   └── frontend/            # Interface utilisateur web
├── config/
│   ├── kafka/               # Configuration Kafka
│   ├── spark/               # Configuration Spark
│   └── airflow/             # DAGs Airflow
├── docker/
│   ├── docker-compose.yml   # Services Docker
│   └── Dockerfile           # Image personnalisée
├── tests/
│   ├── test_spark_connectivity.py    # Tests Spark
│   ├── test_hdfs_connectivity.py     # Tests HDFS
│   ├── test_usgs_api.py              # Tests API USGS
│   └── integration/                  # Tests d'intégration
├── docs/
│   ├── architecture/        # Diagrammes et documentation technique
│   ├── installation/        # Guides d'installation détaillés
│   ├── api/                 # Documentation des APIs
│   ├── frontend/            # Documentation interface utilisateur
│   └── troubleshooting/     # Guide de résolution des problèmes
├── notebooks/               # Jupyter Notebooks
└── requirements.txt         # Dépendances Python
```

## Utilisation

### 1. Démarrage du Pipeline de Données
```bash
# Démarrer l'ingestion des données
python src/ingestion/usgs_kafka_producer.py

# Lancer le traitement Spark
spark-submit src/processing/earthquake_processor.py
```

### 2. Accès aux Interfaces
- **Spark UI** : http://localhost:4040
- **Kafka Manager** : http://localhost:9000
- **Airflow** : http://localhost:8080
- **Superset** : http://localhost:8088

### 3. Requêtes d'Exemple
```python
# Tremblements de terre des dernières 24h
from src.analysis.earthquake_analysis import get_recent_earthquakes
recent_eq = get_recent_earthquakes(hours=24, min_magnitude=4.0)

# Analyse par région
regional_stats = analyze_by_region('Pacific Ring of Fire')
```

### 4. Tests de Connectivité des Services

#### Test Spark
```bash
# Vérifier la connexion Spark
python tests/test_spark_connectivity.py

# Test de job Spark simple
spark-submit tests/spark_health_check.py
```

#### Test HDFS
```bash
# Test d'accès HDFS
hadoop fs -ls /

# Test d'écriture/lecture
python tests/test_hdfs_connectivity.py
```

#### Test de l'API USGS
```bash
# Test de connectivité API
python tests/test_usgs_api.py

# Validation du format des données
python tests/validate_api_response.py
```

### 5. Frontend - Interface Web

#### Accès à l'interface utilisateur
- **URL Frontend** : http://localhost:3000
- **Fonctionnalités disponibles** :
  - Téléchargement des données (bouton Export CSV/JSON)
  - Tableau interactif avec tri par colonnes
  - Filtres par magnitude, date, région
  - Agrégations statistiques en temps réel

## Monitoring et Performance

### Métriques Surveillées
- **Throughput** : Messages traités par seconde
- **Latence** : Délai de traitement des données
- **Disponibilité** : Uptime des services
- **Précision** : Qualité des prédictions

### Alertes Configurées
- Pic d'activité sismique (M > 7.0)
- Latence élevée (> 5 secondes)
- Échec des jobs Spark
- Espace disque faible (< 10%)

## Sécurité et Conformité

### Mesures de Sécurité
- Chiffrement des données en transit (TLS)
- Authentification des services
- Logs d'audit détaillés
- Isolation des environnements

### Conformité GDPR
- Anonymisation des données personnelles
- Politique de rétention des données
- Droit à l'oubli respecté

## Documentation Technique

### Documentation Requise (Exigences Professeur)

#### 1. Documentation d'Architecture
- [Diagramme d'architecture globale](docs/architecture/system_architecture.md)
- [Flux de données détaillé](docs/architecture/data_flow.md)
- [Schéma des services](docs/architecture/services_diagram.md)

#### 2. Guides d'Installation et Configuration
- [Guide d'installation complète](docs/installation/complete_setup.md)
- [Configuration des services Big Data](docs/installation/bigdata_services.md)
- [Déploiement Docker](docs/installation/docker_deployment.md)

#### 3. Documentation des Tests
- [Procédures de test Spark](docs/testing/spark_tests.md)
- [Procédures de test HDFS](docs/testing/hdfs_tests.md)
- [Tests d'intégration API](docs/testing/api_integration.md)
- [Validation des données](docs/testing/data_validation.md)

#### 4. Documentation Frontend
- [Guide utilisateur interface web](docs/frontend/user_guide.md)
- [Fonctionnalités de téléchargement](docs/frontend/download_features.md)
- [Système de tri et filtrage](docs/frontend/sorting_filtering.md)
- [Agrégations disponibles](docs/frontend/aggregations.md)

### APIs Documentées
- [API d'Ingestion](docs/api/ingestion.md)
- [API de Traitement](docs/api/processing.md)
- [API de Visualisation](docs/api/visualization.md)
- [API Frontend](docs/api/frontend.md)

### Guides de Développement
- [Guide de Contribution](docs/CONTRIBUTING.md)
- [Standards de Code](docs/CODE_STANDARDS.md)
- [Guide de Déploiement](docs/DEPLOYMENT.md)

## Livrables du Projet

### Composants Techniques
1. **Pipeline de données Big Data fonctionnel**
   - Ingestion temps réel via Kafka
   - Traitement distribué avec Spark
   - Stockage HDFS opérationnel

2. **Interface utilisateur web complète**
   - Bouton de téléchargement des données (CSV/JSON)
   - Tableau interactif avec données sismiques
   - Fonctionnalités de tri par toutes les colonnes
   - Système d'agrégation (moyenne, min, max, comptage)
   - Filtres par magnitude, date, région

3. **Tests de connectivité validés**
   - Scripts de test pour Spark
   - Validation de l'accès HDFS
   - Tests d'intégration API USGS
   - Monitoring automatique des services

### Documentation Complète
- Architecture système détaillée
- Guide d'installation pas à pas
- Documentation de chaque étape du développement
- Procédures de test et validation
- Guide utilisateur interface web
- Documentation troubleshooting

## Équipe du Projet

- **Chef de Projet** : Tahir Narimane
- **Architecte Big Data** : Lidia Khelifi
- **Développeur Backend** : Mouna KHOLASSI


## Licence

Ce projet est sous licence MIT. Voir le fichier [LICENSE](LICENSE) pour plus de détails.


## Changelog

### Version 1.0.0
- Pipeline d'ingestion USGS complet
- Traitement Spark en temps réel
- Dashboards de visualisation
- Système d'alertes automatisé

---

**Note** : Ce projet est développé dans le cadre d'un projet académique sur l'architecture Big Data. Les données utilisées proviennent de sources publiques (USGS) et sont utilisées uniquement à des fins éducatives et de recherche.