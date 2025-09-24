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
   - Connecteurs API Open-Meteo

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

1. **Analyses Climatiques et Météorologiques**
   - Identification des patterns météorologiques
   - Prédiction de conditions extrêmes
   - Analyse des tendances climatiques par région

2. **Alertes Météorologiques en Temps Réel**
   - Système d'alerte pour conditions météo extrêmes
   - Notifications géolocalisées (tempêtes, canicules, gel)
   - Intégration avec les systèmes d'alerte météorologique

3. **Analyses Statistiques**
   - Tendances de température par région et saison
   - Corrélations pression/précipitations
   - Analyse des vents dominants
   - Comparaison avec données historiques

4. **Visualisations Interactives**
   - Cartes météorologiques en temps réel
   - Historique des conditions météo
   - Dashboards de prévisions
   - Graphiques de tendances climatiques

## Exigences du Projet

### Frontend - Interface Utilisateur
- **Bouton de téléchargement** : Permettre aux utilisateurs de télécharger les données météorologiques au format CSV/JSON
- **Affichage tabulaire** : Présentation des données sous forme de tableau interactif
- **Fonctionnalités d'agrégation** : 
  - Calcul de statistiques (moyenne, min, max des températures, précipitations)
  - Groupement par région, saison, conditions météorologiques
- **Tri dynamique** : Possibilité de trier selon toutes les colonnes (température, précipitations, vent, pression)
- **Filtres** : Filtrage par température, période, région, conditions météo

### Tests de Connectivité des Services
- **Test Spark** : Vérification de la connexion et fonctionnement du cluster Spark
- **Test HDFS** : Validation de l'accès au système de fichiers distribué Hadoop
- **Test Kafka** : Contrôle du streaming et des topics
- **Test API Open-Meteo** : Vérification de la disponibilité de l'API météorologique
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

### 4. Configuration de l'API Open-Meteo
```bash
# Variables d'environnement
export OPENMETEO_API_URL="https://api.open-meteo.com/v1/forecast"
export KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
export SPARK_MASTER_URL="spark://localhost:7077"
```

## Structure du Projet

```
projet_archi_big_data/
├── data/
│   ├── raw/                 # Données brutes de l'API
│   ├── processed/           # Données traitées
│   └── output/              # Résultats des analyses météorologiques
├── src/
│   ├── ingestion/           # Scripts d'ingestion Kafka
│   ├── processing/          # Jobs Spark
│   ├── analysis/            # Scripts d'analyse météorologique
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
│   ├── test_openmeteo_api.py         # Tests API Open-Meteo
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
python src/ingestion/openmeteo_kafka_producer.py

# Lancer le traitement Spark
spark-submit src/processing/weather_processor.py
```

### 2. Accès aux Interfaces
- **Spark UI** : http://localhost:4040
- **Kafka Manager** : http://localhost:9000
- **Airflow** : http://localhost:8080
- **Superset** : http://localhost:8088

### 3. Requêtes d'Exemple
```python
# Données météo des dernières 24h
from src.analysis.weather_analysis import get_recent_weather
recent_weather = get_recent_weather(hours=24, locations=['Paris', 'London'])

# Analyse par région climatique
regional_stats = analyze_by_climate_region('Western Europe')
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

#### Test d'API Open-Meteo
```bash
# Test de connectivité API
python tests/test_openmeteo_api.py

# Validation du format des données
python tests/validate_weather_api_response.py
```

### 5. Frontend - Interface Web

#### Accès à l'interface utilisateur
- **URL Frontend** : http://localhost:3000
- **Fonctionnalités disponibles** :
  - Téléchargement des données météorologiques (bouton Export CSV/JSON)
  - Tableau interactif avec tri par colonnes (température, précipitations, vent)
  - Filtres par température, conditions météo, région, période
  - Agrégations météorologiques en temps réel

## Monitoring et Performance

### Métriques Surveillées
- **Throughput** : Messages traités par seconde
- **Latence** : Délai de traitement des données
- **Disponibilité** : Uptime des services
- **Précision** : Qualité des prédictions

### Alertes Configurées
- Conditions météorologiques extrêmes (températures > 40°C ou < -20°C)
- Précipitations intenses (> 50mm/h)
- Vents forts (> 100 km/h)
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
- [Tests d'intégration API météorologique](docs/testing/weather_api_integration.md)
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
   - Bouton de téléchargement des données météorologiques (CSV/JSON)
   - Tableau interactif avec données météorologiques
   - Fonctionnalités de tri par toutes les colonnes (température, précipitations, vent, pression)
   - Système d'agrégation (moyenne, min, max des températures et précipitations)
   - Filtres par conditions météo, température, région, période

3. **Tests de connectivité validés**
   - Scripts de test pour Spark
   - Validation de l'accès HDFS
   - Tests d'intégration API Open-Meteo
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
- Pipeline d'ingestion Open-Meteo complet
- Traitement Spark en temps réel
- Dashboards météorologiques de visualisation
- Système d'alertes météorologiques automatisé

---

**Note** : Ce projet est développé dans le cadre d'un projet académique sur l'architecture Big Data. Les données utilisées proviennent de sources publiques (Open-Meteo) et sont utilisées uniquement à des fins éducatives et de recherche.

API USGS → Kafka → Spark → HDFS
                     ↓
               Frontend Web (Dashboards)