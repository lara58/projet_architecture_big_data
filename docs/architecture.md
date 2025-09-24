# Architecture du Système Big Data - Open-Meteo

Ce document décrit l'architecture complète du système Big Data pour le traitement des données météorologiques provenant de l'API Open-Meteo.

## Vue d'ensemble de l'Architecture

L'architecture est basée sur les principes d'une architecture Lambda, combinant:
- **Couche Speed** : Pour le traitement temps réel des données météorologiques
- **Couche Batch** : Pour le traitement de gros volumes de données historiques
- **Couche Serving** : Pour l'exposition des résultats via des API et des interfaces

![Architecture Lambda](https://raw.githubusercontent.com/lara58/projet_architecture_big_data/main/docs/images/architecture_lambda.png)

## Composants clés

### 1. Ingestion des données

**API Open-Meteo → Kafka**

- **Source** : API Open-Meteo (https://api.open-meteo.com/v1/forecast)
- **Fréquence d'ingestion** : Horaire
- **Méthode** : Appels REST via scripts Python
- **Format de données** : JSON
- **Destination** : Topics Kafka
  - `weather-data` : Données brutes de l'API
  - `processed-weather` : Données après traitement
  - `weather-alerts` : Alertes détectées

### 2. Traitement des données

**Couche Batch**
- **Technologie** : Apache Spark
- **Type de traitement** : Jobs quotidiens et mensuels
- **Stockage des résultats** : Volumes partagés (remplaçant HDFS)
- **Formats** : Parquet pour les analyses, CSV pour les exports

**Couche Speed**
- **Technologie** : Kafka Streams + Spark Streaming
- **Applications** : Détection d'alertes en temps réel
- **Latence** : < 5 secondes

### 3. Orchestration

**Apache Airflow**
- **DAGs** :
  - `weather_ingestion_dag.py` : Ingestion des données météo
  - `weather_batch_dag.py` : Traitement batch quotidien et mensuel
  - `weather_alerts_dag.py` : Surveillance des alertes météo
- **Fréquence** : Horaire, Quotidienne, Mensuelle

### 4. Stockage

**Volumes Docker**
- **Remplace HDFS** pour simplifier l'architecture
- **Structure** :
  - `/data/raw` : Données brutes de l'API
  - `/data/processed` : Résultats du traitement Spark
  - `/data/output` : Résultats finaux prêts pour la visualisation

### 5. Visualisation

**Dashboard avec Filtrage de Données**
- **Interface utilisateur** : Dashboard Web
- **Fonctionnalités** :
  - Téléchargement des données (CSV/JSON)
  - Affichage tabulaire
  - Filtres dynamiques
  - Graphiques interactifs

## Flux de données

1. **Ingestion** : 
   - Les données sont extraites de l'API Open-Meteo toutes les heures
   - Les données sont stockées en format brut dans `/data/raw`
   - Les données sont publiées dans le topic Kafka `weather-data`

2. **Traitement temps réel** :
   - Kafka Streams traite les données en temps réel
   - Des alertes sont générées pour les conditions météo extrêmes
   - Les alertes sont publiées dans `weather-alerts`

3. **Traitement batch** :
   - Airflow orchestre des jobs Spark quotidiens
   - Les données sont agrégées par ville, région
   - Les résultats sont stockés en format Parquet dans `/data/processed`
   - Des statistiques mensuelles sont générées

4. **Exposition des données** :
   - Les résultats sont exposés via une API REST
   - Des tableaux de bord interactifs affichent les données
   - Les utilisateurs peuvent filtrer, trier et télécharger les données

## Déploiement

L'ensemble du système est déployé avec Docker et Docker Compose :

- **Infrastructure principale** : `docker-compose.yml`
  - Kafka + Zookeeper
  - Spark Master + Worker
  - Volumes partagés

- **Orchestration** : `docker-compose-airflow.yml`
  - Airflow Webserver + Scheduler
  - PostgreSQL (base de données Airflow)

## Connectivité entre services

- Kafka est accessible sur le port 9092
- Spark Master UI est accessible sur le port 8080
- Spark Worker UI est accessible sur le port 8081
- Airflow est accessible sur le port 8080

## Tests de validation

Pour valider l'architecture, exécutez :
```bash
scripts/test-connectivity.bat
```

Ce script teste la connectivité entre tous les composants.

## Schéma technique

```
┌─────────────┐     ┌────────┐     ┌────────────┐     ┌─────────────┐
│ Open-Meteo  │────▶│ Kafka  │────▶│ Spark      │────▶│ Volume      │
│ API         │     │        │     │ Processing │     │ Storage     │
└─────────────┘     └────────┘     └────────────┘     └─────────────┘
                         │                │                 │
                         ▼                ▼                 ▼
                    ┌────────────────────────────────────────────┐
                    │              Airflow                       │
                    │        (Orchestration des tâches)          │
                    └────────────────────────────────────────────┘
                                      │
                                      ▼
                               ┌─────────────┐
                               │ Dashboard   │
                               │ Web         │
                               └─────────────┘
```