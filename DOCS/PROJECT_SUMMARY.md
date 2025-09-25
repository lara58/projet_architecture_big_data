# Documentation synthétique du projet

Ce document présente de façon concise ce que fait le projet, sa structure, et des commandes utiles pour démarrer, tester et dépanner l'environnement local (PowerShell / Docker Compose).

## 1) But du projet

Le projet met en place un pipeline Big Data pour collecter des données météo en continu (via l'API Open‑Meteo), les diffuser via Kafka, les consommer et stocker dans PostgreSQL, et exécuter des traitements batch (PySpark) pour historiser et analyser les données. L'infrastructure locale est orchestrée par Docker Compose et comprend Kafka, Spark, HDFS et PostgreSQL.

## 2) Composants principaux

- Zookeeper + Kafka : infrastructure de messaging
- Spark (master + worker) : traitement batch/submit de jobs PySpark
- HDFS (NameNode / DataNode) : stockage distribué (utilisé pour les jobs batch)
- PostgreSQL + pgAdmin : stockage relationnel et interface
- weather-producer : script Python qui collecte l'API Open‑Meteo et publie sur le topic Kafka `weather`
- weather-consumer : consumer Kafka qui insère les messages dans la table `weather_data` de PostgreSQL
- weather-batch : image/container prévue pour exécuter des jobs PySpark (ex : `weather_batch.py`)

## 3) Arborescence importante (emplacement relatif au repo)

- `docker-compose.yml` : définition des services locaux
- `README.md` / `DOCS/PROJECT_DOC.md` : documentation et guides
- `src/ingestion/` : producteur, consumer, Dockerfile, requirements
  - `producer.py` : collecte et envoi vers Kafka
  - `consumer_postgres.py` : consommation Kafka -> PostgreSQL
- `src/batch/` : jobs batch PySpark et Dockerfile
  - `weather_batch.py` : example de job qui récupère de l'historique et écrit dans HDFS
- `data/` : répertoires de données (raw / processed) (présence selon usage)

## 4) Comment démarrer localement (PowerShell)

1) Builder et démarrer tous les services (mode détaché) :

```powershell
docker-compose -f "docker-compose.yml" up --build -d
```

2) Vérifier l'état des conteneurs :

```powershell
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

3) Arrêter proprement et supprimer :

```powershell
docker-compose -f "docker-compose.yml" down
```

## 5) Commandes utiles (conteneurs + HDFS + Spark)

- Lancer un job PySpark depuis le conteneur `weather-batch` :

```powershell
docker exec -it weather-batch spark-submit --master spark://spark-master:7077 /app/batch/weather_batch.py
```

- Ouvrir un shell Python dans le conteneur producteur (debug) :

```powershell
docker exec -it weather-producer python /app/producer.py
```

- Logs d'un conteneur (suivre en temps réel) :

```powershell
docker logs -f weather-producer
```

- Opérations HDFS (depuis le conteneur NameNode) :

```powershell
docker exec -it hdfs-namenode hdfs dfs -ls /
docker exec -it hdfs-namenode hdfs dfs -mkdir -p /user/spark
docker exec -it hdfs-namenode hdfs dfs -chown spark:spark /user/spark
docker exec -it hdfs-namenode hdfs dfs -chmod 755 /user/spark
```

- Copier un fichier local vers un conteneur :

```powershell
docker cp .\src\batch\weather_batch.py weather-batch:/app/batch/weather_batch.py
```

## 6) Endpoints et interfaces web

- HDFS NameNode UI : http://localhost:9870
- Spark Master UI : http://localhost:8080
- pgAdmin : http://localhost:5050

## 7) Points d'attention / dépannage rapide

- Kafka peut démarrer lentement : si `NoBrokersAvailable` apparait côté producteur, vérifiez l'ordre de démarrage et consultez les logs de Kafka et Zookeeper. Le producteur dans `src/ingestion/producer.py` inclut déjà des retries lors de la connexion.
- Préférez `spark-submit` (dans le conteneur Spark) pour exécuter les scripts PySpark afin d'éviter des ImportError (py4j, versions).
- Si Spark lève une erreur de permission HDFS (ex : AccessControlException), créer `/user/spark` et chown sur le NameNode comme indiqué ci‑dessus.
- L'API Open‑Meteo impose des quotas : implémenter un backoff (déjà présent dans `src/batch/weather_batch.py` et partiellement dans `src/ingestion/producer.py`). Sur 429, respecter `Retry-After` et augmenter le délai.

## 8) Tests rapides

- Vérifier que PostgreSQL est joignable :

```powershell
docker exec -it postgres psql -U admin -d weatherdb -c "\dt"
```

- Vérifier que le topic Kafka existe (depuis le broker ou un container client Kafka) :

```powershell
# Exemple avec outils Kafka (si disponibles dans l'image) : kafka-topics --bootstrap-server kafka:29092 --list
```

## 9) Recommandations / améliorations suggérées

- Ajouter un script d'automatisation `run_all.ps1` pour build → attendre → lancer producers/consumers et lancer les jobs batch de test.
- Ajouter une table de métadonnées (Postgres) pour tracer l'état des collectes historiques et éviter les doublons.
- Renforcer le mécanisme de retry/backoff côté producteur et stockage temporaire des réponses brutes (raw) pour relancer le traitement sans re-requester l'API.

## 10) Assomptions et limites

- J'ai basé la doc sur la configuration observée dans `docker-compose.yml` et les scripts présents sous `src/`. Si d'autres services (Airflow, Superset) sont ajoutés ailleurs, ils ne sont pas couverts ici.
- Les commandes PowerShell listées supposent que Docker Desktop est installé et fonctionne sur Windows.

---

Si tu veux que j'ajoute directement :
- un `run_all.ps1` automatisé,
- ou que j'intègre ce fichier dans le `README.md`,
n'hésite pas à me dire lequel et je l'implémente.
