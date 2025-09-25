#!/bin/bash
# Script pour copier les fichiers de batch vers HDFS
set -e

echo "Script de copie vers HDFS"

# Vérifier qu'un fichier est passé en paramètre
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <fichier_local>"
    exit 1
fi

LOCAL_FILE="$1"
FILENAME=$(basename "$LOCAL_FILE")
HDFS_PATH="/weather-data/$FILENAME"

echo "Fichier local: $LOCAL_FILE"
echo "Destination HDFS: $HDFS_PATH"

# Copier le fichier du conteneur weather-batch vers l'hôte
echo "Copie depuis le conteneur..."
docker cp weather-batch:"$LOCAL_FILE" ./temp_weather_file.json

# Copier vers le conteneur HDFS
echo "Copie vers le conteneur HDFS..."
docker cp ./temp_weather_file.json hdfs-namenode:/tmp/temp_weather_file.json

# Copier vers HDFS
echo "Copie vers HDFS..."
docker exec -it hdfs-namenode hdfs dfs -put -f /tmp/temp_weather_file.json "$HDFS_PATH"

# Nettoyer les fichiers temporaires
echo "Nettoyage..."
rm -f ./temp_weather_file.json
docker exec -it hdfs-namenode rm -f /tmp/temp_weather_file.json

# Vérifier le résultat
echo "Vérification..."
docker exec -it hdfs-namenode hdfs dfs -ls "$HDFS_PATH"

echo "Copie terminée avec succès !"
echo "Fichier disponible dans HDFS: $HDFS_PATH"