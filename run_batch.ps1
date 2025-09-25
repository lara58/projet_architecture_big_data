<#
Script d'aide pour exécuter le job batch Spark depuis l'hôte Windows (PowerShell).
Usage :
  .\run_batch.ps1            # submit default job
  .\run_batch.ps1 -Rebuild   # rebuild weather-batch image before submit
#>

param(
    [switch]$Rebuild
)

if ($Rebuild) {
    Write-Host "Rebuild des images (context: src/batch)..."
    docker-compose build weather-batch
}

Write-Host "Lancement du job Spark dans le conteneur 'weather-batch'..."

# Copie du script dans le conteneur (optionnel si le volume est monté)
docker cp .\src\batch\process_weather_batch.py weather-batch:/app/batch/process_weather_batch.py

docker exec -it weather-batch spark-submit --master spark://spark-master:7077 /app/batch/process_weather_batch.py
