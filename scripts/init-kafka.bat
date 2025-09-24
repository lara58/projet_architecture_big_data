@echo off
REM Script d'initialisation Kafka pour Windows
REM Cree les topics necessaires pour le projet meteorologique

echo Initialisation des topics Kafka pour le projet meteorologique...

REM Attendre que Kafka soit pret
echo Attente de la disponibilite de Kafka...
timeout /t 30 /nobreak > nul

REM Creer le topic principal pour les donnees meteorologiques
echo Creation du topic 'weather-data'...
docker exec kafka kafka-topics --create ^
  --bootstrap-server localhost:9092 ^
  --topic weather-data ^
  --partitions 3 ^
  --replication-factor 1 ^
  --config retention.ms=604800000 ^
  --if-not-exists

REM Creer le topic pour les donnees traitees
echo Creation du topic 'processed-weather'...
docker exec kafka kafka-topics --create ^
  --bootstrap-server localhost:9092 ^
  --topic processed-weather ^
  --partitions 3 ^
  --replication-factor 1 ^
  --config retention.ms=604800000 ^
  --if-not-exists

REM Creer le topic pour les alertes meteorologiques
echo Creation du topic 'weather-alerts'...
docker exec kafka kafka-topics --create ^
  --bootstrap-server localhost:9092 ^
  --topic weather-alerts ^
  --partitions 1 ^
  --replication-factor 1 ^
  --config retention.ms=2592000000 ^
  --if-not-exists

REM Verifier la creation des topics
echo Verification des topics crees...
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092

echo Initialisation Kafka terminee avec succes !
echo.
echo Topics crees :
echo   - weather-data: Donnees meteo brutes de l'API Open-Meteo
echo   - processed-weather: Donnees meteo traitees par Spark
echo   - weather-alerts: Alertes meteorologiques en temps reel

pause