@echo off
REM Script d'initialisation complète de l'environnement Big Data
echo ===========================================================
echo Initialisation de l'environnement complet Big Data
echo ===========================================================

REM Démarrer les conteneurs principaux
echo [1/5] Démarrage des services principaux (Kafka, Zookeeper, Spark)...
cd docker
docker-compose up -d
timeout /t 30 /nobreak > nul

REM Initialiser Kafka topics
echo [2/5] Initialisation des topics Kafka...
cd ..\scripts
call init-kafka.bat

REM Démarrer Airflow
echo [3/5] Démarrage d'Airflow...
cd ..\docker
docker-compose -f docker-compose-airflow.yml up -d
timeout /t 20 /nobreak > nul

REM Créer les dossiers pour les données
echo [4/5] Préparation des dossiers de données...
cd ..\data
if not exist raw mkdir raw
if not exist processed mkdir processed
if not exist output mkdir output

REM Vérifier la connectivité
echo [5/5] Vérification de la connectivité des services...
cd ..\scripts
call test-connectivity.bat

echo ===========================================================
echo Initialisation terminée ! Tous les services sont prêts.
echo URLs des interfaces:
echo  - Spark Master  : http://localhost:8080
echo  - Spark Worker  : http://localhost:8081
echo  - Airflow       : http://localhost:8080 (admin/admin)
echo ===========================================================
pause