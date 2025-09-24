@echo off
REM Script de vérification de la connectivité des services
echo Vérification de la connectivité des services...

REM Test Kafka
echo [1/4] Test Kafka...
docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
IF %ERRORLEVEL% NEQ 0 (
  echo [ÉCHEC] Problème avec Kafka
) ELSE (
  echo [OK] Kafka est opérationnel
)

REM Test Spark Master
echo [2/4] Test Spark Master...
curl -s http://localhost:8080 > nul
IF %ERRORLEVEL% NEQ 0 (
  echo [ÉCHEC] Problème avec Spark Master
) ELSE (
  echo [OK] Spark Master est opérationnel
)

REM Test Spark Worker
echo [3/4] Test Spark Worker...
curl -s http://localhost:8081 > nul
IF %ERRORLEVEL% NEQ 0 (
  echo [ÉCHEC] Problème avec Spark Worker
) ELSE (
  echo [OK] Spark Worker est opérationnel
)

REM Test Airflow
echo [4/4] Test Airflow...
curl -s http://localhost:8080 > nul
IF %ERRORLEVEL% NEQ 0 (
  echo [ÉCHEC] Problème avec Airflow
) ELSE (
  echo [OK] Airflow est opérationnel
)

echo.
echo Résumé des tests de connectivité:
echo ================================
echo Tester un job Spark : docker exec spark-master spark-submit --class org.apache.spark.examples.SparkPi /opt/bitnami/spark/examples/jars/spark-examples_2.12-3.4.3.jar 10
echo Tester le volume partagé : docker exec spark-master sh -c "echo 'Test' > /data/test.txt" ^&^& docker exec spark-worker cat /data/test.txt
echo Tester un DAG Airflow : Accéder à http://localhost:8080 et activer un DAG d'exemple
echo.