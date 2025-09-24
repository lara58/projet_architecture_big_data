"""
DAG de traitement batch des données météorologiques avec Spark
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Définition du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'weather_batch_processing',
    default_args=default_args,
    description='Traitement batch des données météo avec Spark',
    schedule_interval='0 2 * * *',  # Tous les jours à 2h du matin
    start_date=days_ago(1),
    catchup=False,
    tags=['weather', 'spark', 'batch'],
)

# Tâche 1: Préparer le répertoire pour l'exécution batch
prepare_batch_dir = BashOperator(
    task_id='prepare_batch_directory',
    bash_command='mkdir -p /data/batch/$(date +%Y-%m-%d) && echo "Répertoire préparé pour le traitement batch"',
    dag=dag,
)

# Tâche 2: Traitement des données avec Spark (analyse quotidienne)
process_daily_data = SparkSubmitOperator(
    task_id='process_daily_weather',
    conn_id='spark_default',
    application='/app/spark/daily_weather_processor.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='1',
    name='daily_weather_processing',
    verbose=True,
    application_args=[
        '--date', '{{ ds }}',
        '--input-path', '/data/raw',
        '--output-path', '/data/processed/daily_{{ ds_nodash }}'
    ],
    dag=dag,
)

# Tâche 3: Traitement des données avec Spark (agrégations mensuelles)
process_monthly_aggregates = SparkSubmitOperator(
    task_id='process_monthly_aggregates',
    conn_id='spark_default',
    application='/app/spark/monthly_weather_aggregator.py',
    total_executor_cores='2',
    executor_cores='2',
    executor_memory='2g',
    num_executors='1',
    name='monthly_weather_aggregation',
    verbose=True,
    application_args=[
        '--month', '{{ execution_date.strftime("%Y-%m") }}',
        '--input-path', '/data/processed',
        '--output-path', '/data/output/monthly_{{ execution_date.strftime("%Y%m") }}'
    ],
    dag=dag,
)

# Tâche 4: Générer un rapport de synthèse
generate_report = BashOperator(
    task_id='generate_report',
    bash_command='echo "Rapport généré pour la date {{ ds }}" > /data/output/report_{{ ds_nodash }}.txt',
    dag=dag,
)

# Tâche 5: Notifier la fin du processus
notify_completion = BashOperator(
    task_id='notify_completion',
    bash_command='echo "Traitement batch terminé pour la date {{ ds }} à $(date)"',
    dag=dag,
)

# Définition du flux de tâches
prepare_batch_dir >> process_daily_data >> process_monthly_aggregates >> generate_report >> notify_completion