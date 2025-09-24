"""
DAG d'exemple météorologique - Version simplifiée
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Fonction Python simple pour démonstration
def print_weather():
    print("Données météo simulées pour Paris:")
    print("Température: 22°C")
    print("Humidité: 65%")
    print("Vent: 10 km/h")
    return "Données simulées générées avec succès"

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
    'meteo_exemple_simplifie',
    default_args=default_args,
    description='Un DAG d\'exemple simplifié pour la météo',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['exemple', 'meteo'],
)

# Tâche 1: Afficher un message
t1 = BashOperator(
    task_id='afficher_date',
    bash_command='echo "Exécution le $(date)"',
    dag=dag,
)

# Tâche 2: Exécuter une fonction Python
t2 = PythonOperator(
    task_id='generer_donnees_meteo',
    python_callable=print_weather,
    dag=dag,
)

# Tâche 3: Message de fin
t3 = BashOperator(
    task_id='message_fin',
    bash_command='echo "Traitement météo terminé"',
    dag=dag,
)

# Définir l'ordre d'exécution des tâches
t1 >> t2 >> t3