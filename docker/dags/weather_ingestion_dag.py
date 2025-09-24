"""
DAG d'ingestion des données météorologiques depuis l'API Open-Meteo vers Kafka
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

import requests
import json
import os
from kafka import KafkaProducer

# Fonction pour récupérer les données météo de l'API Open-Meteo
def fetch_weather_data(**kwargs):
    # Liste des villes avec leurs coordonnées
    cities = [
        {"name": "Paris", "lat": 48.85, "lon": 2.35},
        {"name": "New York", "lat": 40.71, "lon": -74.01},
        {"name": "Tokyo", "lat": 35.69, "lon": 139.69},
        {"name": "Sydney", "lat": -33.87, "lon": 151.21},
        {"name": "Nairobi", "lat": -1.29, "lon": 36.82}
    ]
    
    all_data = []
    
    for city in cities:
        # Paramètres de l'API Open-Meteo
        params = {
            "latitude": city["lat"],
            "longitude": city["lon"],
            "current_weather": "true",
            "hourly": "temperature_2m,relative_humidity_2m,precipitation,windspeed_10m"
        }
        
        # Appel à l'API
        response = requests.get(
            "https://api.open-meteo.com/v1/forecast",
            params=params
        )
        
        if response.status_code == 200:
            data = response.json()
            # Ajout du nom de la ville
            data["city"] = city["name"]
            all_data.append(data)
            print(f"Données récupérées pour {city['name']}")
        else:
            print(f"Erreur pour {city['name']}: {response.status_code}")
    
    # Sauvegarde des données brutes
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = f"/data/raw/weather_{timestamp}.json"
    
    with open(output_path, "w") as f:
        json.dump(all_data, f)
    
    # Passer le chemin du fichier à la tâche suivante
    kwargs['ti'].xcom_push(key='output_path', value=output_path)
    return output_path

# Fonction pour publier les données dans Kafka
def publish_to_kafka(**kwargs):
    ti = kwargs['ti']
    output_path = ti.xcom_pull(key='output_path', task_ids='fetch_weather_data')
    
    # Charger les données
    with open(output_path, "r") as f:
        all_data = json.load(f)
    
    # Configurer le producteur Kafka
    producer = KafkaProducer(
        bootstrap_servers=['kafka:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Publier chaque ville comme un message séparé
    for data in all_data:
        city = data["city"]
        
        # Créer un message avec les infos actuelles et la première prévision horaire
        if "current_weather" in data:
            message = {
                "city": city,
                "timestamp": datetime.now().isoformat(),
                "current_weather": data["current_weather"],
                "coordinates": {
                    "latitude": data["latitude"],
                    "longitude": data["longitude"]
                }
            }
            
            # Publier dans le topic Kafka
            producer.send('weather-data', message)
            print(f"Message publié pour {city}")
    
    # Assurer que tous les messages sont envoyés
    producer.flush()
    print("Publication Kafka terminée")

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
    'weather_data_ingestion',
    default_args=default_args,
    description='Ingestion des données météo depuis Open-Meteo vers Kafka',
    schedule_interval=timedelta(hours=1),
    start_date=days_ago(1),
    catchup=False,
    tags=['weather', 'api', 'kafka'],
)

# Tâche 1: Récupérer les données météo
fetch_task = PythonOperator(
    task_id='fetch_weather_data',
    python_callable=fetch_weather_data,
    provide_context=True,
    dag=dag,
)

# Tâche 2: Publier les données dans Kafka
publish_task = PythonOperator(
    task_id='publish_to_kafka',
    python_callable=publish_to_kafka,
    provide_context=True,
    dag=dag,
)

# Tâche 3: Notifier la fin du processus
notify_task = BashOperator(
    task_id='notify_completion',
    bash_command='echo "Ingestion des données météo terminée à $(date)"',
    dag=dag,
)

# Définition du flux de tâches
fetch_task >> publish_task >> notify_task