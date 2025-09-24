"""
DAG pour la gestion des alertes météorologiques en temps réel
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

import json
import os
from kafka import KafkaConsumer, KafkaProducer

# Fonction pour détecter les alertes météo
def detect_weather_alerts(**kwargs):
    # Configurer le consumer Kafka
    consumer = KafkaConsumer(
        'weather-data',
        bootstrap_servers=['kafka:29092'],
        auto_offset_reset='latest',
        enable_auto_commit=True,
        group_id='weather-alerts-detector',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=10000  # 10 secondes de timeout
    )
    
    # Configurer le producer Kafka pour les alertes
    producer = KafkaProducer(
        bootstrap_servers=['kafka:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    
    # Seuils pour les alertes
    alert_thresholds = {
        'high_temp': 35.0,       # °C
        'low_temp': -10.0,       # °C
        'high_wind': 80.0,       # km/h
        'heavy_rain': 10.0,      # mm/h
    }
    
    alert_count = 0
    
    # Traiter les messages pendant un certain temps
    for message in consumer:
        data = message.value
        city = data.get('city', 'Unknown')
        current = data.get('current_weather', {})
        
        alerts = []
        
        # Vérifier température élevée
        if current.get('temperature') > alert_thresholds['high_temp']:
            alerts.append({
                'type': 'HIGH_TEMPERATURE',
                'value': current.get('temperature'),
                'threshold': alert_thresholds['high_temp'],
                'message': f"Température élevée détectée à {city}: {current.get('temperature')}°C"
            })
        
        # Vérifier température basse
        if current.get('temperature') < alert_thresholds['low_temp']:
            alerts.append({
                'type': 'LOW_TEMPERATURE',
                'value': current.get('temperature'),
                'threshold': alert_thresholds['low_temp'],
                'message': f"Température basse détectée à {city}: {current.get('temperature')}°C"
            })
        
        # Vérifier vent fort
        if current.get('windspeed') > alert_thresholds['high_wind']:
            alerts.append({
                'type': 'HIGH_WIND',
                'value': current.get('windspeed'),
                'threshold': alert_thresholds['high_wind'],
                'message': f"Vent fort détecté à {city}: {current.get('windspeed')} km/h"
            })
        
        # Si des alertes sont détectées, les publier dans le topic des alertes
        if alerts:
            for alert in alerts:
                alert_message = {
                    'timestamp': datetime.now().isoformat(),
                    'city': city,
                    'coordinates': data.get('coordinates', {}),
                    'alert': alert
                }
                
                producer.send('weather-alerts', alert_message)
                print(f"Alerte publiée: {alert['message']}")
                alert_count += 1
    
    # Assurer que tous les messages sont envoyés
    producer.flush()
    
    return f"{alert_count} alertes détectées et publiées"

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
    'weather_alerts_monitoring',
    default_args=default_args,
    description='Surveillance et détection des alertes météorologiques',
    schedule_interval=timedelta(minutes=30),  # Exécution toutes les 30 minutes
    start_date=days_ago(1),
    catchup=False,
    tags=['weather', 'alerts', 'kafka', 'streaming'],
)

# Tâche 1: Détecter les alertes météo
detect_alerts_task = PythonOperator(
    task_id='detect_weather_alerts',
    python_callable=detect_weather_alerts,
    provide_context=True,
    dag=dag,
)

# Tâche 2: Notifier la fin du processus
notify_task = BashOperator(
    task_id='notify_completion',
    bash_command='echo "Détection des alertes météo terminée à $(date)"',
    dag=dag,
)

# Définition du flux de tâches
detect_alerts_task >> notify_task