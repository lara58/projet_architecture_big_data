"""
DAG d'ingestion des données météorologiques utilisant le nouveau Task SDK d'Airflow 2.7+/3.0
"""
from airflow.sdk import dag, task
import pendulum
import json
import os
import requests
from typing import Dict, List, Any

# Définir le DAG avec le décorateur @dag (nouvelle approche)
@dag(
    schedule="@hourly",
    start_date=pendulum.datetime(2025, 1, 1),
    catchup=False,
    tags=["weather", "api", "kafka", "sdk"],
    description='Ingestion des données météo depuis Open-Meteo avec le Task SDK'
)
def weather_data_sdk():
    """
    Pipeline d'ingestion des données météorologiques utilisant le nouveau style Airflow SDK.
    Ce DAG récupère des données météo de l'API Open-Meteo pour plusieurs villes,
    les stocke dans un fichier brut, puis les publie dans Kafka.
    """
    
    @task(multiple_outputs=True)
    def fetch_weather_data() -> Dict[str, List[Dict[str, Any]]]:
        """Récupère les données météo de plusieurs villes depuis l'API Open-Meteo."""
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
        timestamp = pendulum.now().format("YYYYMMDD_HHmmss")
        output_path = f"/data/raw/weather_{timestamp}.json"
        
        # Créer le répertoire si nécessaire
        os.makedirs("/data/raw", exist_ok=True)
        
        with open(output_path, "w") as f:
            json.dump(all_data, f)
        
        return {
            "all_data": all_data,
            "output_path": output_path,
            "timestamp": timestamp
        }

    @task
    def analyze_temperature(weather_data: Dict[str, List[Dict[str, Any]]]) -> Dict[str, float]:
        """Analyse les données de température et calcule des statistiques."""
        all_data = weather_data["all_data"]
        temperatures = []
        
        for city_data in all_data:
            if "current_weather" in city_data and "temperature" in city_data["current_weather"]:
                temperatures.append(city_data["current_weather"]["temperature"])
        
        if not temperatures:
            return {"avg_temp": None, "min_temp": None, "max_temp": None}
            
        return {
            "avg_temp": sum(temperatures) / len(temperatures),
            "min_temp": min(temperatures),
            "max_temp": max(temperatures)
        }

    @task
    def publish_to_kafka(weather_data: Dict[str, List[Dict[str, Any]]], temp_stats: Dict[str, float]) -> None:
        """Publie les données et les statistiques dans Kafka."""
        try:
            from kafka import KafkaProducer
            
            # Configurer le producteur Kafka
            producer = KafkaProducer(
                bootstrap_servers=['kafka:29092'],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            # Publier chaque ville comme un message séparé
            all_data = weather_data["all_data"]
            for data in all_data:
                city = data["city"]
                
                # Créer un message avec les infos actuelles
                if "current_weather" in data:
                    message = {
                        "city": city,
                        "timestamp": pendulum.now().isoformat(),
                        "current_weather": data["current_weather"],
                        "coordinates": {
                            "latitude": data["latitude"],
                            "longitude": data["longitude"]
                        }
                    }
                    
                    # Publier dans le topic Kafka
                    producer.send('weather-data', message)
                    print(f"Message publié pour {city}")
            
            # Publier les statistiques globales
            producer.send('weather-data', {
                "type": "statistics",
                "timestamp": pendulum.now().isoformat(),
                "data": temp_stats
            })
            
            # Assurer que tous les messages sont envoyés
            producer.flush()
            print("Publication Kafka terminée")
            
        except ImportError:
            print("Module Kafka non disponible. Simulation de publication...")
            print(f"Statistiques qui auraient été publiées : {temp_stats}")
    
    @task
    def send_notification(temp_stats: Dict[str, float]) -> None:
        """Envoie une notification avec un résumé des statistiques météo."""
        if temp_stats["avg_temp"] is not None:
            message = (
                f"Rapport météo du {pendulum.now().format('DD/MM/YYYY HH:mm')} :\n"
                f"Température moyenne : {temp_stats['avg_temp']:.1f}°C\n"
                f"Température minimale : {temp_stats['min_temp']:.1f}°C\n"
                f"Température maximale : {temp_stats['max_temp']:.1f}°C"
            )
            print(f"NOTIFICATION: {message}")
            
            # Dans un cas réel, on pourrait envoyer un email, une alerte ou un message Slack
            # Par exemple: send_slack_message(message)
        else:
            print("NOTIFICATION: Aucune donnée météo disponible")

    # Définir le flux de travail avec le style pythonique du SDK
    # Les dépendances sont implicites par les appels de fonctions
    weather_result = fetch_weather_data()
    temp_stats = analyze_temperature(weather_result)
    publish_to_kafka(weather_result, temp_stats)
    send_notification(temp_stats)

# Instancier le DAG
weather_data_sdk_dag = weather_data_sdk()