#!/usr/bin/env python3
"""
Script batch pour récupérer les données météo
"""
import os
import requests
import json
import time
import logging
import sys
from datetime import datetime

# Configuration du logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger("weather_batch")

# Liste complète des villes européennes
cities = [
    {"name": "Paris", "lat": 48.8566, "lon": 2.3522},
    {"name": "London", "lat": 51.5074, "lon": -0.1278},
    {"name": "Berlin", "lat": 52.5200, "lon": 13.4050},
    {"name": "Madrid", "lat": 40.4168, "lon": -3.7038},
    {"name": "Rome", "lat": 41.9028, "lon": 12.4964},
    {"name": "Amsterdam", "lat": 52.3676, "lon": 4.9041},
    {"name": "Vienna", "lat": 48.2082, "lon": 16.3738},
    {"name": "Prague", "lat": 50.0755, "lon": 14.4378},
    {"name": "Budapest", "lat": 47.4979, "lon": 19.0402},
    {"name": "Warsaw", "lat": 52.2297, "lon": 21.0122},
    {"name": "Stockholm", "lat": 59.3293, "lon": 18.0686},
    {"name": "Copenhagen", "lat": 55.6761, "lon": 12.5683},
    {"name": "Helsinki", "lat": 60.1699, "lon": 24.9384},
    {"name": "Oslo", "lat": 59.9139, "lon": 10.7522},
    {"name": "Zurich", "lat": 47.3769, "lon": 8.5417},
    {"name": "Brussels", "lat": 50.8503, "lon": 4.3517},
    {"name": "Dublin", "lat": 53.3498, "lon": -6.2603},
    {"name": "Lisbon", "lat": 38.7223, "lon": -9.1393},
    {"name": "Athens", "lat": 37.9838, "lon": 23.7275},
    {"name": "Barcelona", "lat": 41.3851, "lon": 2.1734},
]

def fetch_weather_data(city, city_index, total_cities):
    """Récupère les données météo pour une ville"""
    logger.info(f"[{city_index}/{total_cities}] Récupération des données historiques pour {city['name']} (1940-2025)")
    
    # Paramètres complets de l'API - Données historiques depuis 1940
    params = {
        'latitude': city['lat'],
        'longitude': city['lon'],
        'start_date': '1940-01-01',
        'end_date': '2025-09-24',
        'daily': 'weather_code,temperature_2m_mean,temperature_2m_max,temperature_2m_min,apparent_temperature_mean,apparent_temperature_max,apparent_temperature_min,daylight_duration,sunshine_duration,sunset,sunrise,precipitation_sum,rain_sum,snowfall_sum,precipitation_hours,wind_speed_10m_max,shortwave_radiation_sum,wind_direction_10m_dominant,wind_gusts_10m_max,et0_fao_evapotranspiration',
        'timezone': 'Europe/Berlin'
    }
    
    try:
        logger.info(f"Appel API en cours pour {city['name']}... (cela peut prendre du temps)")
        response = requests.get('https://archive-api.open-meteo.com/v1/archive', params=params, timeout=120)
        response.raise_for_status()
        
        data = response.json()
        # Ajouter le nom de la ville aux données
        data['city'] = city['name']
        data['coordinates'] = {'lat': city['lat'], 'lon': city['lon']}
        
        # Calculer quelques statistiques pour info
        if 'daily' in data and 'time' in data['daily']:
            days_count = len(data['daily']['time'])
            logger.info(f"[{city_index}/{total_cities}] {city['name']}: {days_count} jours de données récupérés")
        else:
            logger.info(f"[{city_index}/{total_cities}] Données récupérées pour {city['name']}")
            
        return data
        
    except Exception as e:
        logger.error(f"[{city_index}/{total_cities}] Erreur pour {city['name']}: {e}")
        return None

def save_to_local_file(all_data, output_file):
    """Sauvegarde les données dans un fichier local"""
    try:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(all_data, f, ensure_ascii=False, indent=2)
        
        logger.info(f"Données sauvegardées dans {output_file}")
        return True
        
    except Exception as e:
        logger.error(f"Erreur sauvegarde: {e}")
        return False

def copy_to_hdfs(local_file, hdfs_path):
    """Copie le fichier vers HDFS"""
    try:
        import subprocess
        
        # Créer le répertoire HDFS si nécessaire
        subprocess.run(['hdfs', 'dfs', '-mkdir', '-p', os.path.dirname(hdfs_path)], 
                      capture_output=True, check=False)
        
        # Copier le fichier
        result = subprocess.run(['hdfs', 'dfs', '-put', '-f', local_file, hdfs_path], 
                               capture_output=True, text=True)
        
        if result.returncode == 0:
            logger.info(f"Fichier copié vers HDFS: {hdfs_path}")
            return True
        else:
            logger.warning(f"Échec copie HDFS: {result.stderr}")
            return False
            
    except Exception as e:
        logger.warning(f"HDFS non disponible: {e}")
        return False

def main():
    logger.info("Démarrage du batch météo simplifié")
    
    # Collecter les données historiques de toutes les villes
    logger.info(f"Début de la récupération historique pour {len(cities)} villes européennes")
    logger.info(f"Période: 1940-2025 (85 ans de données)")
    logger.info(f"Estimation: ~{len(cities) * 2} minutes (avec pauses API)")
    
    all_weather_data = []
    successful_cities = []

    for i, city in enumerate(cities, 1):
        data = fetch_weather_data(city, i, len(cities))
        if data:
            all_weather_data.append(data)
            successful_cities.append(city['name'])
        
        # Pause plus longue pour les gros datasets historiques
        if i < len(cities):  # Pas de pause après la dernière ville
            logger.info(f"Pause de 3 secondes avant la prochaine ville...")
            time.sleep(3)
    
    if not all_weather_data:
        logger.error("Aucune donnée récupérées !")
        return 1
    
    logger.info(f"Récupération terminée ! {len(successful_cities)}/{len(cities)} villes réussies")
    
    # Calculer des statistiques sur les données récupérées
    total_days = 0
    for city_data in all_weather_data:
        if 'daily' in city_data and 'time' in city_data['daily']:
            total_days += len(city_data['daily']['time'])
    
    logger.info(f"Total de jours de données récupérés: {total_days:,}")
    logger.info(f"Moyenne par ville: {total_days // len(successful_cities):,} jours")
    
    # Préparer les données de sortie
    output_data = {
        'timestamp': datetime.now().isoformat(),
        'collection_period': '1940-01-01 to 2025-12-31',
        'cities_count': len(successful_cities),
        'total_days': total_days,
        'cities': successful_cities,
        'data': all_weather_data
    }
    
    # Sauvegarde locale
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    local_output = f'/tmp/weather_batch_full_{timestamp}.json'
    if save_to_local_file(output_data, local_output):
        logger.info(f"Données disponibles localement: {local_output}")
        
        # Copie vers HDFS avec timestamp
        hdfs_output = f'/weather-data/batch_full_{timestamp}.json'
        if copy_to_hdfs(local_output, hdfs_output):
            logger.info(f"Données également disponibles dans HDFS: {hdfs_output}")
        else:
            logger.info("Données disponibles uniquement en local pour le moment")
    
    logger.info(f"Job terminé ! {len(successful_cities)} villes traitées: {', '.join(successful_cities)}")
    return 0

if __name__ == "__main__":
    sys.exit(main())