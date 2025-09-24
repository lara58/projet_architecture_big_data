import json
import time
import requests
from kafka import KafkaProducer
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Configuration de la session avec retry automatique
session = requests.Session()
retry_strategy = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)
adapter = HTTPAdapter(max_retries=retry_strategy)
session.mount("http://", adapter)
session.mount("https://", adapter)

# Connexion au broker Kafka
producer = KafkaProducer(
    bootstrap_servers='kafka:29092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Liste des villes avec leurs coordonnées
cities = [
    {"name": "Paris", "lat": 48.8566, "lon": 2.3522},
    {"name": "New York", "lat": 40.7128, "lon": -74.0060},
    {"name": "Tokyo", "lat": 35.6762, "lon": 139.6503}
]

def get_weather_data(city):
    """Récupère les données météo avec gestion d'erreurs"""
    try:
        # URL avec tous les paramètres météorologiques actuels essentiels
        url = f"https://api.open-meteo.com/v1/forecast?latitude={city['lat']}&longitude={city['lon']}&current=temperature_2m,relative_humidity_2m,apparent_temperature,is_day,precipitation,rain,showers,snowfall,weather_code,cloud_cover,pressure_msl,surface_pressure,wind_speed_10m,wind_direction_10m,wind_gusts_10m&timezone=auto"
        response = session.get(url, timeout=10)
        
        if response.status_code == 200:
            data = response.json()
            current = data.get('current', {})
            
            payload = {
                "city": city['name'],
                "timestamp": current.get('time', ''),
                "temperature_2m": current.get('temperature_2m', None),
                "relative_humidity_2m": current.get('relative_humidity_2m', None),
                "apparent_temperature": current.get('apparent_temperature', None),
                "is_day": current.get('is_day', None),
                "precipitation": current.get('precipitation', None),
                "rain": current.get('rain', None),
                "showers": current.get('showers', None),
                "snowfall": current.get('snowfall', None),
                "weather_code": current.get('weather_code', None),
                "cloud_cover": current.get('cloud_cover', None),
                "pressure_msl": current.get('pressure_msl', None),
                "surface_pressure": current.get('surface_pressure', None),
                "wind_speed_10m": current.get('wind_speed_10m', None),
                "wind_direction_10m": current.get('wind_direction_10m', None),
                "wind_gusts_10m": current.get('wind_gusts_10m', None)
            }
            return payload
        else:
            print(f"Erreur HTTP {response.status_code} pour {city['name']}")
            return None
            
    except requests.exceptions.SSLError as e:
        print(f"Erreur SSL pour {city['name']}: {e}")
        return None
    except requests.exceptions.ConnectionError as e:
        print(f"Erreur de connexion pour {city['name']}: {e}")
        return None
    except requests.exceptions.Timeout as e:
        print(f"Timeout pour {city['name']}: {e}")
        return None
    except Exception as e:
        print(f"Erreur inattendue pour {city['name']}: {e}")
        return None

print("🌤️ Démarrage du collecteur météo...")

while True:
    for city in cities:
        payload = get_weather_data(city)
        if payload:
            try:
                producer.send('weather', payload)
                print(f"Sent: {payload}")
            except Exception as e:
                print(f"Erreur Kafka pour {city['name']}: {e}")
        
        # Délai entre chaque ville pour éviter de surcharger l'API
        time.sleep(5)
    
    print("🔄 Cycle terminé, attente 2 minutes...")
    time.sleep(120)  # Attendre 2 minutes entre les cycles
