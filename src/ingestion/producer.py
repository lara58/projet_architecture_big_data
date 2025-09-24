from kafka import KafkaProducer
import requests, json, time
from datetime import datetime

# Configuration Kafka
KAFKA_BROKER = "kafka:9092"   # Nom du service Docker
TOPIC = "weather" # Nom du topic

# Producteur Kafka
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

# Villes européennes à surveiller
CITIES = [
    {"name": "Paris", "lat": 48.8566, "lon": 2.3522, "country": "France"},
    {"name": "London", "lat": 51.5074, "lon": -0.1278, "country": "UK"},
    {"name": "Berlin", "lat": 52.5200, "lon": 13.4050, "country": "Germany"},
    {"name": "Madrid", "lat": 40.4168, "lon": -3.7038, "country": "Spain"},
    {"name": "Rome", "lat": 41.9028, "lon": 12.4964, "country": "Italy"},
    {"name": "Amsterdam", "lat": 52.3676, "lon": 4.9041, "country": "Netherlands"},
    {"name": "Brussels", "lat": 50.8503, "lon": 4.3517, "country": "Belgium"},
    {"name": "Vienna", "lat": 48.2082, "lon": 16.3738, "country": "Austria"},
    {"name": "Stockholm", "lat": 59.3293, "lon": 18.0686, "country": "Sweden"},
    {"name": "Oslo", "lat": 59.9139, "lon": 10.7522, "country": "Norway"}
]

def get_complete_weather_data(city):
    """Récupère toutes les données météorologiques disponibles pour une ville"""
    
    # URL avec tous les paramètres disponibles
    api_url = f"https://api.open-meteo.com/v1/forecast"
    
    params = {
        "latitude": city["lat"],
        "longitude": city["lon"],
        "current_weather": "true",
        "hourly": [
            "temperature_2m", "relativehumidity_2m", "dewpoint_2m", 
            "apparent_temperature", "precipitation_probability", "precipitation",
            "rain", "showers", "snowfall", "snow_depth", "weathercode",
            "pressure_msl", "surface_pressure", "cloudcover", "cloudcover_low",
            "cloudcover_mid", "cloudcover_high", "visibility", "evapotranspiration",
            "et0_fao_evapotranspiration", "vapourpressure_deficit", "windspeed_10m",
            "windspeed_80m", "windspeed_120m", "windspeed_180m", "winddirection_10m",
            "winddirection_80m", "winddirection_120m", "winddirection_180m",
            "windgusts_10m", "temperature_80m", "temperature_120m", "temperature_180m",
            "soil_temperature_0cm", "soil_temperature_6cm", "soil_temperature_18cm",
            "soil_temperature_54cm", "soil_moisture_0_1cm", "soil_moisture_1_3cm",
            "soil_moisture_3_9cm", "soil_moisture_9_27cm", "soil_moisture_27_81cm"
        ],
        "daily": [
            "weathercode", "temperature_2m_max", "temperature_2m_min",
            "apparent_temperature_max", "apparent_temperature_min",
            "sunrise", "sunset", "daylight_duration", "sunshine_duration",
            "uv_index_max", "uv_index_clear_sky_max", "precipitation_sum",
            "rain_sum", "showers_sum", "snowfall_sum", "precipitation_hours",
            "precipitation_probability_max", "precipitation_probability_min",
            "precipitation_probability_mean", "windspeed_10m_max", "windgusts_10m_max",
            "winddirection_10m_dominant", "shortwave_radiation_sum",
            "et0_fao_evapotranspiration"
        ],
        "timezone": "Europe/Paris",
        "forecast_days": 7
    }
    
    try:
        response = requests.get(api_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        # Enrichir les données avec des métadonnées
        enhanced_data = {
            "collection_timestamp": datetime.utcnow().isoformat(),
            "data_source": "open-meteo",
            "city_info": city,
            "raw_data": data,
            # Extraction des données actuelles pour faciliter le filtrage
            "current": data.get("current_weather", {}),
            "hourly_count": len(data.get("hourly", {}).get("time", [])),
            "daily_count": len(data.get("daily", {}).get("time", [])),
            "data_quality": {
                "has_current": "current_weather" in data,
                "has_hourly": "hourly" in data,
                "has_daily": "daily" in data,
                "response_size_kb": len(json.dumps(data)) / 1024
            }
        }
        
        return enhanced_data
        
    except requests.exceptions.RequestException as e:
        print(f"Erreur réseau pour {city['name']}: {e}")
        return None
    except json.JSONDecodeError as e:
        print(f"Erreur de décodage JSON pour {city['name']}: {e}")
        return None
    except Exception as e:
        print(f"Erreur inattendue pour {city['name']}: {e}")
        return None

print("🌤️  Démarrage du producteur météorologique Open Meteo")
print(f"📊 Collecte de données pour {len(CITIES)} villes")
print("🔄 Collecte de TOUTES les données disponibles (current, hourly, daily)")

iteration = 0
while True:
    iteration += 1
    print(f"\n--- Itération {iteration} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} ---")
    
    for city in CITIES:
        try:
            print(f"🌍 Collecte pour {city['name']}, {city['country']}...")
            
            weather_data = get_complete_weather_data(city)
            
            if weather_data:
                # Statistiques sur les données collectées
                data_size = len(json.dumps(weather_data)) / 1024  # KB
                current_temp = weather_data.get("current", {}).get("temperature", "N/A")
                
                print(f"  ✅ Données collectées: {data_size:.1f}KB, Temp: {current_temp}°C")
                print(f"  📈 Hourly points: {weather_data['hourly_count']}, Daily points: {weather_data['daily_count']}")
                
                # Envoi vers Kafka
                producer.send(TOPIC, weather_data)
                producer.flush()  # S'assurer que le message est envoyé
                
            else:
                print(f"  ❌ Échec de collecte pour {city['name']}")
                
        except Exception as e:
            print(f"  💥 Erreur lors du traitement de {city['name']}: {e}")
    
    print(f"⏱️  Attente de 60 secondes avant la prochaine collecte...")
    time.sleep(60)  # Attendre 1 minute entre les collectes complètes