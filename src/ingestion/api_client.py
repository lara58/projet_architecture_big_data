"""
Client API Open Meteo - Version complète
Collecte toutes les données météorologiques disponibles
"""
import requests
import json
from datetime import datetime
from typing import Dict, List, Optional

class OpenMeteoClient:
    """Client pour l'API Open Meteo avec collecte complète des données"""
    
    BASE_URL = "https://api.open-meteo.com/v1/forecast"
    
    # Tous les paramètres horaires disponibles
    HOURLY_PARAMS = [
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
    ]
    
    # Tous les paramètres quotidiens disponibles
    DAILY_PARAMS = [
        "weathercode", "temperature_2m_max", "temperature_2m_min",
        "apparent_temperature_max", "apparent_temperature_min",
        "sunrise", "sunset", "daylight_duration", "sunshine_duration",
        "uv_index_max", "uv_index_clear_sky_max", "precipitation_sum",
        "rain_sum", "showers_sum", "snowfall_sum", "precipitation_hours",
        "precipitation_probability_max", "precipitation_probability_min",
        "precipitation_probability_mean", "windspeed_10m_max", "windgusts_10m_max",
        "winddirection_10m_dominant", "shortwave_radiation_sum",
        "et0_fao_evapotranspiration"
    ]
    
    def __init__(self, timeout: int = 30):
        self.timeout = timeout
    
    def get_complete_weather(
        self, 
        latitude: float, 
        longitude: float, 
        forecast_days: int = 7,
        timezone: str = "Europe/Paris"
    ) -> Optional[Dict]:
        """
        Récupère toutes les données météorologiques disponibles
        
        Args:
            latitude: Latitude de la localisation
            longitude: Longitude de la localisation
            forecast_days: Nombre de jours de prévision (1-16)
            timezone: Fuseau horaire
        
        Returns:
            Dictionnaire avec toutes les données météo ou None en cas d'erreur
        """
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current_weather": "true",
            "hourly": self.HOURLY_PARAMS,
            "daily": self.DAILY_PARAMS,
            "timezone": timezone,
            "forecast_days": min(forecast_days, 16)  # Maximum 16 jours
        }
        
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Erreur API Open Meteo: {e}")
            return None
    
    def get_current_only(self, latitude: float, longitude: float) -> Optional[Dict]:
        """Récupère uniquement les données météorologiques actuelles"""
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current_weather": "true"
        }
        
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Erreur API Open Meteo: {e}")
            return None
    
    def get_custom_data(
        self, 
        latitude: float, 
        longitude: float,
        hourly_params: List[str] = None,
        daily_params: List[str] = None,
        current_weather: bool = True,
        forecast_days: int = 7
    ) -> Optional[Dict]:
        """
        Récupère des données météorologiques personnalisées
        
        Args:
            latitude: Latitude
            longitude: Longitude
            hourly_params: Liste des paramètres horaires souhaités
            daily_params: Liste des paramètres quotidiens souhaités
            current_weather: Inclure la météo actuelle
            forecast_days: Nombre de jours de prévision
        """
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "forecast_days": min(forecast_days, 16)
        }
        
        if current_weather:
            params["current_weather"] = "true"
        
        if hourly_params:
            params["hourly"] = hourly_params
        
        if daily_params:
            params["daily"] = daily_params
        
        try:
            response = requests.get(self.BASE_URL, params=params, timeout=self.timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Erreur API Open Meteo: {e}")
            return None
    
    def get_weather_for_multiple_cities(self, cities: List[Dict]) -> List[Dict]:
        """
        Récupère les données météo pour plusieurs villes
        
        Args:
            cities: Liste de dictionnaires avec 'name', 'lat', 'lon'
        
        Returns:
            Liste des données météo pour chaque ville
        """
        results = []
        
        for city in cities:
            print(f"Collecte données pour {city['name']}...")
            
            data = self.get_complete_weather(city['lat'], city['lon'])
            
            if data:
                enhanced_data = {
                    "collection_timestamp": datetime.utcnow().isoformat(),
                    "city_info": city,
                    "weather_data": data,
                    "data_summary": {
                        "current_temperature": data.get("current_weather", {}).get("temperature"),
                        "current_windspeed": data.get("current_weather", {}).get("windspeed"),
                        "current_weathercode": data.get("current_weather", {}).get("weathercode"),
                        "hourly_points": len(data.get("hourly", {}).get("time", [])),
                        "daily_points": len(data.get("daily", {}).get("time", [])),
                        "data_size_kb": len(json.dumps(data)) / 1024
                    }
                }
                results.append(enhanced_data)
            else:
                print(f"Échec pour {city['name']}")
        
        return results

# Liste des villes européennes principales
EUROPEAN_CITIES = [
    {"name": "Paris", "lat": 48.8566, "lon": 2.3522, "country": "France"},
    {"name": "London", "lat": 51.5074, "lon": -0.1278, "country": "UK"},
    {"name": "Berlin", "lat": 52.5200, "lon": 13.4050, "country": "Germany"},
    {"name": "Madrid", "lat": 40.4168, "lon": -3.7038, "country": "Spain"},
    {"name": "Rome", "lat": 41.9028, "lon": 12.4964, "country": "Italy"},
    {"name": "Amsterdam", "lat": 52.3676, "lon": 4.9041, "country": "Netherlands"},
    {"name": "Brussels", "lat": 50.8503, "lon": 4.3517, "country": "Belgium"},
    {"name": "Vienna", "lat": 48.2082, "lon": 16.3738, "country": "Austria"},
    {"name": "Stockholm", "lat": 59.3293, "lon": 18.0686, "country": "Sweden"},
    {"name": "Oslo", "lat": 59.9139, "lon": 10.7522, "country": "Norway"},
    {"name": "Copenhagen", "lat": 55.6761, "lon": 12.5683, "country": "Denmark"},
    {"name": "Helsinki", "lat": 60.1695, "lon": 24.9354, "country": "Finland"},
    {"name": "Warsaw", "lat": 52.2297, "lon": 21.0122, "country": "Poland"},
    {"name": "Prague", "lat": 50.0755, "lon": 14.4378, "country": "Czech Republic"},
    {"name": "Budapest", "lat": 47.4979, "lon": 19.0402, "country": "Hungary"}
]

if __name__ == "__main__":
    # Test du client
    client = OpenMeteoClient()
    
    print("🌤️  Test du client Open Meteo")
    print("Collecte pour Paris...")
    
    data = client.get_complete_weather(48.8566, 2.3522)
    
    if data:
        print(f"✅ Données reçues!")
        print(f"📊 Température actuelle: {data.get('current_weather', {}).get('temperature')}°C")
        print(f"📈 Points horaires: {len(data.get('hourly', {}).get('time', []))}")
        print(f"📅 Points quotidiens: {len(data.get('daily', {}).get('time', []))}")
        print(f"💾 Taille des données: {len(json.dumps(data)) / 1024:.1f}KB")
    else:
        print("❌ Échec de collecte")