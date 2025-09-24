"""
Consumer Kafka avec filtrage des données météorologiques
Exemple de filtrage des données complètes Open Meteo
"""
from kafka import KafkaConsumer
import json
from datetime import datetime
from typing import Dict, List

class WeatherDataFilter:
    """Classe pour filtrer et traiter les données météorologiques"""
    
    def __init__(self):
        self.processed_count = 0
        self.filtered_count = 0
    
    def filter_temperature_alerts(self, weather_data: Dict) -> Dict:
        """Filtre les alertes de température (>30°C ou <-10°C)"""
        current = weather_data.get("raw_data", {}).get("current_weather", {})
        temp = current.get("temperature", 0)
        
        if temp > 30 or temp < -10:
            return {
                "alert_type": "temperature_extreme",
                "city": weather_data.get("city_info", {}).get("name"),
                "temperature": temp,
                "timestamp": weather_data.get("collection_timestamp"),
                "severity": "high" if temp > 35 or temp < -15 else "medium"
            }
        return None
    
    def filter_wind_alerts(self, weather_data: Dict) -> Dict:
        """Filtre les alertes de vent fort (>50 km/h)"""
        current = weather_data.get("raw_data", {}).get("current_weather", {})
        windspeed = current.get("windspeed", 0)
        
        if windspeed > 50:
            return {
                "alert_type": "high_wind",
                "city": weather_data.get("city_info", {}).get("name"),
                "windspeed": windspeed,
                "timestamp": weather_data.get("collection_timestamp"),
                "severity": "high" if windspeed > 80 else "medium"
            }
        return None
    
    def filter_precipitation_forecast(self, weather_data: Dict) -> List[Dict]:
        """Filtre les prévisions de précipitations importantes"""
        raw_data = weather_data.get("raw_data", {})
        daily_data = raw_data.get("daily", {})
        
        if not daily_data:
            return []
        
        alerts = []
        times = daily_data.get("time", [])
        precipitation_sums = daily_data.get("precipitation_sum", [])
        
        for i, (date, precip) in enumerate(zip(times, precipitation_sums)):
            if precip and precip > 10:  # Plus de 10mm de pluie
                alerts.append({
                    "alert_type": "heavy_precipitation",
                    "city": weather_data.get("city_info", {}).get("name"),
                    "date": date,
                    "precipitation_mm": precip,
                    "severity": "high" if precip > 25 else "medium"
                })
        
        return alerts
    
    def extract_current_summary(self, weather_data: Dict) -> Dict:
        """Extrait un résumé des conditions actuelles"""
        current = weather_data.get("raw_data", {}).get("current_weather", {})
        city_info = weather_data.get("city_info", {})
        
        return {
            "type": "current_summary",
            "city": city_info.get("name"),
            "country": city_info.get("country"),
            "timestamp": weather_data.get("collection_timestamp"),
            "conditions": {
                "temperature": current.get("temperature"),
                "windspeed": current.get("windspeed"),
                "winddirection": current.get("winddirection"),
                "weathercode": current.get("weathercode"),
                "is_day": current.get("is_day")
            },
            "coordinates": {
                "latitude": city_info.get("lat"),
                "longitude": city_info.get("lon")
            }
        }
    
    def extract_hourly_trends(self, weather_data: Dict) -> Dict:
        """Extrait les tendances des prochaines 24h"""
        raw_data = weather_data.get("raw_data", {})
        hourly_data = raw_data.get("hourly", {})
        
        if not hourly_data:
            return None
        
        # Prendre les 24 premières heures
        times = hourly_data.get("time", [])[:24]
        temps = hourly_data.get("temperature_2m", [])[:24]
        precips = hourly_data.get("precipitation_probability", [])[:24]
        
        if not times or not temps:
            return None
        
        return {
            "type": "hourly_trends",
            "city": weather_data.get("city_info", {}).get("name"),
            "period": "next_24h",
            "temperature_trend": {
                "min": min(temps) if temps else None,
                "max": max(temps) if temps else None,
                "avg": sum(temps) / len(temps) if temps else None
            },
            "precipitation_probability": {
                "max": max(precips) if precips else None,
                "avg": sum(precips) / len(precips) if precips else None
            },
            "data_points": len(times)
        }

def start_consumer_with_filters():
    """Démarre le consumer avec différents filtres"""
    
    consumer = KafkaConsumer(
        'weather',
        bootstrap_servers=['localhost:9092'],
        value_deserializer=lambda m: json.loads(m.decode('utf-8')),
        auto_offset_reset='latest',
        group_id='weather-filter-group'
    )
    
    filter_engine = WeatherDataFilter()
    
    print("🔍 Démarrage du consumer avec filtrage des données")
    print("📊 Filtres actifs:")
    print("   - Alertes température extrême")
    print("   - Alertes vent fort")
    print("   - Prévisions précipitations")
    print("   - Résumés conditions actuelles")
    print("   - Tendances 24h")
    print("\n🎯 En attente de messages Kafka...")
    
    for message in consumer:
        try:
            weather_data = message.value
            city_name = weather_data.get("city_info", {}).get("name", "Unknown")
            
            print(f"\n📨 Message reçu pour {city_name}")
            
            # Appliquer différents filtres
            filters_applied = []
            
            # 1. Alertes température
            temp_alert = filter_engine.filter_temperature_alerts(weather_data)
            if temp_alert:
                print(f"🌡️  ALERTE TEMPÉRATURE: {temp_alert}")
                filters_applied.append("temperature_alert")
            
            # 2. Alertes vent
            wind_alert = filter_engine.filter_wind_alerts(weather_data)
            if wind_alert:
                print(f"💨 ALERTE VENT: {wind_alert}")
                filters_applied.append("wind_alert")
            
            # 3. Alertes précipitations
            precip_alerts = filter_engine.filter_precipitation_forecast(weather_data)
            if precip_alerts:
                print(f"🌧️  ALERTES PRÉCIPITATIONS: {len(precip_alerts)} alertes")
                for alert in precip_alerts[:2]:  # Afficher max 2
                    print(f"   - {alert}")
                filters_applied.append("precipitation_alerts")
            
            # 4. Résumé conditions actuelles
            current_summary = filter_engine.extract_current_summary(weather_data)
            print(f"📋 RÉSUMÉ ACTUEL: {current_summary}")
            filters_applied.append("current_summary")
            
            # 5. Tendances 24h
            hourly_trends = filter_engine.extract_hourly_trends(weather_data)
            if hourly_trends:
                print(f"📈 TENDANCES 24H: {hourly_trends}")
                filters_applied.append("hourly_trends")
            
            filter_engine.processed_count += 1
            filter_engine.filtered_count += len(filters_applied)
            
            print(f"✅ Traité: {filter_engine.processed_count} messages, {filter_engine.filtered_count} filtres appliqués")
            
        except Exception as e:
            print(f"❌ Erreur traitement message: {e}")

if __name__ == "__main__":
    print("🚀 Consumer avec filtrage des données Open Meteo")
    print("💡 Assurez-vous que:")
    print("   1. Kafka est démarré (docker-compose up)")
    print("   2. Le producer envoie des données")
    print("   3. Le topic 'weather' existe")
    
    try:
        start_consumer_with_filters()
    except KeyboardInterrupt:
        print("\n⏹️  Arrêt du consumer")
    except Exception as e:
        print(f"💥 Erreur: {e}")
        print("Vérifiez que Kafka est accessible sur localhost:9092")