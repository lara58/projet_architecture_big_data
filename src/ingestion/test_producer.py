"""
Script de test pour le producer Open Meteo
Teste la collecte complète des données sans Kafka
"""
import sys
import os
sys.path.append(os.path.dirname(__file__))

from api_client import OpenMeteoClient, EUROPEAN_CITIES
import json

def test_single_city():
    """Test pour une seule ville"""
    print("🧪 Test collecte pour une ville (Paris)")
    
    client = OpenMeteoClient()
    data = client.get_complete_weather(48.8566, 2.3522)  # Paris
    
    if data:
        print("✅ Collecte réussie!")
        
        # Informations sur les données collectées
        current = data.get("current_weather", {})
        hourly_count = len(data.get("hourly", {}).get("time", []))
        daily_count = len(data.get("daily", {}).get("time", []))
        data_size = len(json.dumps(data)) / 1024
        
        print(f"🌡️  Température: {current.get('temperature')}°C")
        print(f"💨 Vent: {current.get('windspeed')} km/h")
        print(f"🌤️  Code météo: {current.get('weathercode')}")
        print(f"📊 Données horaires: {hourly_count} points")
        print(f"📅 Données quotidiennes: {daily_count} points")
        print(f"💾 Taille totale: {data_size:.1f}KB")
        
        # Aperçu des paramètres disponibles
        print("\n📋 Paramètres horaires disponibles:")
        hourly_data = data.get("hourly", {})
        for i, param in enumerate(hourly_data.keys()):
            if i < 5:  # Afficher seulement les 5 premiers
                print(f"   - {param}")
            elif i == 5:
                print(f"   - ... et {len(hourly_data) - 5} autres paramètres")
                break
        
        print("\n📋 Paramètres quotidiens disponibles:")
        daily_data = data.get("daily", {})
        for param in daily_data.keys():
            print(f"   - {param}")
        
        return True
    else:
        print("❌ Échec de la collecte")
        return False

def test_multiple_cities():
    """Test pour plusieurs villes"""
    print("\n🧪 Test collecte pour 3 villes européennes")
    
    client = OpenMeteoClient()
    test_cities = EUROPEAN_CITIES[:3]  # Prendre seulement les 3 premières
    
    results = client.get_weather_for_multiple_cities(test_cities)
    
    print(f"\n📊 Résultats: {len(results)}/{len(test_cities)} villes collectées")
    
    total_size = 0
    for result in results:
        city_name = result["city_info"]["name"]
        summary = result["data_summary"]
        
        print(f"\n🌍 {city_name}:")
        print(f"   Température: {summary['current_temperature']}°C")
        print(f"   Vent: {summary['current_windspeed']} km/h")
        print(f"   Points horaires: {summary['hourly_points']}")
        print(f"   Points quotidiens: {summary['daily_points']}")
        print(f"   Taille: {summary['data_size_kb']:.1f}KB")
        
        total_size += summary['data_size_kb']
    
    print(f"\n💾 Taille totale des données: {total_size:.1f}KB")
    return len(results) == len(test_cities)

def test_custom_parameters():
    """Test avec des paramètres personnalisés"""
    print("\n🧪 Test avec paramètres personnalisés")
    
    client = OpenMeteoClient()
    
    # Seulement température et précipitations
    custom_hourly = ["temperature_2m", "precipitation", "windspeed_10m"]
    custom_daily = ["temperature_2m_max", "temperature_2m_min", "precipitation_sum"]
    
    data = client.get_custom_data(
        latitude=48.8566, 
        longitude=2.3522,
        hourly_params=custom_hourly,
        daily_params=custom_daily,
        forecast_days=3
    )
    
    if data:
        print("✅ Collecte personnalisée réussie!")
        
        hourly_count = len(data.get("hourly", {}).get("time", []))
        daily_count = len(data.get("daily", {}).get("time", []))
        data_size = len(json.dumps(data)) / 1024
        
        print(f"📊 Points horaires: {hourly_count}")
        print(f"📅 Points quotidiens: {daily_count}")
        print(f"💾 Taille réduite: {data_size:.1f}KB")
        
        print("\n📋 Paramètres horaires récupérés:")
        hourly_data = data.get("hourly", {})
        for param in hourly_data.keys():
            if param != "time":
                print(f"   - {param}")
        
        return True
    else:
        print("❌ Échec de la collecte personnalisée")
        return False

if __name__ == "__main__":
    print("🚀 Tests du système de collecte Open Meteo")
    print("=" * 50)
    
    # Tests séquentiels
    test1 = test_single_city()
    test2 = test_multiple_cities()
    test3 = test_custom_parameters()
    
    print("\n" + "=" * 50)
    print("📋 Résumé des tests:")
    print(f"   Test ville unique: {'✅' if test1 else '❌'}")
    print(f"   Test multi-villes: {'✅' if test2 else '❌'}")
    print(f"   Test personnalisé: {'✅' if test3 else '❌'}")
    
    if all([test1, test2, test3]):
        print("\n🎉 Tous les tests réussis! Le producer est prêt.")
        print("\n💡 Vous pouvez maintenant:")
        print("   1. Démarrer Kafka avec docker-compose up")
        print("   2. Lancer le producer avec: python src/ingestion/producer.py")
        print("   3. Les données complètes seront envoyées vers Kafka")
        print("   4. Utiliser un consumer pour filtrer les données selon vos besoins")
    else:
        print("\n⚠️  Certains tests ont échoué. Vérifiez votre connexion internet.")