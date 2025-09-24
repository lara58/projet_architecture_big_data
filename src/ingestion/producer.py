import json
import time
import requests
from kafka import KafkaProducer

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

while True:
    for city in cities:
        response = requests.get(
            f"https://api.open-meteo.com/v1/forecast?latitude={city['lat']}&longitude={city['lon']}&current_weather=true"
        )
        if response.status_code == 200:
            data = response.json()
            payload = {
                "city": city['name'],
                "temperature": data['current_weather']['temperature'],
                "windspeed": data['current_weather']['windspeed'],
                "time": data['current_weather']['time']
            }
            # Publier dans Kafka
            producer.send('weather', payload)
            print(f"Sent: {payload}")
    time.sleep(60)  # toutes les 60 secondes
