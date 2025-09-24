"""
Consumer Kafka pour stocker les données météo dans PostgreSQL
"""
import json
import time
import psycopg2
from kafka import KafkaConsumer
from psycopg2.extras import RealDictCursor
from datetime import datetime

# Configuration Kafka
KAFKA_SERVER = "kafka:29092"
TOPIC = "weather"

# Configuration PostgreSQL
POSTGRES_CONFIG = {
    'host': 'postgres',
    'port': 5432,
    'database': 'weatherdb',
    'user': 'admin',
    'password': 'admin'
}

def create_connection():
    """Crée une connexion à PostgreSQL"""
    try:
        conn = psycopg2.connect(**POSTGRES_CONFIG)
        return conn
    except Exception as e:
        print(f"Erreur de connexion à PostgreSQL: {e}")
        return None

def create_table_if_not_exists(conn):
    """Crée la table weather_data si elle n'existe pas"""
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS weather_data (
        id SERIAL PRIMARY KEY,
        city VARCHAR(100) NOT NULL,
        timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
        temperature_2m DECIMAL(5,2),
        relative_humidity_2m INTEGER,
        apparent_temperature DECIMAL(5,2),
        is_day INTEGER,
        precipitation DECIMAL(5,2),
        rain DECIMAL(5,2),
        showers DECIMAL(5,2),
        snowfall DECIMAL(5,2),
        weather_code INTEGER,
        cloud_cover INTEGER,
        pressure_msl DECIMAL(7,2),
        surface_pressure DECIMAL(7,2),
        wind_speed_10m DECIMAL(5,2),
        wind_direction_10m INTEGER,
        wind_gusts_10m DECIMAL(5,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    
    CREATE INDEX IF NOT EXISTS idx_weather_city_timestamp 
    ON weather_data (city, timestamp);
    
    CREATE INDEX IF NOT EXISTS idx_weather_timestamp 
    ON weather_data (timestamp);
    """
    
    try:
        cursor = conn.cursor()
        cursor.execute(create_table_sql)
        conn.commit()
        cursor.close()
        print("Table weather_data créée ou vérifiée avec succès")
    except Exception as e:
        print(f"Erreur lors de la création de la table: {e}")
        conn.rollback()

def insert_weather_data(conn, data):
    """Insert les données météo dans PostgreSQL"""
    insert_sql = """
    INSERT INTO weather_data (
        city, timestamp, temperature_2m, relative_humidity_2m, apparent_temperature,
        is_day, precipitation, rain, showers, snowfall, weather_code,
        cloud_cover, pressure_msl, surface_pressure, wind_speed_10m,
        wind_direction_10m, wind_gusts_10m
    ) VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    """
    
    try:
        cursor = conn.cursor()
        
        # Conversion du timestamp
        timestamp_str = data.get('timestamp', '')
        if timestamp_str:
            # Convertir le timestamp ISO en datetime
            timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        else:
            timestamp = datetime.utcnow()
        
        # Préparer les données pour l'insertion
        values = (
            data.get('city'),
            timestamp,
            data.get('temperature_2m'),
            data.get('relative_humidity_2m'),
            data.get('apparent_temperature'),
            data.get('is_day'),
            data.get('precipitation'),
            data.get('rain'),
            data.get('showers'),
            data.get('snowfall'),
            data.get('weather_code'),
            data.get('cloud_cover'),
            data.get('pressure_msl'),
            data.get('surface_pressure'),
            data.get('wind_speed_10m'),
            data.get('wind_direction_10m'),
            data.get('wind_gusts_10m')
        )
        
        cursor.execute(insert_sql, values)
        conn.commit()
        cursor.close()
        
        print(f"Données insérées pour {data.get('city')} à {timestamp}")
        return True
        
    except Exception as e:
        print(f"Erreur lors de l'insertion: {e}")
        conn.rollback()
        return False

def main():
    """Fonction principale du consumer"""
    print("Démarrage du consumer Kafka vers PostgreSQL...")
    
    # Attendre que PostgreSQL soit prêt
    print("Attente de PostgreSQL...")
    time.sleep(10)
    
    # Connexion à PostgreSQL
    conn = create_connection()
    if not conn:
        print("Impossible de se connecter à PostgreSQL. Arrêt du consumer.")
        return
    
    # Créer la table si nécessaire
    create_table_if_not_exists(conn)
    
    # Configuration du consumer Kafka
    try:
        consumer = KafkaConsumer(
            TOPIC,
            bootstrap_servers=[KAFKA_SERVER],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda m: m.decode('utf-8') if m else None,
            group_id='postgres_consumer_group',
            auto_offset_reset='latest',  # Commence par les nouveaux messages
            enable_auto_commit=True
        )
        
        print(f"Consumer connecté au topic '{TOPIC}' sur {KAFKA_SERVER}")
        print("En attente de messages...")
        
        # Consommation des messages
        for message in consumer:
            try:
                data = message.value
                print(f"Message reçu: {data.get('city')} - {data.get('timestamp')}")
                
                # Insérer en base
                success = insert_weather_data(conn, data)
                
                if success:
                    print(f"✅ Données stockées: {data.get('city')}")
                else:
                    print(f"❌ Échec stockage: {data.get('city')}")
                    
            except Exception as e:
                print(f"Erreur lors du traitement du message: {e}")
                
    except Exception as e:
        print(f"Erreur du consumer Kafka: {e}")
    
    finally:
        if conn:
            conn.close()
            print("Connexion PostgreSQL fermée")

if __name__ == "__main__":
    main()