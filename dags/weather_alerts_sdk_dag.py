"""
Pipeline d'alertes météorologiques avec le Task API d'Airflow 3.0
"""
from airflow.decorators import dag, task, task_group
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.kafka.hooks.kafka import KafkaHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
import pendulum
import pandas as pd
import json
import os
from typing import Dict, List, Any, Optional
import logging

# Configuration des seuils d'alerte pour différents types de conditions météo
ALERT_THRESHOLDS = {
    "temperature_high": 35.0,   # °C
    "temperature_low": 0.0,     # °C
    "precipitation": 25.0,      # mm/jour
    "wind_speed": 50.0,         # km/h
}

# Configuration des villes à surveiller
MONITORED_CITIES = ["Paris", "Lyon", "Marseille", "Bordeaux", "Lille", "Strasbourg"]

# Configuration du DAG avec le décorateur @dag
@dag(
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="0 */3 * * *",  # Toutes les 3 heures
    catchup=False,
    tags=['weather', 'alerts', 'monitoring'],
    description='Pipeline de surveillance et d\'alertes météorologiques avec Task SDK'
)
def weather_alerts():
    """
    DAG pour la surveillance des conditions météorologiques et le déclenchement d'alertes.
    Utilise le nouveau Task SDK d'Airflow pour créer un workflow de surveillance et d'alerte.
    """
    
    @task
    def fetch_current_weather() -> Dict[str, Any]:
        """Récupère les données météo actuelles pour les villes surveillées"""
        http_hook = HttpHook(http_conn_id="open_meteo_api", method="GET")
        
        all_weather_data = []
        for city in MONITORED_CITIES:
            try:
                # Coordonnées des villes (à adapter selon vos besoins)
                city_coordinates = {
                    "Paris": {"latitude": 48.8566, "longitude": 2.3522},
                    "Lyon": {"latitude": 45.7640, "longitude": 4.8357},
                    "Marseille": {"latitude": 43.2965, "longitude": 5.3698},
                    "Bordeaux": {"latitude": 44.8378, "longitude": -0.5792},
                    "Lille": {"latitude": 50.6292, "longitude": 3.0573},
                    "Strasbourg": {"latitude": 48.5734, "longitude": 7.7521}
                }
                
                if city not in city_coordinates:
                    logging.warning(f"Coordonnées non disponibles pour {city}, ignoré")
                    continue
                
                coords = city_coordinates[city]
                
                # Construire l'URL avec les paramètres
                endpoint = "v1/forecast"
                params = {
                    "latitude": coords["latitude"],
                    "longitude": coords["longitude"],
                    "current": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
                    "hourly": "temperature_2m,relative_humidity_2m,precipitation,wind_speed_10m",
                    "forecast_days": 1
                }
                
                # Faire la requête API
                response = http_hook.run(endpoint=endpoint, data=params)
                weather_data = response.json()
                
                # Ajouter le nom de la ville aux données
                weather_data["city"] = city
                weather_data["timestamp"] = pendulum.now().to_iso8601_string()
                
                all_weather_data.append(weather_data)
                logging.info(f"Données météo récupérées pour {city}")
                
            except Exception as e:
                logging.error(f"Erreur lors de la récupération des données pour {city}: {str(e)}")
        
        return {"current_weather": all_weather_data}
    
    @task
    def detect_weather_alerts(weather_data: Dict[str, Any]) -> Dict[str, Any]:
        """Analyse les données météo pour détecter les conditions d'alerte"""
        current_data = weather_data["current_weather"]
        alerts = []
        
        for city_data in current_data:
            city = city_data.get("city", "Unknown")
            current = city_data.get("current", {})
            hourly = city_data.get("hourly", {})
            
            # Récupérer les valeurs actuelles
            current_temp = current.get("temperature_2m")
            current_precip = current.get("precipitation")
            current_wind = current.get("wind_speed_10m")
            timestamp = city_data.get("timestamp")
            
            city_alerts = []
            
            # Vérifier les conditions d'alerte
            if current_temp is not None and current_temp > ALERT_THRESHOLDS["temperature_high"]:
                city_alerts.append({
                    "type": "temperature_high",
                    "value": current_temp,
                    "threshold": ALERT_THRESHOLDS["temperature_high"],
                    "message": f"Température élevée à {city}: {current_temp}°C (seuil: {ALERT_THRESHOLDS['temperature_high']}°C)"
                })
            
            if current_temp is not None and current_temp < ALERT_THRESHOLDS["temperature_low"]:
                city_alerts.append({
                    "type": "temperature_low",
                    "value": current_temp,
                    "threshold": ALERT_THRESHOLDS["temperature_low"],
                    "message": f"Température basse à {city}: {current_temp}°C (seuil: {ALERT_THRESHOLDS['temperature_low']}°C)"
                })
            
            if current_precip is not None and current_precip > ALERT_THRESHOLDS["precipitation"]:
                city_alerts.append({
                    "type": "precipitation",
                    "value": current_precip,
                    "threshold": ALERT_THRESHOLDS["precipitation"],
                    "message": f"Fortes précipitations à {city}: {current_precip}mm (seuil: {ALERT_THRESHOLDS['precipitation']}mm)"
                })
            
            if current_wind is not None and current_wind > ALERT_THRESHOLDS["wind_speed"]:
                city_alerts.append({
                    "type": "wind_speed",
                    "value": current_wind,
                    "threshold": ALERT_THRESHOLDS["wind_speed"],
                    "message": f"Vents violents à {city}: {current_wind}km/h (seuil: {ALERT_THRESHOLDS['wind_speed']}km/h)"
                })
            
            # Vérifier aussi les prévisions horaires pour les prochaines 24h
            if hourly and all(k in hourly for k in ["time", "temperature_2m", "precipitation", "wind_speed_10m"]):
                times = hourly["time"]
                temps = hourly["temperature_2m"]
                precips = hourly["precipitation"]
                winds = hourly["wind_speed_10m"]
                
                for i, (time, temp, precip, wind) in enumerate(zip(times, temps, precips, winds)):
                    forecast_alerts = []
                    
                    if temp > ALERT_THRESHOLDS["temperature_high"]:
                        forecast_alerts.append({
                            "type": "forecast_temperature_high",
                            "value": temp,
                            "threshold": ALERT_THRESHOLDS["temperature_high"],
                            "time": time,
                            "message": f"Prévision de température élevée à {city} pour {time}: {temp}°C"
                        })
                    
                    if temp < ALERT_THRESHOLDS["temperature_low"]:
                        forecast_alerts.append({
                            "type": "forecast_temperature_low",
                            "value": temp,
                            "threshold": ALERT_THRESHOLDS["temperature_low"],
                            "time": time,
                            "message": f"Prévision de température basse à {city} pour {time}: {temp}°C"
                        })
                    
                    if precip > ALERT_THRESHOLDS["precipitation"]:
                        forecast_alerts.append({
                            "type": "forecast_precipitation",
                            "value": precip,
                            "threshold": ALERT_THRESHOLDS["precipitation"],
                            "time": time,
                            "message": f"Prévision de fortes précipitations à {city} pour {time}: {precip}mm"
                        })
                    
                    if wind > ALERT_THRESHOLDS["wind_speed"]:
                        forecast_alerts.append({
                            "type": "forecast_wind_speed",
                            "value": wind,
                            "threshold": ALERT_THRESHOLDS["wind_speed"],
                            "time": time,
                            "message": f"Prévision de vents violents à {city} pour {time}: {wind}km/h"
                        })
                    
                    if forecast_alerts:
                        city_alerts.extend(forecast_alerts)
            
            # Ajouter les alertes de cette ville à la liste globale
            if city_alerts:
                alerts.append({
                    "city": city,
                    "timestamp": timestamp,
                    "alerts": city_alerts
                })
        
        logging.info(f"Détection des alertes terminée: {len(alerts)} villes avec des alertes")
        return {"alerts": alerts}
    
    @task
    def save_alerts_to_db(alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Enregistre les alertes dans une base de données SQLite"""
        alerts = alert_data["alerts"]
        
        if not alerts:
            logging.info("Aucune alerte à sauvegarder")
            return alert_data
        
        # Créer le répertoire pour la base de données si nécessaire
        os.makedirs("/data/db", exist_ok=True)
        
        # Connexion à SQLite
        db_path = "/data/db/weather_alerts.db"
        sqlite_hook = SqliteHook(sqlite_conn_id="sqlite_default")
        conn = sqlite_hook.get_conn()
        
        # Créer la table si elle n'existe pas
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS weather_alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            city TEXT NOT NULL,
            timestamp TEXT NOT NULL,
            alert_type TEXT NOT NULL,
            value REAL NOT NULL,
            threshold REAL NOT NULL,
            message TEXT NOT NULL,
            forecast_time TEXT,
            processed INTEGER DEFAULT 0
        )
        ''')
        conn.commit()
        
        # Insérer les alertes
        for city_alert in alerts:
            city = city_alert["city"]
            timestamp = city_alert["timestamp"]
            
            for alert in city_alert["alerts"]:
                forecast_time = alert.get("time", None)
                cursor.execute('''
                INSERT INTO weather_alerts 
                (city, timestamp, alert_type, value, threshold, message, forecast_time)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ''', (
                    city,
                    timestamp,
                    alert["type"],
                    alert["value"],
                    alert["threshold"],
                    alert["message"],
                    forecast_time
                ))
        
        conn.commit()
        logging.info("Alertes sauvegardées dans la base de données")
        return alert_data
    
    @task
    def publish_alerts_to_kafka(alert_data: Dict[str, Any]) -> Dict[str, Any]:
        """Publie les alertes sur un topic Kafka"""
        alerts = alert_data["alerts"]
        
        if not alerts:
            logging.info("Aucune alerte à publier sur Kafka")
            return alert_data
        
        # Utilisation du hook Kafka
        kafka_hook = KafkaHook(kafka_conn_id="kafka_default")
        
        # Produire chaque alerte dans le topic
        for city_alert in alerts:
            city = city_alert["city"]
            
            # Créer un message JSON pour toutes les alertes de cette ville
            message = json.dumps({
                "city": city,
                "timestamp": city_alert["timestamp"],
                "alerts": city_alert["alerts"]
            })
            
            # Publier sur le topic avec la ville comme clé
            kafka_hook.send(
                topic="weather.alerts",
                value=message,
                key=city
            )
        
        logging.info(f"Alertes publiées sur le topic Kafka weather.alerts")
        return alert_data
    
    # Tâche pour les alertes à haute priorité (condition directe, pas de dépendance de tâche)
    def _send_high_priority_alert(alert_msg: str):
        """Envoie une alerte haute priorité via Slack"""
        return SlackWebhookOperator(
            task_id="send_slack_alert",
            http_conn_id="slack_webhook",
            webhook_token="your_slack_token",
            message=f":warning: *ALERTE MÉTÉO HAUTE PRIORITÉ* :warning:\n{alert_msg}",
            channel="#weather-alerts"
        )
    
    @task
    def send_high_priority_notifications(alert_data: Dict[str, Any]) -> None:
        """Analyse les alertes et envoie des notifications pour celles à haute priorité"""
        alerts = alert_data["alerts"]
        
        if not alerts:
            return
        
        # Critères pour les alertes haute priorité
        high_priority_alerts = []
        for city_alert in alerts:
            city = city_alert["city"]
            
            for alert in city_alert["alerts"]:
                # Conditions pour les alertes haute priorité (à adapter selon vos besoins)
                is_high_priority = False
                
                if alert["type"] == "temperature_high" and alert["value"] > ALERT_THRESHOLDS["temperature_high"] + 5:
                    is_high_priority = True
                elif alert["type"] == "precipitation" and alert["value"] > ALERT_THRESHOLDS["precipitation"] * 1.5:
                    is_high_priority = True
                elif alert["type"] == "wind_speed" and alert["value"] > ALERT_THRESHOLDS["wind_speed"] * 1.2:
                    is_high_priority = True
                
                if is_high_priority:
                    high_priority_alerts.append(alert["message"])
        
        if high_priority_alerts:
            # Créer un seul message avec toutes les alertes haute priorité
            alert_msg = "\n".join(high_priority_alerts)
            
            # Appel à la fonction qui renvoie l'opérateur Slack
            slack_alert = _send_high_priority_alert(alert_msg)
            
            # Exécuter l'opérateur
            slack_alert.execute(context=None)
            
            logging.info(f"Alertes haute priorité envoyées: {len(high_priority_alerts)}")
    
    @task_group(group_id="alert_summary")
    def generate_alert_summary():
        """Groupe de tâches pour générer et sauvegarder un résumé des alertes"""
        
        @task
        def create_daily_alert_summary() -> Dict[str, Any]:
            """Crée un résumé quotidien des alertes"""
            # Connexion à SQLite
            sqlite_hook = SqliteHook(sqlite_conn_id="sqlite_default")
            
            # Récupérer les alertes du jour
            today = pendulum.today().format("YYYY-MM-DD")
            query = f"""
            SELECT city, alert_type, COUNT(*) as alert_count
            FROM weather_alerts
            WHERE timestamp LIKE '{today}%'
            GROUP BY city, alert_type
            ORDER BY city, alert_count DESC
            """
            
            df = sqlite_hook.get_pandas_df(query)
            
            if df.empty:
                logging.info("Aucune alerte aujourd'hui pour le résumé")
                return {"summary": None}
            
            # Créer un résumé par ville
            summary = {}
            for city in df["city"].unique():
                city_df = df[df["city"] == city]
                summary[city] = {}
                
                for _, row in city_df.iterrows():
                    alert_type = row["alert_type"]
                    alert_count = row["alert_count"]
                    summary[city][alert_type] = alert_count
            
            # Créer le répertoire pour les rapports si nécessaire
            os.makedirs("/data/reports", exist_ok=True)
            
            # Enregistrer le résumé au format JSON
            summary_path = f"/data/reports/alert_summary_{today}.json"
            with open(summary_path, "w") as f:
                json.dump(summary, f, indent=2)
            
            return {
                "summary": summary,
                "summary_path": summary_path,
                "date": today
            }
        
        @task
        def create_html_report(summary_data: Dict[str, Any]) -> None:
            """Crée un rapport HTML à partir du résumé des alertes"""
            if not summary_data.get("summary"):
                logging.info("Pas de données de résumé pour le rapport HTML")
                return
            
            summary = summary_data["summary"]
            date = summary_data["date"]
            
            # Créer le rapport HTML
            html_content = f"""
            <!DOCTYPE html>
            <html>
            <head>
                <title>Résumé des Alertes Météo - {date}</title>
                <style>
                    body {{ font-family: Arial, sans-serif; margin: 20px; }}
                    .header {{ background-color: #f44336; color: white; padding: 10px; text-align: center; }}
                    .city-section {{ margin-top: 20px; border: 1px solid #ddd; padding: 15px; border-radius: 5px; }}
                    table {{ width: 100%; border-collapse: collapse; }}
                    th, td {{ padding: 8px; text-align: left; border-bottom: 1px solid #ddd; }}
                    th {{ background-color: #f2f2f2; }}
                </style>
            </head>
            <body>
                <div class="header">
                    <h1>Résumé des Alertes Météorologiques</h1>
                    <p>Date: {date}</p>
                </div>
            """
            
            for city, alerts in summary.items():
                html_content += f"""
                <div class="city-section">
                    <h2>{city}</h2>
                    <table>
                        <tr>
                            <th>Type d'Alerte</th>
                            <th>Nombre d'Occurrences</th>
                        </tr>
                """
                
                for alert_type, count in alerts.items():
                    # Formatter le type d'alerte pour l'affichage
                    display_type = alert_type.replace("_", " ").title()
                    html_content += f"""
                        <tr>
                            <td>{display_type}</td>
                            <td>{count}</td>
                        </tr>
                    """
                
                html_content += """
                    </table>
                </div>
                """
            
            html_content += """
            </body>
            </html>
            """
            
            # Enregistrer le rapport HTML
            report_path = f"/data/reports/alert_report_{date}.html"
            with open(report_path, "w") as f:
                f.write(html_content)
            
            logging.info(f"Rapport HTML des alertes créé: {report_path}")
        
        # Définir les dépendances dans le groupe
        summary = create_daily_alert_summary()
        create_html_report(summary)
    
    # Définir la séquence du workflow avec les dépendances
    weather_data = fetch_current_weather()
    alert_data = detect_weather_alerts(weather_data)
    db_saved = save_alerts_to_db(alert_data)
    kafka_published = publish_alerts_to_kafka(alert_data)
    high_priority = send_high_priority_notifications(alert_data)
    
    # Le résumé des alertes est généré à la fin
    [db_saved, kafka_published, high_priority] >> generate_alert_summary()

# Instancier le DAG
weather_alerts_dag = weather_alerts()