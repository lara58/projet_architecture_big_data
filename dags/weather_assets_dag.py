"""
Pipeline batch météorologique avec assets et lineage utilisant le Task API d'Airflow 3.0
"""
from airflow.datasets import Dataset
from airflow.decorators import dag, task
import pendulum
import json
import os
import pandas as pd
from typing import Dict, Any

# Définir un dataset de données météorologiques
daily_weather_stats_dataset = Dataset("file:///data/processed/daily_weather_stats.parquet")

@task(outlets=[daily_weather_stats_dataset])
def daily_weather_stats():
    """
    Asset qui produit des statistiques quotidiennes météorologiques.
    Cet asset est exécuté quotidiennement et crée un fichier Parquet.
    """
    # Chercher tous les fichiers JSON de données météo de la journée
    import glob
    today = pendulum.today().format("YYYYMMDD")
    json_files = glob.glob(f"/data/raw/weather_{today}_*.json")
    
    if not json_files:
        print("Aucun fichier de données météo trouvé pour aujourd'hui")
        # Créer un DataFrame vide avec les colonnes attendues
        df = pd.DataFrame(columns=["city", "date", "avg_temp", "min_temp", "max_temp", 
                                  "avg_humidity", "precipitation", "avg_windspeed"])
    else:
        all_data = []
        
        # Traiter chaque fichier JSON
        for file_path in json_files:
            with open(file_path, 'r') as f:
                try:
                    data = json.load(f)
                    for city_data in data:
                        city = city_data.get("city", "Unknown")
                        
                        # Extraire les données météo horaires
                        hourly = city_data.get("hourly", {})
                        temps = hourly.get("temperature_2m", [])
                        humidity = hourly.get("relative_humidity_2m", [])
                        precip = hourly.get("precipitation", [])
                        wind = hourly.get("windspeed_10m", [])
                        
                        # Calculer les statistiques
                        if temps:
                            all_data.append({
                                "city": city,
                                "date": today,
                                "avg_temp": sum(temps) / len(temps) if temps else None,
                                "min_temp": min(temps) if temps else None,
                                "max_temp": max(temps) if temps else None,
                                "avg_humidity": sum(humidity) / len(humidity) if humidity else None,
                                "precipitation": sum(precip) if precip else None,
                                "avg_windspeed": sum(wind) / len(wind) if wind else None
                            })
                except Exception as e:
                    print(f"Erreur de traitement du fichier {file_path}: {str(e)}")
        
        # Créer un DataFrame pandas
        df = pd.DataFrame(all_data)
    
    # Créer le répertoire de sortie si nécessaire
    os.makedirs("/data/processed", exist_ok=True)
    
    # Enregistrer au format Parquet
    output_path = "/data/processed/daily_weather_stats.parquet"
    df.to_parquet(output_path, index=False)
    
    print(f"Statistiques quotidiennes enregistrées dans {output_path}")
    
    # Renvoyer des métadonnées sur l'asset (sera utilisé pour la lineage)
    return {
        "rows": len(df),
        "cities": df["city"].nunique() if not df.empty else 0,
        "date": today
    }

# Définir un dataset qui dépend du précédent
monthly_weather_report_dataset = Dataset("file:///data/output/monthly_weather_report.csv")

@task(inlets=[daily_weather_stats_dataset], outlets=[monthly_weather_report_dataset])
def monthly_weather_report(context):
    """
    Asset qui produit un rapport mensuel à partir des statistiques quotidiennes.
    Cet asset dépend de daily_weather_stats et est exécuté mensuellement.
    """
    # Obtenir les informations sur l'exécution précédente de daily_weather_stats
    daily_stats_metadata = context["ti"].xcom_pull(
        dag_id="daily_weather_stats", 
        task_ids="daily_weather_stats"
    )
    
    if daily_stats_metadata:
        print(f"Utilisation des statistiques quotidiennes avec {daily_stats_metadata['rows']} lignes")
    
    # Charger toutes les données quotidiennes du mois
    current_month = pendulum.now().format("YYYYMM")
    stats_path = "/data/processed/daily_weather_stats.parquet"
    
    if os.path.exists(stats_path):
        df = pd.read_parquet(stats_path)
        
        # Filtrer pour le mois en cours
        df["month"] = df["date"].str[:6]
        monthly_data = df[df["month"] == current_month]
        
        # Calculer les agrégations mensuelles par ville
        monthly_agg = monthly_data.groupby("city").agg({
            "avg_temp": "mean",
            "min_temp": "min",
            "max_temp": "max",
            "avg_humidity": "mean",
            "precipitation": "sum",
            "avg_windspeed": "mean"
        }).reset_index()
        
        # Formater pour le rapport
        monthly_agg = monthly_agg.round(2)
        
        # Créer le répertoire de sortie si nécessaire
        os.makedirs("/data/output", exist_ok=True)
        
        # Enregistrer le rapport
        output_path = f"/data/output/monthly_weather_report_{current_month}.csv"
        monthly_agg.to_csv(output_path, index=False)
        
        # Créer un fichier de résumé texte
        summary_path = f"/data/output/monthly_weather_summary_{current_month}.txt"
        with open(summary_path, "w") as f:
            f.write(f"RAPPORT MÉTÉOROLOGIQUE MENSUEL - {current_month}\n")
            f.write("=" * 50 + "\n\n")
            
            for _, row in monthly_agg.iterrows():
                f.write(f"Ville: {row['city']}\n")
                f.write(f"  Température moyenne: {row['avg_temp']}°C\n")
                f.write(f"  Température min/max: {row['min_temp']}°C / {row['max_temp']}°C\n")
                f.write(f"  Humidité moyenne: {row['avg_humidity']}%\n")
                f.write(f"  Précipitations totales: {row['precipitation']} mm\n")
                f.write(f"  Vitesse moyenne du vent: {row['avg_windspeed']} km/h\n\n")
        
        print(f"Rapport mensuel généré: {output_path}")
        print(f"Résumé textuel: {summary_path}")
        
        # Renvoyer des métadonnées pour la lineage
        return {
            "month": current_month,
            "cities": len(monthly_agg),
            "report_path": output_path,
            "summary_path": summary_path
        }
    else:
        print(f"Aucune donnée quotidienne trouvée dans {stats_path}")
        return {
            "month": current_month,
            "cities": 0,
            "error": "No data found"
        }

# Créer le DAG avec les tâches
@dag(
    dag_id='weather_batch_processing',
    schedule='@daily',
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    tags=['weather', 'batch', 'datasets'],
)
def weather_batch_dag():
    daily_stats = daily_weather_stats()
    monthly_report = monthly_weather_report(daily_stats)
    
    return {"daily_stats": daily_stats, "monthly_report": monthly_report}

# Instancier le DAG
weather_batch_processing = weather_batch_dag()