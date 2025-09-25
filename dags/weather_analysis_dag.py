"""
Pipeline d'analyse et de visualisation météorologique avec le Task API d'Airflow 3.0
"""
from airflow.decorators import dag, task
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
import pendulum
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os
import json
from typing import Dict, List, Any, Optional

# Configuration du DAG avec le décorateur @dag
@dag(
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule="0 2 * * *",  # Tous les jours à 2h du matin
    catchup=False,
    tags=['weather', 'analysis', 'visualization'],
    description='Pipeline d\'analyse et visualisation de données météorologiques avec Task SDK'
)
def weather_analysis():
    """
    DAG pour l'analyse et la visualisation des données météorologiques.
    Utilise le nouveau Task SDK d'Airflow pour créer un workflow d'analyse de données.
    """
    
    @task
    def extract_weather_data() -> Dict[str, Any]:
        """Extrait les données météo des fichiers JSON et les prépare pour l'analyse"""
        import glob
        
        # Récupérer les données des 7 derniers jours
        all_data = []
        end_date = pendulum.today()
        start_date = end_date.subtract(days=7)
        
        current_date = start_date
        while current_date <= end_date:
            date_str = current_date.format("YYYYMMDD")
            json_files = glob.glob(f"/data/raw/weather_{date_str}_*.json")
            
            for file_path in json_files:
                try:
                    with open(file_path, 'r') as f:
                        data = json.load(f)
                        # Ajouter la date aux données
                        for city_data in data:
                            city_data["extraction_date"] = date_str
                        all_data.extend(data)
                except Exception as e:
                    print(f"Erreur lors de la lecture du fichier {file_path}: {str(e)}")
            
            current_date = current_date.add(days=1)
        
        print(f"Données extraites pour {len(all_data)} entrées météo")
        return {"weather_data": all_data}
    
    @task
    def transform_data(extracted_data: Dict[str, Any]) -> Dict[str, Any]:
        """Transforme les données brutes en DataFrames prêts pour l'analyse"""
        raw_data = extracted_data["weather_data"]
        
        if not raw_data:
            print("Aucune donnée météo à transformer")
            return {
                "temperature_df": pd.DataFrame(),
                "precipitation_df": pd.DataFrame(),
                "wind_df": pd.DataFrame()
            }
        
        # Préparer les DataFrames pour chaque type de donnée
        temp_records = []
        precip_records = []
        wind_records = []
        
        for entry in raw_data:
            city = entry.get("city", "Unknown")
            date = entry.get("extraction_date", "Unknown")
            
            # Extraire les données horaires
            hourly = entry.get("hourly", {})
            times = hourly.get("time", [])
            
            # Températures
            temps = hourly.get("temperature_2m", [])
            for i, (time, temp) in enumerate(zip(times, temps)):
                hour = time[-5:]  # Extraire HH:MM
                temp_records.append({
                    "city": city,
                    "date": date,
                    "hour": hour,
                    "temperature": temp
                })
            
            # Précipitations
            precips = hourly.get("precipitation", [])
            for i, (time, precip) in enumerate(zip(times, precips)):
                hour = time[-5:]
                precip_records.append({
                    "city": city,
                    "date": date,
                    "hour": hour,
                    "precipitation": precip
                })
            
            # Vent
            winds = hourly.get("windspeed_10m", [])
            wind_dirs = hourly.get("winddirection_10m", [])
            for i, (time, wind, direction) in enumerate(zip(times, winds, wind_dirs)):
                hour = time[-5:]
                wind_records.append({
                    "city": city,
                    "date": date,
                    "hour": hour,
                    "wind_speed": wind,
                    "wind_direction": direction
                })
        
        # Créer les DataFrames
        temp_df = pd.DataFrame(temp_records)
        precip_df = pd.DataFrame(precip_records)
        wind_df = pd.DataFrame(wind_records)
        
        print(f"Transformation terminée: {len(temp_df)} enregistrements de température, "
              f"{len(precip_df)} enregistrements de précipitations, "
              f"{len(wind_df)} enregistrements de vent")
        
        return {
            "temperature_df": temp_df,
            "precipitation_df": precip_df,
            "wind_df": wind_df
        }
    
    @task
    def save_to_database(transformed_data: Dict[str, Any]) -> None:
        """Sauvegarde les données transformées dans une base SQLite"""
        temp_df = transformed_data["temperature_df"]
        precip_df = transformed_data["precipitation_df"]
        wind_df = transformed_data["wind_df"]
        
        if temp_df.empty or precip_df.empty or wind_df.empty:
            print("Pas de données à sauvegarder")
            return
        
        # Créer le répertoire pour la base de données si nécessaire
        os.makedirs("/data/db", exist_ok=True)
        
        # Connexion à SQLite
        db_path = "/data/db/weather_analytics.db"
        sqlite_hook = SqliteHook(sqlite_conn_id="sqlite_default")
        
        # Sauvegarder chaque DataFrame dans sa propre table
        conn = sqlite_hook.get_conn()
        
        temp_df.to_sql("temperature", conn, if_exists="replace", index=False)
        precip_df.to_sql("precipitation", conn, if_exists="replace", index=False)
        wind_df.to_sql("wind", conn, if_exists="replace", index=False)
        
        print(f"Données sauvegardées dans la base SQLite {db_path}")
    
    @task
    def generate_temperature_visualizations(transformed_data: Dict[str, Any]) -> str:
        """Génère des visualisations pour les données de température"""
        temp_df = transformed_data["temperature_df"]
        
        if temp_df.empty:
            print("Pas de données de température pour les visualisations")
            return ""
        
        # Créer le répertoire pour les visualisations
        output_dir = "/data/visualizations"
        os.makedirs(output_dir, exist_ok=True)
        
        # Définir le style
        plt.style.use('ggplot')
        sns.set(style="whitegrid")
        
        # 1. Graphique de température moyenne par ville
        plt.figure(figsize=(12, 6))
        city_avg_temp = temp_df.groupby('city')['temperature'].mean().sort_values(ascending=False)
        ax = sns.barplot(x=city_avg_temp.index, y=city_avg_temp.values)
        plt.title('Température moyenne par ville (7 derniers jours)')
        plt.xlabel('Ville')
        plt.ylabel('Température (°C)')
        plt.xticks(rotation=45)
        plt.tight_layout()
        temp_by_city_path = f"{output_dir}/temperature_by_city.png"
        plt.savefig(temp_by_city_path)
        plt.close()
        
        # 2. Graphique d'évolution de la température par jour
        plt.figure(figsize=(14, 7))
        # Convertir la date en datetime pour un meilleur formatage
        temp_df['datetime'] = pd.to_datetime(temp_df['date'] + ' ' + temp_df['hour'])
        
        # Sélectionner quelques villes pour la lisibilité
        top_cities = city_avg_temp.head(5).index.tolist()
        for city in top_cities:
            city_data = temp_df[temp_df['city'] == city]
            city_data = city_data.sort_values('datetime')
            plt.plot(city_data['datetime'], city_data['temperature'], label=city)
        
        plt.title('Évolution de la température (7 derniers jours)')
        plt.xlabel('Date/Heure')
        plt.ylabel('Température (°C)')
        plt.legend()
        plt.grid(True)
        plt.tight_layout()
        temp_evolution_path = f"{output_dir}/temperature_evolution.png"
        plt.savefig(temp_evolution_path)
        plt.close()
        
        # 3. Heatmap des températures
        pivot_temp = temp_df.pivot_table(
            index='date', 
            columns='city', 
            values='temperature',
            aggfunc='mean'
        )
        
        plt.figure(figsize=(12, 8))
        sns.heatmap(pivot_temp, annot=True, fmt=".1f", cmap="YlOrRd")
        plt.title('Heatmap des températures moyennes journalières par ville')
        plt.tight_layout()
        temp_heatmap_path = f"{output_dir}/temperature_heatmap.png"
        plt.savefig(temp_heatmap_path)
        plt.close()
        
        print(f"Visualisations de température générées dans {output_dir}")
        return output_dir
    
    @task
    def generate_precipitation_visualizations(transformed_data: Dict[str, Any]) -> str:
        """Génère des visualisations pour les données de précipitations"""
        precip_df = transformed_data["precipitation_df"]
        
        if precip_df.empty:
            print("Pas de données de précipitations pour les visualisations")
            return ""
        
        # Créer le répertoire pour les visualisations
        output_dir = "/data/visualizations"
        os.makedirs(output_dir, exist_ok=True)
        
        # Définir le style
        plt.style.use('ggplot')
        sns.set(style="whitegrid")
        
        # 1. Précipitations totales par ville
        plt.figure(figsize=(12, 6))
        city_total_precip = precip_df.groupby('city')['precipitation'].sum().sort_values(ascending=False)
        ax = sns.barplot(x=city_total_precip.index, y=city_total_precip.values)
        plt.title('Précipitations totales par ville (7 derniers jours)')
        plt.xlabel('Ville')
        plt.ylabel('Précipitations (mm)')
        plt.xticks(rotation=45)
        plt.tight_layout()
        precip_by_city_path = f"{output_dir}/precipitation_by_city.png"
        plt.savefig(precip_by_city_path)
        plt.close()
        
        # 2. Précipitations quotidiennes
        daily_precip = precip_df.groupby(['date', 'city'])['precipitation'].sum().reset_index()
        pivot_daily = daily_precip.pivot(index='date', columns='city', values='precipitation')
        
        plt.figure(figsize=(14, 7))
        pivot_daily.plot(kind='bar', figsize=(14, 7))
        plt.title('Précipitations quotidiennes par ville')
        plt.xlabel('Date')
        plt.ylabel('Précipitations (mm)')
        plt.legend(title='Ville')
        plt.grid(True, axis='y')
        plt.tight_layout()
        daily_precip_path = f"{output_dir}/daily_precipitation.png"
        plt.savefig(daily_precip_path)
        plt.close()
        
        print(f"Visualisations de précipitations générées dans {output_dir}")
        return output_dir
    
    @task
    def generate_wind_visualizations(transformed_data: Dict[str, Any]) -> str:
        """Génère des visualisations pour les données de vent"""
        wind_df = transformed_data["wind_df"]
        
        if wind_df.empty:
            print("Pas de données de vent pour les visualisations")
            return ""
        
        # Créer le répertoire pour les visualisations
        output_dir = "/data/visualizations"
        os.makedirs(output_dir, exist_ok=True)
        
        # Définir le style
        plt.style.use('ggplot')
        sns.set(style="whitegrid")
        
        # 1. Vitesse moyenne du vent par ville
        plt.figure(figsize=(12, 6))
        city_avg_wind = wind_df.groupby('city')['wind_speed'].mean().sort_values(ascending=False)
        ax = sns.barplot(x=city_avg_wind.index, y=city_avg_wind.values)
        plt.title('Vitesse moyenne du vent par ville (7 derniers jours)')
        plt.xlabel('Ville')
        plt.ylabel('Vitesse du vent (km/h)')
        plt.xticks(rotation=45)
        plt.tight_layout()
        wind_by_city_path = f"{output_dir}/wind_by_city.png"
        plt.savefig(wind_by_city_path)
        plt.close()
        
        # 2. Rose des vents pour quelques villes
        from windrose import WindroseAxes
        
        top_cities = city_avg_wind.head(4).index.tolist()
        
        fig = plt.figure(figsize=(15, 15))
        for i, city in enumerate(top_cities, 1):
            city_wind = wind_df[wind_df['city'] == city]
            
            ax = fig.add_subplot(2, 2, i, projection='windrose')
            ax.bar(city_wind['wind_direction'], city_wind['wind_speed'], 
                   normed=True, opening=0.8, edgecolor='white')
            ax.set_legend(title='Vitesse (km/h)')
            ax.set_title(f'Rose des vents - {city}')
        
        plt.tight_layout()
        windrose_path = f"{output_dir}/wind_rose.png"
        plt.savefig(windrose_path)
        plt.close()
        
        print(f"Visualisations de vent générées dans {output_dir}")
        return output_dir
    
    @task
    def create_html_report(
        temp_visuals: str, 
        precip_visuals: str, 
        wind_visuals: str
    ) -> None:
        """Crée un rapport HTML avec toutes les visualisations générées"""
        if not temp_visuals and not precip_visuals and not wind_visuals:
            print("Aucune visualisation disponible pour le rapport")
            return
        
        output_dir = "/data/reports"
        os.makedirs(output_dir, exist_ok=True)
        
        report_date = pendulum.now().format("YYYY-MM-DD")
        report_path = f"{output_dir}/weather_report_{report_date}.html"
        
        html_content = f"""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Rapport Météorologique - {report_date}</title>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .header {{ background-color: #4CAF50; color: white; padding: 10px; text-align: center; }}
                .section {{ margin-top: 30px; border: 1px solid #ddd; padding: 20px; border-radius: 5px; }}
                img {{ max-width: 100%; height: auto; margin: 10px 0; }}
                .image-container {{ display: flex; flex-wrap: wrap; justify-content: center; }}
                .image-container img {{ margin: 10px; box-shadow: 0 4px 8px rgba(0,0,0,0.1); }}
            </style>
        </head>
        <body>
            <div class="header">
                <h1>Rapport Météorologique</h1>
                <p>Données des 7 derniers jours - Généré le {report_date}</p>
            </div>
            
            <div class="section">
                <h2>Analyse des Températures</h2>
                <div class="image-container">
                    <img src="../visualizations/temperature_by_city.png" alt="Températures moyennes par ville">
                    <img src="../visualizations/temperature_evolution.png" alt="Évolution des températures">
                    <img src="../visualizations/temperature_heatmap.png" alt="Heatmap des températures">
                </div>
            </div>
            
            <div class="section">
                <h2>Analyse des Précipitations</h2>
                <div class="image-container">
                    <img src="../visualizations/precipitation_by_city.png" alt="Précipitations totales par ville">
                    <img src="../visualizations/daily_precipitation.png" alt="Précipitations quotidiennes">
                </div>
            </div>
            
            <div class="section">
                <h2>Analyse du Vent</h2>
                <div class="image-container">
                    <img src="../visualizations/wind_by_city.png" alt="Vitesse moyenne du vent par ville">
                    <img src="../visualizations/wind_rose.png" alt="Rose des vents">
                </div>
            </div>
        </body>
        </html>
        """
        
        with open(report_path, "w") as f:
            f.write(html_content)
        
        print(f"Rapport HTML généré: {report_path}")
    
    # Définir la séquence du workflow avec les dépendances
    extracted_data = extract_weather_data()
    transformed_data = transform_data(extracted_data)
    save_to_db = save_to_database(transformed_data)
    
    temp_visuals = generate_temperature_visualizations(transformed_data)
    precip_visuals = generate_precipitation_visualizations(transformed_data)
    wind_visuals = generate_wind_visualizations(transformed_data)
    
    create_html_report(temp_visuals, precip_visuals, wind_visuals)

# Instancier le DAG
weather_analysis_dag = weather_analysis()