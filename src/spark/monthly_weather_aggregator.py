#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Agrégation mensuelle des données météorologiques
Ce script Spark calcule des agrégations mensuelles des données météo
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import argparse
import os
from datetime import datetime

def aggregate_monthly_data(spark, month_str, input_path, output_path):
    """
    Agrège les données météorologiques pour un mois spécifique
    
    Args:
        spark: SparkSession
        month_str: Mois au format YYYY-MM
        input_path: Chemin des données d'entrée
        output_path: Chemin pour les données de sortie
    """
    print(f"Agrégation des données météo pour le mois: {month_str}")
    
    try:
        # Déterminer les jours du mois
        year, month = map(int, month_str.split('-'))
        
        # Trouver tous les dossiers quotidiens pour ce mois
        daily_dirs = []
        for day in range(1, 32):
            try:
                date_str = f"{year}-{month:02d}-{day:02d}"
                date_formatted = date_str.replace('-', '')
                daily_path = f"{input_path}/daily_{date_formatted}"
                
                if os.path.exists(daily_path):
                    daily_dirs.append(daily_path)
            except:
                continue
        
        if not daily_dirs:
            print(f"Aucune donnée trouvée pour le mois {month_str}")
            return
        
        print(f"Traitement de {len(daily_dirs)} jours de données")
        
        # Charger toutes les statistiques quotidiennes par ville
        city_stats_paths = [f"{d}/city_stats" for d in daily_dirs]
        city_stats_df = spark.read.parquet(*city_stats_paths)
        
        # Calculer les agrégations mensuelles par ville
        monthly_city_stats = city_stats_df.groupBy("city").agg(
            avg("avg_temperature").alias("monthly_avg_temperature"),
            min("min_temperature").alias("monthly_min_temperature"),
            max("max_temperature").alias("monthly_max_temperature"),
            avg("avg_windspeed").alias("monthly_avg_windspeed"),
            max("max_windspeed").alias("monthly_max_windspeed")
        )
        
        # Afficher les résultats
        print("Statistiques mensuelles par ville:")
        monthly_city_stats.show()
        
        # Enregistrer les résultats
        print(f"Enregistrement des résultats dans {output_path}")
        monthly_city_stats.write.parquet(f"{output_path}/city_monthly_stats", mode="overwrite")
        
        # Créer un résumé au format CSV pour faciliter l'accès
        monthly_city_stats.write.option("header", "true").csv(f"{output_path}/city_monthly_summary", mode="overwrite")
        
        print("Agrégation mensuelle terminée avec succès.")
        return monthly_city_stats
    
    except Exception as e:
        print(f"Erreur lors de l'agrégation mensuelle: {str(e)}")
        raise e

def main():
    parser = argparse.ArgumentParser(description="Agrégation mensuelle des données météorologiques")
    parser.add_argument("--month", required=True, help="Mois au format YYYY-MM")
    parser.add_argument("--input-path", required=True, help="Chemin des données d'entrée")
    parser.add_argument("--output-path", required=True, help="Chemin pour les données de sortie")
    
    args = parser.parse_args()
    
    # Créer une session Spark
    spark = SparkSession.builder \
        .appName("MonthlyWeatherAggregation") \
        .getOrCreate()
    
    try:
        # Exécuter l'agrégation
        aggregate_monthly_data(spark, args.month, args.input_path, args.output_path)
    finally:
        # Arrêter la session Spark
        spark.stop()

if __name__ == "__main__":
    main()