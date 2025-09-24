#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Traitement quotidien des données météorologiques
Ce script Spark analyse les données météorologiques quotidiennes
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import argparse
from datetime import datetime

def process_weather_data(spark, date_str, input_path, output_path):
    """
    Traite les données météorologiques pour une date spécifique
    
    Args:
        spark: SparkSession
        date_str: Date au format YYYY-MM-DD
        input_path: Chemin des données d'entrée
        output_path: Chemin pour les données de sortie
    """
    print(f"Traitement des données météo pour la date: {date_str}")
    
    # Construire le schéma pour les données météo
    schema = StructType([
        StructField("city", StringType(), True),
        StructField("latitude", DoubleType(), True),
        StructField("longitude", DoubleType(), True),
        StructField("generationtime_ms", DoubleType(), True),
        StructField("timezone", StringType(), True),
        StructField("current_weather", StructType([
            StructField("temperature", DoubleType(), True),
            StructField("windspeed", DoubleType(), True),
            StructField("winddirection", DoubleType(), True),
            StructField("weathercode", IntegerType(), True),
            StructField("time", StringType(), True)
        ]), True),
        StructField("hourly", StructType([
            StructField("temperature_2m", ArrayType(DoubleType()), True),
            StructField("relative_humidity_2m", ArrayType(IntegerType()), True),
            StructField("precipitation", ArrayType(DoubleType()), True),
            StructField("windspeed_10m", ArrayType(DoubleType()), True),
            StructField("time", ArrayType(StringType()), True)
        ]), True)
    ])
    
    # Lire les fichiers JSON du jour spécifié
    try:
        # Format de date pour les fichiers: weather_YYYYMMDD_*.json
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        date_formatted = date_obj.strftime("%Y%m%d")
        file_pattern = f"{input_path}/weather_{date_formatted}_*.json"
        
        # Charger les données
        print(f"Chargement des fichiers: {file_pattern}")
        df = spark.read.schema(schema).json(file_pattern)
        
        # Vérifier si des données ont été chargées
        if df.count() == 0:
            print(f"Aucune donnée trouvée pour la date {date_str}")
            return
        
        # Afficher le schéma et les premières lignes
        print("Schéma des données:")
        df.printSchema()
        
        print("Aperçu des données:")
        df.show(5, truncate=False)
        
        # Extraire les données météo actuelles
        current_df = df.select(
            col("city"),
            col("latitude"),
            col("longitude"),
            col("current_weather.temperature").alias("temperature"),
            col("current_weather.windspeed").alias("windspeed"),
            col("current_weather.winddirection").alias("winddirection"),
            col("current_weather.weathercode").alias("weathercode"),
            col("current_weather.time").alias("time")
        )
        
        # Statistiques par ville
        city_stats = current_df.groupBy("city").agg(
            avg("temperature").alias("avg_temperature"),
            min("temperature").alias("min_temperature"),
            max("temperature").alias("max_temperature"),
            avg("windspeed").alias("avg_windspeed"),
            max("windspeed").alias("max_windspeed")
        )
        
        # Afficher les statistiques
        print("Statistiques par ville:")
        city_stats.show()
        
        # Enregistrer les résultats
        print(f"Enregistrement des résultats dans {output_path}")
        current_df.write.parquet(f"{output_path}/current", mode="overwrite")
        city_stats.write.parquet(f"{output_path}/city_stats", mode="overwrite")
        
        print("Traitement terminé avec succès.")
        return city_stats
    
    except Exception as e:
        print(f"Erreur lors du traitement des données: {str(e)}")
        raise e

def main():
    parser = argparse.ArgumentParser(description="Traitement quotidien des données météorologiques")
    parser.add_argument("--date", required=True, help="Date au format YYYY-MM-DD")
    parser.add_argument("--input-path", required=True, help="Chemin des données d'entrée")
    parser.add_argument("--output-path", required=True, help="Chemin pour les données de sortie")
    
    args = parser.parse_args()
    
    # Créer une session Spark
    spark = SparkSession.builder \
        .appName("DailyWeatherProcessing") \
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY") \
        .getOrCreate()
    
    try:
        # Exécuter le traitement
        process_weather_data(spark, args.date, args.input_path, args.output_path)
    finally:
        # Arrêter la session Spark
        spark.stop()

if __name__ == "__main__":
    main()