import os
import requests
import json
import time
import logging
import sys
from pyspark.sql import SparkSession

# Configure logging vers stdout (non-buffered si PYTHONUNBUFFERED=1 ou via flush si nécessaire)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("weather_batch")

# -------------------------------
# 1. Télécharger les données API (configurable via env vars)
# -------------------------------
url = os.getenv("OPENMETEO_ARCHIVE_URL", "https://archive-api.open-meteo.com/v1/archive")

# Paramètres configurables (par défaut : latitude=52.52, longitude=13.41, plage fournie)
params = {
    "latitude": os.getenv("LATITUDE", "52.52"),
    "longitude": os.getenv("LONGITUDE", "13.41"),
    "start_date": os.getenv("START_DATE", "2025-09-09"),
    "end_date": os.getenv("END_DATE", "2025-09-23"),
    "daily": os.getenv(
        "DAILY_FIELDS",
        "weather_code,temperature_2m_mean,temperature_2m_max,temperature_2m_min,apparent_temperature_mean,apparent_temperature_max,apparent_temperature_min,daylight_duration,sunshine_duration,sunset,sunrise,precipitation_sum,rain_sum,snowfall_sum,precipitation_hours,wind_speed_10m_max,shortwave_radiation_sum,wind_direction_10m_dominant,wind_gusts_10m_max,et0_fao_evapotranspiration",
    ),
    "hourly": os.getenv("HOURLY_FIELDS", "temperature_2m"),
}

logger.info("📡 Récupération des données depuis %s", url)
logger.info("Paramètres: latitude=%s longitude=%s start_date=%s end_date=%s", params["latitude"], params["longitude"], params["start_date"], params["end_date"])


def fetch_with_retries(url, params, max_retries=8, backoff_base=30):
    """Effectue une requête GET avec retry/backoff sur 429 et erreurs transitoires."""
    attempt = 0
    while True:
        attempt += 1
        try:
            resp = requests.get(url, params=params, timeout=60)
        except Exception as e:
            if attempt >= max_retries:
                logger.exception("Erreur réseau finale lors de la requête")
                raise
            wait = backoff_base * attempt
            logger.warning("Erreur réseau: %s. Retry dans %ss (attempt %s/%s)", e, wait, attempt, max_retries)
            time.sleep(wait)
            continue

        if resp.status_code == 200:
            logger.info("✅ Données récupérées (status 200)")
            try:
                j = resp.json()
                # log basic size/keys info to help debugging when logs are viewed
                if isinstance(j, dict):
                    keys = list(j.keys())
                    logger.info("Response JSON keys: %s", keys)
                    # log number of daily/hourly entries if present
                    if "daily" in j and isinstance(j["daily"], dict):
                        for k, v in j["daily"].items():
                            if isinstance(v, list):
                                logger.info("daily.%s -> %d entries", k, len(v))
                    if "hourly" in j and isinstance(j["hourly"], dict):
                        for k, v in j["hourly"].items():
                            if isinstance(v, list):
                                logger.info("hourly.%s -> %d entries", k, len(v))
                return j
            except Exception as e:
                logger.exception("Erreur lors du parsing JSON: %s", e)
                raise

        if resp.status_code == 429:
            # Rate limited — backoff and retry
            if attempt >= max_retries:
                logger.error("Erreur API 429 persistante, abandon")
                raise Exception(f"Erreur API {resp.status_code}: {resp.text}")
            # Try exponential-ish backoff
            wait = backoff_base * attempt
            logger.warning("429 Rate limit. Attente %ss avant retry (attempt %s/%s)", wait, attempt, max_retries)
            time.sleep(wait)
            continue

        # Other HTTP errors: log and raise immediately
        logger.error("Erreur API %s: %s", resp.status_code, resp.text)
        raise Exception(f"Erreur API {resp.status_code}: {resp.text}")


data = fetch_with_retries(url, params)

# -------------------------------
# 2. Créer session Spark
# -------------------------------
logger.info("Démarrage de la session Spark")
spark = SparkSession.builder \
    .appName("HistoricalWeatherToHDFS") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

# Convert JSON en RDD puis DataFrame
rdd = spark.sparkContext.parallelize([data])
df = spark.read.json(rdd)

logger.info("📊 Aperçu du DataFrame")
df.printSchema()
df.show(2, truncate=False)

# -------------------------------
# 3. Sauvegarde dans HDFS
# -------------------------------
output_path = "hdfs://hdfs-namenode:9000/tmp/historical-weather"

logger.info("Écriture des données dans HDFS: %s", output_path)
df.write.mode("overwrite").json(output_path)

logger.info("✅ Données stockées dans HDFS : %s", output_path)

spark.stop()