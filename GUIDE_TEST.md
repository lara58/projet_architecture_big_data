# 🌤️ Guide de Test - Collecte et Publication Kafka

## 📋 Ce que font vos scripts

### 1. **Collecte des données via API Open Meteo**
- ✅ Collecte météo pour 5 villes européennes
- ✅ Données complètes : actuelles, horaires, quotidiennes
- ✅ Enrichissement avec métadonnées et qualité des données

### 2. **Publication des événements dans Kafka**
- ✅ Envoi vers le topic `weather-data`
- ✅ Partitioning par ville (clé = city_id)
- ✅ Sérialisation JSON automatique
- ✅ Confirmation de réception

## 🚀 Instructions de Test

### Étape 1 : Démarrer les services Docker
```powershell
# Démarrer tous les services
.\scripts\manage-services.ps1 start

# Ou manuellement
docker-compose up -d
```

### Étape 2 : Vérifier que Kafka fonctionne
```powershell
# Script de vérification complet
python src/ingestion/kafka_checker.py
```

**Ce script va :**
- ✅ Tester la connexion Kafka
- ✅ Créer les topics nécessaires
- ✅ Tester producer/consumer
- ✅ Afficher un rapport de santé

### Étape 3 : Lancer la collecte et publication
```powershell
# Collecteur principal (collecte + publication Kafka)
python src/ingestion/weather_collector.py
```

**Ce script va :**
- 🌐 Collecter les données Open Meteo toutes les 2 minutes
- 📤 Publier chaque message dans Kafka
- 📊 Afficher les statistiques en temps réel
- 🔄 Tourner en continu (Ctrl+C pour arrêter)

### Étape 4 : Vérifier la réception des données
```powershell
# Dans un autre terminal - Consumer de test
python src/ingestion/consumer_with_filters.py
```

## 📊 Monitoring

### Vérifier l'état des services
```powershell
.\scripts\manage-services.ps1 status
```

### Voir les logs
```powershell
.\scripts\manage-services.ps1 logs
```

### Interfaces Web disponibles
- **Spark Master UI** : http://localhost:8080
- **Spark Worker UI** : http://localhost:8081

## 🎯 Structure des Messages Kafka

Chaque message publié contient :

```json
{
  "message_id": "paris_1727123456",
  "timestamp": "2025-09-24T10:30:00Z",
  "city_info": {
    "id": "paris",
    "name": "Paris",
    "lat": 48.8566,
    "lon": 2.3522,
    "country": "France"
  },
  "weather_data": {
    "current_weather": {...},
    "hourly": {...},
    "daily": {...}
  },
  "data_quality": {
    "api_response_time": 0.234,
    "data_size_bytes": 15234,
    "has_current": true,
    "has_hourly": true,
    "has_daily": true
  },
  "metadata": {
    "source": "open-meteo-api",
    "collector_version": "1.0",
    "collection_method": "http_api"
  }
}
```

## 🔧 Configuration

### Topics Kafka créés automatiquement
- `weather-data` (3 partitions) - Données météo principales
- `weather-alerts` (2 partitions) - Alertes météo

### Villes surveillées
- Paris, France
- London, UK  
- Berlin, Germany
- Madrid, Spain
- Rome, Italy

### Paramètres collectés
- **Actuels** : Température, vent, conditions
- **Horaires** : Température, humidité, précipitations, pression, vent
- **Quotidiens** : Min/max températures, précipitations, vent max

## 🛠️ Dépannage

### Kafka ne démarre pas
```powershell
# Redémarrer les services
.\scripts\manage-services.ps1 restart

# Vérifier les logs
docker-compose logs kafka
```

### Erreur de connexion API
- Vérifiez votre connexion internet
- L'API Open Meteo est gratuite et sans clé

### Producer ne se connecte pas
- Vérifiez que Kafka est démarré : `docker-compose ps`
- Testez la connectivité : `python src/ingestion/kafka_checker.py`

## 📈 Résultats Attendus

Après quelques minutes, vous devriez voir :
- ✅ **5 villes** collectées par cycle
- ✅ **Messages Kafka** publiés avec succès
- ✅ **Statistiques** en temps réel
- ✅ **Données enrichies** avec métadonnées

Le système collecte et publie automatiquement toutes les 2 minutes ! 🎉