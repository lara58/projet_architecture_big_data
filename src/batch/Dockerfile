FROM bitnami/spark:3.4

USER root
ENV PYTHONUNBUFFERED=1

RUN pip install --upgrade pip

WORKDIR /app

# Copier requirements.txt en premier pour optimiser le cache Docker
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copier le reste du code
COPY . /app

# Exposer le port Spark si nécessaire
EXPOSE 4040
