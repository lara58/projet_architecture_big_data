from flask import Flask, request, jsonify
import pandas as pd

app = Flask(__name__)

# Charger le fichier CSV
df = pd.read_csv("dummy_data.csv")
df["timestamp"] = pd.to_datetime(df["timestamp"])

@app.route("/health")
def health():
    return jsonify({"status": "ok"})

@app.route("/latest")
def latest():
    city = request.args.get("city")
    n = int(request.args.get("n", 5))
    data = df.copy()
    if city:
        data = data[data["city"].str.lower() == city.lower()]
    result = data.sort_values("timestamp", ascending=False).head(n)
    return result.to_json(orient="records")

@app.route("/daily_avg")
def daily_avg():
    city = request.args.get("city")
    data = df.copy()
    if city:
        data = data[data["city"].str.lower() == city.lower()]
    data["date"] = data["timestamp"].dt.date
    avg = data.groupby("date").agg({"temperature":"mean"}).reset_index()
    return avg.to_json(orient="records")

if __name__ == "__main__":
    app.run(debug=True)
