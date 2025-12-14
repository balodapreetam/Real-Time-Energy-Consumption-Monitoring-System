from kafka import KafkaProducer
import requests
import json
import time
from datetime import datetime

API_KEY = "ff709caf6e09909d7fb322c7b3420aa6"
CITY = "Delhi"
URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}"

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("✅ Weather Producer Started")

while True:
    response = requests.get(URL)
    data = response.json()

    if "main" not in data:
        print("Error in API response")
        time.sleep(30)
        continue

    weather = {
        "source": "weather",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": float(data["main"]["temp"]),
        "humidity": float(data["main"]["humidity"]),
        "pressure": float(data["main"]["pressure"]),
        "wind_speed": float(data["wind"]["speed"]),
        "city": CITY
    }

    producer.send("energy_consumption", value=weather)
    producer.flush()

    print("✅ Weather Sent:", weather)
    time.sleep(30)