from kafka import KafkaProducer
import pandas as pd
import json
import time

df = pd.read_csv("household_power_consumption_cleaned.csv")

producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

print("\n📡 Producer 1 Connected — Streaming Energy Data...\n")

for index, row in df.iterrows():

    message = {
        "source": "energy",   

        "DateTime": row["DateTime"],
        "Global_active_power": row["Global_active_power"],
        "Global_reactive_power": row["Global_reactive_power"],
        "Voltage": row["Voltage"],
        "Global_intensity": row["Global_intensity"],
        "Sub_metering_1": row["Sub_metering_1"],
        "Sub_metering_2": row["Sub_metering_2"],
        "Sub_metering_3": row["Sub_metering_3"],

        # ✅ Weather fields must exist but NULL for energy
        "timestamp": None,
        "temperature": None,
        "humidity": None,
        "pressure": None,
        "wind_speed": None,
        "city": None
    }

    producer.send("energy_consumption", message)

    if index % 10000 == 0:
        print(f"✅ Sent record #{index}")

    time.sleep(0.5)

producer.flush()
producer.close()
print("✅ Energy Streaming Completed")
 

  

