import streamlit as st
import pandas as pd
import mysql.connector
import matplotlib.pyplot as plt

# -------------------------------
# MySQL CONNECTION
# -------------------------------
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Root@123",   # 👈 put your MySQL password
    database="energy_db"
)

# -------------------------------
# STREAMLIT UI
# -------------------------------
st.set_page_config(page_title="Energy & Weather Dashboard", layout="wide")
st.title("⚡ Energy Consumption & 🌡 Weather Dashboard")

# -------------------------------
# SQL QUERY
# -------------------------------
query = """
SELECT 
    DateTime,
    Global_active_power,
    temperature
FROM energy_weather_data
ORDER BY DateTime DESC
LIMIT 500;
"""

df = pd.read_sql(query, conn)

# Convert DateTime format
df["DateTime"] = pd.to_datetime(df["DateTime"])

# Sort for graph
df = df.sort_values("DateTime")

# -------------------------------
# DISPLAY DATA
# -------------------------------
st.subheader("Latest Energy & Weather Records")
st.dataframe(df.tail(20), use_container_width=True)

# -------------------------------
# GRAPH SECTION
# -------------------------------
st.subheader("📈 Energy vs Temperature Trend")

fig, ax1 = plt.subplots(figsize=(10,5))

# Energy graph
ax1.plot(df["DateTime"], df["Global_active_power"], color="blue", label="Energy Usage (kW)")
ax1.set_xlabel("Time")
ax1.set_ylabel("Energy (kW)", color="blue")
ax1.tick_params(axis="y", labelcolor="blue")

# Temperature graph
ax2 = ax1.twinx()
ax2.plot(df["DateTime"], df["temperature"], color="red", label="Temperature (°C)")
ax2.set_ylabel("Temperature (°C)", color="red")
ax2.tick_params(axis="y", labelcolor="red")

# Title
plt.title("Energy Consumption vs Temperature")

# Show chart
st.pyplot(fig)

# -------------------------------
# STATS SECTION
# -------------------------------
st.subheader("📊 Statistics")

col1, col2, col3 = st.columns(3)

col1.metric("Avg Energy", round(df["Global_active_power"].mean(), 2))
col2.metric("Max Energy", round(df["Global_active_power"].max(), 2))
col3.metric("Avg Temperature", round(df["temperature"].mean(), 2))

conn.close()