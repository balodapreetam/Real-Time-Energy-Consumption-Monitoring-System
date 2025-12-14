# Real-Time Energy Monitoring System

## 📌 Project Overview
This project is a Real-Time Energy Monitoring System developed as a 7th Semester Academic Project.
It demonstrates how energy consumption data can be streamed, processed, stored, and visualized using
modern data engineering tools.

## 🛠️ Tech Stack
- Python
- Apache Kafka
- Apache Spark (Structured Streaming)
- MySQL
- Streamlit

## ⚙️ System Architecture
Energy Producer → Kafka → Spark Streaming → MySQL → Streamlit Dashboard

## 🔄 How It Works
1. Energy producer streams cleaned household energy data to Kafka.
2. Weather producer streams simulated weather data.
3. Spark consumes Kafka messages in real time.
4. Processed data is stored in MySQL.
5. Streamlit dashboard visualizes energy trends.

## 📊 Features
- Real-time energy data streaming
- Spark-based stream processing
- Database storage using MySQL
- Live dashboard visualization

## 🎓 Academic Details
- Course: B.Tech
- Semester: 7th Semester
- Project Type: Academic Major Project
- Guided by: Mrs. Harshita

## 🚀 Future Scope
- Cloud deployment
- Machine learning for energy prediction
- Alert system for abnormal consumption
