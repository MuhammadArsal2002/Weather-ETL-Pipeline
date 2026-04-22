# Weather-ETL-Pipeline

**Overview:**
A real-time weather data pipeline that automatically fetches 14-day hourly forecast data every hour, processes and cleans it using Apache Spark, stores it in MySQL, and visualizes it in a live Power BI dashboard.
📊 Dashboard Preview
<img width="1487" height="838" alt="Screenshot 2026-04-22 062508" src="https://github.com/user-attachments/assets/1cdf9d01-25d8-4e0c-90cc-201e5d023bbc" />


**View live dashboard**

https://app.powerbi.com/view?r=eyJrIjoiMzcyMGQxZjEtM2FhNy00OTAxLWFkNGUtODM4Y2RkOWNiZmI3IiwidCI6ImVkY2ZiMTUwLTMwOWEtNGIwOS04YzM4LWMyZmVhOGRjNzA4MSIsImMiOjl9


**The dashboard includes:**

KPI Cards — Avg Temp, Humidity, Cloud Cover, Total Rain, Min/Max Temp, Max Wind Gust, Dew Point
Temperature Trend — Temperature vs Apparent Temperature over time
Soil Moisture by Time — Hourly soil moisture levels
Wind Speed — Multi-level wind speed (10m, 80m, 120m, 180m)
Cloud Cover — Low, Mid, High cloud cover breakdown
Visibility (km) by Time — Hourly visibility trend
Rain by Time — Hourly rainfall amounts
Probability of Rain by Time — Forecast rain probability
Filters — Select Date and Select Time slicers

**Architecture:**
Open-Meteo API → Apache Kafka → Apache Spark Streaming → MySQL → Power BI

**Tech Stack:**
**Layer                        Technology** 
Data Source                  Open-Meteo API (free, no key needed)
Ingestion                    Python, Apache Kafka
Processing                   Apache Spark Structured Streaming
Storage                      MySQL
Visualization                Power BI
Language                     Python 3


**
Project Structure:**
weather-etl-pipeline/
│
├── etl/
│   └── Extrect.py           ← Main ETL script (API → Kafka → Spark → MySQL)
│
├── dashboard/
│   ├── weather_dashboard.pbix
│   └── screenshots/
│       └── overview.png
│
├── .gitignore
└── README.md


**How to Run Prerequisites**

Python 3.x
Apache Kafka running on localhost:9092
Apache Spark installed
MySQL running on localhost:3307
JDK 17

**1. Install Python dependencies**

bashpip install requests kafka-python pyspark pymysql pandas

**2. Start Kafka**

**Start Zookeeper**

bin\windows\zookeeper-server-start.bat config\zookeeper.properties

**Start Kafka broker**

bin\windows\kafka-server-start.bat config\server.properties

**3. Set environment variables**

Create a .env file or set these variables:
MYSQL_HOST= HOST
MYSQL_PORT= 3307
MYSQL_DB= weather_db
MYSQL_USER=  root
MYSQL_PASS=  your_password

**4. Run the ETL pipeline**

bashpython etl/EXTRACT.py

**The pipeline will:**

Immediately fetch weather data from Open-Meteo API
Send records to Kafka topic weather_raw_data
Spark reads, cleans, and writes to MySQL every hour
Repeat automatically every hour ♻️

**Data Pipeline Details**
**Data Source:**

API: Open-Meteo — free, no API key required
Location: Berlin, Germany (lat: 52.52, lon: 13.41)
Forecast window: Today + 14 days
Refresh rate: Every 1 hour

**Fields collected (40+ parameters):**
Temperature, Humidity, Rain, Snowfall, Wind Speed (10m/80m/120m/180m), Wind Direction, Cloud Cover (Low/Mid/High), Visibility, Soil Temperature, Soil Moisture, Pressure, Dew Point, Evapotranspiration, and more.


**Data Cleaning (Spark):**

Convert timestamps to proper format
Drop rows with NULL critical fields
Remove duplicates by timestamp
Sanity range checks (e.g. temperature between -80°C and 60°C)
Fill non-critical NULLs with 0.0

**Notes:**

Spark and the Kafka producer run concurrently using Python threading
INSERT IGNORE is used in MySQL to prevent duplicate records
Checkpointing is enabled for Spark fault tolerance
