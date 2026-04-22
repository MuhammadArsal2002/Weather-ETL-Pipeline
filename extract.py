import requests 
import pandas as pd
from kafka import KafkaProducer
import json
from pyspark.sql import SparkSession
import time 
from datetime import date, timedelta
import threading
import os



def run_producer():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

    while True:

        today = date.today()
        end = today + timedelta(days=14)  

        url = (
            f'https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41'
            f'&hourly=temperature_2m,rain,relative_humidity_2m,apparent_temperature,'
            f'snowfall,snow_depth,pressure_msl,surface_pressure,cloud_cover,visibility,'
            f'wind_speed_10m,wind_speed_80m,wind_speed_120m,wind_direction_10m,'
            f'wind_direction_80m,wind_direction_120m,temperature_80m,temperature_120m,'
            f'temperature_180m,soil_temperature_0cm,soil_temperature_6cm,'
            f'soil_temperature_18cm,soil_temperature_54cm,soil_moisture_0_to_1cm,'
            f'soil_moisture_1_to_3cm,soil_moisture_3_to_9cm,soil_moisture_9_to_27cm,'
            f'soil_moisture_27_to_81cm,wind_direction_180m,wind_speed_180m,weather_code,'
            f'cloud_cover_low,cloud_cover_mid,cloud_cover_high,showers,'
            f'vapour_pressure_deficit,wind_gusts_10m,evapotranspiration,'
            f'precipitation_probability,dew_point_2m'
            f'&start_date={today}&end_date={end}'
        )
        # -----------------get the data from the api ------------------
        # get data from API
        response = requests.get(url)
        data = response.json()

        if 'hourly' not in data:
            print("ERROR from API:", data)
            time.sleep(3600)   # wait 1 hour even on error, then retry
            continue

        hourly = data['hourly']

        records = []
        for i in range(len(hourly['time'])):
            record = {key: hourly[key][i] for key in hourly}
            records.append(record)

        # df = pd.DataFrame(hourly)
        # df.to_excel('data.xlsx', index=False)

        # # --------------send it to kafka ---------------------

        for record in records:
            # Skip records where ALL weather values are None
            values = [v for k, v in record.items() if k != 'time']
            if all(v is None for v in values):
                continue
            producer.send('weather_raw_data', value=record)
        producer.flush()

        # print(records[0])
        print ('all send') 
        time.sleep(3600)

threading.Thread(target=run_producer, daemon=True).start()

# # ---------------------using spark to clean data from kafka ------------------



# Ensure JAVA_HOME points to a supported JDK
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.18.8-hotspot"
os.environ["SPARK_HOME"] = r"C:\spark"
os.environ["PATH"] = os.path.join(os.environ["JAVA_HOME"], "bin") + ";" + os.environ["SPARK_HOME"] + r"\bin;" + os.environ["PATH"]

# Build Spark session with Kafka JARs
spark = SparkSession.builder \
    .appName("KafkaWeatherStreaming") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
    .config("spark.jars", r"C:\spark\jars\mysql-connector-j-9.3.0.jar") \
    .config("spark.sql.shuffle.partitions", "3") \
    .master("local[3]") \
    .getOrCreate()

# print("444")

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather_raw_data") \
    .option("startingOffsets", "earliest") \
    .load()

print('connection build')



from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType
schema = StructType() \
    .add("time", StringType()) \
    .add("temperature_2m", DoubleType()) \
    .add("rain", DoubleType()) \
    .add("relative_humidity_2m", IntegerType()) \
    .add("apparent_temperature", DoubleType()) \
    .add("snowfall", DoubleType()) \
    .add("snow_depth", DoubleType()) \
    .add("pressure_msl", DoubleType()) \
    .add("surface_pressure", DoubleType()) \
    .add("cloud_cover", IntegerType()) \
    .add("visibility", DoubleType()) \
    .add("wind_speed_10m", DoubleType()) \
    .add("wind_speed_80m", DoubleType()) \
    .add("wind_speed_120m", DoubleType()) \
    .add("wind_direction_10m", IntegerType()) \
    .add("wind_direction_80m", IntegerType()) \
    .add("wind_direction_120m", IntegerType()) \
    .add("temperature_80m", DoubleType()) \
    .add("temperature_120m", DoubleType()) \
    .add("temperature_180m", DoubleType()) \
    .add("soil_temperature_0cm", DoubleType()) \
    .add("soil_temperature_6cm", DoubleType()) \
    .add("soil_temperature_18cm", DoubleType()) \
    .add("soil_temperature_54cm", DoubleType()) \
    .add("soil_moisture_0_to_1cm", DoubleType()) \
    .add("soil_moisture_1_to_3cm", DoubleType()) \
    .add("soil_moisture_3_to_9cm", DoubleType()) \
    .add("soil_moisture_9_to_27cm", DoubleType()) \
    .add("soil_moisture_27_to_81cm", DoubleType()) \
    .add("wind_direction_180m", IntegerType()) \
    .add("wind_speed_180m", DoubleType()) \
    .add("weather_code", IntegerType()) \
    .add("cloud_cover_low", IntegerType()) \
    .add("cloud_cover_mid", IntegerType()) \
    .add("cloud_cover_high", IntegerType()) \
    .add("showers", DoubleType()) \
    .add("vapour_pressure_deficit", DoubleType()) \
    .add("wind_gusts_10m", DoubleType()) \
    .add("evapotranspiration", DoubleType()) \
    .add("precipitation_probability", IntegerType()) \
    .add("dew_point_2m", DoubleType())


from pyspark.sql.functions import col, from_json

weather_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

print('converted binary to string')

#  cleaning


from pyspark.sql.functions import col, to_timestamp, coalesce, lit

# 1. Convert time string to proper timestamp
weather_df = weather_df.withColumn("time", to_timestamp(col("time"), "yyyy-MM-dd'T'HH:mm"))

# 2. Drop rows where critical columns are NULL
critical_cols = ["time", "temperature_2m", "wind_speed_10m", "pressure_msl", "relative_humidity_2m"]
for c in critical_cols:
    weather_df = weather_df.filter(col(c).isNotNull())

# 3. Drop duplicates based on timestamp
weather_df = weather_df.dropDuplicates(["time"])

# 4. Sanity range checks - filter impossible values
weather_df = weather_df.filter(
    (col("temperature_2m").between(-80, 60)) &
    (col("relative_humidity_2m").between(0, 100)) &
    (col("wind_speed_10m") >= 0) &
    (col("pressure_msl").between(870, 1084)) &
    (col("visibility") >= 0) &
    (col("rain") >= 0) &
    (col("snowfall") >= 0)
)

# 5. Fill non-critical NULLs with 0.0 (far-future forecast fields)
fill_cols = [
    "temperature_180m", "soil_temperature_0cm", "soil_temperature_6cm",
    "soil_temperature_18cm", "soil_temperature_54cm", "soil_moisture_0_to_1cm",
    "soil_moisture_1_to_3cm", "soil_moisture_3_to_9cm", "soil_moisture_9_to_27cm",
    "soil_moisture_27_to_81cm", "wind_direction_180m", "wind_speed_180m"
]
for c in fill_cols:
    weather_df = weather_df.withColumn(c, coalesce(col(c), lit(0.0)))

print("Data cleaned successfully")



# --------------------------------------- send data to sql --------------------
import pymysql


MYSQL_HOST = "host"        
MYSQL_PORT = "3307"             
MYSQL_DB   = "weather_db"
MYSQL_USER = "root"
MYSQL_PASS = "password"
MYSQL_URL  = f"jdbc:mysql://{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?useSSL=false&allowPublicKeyRetrieval=true"

print("Connected ✅")

def write_to_mysql(batch_df, batch_id):
    print(f"Writing batch {batch_id} — {batch_df.count()} rows")
    
    rows = batch_df.collect()
    
    if not rows:
        print(f"Batch {batch_id} is empty, skipping")
        return

    conn = pymysql.connect(
        host=MYSQL_HOST,
        port=int(MYSQL_PORT),
        user=MYSQL_USER,
        password=MYSQL_PASS,
        database=MYSQL_DB
    )
    cursor = conn.cursor()

    columns = batch_df.columns
    placeholders = ", ".join(["%s"] * len(columns))
    col_names = ", ".join([f"`{c}`" for c in columns])

    sql = f"INSERT IGNORE INTO weather_data ({col_names}) VALUES ({placeholders})"

    inserted = 0
    skipped = 0
    for row in rows:
        values = [str(v) if not isinstance(v, (int, float, type(None))) else v for v in row]
        result = cursor.execute(sql, values)
        if result == 1:
            inserted += 1
        else:
            skipped += 1

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Batch {batch_id} done — {inserted} inserted, {skipped} skipped (duplicates)")


print("Data export successfully")

query = weather_df.writeStream \
    .foreachBatch(write_to_mysql) \
    .outputMode("append") \
    .option("checkpointLocation", r"C:\spark\checkpoints\weather") \
    .option("kafka.metrics.reporters", "") \
    .option("minOffsetsPerTrigger", "0") \
    .start()
query.awaitTermination()

print("successfully run etl")