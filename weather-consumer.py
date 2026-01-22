import json
import time
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

while True:
    try:
        consumer = KafkaConsumer(
            'weather',
            bootstrap_servers=['kafka:9092'],
            value_deserializer=lambda x: json.loads(x.decode('utf-8')),
            auto_offset_reset='earliest',
            group_id='weather-group'
        )
        print("Connected to Kafka!")
        break
    except NoBrokersAvailable:
        print("Kafka not ready for consumer. Retrying...")
        time.sleep(5)

while True:
    try:
        conn = psycopg2.connect(
            dbname="weatherdb",
            user="sabari",
            password="sabbyogi",
            host="postgres",
            port="5432"
        )
        cur = conn.cursor()
        print("Connected to Postgres!")
        break
    except Exception:
        print("Postgres not ready, retrying in 5 seconds...")
        time.sleep(5)

cur.execute("""
    CREATE TABLE IF NOT EXISTS weather_data (
        id SERIAL PRIMARY KEY,
        temperature FLOAT,
        city TEXT,
        condition TEXT,
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
""")
conn.commit()

for message in consumer:
    weather = message.value
    try:
        city = weather['location']['name']
        temp = weather['current']['temp_c']
        condition = weather['current']['condition']['text']

        cur.execute(
            "INSERT INTO weather_data (temperature, city, condition) VALUES (%s, %s, %s)",
            (temp, city, condition)
        )
        conn.commit()

        print(f"Inserted: {city}, {temp}, {condition}")

    except Exception as e:
        print("Insert error:", e)
