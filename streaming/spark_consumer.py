import os
import json
import psycopg2
from kafka import KafkaConsumer
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

print("Connecting to PostgreSQL...")
conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=os.getenv("POSTGRES_PORT", "5432"),
    dbname=os.getenv("POSTGRES_DB", "sales_analytics"),
    user=os.getenv("POSTGRES_USER", "admin"),
    password=os.getenv("POSTGRES_PASSWORD", "secret")
)
cursor = conn.cursor()
print("PostgreSQL connected!")

print("Connecting to Kafka...")
consumer = KafkaConsumer(
    os.getenv("KAFKA_TOPIC", "sales-events"),
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="sales-consumer-group"
)
print("Kafka connected! Listening for sales events...")

try:
    for message in consumer:
        event = message.value
        print(f"Received: {event}")
        cursor.execute("""
            INSERT INTO raw_sales_events 
            (order_id, product, region, amount, event_time)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            event["order_id"],
            event["product"],
            event["region"],
            event["amount"],
            datetime.fromisoformat(event["timestamp"])
        ))
        conn.commit()
        print(f"Saved to PostgreSQL!")

except KeyboardInterrupt:
    print("Stopping consumer...")
    cursor.close()
    conn.close()