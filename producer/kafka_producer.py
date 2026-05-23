import json
import random
import time
from datetime import datetime
from kafka import KafkaProducer
from dotenv import load_dotenv
import os

load_dotenv()

producer = KafkaProducer(
    bootstrap_servers=os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092'),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

products = ['Laptop', 'Phone', 'Tablet', 'Monitor', 'Keyboard', 'Mouse', 'Headphones']
regions  = ['North', 'South', 'East', 'West']

def generate_sale():
    return {
        "order_id":   f"ORD-{random.randint(1000, 9999)}",
        "product":    random.choice(products),
        "region":     random.choice(regions),
        "amount":     round(random.uniform(50, 2000), 2),
        "timestamp":  datetime.utcnow().isoformat()
    }

print("Starting Kafka producer... sending sales events")

try:
    while True:
        event = generate_sale()
        producer.send(
            os.getenv('KAFKA_TOPIC', 'sales-events'),
            value=event
        )
        print(f"Sent: {event}")
        time.sleep(0.5)

except KeyboardInterrupt:
    print("Stopping producer...")
    producer.close()