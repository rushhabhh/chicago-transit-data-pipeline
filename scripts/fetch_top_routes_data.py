import requests
import json
import os
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from kafka import KafkaProducer

# Load API key
load_dotenv()
API_KEY = os.getenv("CTA_API_KEY")

# Kafka setup
KAFKA_TOPIC = 'raw_bus_data'
KAFKA_BROKER = 'localhost:9092'

def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

def fetch_top_routes_data():
    # Top 5 most popular CTA bus routes
    top_routes = ['22', '8', '49', '66', '79']
    rt_param = ",".join(top_routes)
    
    url = f"http://www.ctabustracker.com/bustime/api/v2/getvehicles?key={API_KEY}&rt={rt_param}&format=json"

    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        vehicles = data.get('bustime-response', {}).get('vehicle', [])
        if not vehicles:
            print("No active buses found for top routes.")
            return

        timestamp = datetime.now(timezone.utc).isoformat()
        for v in vehicles:
            v['fetched_at'] = timestamp

        os.makedirs("data", exist_ok=True)
        filename = f"data/top_routes_data_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w') as f:
            json.dump(vehicles, f, indent=2)

        print(f"Saved {len(vehicles)} buses to {filename}")
        print(f"Sending to Kafka topic '{KAFKA_TOPIC}'...")

        producer = create_kafka_producer()
        for v in vehicles:
            producer.send(KAFKA_TOPIC, value=v)

        producer.flush()
        producer.close()
        print("All records sent to Kafka successfully.\n")

    except Exception as e:
        print(f"Error: {e}")

# === Main Loop ===
if __name__ == "__main__":
    while True:
        fetch_top_routes_data()
        print("Waiting 2 minutes (120 seconds) for next pull...\n")
        time.sleep(120)