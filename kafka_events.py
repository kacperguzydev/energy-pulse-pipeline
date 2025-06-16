from kafka import KafkaProducer
from faker import Faker
from datetime import datetime
import json
import random
import time

faker = Faker()
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

regions = ["North", "South", "East", "West", "Central"]

def create_event():
    return {
        "household_id": faker.uuid4(),
        "region": random.choice(regions),
        "kw": round(random.uniform(0.1, 10.0), 2),
        "timestamp": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    }

if __name__ == "__main__":
    print("Producer started. Streaming smart meter data to 'energy_meters'...")
    try:
        while True:
            event = create_event()
            producer.send("energy_meters", value=event)
            print("Sent:", event)
            time.sleep(1.0)
    except KeyboardInterrupt:
        print("Producer stopped by user.")
    finally:
        producer.close()
