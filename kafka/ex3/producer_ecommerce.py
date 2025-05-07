import random
import json
import time
from datetime import datetime
from confluent_kafka import Producer

producer = Producer({'bootstrap.servers': 'localhost:9092'})
topic = 'ecommerce_transactions'

user_ids = [f"user_{i}" for i in range(1, 6)]
product_ids = [f"product_{i}" for i in range(100, 106)]

print("Starting e-commerce transaction producer (Ctrl+C to stop)...")
try:
    while True:
        event = {
            "user_id": random.choice(user_ids),
            "product_id": random.choice(product_ids),
            "amount": random.randint(100000, 2000000),
            "timestamp": datetime.now().isoformat()
        }
        producer.produce(topic, value=json.dumps(event).encode('utf-8'))
        print(f"Produced: {event}")
        producer.poll(0)
        time.sleep(20)
except KeyboardInterrupt:
    print("\nStopped producer.")
finally:
    producer.flush()
