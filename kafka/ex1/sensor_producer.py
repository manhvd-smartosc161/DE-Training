from confluent_kafka import Producer
import json
import random
import time
from datetime import datetime

# Kafka configurations
conf = {
    'bootstrap.servers': 'localhost:9092'
}

# Initialize Producer
producer = Producer(conf)

# Define your topic
topic = "sensor_data"

def generate_sensor_data():
    """Generate random sensor data"""
    return {
        "sensor_id": f"SENSOR-{random.randint(1, 5)}",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "temperature": round(random.uniform(20, 30), 2),  # Temperature in Celsius
        "humidity": round(random.uniform(40, 60), 2),     # Humidity in percentage
        "pressure": round(random.uniform(980, 1020), 2),  # Pressure in hPa
        "vibration": round(random.uniform(0, 10), 2),     # Vibration in mm/s
        "battery_level": round(random.uniform(20, 100), 2)  # Battery level in percentage
    }

def delivery_report(err, msg):
    """Callback for delivery reports"""
    if err is not None:
        print(f"❌ Delivery failed for record {msg.key()}: {err}")
    else:
        print(f"✅ Record {msg.key()} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def main():
    print("Starting sensor data producer...")
    print("Press Ctrl+C to stop")
    
    try:
        while True:
            # Generate random sensor data
            sensor_data = generate_sensor_data()
            
            # Convert to JSON string
            value = json.dumps(sensor_data)
            
            # Use sensor_id as the key
            key = sensor_data["sensor_id"]
            
            # Produce message
            producer.produce(
                topic=topic,
                key=key,
                value=value,
                callback=delivery_report
            )
            
            # Trigger delivery callbacks
            producer.poll(0)
            
            # Print the data being sent
            print(f"\nSending sensor data: {sensor_data}")
            
            # Wait for 2 seconds before sending next data
            time.sleep(3)
            
    except KeyboardInterrupt:
        print("\nStopping producer...")
    finally:
        # Wait for any outstanding messages to be delivered
        producer.flush()

if __name__ == "__main__":
    main() 