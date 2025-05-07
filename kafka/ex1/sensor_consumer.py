from confluent_kafka import Consumer
import json
from datetime import datetime

# Kafka configurations
conf = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'sensor_consumer_group',
    'auto.offset.reset': 'earliest'
}

# Initialize Consumer
consumer = Consumer(conf)

# Define your topic
topic = "sensor_data"

def process_sensor_data(data):
    """Process and analyze sensor data"""
    # Extract values
    sensor_id = data['sensor_id']
    temperature = data['temperature']
    humidity = data['humidity']
    pressure = data['pressure']
    vibration = data['vibration']
    battery_level = data['battery_level']
    
    # Simple analysis
    alerts = []
    
    if temperature > 28:
        alerts.append("⚠️ High temperature alert!")
    if humidity > 55:
        alerts.append("⚠️ High humidity alert!")
    if pressure < 990:
        alerts.append("⚠️ Low pressure alert!")
    if vibration > 8:
        alerts.append("⚠️ High vibration alert!")
    if battery_level < 30:
        alerts.append("⚠️ Low battery alert!")
    
    return alerts

def main():
    print("Starting sensor data consumer...")
    print("Press Ctrl+C to stop")
    
    # Subscribe to topic
    consumer.subscribe([topic])
    
    try:
        while True:
            # Poll for messages
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            
            # Parse the message
            try:
                data = json.loads(msg.value().decode('utf-8'))
                
                # Print received data
                print("\n" + "="*50)
                print(f"📥 Received data from {data['sensor_id']} at {data['timestamp']}")
                print(f"Temperature: {data['temperature']}°C")
                print(f"Humidity: {data['humidity']}%")
                print(f"Pressure: {data['pressure']} hPa")
                print(f"Vibration: {data['vibration']} mm/s")
                print(f"Battery Level: {data['battery_level']}%")
                
                # Process and check for alerts
                alerts = process_sensor_data(data)
                if alerts:
                    print("\n🔔 Alerts:")
                    for alert in alerts:
                        print(alert)
                
            except json.JSONDecodeError:
                print(f"Error decoding message: {msg.value()}")
            except Exception as e:
                print(f"Error processing message: {e}")
                
    except KeyboardInterrupt:
        print("\nStopping consumer...")
    finally:
        # Close consumer
        consumer.close()

if __name__ == "__main__":
    main() 