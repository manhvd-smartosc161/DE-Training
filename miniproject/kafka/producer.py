from confluent_kafka import Producer
import time
import random
from datetime import datetime
from fastavro import parse_schema, schemaless_writer
import io
import json

with open("kafka/order_schema.avsc", "r") as f:
    order_schema = json.load(f)
parsed_schema = parse_schema(order_schema)

# Initialize the starting order ID
current_order_id = 300000

# Configure Confluent Kafka producer
conf = {
    'bootstrap.servers': '3.25.206.27:9092',
    'acks': 'all'  # Wait for all in-sync replicas to acknowledge the message
}
producer = Producer(conf)

# Function to generate order data based on Parch and Posey structure
def generate_order():
    global current_order_id
    now = datetime.now()
    order_data = {
        "id": current_order_id,
        "account_id": random.randint(1001, 4321),
        "occurred_at": now.isoformat(),
        "standard_qty": random.randint(0, 1000),
        "gloss_qty": random.randint(0, 1000),
        "poster_qty": random.randint(0, 1000),
        "total": 0,
        "standard_amt_usd": 0.0,
        "gloss_amt_usd": 0.0,
        "poster_amt_usd": 0.0,
        "total_amt_usd": 0.0
    }
    current_order_id += 1
    return order_data

# Function to calculate total quantities and amounts
def calculate_totals(order):
    standard_price = 0.5
    gloss_price = 0.75
    poster_price = 1.0

    order["total"] = order["standard_qty"] + order["gloss_qty"] + order["poster_qty"]
    order["standard_amt_usd"] = order["standard_qty"] * standard_price
    order["gloss_amt_usd"] = order["gloss_qty"] * gloss_price
    order["poster_amt_usd"] = order["poster_qty"] * poster_price
    order["total_amt_usd"] = order["standard_amt_usd"] + order["gloss_amt_usd"] + order["poster_amt_usd"]

    return order

def avro_serialize(schema, record):
    buf = io.BytesIO()
    schemaless_writer(buf, schema, record)
    return buf.getvalue()

# Function to send data to Kafka
def send_to_kafka(producer, topic, data):
    try:
        serialized_data = avro_serialize(parsed_schema, data)
        
        # Delivery callback function
        def delivery_callback(err, msg):
            if err:
                print(f"Failed to send order: {err}")
                return False
            else:
                print(f"Successfully sent order to topic {msg.topic()} partition {msg.partition()} offset {msg.offset()}")
                return True
        
        # Produce the message
        producer.produce(topic, value=serialized_data, callback=delivery_callback)
        producer.poll(0)  # Trigger any callbacks
        return True
    except Exception as e:
        print(f"Failed to send order: {e}")
        return False

# Main loop to generate and send order data every 20 seconds
while True:
    try:
        order_data = generate_order()
        order_data = calculate_totals(order_data)
        if send_to_kafka(producer, 'order_topic', order_data):
            print(f"Sent order: {order_data}")
        time.sleep(6)
    except Exception as e:
        print(f"An error occurred: {e}")
        time.sleep(20)  # Still wait before retrying

# Ensure all pending messages are sent before closing
producer.flush()
