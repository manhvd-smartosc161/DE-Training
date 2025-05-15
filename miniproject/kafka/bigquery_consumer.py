from google.cloud import bigquery
from confluent_kafka import Consumer
import json
import os

# Thiết lập credentials cho Google Cloud
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "parch-and-posey-16-e91aa306a6d9.json"

# Configure BigQuery client
bq_client = bigquery.Client()
project_id = "parch-and-posey-16"
dataset_id = "data_warehouse"
table_id = f"{project_id}.{dataset_id}.orders"

# Configure Kafka consumer
kafka_conf = {
    'bootstrap.servers': '3.25.206.27:9092',
    'group.id': 'bq_order_consumer',
    'auto.offset.reset': 'latest'
}
consumer = Consumer(kafka_conf)
consumer.subscribe(['order_topic'])

print("Listening to data from Kafka and writing to BigQuery...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Lỗi Kafka: {msg.error()}")
            continue

        try:
            order = json.loads(msg.value().decode('utf-8'))
            # Convert data for BigQuery (if needed)
            rows_to_insert = [order]
            errors = bq_client.insert_rows_json(table_id, rows_to_insert)
            if errors == []:
                print(f"Wrote order id={order.get('id')} to BigQuery.")
            else:
                print(f"Error writing to BigQuery: {errors}")
        except Exception as e:
            print(f"Error processing/sending data: {e}")

except KeyboardInterrupt:
    print("Stopping consumer.")
finally:
    consumer.close()

