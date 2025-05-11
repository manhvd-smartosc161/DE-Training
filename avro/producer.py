from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import json
import time

# Read schema from file
with open('user_schema.avsc', 'r') as f:
    schema_str = f.read()

# Configure Schema Registry
schema_registry_conf = {
    'url': 'http://localhost:8081',
}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Create Avro serializer
avro_serializer = AvroSerializer(
    schema_registry_client,
    schema_str,
    lambda x, ctx: x
)

# Configure producer
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'key.serializer': StringSerializer('utf_8'),
    'value.serializer': avro_serializer
}

# Create producer
producer = SerializingProducer(producer_conf)

def delivery_report(err, msg):
    if err is not None:
        print(f'Error while sending message: {err}')
    else:
        print(f'Message was sent to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}')

# Create user data
for i in range(10):
    user_data = {
        'id': '1',
        'name': f'User {i}',
        'email': f'user{i}@example.com',
        'created_at': int(time.time() * 1000)
    }
    
    # Send message
    producer.produce(
        topic='users-topic',
        key=str(i),
        value=user_data,
        on_delivery=delivery_report
    )
    
    print(f'Sent data: {user_data}')
    
    # Flush data
    producer.flush()
    time.sleep(0.5)

print('Finished sending data')
