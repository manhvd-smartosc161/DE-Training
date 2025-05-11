from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

# Configure Schema Registry
schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# Create AvroDeserializer
avro_deserializer = AvroDeserializer(
    schema_registry_client=schema_registry_client,
    schema_str=None,  # Schema will be automatically fetched from Schema Registry
    from_dict=lambda x, _: x  # Function to convert Avro data to dict
)

# Configure consumer
consumer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'key.deserializer': StringDeserializer('utf_8'),
    'value.deserializer': avro_deserializer,
    'group.id': 'example-consumer-group',
    'auto.offset.reset': 'earliest'
}

# Create consumer
consumer = DeserializingConsumer(consumer_conf)
consumer.subscribe(['users-topic'])

# Read and process messages
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f'Consumer error: {msg.error()}')
            continue

        try:
            user = msg.value()  # Data is deserialized into a dict
            if user is None:
                print("Received a message with no value or incompatible serialization.")
            else:
                print(f'Received data: {user}')
        except Exception as e:
            print(f'Error while deserializing data: {e}')

except KeyboardInterrupt:
    pass
finally:
    consumer.close()