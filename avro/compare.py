from confluent_kafka.schema_registry import SchemaRegistryClient, Schema

# Schema Registry configuration
schema_registry_conf = {'url': 'http://localhost:8081'}
schema_registry_client = SchemaRegistryClient(schema_registry_conf)

# New schema
new_schema_str = """
{
  "type": "record",
  "name": "User",
  "namespace": "com.example",
  "fields": [
    { "name": "id", "type": "string" },
    { "name": "name", "type": "string" },
    { "name": "email", "type": ["null", "string"], "default": null },
    { "name": "created_at", "type": "long", "default": 0 },
    { "name": "age", "type": "int", "default": 0 }
  ]
}
"""

# Create Schema object from JSON string
new_schema = Schema(new_schema_str, "AVRO")

# Check compatibility
subject = "users-topic-value"
is_compatible = schema_registry_client.test_compatibility(subject, new_schema)

if is_compatible:
    print("✅ The new schema is compatible with the current schema.")
else:
    print("❌ The new schema is not compatible with the current schema.")