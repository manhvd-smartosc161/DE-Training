from google.cloud import bigquery
import os

# Set up credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "parch-and-posey-16-e91aa306a6d9.json"

# Initialize BigQuery client
client = bigquery.Client()

# BigQuery project information
project_id = "parch-and-posey-16"
dataset_id = "data_warehouse"
table_name = "orders"
table_id = f"{project_id}.{dataset_id}.{table_name}"

# Create a query to delete records with id >= 300000
query = f"""
CREATE OR REPLACE TABLE `{table_id}` AS
SELECT * FROM `{table_id}`
WHERE id < 300000
"""

# Execute the query
query_job = client.query(query)
query_job.result()  # Wait for the query to complete

print(f"Successfully removed all orders with id >= 300000 from {table_id}")
