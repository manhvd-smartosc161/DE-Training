from google.cloud import bigquery
import csv

def query_order_from_bigquery():
    # Construct a BigQuery client object without explicit credentials.
    client = bigquery.Client()

    # Define the query to fetch order data
    # Get the top 10 accounts with the highest total order amount
    query = """
    SELECT o.account_id, a.name as account_name, SUM(CAST(o.total_amt_usd AS FLOAT64)) as total_amt_usd, COUNT(*) as order_count
    FROM `sqltrain.orders` o
    JOIN `sqltrain.accounts` a ON o.account_id = a.id
    GROUP BY o.account_id, a.name
    ORDER BY total_amt_usd DESC
    LIMIT 10
    """

    # Execute the query
    query_job = client.query(query)  # Make an API request.

    # Wait for the job to complete and process the results.
    results = [dict(row) for row in query_job.result()]

    return results

def write_to_csv(data, filename='order_results.csv'):
    # Check if we have data to write
    if not data:
        print("No data to write to CSV")
        return
    
    # Get the field names from the first row
    fieldnames = data[0].keys()
    
    # Write data to CSV file
    with open(filename, mode='w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)
    
    print(f"Data successfully written to {filename}")

# Example usage
orders = query_order_from_bigquery()
write_to_csv(orders)
