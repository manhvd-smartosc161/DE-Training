from google.cloud import bigquery
from google.oauth2 import service_account

def fetch_data_from_bigquery():
    # Load credentials from a service account file.
    credentials = service_account.Credentials.from_service_account_file(
        './training-de-457603-a41a2de0ae51.json',
        scopes=[
            "https://www.googleapis.com/auth/bigquery",
            "https://www.googleapis.com/auth/drive",
            "https://www.googleapis.com/auth/cloud-platform"
        ]
    )

    # Construct a BigQuery client object with the credentials.
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # Perform a query.
    query = """
    SELECT * FROM `training-de-457603.Demo.test` LIMIT 10
    """
    query_job = client.query(query)  # Make an API request.

    # Wait for the job to complete and process the results.
    return [dict(row) for row in query_job.result()]

# Example usage
data = fetch_data_from_bigquery()
print(data)
