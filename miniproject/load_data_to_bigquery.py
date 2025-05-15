from pyspark.sql import SparkSession
from pyspark.sql.functions import col, year, month

# PostgreSQL connection config
pg_url = "jdbc:postgresql://localhost:5432/parch-and-posey"
pg_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}

# BigQuery config
bq_project = "parch-and-posey-16"
bq_dataset = "data_warehouse"

# Spark session
spark = SparkSession.builder \
    .appName("ParchPoseyFullETL") \
    .config("spark.jars", "jars/postgresql-42.6.0.jar,jars/spark-3.4-bigquery-0.34.1.jar,jars/gcs-connector-hadoop3-latest.jar") \
    .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
    .config("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS") \
    .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", "parch-and-posey-16-e91aa306a6d9.json") \
    .config("spark.driver.memory", "8g") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

# List of tables to process
tables = ["orders", "region", "sales_reps", "web_events", "accounts"]

string_columns = {
    "region": ["name"],
    "sales_reps": ["name"],
    "web_events": ["channel"],
    "accounts": ["name", "website", "primary_poc"],
}

for table in tables:
    print(f"Processing table: {table}")
    if table in string_columns:
        # Get all columns from the table
        df_schema = spark.read.jdbc(
            pg_url,
            table=f"public.{table}",
            properties=pg_properties
        ).limit(0)
        
        # Create a list of columns for the SELECT query
        cols = []
        for column in df_schema.columns:
            if column in string_columns[table]:
                cols.append(f"LEFT({column}, 255) as {column}")
            else:
                cols.append(column)
        
        select_clause = ", ".join(cols)
        query = f"(SELECT {select_clause} FROM public.{table}) AS {table}_sub"
        df = spark.read.jdbc(
            pg_url,
            table=query,
            properties=pg_properties
        )
    else:
        df = spark.read.jdbc(
            pg_url,
            table=f"public.{table}",
            properties=pg_properties
        )

    # Step 2: Transform (optional basic cleaning)
    df_cleaned = df.dropDuplicates()
    if "id" in df_cleaned.columns:
        df_cleaned = df_cleaned.dropna(subset=["id"])

    # Step 3: Load
    df_cleaned.write \
        .format("bigquery") \
        .option("table", f"{bq_project}.{bq_dataset}.{table}") \
        .option("parentProject", "parch-and-posey-16") \
        .option("project", "parch-and-posey-16") \
        .option("temporaryGcsBucket", "parch-and-posey-16-temp") \
        .mode("overwrite") \
        .save()

print("✅ All tables loaded into BigQuery successfully.")
