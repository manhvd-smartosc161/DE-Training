from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, regexp_replace

# Initialize Spark session with PostgreSQL JDBC driver
spark = SparkSession.builder \
    .appName("Data Processing Example") \
    .config("spark.jars", "postgresql-42.7.5.jar") \
    .getOrCreate()

# Assuming we have processed data in a DataFrame called 'processed_df'
# For demonstration, let's create a sample DataFrame
data = [
    (1, "John Doe", 30, "New York"),
    (2, "Jane Smith", 25, "San Francisco"),
    (3, "Bob Johnson", 45, "Chicago"),
    (4, "Alice Williams", 35, "Boston"),
    (5, "Michael Brown", 40, "Los Angeles"),
    (6, "Emily Davis", 28, "Seattle"),
    (7, "David Wilson", 50, "Miami"),
    (8, "Sarah Miller", 33, "Denver"),
    (9, "Thomas Anderson", 42, "Austin"),
    (10, "Jennifer Taylor", 38, "Portland"),
    (11, "James Martinez", 27, "Phoenix"),
    (12, "Lisa Robinson", 44, "Atlanta"),
    (13, "Robert Garcia", 31, "Dallas"),
    (14, "Patricia Lee", 29, "Houston"),
    (15, "Daniel Clark", 36, "Philadelphia")
]

schema = ["id", "name", "age", "city"]
processed_df = spark.createDataFrame(data, schema)

# Show the DataFrame
print("Processed DataFrame:")
processed_df.show()

# Save the DataFrame to a single CSV file
csv_output_path = "output"
processed_df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_output_path)
print(f"Data saved to CSV at: {csv_output_path}")

# Load the data into PostgreSQL
# First, we need to set up the database connection properties
db_properties = {
    "user": "postgres",
    "password": "postgres",
    "driver": "org.postgresql.Driver"
}
db_url = "jdbc:postgresql://localhost:5432/sqltrain"
table_name = "processed_data"

# Write the DataFrame to PostgreSQL
processed_df.write \
    .format("jdbc") \
    .option("url", db_url) \
    .option("dbtable", table_name) \
    .option("user", db_properties["user"]) \
    .option("password", db_properties["password"]) \
    .option("driver", db_properties["driver"]) \
    .mode("overwrite") \
    .save()

print(f"Data loaded into PostgreSQL table: {table_name}")

# Stop the Spark session
spark.stop()
