from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr, window, udf, sum, avg, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, BinaryType, MapType
import os
import fastavro
import io
import json

# ====== Colorful Logging Utilities ======
class LogColors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'
    GREY = '\033[90m'
    YELLOW = '\033[33m'
    RED = '\033[31m'
    GREEN = '\033[32m'
    CYAN = '\033[36m'
    BLUE = '\033[34m'

def log_info(msg):
    print(f"{LogColors.OKCYAN}[INFO]{LogColors.ENDC} {msg}")

def log_success(msg):
    print(f"{LogColors.OKGREEN}[SUCCESS]{LogColors.ENDC} {msg}")

def log_warning(msg):
    print(f"{LogColors.WARNING}[WARNING]{LogColors.ENDC} {msg}")

def log_error(msg):
    print(f"{LogColors.FAIL}[ERROR]{LogColors.ENDC} {msg}")

def log_alert(msg):
    print(f"{LogColors.RED}{LogColors.BOLD}[ALERT]{LogColors.ENDC} {LogColors.BOLD}{msg}{LogColors.ENDC}")

def log_batch_header(msg):
    print(f"\n{LogColors.HEADER}{LogColors.BOLD}{msg}{LogColors.ENDC}\n")

def log_section(msg):
    print(f"{LogColors.OKBLUE}{LogColors.BOLD}{msg}{LogColors.ENDC}")

# ========================================

# Load Avro schema from file for validation
with open("kafka/order_schema.avsc", "r") as f:
    avro_schema = json.load(f)

# Define the schema for order data (for Spark DataFrame)
order_schema = StructType([
    StructField("id", IntegerType(), True),
    StructField("account_id", IntegerType(), True),
    StructField("occurred_at", StringType(), True),
    StructField("standard_qty", IntegerType(), True),
    StructField("gloss_qty", IntegerType(), True),
    StructField("poster_qty", IntegerType(), True),
    StructField("total", IntegerType(), True),
    StructField("standard_amt_usd", DoubleType(), True),
    StructField("gloss_amt_usd", DoubleType(), True),
    StructField("poster_amt_usd", DoubleType(), True),
    StructField("total_amt_usd", DoubleType(), True)
])

# Initialize Spark session
spark = SparkSession.builder \
    .appName("KafkaOrderConsumer") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.0") \
    .getOrCreate()

# Set log level to reduce unnecessary messages
spark.sparkContext.setLogLevel("ERROR")

# Function to validate Avro schema before processing
def validate_avro_schema(row_bytes):
    try:
        buf = io.BytesIO(row_bytes)
        record = fastavro.schemaless_reader(buf, avro_schema)
        return record
    except Exception as e:
        log_error(f"Avro schema validation failed: {e}")
        return None

def avro_bytes_to_dict(avro_bytes):
    if avro_bytes is None:
        return None
    try:
        buf = io.BytesIO(avro_bytes)
        record = fastavro.schemaless_reader(buf, avro_schema)
        return record
    except Exception as e:
        return None

from pyspark.sql.types import StructType as SparkStructType, StructField as SparkStructField, StringType as SparkStringType, IntegerType as SparkIntegerType, DoubleType as SparkDoubleType

# Register UDF
from pyspark.sql.types import MapType, StringType
avro_bytes_to_dict_udf = udf(avro_bytes_to_dict, MapType(StringType(), StringType()))

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "3.25.206.27:9092") \
    .option("subscribe", "order_topic") \
    .option("startingOffsets", "latest") \
    .load()

# Check Avro schema before processing
# Parse Avro data from Kafka 'value' column (bytes)
def parse_and_validate_avro(df):
    # Use mapInPandas for batch processing and schema validation
    import pandas as pd

    def avro_validator_udf(iterator):
        for pdf in iterator:
            records = []
            for b in pdf['value']:
                try:
                    if b is not None:
                        buf = io.BytesIO(b)
                        record = fastavro.schemaless_reader(buf, avro_schema)
                        records.append(record)
                    else:
                        records.append(None)
                except Exception as e:
                    log_error(f"Avro validation error: {e}")
                    records.append(None)
            yield pd.DataFrame(records)

    # The output schema must match order_schema
    return df.mapInPandas(avro_validator_udf, schema=order_schema)

parsed_df = parse_and_validate_avro(df)

# Convert occurred_at to timestamp to use for windowing
parsed_df = parsed_df.withColumn("timestamp", col("occurred_at").cast("timestamp"))

# 1. Total revenue by time (window 1 minute)
total_revenue_stream = parsed_df \
    .withWatermark("timestamp", "10 minutes") \
    .groupBy(window(col("timestamp"), "30 seconds")) \
    .agg(sum("total_amt_usd").alias("total_revenue")) \
    .select("window.start", "window.end", "total_revenue")

# 2. Average order value (global average updated with each batch)
avg_order_value_stream = parsed_df \
    .agg(avg("total_amt_usd").alias("average_order_value"))

# 3. Query: Detect orders with unusually high value (eg > 2000 USD)
HIGH_ORDER_THRESHOLD = 2000.0

def process_high_value_orders(batch_df, batch_id):
    high_orders = batch_df.filter(col("total_amt_usd") > HIGH_ORDER_THRESHOLD)
    if not high_orders.isEmpty():
        log_alert(f"Found high value orders (total_amt_usd > {HIGH_ORDER_THRESHOLD}):")
        for row in high_orders.select("id", "account_id", "occurred_at", "total_amt_usd").collect():
            log_alert(f"Order ID: {row['id']}, Account ID: {row['account_id']}, Occurred At: {row['occurred_at']}, Total Amount USD: {row['total_amt_usd']:.2f}")

# 4. Query: Count the number of orders by account_id in each batch
def process_order_count_by_account(batch_df, batch_id):
    if not batch_df.isEmpty():
        log_section(f"Order count by account_id:")
        batch_df.groupBy("account_id").count().orderBy(col("count").desc()).show(truncate=False)

# Combine processing: Print list of orders, total revenue table, and average order value
def process_orders_and_aggregates(batch_df, batch_id):
    if not batch_df.isEmpty():
        log_batch_header(f"=================== [OrderAggregatesStream] Batch ID: {batch_id} ===================")
        batch_df.cache()
        
        # 1. Print list of orders in this batch
        log_section("List of orders in this batch:")
        batch_df.select("id", "account_id", "occurred_at", "total_amt_usd").show(truncate=False)
        
        # 2. Print total revenue table
        total_revenue = batch_df.agg(sum("total_amt_usd")).collect()[0][0]
        log_success(f"Total revenue for this batch: {total_revenue:.2f} USD")
        
        # 3. Print average order value
        avg_val = batch_df.agg(avg("total_amt_usd")).collect()[0][0]
        log_info(f"Average order value for this batch: {avg_val:.2f} USD")
        
        # 4. Notify high value orders
        process_high_value_orders(batch_df, batch_id)
        
        # 5. Count the number of orders by account_id
        process_order_count_by_account(batch_df, batch_id)
        
        batch_df.unpersist()

# Print list of orders, total revenue, and average order value to console using the defined function
query_orders_and_aggregates = parsed_df.writeStream \
    .outputMode("append") \
    .foreachBatch(process_orders_and_aggregates) \
    .queryName("OrderAggregatesStream") \
    .trigger(processingTime="30 seconds") \
    .start()

log_info("Start Spark Streaming consumer with aggregation calculations. Waiting for messages...")

try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    log_warning("Stopping Spark Streaming consumer...")
finally:
    log_warning("Stopping all streams...")
    for q in spark.streams.active:
        try:
            q.stop()
        except Exception as e:
            log_error(f"Error stopping query {q.name}: {e}")
    spark.stop()
    log_success("Spark session has been stopped.")
