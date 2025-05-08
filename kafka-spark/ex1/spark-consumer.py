from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, sum, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# Khởi tạo Spark Session với Kafka package
spark = SparkSession.builder \
    .appName("KafkaSparkConsumer") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
    .getOrCreate()

# Định nghĩa schema cho dữ liệu
schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("amount", DoubleType(), True),
    StructField("timestamp", TimestampType(), True)
])

# Đọc dữ liệu từ Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "ecommerce_transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse JSON và tính toán
parsed_df = df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Tính tổng doanh thu theo sản phẩm
revenue_df = parsed_df \
    .groupBy("product_id") \
    .agg(
        sum("amount").alias("total_revenue"),
        count("*").alias("transaction_count")
    )

# In kết quả
query = revenue_df \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Chạy cho đến khi bị dừng
query.awaitTermination()
