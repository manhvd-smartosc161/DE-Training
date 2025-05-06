from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count, col, desc

# Initialize Spark session
spark = SparkSession.builder.appName("Sales Analysis").getOrCreate()

# Read the CSV file
df = spark.read.csv("./sales_data.csv", header=True, inferSchema=True)

# 1. Hiển thị 5 dòng đầu tiên của dữ liệu
print("=== 5 dòng đầu tiên của dữ liệu ===")
df.show(5)

# 2. Phân tích theo Region
print("\n=== Phân tích theo Region ===")
region_analysis = df.groupBy("Region").agg(
    sum("Weighted Revenue").alias("Total Revenue"),
    avg("Weighted Revenue").alias("Average Revenue"),
    count("*").alias("Number of Sales")
).orderBy(desc("Total Revenue"))
region_analysis.show()

# 3. Phân tích theo Segment
print("\n=== Phân tích theo Segment ===")
segment_analysis = df.groupBy("Segment").agg(
    sum("Weighted Revenue").alias("Total Revenue"),
    avg("Weighted Revenue").alias("Average Revenue"),
    count("*").alias("Number of Sales")
).orderBy(desc("Total Revenue"))
segment_analysis.show()

# 4. Top 5 Salesperson có doanh số cao nhất
print("\n=== Top 5 Salesperson có doanh số cao nhất ===")
top_salespeople = df.groupBy("Salesperson").agg(
    sum("Weighted Revenue").alias("Total Revenue")
).orderBy(desc("Total Revenue")).limit(5)
top_salespeople.show()

# 5. Phân tích theo Opportunity Stage
print("\n=== Phân tích theo Opportunity Stage ===")
stage_analysis = df.groupBy("Opportunity Stage").agg(
    sum("Weighted Revenue").alias("Total Revenue"),
    count("*").alias("Number of Opportunities")
).orderBy(desc("Total Revenue"))
stage_analysis.show()

# Đóng Spark session
spark.stop()