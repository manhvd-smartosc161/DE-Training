from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count, col, desc, row_number, month, year, when
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder.appName("Sales Analysis").getOrCreate()

# Read the CSV files
sale_df = spark.read.csv("./data/sales.csv", header=True, inferSchema=True)
store_df = spark.read.csv("./data/stores.csv", header=True, inferSchema=True)
product_df = spark.read.csv("./data/products.csv", header=True, inferSchema=True)

# 1. Hiển thị 5 dòng đầu tiên của dữ liệu
print("=== 5 dòng đầu tiên của dữ liệu ===")
sale_df.show()
store_df.show()
product_df.show()

# 2. Tìm top 10 sản phẩm bán chạy theo vùng
# Join các bảng
sales_products = sale_df.join(store_df, "store_id").join(product_df, "product_id")

# Tổng hợp số lượng bán theo region và product
sales_by_region = sales_products.groupBy("region", "product_id", "product_name") \
    .agg(sum("quantity").alias("total_quantity"))

# Tạo window để xếp hạng theo số lượng bán trong từng vùng
window_region = Window.partitionBy("region").orderBy(desc("total_quantity"))

# Thêm cột rank và lọc top 10
top10_products_region = sales_by_region.withColumn(
    "rank", row_number().over(window_region)
).filter(col("rank") <= 10)

# Sắp xếp kết quả
top10_products_region = top10_products_region.orderBy("region", "rank")
print("Top 10 best-selling products by region:")
top10_products_region.show(truncate=False)

# 3. Phân tích xu hướng doanh thu theo tháng và danh mục
print("\n=== Phân tích xu hướng doanh thu theo tháng và danh mục ===")
# Thêm cột tháng và năm
sales_with_date = sales_products.withColumn("month", month("date")) \
    .withColumn("year", year("date"))

# Tổng hợp doanh thu theo tháng và danh mục
revenue_trend = sales_with_date.groupBy("year", "month", "category") \
    .agg(sum("revenue").alias("total_revenue")) \
    .orderBy("year", "month", "category")

print("Revenue trend by month and category:")
revenue_trend.show(truncate=False)

# 4. Xác định cửa hàng hiệu suất cao và thấp
print("\n=== Phân tích hiệu suất cửa hàng ===")
# Tính toán các chỉ số cho mỗi cửa hàng
store_performance = sales_products.groupBy("store_id", "store_name", "region") \
    .agg(
        sum("revenue").alias("total_revenue"),
        sum("quantity").alias("total_quantity"),
        count("*").alias("total_transactions")
    )

# Tính doanh thu trung bình cho mỗi vùng
region_avg = store_performance.groupBy("region") \
    .agg(avg("total_revenue").alias("avg_revenue"))

# Join để so sánh với trung bình vùng
store_performance = store_performance.join(region_avg, "region") \
    .withColumn("performance", 
        when(col("total_revenue") > col("avg_revenue"), "High")
        .otherwise("Low")
    )

print("Store performance analysis:")
store_performance.show(truncate=False)

# 5. Lưu kết quả dưới định dạng Parquet
print("\n=== Lưu kết quả dưới định dạng Parquet ===")

# Tạo thư mục output nếu chưa tồn tại
output_dir = "output/analysis_results"

# Lưu các kết quả phân tích
top10_products_region.write.mode("overwrite").parquet(f"{output_dir}/top10_products_by_region")
revenue_trend.write.mode("overwrite").parquet(f"{output_dir}/revenue_trend")
store_performance.write.mode("overwrite").parquet(f"{output_dir}/store_performance")

print(f"Results saved to {output_dir}")

# Stop Spark session
spark.stop()


