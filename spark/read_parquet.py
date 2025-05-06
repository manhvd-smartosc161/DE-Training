from pyspark.sql import SparkSession

# Initialize Spark session
spark = SparkSession.builder.appName("Read Parquet Results").getOrCreate()

# Đường dẫn đến thư mục chứa kết quả
output_dir = "output/analysis_results"

# 1. Đọc và hiển thị top 10 sản phẩm theo vùng
print("\n=== Top 10 sản phẩm bán chạy theo vùng ===")
top10_products = spark.read.parquet(f"{output_dir}/top10_products_by_region")
top10_products.show(truncate=False)

# 2. Đọc và hiển thị xu hướng doanh thu
print("\n=== Xu hướng doanh thu theo tháng và danh mục ===")
revenue_trend = spark.read.parquet(f"{output_dir}/revenue_trend")
revenue_trend.show(truncate=False)

# 3. Đọc và hiển thị hiệu suất cửa hàng
print("\n=== Phân tích hiệu suất cửa hàng ===")
store_performance = spark.read.parquet(f"{output_dir}/store_performance")
store_performance.show(truncate=False)

# Stop Spark session
spark.stop() 