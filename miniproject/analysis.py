from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window

# Initialize Spark Session with BigQuery configuration
spark = SparkSession.builder \
    .appName("ParchAndPoseyAnalysis") \
    .config("spark.jars", "jars/spark-3.4-bigquery-0.34.1.jar") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.host", "127.0.0.1") \
    .getOrCreate()

# BigQuery project information
project_id = "parch-and-posey-16"
dataset_id = "data_warehouse"

# Read data from BigQuery
def read_bigquery_table(table_name):
    return spark.read.format("bigquery") \
        .option("table", f"{project_id}.{dataset_id}.{table_name}") \
        .load()

orders_df = read_bigquery_table("orders")
accounts_df = read_bigquery_table("accounts")
web_events_df = read_bigquery_table("web_events")

# 1. Total revenue by product type
def revenue_by_product_type():
    revenue_by_product = orders_df.groupBy() \
        .agg(sum("standard_amt_usd").alias("total_standard"),
             sum("gloss_amt_usd").alias("total_gloss"),
             sum("poster_amt_usd").alias("total_poster"))
    print("Revenue by product type:")
    revenue_by_product.show()

# 2. Top 5 customers with highest sales
def top_5_customers():
    top_customers = orders_df.join(accounts_df, orders_df.account_id == accounts_df.id) \
        .groupBy("account_id", "name") \
        .agg(sum("total_amt_usd").alias("total_revenue")) \
        .orderBy(desc("total_revenue")) \
        .limit(5)
    print("Top 5 customers with highest sales:")
    top_customers.show()

# 3. Monthly sales
def monthly_sales():
    monthly_sales = orders_df.withColumn("month", month("occurred_at")) \
        .groupBy("month") \
        .agg(sum("total_amt_usd").alias("monthly_revenue")) \
        .orderBy("month")
    print("Monthly sales:")
    monthly_sales.show()

# 4. Number of orders by region
def orders_by_region():
    sales_reps_df = read_bigquery_table("sales_reps")
    region_df = read_bigquery_table("region")
    
    region_orders = orders_df.join(accounts_df, orders_df.account_id == accounts_df.id) \
        .join(sales_reps_df, accounts_df["sales_rep_id"] == sales_reps_df.id) \
        .join(region_df, sales_reps_df.region_id == region_df.id) \
        .groupBy(region_df.name) \
        .agg(count("*").alias("order_count")) \
        .orderBy(desc("order_count"))
    print("Number of orders by region:")
    region_orders.show()

# 5. Conversion rate of web_events
def web_events_conversion():
    conversion_rate = web_events_df.groupBy("channel") \
        .agg(count("*").alias("total_events"),
             sum(when(col("channel") == "direct", 1).otherwise(0)).alias("direct_conversions")) \
        .withColumn("conversion_rate", col("direct_conversions") / col("total_events"))
    print("Conversion rate of web_events:")
    conversion_rate.show()

# 6. Average order value by sales representative
def avg_order_value_by_sales_rep():
    sales_reps_df = read_bigquery_table("sales_reps")
    
    avg_order_value = orders_df.join(accounts_df, orders_df.account_id == accounts_df.id) \
        .join(sales_reps_df, accounts_df.sales_rep_id == sales_reps_df.id) \
        .groupBy(sales_reps_df.name.alias("sales_rep")) \
        .agg(
            avg("total_amt_usd").alias("avg_order_value"),
            count("*").alias("total_orders")
        ) \
        .orderBy(desc("avg_order_value"))
    
    print("Average order value by sales representative:")
    avg_order_value.show()

# 7. Year-over-year growth in total sales
def yoy_sales_growth():
    yearly_sales = orders_df \
        .withColumn("year", year("occurred_at")) \
        .groupBy("year") \
        .agg(sum("total_amt_usd").alias("total_sales")) \
        .orderBy("year")
    
    window_spec = Window.orderBy("year")
    
    yoy_growth = yearly_sales \
        .withColumn("prev_year_sales", lag("total_sales").over(window_spec)) \
        .withColumn("yoy_growth", 
                    (col("total_sales") - col("prev_year_sales")) / col("prev_year_sales") * 100) \
        .orderBy("year")
    
    print("Year-over-year growth in total sales:")
    yoy_growth.show()

# Perform analyses
if __name__ == "__main__":
    # revenue_by_product_type()
    # top_5_customers()
    # monthly_sales()
    # orders_by_region()
    # web_events_conversion()
    avg_order_value_by_sales_rep()
    yoy_sales_growth()

# Close Spark Session
spark.stop()
