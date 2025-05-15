from fastapi import FastAPI, HTTPException
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from typing import List, Dict, Any
import json
from datetime import datetime

app = FastAPI(
    title="Parch and Posey Analysis API",
    description="API for analyzing Parch and Posey sales data using PySpark",
    version="1.0.0"
)

# Initialize Spark Session with BigQuery configuration
spark = SparkSession.builder \
    .appName("ParchAndPoseyAnalysisAPI") \
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

# Load data once when the API starts
orders_df = read_bigquery_table("orders")
accounts_df = read_bigquery_table("accounts")
web_events_df = read_bigquery_table("web_events")
sales_reps_df = read_bigquery_table("sales_reps")
region_df = read_bigquery_table("region")

# Helper function to convert DataFrame to JSON
def df_to_json(df):
    # Convert DataFrame to list of dictionaries
    columns = df.columns
    data = []
    for row in df.collect():
        data.append({col: row[col] for col in columns})
    return data

@app.get("/")
async def root():
    return {"message": "Welcome to Parch and Posey Analysis API"}

@app.get("/revenue-by-product")
async def revenue_by_product_type():
    """Total revenue by product type"""
    try:
        revenue_by_product = orders_df.groupBy() \
            .agg(sum("standard_amt_usd").alias("total_standard"),
                 sum("gloss_amt_usd").alias("total_gloss"),
                 sum("poster_amt_usd").alias("total_poster"))
        return df_to_json(revenue_by_product)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/top-customers")
async def top_5_customers():
    """Top 5 customers with highest revenue"""
    try:
        top_customers = orders_df.join(accounts_df, orders_df.account_id == accounts_df.id) \
            .groupBy("account_id", "name") \
            .agg(sum("total_amt_usd").alias("total_revenue")) \
            .orderBy(desc("total_revenue")) \
            .limit(5)
        return df_to_json(top_customers)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/monthly-sales")
async def monthly_sales():
    """Monthly sales"""
    try:
        monthly_sales = orders_df.withColumn("month", month("occurred_at")) \
            .groupBy("month") \
            .agg(sum("total_amt_usd").alias("monthly_revenue")) \
            .orderBy("month")
        return df_to_json(monthly_sales)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/orders-by-region")
async def orders_by_region():
    """Number of orders by region"""
    try:
        region_orders = orders_df.join(accounts_df, orders_df.account_id == accounts_df.id) \
            .join(sales_reps_df, accounts_df["sales_rep_id"] == sales_reps_df.id) \
            .join(region_df, sales_reps_df.region_id == region_df.id) \
            .groupBy(region_df.name) \
            .agg(count("*").alias("order_count")) \
            .orderBy(desc("order_count"))
        return df_to_json(region_orders)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/web-events-conversion")
async def web_events_conversion():
    """Web events conversion rate"""
    try:
        conversion_rate = web_events_df.groupBy("channel") \
            .agg(count("*").alias("total_events"),
                 sum(when(col("channel") == "direct", 1).otherwise(0)).alias("direct_conversions")) \
            .withColumn("conversion_rate", col("direct_conversions") / col("total_events"))
        return df_to_json(conversion_rate)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/yoy-sales-growth")
async def yoy_sales_growth():
    """Year-over-year growth in total sales"""
    try:
        yearly_sales = orders_df \
            .withColumn("year", year("occurred_at")) \
            .groupBy("year") \
            .agg(sum("total_amt_usd").alias("total_sales")) \
            .orderBy("year")
        
        window_spec = Window.partitionBy(lit(1)).orderBy("year")
        
        yoy_growth = yearly_sales \
            .withColumn("prev_year_sales", lag("total_sales").over(window_spec)) \
            .withColumn("yoy_growth", 
                        (col("total_sales") - col("prev_year_sales")) / col("prev_year_sales") * 100) \
            .orderBy("year")
        
        return df_to_json(yoy_growth)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/avg-order-value-by-sales-rep")
async def avg_order_value_by_sales_rep():
    """Average order value by sales representative"""
    try:
        avg_order_value = orders_df.join(accounts_df, orders_df.account_id == accounts_df.id) \
            .join(sales_reps_df, accounts_df["sales_rep_id"] == sales_reps_df.id) \
            .groupBy(sales_reps_df.name.alias("sales_rep")) \
            .agg(
                avg("total_amt_usd").alias("avg_order_value"),
                count("*").alias("total_orders")
            ) \
            .orderBy(desc("avg_order_value"))
        
        return df_to_json(avg_order_value)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Close Spark Session when the API shuts down
@app.on_event("shutdown")
def shutdown_event():
    spark.stop()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 

      