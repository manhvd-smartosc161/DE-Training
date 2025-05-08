from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os

def create_spark_session():
    """Create and return a Spark session with BigQuery connector"""
    return (SparkSession.builder \
        .appName("BigQuery Spark Job - Sales Analysis") \
        .config("spark.jars.packages", "com.google.cloud.spark:spark-bigquery-with-dependencies_2.12:0.32.0") \
        .getOrCreate())

def read_from_bigquery(spark, project_id, dataset, table):
    """Read data from BigQuery into a Spark DataFrame"""
    # Format: project_id.dataset.table
    table_path = f"{project_id}.{dataset}.{table}"
    
    # Read data from BigQuery với cấu hình temporaryGcsBucket một lần nữa
    df = spark.read.format("bigquery") \
        .option("table", table_path) \
        .load()
    
    return df

def write_to_bigquery(df, project_id, dataset, table, mode="overwrite"):
    """Write Spark DataFrame to BigQuery table"""
    # Format: project_id.dataset.table
    table_path = f"{project_id}.{dataset}.{table}"
    
    # Write data to BigQuery với cấu hình temporaryGcsBucket
    df.write.format("bigquery") \
        .option("table", table_path) \
        .option("writeMethod", "direct") \
        .mode(mode) \
        .save()
    
    print(f"Successfully wrote data to {table_path}")

# [phần save_to_file giữ nguyên]

def main():
    # Initialize Spark session
    spark = create_spark_session()
    
    # BigQuery configuration
    project_id = "training-de-457603"
    dataset = "bigquery_train"
    table = "sales_data"
    
    try:
        # Read data from BigQuery
        df = read_from_bigquery(spark, project_id, dataset, table)
        
        print("Schema:")
        df.printSchema()
        
        print("\nSample data:")
        df.show(5)
        
        # 1. Calculate total forecasted revenue by region
        revenue_by_region = df.groupBy("Region") \
            .agg(sum("Forecasted Monthly Revenue").alias("Total_Forecasted_Revenue")) \
            .orderBy(desc("Total_Forecasted_Revenue"))
        
        print("\nTotal Forecasted Revenue by Region:")
        revenue_by_region.show()
        
        # 2. Calculate opportunity stages distribution
        stage_distribution = df.groupBy("Opportunity Stage") \
            .count() \
            .orderBy(desc("count"))
        
        print("\nOpportunity Stages Distribution:")
        stage_distribution.show()
        
        # Write analysis results back to BigQuery
        print("\nWriting analysis results back to BigQuery...")
        
        # 1. Write revenue by region to BigQuery
        write_to_bigquery(
            revenue_by_region,
            project_id,
            dataset,
            "revenue_by_region",
            "overwrite"
        )
        
        # 2. Write opportunity stages distribution to BigQuery
        write_to_bigquery(
            stage_distribution,
            project_id,
            dataset,
            "stage_distribution",
            "overwrite"
        )
        
        print("All analysis results have been written to BigQuery successfully.")
        
    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()
