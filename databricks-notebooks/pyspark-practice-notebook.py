# Databricks notebook source
# MAGIC %md
# MAGIC # PySpark Interview Practice Notebook
# MAGIC 
# MAGIC This notebook contains practice exercises for PySpark interview questions.
# MAGIC 
# MAGIC ## Setup Instructions
# MAGIC 1. Upload the sample datasets to your Databricks workspace
# MAGIC 2. Run the data generation script to create large-scale datasets
# MAGIC 3. Practice the exercises below
# MAGIC 
# MAGIC ## Datasets Available
# MAGIC - transactions: Large-scale transaction data (1M+ records)
# MAGIC - user_behavior: User interaction data (500K+ records)
# MAGIC - product_catalog: Product information (10K+ records)
# MAGIC - sensor_data: IoT sensor data (2M+ records)
# MAGIC - financial_data: Market data (500K+ records)
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Data Loading
# MAGIC 
# MAGIC First, let's set up the Spark session and load our datasets.

# COMMAND ----------

# Import required libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import time

# Create Spark session with optimal configuration
spark = SparkSession.builder \
    .appName("PySparkInterviewPrep") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
    .getOrCreate()

# Set log level to reduce noise
spark.sparkContext.setLogLevel("WARN")

print("Spark session created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Load Sample Datasets
# MAGIC 
# MAGIC Load the generated datasets for practice.

# COMMAND ----------

# Load datasets (adjust paths based on your setup)
try:
    # Load transactions data
    transactions_df = spark.read.parquet("/tmp/interview_data/transactions")
    print(f"Transactions data loaded: {transactions_df.count()} records")
    
    # Load user behavior data
    user_behavior_df = spark.read.parquet("/tmp/interview_data/user_behavior")
    print(f"User behavior data loaded: {user_behavior_df.count()} records")
    
    # Load product catalog data
    product_catalog_df = spark.read.parquet("/tmp/interview_data/product_catalog")
    print(f"Product catalog data loaded: {product_catalog_df.count()} records")
    
    # Load sensor data
    sensor_data_df = spark.read.parquet("/tmp/interview_data/sensor_data")
    print(f"Sensor data loaded: {sensor_data_df.count()} records")
    
    # Load financial data
    financial_data_df = spark.read.parquet("/tmp/interview_data/financial_data")
    print(f"Financial data loaded: {financial_data_df.count()} records")
    
except Exception as e:
    print(f"Error loading datasets: {e}")
    print("Please run the data generation script first!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Advanced PySpark Practice Questions
# MAGIC 
# MAGIC ### Question 1: Complex DataFrame Operations with Window Functions
# MAGIC Find the top 3 salespeople by total sales amount within each region, calculate their rank, and show their percentage of total regional sales.

# COMMAND ----------

# Your solution here
# Hint: Use window functions, partitioning, and percentage calculations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 2: RDD vs DataFrame Performance Optimization
# MAGIC Compare RDD and DataFrame approaches for the same transformation and benchmark performance.

# COMMAND ----------

# Your solution here
# Hint: Implement both RDD and DataFrame solutions, then benchmark

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 3: Complex Joins with Broadcast Variables
# MAGIC Implement an optimized join strategy using broadcast variables for large fact table and small dimension table.

# COMMAND ----------

# Your solution here
# Hint: Use broadcast() function for small tables

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 4: Custom Accumulators for Business Metrics
# MAGIC Create custom accumulators to track business metrics during data processing.

# COMMAND ----------

# Your solution here
# Hint: Create custom accumulator classes and implement error handling

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 5: Advanced Caching and Persistence Strategies
# MAGIC Design an optimal caching strategy for a complex data pipeline with multiple transformations.

# COMMAND ----------

# Your solution here
# Hint: Use different storage levels and implement cache management

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 6: Complex Data Quality and Validation Pipeline
# MAGIC Create a comprehensive data quality framework that validates data and handles schema evolution.

# COMMAND ----------

# Your solution here
# Hint: Implement data quality checks, schema validation, and error reporting

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 7: Advanced Partitioning and Bucketing
# MAGIC Design partitioning and bucketing strategies for optimal query performance.

# COMMAND ----------

# Your solution here
# Hint: Use repartition(), coalesce(), and bucketBy() functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 8: Complex Time Series Processing
# MAGIC Implement time series operations including gap filling, interpolation, and anomaly detection.

# COMMAND ----------

# Your solution here
# Hint: Use window functions with time-based frames and statistical functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 9: Advanced UDFs and Custom Functions
# MAGIC Create optimized UDFs for complex business logic that can't be expressed with built-in functions.

# COMMAND ----------

# Your solution here
# Hint: Create UDFs with proper error handling and performance optimization

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 10: Complex Data Pipeline with Error Recovery
# MAGIC Design a robust pipeline with checkpointing, error recovery, retry logic, and monitoring.

# COMMAND ----------

# Your solution here
# Hint: Implement checkpointing, error handling, and monitoring mechanisms

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Performance Optimization Exercises
# MAGIC 
# MAGIC ### Exercise 1: Query Optimization
# MAGIC Analyze and optimize a slow-running PySpark job.

# COMMAND ----------

# Your solution here
# Hint: Use explain(), analyze bottlenecks, and implement optimizations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 2: Memory Management
# MAGIC Implement memory management strategies for large-scale data processing.

# COMMAND ----------

# Your solution here
# Hint: Use caching strategies, memory optimization, and resource management

# COMMAND ----------

# MAGIC %md
# MAGIC ### Exercise 3: Data Skew Handling
# MAGIC Handle data skew in large datasets for optimal performance.

# COMMAND ----------

# Your solution here
# Hint: Use salting, custom partitioning, and skew detection

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Practice Tips
# MAGIC 
# MAGIC 1. **Understand the problem** - Always clarify requirements and constraints
# MAGIC 2. **Think about scalability** - Consider performance with large datasets
# MAGIC 3. **Handle edge cases** - Data quality, null values, schema evolution
# MAGIC 4. **Explain your approach** - Walk through your design decisions
# MAGIC 5. **Consider alternatives** - Discuss different approaches and trade-offs
# MAGIC 6. **Think about production** - Monitoring, error handling, maintenance
# MAGIC 7. **Optimize for performance** - Caching, partitioning, resource management
# MAGIC 
# MAGIC ## 6. Additional Challenges
# MAGIC 
# MAGIC Try these advanced scenarios:
# MAGIC - Implement real-time streaming with Structured Streaming
# MAGIC - Create machine learning pipelines with MLlib
# MAGIC - Design data lake architectures
# MAGIC - Implement data governance and security
# MAGIC - Build monitoring and alerting systems
# MAGIC 
# MAGIC ## 7. Resources
# MAGIC 
# MAGIC - [PySpark Documentation](https://spark.apache.org/docs/latest/api/python/)
# MAGIC - [Performance Tuning Guide](https://spark.apache.org/docs/latest/sql-performance-tuning.html)
# MAGIC - [Delta Lake Documentation](https://docs.databricks.com/delta/index.html)
# MAGIC - [Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)

# COMMAND ----------

# Clean up
spark.stop()
print("Spark session stopped.")
