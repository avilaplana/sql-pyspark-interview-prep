# PySpark Practice Solutions - Complete Answer Key

## 🟢 BEGINNER LEVEL SOLUTIONS (Queries 1-10)

### Query 1: Basic DataFrame Operations
```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder.appName("TransactionAnalysis").getOrCreate()

# Load transactions dataset
transactions_df = spark.read.option("header", "true").option("inferSchema", "true").csv("transactions.csv")

# Display basic information
print("Dataset Schema:")
transactions_df.printSchema()

print("\nFirst 10 rows:")
transactions_df.show(10)

print(f"\nTotal records: {transactions_df.count()}")
```
**Explanation**: Basic DataFrame operations including loading data, schema inspection, and data preview.

### Query 2: Column Selection and Filtering
```python
# Select specific columns and filter
filtered_df = transactions_df.select("transaction_id", "customer_id", "amount", "region") \
    .filter(col("amount") > 200)

# Show results
filtered_df.show()

# Alternative syntax
filtered_df2 = transactions_df.select("transaction_id", "customer_id", "amount", "region") \
    .where(transactions_df.amount > 200)
```
**Explanation**: Column selection using select() and filtering using filter() or where() methods.

### Query 3: Basic Aggregations
```python
# Calculate total sales and count by region
region_stats = transactions_df.groupBy("region") \
    .agg(
        sum("amount").alias("total_sales"),
        count("transaction_id").alias("transaction_count"),
        avg("amount").alias("avg_transaction_value")
    ) \
    .orderBy(desc("total_sales"))

region_stats.show()
```
**Explanation**: GroupBy operations with multiple aggregation functions and ordering.

### Query 4: Data Type Conversion
```python
from pyspark.sql.types import *
from pyspark.sql.functions import to_date, col

# Convert data types
transactions_typed = transactions_df \
    .withColumn("transaction_date", to_date(col("transaction_date"), "yyyy-MM-dd")) \
    .withColumn("amount", col("amount").cast(DoubleType())) \
    .withColumn("quantity", col("quantity").cast(IntegerType()))

# Show schema after conversion
transactions_typed.printSchema()
```
**Explanation**: Data type casting and date parsing using withColumn() and cast() functions.

### Query 5: Simple Joins
```python
# Load customer behavior data
customer_behavior_df = spark.read.option("header", "true").option("inferSchema", "true").csv("customer_behavior.csv")

# Join transactions with customer behavior
enriched_df = transactions_df.join(
    customer_behavior_df, 
    transactions_df.customer_id == customer_behavior_df.customer_id, 
    "inner"
).select(
    transactions_df["*"],
    customer_behavior_df["page_views"],
    customer_behavior_df["time_on_site"],
    customer_behavior_df["device_type"]
)

enriched_df.show(5)
```
**Explanation**: Inner join between two DataFrames with column selection to avoid duplicates.

### Query 6: String Operations
```python
# Load web logs
web_logs_df = spark.read.option("header", "true").option("inferSchema", "true").csv("web_logs.csv")

# String operations on web logs
processed_logs = web_logs_df \
    .withColumn("page_name", regexp_extract(col("page_url"), r"/([^/]+)$", 1)) \
    .withColumn("browser_type", split(col("user_agent"), " ")[0]) \
    .withColumn("is_mobile", when(col("device_type") == "mobile", 1).otherwise(0)) \
    .withColumn("url_length", length(col("page_url")))

processed_logs.select("page_url", "page_name", "browser_type", "is_mobile", "url_length").show()
```
**Explanation**: String manipulation using regex, split, when/otherwise, and length functions.

### Query 7: Basic Window Functions
```python
from pyspark.sql.window import Window

# Define window specification
window_spec = Window.partitionBy("region").orderBy("transaction_date")

# Calculate running totals
running_totals = transactions_df \
    .withColumn("running_total", sum("amount").over(window_spec)) \
    .withColumn("row_number", row_number().over(window_spec))

running_totals.select("transaction_date", "region", "amount", "running_total", "row_number").show()
```
**Explanation**: Window functions for calculating running totals and row numbers within partitions.

### Query 8: Data Cleaning
```python
# Load stock prices
stock_df = spark.read.option("header", "true").option("inferSchema", "true").csv("stock_prices.csv")

# Data cleaning operations
cleaned_stock = stock_df \
    .fillna({"pe_ratio": 0, "dividend_yield": 0}) \
    .dropna(subset=["close_price", "volume"]) \
    .withColumn("close_price", col("close_price").cast(DoubleType())) \
    .withColumn("volume", col("volume").cast(LongType()))

print("Original count:", stock_df.count())
print("Cleaned count:", cleaned_stock.count())
cleaned_stock.show()
```
**Explanation**: Data cleaning using fillna(), dropna(), and data type casting.

### Query 9: Simple Statistics
```python
# Calculate statistics for transaction amounts
stats = transactions_df.select(
    mean("amount").alias("mean_amount"),
    median("amount").alias("median_amount"),
    stddev("amount").alias("std_amount"),
    min("amount").alias("min_amount"),
    max("amount").alias("max_amount")
).collect()[0]

print(f"Mean: {stats['mean_amount']:.2f}")
print(f"Median: {stats['median_amount']:.2f}")
print(f"Std Dev: {stats['std_amount']:.2f}")
print(f"Min: {stats['min_amount']:.2f}")
print(f"Max: {stats['max_amount']:.2f}")
```
**Explanation**: Statistical calculations using built-in aggregation functions.

### Query 10: Data Export
```python
# Export to different formats
# Parquet (recommended for analytics)
transactions_df.write.mode("overwrite").parquet("output/transactions.parquet")

# JSON
transactions_df.write.mode("overwrite").json("output/transactions.json")

# CSV with specific options
transactions_df.write \
    .mode("overwrite") \
    .option("header", "true") \
    .csv("output/transactions_processed.csv")

print("Data exported to multiple formats")
```
**Explanation**: Data persistence in different formats with various options.

---

## 🟡 INTERMEDIATE LEVEL SOLUTIONS (Queries 11-20)

### Query 11: Complex Aggregations
```python
# Multiple metrics in single query
comprehensive_stats = transactions_df.groupBy("region", "payment_method") \
    .agg(
        count("transaction_id").alias("transaction_count"),
        sum("amount").alias("total_sales"),
        avg("amount").alias("avg_transaction_value"),
        max("amount").alias("max_transaction"),
        min("amount").alias("min_transaction"),
        countDistinct("customer_id").alias("unique_customers")
    ) \
    .withColumn("sales_per_customer", col("total_sales") / col("unique_customers")) \
    .orderBy(desc("total_sales"))

comprehensive_stats.show()
```
**Explanation**: Complex aggregations with multiple metrics and calculated fields.

### Query 12: Advanced Window Functions
```python
# Advanced window functions for stock analysis
stock_window = Window.partitionBy("symbol").orderBy("date")

stock_analysis = stock_df \
    .withColumn("price_change", col("close_price") - lag("close_price", 1).over(stock_window)) \
    .withColumn("price_change_pct", (col("price_change") / lag("close_price", 1).over(stock_window)) * 100) \
    .withColumn("sma_5", avg("close_price").over(stock_window.rowsBetween(-4, 0))) \
    .withColumn("sma_10", avg("close_price").over(stock_window.rowsBetween(-9, 0))) \
    .withColumn("rank_by_volume", rank().over(Window.partitionBy("date").orderBy(desc("volume"))))

stock_analysis.select("symbol", "date", "close_price", "price_change_pct", "sma_5", "rank_by_volume").show()
```
**Explanation**: Advanced window functions including lag, moving averages, and ranking.

### Query 13: UDFs (User Defined Functions)
```python
from pyspark.sql.types import StringType

# Define UDF for customer categorization
def categorize_customer(page_views, time_on_site, conversion_rate):
    if page_views > 20 and time_on_site > 1500 and conversion_rate > 0.2:
        return "High Value"
    elif page_views > 10 and time_on_site > 800 and conversion_rate > 0.1:
        return "Medium Value"
    else:
        return "Low Value"

# Register UDF
categorize_udf = udf(categorize_customer, StringType())

# Apply UDF
customer_categorized = customer_behavior_df \
    .withColumn("customer_segment", categorize_udf(col("page_views"), col("time_on_site"), col("conversion_rate")))

customer_categorized.select("customer_id", "page_views", "time_on_site", "conversion_rate", "customer_segment").show()
```
**Explanation**: User-defined functions for custom business logic implementation.

### Query 14: Complex Joins and Data Enrichment
```python
# Multi-table join for comprehensive analysis
enriched_analysis = transactions_df \
    .join(customer_behavior_df, "customer_id", "left") \
    .join(stock_df, transactions_df.transaction_date == stock_df.date, "left") \
    .select(
        transactions_df["*"],
        customer_behavior_df["device_type"],
        customer_behavior_df["age_group"],
        customer_behavior_df["income_bracket"],
        stock_df["sector"].alias("market_sector")
    ) \
    .withColumn("transaction_value_tier", 
        when(col("amount") > 500, "High")
        .when(col("amount") > 200, "Medium")
        .otherwise("Low")
    )

enriched_analysis.show(5)
```
**Explanation**: Multiple joins with data enrichment and conditional logic.

### Query 15: Time Series Analysis
```python
# Time series analysis for stock prices
stock_window = Window.partitionBy("symbol").orderBy("date")

time_series_analysis = stock_df \
    .withColumn("daily_return", (col("close_price") - lag("close_price", 1).over(stock_window)) / lag("close_price", 1).over(stock_window)) \
    .withColumn("volatility", stddev("daily_return").over(stock_window.rowsBetween(-19, 0))) \
    .withColumn("rsi", 
        when(col("close_price") > lag("close_price", 1).over(stock_window), 1).otherwise(-1)
    ) \
    .withColumn("price_trend", 
        when(col("close_price") > col("sma_5"), "Bullish")
        .when(col("close_price") < col("sma_5"), "Bearish")
        .otherwise("Neutral")
    )

time_series_analysis.select("symbol", "date", "close_price", "daily_return", "volatility", "price_trend").show()
```
**Explanation**: Time series analysis with technical indicators and trend analysis.

### Query 16: Data Pivoting
```python
# Pivot data to show sales by region and payment method
pivot_sales = transactions_df \
    .groupBy("region") \
    .pivot("payment_method") \
    .agg(sum("amount").alias("total_sales")) \
    .fillna(0)

pivot_sales.show()

# Alternative: Pivot with multiple aggregations
pivot_detailed = transactions_df \
    .groupBy("region") \
    .pivot("payment_method") \
    .agg(
        sum("amount").alias("total_sales"),
        count("transaction_id").alias("transaction_count")
    )
```
**Explanation**: Pivot operations for data reshaping and cross-tabulation.

### Query 17: Advanced Filtering and Conditional Logic
```python
# Complex business rules for customer segmentation
customer_segmentation = customer_behavior_df \
    .withColumn("engagement_score", 
        (col("page_views") * 0.3 + 
         col("time_on_site") * 0.4 + 
         col("conversion_rate") * 100 * 0.3)
    ) \
    .withColumn("customer_tier",
        when((col("engagement_score") > 80) & (col("income_bracket").isin(["75000-100000", "100000-150000"])), "Premium")
        .when((col("engagement_score") > 60) & (col("bounce_rate") < 0.3), "High Value")
        .when(col("engagement_score") > 40, "Medium Value")
        .otherwise("Low Value")
    ) \
    .withColumn("retention_risk",
        when((col("bounce_rate") > 0.5) & (col("time_on_site") < 600), "High Risk")
        .when(col("bounce_rate") > 0.4, "Medium Risk")
        .otherwise("Low Risk")
    )

customer_segmentation.select("customer_id", "engagement_score", "customer_tier", "retention_risk").show()
```
**Explanation**: Complex conditional logic with multiple criteria and business rules.

### Query 18: Performance Optimization
```python
# Performance optimization techniques
# 1. Cache frequently used DataFrames
transactions_df.cache()

# 2. Use broadcast join for small lookup tables
from pyspark.sql.functions import broadcast

small_lookup = customer_behavior_df.filter(col("customer_id") < 1010)
optimized_join = transactions_df.join(broadcast(small_lookup), "customer_id")

# 3. Repartition for better parallelism
repartitioned_df = transactions_df.repartition(8, "region")

# 4. Use column pruning
pruned_df = transactions_df.select("customer_id", "amount", "region")

# 5. Optimize aggregations
optimized_agg = transactions_df \
    .groupBy("region") \
    .agg(sum("amount").alias("total_sales")) \
    .coalesce(1)  # Reduce partitions for small result

print("Optimization techniques applied")
```
**Explanation**: Performance optimization using caching, broadcasting, repartitioning, and column pruning.

### Query 19: Data Validation and Quality Checks
```python
# Comprehensive data quality checks
def data_quality_report(df, df_name):
    print(f"\n=== Data Quality Report for {df_name} ===")
    
    # Basic statistics
    total_rows = df.count()
    print(f"Total rows: {total_rows}")
    
    # Null value analysis
    null_counts = {}
    for col_name in df.columns:
        null_count = df.filter(col(col_name).isNull()).count()
        null_counts[col_name] = null_count
        if null_count > 0:
            print(f"Column '{col_name}' has {null_count} null values ({null_count/total_rows*100:.2f}%)")
    
    # Duplicate analysis
    duplicate_count = df.count() - df.dropDuplicates().count()
    print(f"Duplicate rows: {duplicate_count}")
    
    # Data type validation
    print("\nSchema:")
    df.printSchema()
    
    return null_counts

# Apply to all datasets
transactions_quality = data_quality_report(transactions_df, "Transactions")
customer_quality = data_quality_report(customer_behavior_df, "Customer Behavior")
```
**Explanation**: Comprehensive data quality assessment with null analysis, duplicate detection, and schema validation.

### Query 20: Complex Data Transformations
```python
# Transform web logs into session analytics
session_analytics = web_logs_df \
    .withColumn("session_duration", 
        max("timestamp").over(Window.partitionBy("session_id")) - 
        min("timestamp").over(Window.partitionBy("session_id"))
    ) \
    .withColumn("page_sequence", 
        row_number().over(Window.partitionBy("session_id").orderBy("timestamp"))
    ) \
    .withColumn("is_bounce", 
        when(col("page_sequence") == 1, 1).otherwise(0)
    ) \
    .withColumn("user_journey",
        collect_list("page_url").over(Window.partitionBy("session_id").orderBy("timestamp"))
    ) \
    .groupBy("session_id", "user_id") \
    .agg(
        count("page_url").alias("page_views"),
        max("session_duration").alias("session_duration"),
        sum("is_bounce").alias("is_bounce_session"),
        max("user_journey").alias("complete_journey"),
        countDistinct("page_url").alias("unique_pages"),
        avg("response_time").alias("avg_response_time")
    ) \
    .withColumn("bounce_rate", col("is_bounce_session")) \
    .withColumn("engagement_level",
        when(col("page_views") > 10, "High")
        .when(col("page_views") > 5, "Medium")
        .otherwise("Low")
    )

session_analytics.show()
```
**Explanation**: Complex transformations to create session-based analytics from web log data.

---

## 🔴 ADVANCED LEVEL SOLUTIONS (Queries 21-30)

### Query 21: Machine Learning Integration
```python
from pyspark.ml.feature import VectorAssembler, StandardScaler, StringIndexer
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml import Pipeline

# Prepare features for ML model
feature_columns = ["page_views", "time_on_site", "bounce_rate", "conversion_rate"]

# String indexing for categorical variables
indexer = StringIndexer(inputCol="device_type", outputCol="device_type_indexed")

# Vector assembly
assembler = VectorAssembler(
    inputCols=feature_columns + ["device_type_indexed"],
    outputCol="features"
)

# Feature scaling
scaler = StandardScaler(inputCol="features", outputCol="scaled_features")

# ML pipeline
pipeline = Pipeline(stages=[indexer, assembler, scaler])
model = pipeline.fit(customer_behavior_df)

# Transform data
ml_features = model.transform(customer_behavior_df)
ml_features.select("customer_id", "features", "scaled_features").show(5)
```
**Explanation**: ML pipeline with feature engineering, scaling, and preprocessing for machine learning.

### Query 22: Streaming Data Processing
```python
from pyspark.sql.functions import window, current_timestamp

# Simulate streaming data processing
streaming_df = spark \
    .readStream \
    .format("rate") \
    .option("rowsPerSecond", 10) \
    .load() \
    .withColumn("timestamp", current_timestamp()) \
    .withColumn("value", (col("value") % 1000) + 100)  # Simulate transaction amounts

# Sliding window aggregations
streaming_aggregations = streaming_df \
    .withWatermark("timestamp", "1 minute") \
    .groupBy(
        window(col("timestamp"), "30 seconds", "10 seconds"),
        col("value")
    ) \
    .agg(
        count("*").alias("transaction_count"),
        sum("value").alias("total_amount")
    )

# Start streaming query
query = streaming_aggregations \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .start()

# Note: In real scenarios, you would handle the streaming query lifecycle
print("Streaming query configured (not started in this example)")
```
**Explanation**: Structured streaming with sliding windows and watermarking for real-time processing.

### Query 23: Advanced Analytics - Cohort Analysis
```python
# Cohort analysis for customer retention
cohort_analysis = customer_behavior_df \
    .withColumn("registration_month", date_format(col("registration_date"), "yyyy-MM")) \
    .withColumn("activity_month", date_format(col("session_date"), "yyyy-MM")) \
    .groupBy("registration_month", "activity_month") \
    .agg(
        countDistinct("customer_id").alias("active_customers")
    ) \
    .withColumn("months_since_registration", 
        months_between(to_date(col("activity_month"), "yyyy-MM"), 
                      to_date(col("registration_month"), "yyyy-MM"))
    ) \
    .groupBy("registration_month") \
    .pivot("months_since_registration") \
    .agg(first("active_customers")) \
    .fillna(0)

cohort_analysis.show()
```
**Explanation**: Cohort analysis to track customer retention over time with month-over-month analysis.

### Query 24: Graph Processing
```python
# Note: GraphFrames requires additional setup
# from graphframes import GraphFrame

# Create vertices (customers and products)
vertices = transactions_df.select("customer_id").distinct() \
    .union(transactions_df.select("product_id").distinct()) \
    .withColumnRenamed("customer_id", "id")

# Create edges (customer-product relationships)
edges = transactions_df.select("customer_id", "product_id", "amount") \
    .withColumnRenamed("customer_id", "src") \
    .withColumnRenamed("product_id", "dst")

# Create graph (commented out as GraphFrames requires additional setup)
# graph = GraphFrame(vertices, edges)

# Graph analysis would include:
# - PageRank for product popularity
# - Connected components for customer segments
# - Triangle counting for recommendation systems

print("Graph processing setup (GraphFrames required for full implementation)")
```
**Explanation**: Graph processing setup for network analysis and recommendation systems.

### Query 25: Complex Business Logic Implementation
```python
# Fraud detection and risk scoring
fraud_analysis = transactions_df \
    .withColumn("transaction_hour", hour(col("transaction_date"))) \
    .withColumn("is_weekend", 
        when(dayofweek(col("transaction_date")).isin([1, 7]), 1).otherwise(0)
    ) \
    .withColumn("amount_zscore", 
        (col("amount") - avg("amount").over(Window.partitionBy("customer_id"))) / 
        stddev("amount").over(Window.partitionBy("customer_id"))
    ) \
    .withColumn("fraud_score",
        when(col("amount") > 1000, 0.3)
        .when(col("transaction_hour") < 6, 0.2)
        .when(col("is_weekend") == 1, 0.1)
        .when(col("amount_zscore") > 2, 0.4)
        .otherwise(0.0)
    ) \
    .withColumn("risk_level",
        when(col("fraud_score") > 0.3, "High Risk")
        .when(col("fraud_score") > 0.1, "Medium Risk")
        .otherwise("Low Risk")
    ) \
    .withColumn("requires_review", col("fraud_score") > 0.2)

fraud_analysis.select("transaction_id", "customer_id", "amount", "fraud_score", "risk_level", "requires_review").show()
```
**Explanation**: Complex business logic for fraud detection with multiple risk factors and scoring.

### Query 26: Performance Tuning and Optimization
```python
# Advanced performance optimization
def optimize_dataframe(df, optimization_type="default"):
    if optimization_type == "partitioning":
        # Partition by frequently filtered columns
        return df.repartition(8, "region")
    
    elif optimization_type == "bucketing":
        # Bucket by join keys
        return df.write.bucketBy(10, "customer_id").saveAsTable("bucketed_transactions")
    
    elif optimization_type == "caching":
        # Cache with appropriate storage level
        return df.persist(StorageLevel.MEMORY_AND_DISK_SER)
    
    elif optimization_type == "column_pruning":
        # Select only necessary columns
        return df.select("customer_id", "amount", "region")
    
    elif optimization_type == "predicate_pushdown":
        # Push filters down
        return df.filter(col("amount") > 100).select("customer_id", "amount")

# Apply optimizations
optimized_df = optimize_dataframe(transactions_df, "partitioning")
cached_df = optimize_dataframe(transactions_df, "caching")

print("Performance optimizations applied")
```
**Explanation**: Advanced performance optimization techniques including partitioning, bucketing, and caching strategies.

### Query 27: Data Pipeline Architecture
```python
# Complete ETL pipeline with error handling
class DataPipeline:
    def __init__(self, spark):
        self.spark = spark
        self.errors = []
    
    def extract(self, source_path, file_format="csv"):
        try:
            if file_format == "csv":
                return self.spark.read.option("header", "true").option("inferSchema", "true").csv(source_path)
            elif file_format == "parquet":
                return self.spark.read.parquet(source_path)
        except Exception as e:
            self.errors.append(f"Extract error: {str(e)}")
            return None
    
    def transform(self, df, transformations):
        try:
            for transformation in transformations:
                df = transformation(df)
            return df
        except Exception as e:
            self.errors.append(f"Transform error: {str(e)}")
            return None
    
    def load(self, df, target_path, file_format="parquet"):
        try:
            if file_format == "parquet":
                df.write.mode("overwrite").parquet(target_path)
            elif file_format == "csv":
                df.write.mode("overwrite").option("header", "true").csv(target_path)
        except Exception as e:
            self.errors.append(f"Load error: {str(e)}")
    
    def run_pipeline(self, source_path, target_path, transformations):
        # Extract
        df = self.extract(source_path)
        if df is None:
            return False
        
        # Transform
        df = self.transform(df, transformations)
        if df is None:
            return False
        
        # Load
        self.load(df, target_path)
        
        return len(self.errors) == 0

# Usage example
pipeline = DataPipeline(spark)
transformations = [
    lambda df: df.filter(col("amount") > 0),
    lambda df: df.withColumn("processed_date", current_timestamp())
]

success = pipeline.run_pipeline("transactions.csv", "output/processed_transactions", transformations)
print(f"Pipeline success: {success}")
```
**Explanation**: Complete ETL pipeline architecture with error handling and monitoring.

### Query 28: Advanced Window Functions and Analytics
```python
# Advanced financial analytics
financial_analysis = stock_df \
    .withColumn("daily_return", 
        (col("close_price") - lag("close_price", 1).over(Window.partitionBy("symbol").orderBy("date"))) / 
        lag("close_price", 1).over(Window.partitionBy("symbol").orderBy("date"))
    ) \
    .withColumn("volatility_20d", 
        stddev("daily_return").over(Window.partitionBy("symbol").orderBy("date").rowsBetween(-19, 0))
    ) \
    .withColumn("rsi_14d", 
        # Simplified RSI calculation
        when(col("daily_return") > 0, 1).otherwise(-1)
    ) \
    .withColumn("bollinger_upper", 
        avg("close_price").over(Window.partitionBy("symbol").orderBy("date").rowsBetween(-19, 0)) + 
        2 * stddev("close_price").over(Window.partitionBy("symbol").orderBy("date").rowsBetween(-19, 0))
    ) \
    .withColumn("bollinger_lower", 
        avg("close_price").over(Window.partitionBy("symbol").orderBy("date").rowsBetween(-19, 0)) - 
        2 * stddev("close_price").over(Window.partitionBy("symbol").orderBy("date").rowsBetween(-19, 0))
    ) \
    .withColumn("price_position", 
        (col("close_price") - col("bollinger_lower")) / (col("bollinger_upper") - col("bollinger_lower"))
    )

financial_analysis.select("symbol", "date", "close_price", "daily_return", "volatility_20d", "price_position").show()
```
**Explanation**: Advanced financial analytics with technical indicators and statistical measures.

### Query 29: Data Governance and Security
```python
# Data masking and security
def mask_sensitive_data(df, sensitive_columns):
    masked_df = df
    for col_name in sensitive_columns:
        if col_name in df.columns:
            masked_df = masked_df.withColumn(
                f"{col_name}_masked",
                when(length(col(col_name)) > 4, 
                     concat(substring(col(col_name), 1, 2), lit("***"), substring(col(col_name), -2, 2))
                ).otherwise(lit("***"))
            )
    return masked_df

# Apply data masking
masked_transactions = mask_sensitive_data(transactions_df, ["customer_id"])

# Data encryption simulation
from pyspark.sql.functions import sha2, concat, lit

encrypted_df = transactions_df \
    .withColumn("encrypted_customer_id", sha2(concat(col("customer_id"), lit("salt")), 256)) \
    .withColumn("data_hash", sha2(concat(col("transaction_id"), col("amount")), 256))

# Access control simulation
def apply_access_control(df, user_role):
    if user_role == "analyst":
        return df.select("transaction_id", "amount", "region")
    elif user_role == "admin":
        return df
    else:
        return df.select("region", "amount")

# Apply access control
restricted_df = apply_access_control(transactions_df, "analyst")
print("Data governance and security measures applied")
```
**Explanation**: Data governance with masking, encryption, and access control mechanisms.

### Query 30: Scalability and Distributed Processing
```python
# Scalability and distributed processing
def optimize_for_scale(df, scale_type="large"):
    if scale_type == "large":
        # Optimize for large datasets
        return df \
            .repartition(200, "region") \
            .persist(StorageLevel.MEMORY_AND_DISK_SER_2) \
            .withColumn("partition_id", spark_partition_id())
    
    elif scale_type == "streaming":
        # Optimize for streaming
        return df \
            .withWatermark("timestamp", "1 hour") \
            .groupBy(window(col("timestamp"), "5 minutes")) \
            .agg(sum("amount").alias("total_amount"))
    
    elif scale_type == "ml":
        # Optimize for ML workloads
        return df \
            .repartition(100) \
            .persist(StorageLevel.MEMORY_ONLY_SER)

# Resource optimization
def optimize_resources():
    # Set optimal configurations
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    spark.conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")

optimize_resources()
scaled_df = optimize_for_scale(transactions_df, "large")
print("Scalability optimizations applied")
```
**Explanation**: Scalability optimization with resource management and distributed processing techniques.

---

## 🎯 Key Learning Points

### Performance Optimization
- **Caching**: Use appropriate storage levels for different use cases
- **Partitioning**: Optimize data layout for query patterns
- **Broadcasting**: Use broadcast joins for small lookup tables
- **Column Pruning**: Select only necessary columns

### Best Practices
- **Lazy Evaluation**: Understand when operations are executed
- **Error Handling**: Implement robust error handling and monitoring
- **Data Quality**: Always validate and clean data
- **Resource Management**: Optimize for your cluster resources

### Advanced Concepts
- **Window Functions**: Master analytical functions for complex calculations
- **UDFs**: Create custom functions for business logic
- **Streaming**: Handle real-time data processing
- **ML Integration**: Prepare data for machine learning workflows

## 🚀 Interview Tips

1. **Explain Performance Implications**: Always discuss the performance impact of your solutions
2. **Consider Scalability**: Think about how solutions scale with data size
3. **Handle Edge Cases**: Address data quality and error scenarios
4. **Know the Trade-offs**: Understand when to use different approaches
5. **Practice Optimization**: Be prepared to optimize existing code

Remember: Focus on understanding Spark's distributed computing model and how to leverage it effectively! 🚀
