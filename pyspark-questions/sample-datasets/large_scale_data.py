# Large Scale Dataset for PySpark Practice
# This script generates sample data for PySpark interview questions

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import random
from datetime import datetime, timedelta

def create_spark_session():
    """Create Spark session with optimal configuration"""
    return SparkSession.builder \
        .appName("InterviewPrep") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .getOrCreate()

def generate_transactions_data(spark, num_records=1000000):
    """Generate large-scale transaction data"""
    
    # Define schema
    schema = StructType([
        StructField("transaction_id", LongType(), True),
        StructField("account_id", LongType(), True),
        StructField("amount", DoubleType(), True),
        StructField("transaction_date", TimestampType(), True),
        StructField("transaction_type", StringType(), True),
        StructField("merchant_id", LongType(), True),
        StructField("region", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("status", StringType(), True)
    ])
    
    # Generate data
    data = []
    regions = ['North', 'South', 'East', 'West', 'Central']
    transaction_types = ['Purchase', 'Transfer', 'Withdrawal', 'Deposit', 'Refund']
    currencies = ['USD', 'EUR', 'GBP', 'JPY', 'CAD']
    statuses = ['Completed', 'Pending', 'Failed', 'Cancelled']
    
    for i in range(num_records):
        data.append((
            i + 1,
            random.randint(1, 10000),
            round(random.uniform(10.0, 10000.0), 2),
            datetime.now() - timedelta(days=random.randint(0, 365)),
            random.choice(transaction_types),
            random.randint(1, 1000),
            random.choice(regions),
            random.choice(currencies),
            random.choice(statuses)
        ))
    
    return spark.createDataFrame(data, schema)

def generate_user_behavior_data(spark, num_records=500000):
    """Generate user behavior data for analytics"""
    
    schema = StructType([
        StructField("user_id", LongType(), True),
        StructField("session_id", StringType(), True),
        StructField("page_url", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("action_type", StringType(), True),
        StructField("duration_seconds", IntegerType(), True),
        StructField("device_type", StringType(), True),
        StructField("browser", StringType(), True),
        StructField("country", StringType(), True)
    ])
    
    data = []
    action_types = ['page_view', 'click', 'scroll', 'form_submit', 'purchase', 'signup']
    device_types = ['mobile', 'desktop', 'tablet']
    browsers = ['Chrome', 'Firefox', 'Safari', 'Edge']
    countries = ['US', 'CA', 'UK', 'DE', 'FR', 'JP', 'AU', 'BR']
    
    for i in range(num_records):
        data.append((
            random.randint(1, 50000),
            f"session_{random.randint(1, 100000)}",
            f"https://example.com/page{random.randint(1, 100)}",
            datetime.now() - timedelta(minutes=random.randint(0, 1440)),
            random.choice(action_types),
            random.randint(1, 300),
            random.choice(device_types),
            random.choice(browsers),
            random.choice(countries)
        ))
    
    return spark.createDataFrame(data, schema)

def generate_product_catalog_data(spark, num_records=10000):
    """Generate product catalog data"""
    
    schema = StructType([
        StructField("product_id", LongType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("subcategory", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("supplier_id", LongType(), True),
        StructField("created_date", DateType(), True),
        StructField("is_active", BooleanType(), True),
        StructField("inventory_count", IntegerType(), True),
        StructField("rating", DoubleType(), True)
    ])
    
    data = []
    categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports', 'Beauty', 'Automotive']
    subcategories = {
        'Electronics': ['Phones', 'Laptops', 'Tablets', 'Accessories'],
        'Clothing': ['Shirts', 'Pants', 'Shoes', 'Accessories'],
        'Books': ['Fiction', 'Non-Fiction', 'Technical', 'Children'],
        'Home': ['Furniture', 'Decor', 'Kitchen', 'Bathroom'],
        'Sports': ['Fitness', 'Outdoor', 'Team Sports', 'Water Sports'],
        'Beauty': ['Skincare', 'Makeup', 'Hair', 'Fragrance'],
        'Automotive': ['Parts', 'Accessories', 'Tools', 'Maintenance']
    }
    
    for i in range(num_records):
        category = random.choice(categories)
        subcategory = random.choice(subcategories[category])
        
        data.append((
            i + 1,
            f"Product_{i+1}",
            category,
            subcategory,
            round(random.uniform(10.0, 1000.0), 2),
            random.randint(1, 100),
            datetime.now().date() - timedelta(days=random.randint(0, 365)),
            random.choice([True, False]),
            random.randint(0, 1000),
            round(random.uniform(1.0, 5.0), 1)
        ))
    
    return spark.createDataFrame(data, schema)

def generate_sensor_data(spark, num_records=2000000):
    """Generate IoT sensor data for time series analysis"""
    
    schema = StructType([
        StructField("sensor_id", LongType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("value", DoubleType(), True),
        StructField("sensor_type", StringType(), True),
        StructField("location", StringType(), True),
        StructField("status", StringType(), True),
        StructField("battery_level", DoubleType(), True),
        StructField("temperature", DoubleType(), True)
    ])
    
    data = []
    sensor_types = ['Temperature', 'Humidity', 'Pressure', 'Motion', 'Light', 'Sound']
    locations = ['Building_A', 'Building_B', 'Building_C', 'Building_D']
    statuses = ['Active', 'Inactive', 'Maintenance', 'Error']
    
    for i in range(num_records):
        data.append((
            random.randint(1, 1000),
            datetime.now() - timedelta(minutes=random.randint(0, 10080)),  # 7 days
            round(random.uniform(0.0, 100.0), 2),
            random.choice(sensor_types),
            random.choice(locations),
            random.choice(statuses),
            round(random.uniform(0.0, 100.0), 1),
            round(random.uniform(-10.0, 50.0), 1)
        ))
    
    return spark.createDataFrame(data, schema)

def generate_financial_data(spark, num_records=500000):
    """Generate financial market data"""
    
    schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("timestamp", TimestampType(), True),
        StructField("open_price", DoubleType(), True),
        StructField("high_price", DoubleType(), True),
        StructField("low_price", DoubleType(), True),
        StructField("close_price", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("market_cap", DoubleType(), True),
        StructField("sector", StringType(), True)
    ])
    
    data = []
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'META', 'NVDA', 'NFLX', 'AMD', 'INTC']
    sectors = ['Technology', 'Healthcare', 'Finance', 'Energy', 'Consumer', 'Industrial']
    
    for i in range(num_records):
        symbol = random.choice(symbols)
        base_price = random.uniform(50.0, 500.0)
        
        data.append((
            symbol,
            datetime.now() - timedelta(minutes=random.randint(0, 10080)),
            round(base_price, 2),
            round(base_price * random.uniform(1.0, 1.1), 2),
            round(base_price * random.uniform(0.9, 1.0), 2),
            round(base_price * random.uniform(0.95, 1.05), 2),
            random.randint(1000, 1000000),
            round(base_price * random.uniform(1000000, 10000000), 2),
            random.choice(sectors)
        ))
    
    return spark.createDataFrame(data, schema)

def save_datasets_to_parquet(spark, output_path="/tmp/interview_data"):
    """Generate and save all datasets to Parquet format"""
    
    print("Generating transactions data...")
    transactions_df = generate_transactions_data(spark, 1000000)
    transactions_df.write.mode("overwrite").parquet(f"{output_path}/transactions")
    
    print("Generating user behavior data...")
    user_behavior_df = generate_user_behavior_data(spark, 500000)
    user_behavior_df.write.mode("overwrite").parquet(f"{output_path}/user_behavior")
    
    print("Generating product catalog data...")
    product_catalog_df = generate_product_catalog_data(spark, 10000)
    product_catalog_df.write.mode("overwrite").parquet(f"{output_path}/product_catalog")
    
    print("Generating sensor data...")
    sensor_data_df = generate_sensor_data(spark, 2000000)
    sensor_data_df.write.mode("overwrite").parquet(f"{output_path}/sensor_data")
    
    print("Generating financial data...")
    financial_data_df = generate_financial_data(spark, 500000)
    financial_data_df.write.mode("overwrite").parquet(f"{output_path}/financial_data")
    
    print(f"All datasets saved to {output_path}")
    
    return {
        "transactions": transactions_df,
        "user_behavior": user_behavior_df,
        "product_catalog": product_catalog_df,
        "sensor_data": sensor_data_df,
        "financial_data": financial_data_df
    }

if __name__ == "__main__":
    # Create Spark session
    spark = create_spark_session()
    
    # Generate and save datasets
    datasets = save_datasets_to_parquet(spark)
    
    # Show sample data
    print("\nSample transactions data:")
    datasets["transactions"].show(5)
    
    print("\nSample user behavior data:")
    datasets["user_behavior"].show(5)
    
    print("\nSample product catalog data:")
    datasets["product_catalog"].show(5)
    
    print("\nSample sensor data:")
    datasets["sensor_data"].show(5)
    
    print("\nSample financial data:")
    datasets["financial_data"].show(5)
    
    spark.stop()
