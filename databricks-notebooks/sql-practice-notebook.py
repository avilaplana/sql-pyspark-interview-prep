# Databricks notebook source
# MAGIC %md
# MAGIC # SQL Interview Practice Notebook
# MAGIC 
# MAGIC This notebook contains practice exercises for SQL interview questions.
# MAGIC 
# MAGIC ## Setup Instructions
# MAGIC 1. Upload the sample datasets to your Databricks workspace
# MAGIC 2. Create tables from the uploaded data
# MAGIC 3. Run the exercises below
# MAGIC 
# MAGIC ## Datasets Available
# MAGIC - sales_data: Sales transactions with products, customers, and salespeople
# MAGIC - employee_hierarchy: Employee data with manager relationships
# MAGIC - time_series_data: Sensor data with timestamps
# MAGIC 
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Setup and Data Loading
# MAGIC 
# MAGIC First, let's load the sample datasets into tables.

# COMMAND ----------

# Load sales data
%sql
CREATE TABLE IF NOT EXISTS sales (
    sales_id INT,
    product_id INT,
    salesperson_id INT,
    sale_date DATE,
    amount DECIMAL(10,2),
    region VARCHAR(50),
    customer_id INT,
    discount_percent DECIMAL(5,2),
    tax_amount DECIMAL(10,2)
) USING DELTA;

-- Insert sample data (you would upload this from your CSV files)
-- INSERT INTO sales VALUES (1, 101, 201, '2024-01-15', 1500.00, 'North', 1001, 5.00, 120.00);
-- ... (rest of the data)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Advanced SQL Practice Questions
# MAGIC 
# MAGIC ### Question 1: Complex Window Functions
# MAGIC Find the top 3 salespeople by total sales amount within each region, and show their rank and percentage of total regional sales.

# COMMAND ----------

%sql
-- Your solution here
-- Hint: Use ROW_NUMBER(), SUM() OVER(), and percentage calculations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 2: Recursive CTE for Hierarchical Data
# MAGIC Find all employees at any level under a specific manager and calculate total salary cost.

# COMMAND ----------

%sql
-- Your solution here
-- Hint: Use WITH RECURSIVE for hierarchical queries

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 3: Complex Date Range Analysis
# MAGIC Find users who had at least 3 different actions within any 30-minute window.

# COMMAND ----------

%sql
-- Your solution here
-- Hint: Use window functions with time-based frames

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 4: Advanced Aggregation with Multiple Metrics
# MAGIC Create a comprehensive dashboard showing sales trends, customer acquisition, and product performance.

# COMMAND ----------

%sql
-- Your solution here
-- Hint: Use multiple CTEs and complex aggregations

# COMMAND ----------

# MAGIC %md
# MAGIC ### Question 5: Performance Optimization
# MAGIC Analyze and optimize a slow-running query on a large dataset.

# COMMAND ----------

%sql
-- Your solution here
-- Hint: Consider indexing, query structure, and execution plans

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Practice Tips
# MAGIC 
# MAGIC 1. **Start with the business problem** - Understand what you're trying to achieve
# MAGIC 2. **Think about performance** - Consider indexing and query optimization
# MAGIC 3. **Handle edge cases** - NULL values, duplicates, data quality
# MAGIC 4. **Explain your approach** - Walk through your thought process
# MAGIC 5. **Test with sample data** - Always validate your results
# MAGIC 
# MAGIC ## 4. Additional Exercises
# MAGIC 
# MAGIC Try these additional challenges:
# MAGIC - Implement data quality checks
# MAGIC - Create complex reporting queries
# MAGIC - Handle schema evolution
# MAGIC - Design optimal data models
# MAGIC - Implement data validation rules

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Resources
# MAGIC 
# MAGIC - [SQL Best Practices](https://docs.databricks.com/sql/language-manual/index.html)
# MAGIC - [Performance Tuning Guide](https://docs.databricks.com/optimizations/index.html)
# MAGIC - [Delta Lake Documentation](https://docs.databricks.com/delta/index.html)
