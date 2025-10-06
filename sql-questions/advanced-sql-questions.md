# 25 Advanced SQL Interview Questions

## Question 1: Complex Window Functions with Multiple Partitions
**Scenario**: You have a sales table with columns: `sales_id`, `product_id`, `salesperson_id`, `sale_date`, `amount`, `region`. 

**Task**: Write a query to find the top 3 salespeople by total sales amount within each region, and also show their rank within the region and their percentage of total regional sales.

**Expected Skills**: Window functions, ROW_NUMBER(), RANK(), percentage calculations

---

## Question 2: Recursive CTE for Hierarchical Data
**Scenario**: You have an employee table with `employee_id`, `name`, `manager_id`, `department`, `salary`. Some employees have managers, creating a hierarchy.

**Task**: Write a recursive CTE to find all employees at any level under a specific manager, and calculate the total salary cost for each manager's entire team.

**Expected Skills**: Recursive CTEs, hierarchical queries, aggregation

---

## Question 3: Pivot with Dynamic Columns
**Scenario**: You have a table with `student_id`, `subject`, `grade`, `semester`. 

**Task**: Create a pivot table showing each student's grades across all subjects, with subjects as columns. Handle cases where students might not have grades in all subjects.

**Expected Skills**: PIVOT operations, dynamic SQL concepts, NULL handling

---

## Question 4: Complex Date Range Analysis
**Scenario**: You have a table with `user_id`, `action`, `timestamp`, `session_id`. 

**Task**: Find users who had at least 3 different actions within any 30-minute window, and show the time range and action sequence.

**Expected Skills**: Date functions, window functions, complex filtering

---

## Question 5: Self-Join with Aggregation
**Scenario**: You have a transactions table with `transaction_id`, `from_account`, `to_account`, `amount`, `transaction_date`.

**Task**: Find all account pairs that have had transactions in both directions (A→B and B→A) and calculate the net flow between them.

**Expected Skills**: Self-joins, conditional aggregation, complex WHERE clauses

---

## Question 6: Advanced Subquery with Correlated Subquery
**Scenario**: You have tables: `orders` (order_id, customer_id, order_date, total_amount) and `order_items` (item_id, order_id, product_id, quantity, price).

**Task**: Find customers who have placed orders where the total amount is greater than the average order amount for that specific product category.

**Expected Skills**: Correlated subqueries, multiple table joins, aggregation

---

## Question 7: Window Functions with Frame Specification
**Scenario**: You have a stock_prices table with `symbol`, `date`, `price`, `volume`.

**Task**: Calculate the 7-day moving average price, the price change from the previous day, and identify days where the price was the highest in the last 30 days.

**Expected Skills**: Window frame specifications, LAG(), MAX() with frames

---

## Question 8: Complex CASE Statements with Aggregation
**Scenario**: You have a customer_activity table with `customer_id`, `activity_type`, `activity_date`, `value`.

**Task**: Create a summary showing for each customer: total purchases, total returns, net value, and a customer segment (High/Medium/Low) based on net value percentiles.

**Expected Skills**: CASE statements, percentiles, complex aggregation

---

## Question 9: Multiple CTEs with Complex Logic
**Scenario**: You have tables: `products`, `categories`, `sales`, `inventory`.

**Task**: Find products that are in the top 10% by sales volume but have inventory levels below the category average, and suggest reorder quantities.

**Expected Skills**: Multiple CTEs, percentiles, complex joins, calculations

---

## Question 10: Advanced String Manipulation
**Scenario**: You have a table with `full_name`, `email`, `phone`, `address`.

**Task**: Parse the full_name into first_name, middle_name, last_name, extract domain from email, format phone numbers consistently, and validate email format.

**Expected Skills**: String functions, REGEX, data cleaning

---

## Question 11: Time Series Analysis with Gaps
**Scenario**: You have a sensor_data table with `sensor_id`, `timestamp`, `value`, `status`.

**Task**: Identify gaps in data collection (missing timestamps), interpolate missing values, and detect anomalies (values that are 3 standard deviations from the mean).

**Expected Skills**: Time series analysis, statistical functions, gap detection

---

## Question 12: Complex Join with Multiple Conditions
**Scenario**: You have tables: `customers`, `orders`, `products`, `categories`, `suppliers`.

**Task**: Find customers who have purchased products from suppliers in the same region as the customer, but never from suppliers in different regions.

**Expected Skills**: Complex joins, EXISTS/NOT EXISTS, multiple conditions

---

## Question 13: Advanced Grouping and Filtering
**Scenario**: You have a web_analytics table with `user_id`, `page_url`, `session_id`, `timestamp`, `action_type`.

**Task**: Find user sessions that lasted more than 30 minutes, visited at least 5 different pages, and had at least one conversion action.

**Expected Skills**: GROUP BY with HAVING, complex filtering, session analysis

---

## Question 14: Performance Optimization Query
**Scenario**: You have a large table with 100M+ rows: `transactions` (id, account_id, amount, date, type, merchant_id).

**Task**: Write an optimized query to find the top 100 accounts by transaction volume in the last 30 days, considering indexing strategies.

**Expected Skills**: Query optimization, indexing concepts, performance considerations

---

## Question 15: Data Quality and Validation
**Scenario**: You have a customer_data table with various data quality issues.

**Task**: Write queries to identify and fix: duplicate records, invalid email formats, missing required fields, inconsistent phone number formats, and data type mismatches.

**Expected Skills**: Data quality assessment, deduplication, data validation

---

## Question 16: Advanced Window Functions with Custom Logic
**Scenario**: You have a table with `employee_id`, `project_id`, `hours_worked`, `date`, `bill_rate`.

**Task**: Calculate running totals, project completion percentages, and identify employees who have worked on projects worth more than $10,000 in the last quarter.

**Expected Skills**: Complex window functions, running calculations, business logic

---

## Question 17: Multi-Table Aggregation with Complex Logic
**Scenario**: You have tables: `departments`, `employees`, `projects`, `timesheets`, `budgets`.

**Task**: Create a departmental budget utilization report showing actual vs. budgeted hours, cost overruns, and project completion status.

**Expected Skills**: Multiple table joins, complex aggregations, business reporting

---

## Question 18: Advanced Date and Time Functions
**Scenario**: You have a table with `event_id`, `event_type`, `start_time`, `end_time`, `timezone`.

**Task**: Convert all times to UTC, calculate event durations, find overlapping events, and create a calendar view of events.

**Expected Skills**: Time zone handling, date arithmetic, overlap detection

---

## Question 19: Complex Business Logic with SQL
**Scenario**: You have tables: `customers`, `subscriptions`, `payments`, `usage_logs`.

**Task**: Calculate customer lifetime value, identify churn risk (customers with declining usage), and create a subscription health score.

**Expected Skills**: Business logic implementation, trend analysis, scoring algorithms

---

## Question 20: Advanced Analytics with Statistical Functions
**Scenario**: You have a table with `product_id`, `sales_date`, `quantity`, `price`, `region`.

**Task**: Calculate correlation between price and quantity sold, identify seasonal patterns, and predict next month's sales using historical trends.

**Expected Skills**: Statistical functions, trend analysis, predictive analytics concepts

---

## Question 21: Complex Data Transformation
**Scenario**: You have a denormalized table with `order_id`, `customer_info`, `product_info`, `shipping_info` (all as JSON strings).

**Task**: Parse JSON data, normalize the structure, and create a star schema with fact and dimension tables.

**Expected Skills**: JSON parsing, data modeling, normalization

---

## Question 22: Advanced Error Handling and Edge Cases
**Scenario**: You have a table with `id`, `value`, `status`, `created_date` with various data quality issues.

**Task**: Handle NULL values, invalid data types, duplicate keys, and create a robust data pipeline with error logging.

**Expected Skills**: Error handling, data validation, robust query design

---

## Question 23: Complex Reporting with Multiple Metrics
**Scenario**: You have tables: `sales`, `inventory`, `customers`, `products`, `returns`.

**Task**: Create a comprehensive dashboard query showing: sales trends, inventory turnover, customer acquisition, return rates, and product performance.

**Expected Skills**: Complex reporting, multiple metrics, dashboard queries

---

## Question 24: Advanced Optimization Techniques
**Scenario**: You have a query that's running slowly on a 500M row table.

**Task**: Analyze the query execution plan, suggest indexing strategies, rewrite the query for better performance, and implement query hints.

**Expected Skills**: Query optimization, indexing, performance tuning

---

## Question 25: Real-World Data Pipeline Scenario
**Scenario**: You need to process daily data from multiple sources with different schemas and create a unified reporting table.

**Task**: Design a SQL solution that handles schema evolution, data validation, incremental processing, and error recovery.

**Expected Skills**: Data pipeline design, schema evolution, incremental processing, error handling

---

## 🎯 Interview Tips

1. **Start with the business problem** - Always understand what the business is trying to achieve
2. **Think about performance** - Consider indexing, query optimization, and scalability
3. **Handle edge cases** - NULL values, duplicates, data quality issues
4. **Explain your approach** - Walk through your thought process
5. **Consider alternatives** - Discuss different approaches and trade-offs
6. **Test your solution** - Always validate with sample data
7. **Think about maintenance** - How would you maintain and monitor this query?

## 📊 Difficulty Levels

- **Beginner (1-5)**: Basic joins, simple aggregations
- **Intermediate (6-15)**: Window functions, CTEs, complex joins
- **Advanced (16-25)**: Performance optimization, complex business logic, data pipeline design

## 🚀 Practice Strategy

1. **Time yourself** - Aim for 15-20 minutes per question
2. **Start simple** - Build up complexity gradually
3. **Test with real data** - Use the provided sample datasets
4. **Explain your solution** - Practice verbalizing your approach
5. **Consider edge cases** - What could go wrong?
6. **Optimize** - How could this be made faster/more efficient?
