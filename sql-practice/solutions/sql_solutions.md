# SQL Practice Solutions - Complete Answer Key

## 🟢 BEGINNER LEVEL SOLUTIONS (Queries 1-10)

### Query 1: Basic SELECT and WHERE
```sql
SELECT employee_id, first_name, last_name, salary
FROM employees
WHERE salary > 10000;
```
**Explanation**: Simple filtering using WHERE clause to find employees earning more than $10,000.

### Query 2: Simple Aggregation
```sql
SELECT department_id, COUNT(*) as employee_count
FROM employees
GROUP BY department_id
ORDER BY employee_count DESC;
```
**Explanation**: Uses GROUP BY to group employees by department and COUNT to get the total number in each group.

### Query 3: Basic JOIN
```sql
SELECT e.first_name, e.last_name, d.department_name
FROM employees e
INNER JOIN departments d ON e.department_id = d.department_id;
```
**Explanation**: INNER JOIN connects employees with their departments using the department_id foreign key.

### Query 4: Date Filtering
```sql
SELECT employee_id, first_name, last_name, hire_date
FROM employees
WHERE hire_date > '2005-01-01';
```
**Explanation**: Date comparison using WHERE clause. Note the date format and comparison operator.

### Query 5: String Functions
```sql
SELECT CONCAT(first_name, ' ', last_name) as full_name
FROM employees;
```
**Explanation**: CONCAT function combines first and last names with a space separator.

### Query 6: Basic Sorting
```sql
SELECT product_name, unit_price
FROM products
ORDER BY unit_price DESC;
```
**Explanation**: ORDER BY with DESC sorts products from highest to lowest price.

### Query 7: Simple Math Operations
```sql
SELECT product_name, unit_price, cost_price, 
       (unit_price - cost_price) as profit_margin
FROM products;
```
**Explanation**: Mathematical operations in SELECT clause to calculate profit margin.

### Query 8: Basic Filtering with Multiple Conditions
```sql
SELECT sale_id, product_id, total_amount, region
FROM sales
WHERE region = 'North' AND total_amount > 200;
```
**Explanation**: Multiple conditions using AND operator to filter by both region and amount.

### Query 9: NULL Handling
```sql
SELECT department_id, department_name
FROM departments
WHERE manager_id IS NULL;
```
**Explanation**: IS NULL operator to find departments without assigned managers.

### Query 10: Basic CASE Statement
```sql
SELECT first_name, last_name, salary,
       CASE 
           WHEN salary >= 15000 THEN 'High'
           WHEN salary >= 8000 THEN 'Medium'
           ELSE 'Low'
       END as salary_category
FROM employees;
```
**Explanation**: CASE WHEN statement for conditional logic to categorize salaries.

---

## 🟡 INTERMEDIATE LEVEL SOLUTIONS (Queries 11-20)

### Query 11: Complex JOINs
```sql
SELECT s.sale_id, c.first_name, c.last_name, p.product_name, 
       e.first_name as salesperson_first, e.last_name as salesperson_last
FROM sales s
INNER JOIN customers c ON s.customer_id = c.customer_id
INNER JOIN products p ON s.product_id = p.product_id
INNER JOIN employees e ON s.salesperson_id = e.employee_id;
```
**Explanation**: Multiple INNER JOINs to connect sales with customers, products, and salespeople.

### Query 12: Window Functions - ROW_NUMBER
```sql
SELECT first_name, last_name, salary, department_id,
       ROW_NUMBER() OVER (PARTITION BY department_id ORDER BY salary DESC) as salary_rank
FROM employees;
```
**Explanation**: ROW_NUMBER() with PARTITION BY creates ranking within each department.

### Query 13: Window Functions - RANK and DENSE_RANK
```sql
WITH ranked_employees AS (
    SELECT first_name, last_name, salary, department_id,
           RANK() OVER (PARTITION BY department_id ORDER BY salary DESC) as salary_rank
    FROM employees
)
SELECT * FROM ranked_employees WHERE salary_rank <= 3;
```
**Explanation**: Uses RANK() to handle ties and CTE to filter top 3 employees per department.

### Query 14: Window Functions - LAG/LEAD
```sql
SELECT first_name, last_name, salary, department_id,
       LAG(salary) OVER (PARTITION BY department_id ORDER BY salary) as previous_salary,
       salary - LAG(salary) OVER (PARTITION BY department_id ORDER BY salary) as salary_difference
FROM employees;
```
**Explanation**: LAG() function to compare current salary with previous employee's salary in the same department.

### Query 15: Window Functions - Running Totals
```sql
SELECT sale_date, total_amount,
       SUM(total_amount) OVER (ORDER BY sale_date) as running_total
FROM sales
ORDER BY sale_date;
```
**Explanation**: SUM() OVER with ORDER BY calculates cumulative sum of sales over time.

### Query 16: Subqueries - EXISTS
```sql
SELECT c.customer_id, c.first_name, c.last_name
FROM customers c
WHERE EXISTS (
    SELECT 1 FROM sales s WHERE s.customer_id = c.customer_id
);
```
**Explanation**: EXISTS subquery to find customers who have made at least one purchase.

### Query 17: Subqueries - IN with Subquery
```sql
SELECT product_name, unit_price
FROM products
WHERE product_id IN (
    SELECT DISTINCT product_id FROM sales
);
```
**Explanation**: IN clause with subquery to find products that have been sold at least once.

### Query 18: Correlated Subqueries
```sql
SELECT e1.first_name, e1.last_name, e1.salary, e1.department_id
FROM employees e1
WHERE e1.salary > (
    SELECT AVG(e2.salary) 
    FROM employees e2 
    WHERE e2.department_id = e1.department_id
);
```
**Explanation**: Correlated subquery compares each employee's salary with their department's average.

### Query 19: CTEs (Common Table Expressions)
```sql
WITH dept_avg_salary AS (
    SELECT department_id, AVG(salary) as avg_salary
    FROM employees
    GROUP BY department_id
),
max_avg_salary AS (
    SELECT MAX(avg_salary) as highest_avg
    FROM dept_avg_salary
)
SELECT d.department_name, das.avg_salary
FROM dept_avg_salary das
JOIN departments d ON das.department_id = d.department_id
JOIN max_avg_salary mas ON das.avg_salary = mas.highest_avg;
```
**Explanation**: Multiple CTEs to find the department with the highest average salary.

### Query 20: Complex Aggregation with HAVING
```sql
SELECT d.department_name, AVG(e.salary) as avg_salary
FROM employees e
JOIN departments d ON e.department_id = d.department_id
GROUP BY d.department_name
HAVING AVG(e.salary) > 8000
ORDER BY avg_salary DESC;
```
**Explanation**: HAVING clause filters aggregated results to show only departments with average salary > $8,000.

---

## 🔴 ADVANCED LEVEL SOLUTIONS (Queries 21-30)

### Query 21: Advanced Window Functions - Percentiles
```sql
SELECT department_id,
       PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY salary) as p90_salary
FROM employees
GROUP BY department_id;
```
**Explanation**: PERCENTILE_CONT() calculates the 90th percentile salary for each department.

### Query 22: Recursive CTEs
```sql
WITH RECURSIVE employee_hierarchy AS (
    -- Base case: top-level employees (no manager)
    SELECT employee_id, first_name, last_name, manager_id, 0 as level
    FROM employees
    WHERE manager_id IS NULL
    
    UNION ALL
    
    -- Recursive case: employees with managers
    SELECT e.employee_id, e.first_name, e.last_name, e.manager_id, eh.level + 1
    FROM employees e
    JOIN employee_hierarchy eh ON e.manager_id = eh.employee_id
)
SELECT * FROM employee_hierarchy ORDER BY level, employee_id;
```
**Explanation**: Recursive CTE to build complete employee hierarchy tree.

### Query 23: Advanced Window Functions - Multiple Partitions
```sql
SELECT region, sale_date, total_amount,
       SUM(total_amount) OVER (PARTITION BY region ORDER BY sale_date) as running_total_by_region,
       AVG(total_amount) OVER (PARTITION BY region ORDER BY sale_date) as running_avg_by_region
FROM sales
ORDER BY region, sale_date;
```
**Explanation**: Complex window functions with multiple partitioning and ordering.

### Query 24: Pivot Operations
```sql
SELECT region,
       SUM(CASE WHEN p.category = 'Electronics' THEN s.total_amount ELSE 0 END) as electronics_sales,
       SUM(CASE WHEN p.category = 'Clothing' THEN s.total_amount ELSE 0 END) as clothing_sales,
       SUM(CASE WHEN p.category = 'Books' THEN s.total_amount ELSE 0 END) as books_sales
FROM sales s
JOIN products p ON s.product_id = p.product_id
GROUP BY region;
```
**Explanation**: Pivot operation using conditional aggregation to show sales by category and region.

### Query 25: Advanced Date Functions
```sql
SELECT employee_id, first_name, last_name, hire_date
FROM employees
WHERE EXTRACT(YEAR FROM CURRENT_DATE) - EXTRACT(YEAR FROM hire_date) = 5
  AND EXTRACT(MONTH FROM CURRENT_DATE) = EXTRACT(MONTH FROM hire_date)
  AND EXTRACT(DAY FROM CURRENT_DATE) >= EXTRACT(DAY FROM hire_date);
```
**Explanation**: Complex date arithmetic to find employees with exactly 5 years of service.

### Query 26: Complex Business Logic with Multiple CTEs
```sql
WITH first_purchases AS (
    SELECT customer_id, MIN(sale_date) as first_purchase_date
    FROM sales
    GROUP BY customer_id
),
q1_2023_customers AS (
    SELECT customer_id, first_purchase_date
    FROM first_purchases
    WHERE first_purchase_date >= '2023-01-01' 
      AND first_purchase_date < '2023-04-01'
),
customer_lifetime_value AS (
    SELECT customer_id, SUM(total_amount) as lifetime_value
    FROM sales
    GROUP BY customer_id
)
SELECT c.first_name, c.last_name, qc.first_purchase_date, clv.lifetime_value
FROM q1_2023_customers qc
JOIN customers c ON qc.customer_id = c.customer_id
JOIN customer_lifetime_value clv ON qc.customer_id = clv.customer_id
ORDER BY clv.lifetime_value DESC;
```
**Explanation**: Multiple CTEs to identify Q1 2023 first-time customers and calculate their lifetime value.

### Query 27: Performance Optimization - Index Usage
```sql
-- Create indexes for optimization
CREATE INDEX idx_sales_customer_id ON sales(customer_id);
CREATE INDEX idx_sales_total_amount ON sales(total_amount);

-- Optimized query
SELECT c.customer_id, c.first_name, c.last_name, 
       SUM(s.total_amount) as total_purchases
FROM customers c
JOIN sales s ON c.customer_id = s.customer_id
GROUP BY c.customer_id, c.first_name, c.last_name
ORDER BY total_purchases DESC
LIMIT 10;
```
**Explanation**: Index creation and optimized query for top 10 customers by total purchases.

### Query 28: Advanced Analytics - Cohort Analysis
```sql
WITH customer_cohorts AS (
    SELECT customer_id, 
           DATE_TRUNC('month', registration_date) as cohort_month
    FROM customers
),
sales_by_cohort AS (
    SELECT cc.customer_id, cc.cohort_month,
           DATE_TRUNC('month', s.sale_date) as purchase_month,
           COUNT(*) as purchases
    FROM customer_cohorts cc
    LEFT JOIN sales s ON cc.customer_id = s.customer_id
    GROUP BY cc.customer_id, cc.cohort_month, DATE_TRUNC('month', s.sale_date)
)
SELECT cohort_month,
       COUNT(DISTINCT customer_id) as cohort_size,
       COUNT(DISTINCT CASE WHEN purchase_month = cohort_month THEN customer_id END) as month_0_retention,
       COUNT(DISTINCT CASE WHEN purchase_month = cohort_month + INTERVAL '1 month' THEN customer_id END) as month_1_retention
FROM sales_by_cohort
GROUP BY cohort_month
ORDER BY cohort_month;
```
**Explanation**: Cohort analysis to track customer retention by registration month.

### Query 29: Complex Data Quality Checks
```sql
WITH data_quality_issues AS (
    -- Duplicate customers
    SELECT 'duplicate_customers' as issue_type, customer_id, COUNT(*) as count
    FROM customers
    GROUP BY customer_id
    HAVING COUNT(*) > 1
    
    UNION ALL
    
    -- Invalid email formats
    SELECT 'invalid_email' as issue_type, customer_id, 1 as count
    FROM customers
    WHERE email NOT LIKE '%@%.%'
    
    UNION ALL
    
    -- Missing data
    SELECT 'missing_phone' as issue_type, customer_id, 1 as count
    FROM customers
    WHERE phone IS NULL OR phone = ''
)
SELECT issue_type, COUNT(*) as total_issues
FROM data_quality_issues
GROUP BY issue_type;
```
**Explanation**: Comprehensive data quality assessment identifying duplicates, invalid emails, and missing data.

### Query 30: Advanced Reporting - Dynamic SQL
```sql
-- This would typically be implemented in a stored procedure or application
-- Here's the concept for dynamic date range and region filtering

SELECT 
    s.sale_date,
    s.region,
    COUNT(*) as transaction_count,
    SUM(s.total_amount) as total_sales,
    AVG(s.total_amount) as avg_transaction_value
FROM sales s
WHERE s.sale_date BETWEEN '2023-01-01' AND '2023-12-31'  -- Dynamic date range
  AND s.region IN ('North', 'South', 'East', 'West')     -- Dynamic region filter
GROUP BY s.sale_date, s.region
ORDER BY s.sale_date, s.region;
```
**Explanation**: Template for dynamic reporting with parameterized date ranges and regions.

---

## 🎯 Key Learning Points

### Performance Optimization
- **Indexing**: Create appropriate indexes for frequently queried columns
- **Query Structure**: Use efficient JOINs and avoid unnecessary subqueries
- **Window Functions**: Leverage for complex analytics without self-joins
- **CTEs**: Improve readability and can be more efficient than subqueries

### Best Practices
- **Readability**: Use meaningful aliases and proper formatting
- **Documentation**: Comment complex business logic
- **Testing**: Always verify results make business sense
- **Scalability**: Consider query performance with large datasets

### Common Pitfalls to Avoid
- **N+1 Queries**: Use JOINs instead of multiple queries
- **Missing Indexes**: Ensure proper indexing for WHERE and JOIN conditions
- **Inefficient Subqueries**: Consider CTEs or JOINs for better performance
- **Data Type Mismatches**: Ensure proper data type handling in comparisons

## 🚀 Interview Tips

1. **Explain Your Approach**: Walk through your thinking process
2. **Consider Edge Cases**: Handle NULLs, duplicates, and data quality issues
3. **Optimize for Performance**: Discuss indexing and query optimization
4. **Business Context**: Understand the business meaning of your queries
5. **Alternative Solutions**: Be prepared to discuss different approaches

Remember: The goal is not just to write correct SQL, but to write efficient, maintainable, and business-relevant queries! 🎯
