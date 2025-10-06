# SQL Practice Queries - 30 Expert-Level Challenges

## 📊 Dataset Overview
- **employees.csv**: Employee data with departments, salaries, and hierarchy
- **departments.csv**: Department information with managers and locations
- **sales.csv**: Sales transactions with products, customers, and salespeople
- **products.csv**: Product catalog with categories, pricing, and inventory
- **customers.csv**: Customer information with segments and registration dates

---

## 🟢 BEGINNER LEVEL (Queries 1-10)

### Query 1: Basic SELECT and WHERE
**Task**: Find all employees who earn more than $10,000 per year.
**Learning Focus**: Basic filtering with WHERE clause

### Query 2: Simple Aggregation
**Task**: Calculate the total number of employees in each department.
**Learning Focus**: GROUP BY and COUNT functions

### Query 3: Basic JOIN
**Task**: Show employee names along with their department names.
**Learning Focus**: INNER JOIN between employees and departments

### Query 4: Date Filtering
**Task**: Find all employees hired after January 1, 2005.
**Learning Focus**: Date filtering with WHERE clause

### Query 5: String Functions
**Task**: Display employee first and last names concatenated with a space.
**Learning Focus**: String concatenation and aliasing

### Query 6: Basic Sorting
**Task**: List all products sorted by price in descending order.
**Learning Focus**: ORDER BY clause

### Query 7: Simple Math Operations
**Task**: Calculate the profit margin for each product (unit_price - cost_price).
**Learning Focus**: Mathematical operations in SELECT

### Query 8: Basic Filtering with Multiple Conditions
**Task**: Find all sales in the North region with total amount greater than $200.
**Learning Focus**: Multiple WHERE conditions with AND

### Query 9: NULL Handling
**Task**: Find all departments that don't have a manager assigned.
**Learning Focus**: IS NULL operator

### Query 10: Basic CASE Statement
**Task**: Categorize employees as 'High', 'Medium', or 'Low' based on salary ranges.
**Learning Focus**: CASE WHEN statements

---

## 🟡 INTERMEDIATE LEVEL (Queries 11-20)

### Query 11: Complex JOINs
**Task**: Show sales details with customer names, product names, and salesperson names.
**Learning Focus**: Multiple table JOINs

### Query 12: Window Functions - ROW_NUMBER
**Task**: Rank employees by salary within each department.
**Learning Focus**: ROW_NUMBER() OVER PARTITION BY

### Query 13: Window Functions - RANK and DENSE_RANK
**Task**: Find the top 3 highest-paid employees in each department.
**Learning Focus**: RANK() and DENSE_RANK() functions

### Query 14: Window Functions - LAG/LEAD
**Task**: Compare each employee's salary with the previous employee's salary in the same department.
**Learning Focus**: LAG() and LEAD() functions

### Query 15: Window Functions - Running Totals
**Task**: Calculate running total of sales by month.
**Learning Focus**: SUM() OVER with ORDER BY

### Query 16: Subqueries - EXISTS
**Task**: Find all customers who have made at least one purchase.
**Learning Focus**: EXISTS subquery

### Query 17: Subqueries - IN with Subquery
**Task**: Find all products that have been sold at least once.
**Learning Focus**: IN clause with subquery

### Query 18: Correlated Subqueries
**Task**: Find employees whose salary is above the average salary in their department.
**Learning Focus**: Correlated subqueries

### Query 19: CTEs (Common Table Expressions)
**Task**: Find the department with the highest average salary using CTEs.
**Learning Focus**: WITH clause and CTEs

### Query 20: Complex Aggregation with HAVING
**Task**: Find departments where the average salary is greater than $8,000.
**Learning Focus**: HAVING clause for filtering aggregated results

---

## 🔴 ADVANCED LEVEL (Queries 21-30)

### Query 21: Advanced Window Functions - Percentiles
**Task**: Calculate the 90th percentile salary for each department.
**Learning Focus**: PERCENTILE_CONT() and PERCENTILE_DISC()

### Query 22: Recursive CTEs
**Task**: Build a complete employee hierarchy tree showing all reporting relationships.
**Learning Focus**: Recursive CTEs for hierarchical data

### Query 23: Advanced Window Functions - Multiple Partitions
**Task**: Calculate running totals and running averages for sales by region and month.
**Learning Focus**: Complex window function partitioning

### Query 24: Pivot Operations
**Task**: Create a pivot table showing total sales by product category and region.
**Learning Focus**: PIVOT operations and conditional aggregation

### Query 25: Advanced Date Functions
**Task**: Find all employees who have been with the company for exactly 5 years.
**Learning Focus**: Date arithmetic and EXTRACT functions

### Query 26: Complex Business Logic with Multiple CTEs
**Task**: Identify customers who made their first purchase in Q1 2023 and their total lifetime value.
**Learning Focus**: Multiple CTEs and complex business logic

### Query 27: Performance Optimization - Index Usage
**Task**: Write an optimized query to find the top 10 customers by total purchase amount.
**Learning Focus**: Query optimization and indexing strategies

### Query 28: Advanced Analytics - Cohort Analysis
**Task**: Perform cohort analysis to track customer retention by registration month.
**Learning Focus**: Advanced analytics and cohort analysis

### Query 29: Complex Data Quality Checks
**Task**: Identify data quality issues: duplicate customers, invalid email formats, and missing data.
**Learning Focus**: Data quality assessment and validation

### Query 30: Advanced Reporting - Dynamic SQL
**Task**: Create a dynamic report that can generate sales summaries for any date range and region.
**Learning Focus**: Dynamic SQL and parameterized queries

---

## 🎯 Key Learning Objectives

### SQL Fundamentals
- SELECT, FROM, WHERE, GROUP BY, HAVING, ORDER BY
- Data types and functions
- NULL handling and data validation

### Advanced Querying
- Complex JOINs (INNER, LEFT, RIGHT, FULL OUTER, CROSS)
- Subqueries and CTEs
- Window functions and analytics
- Set operations (UNION, INTERSECT, EXCEPT)

### Performance & Optimization
- Index usage and query optimization
- Execution plan analysis
- Query tuning techniques
- Best practices for large datasets

### Business Intelligence
- Reporting and analytics
- Data aggregation and summarization
- Time series analysis
- Cohort and customer analytics

## 📝 Practice Tips

1. **Start with the basics** - Master simple queries before moving to complex ones
2. **Understand the data** - Explore the datasets to understand relationships
3. **Test your solutions** - Always verify results make business sense
4. **Optimize for performance** - Consider execution plans and indexing
5. **Document your approach** - Explain your thinking process
6. **Practice regularly** - Consistent practice builds expertise

## 🚀 Next Steps

After completing these queries:
1. Review the solutions to understand different approaches
2. Practice explaining your queries to others
3. Focus on optimization techniques
4. Prepare for follow-up questions about performance
5. Practice with larger datasets to understand scalability

Good luck with your interview preparation! 🎯
