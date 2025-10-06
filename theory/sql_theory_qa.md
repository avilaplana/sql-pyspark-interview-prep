# SQL Theory - Comprehensive Q&A for Senior Data Engineers

## 📚 Table of Contents
1. [Database Fundamentals](#database-fundamentals)
2. [SQL Syntax & Operations](#sql-syntax--operations)
3. [Indexes & Performance](#indexes--performance)
4. [Transactions & ACID](#transactions--acid)
5. [Data Types & Functions](#data-types--functions)
6. [Advanced Querying](#advanced-querying)
7. [Database Design](#database-design)
8. [Performance Optimization](#performance-optimization)
9. [Data Quality & Integrity](#data-quality--integrity)
10. [Real-World Scenarios](#real-world-scenarios)

---

## 🏗️ Database Fundamentals

### Q1: What is a Primary Key and why is it important?
**A:** A Primary Key is a column or combination of columns that uniquely identifies each row in a table. It's important because:
- **Uniqueness**: Ensures no duplicate rows
- **Non-nullability**: Cannot contain NULL values
- **Immutability**: Should not change over time
- **Indexing**: Automatically creates a unique index for fast lookups
- **Referential Integrity**: Used as foreign key references

**Example:**
```sql
CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50)
);
```

### Q2: What's the difference between Primary Key and Unique Key?
**A:** 
| Primary Key | Unique Key |
|-------------|------------|
| Only one per table | Multiple allowed per table |
| Cannot be NULL | Can have one NULL value |
| Automatically creates clustered index | Creates non-clustered index |
| Used for relationships | Used for business constraints |

### Q3: Explain Foreign Keys and Referential Integrity
**A:** Foreign Keys maintain referential integrity by ensuring that values in one table correspond to values in another table.

**Benefits:**
- Prevents orphaned records
- Maintains data consistency
- Enables cascading operations
- Supports data relationships

**Example:**
```sql
CREATE TABLE orders (
    order_id INT PRIMARY KEY,
    customer_id INT,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);
```

### Q4: What are the different types of relationships in databases?
**A:**
1. **One-to-One**: Each record in Table A relates to exactly one record in Table B
2. **One-to-Many**: One record in Table A can relate to many records in Table B
3. **Many-to-Many**: Records in Table A can relate to multiple records in Table B and vice versa

**Many-to-Many Example:**
```sql
-- Junction table
CREATE TABLE student_courses (
    student_id INT,
    course_id INT,
    PRIMARY KEY (student_id, course_id),
    FOREIGN KEY (student_id) REFERENCES students(student_id),
    FOREIGN KEY (course_id) REFERENCES courses(course_id)
);
```

---

## 🔧 SQL Syntax & Operations

### Q5: Explain the difference between WHERE and HAVING clauses
**A:**
| WHERE | HAVING |
|--------|--------|
| Filters rows before grouping | Filters groups after grouping |
| Cannot use aggregate functions | Can use aggregate functions |
| Applied to individual rows | Applied to grouped results |
| Used with SELECT, UPDATE, DELETE | Used only with SELECT |

**Example:**
```sql
-- WHERE: Filter individual employees
SELECT department_id, COUNT(*) 
FROM employees 
WHERE salary > 50000 
GROUP BY department_id;

-- HAVING: Filter departments with > 5 employees
SELECT department_id, COUNT(*) 
FROM employees 
GROUP BY department_id 
HAVING COUNT(*) > 5;
```

### Q6: What are the different types of JOINs and when to use them?
**A:**

1. **INNER JOIN**: Returns only matching records from both tables
2. **LEFT JOIN**: Returns all records from left table + matching from right
3. **RIGHT JOIN**: Returns all records from right table + matching from left
4. **FULL OUTER JOIN**: Returns all records from both tables
5. **CROSS JOIN**: Returns Cartesian product of both tables
6. **SELF JOIN**: Joins table with itself

**Performance Order (fastest to slowest):**
1. INNER JOIN
2. LEFT/RIGHT JOIN
3. FULL OUTER JOIN
4. CROSS JOIN

### Q7: Explain the difference between UNION and UNION ALL
**A:**
| UNION | UNION ALL |
|-------|-----------|
| Removes duplicates | Keeps all rows including duplicates |
| Slower performance | Faster performance |
| Requires sorting | No sorting required |
| Uses more memory | Uses less memory |

**Example:**
```sql
-- UNION: Removes duplicates
SELECT name FROM table1
UNION
SELECT name FROM table2;

-- UNION ALL: Keeps all rows
SELECT name FROM table1
UNION ALL
SELECT name FROM table2;
```

---

## 🚀 Indexes & Performance

### Q8: What are indexes and what types exist?
**A:** Indexes are database objects that improve query performance by providing fast access to data.

**Types:**
1. **Clustered Index**: Determines physical order of data storage
2. **Non-clustered Index**: Separate structure pointing to data
3. **Composite Index**: Multiple columns in one index
4. **Covering Index**: Contains all columns needed for a query
5. **Partial Index**: Index on subset of rows (WHERE clause)

**Example:**
```sql
-- Clustered index (Primary Key)
CREATE TABLE employees (
    employee_id INT PRIMARY KEY,  -- Clustered index
    name VARCHAR(50)
);

-- Non-clustered index
CREATE INDEX idx_employee_name ON employees(name);

-- Composite index
CREATE INDEX idx_dept_salary ON employees(department_id, salary);
```

### Q9: When should you create indexes and when should you avoid them?
**A:**

**Create indexes when:**
- Frequently queried columns
- Columns used in WHERE clauses
- Foreign key columns
- Columns used in ORDER BY
- Columns used in GROUP BY

**Avoid indexes when:**
- Tables with frequent INSERT/UPDATE/DELETE
- Small tables (< 1000 rows)
- Columns with low cardinality
- Too many indexes on one table

**Best Practices:**
- Monitor index usage
- Regular maintenance (REBUILD/REORGANIZE)
- Consider index overhead
- Use covering indexes when possible

### Q10: What is the difference between clustered and non-clustered indexes?
**A:**

| Clustered Index | Non-clustered Index |
|-----------------|-------------------|
| Only one per table | Multiple allowed per table |
| Determines physical order | Separate structure |
| Faster for range queries | Faster for point lookups |
| No additional storage | Additional storage required |
| Automatically created for PK | Must be explicitly created |

---

## 🔄 Transactions & ACID

### Q11: Explain ACID properties in database transactions
**A:** ACID ensures reliable database transactions:

1. **Atomicity**: All operations succeed or all fail
2. **Consistency**: Database remains in valid state
3. **Isolation**: Concurrent transactions don't interfere
4. **Durability**: Committed changes persist

**Example:**
```sql
BEGIN TRANSACTION;
    UPDATE accounts SET balance = balance - 100 WHERE account_id = 1;
    UPDATE accounts SET balance = balance + 100 WHERE account_id = 2;
    -- If either fails, both are rolled back
COMMIT;
```

### Q12: What are transaction isolation levels?
**A:**

1. **READ UNCOMMITTED**: Can read uncommitted data (dirty reads)
2. **READ COMMITTED**: Can only read committed data (default in most DBs)
3. **REPEATABLE READ**: Consistent reads within transaction
4. **SERIALIZABLE**: Highest isolation, prevents all anomalies

**Trade-offs:**
- Higher isolation = Better consistency, Lower performance
- Lower isolation = Better performance, Potential inconsistencies

### Q13: What are database locks and their types?
**A:** Locks prevent concurrent access conflicts:

**Types:**
1. **Shared Lock (S)**: Multiple readers, no writers
2. **Exclusive Lock (X)**: One writer, no others
3. **Update Lock (U)**: Intent to update
4. **Intent Locks**: Table-level locking hints

**Lock Granularity:**
- Row-level: Most granular, best concurrency
- Page-level: Medium granularity
- Table-level: Least granular, worst concurrency

---

## 📊 Data Types & Functions

### Q14: What are the main SQL data types and when to use them?
**A:**

**Numeric Types:**
- `INT`: 32-bit integer (-2B to 2B)
- `BIGINT`: 64-bit integer
- `DECIMAL(p,s)`: Fixed precision decimal
- `FLOAT`: Approximate numeric
- `REAL`: Single precision float

**String Types:**
- `CHAR(n)`: Fixed length string
- `VARCHAR(n)`: Variable length string
- `TEXT`: Large text data
- `NVARCHAR`: Unicode string

**Date/Time Types:**
- `DATE`: Date only
- `TIME`: Time only
- `DATETIME`: Date and time
- `TIMESTAMP`: Auto-updating timestamp

### Q15: Explain string functions and their use cases
**A:**

**Common String Functions:**
```sql
-- Length and position
LEN('Hello') = 5
CHARINDEX('l', 'Hello') = 3

-- Substring operations
SUBSTRING('Hello', 2, 3) = 'ell'
LEFT('Hello', 2) = 'He'
RIGHT('Hello', 2) = 'lo'

-- Case functions
UPPER('hello') = 'HELLO'
LOWER('HELLO') = 'hello'

-- Trim functions
LTRIM('  hello') = 'hello'
RTRIM('hello  ') = 'hello'
TRIM('  hello  ') = 'hello'

-- Concatenation
CONCAT('Hello', ' ', 'World') = 'Hello World'
```

### Q16: What are date functions and how to handle time zones?
**A:**

**Common Date Functions:**
```sql
-- Current date/time
GETDATE() -- Current timestamp
GETUTCDATE() -- UTC timestamp

-- Date arithmetic
DATEADD(day, 1, '2023-01-01') -- Add 1 day
DATEDIFF(day, '2023-01-01', '2023-01-31') -- Difference in days

-- Date parts
YEAR('2023-01-01') = 2023
MONTH('2023-01-01') = 1
DAY('2023-01-01') = 1

-- Formatting
FORMAT(GETDATE(), 'yyyy-MM-dd') -- Format date
```

**Time Zone Handling:**
- Store in UTC
- Convert for display
- Use AT TIME ZONE for conversions
- Consider daylight saving time

---

## 🔍 Advanced Querying

### Q17: What are window functions and their types?
**A:** Window functions perform calculations across a set of rows related to the current row.

**Types:**
1. **Ranking Functions**: ROW_NUMBER(), RANK(), DENSE_RANK()
2. **Aggregate Functions**: SUM(), AVG(), COUNT(), MIN(), MAX()
3. **Value Functions**: LAG(), LEAD(), FIRST_VALUE(), LAST_VALUE()
4. **Distribution Functions**: PERCENT_RANK(), CUME_DIST()

**Example:**
```sql
SELECT 
    employee_id,
    salary,
    ROW_NUMBER() OVER (ORDER BY salary DESC) as row_num,
    RANK() OVER (ORDER BY salary DESC) as rank,
    LAG(salary) OVER (ORDER BY salary) as prev_salary
FROM employees;
```

### Q18: Explain Common Table Expressions (CTEs) vs Subqueries
**A:**

**CTEs:**
- More readable and maintainable
- Can be referenced multiple times
- Support recursion
- Better for complex logic

**Subqueries:**
- Can be correlated or non-correlated
- More compact for simple cases
- Better performance for simple lookups

**Example:**
```sql
-- CTE approach
WITH high_earners AS (
    SELECT * FROM employees WHERE salary > 100000
)
SELECT * FROM high_earners WHERE department_id = 10;

-- Subquery approach
SELECT * FROM employees 
WHERE salary > 100000 AND department_id = 10;
```

### Q19: What are the different types of subqueries?
**A:**

1. **Scalar Subquery**: Returns single value
2. **Row Subquery**: Returns single row
3. **Table Subquery**: Returns multiple rows/columns
4. **Correlated Subquery**: References outer query
5. **Non-correlated Subquery**: Independent of outer query

**Example:**
```sql
-- Scalar subquery
SELECT name, (SELECT AVG(salary) FROM employees) as avg_salary
FROM employees;

-- Correlated subquery
SELECT e1.name, e1.salary
FROM employees e1
WHERE e1.salary > (SELECT AVG(e2.salary) FROM employees e2 WHERE e2.department_id = e1.department_id);
```

---

## 🏗️ Database Design

### Q20: What is database normalization and its forms?
**A:** Normalization reduces data redundancy and improves data integrity.

**Normal Forms:**
1. **1NF**: All attributes contain atomic values
2. **2NF**: 1NF + no partial dependencies
3. **3NF**: 2NF + no transitive dependencies
4. **BCNF**: 3NF + every determinant is a candidate key
5. **4NF**: BCNF + no multi-valued dependencies
6. **5NF**: 4NF + no join dependencies

**Trade-offs:**
- **Pros**: Reduced redundancy, better integrity
- **Cons**: More complex queries, potential performance impact

### Q21: When should you denormalize a database?
**A:** Denormalization is acceptable when:

**Reasons:**
- Performance requirements
- Read-heavy workloads
- Complex reporting needs
- Data warehouse scenarios

**Techniques:**
- Duplicate frequently accessed data
- Create summary tables
- Use materialized views
- Implement caching strategies

**Example:**
```sql
-- Denormalized table for reporting
CREATE TABLE sales_summary (
    product_id INT,
    product_name VARCHAR(100),
    category VARCHAR(50),
    total_sales DECIMAL(10,2),
    total_quantity INT,
    last_sale_date DATE
);
```

### Q22: What is the difference between OLTP and OLAP?
**A:**

| OLTP (Online Transaction Processing) | OLAP (Online Analytical Processing) |
|--------------------------------------|-------------------------------------|
| Transactional data | Analytical data |
| Normalized structure | Denormalized structure |
| Real-time operations | Batch processing |
| ACID compliance | Eventual consistency |
| Row-oriented | Column-oriented |
| High concurrency | High throughput |

---

## ⚡ Performance Optimization

### Q23: How do you optimize slow SQL queries?
**A:**

**Analysis Steps:**
1. **Identify bottlenecks**: Use execution plans
2. **Check indexes**: Missing or unused indexes
3. **Review query structure**: Inefficient JOINs, subqueries
4. **Consider statistics**: Outdated or missing statistics
5. **Hardware constraints**: Memory, CPU, I/O

**Optimization Techniques:**
- Add appropriate indexes
- Rewrite queries for better performance
- Use query hints when necessary
- Consider partitioning for large tables
- Update statistics regularly

### Q24: What is an execution plan and how do you read it?
**A:** Execution plan shows how the database engine executes a query.

**Key Components:**
- **Table Scans**: Full table reads (expensive)
- **Index Scans**: Reading entire index
- **Index Seeks**: Direct index lookups (efficient)
- **Hash Joins**: Hash table-based joins
- **Nested Loops**: Row-by-row comparisons
- **Sorts**: Data sorting operations

**Reading Tips:**
- Read from right to left, top to bottom
- Look for high-cost operations
- Check for missing indexes
- Identify unnecessary sorts or scans

### Q25: What are query hints and when to use them?
**A:** Query hints direct the optimizer to use specific execution strategies.

**Common Hints:**
```sql
-- Index hints
SELECT * FROM employees WITH (INDEX(idx_employee_name))
WHERE name = 'John';

-- Join hints
SELECT * FROM table1 t1
INNER HASH JOIN table2 t2 ON t1.id = t2.id;

-- Lock hints
SELECT * FROM employees WITH (NOLOCK)
WHERE department_id = 10;
```

**Use Cases:**
- Override poor optimizer decisions
- Force specific join algorithms
- Control locking behavior
- Optimize for specific scenarios

**Best Practices:**
- Use sparingly
- Document reasoning
- Test thoroughly
- Monitor performance impact

---

## 🔍 Data Quality & Integrity

### Q26: How do you ensure data quality in SQL?
**A:**

**Techniques:**
1. **Constraints**: PRIMARY KEY, FOREIGN KEY, CHECK, UNIQUE
2. **Data Validation**: Format checks, range validation
3. **Data Profiling**: Identify patterns and anomalies
4. **Cleansing**: Remove duplicates, fix inconsistencies
5. **Monitoring**: Regular quality checks

**Example:**
```sql
-- Data quality check
SELECT 
    COUNT(*) as total_records,
    COUNT(DISTINCT email) as unique_emails,
    COUNT(*) - COUNT(DISTINCT email) as duplicate_emails,
    SUM(CASE WHEN email LIKE '%@%.%' THEN 1 ELSE 0 END) as valid_emails
FROM customers;
```

### Q27: What are common data quality issues and how to handle them?
**A:**

**Common Issues:**
1. **Missing Data**: NULL values, empty strings
2. **Duplicate Data**: Exact duplicates, near duplicates
3. **Inconsistent Data**: Different formats, case sensitivity
4. **Invalid Data**: Wrong data types, out-of-range values
5. **Referential Issues**: Orphaned records, broken relationships

**Handling Strategies:**
- **Prevention**: Constraints and validation
- **Detection**: Regular data profiling
- **Correction**: Data cleansing processes
- **Monitoring**: Automated quality checks

### Q28: How do you handle NULL values in SQL?
**A:**

**NULL Characteristics:**
- Represents unknown or missing data
- Cannot be compared with = or !=
- Requires special functions: IS NULL, IS NOT NULL
- Affects aggregate functions
- Can cause unexpected results

**Handling Techniques:**
```sql
-- Check for NULL
WHERE column IS NULL
WHERE column IS NOT NULL

-- Replace NULL values
COALESCE(column, 'default_value')
ISNULL(column, 'default_value')

-- Conditional logic
CASE WHEN column IS NULL THEN 'Unknown' ELSE column END
```

---

## 🌍 Real-World Scenarios

### Q29: How would you design a data warehouse schema?
**A:**

**Design Principles:**
1. **Star Schema**: Central fact table with dimension tables
2. **Snowflake Schema**: Normalized dimensions
3. **Galaxy Schema**: Multiple fact tables

**Key Components:**
- **Fact Tables**: Measures and metrics
- **Dimension Tables**: Descriptive attributes
- **Slowly Changing Dimensions**: Type 1, 2, 3
- **Aggregate Tables**: Pre-computed summaries

**Example:**
```sql
-- Fact table
CREATE TABLE sales_fact (
    date_key INT,
    product_key INT,
    customer_key INT,
    sales_amount DECIMAL(10,2),
    quantity INT
);

-- Dimension table
CREATE TABLE product_dim (
    product_key INT PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    subcategory VARCHAR(50)
);
```

### Q30: How do you handle large datasets in SQL?
**A:**

**Strategies:**
1. **Partitioning**: Divide tables by ranges, lists, or hash
2. **Indexing**: Strategic index placement
3. **Archiving**: Move old data to separate tables
4. **Compression**: Reduce storage requirements
5. **Parallel Processing**: Utilize multiple cores

**Partitioning Example:**
```sql
-- Range partitioning by date
CREATE TABLE sales (
    sale_id INT,
    sale_date DATE,
    amount DECIMAL(10,2)
) PARTITION BY RANGE (sale_date) (
    PARTITION p2023 VALUES LESS THAN ('2024-01-01'),
    PARTITION p2024 VALUES LESS THAN ('2025-01-01'),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);
```

---

## 🎯 Interview Preparation Tips

### Key Areas to Master:
1. **Fundamentals**: ACID, normalization, relationships
2. **Performance**: Indexing, query optimization, execution plans
3. **Advanced Features**: Window functions, CTEs, recursive queries
4. **Real-World Scenarios**: Data warehousing, ETL, reporting
5. **Best Practices**: Security, maintenance, monitoring

### Common Interview Questions:
- Explain the difference between INNER and LEFT JOIN
- How would you optimize a slow query?
- What's the difference between DELETE and TRUNCATE?
- How do you handle duplicate data?
- Explain database normalization with examples

### Practice Areas:
- Write complex queries from scratch
- Explain query execution plans
- Design database schemas
- Optimize existing queries
- Handle data quality issues

Remember: Focus on understanding concepts deeply rather than memorizing syntax. Be prepared to explain your reasoning and discuss trade-offs! 🚀
