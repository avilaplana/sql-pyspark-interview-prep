-- Sales Dataset for SQL Practice
-- This dataset contains sales data with various complexity levels

-- Create sales table
CREATE TABLE sales (
    sales_id INT PRIMARY KEY,
    product_id INT,
    salesperson_id INT,
    sale_date DATE,
    amount DECIMAL(10,2),
    region VARCHAR(50),
    customer_id INT,
    discount_percent DECIMAL(5,2),
    tax_amount DECIMAL(10,2)
);

-- Insert sample data
INSERT INTO sales VALUES
(1, 101, 201, '2024-01-15', 1500.00, 'North', 1001, 5.00, 120.00),
(2, 102, 202, '2024-01-16', 2300.50, 'South', 1002, 10.00, 184.04),
(3, 103, 201, '2024-01-17', 800.75, 'North', 1003, 0.00, 64.06),
(4, 101, 203, '2024-01-18', 1200.00, 'East', 1004, 15.00, 96.00),
(5, 104, 202, '2024-01-19', 3500.25, 'South', 1005, 8.00, 280.02),
(6, 105, 204, '2024-01-20', 950.00, 'West', 1006, 12.00, 76.00),
(7, 102, 201, '2024-01-21', 1800.00, 'North', 1007, 5.00, 144.00),
(8, 103, 203, '2024-01-22', 2200.00, 'East', 1008, 20.00, 176.00),
(9, 106, 202, '2024-01-23', 750.50, 'South', 1009, 0.00, 60.04),
(10, 101, 204, '2024-01-24', 3000.00, 'West', 1010, 25.00, 240.00),
(11, 107, 201, '2024-01-25', 1650.75, 'North', 1011, 7.50, 132.06),
(12, 102, 202, '2024-01-26', 2800.00, 'South', 1012, 15.00, 224.00),
(13, 108, 203, '2024-01-27', 1200.25, 'East', 1013, 0.00, 96.02),
(14, 103, 204, '2024-01-28', 4500.00, 'West', 1014, 30.00, 360.00),
(15, 109, 201, '2024-01-29', 850.00, 'North', 1015, 5.00, 68.00),
(16, 104, 202, '2024-01-30', 3200.50, 'South', 1016, 18.00, 256.04),
(17, 110, 203, '2024-02-01', 1950.75, 'East', 1017, 10.00, 156.06),
(18, 105, 204, '2024-02-02', 2750.00, 'West', 1018, 22.00, 220.00),
(19, 111, 201, '2024-02-03', 1100.25, 'North', 1019, 0.00, 88.02),
(20, 106, 202, '2024-02-04', 4200.00, 'South', 1020, 35.00, 336.00);

-- Create products table
CREATE TABLE products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10,2),
    supplier_id INT,
    created_date DATE
);

INSERT INTO products VALUES
(101, 'Laptop Pro', 'Electronics', 1500.00, 301, '2023-01-15'),
(102, 'Wireless Mouse', 'Electronics', 25.50, 302, '2023-02-20'),
(103, 'Office Chair', 'Furniture', 200.75, 303, '2023-03-10'),
(104, 'Monitor 24"', 'Electronics', 350.25, 301, '2023-04-05'),
(105, 'Desk Lamp', 'Furniture', 45.00, 304, '2023-05-12'),
(106, 'Keyboard', 'Electronics', 75.50, 302, '2023-06-18'),
(107, 'Standing Desk', 'Furniture', 450.75, 303, '2023-07-22'),
(108, 'Webcam HD', 'Electronics', 120.25, 305, '2023-08-30'),
(109, 'Office Plant', 'Decor', 35.00, 306, '2023-09-15'),
(110, 'Printer', 'Electronics', 250.75, 307, '2023-10-08'),
(111, 'Notebook Set', 'Office Supplies', 15.25, 308, '2023-11-20');

-- Create salespeople table
CREATE TABLE salespeople (
    salesperson_id INT PRIMARY KEY,
    name VARCHAR(100),
    region VARCHAR(50),
    hire_date DATE,
    salary DECIMAL(10,2),
    commission_rate DECIMAL(5,2)
);

INSERT INTO salespeople VALUES
(201, 'John Smith', 'North', '2022-01-15', 50000.00, 5.00),
(202, 'Sarah Johnson', 'South', '2022-03-20', 52000.00, 5.50),
(203, 'Mike Wilson', 'East', '2022-05-10', 48000.00, 4.50),
(204, 'Lisa Brown', 'West', '2022-07-08', 51000.00, 5.25);

-- Create customers table
CREATE TABLE customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    region VARCHAR(50),
    registration_date DATE,
    customer_tier VARCHAR(20)
);

INSERT INTO customers VALUES
(1001, 'Alice Cooper', 'alice@email.com', 'North', '2023-01-10', 'Gold'),
(1002, 'Bob Davis', 'bob@email.com', 'South', '2023-02-15', 'Silver'),
(1003, 'Carol White', 'carol@email.com', 'North', '2023-03-20', 'Bronze'),
(1004, 'David Lee', 'david@email.com', 'East', '2023-04-25', 'Gold'),
(1005, 'Eva Green', 'eva@email.com', 'South', '2023-05-30', 'Platinum'),
(1006, 'Frank Miller', 'frank@email.com', 'West', '2023-06-05', 'Silver'),
(1007, 'Grace Taylor', 'grace@email.com', 'North', '2023-07-10', 'Bronze'),
(1008, 'Henry Clark', 'henry@email.com', 'East', '2023-08-15', 'Gold'),
(1009, 'Ivy Adams', 'ivy@email.com', 'South', '2023-09-20', 'Silver'),
(1010, 'Jack Wilson', 'jack@email.com', 'West', '2023-10-25', 'Platinum'),
(1011, 'Kate Moore', 'kate@email.com', 'North', '2023-11-30', 'Bronze'),
(1012, 'Leo Turner', 'leo@email.com', 'South', '2023-12-05', 'Gold'),
(1013, 'Mia Garcia', 'mia@email.com', 'East', '2024-01-10', 'Silver'),
(1014, 'Noah Martinez', 'noah@email.com', 'West', '2024-01-15', 'Platinum'),
(1015, 'Olivia Rodriguez', 'olivia@email.com', 'North', '2024-01-20', 'Bronze'),
(1016, 'Paul Anderson', 'paul@email.com', 'South', '2024-01-25', 'Gold'),
(1017, 'Quinn Thompson', 'quinn@email.com', 'East', '2024-01-30', 'Silver'),
(1018, 'Ruby White', 'ruby@email.com', 'West', '2024-02-05', 'Bronze'),
(1019, 'Sam Brown', 'sam@email.com', 'North', '2024-02-10', 'Silver'),
(1020, 'Tina Davis', 'tina@email.com', 'South', '2024-02-15', 'Gold');

-- Create regions table
CREATE TABLE regions (
    region_name VARCHAR(50) PRIMARY KEY,
    manager_name VARCHAR(100),
    budget DECIMAL(12,2),
    target_sales DECIMAL(12,2)
);

INSERT INTO regions VALUES
('North', 'Tom Manager', 1000000.00, 1500000.00),
('South', 'Jane Director', 1200000.00, 1800000.00),
('East', 'Bill VP', 800000.00, 1200000.00),
('West', 'Sue Executive', 900000.00, 1350000.00);
