-- Employee Hierarchy Dataset for SQL Practice
-- This dataset contains hierarchical employee data for recursive CTE practice

-- Create employees table
CREATE TABLE employees (
    employee_id INT PRIMARY KEY,
    name VARCHAR(100),
    manager_id INT,
    department VARCHAR(50),
    salary DECIMAL(10,2),
    hire_date DATE,
    job_title VARCHAR(100)
);

-- Insert sample data with hierarchy
INSERT INTO employees VALUES
(1, 'CEO John', NULL, 'Executive', 200000.00, '2020-01-01', 'Chief Executive Officer'),
(2, 'CTO Sarah', 1, 'Technology', 180000.00, '2020-02-01', 'Chief Technology Officer'),
(3, 'CFO Mike', 1, 'Finance', 170000.00, '2020-03-01', 'Chief Financial Officer'),
(4, 'VP Engineering Lisa', 2, 'Technology', 150000.00, '2020-04-01', 'VP Engineering'),
(5, 'VP Sales Tom', 1, 'Sales', 160000.00, '2020-05-01', 'VP Sales'),
(6, 'Senior Manager Alice', 4, 'Technology', 120000.00, '2020-06-01', 'Senior Engineering Manager'),
(7, 'Senior Manager Bob', 4, 'Technology', 125000.00, '2020-07-01', 'Senior Engineering Manager'),
(8, 'Manager Carol', 6, 'Technology', 100000.00, '2020-08-01', 'Engineering Manager'),
(9, 'Manager David', 6, 'Technology', 105000.00, '2020-09-01', 'Engineering Manager'),
(10, 'Manager Eva', 7, 'Technology', 110000.00, '2020-10-01', 'Engineering Manager'),
(11, 'Senior Developer Frank', 8, 'Technology', 95000.00, '2020-11-01', 'Senior Software Engineer'),
(12, 'Senior Developer Grace', 8, 'Technology', 98000.00, '2020-12-01', 'Senior Software Engineer'),
(13, 'Developer Henry', 9, 'Technology', 85000.00, '2021-01-01', 'Software Engineer'),
(14, 'Developer Ivy', 9, 'Technology', 88000.00, '2021-02-01', 'Software Engineer'),
(15, 'Developer Jack', 10, 'Technology', 90000.00, '2021-03-01', 'Software Engineer'),
(16, 'Senior Manager Kate', 5, 'Sales', 115000.00, '2020-06-15', 'Senior Sales Manager'),
(17, 'Manager Leo', 16, 'Sales', 95000.00, '2020-07-15', 'Sales Manager'),
(18, 'Manager Mia', 16, 'Sales', 98000.00, '2020-08-15', 'Sales Manager'),
(19, 'Sales Rep Noah', 17, 'Sales', 70000.00, '2020-09-15', 'Sales Representative'),
(20, 'Sales Rep Olivia', 17, 'Sales', 72000.00, '2020-10-15', 'Sales Representative'),
(21, 'Sales Rep Paul', 18, 'Sales', 75000.00, '2020-11-15', 'Sales Representative'),
(22, 'Sales Rep Quinn', 18, 'Sales', 73000.00, '2020-12-15', 'Sales Representative'),
(23, 'Finance Manager Ruby', 3, 'Finance', 110000.00, '2020-07-01', 'Finance Manager'),
(24, 'Accountant Sam', 23, 'Finance', 65000.00, '2020-08-01', 'Senior Accountant'),
(25, 'Accountant Tina', 23, 'Finance', 68000.00, '2020-09-01', 'Senior Accountant');

-- Create departments table
CREATE TABLE departments (
    department_name VARCHAR(50) PRIMARY KEY,
    budget DECIMAL(12,2),
    head_count INT,
    location VARCHAR(100)
);

INSERT INTO departments VALUES
('Executive', 500000.00, 1, 'Corporate HQ'),
('Technology', 2000000.00, 15, 'Tech Center'),
('Sales', 1500000.00, 8, 'Sales Office'),
('Finance', 800000.00, 3, 'Finance Center');

-- Create projects table
CREATE TABLE projects (
    project_id INT PRIMARY KEY,
    project_name VARCHAR(100),
    department VARCHAR(50),
    budget DECIMAL(10,2),
    start_date DATE,
    end_date DATE,
    status VARCHAR(20)
);

INSERT INTO projects VALUES
(1, 'Website Redesign', 'Technology', 50000.00, '2024-01-01', '2024-03-31', 'Active'),
(2, 'Mobile App Development', 'Technology', 75000.00, '2024-02-01', '2024-06-30', 'Active'),
(3, 'Q1 Sales Campaign', 'Sales', 30000.00, '2024-01-15', '2024-03-31', 'Active'),
(4, 'Financial System Upgrade', 'Finance', 40000.00, '2024-01-01', '2024-04-30', 'Active'),
(5, 'Customer Portal', 'Technology', 60000.00, '2024-03-01', '2024-08-31', 'Planning'),
(6, 'Q2 Sales Campaign', 'Sales', 35000.00, '2024-04-01', '2024-06-30', 'Planning'),
(7, 'Budget Planning 2025', 'Finance', 15000.00, '2024-09-01', '2024-12-31', 'Planning');

-- Create employee_projects table (many-to-many relationship)
CREATE TABLE employee_projects (
    employee_id INT,
    project_id INT,
    role VARCHAR(50),
    hours_allocated INT,
    PRIMARY KEY (employee_id, project_id)
);

INSERT INTO employee_projects VALUES
(4, 1, 'Project Manager', 40),
(6, 1, 'Lead Developer', 35),
(8, 1, 'Developer', 30),
(11, 1, 'Developer', 25),
(4, 2, 'Project Manager', 40),
(7, 2, 'Lead Developer', 35),
(9, 2, 'Developer', 30),
(13, 2, 'Developer', 25),
(16, 3, 'Campaign Manager', 40),
(17, 3, 'Sales Lead', 35),
(19, 3, 'Sales Rep', 30),
(21, 3, 'Sales Rep', 25),
(23, 4, 'Project Manager', 40),
(24, 4, 'Analyst', 35),
(25, 4, 'Analyst', 30);
