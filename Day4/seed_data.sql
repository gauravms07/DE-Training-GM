-- create database day4;

use day4;

DROP TABLE IF EXISTS employees;
CREATE TABLE employees (
  emp_id INT PRIMARY KEY,
  name VARCHAR(50),
  department VARCHAR(50),
  salary INT
);

INSERT INTO employees (emp_id, name, department, salary) VALUES
(1, 'Alice', 'HR', 50000),
(2, 'Bob', 'HR', 60000),
(3, 'Charlie', 'IT', 80000),
(4, 'David', 'IT', 75000),
(5, 'Eve', 'HR', 55000);

DROP TABLE IF EXISTS new_employees;
CREATE TABLE new_employees (
  emp_id INT,
  name VARCHAR(50),
  salary INT
);

INSERT INTO new_employees (emp_id, name, salary) VALUES
(1, 'Alice', 51000),
(6, 'Frank', 62000);


DROP TABLE IF EXISTS customers;
CREATE TABLE customers (
  name VARCHAR(50)
);

INSERT INTO customers (name) VALUES
('Alice'), ('Bob'), ('Charlie');

DROP TABLE IF EXISTS suppliers;
CREATE TABLE suppliers (
  name VARCHAR(50)
);

INSERT INTO suppliers (name) VALUES
('Bob'), ('David');

DROP TABLE IF EXISTS orders;
CREATE TABLE orders (
  order_id INT PRIMARY KEY,
  region VARCHAR(50),
  amount INT
);

INSERT INTO orders (order_id, region, amount) VALUES
(1, 'North', 200),
(2, 'North', 300),
(3, 'South', 400),
(4, 'South', 100);

DROP TABLE IF EXISTS accounts;
CREATE TABLE accounts (
  acc_id INT PRIMARY KEY,
  balance INT
);

INSERT INTO accounts (acc_id, balance) VALUES
(1, 1000),
(2, 1000);


DROP TABLE IF EXISTS audit_log;
CREATE TABLE audit_log (
  emp_id INT,
  old_salary INT,
  new_salary INT,
  changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
