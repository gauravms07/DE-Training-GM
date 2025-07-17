-- DROP existing tables if any
DROP TABLE IF EXISTS page_views, orders, customers, regions;

-- Create table: regions
CREATE TABLE regions (
    region_id INT PRIMARY KEY,
    region_name VARCHAR(100)
);

-- Insert data into regions
INSERT INTO regions (region_id, region_name) VALUES
(1, 'North'),
(2, 'South'),
(3, 'East'),
(4, 'West');

-- Create table: customers
CREATE TABLE customers (
    id INT PRIMARY KEY,
    name VARCHAR(100),
    region INT,
    FOREIGN KEY (region) REFERENCES regions(region_id)
);

-- Insert data into customers
INSERT INTO customers (id, name, region) VALUES
(1, 'Alice', 1),
(2, 'Bob', 2),
(3, 'Charlie', 3),
(4, 'Diana', 4),
(5, 'Evan', 1);

-- Create table: orders
CREATE TABLE orders (
    id INT PRIMARY KEY,
    customer_id INT,
    order_date DATE,
    amount DECIMAL(10, 2),
    FOREIGN KEY (customer_id) REFERENCES customers(id)
);

-- Insert data into orders
INSERT INTO orders (id, customer_id, order_date, amount) VALUES
(101, 1, '2024-01-10', 6000),
(102, 1, '2024-03-15', 5200),
(103, 2, '2024-02-20', 450),
(104, 2, '2024-04-01', 1200),
(105, 2, '2024-06-22', 7000),
(106, 3, '2024-05-05', 11000),
(107, 4, '2024-01-11', 990),
(108, 5, '2024-06-10', 3000),
(109, 5, '2024-07-01', 4000),
(110, 5, '2024-07-10', 5000);

-- Create table: page_views
CREATE TABLE page_views (
    id INT PRIMARY KEY,
    user_id INT,
    page VARCHAR(100),
    view_time DATETIME
);

-- Insert data into page_views
INSERT INTO page_views (id, user_id, page, view_time) VALUES
(1, 1, 'home', '2024-07-01 10:00:00'),
(2, 1, 'products', '2024-07-01 10:05:00'),
(3, 1, 'cart', '2024-07-01 10:10:00'),
(4, 2, 'home', '2024-07-01 09:00:00'),
(5, 2, 'products', '2024-07-01 09:15:00'),
(6, 2, 'checkout', '2024-07-01 09:45:00'),
(7, 3, 'home', '2024-07-01 11:00:00'),
(8, 3, 'products', '2024-07-01 11:05:00'),
(9, 3, 'products', '2024-07-01 11:10:00'),
(10, 3, 'products', '2024-07-01 11:15:00'),
(11, 3, 'cart', '2024-07-01 11:20:00');