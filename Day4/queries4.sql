-- INSERT INTO new_customers (id, name, region) VALUES
-- (1, 'Alice', 'North'),
-- (2, 'Bob', 'South'),
-- (3, 'Charlie', 'East'),
-- (4, 'Diana', 'West'),
-- (5, 'Ethan', 'North'),
-- (6, 'Fiona', 'South'),
-- (7, 'George', 'East'),
-- (8, 'Hannah', 'West'),
-- (9, 'Ivan', 'North'),
-- (10, 'Julia', 'South');

-- INSERT INTO new_orders (order_id, c_id, region, amount, order_date) VALUES
-- (101, 1, 'North', 500, '2024-06-10'),
-- (102, 2, 'South', 800, '2023-05-15'),   -- over a year ago
-- (103, 3, 'East', 300, '2024-10-01'),
-- (104, 4, 'West', 400, '2022-08-20'),   -- over a year ago
-- (105, 5, 'North', 700, '2024-12-05'),
-- (106, 1, 'North', 600, '2025-03-12');

select emp_id, department, salary,
LAG(salary) OVER (partition by department order by salary) AS prev_salary
FROM employees;

select emp_id, department, salary, 
sum(salary) over (partition by department) as dept_total
from employees;

with dept_avg as (
select department, avg(salary) as average_salary
from employees
group by department)
select * from dept_avg where average_salary > 50000;

select * 
from (
	select department, avg(salary) as avg_salary
    from employees
    group by department) D
where avg_salary > 50000;

select name as unionname from (select name from customers
union
select name from suppliers) Final;

select name from customers
except 
select name from suppliers;

create view sales_summary as
select region, sum(amount) as total_sales
from orders
group by region;




