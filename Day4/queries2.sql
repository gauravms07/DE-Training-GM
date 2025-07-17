-- select department as Department, SUM(salary) as Total_Department_Salary
-- from employees
-- group by department;

WITH ranked_employees AS (
  SELECT
    emp_id,
    name,
    department,
    salary,
    DENSE_RANK() OVER (
      PARTITION BY department 
      ORDER BY salary DESC
    ) AS rank_in_dept,
    SUM(salary) OVER (
      PARTITION BY department
    ) AS total_dept_salary
  FROM employees
)

SELECT *
FROM ranked_employees
WHERE rank_in_dept <= 3;


