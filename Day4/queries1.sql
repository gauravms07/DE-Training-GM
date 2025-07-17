-- select emp_id, department, salary,
-- row_number() OVER (partition by department order by Salary desc) as Salary_Rank
-- from employees;

-- select department, AVG(salary)
-- from employees
-- group by department;

-- SELECT 
--     department,
--     AVG(salary) OVER (PARTITION BY department) AS dept_avg_salary
-- FROM 
--     employees;


