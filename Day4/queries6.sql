use day4;

-- MERGE INTO employees as target
-- using new_employees as source
-- on target.emp_id = source.emp_id
-- when matched then
-- 	update set salary = source.salary
-- when not matched then
-- 	insert (emp_id, name, salary) 
--     values (source.emp_id, source.name, source.salary);

select * from employees;

insert into employees(emp_id, name, department)
VALUES (101, 'Vikash', 'IT'),
(1,'Sujay','Sales')
on duplicate key update
name = VALUES(name),
department = VALUES(department);

select * from employees;