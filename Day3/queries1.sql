-- select * from Employees;
-- select * from projects;

-- select name, department,proj_name
-- from Employees E INNER JOIN projects P on E.emp_id = P.emp_id;

select * from Employees E LEFT JOIN projects P on E.emp_id = P.emp_id;

select * from Employees E Right Join projects P on E.emp_id = P.emp_id;

select * from Employees E JOIN projects P;

select * from Employees E LEFT JOIN projects P on E.emp_id = P.emp_id UNION
select * from Employees E RIGHT JOIN projects P on E.emp_id = P.emp_id;