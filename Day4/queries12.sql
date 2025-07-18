DELIMITER $$

CREATE TRIGGER LOG_SALARY_CHANGE
AFTER UPDATE ON employees
FOR EACH ROW
BEGIN 
INSERT INTO audit_log(emp_id, old_salary, new_salary) 
VALUES(old.emp_id, old.salary, new.salary);
END $$

DELIMITER ;

Select * from audit_log;

update employees set salary = 10 
where emp_id = 1;