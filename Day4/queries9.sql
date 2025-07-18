create table Sample(
id int primary key auto_increment,
emp_id nvarchar(10),
employee_name varchar(200)
);

DELIMITER $$

CREATE PROCEDURE insert_employee(IN EMPName VARCHAR(1000))
BEGIN
    -- Insert the new employee
    INSERT INTO Sample(employee_name) VALUES (EMPName);

    -- Update EMPID for the last inserted row
    UPDATE Sample
    SET EMPID = CONCAT('EMP', LAST_INSERT_ID())
    WHERE id = LAST_INSERT_ID();
END $$

DELIMITER ;

select * from Sample;
