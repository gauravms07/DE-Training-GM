DELIMITER $$
USE `day4`$$
CREATE DEFINER=`root`@`localhost` PROCEDURE `InsertEmployee`(EMPName VARCHAR(1000))
BEGIN
 -- Insert the new employee
    INSERT INTO Sample(employee_name) VALUES (EMPName);
    -- Update EMPID for the last inserted row
    UPDATE Sample
    SET EMPID = CONCAT('EMP00', LAST_INSERT_ID())
    WHERE id = LAST_INSERT_ID();
END$$

DELIMITER ;
;