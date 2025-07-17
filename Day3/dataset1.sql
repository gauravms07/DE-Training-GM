CREATE TABLE Employees (
    emp_id INT PRIMARY KEY,
    name VARCHAR(100),
    department VARCHAR(50),
    salary DECIMAL(10, 2),
    manager_id INT
);
INSERT INTO Employees (emp_id, name, department, salary, manager_id) VALUES
(101, 'Alice Johnson', 'Engineering', 75000, NULL),
(102, 'Bob Smith', 'Engineering', 72000, 101),
(103, 'Charlie Lee', 'Sales', 68000, NULL),
(104, 'Diana Prince', 'Sales', 70000, 103),
(105, 'Evan Wright', 'HR', 50000, NULL),
(106, 'Fiona Glen', 'Engineering', 71000, 101),
(107, 'George Martin', 'HR', 52000, 105),
(108, 'Hannah Kim', 'Marketing', 65000, NULL),
(109, 'Ian Bell', 'Marketing', 63000, 108),
(110, 'Jane Fox', 'Engineering', 73000, 101);


CREATE TABLE Projects (
    proj_id INT PRIMARY KEY,
    proj_name VARCHAR(100),
    emp_id INT,
    FOREIGN KEY (emp_id) REFERENCES Employees(emp_id)
);


INSERT INTO Projects (proj_id, proj_name, emp_id) VALUES
(201, 'Apollo', 102),
(202, 'Zeus', 106),
(203, 'Hermes', 110),
(204, 'Hera', 104),
(205, 'Poseidon', 103),
(206, 'Athena', 101),
(207, 'Ares', 108),
(208, 'Demeter', 109),
(209, 'Hephaestus', 105);

CREATE INDEX idx_employees_department ON Employees(department);
CREATE INDEX idx_projects_emp_id ON Projects(emp_id);