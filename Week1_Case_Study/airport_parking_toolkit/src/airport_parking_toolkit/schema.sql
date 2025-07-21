CREATE DATABASE Airport_Parking;

USE Airport_Parking; 

DROP TABLE IF EXISTS Vehicles;
DROP TABLE IF EXISTS Parking_Events;

CREATE TABLE Vehicles(
	vehicle_id VARCHAR(20) PRIMARY KEY,
    plate_number VARCHAR(20),
    v_type VARCHAR(20),
    owner_name VARCHAR(100)
    );
    
CREATE TABLE Parking_Events(
	event_id VARCHAR(20) PRIMARY KEY,
    vehicle_id VARCHAR(20),
	zone_id VARCHAR(20),
    entry_time DATETIME,
    exit_time DATETIME,
    paid_amt DECIMAL
    );
  
DROP TABLE IF EXISTS Parking_Zones;

CREATE TABLE Parking_Zones(
	zone_id VARCHAR(20) PRIMARY KEY,
    zone_name VARCHAR(20),
    zone_rate DECIMAL,
    is_valet BOOL
    );
    
INSERT INTO Vehicles
	VALUES ("V001","MH12AB1234","sedan","Rahul Sharma"), 
    ("V002","MH14XY9876","SUV","Neha Verma"),
	("V003","DL01CD4567","hatchback","Aamir Sheikh"),
	("V004","KA03ZX7788","SUV","Sneha Kulkarni"),
	("V005","TN09QW1100","sedan","Arun Raj"),
	("V006","MH12KL9988","EV","Manisha Pandey"),
	("V007","GJ05HH2299","SUV","Rakesh Singh");
    

    
INSERT INTO Parking_Events
	VALUES ("E001","V001","Z001","2024-07-18 08:00:00","2024-07-18 10:30:00",120.0),
    ("E002","V002","Z002","2024-07-18 09:00:00","2024-07-18 11:00:00",80.0),
	("E003","V003","Z004","2024-07-18 12:00:00","2024-07-18 12:45:00",70.0),
    ("E004","V001","Z003","2024-07-17 15:00:00","2024-07-18 15:00:00",300.0),
    ("E005","V004","Z005","2024-07-16 07:00:00","2024-07-16 10:00:00",75.0),
	("E006","V005","Z003","2024-07-15 18:00:00","2024-07-15 18:30:00",15.0),
	("E007","V002","Z001","2024-07-14 08:00:00","2024-07-14 09:00:00",50.0),
	("E008","V006","Z004","2024-07-18 10:15:00","2024-07-18 11:15:00",70.0),
	("E009","V007","Z001","2024-07-18 07:30:00","2024-07-18 08:00:00",25.0),
	("E010","V006","Z005","2024-07-17 06:00:00","2024-07-17 08:00:00",50.0);
    
INSERT INTO Parking_Zones
	VALUES ("Z001","Short Term A",50,0),
	("Z002","Short Term B",40,0),
	("Z003","Long Term A",30,0),
	("Z004","Valet A",70,1),
	("Z005","Economy Lot B",25,0);
    
DROP TABLE IF EXISTS Parking_Event_Audit;

CREATE TABLE parking_event_audit (
  audit_id INT AUTO_INCREMENT PRIMARY KEY,
  event_id VARCHAR(20),
  vehicle_id VARCHAR(20),
  zone_id VARCHAR(20),
  action_type VARCHAR(10),
  action_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

DELIMITER //

CREATE TRIGGER log_parking_event_insert
AFTER INSERT ON parking_events
FOR EACH ROW
BEGIN
  INSERT INTO parking_event_audit (event_id, vehicle_id, zone_id, action_type)
  VALUES (NEW.event_id, NEW.vehicle_id, NEW.zone_id, 'INSERT');
END;
//

DELIMITER ;



    


	
    

