SELECT zone_id, COUNT(*) AS total_visits, SUM(paid_amt) AS total_revenue
FROM parking_events
GROUP BY zone_id
ORDER BY total_revenue DESC;

SELECT vehicle_id, COUNT(*) AS visits, SUM(TIMESTAMPDIFF(MINUTE, entry_time, exit_time)) AS total_duration_minutes
FROM parking_events
GROUP BY vehicle_id
ORDER BY visits DESC;

INSERT INTO Parking_Events
VALUES ("E011","V009","Z002","2024-07-15 12:00","2024-07-16 00:30",500);

SELECT * FROM parking_event_audit;

SELECT pz.zone_name, SUM(pe.paid_amt) AS total_revenue
FROM parking_events pe
JOIN parking_zones pz ON pe.zone_id = pz.zone_id
GROUP BY pz.zone_name
ORDER BY total_revenue DESC;

SELECT pz.zone_name, COUNT(*) AS total_parkings
FROM parking_events pe
JOIN parking_zones pz ON pe.zone_id = pz.zone_id
GROUP BY pz.zone_name
ORDER BY total_parkings DESC;

SELECT pz.zone_name, SUM(TIMESTAMPDIFF(MINUTE, pe.entry_time, pe.exit_time)) AS total_minutes_occupied
FROM parking_events pe
JOIN parking_zones pz ON pe.zone_id = pz.zone_id
WHERE pe.exit_time IS NOT NULL
GROUP BY pz.zone_name
ORDER BY total_minutes_occupied DESC;









