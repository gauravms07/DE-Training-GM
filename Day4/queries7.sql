use day4;

CREATE TABLE sales (
  sale_id INT PRIMARY KEY,
  sale_date DATE,
  amount DECIMAL(10,2)
);

INSERT INTO sales VALUES
(1, '2024-01-05', 1500),
(2, '2024-01-20', 2200),
(3, '2024-02-10', 3000),
(4, '2024-03-12', 3500),
(5, '2024-03-20', 1800);

SELECT
  DATE_FORMAT(sale_date, '%Y-%m') AS sale_month,
  SUM(amount) AS monthly_total,
  SUM(SUM(amount)) OVER (
    ORDER BY DATE_FORMAT(sale_date, '%Y-%m')
  ) AS cumulative_total
FROM sales
GROUP BY DATE_FORMAT(sale_date, '%Y-%m')
ORDER BY sale_month;

