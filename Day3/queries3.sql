select C.name, sum(O.amount) as total_amount from customers C INNER JOIN orders O on C.id = O.customer_id group by C.name;

select * from (select C.name, sum(O.amount) as total_amount from customers C INNER JOIN orders O on C.id = O.customer_id group by C.name
) D where total_amount > 10000;

-- SELECT order_id, customer_name, order_date
-- FROM (
--     SELECT *,
--            ROW_NUMBER() OVER (PARTITION BY customer_name ORDER BY order_date DESC) AS rn
--     FROM orders
-- ) ranked
-- WHERE rn = 1;









