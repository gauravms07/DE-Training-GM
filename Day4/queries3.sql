use day4;

create table new_orders(
order_id int PRIMARY KEY,
c_id int,
region varchar(30),
amount int,
order_date datetime
);


create table new_customers(
id int PRIMARY KEY,
name varchar(40),
region varchar(40)
);

